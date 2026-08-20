# pyright: reportArgumentType=false

import json
from urllib.parse import unquote
from unittest.mock import Mock, patch
from typing import cast

import pytest
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from jsonschema import validate
from tap_itsperfect.client import ItsPerfectStream
from tap_itsperfect.tap import TapItsPerfect


def config():
    return {
        "api_url": "https://example.itsperfect.it",
        "username": "user",
        "password": "password",
        "start_date": "2025-01-01T00:00:00Z",
        "page_size": 250,
    }


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def stream(name="stocks") -> ItsPerfectStream:
    return cast(ItsPerfectStream, TapItsPerfect(config=config()).streams[name])


def test_discovery_contains_derived_streams_and_safe_replication():
    tap = TapItsPerfect(config=config())
    assert set(tap.streams) == {
        "products",
        "stocks",
        "stores",
        "warehouses",
        "vendors",
        "sales_orders",
        "sales_order_lines",
        "purchase_orders",
        "purchase_order_lines",
        "puts",
        "put_lines",
        "qualities",
        "quality_compositions",
    }
    assert tap.alerting_level.name == "WARNING"
    assert tap.streams["products"].replication_key == "last_update_timestamp"
    assert {
        name for name, stream_instance in tap.streams.items()
        if stream_instance.replication_key
    } == {"products"}


def test_credentials_are_secret_in_discovery_schema():
    properties = TapItsPerfect.config_jsonschema["properties"]
    assert properties["username"]["secret"]
    assert properties["password"]["secret"]


def test_authentication_success_is_cached():
    response = Response(200, {"token": "token", "expires_in": 1800})
    with patch("tap_itsperfect.client.requests.post", return_value=response) as post:
        client = stream()
        assert client._access_token() == "token"
        assert client._access_token() == "token"
    assert post.call_count == 1
    assert post.call_args.kwargs["timeout"] == 60


@pytest.mark.parametrize(
    ("status", "error"),
    [(401, InvalidCredentialsError), (403, FatalAPIError), (400, FatalAPIError)],
)
def test_authentication_permanent_errors_are_not_retried(status, error):
    with patch(
        "tap_itsperfect.client.requests.post",
        return_value=Response(status, {"error": "redacted"}),
    ) as post:
        with pytest.raises(error):
            stream()._authenticate()
    assert post.call_count == 1


def test_authentication_rejects_malformed_success():
    with patch(
        "tap_itsperfect.client.requests.post",
        return_value=Response(200, ValueError("bad json")),
    ):
        with pytest.raises(FatalAPIError, match="malformed JSON"):
            stream()._authenticate()


def test_resource_401_refreshes_once():
    client = stream()
    request = Mock(headers={"Authorization": "Bearer expired"})
    fresh_response = Response(200, [])
    with patch(
        "tap_itsperfect.client.RESTStream._request",
        side_effect=[InvalidCredentialsError("expired"), fresh_response],
    ) as send, patch.object(client, "_authenticate", return_value="fresh") as authenticate:
        assert client._request(request, None) is fresh_response
    assert send.call_count == 2
    authenticate.assert_called_once_with()
    assert request.headers["Authorization"] == "Bearer fresh"


def test_response_status_handling_and_retry_after_bound():
    client = stream()
    with patch("tap_itsperfect.client.time.sleep") as sleep:
        with pytest.raises(RetriableAPIError):
            client.validate_response(Response(429, [], {"Retry-After": "999"}))
    sleep.assert_called_once_with(60.0)

    with pytest.raises(InvalidCredentialsError):
        client.validate_response(Response(401, []))
    with pytest.raises(FatalAPIError):
        client.validate_response(Response(422, []))
    with pytest.raises(FatalAPIError, match="not found"):
        client.validate_response(Response(404, None))
    client.validate_response(Response(204, None))
    stream("sales_order_lines").validate_response(Response(404, None))


def test_parse_response_handles_empty_and_malformed_responses():
    client = stream()
    assert list(client.parse_response(Response(204))) == []
    assert list(stream("sales_order_lines").parse_response(Response(404))) == []
    with pytest.raises(FatalAPIError, match="not found"):
        list(client.parse_response(Response(404)))
    assert list(client.parse_response(Response(200, [{"id": 1}]))) == [{"id": 1}]
    with pytest.raises(FatalAPIError, match="malformed JSON"):
        list(client.parse_response(Response(200, json.JSONDecodeError("bad", "x", 0))))
    with pytest.raises(FatalAPIError, match="expected a list"):
        list(client.parse_response(Response(200, {"items": []})))


def test_pagination_first_continuation_final_and_empty_pages():
    client = stream()
    first = Response(
        200,
        [{"id": 1}],
        {
            "X-Pagination-Current-Page": "1",
            "X-Pagination-Page-Count": "3",
        },
    )
    middle = Response(
        200,
        [{"id": 2}],
        {
            "X-Pagination-Current-Page": "2",
            "X-Pagination-Page-Count": "3",
        },
    )
    final = Response(
        200,
        [{"id": 3}],
        {
            "X-Pagination-Current-Page": "3",
            "X-Pagination-Page-Count": "3",
        },
    )
    empty = Response(
        200,
        [],
        {
            "X-Pagination-Current-Page": "1",
            "X-Pagination-Page-Count": "0",
        },
    )
    assert client.get_next_page_token(first, None) == 2
    assert client.get_next_page_token(middle, 2) == 3
    assert client.get_next_page_token(final, 3) is None
    assert client.get_next_page_token(empty, None) is None


def test_pagination_rejects_missing_skipped_and_early_empty_pages():
    client = stream()
    with pytest.raises(FatalAPIError, match="pagination headers"):
        client.get_next_page_token(Response(200, [{"id": 1}]), None)
    with pytest.raises(FatalAPIError, match="returned page 3, expected 2"):
        client.get_next_page_token(
            Response(
                200,
                [{"id": 1}],
                {
                    "X-Pagination-Current-Page": "3",
                    "X-Pagination-Page-Count": "4",
                },
            ),
            2,
        )
    with pytest.raises(FatalAPIError, match="empty page before the final page"):
        client.get_next_page_token(
            Response(
                200,
                [],
                {
                    "X-Pagination-Current-Page": "2",
                    "X-Pagination-Page-Count": "3",
                },
            ),
            2,
        )


def test_incremental_params_resume_from_state_marker():
    products = stream("products")
    with patch.object(
        products,
        "get_starting_replication_key_value",
        return_value="2026-01-02T03:04:05Z",
    ):
        params = products.get_url_params(None, 4)
    assert params == {
        "limit": 250,
        "page": 4,
        "includes": "colors,barcodes",
        "last_update_timestamp>": "2026-01-02T03:04:05Z",
    }
    with patch.object(
        products,
        "get_starting_replication_key_value",
        return_value="2026-01-02T03:04:05Z",
    ), patch.object(products, "_access_token", return_value="token"):
        request = products.prepare_request(None, 4)
    assert request.url is not None
    assert "last_update_timestamp>=2026-01-02T03:04:05Z" in unquote(request.url)


def test_incremental_records_require_a_non_null_marker():
    products = stream("products")
    with pytest.raises(FatalAPIError, match="omitted replication key"):
        products.post_process({"id": 1, "last_update_timestamp": None})
    record = {
        "id": 1,
        "status": 6,
        "type": 1,
        "composition": [],
        "sales_price": "0.00",
        "purchase_price": 1.5,
        "last_update_timestamp": "2026-01-02T03:04:05Z",
    }
    assert products.post_process(record) == record
    validate(record, products.schema)


def test_reference_status_codes_accept_live_integer_values():
    warehouse = {"id": 1, "warehouse": "Test", "active": 1}
    validate(warehouse, stream("warehouses").schema)
    validate({"id": 1, "active": 1}, stream("qualities").schema)


def test_sales_orders_keep_only_completed_and_cancelled_records():
    orders = stream("sales_orders")
    sent = orders.post_process({"id": 1, "status": "1"})
    cancelled = orders.post_process({"id": 2, "status": 2})
    assert sent is not None
    assert cancelled is not None
    assert cancelled["status"] == "2"
    validate(cancelled, orders.schema)
    assert orders.post_process({"id": 3, "status": "0"}) is None


def test_child_identity_and_nested_schema_are_preserved():
    lines = stream("sales_order_lines")
    row = lines.post_process(
        {"id": 9, "item": {"id": 8}, "quantity": None},
        {"sales_order_id": 7},
    )
    assert row is not None
    assert row["sales_order_id"] == 7
    assert lines.primary_keys == ["sales_order_id", "id"]
    assert lines.schema["properties"]["item"]["type"] == ["object", "null"]
    assert lines.schema["properties"]["quantity"]["type"] == ["string", "null"]
    with patch.object(lines, "_access_token", return_value="token"):
        request = lines.prepare_request({"sales_order_id": 7}, None)
    assert request.url is not None
    assert request.url.startswith(
        "https://example.itsperfect.it/api/v3/sales_orders/7/lines?"
    )


def test_purchase_and_put_lines_preserve_live_etl_fields():
    purchase_lines = stream("purchase_order_lines")
    assert "price_rcy" in purchase_lines.schema["properties"]
    assert "purchase_price" not in purchase_lines.schema["properties"]

    put_lines = stream("put_lines")
    row = put_lines.post_process(
        {"id": 3, "order_id": 2, "quantity": "1"},
        {"put_id": 1},
    )
    assert row == {"id": 3, "order_id": 2, "quantity": "1", "put_id": 1}
    assert put_lines.schema["properties"]["order_id"] == {"type": "integer"}
