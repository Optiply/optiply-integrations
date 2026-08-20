import copy
import json
from datetime import datetime, timezone
from io import StringIO
from unittest.mock import Mock, patch

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from target_itsperfect.client import AmbiguousWriteError
from target_itsperfect.sinks import BuyOrdersSink
from target_itsperfect.target import TargetItsPerfect


SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": "integer"}},
}


def config():
    return {
        "api_url": "https://example.itsperfect.it",
        "username": "user",
        "password": "password",
        "warehouse_id": 5,
        "request_timeout_seconds": 30,
    }


def response(status, payload=None, headers=None, text=None, pagination=True):
    result = requests.Response()
    result.status_code = status
    result.headers.update(headers or {})
    if status == 200 and isinstance(payload, list) and pagination:
        result.headers.setdefault("X-Pagination-Current-Page", "1")
        result.headers.setdefault("X-Pagination-Page-Count", "1" if payload else "0")
    if payload is not None:
        result._content = json.dumps(payload).encode()
        result.headers["Content-Type"] = "application/json"
    else:
        result._content = (text or "").encode()
    return result


def sink():
    target = TargetItsPerfect(config=config())
    return BuyOrdersSink(target, "BuyOrders", SCHEMA, ["id"])


def input_order(price: str | None = "3.50"):
    line = {
        "product_remoteId": "101",
        "quantity": "2",
        "sku": "ignored-as-barcode",
    }
    if price is not None:
        line["purchase_price"] = price
    return {
        "id": 123,
        "customer_id": 7,
        "transaction_date": "2026-08-20T10:00:00Z",
        "created_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "line_items": [line],
    }


def mapped_order(price: str | None = "3.50"):
    return sink().preprocess_record(input_order(price), {})


def existing_order(price="3.50"):
    return {
        "id": 99,
        "reference": "OPTIPLY-123",
        "vendor": {"id": 7},
        "warehouse": {"id": 5},
        "date": "2026-08-20",
        "lines": [
            {
                "id": 1001,
                "item": {"id": 101},
                "quantity_ordered": "2.000",
                "purchase_price": price,
            }
        ],
    }


def test_target_routes_only_buy_orders():
    target = TargetItsPerfect(config=config())
    assert target.get_sink_class("BuyOrders") is BuyOrdersSink
    with pytest.raises(ValueError, match="Unsupported stream"):
        target.get_sink_class("Products")
    with pytest.raises(ValueError, match="Unsupported stream"):
        target.get_sink_class("BuyOrderCancellations")


def test_config_secrets_and_warning_alerting():
    properties = TargetItsPerfect.config_jsonschema["properties"]
    assert properties["username"]["secret"]
    assert properties["password"]["secret"]
    assert "reference_prefix" not in properties
    assert TargetItsPerfect.alerting_level.name == "WARNING"
    assert TargetItsPerfect.MAX_PARALLELISM == 1


def test_maps_buy_order_and_lines_atomically():
    payload = mapped_order()
    assert payload == {
        "vendor": {"id": 7},
        "warehouse": {"id": 5},
        "reference": "OPTIPLY-123",
        "date": "2026-08-20",
        "lines": [
            {
                "item_id": "101",
                "quantity": 2,
                "price": "3.50",
            }
        ],
    }
    assert "barcode" not in payload["lines"][0]


def test_maps_json_lines_and_fractional_quantity():
    record = input_order(price=None)
    record["line_items"] = json.dumps(
        [{"product_remoteId": 101, "quantity": "1.5"}]
    )
    payload = sink().preprocess_record(record, {})
    assert payload["lines"] == [{"item_id": "101", "quantity": 1.5}]


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda record: record.pop("id"), "requires one of: id"),
        (
            lambda record: record.pop("customer_id"),
            "requires one of: targetSupplierId",
        ),
        (lambda record: record.update(line_items=[]), "at least one line"),
        (
            lambda record: record.update(line_items=[{"quantity": 1}]),
            "product_remoteId",
        ),
        (
            lambda record: record.update(
                line_items=[{"product_remoteId": 1, "quantity": 0}]
            ),
            "quantity must be positive",
        ),
    ],
)
def test_rejects_invalid_records(mutate, message):
    record = input_order()
    mutate(record)
    with pytest.raises(InvalidPayloadError, match=message):
        sink().preprocess_record(record, {})


def test_rejects_invalid_configured_warehouse():
    target = TargetItsPerfect(config={**config(), "warehouse_id": 0})
    target_sink = BuyOrdersSink(target, "BuyOrders", SCHEMA, ["id"])
    with pytest.raises(InvalidPayloadError, match="warehouse ID must be positive"):
        target_sink.preprocess_record(input_order(), {})


def test_authentication_success_is_cached():
    target_sink = sink()
    target_sink._session.post = Mock(
        return_value=response(200, {"token": "token", "expires_in": 1800})
    )
    assert target_sink._access_token() == "token"
    assert target_sink._access_token() == "token"
    assert target_sink._session.post.call_count == 1


@pytest.mark.parametrize(
    "status, error",
    [(401, InvalidCredentialsError), (403, FatalAPIError), (400, FatalAPIError)],
)
def test_authentication_permanent_errors_are_not_retried(status, error):
    target_sink = sink()
    target_sink._session.post = Mock(return_value=response(status, {"error": "x"}))
    with pytest.raises(error):
        target_sink._authenticate()
    assert target_sink._session.post.call_count == 1


def test_get_retries_rate_limit_and_bounds_retry_after():
    target_sink = sink()
    setattr(target_sink._target, "_itsperfect_token", "token")
    setattr(target_sink._target, "_itsperfect_token_expires_at", float("inf"))
    target_sink._session.get = Mock(
        side_effect=[
            response(429, [], {"Retry-After": "999"}),
            response(200, []),
        ]
    )
    with patch("target_itsperfect.client.time.sleep") as sleep:
        assert target_sink.lookup_purchase_order("OPTIPLY-123") is None
    sleep.assert_called_once_with(60.0)
    assert target_sink._session.get.call_count == 2


def test_lookup_requires_exact_unique_reference():
    target_sink = sink()
    target_sink._get = Mock(
        return_value=response(
            200,
            [
                {"id": 1, "reference": "OPTIPLY-123"},
                {"id": 2, "reference": "OPTIPLY-123"},
            ],
        )
    )
    with pytest.raises(FatalAPIError, match="duplicate purchase orders"):
        target_sink.lookup_purchase_order("OPTIPLY-123")


def test_lookup_rejects_multiple_pages_for_one_reference():
    target_sink = sink()
    target_sink._get = Mock(
        return_value=response(
            200,
            [],
            {
                "X-Pagination-Current-Page": "1",
                "X-Pagination-Page-Count": "2",
            },
        )
    )
    with pytest.raises(FatalAPIError, match="not a unique first page"):
        target_sink.lookup_purchase_order("OPTIPLY-123")


def test_lookup_requires_pagination_headers_before_create():
    target_sink = sink()
    target_sink._get = Mock(
        return_value=response(200, [], pagination=False)
    )
    with pytest.raises(FatalAPIError, match="omitted valid pagination headers"):
        target_sink.lookup_purchase_order("OPTIPLY-123")


def test_creates_and_confirms_new_order():
    target_sink = sink()
    payload = mapped_order()
    target_sink.lookup_purchase_order = Mock(return_value=None)
    target_sink._post_purchase_order_once = Mock(
        return_value=response(201, {"insertId": 99})
    )
    target_sink._wait_for_purchase_order = Mock(return_value=existing_order())
    remote_id, was_existing = target_sink.ensure_purchase_order(payload)
    assert (remote_id, was_existing) == (99, False)


def test_accepts_200_create_response_and_confirms_readback():
    target_sink = sink()
    target_sink.lookup_purchase_order = Mock(return_value=None)
    target_sink._post_purchase_order_once = Mock(
        return_value=response(200, {"insertId": 99})
    )
    target_sink._wait_for_purchase_order = Mock(return_value=existing_order())
    assert target_sink.ensure_purchase_order(mapped_order()) == (99, False)


def test_replay_reuses_identical_order_without_post():
    target_sink = sink()
    target_sink.lookup_purchase_order = Mock(return_value=existing_order())
    target_sink._post_purchase_order_once = Mock()
    assert target_sink.ensure_purchase_order(mapped_order()) == (99, True)
    target_sink._post_purchase_order_once.assert_not_called()


def test_replay_without_input_price_ignores_destination_default_price():
    target_sink = sink()
    target_sink.lookup_purchase_order = Mock(return_value=existing_order("0.00"))
    assert target_sink.ensure_purchase_order(mapped_order(price=None)) == (99, True)


def test_replay_compares_supplied_prices_per_line():
    target_sink = sink()
    expected = mapped_order()
    expected["lines"].append({"item_id": "102", "quantity": 1})
    actual = existing_order("999.00")
    actual["lines"].append(
        {
            "id": 1002,
            "item": {"id": 102},
            "quantity_ordered": "1",
            "purchase_price": "0.00",
        }
    )
    target_sink.lookup_purchase_order = Mock(return_value=actual)
    with pytest.raises(FatalAPIError, match="lines differ"):
        target_sink.ensure_purchase_order(expected)

    actual["lines"][0]["purchase_price"] = "3.50"
    assert target_sink.ensure_purchase_order(expected) == (99, True)


def test_replay_accepts_documented_price_rcy_fallback():
    target_sink = sink()
    actual = existing_order()
    actual["lines"][0]["price_rcy"] = actual["lines"][0].pop("purchase_price")
    target_sink.lookup_purchase_order = Mock(return_value=actual)
    assert target_sink.ensure_purchase_order(mapped_order()) == (99, True)


def test_replay_rejects_changed_existing_order():
    target_sink = sink()
    changed = existing_order()
    changed["lines"][0]["quantity_ordered"] = "3"
    target_sink.lookup_purchase_order = Mock(return_value=changed)
    with pytest.raises(FatalAPIError, match="lines differ; updates are unsupported"):
        target_sink.ensure_purchase_order(mapped_order())


def test_timeout_reconciles_an_accepted_write():
    target_sink = sink()
    target_sink.lookup_purchase_order = Mock(return_value=None)
    target_sink._post_purchase_order_once = Mock(return_value=None)
    target_sink._wait_for_purchase_order = Mock(return_value=existing_order())
    assert target_sink.ensure_purchase_order(mapped_order()) == (99, True)


def test_timeout_without_readback_fails_ambiguously_without_retry():
    target_sink = sink()
    target_sink.lookup_purchase_order = Mock(return_value=None)
    target_sink._post_purchase_order_once = Mock(return_value=None)
    target_sink._wait_for_purchase_order = Mock(return_value=None)
    with pytest.raises(AmbiguousWriteError, match="outcome is ambiguous"):
        target_sink.ensure_purchase_order(mapped_order())
    assert target_sink._post_purchase_order_once.call_count == 1


def test_successful_post_with_mismatched_readback_is_quarantined():
    target_sink = sink()
    changed = existing_order()
    changed["lines"][0]["quantity_ordered"] = "999"
    target_sink.lookup_purchase_order = Mock(return_value=None)
    target_sink._post_purchase_order_once = Mock(
        return_value=response(201, {"insertId": 99})
    )
    target_sink._wait_for_purchase_order = Mock(return_value=changed)

    target_sink.process_record(input_order(), {})
    target_sink.process_record(input_order(), {})

    assert target_sink._post_purchase_order_once.call_count == 1
    state = target_sink.latest_state
    assert state is not None
    assert state["bookmarks"]["BuyOrders"][-1]["success"] is False
    assert (
        state["bookmarks"]["BuyOrders"][-1]["ambiguous_reference"]
        == "OPTIPLY-123"
    )


def test_transient_create_response_is_reconciled_not_reposted():
    target_sink = sink()
    setattr(target_sink._target, "_itsperfect_token", "token")
    setattr(target_sink._target, "_itsperfect_token_expires_at", float("inf"))
    target_sink._session.post = Mock(return_value=response(503, {"error": "x"}))
    assert target_sink._post_purchase_order_once(mapped_order()) is None
    assert target_sink._session.post.call_count == 1


def test_create_permanent_error_is_redacted_and_not_retried():
    target_sink = sink()
    setattr(target_sink._target, "_itsperfect_token", "token")
    setattr(target_sink._target, "_itsperfect_token_expires_at", float("inf"))
    target_sink._session.post = Mock(
        return_value=response(400, text="private customer payload secret")
    )
    with pytest.raises(FatalAPIError) as exc:
        target_sink._post_purchase_order_once(mapped_order())
    assert "private customer" not in str(exc.value)
    assert target_sink._session.post.call_count == 1


def test_sink_state_records_success_only_after_confirmed_upsert():
    target_sink = sink()
    target_sink.ensure_purchase_order = Mock(return_value=(99, False))
    target_sink.process_record(input_order(), {})
    state = target_sink.latest_state
    assert state is not None
    assert state["bookmarks"]["BuyOrders"][0]["success"] is True
    assert state["bookmarks"]["BuyOrders"][0]["id"] == 99


def test_sink_state_marks_unconfirmed_write_as_failed():
    target_sink = sink()
    target_sink.ensure_purchase_order = Mock(
        side_effect=AmbiguousWriteError("ambiguous")
    )
    target_sink.process_record(input_order(), {})
    state = target_sink.latest_state
    assert state is not None
    failed = state["bookmarks"]["BuyOrders"][0]
    assert failed["success"] is False
    assert failed["ambiguous_reference"] == "OPTIPLY-123"
    assert state["summary"]["BuyOrders"]["fail"] == 1


def test_same_run_ambiguous_replay_is_lookup_only():
    target_sink = sink()
    target_sink.ensure_purchase_order = Mock(
        side_effect=AmbiguousWriteError("ambiguous")
    )
    target_sink._reconcile_ambiguous = Mock(return_value=(99, True))
    target_sink.process_record(input_order(), {})
    target_sink.process_record(input_order(), {})
    assert target_sink.ensure_purchase_order.call_count == 1
    target_sink._reconcile_ambiguous.assert_called_once()
    state = target_sink.latest_state
    assert state is not None
    assert state["bookmarks"]["BuyOrders"][-1]["success"] is True


def test_restored_ambiguous_state_replay_is_lookup_only():
    first_sink = sink()
    first_sink.ensure_purchase_order = Mock(
        side_effect=AmbiguousWriteError("ambiguous")
    )
    first_sink.process_record(input_order(), {})
    previous_state = copy.deepcopy(first_sink.latest_state)

    target = TargetItsPerfect(config=config())
    target._latest_state = previous_state
    restored = BuyOrdersSink(target, "BuyOrders", SCHEMA, ["id"])
    restored.ensure_purchase_order = Mock()
    restored._reconcile_ambiguous = Mock(return_value=(99, True))
    restored.process_record(input_order(), {})
    restored.ensure_purchase_order.assert_not_called()
    restored._reconcile_ambiguous.assert_called_once()
    state = restored.latest_state
    assert state is not None
    assert state["bookmarks"]["BuyOrders"][-1]["success"] is True


def test_singer_state_is_emitted_after_record_processing():
    target = TargetItsPerfect(config=config())
    order = input_order()
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "customer_id": {"type": "integer"},
            "transaction_date": {"type": "string"},
            "created_at": {"type": "string"},
            "line_items": {"type": "array"},
        },
    }
    singer_input = "\n".join(
        json.dumps(message, default=str)
        for message in (
            {
                "type": "SCHEMA",
                "stream": "BuyOrders",
                "schema": schema,
                "key_properties": ["id"],
            },
            {"type": "RECORD", "stream": "BuyOrders", "record": order},
            {"type": "STATE", "value": {"bookmark": 1}},
        )
    )
    with patch.object(BuyOrdersSink, "ensure_purchase_order", return_value=(99, False)):
        output = StringIO()
        with patch("sys.stdout", output):
            target.listen(StringIO(singer_input))
    messages = [json.loads(line) for line in output.getvalue().splitlines() if line]
    completed_state = messages[-1]
    assert completed_state["bookmarks"]["BuyOrders"][0]["success"] is True
    assert completed_state["bookmarks"]["BuyOrders"][0]["id"] == 99
