import importlib
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class _Response:
    def __init__(self, items):
        self._items = items

    def json(self):
        return {"items": self._items}


class _FakeVenditSink:
    """Sink stub; the Vendit side starts empty unless a test fills it."""

    pre_purchase_lines = []
    open_purchase_orders = []

    def __init__(self):
        self.config = {}
        self.logger = _Logger()
        self.payloads = []
        self.http_headers = {}

    def request_api(self, method, endpoint, **kwargs):
        assert (method, endpoint) == ("GET", "PrePurchaseOrders/GetAll")
        return _Response(self.pre_purchase_lines)

    def validate_response(self, response):
        pass

    def process_record(self, record, context):
        self.payloads.append(record)


singer_sdk = types.ModuleType("hotglue_singer_sdk")
singer_sdk_exceptions = types.ModuleType("hotglue_singer_sdk.exceptions")
singer_sdk_exceptions.FatalAPIError = type("FatalAPIError", (Exception,), {})
singer_sdk.exceptions = singer_sdk_exceptions
sys.modules["hotglue_singer_sdk"] = singer_sdk
sys.modules["hotglue_singer_sdk.exceptions"] = singer_sdk_exceptions

fake_client = types.ModuleType("target_vendit.client")
fake_client.VenditSink = _FakeVenditSink
sys.modules["target_vendit.client"] = fake_client

sinks = importlib.import_module("target_vendit.sinks")


def _get_open_purchase_orders(url, headers=None):
    assert url.endswith("/Optiply/GetProductPurchaseOrdersFromDate/0")
    return _Response(_FakeVenditSink.open_purchase_orders)


sinks.requests = types.SimpleNamespace(get=_get_open_purchase_orders)


def test_buy_orders_maps_line_item_unit_price_to_vendit_purchase_price_ex():
    sink = sinks.BuyOrders()
    sink.process_record(
        {
            "id": "bo-123",
            "targetSupplierId": 456,
            "creationDatetime": "2026-05-13T10:15:00Z",
            "line_items": [
                {"productId": 27043329, "quantity": 2, "unit_price": "145.00"},
                {"productId": 27043156, "quantity": 4, "unit_price": 3.5},
            ],
        },
        {},
    )

    assert [payload["items"][0]["purchasePriceEx"] for payload in sink.payloads] == [145.0, 3.5]
    assert "onetimePurchasePrice" not in sink.payloads[0]["items"][0]
    assert "price" not in sink.payloads[0]["items"][0]
    assert sink.payloads[0]["items"][0]["targetSupplierId"] == 456


def test_buy_orders_resend_only_sends_lines_vendit_does_not_have(monkeypatch):
    # Resend of a buy order after a partial failure: one line is still on the pre-purchase
    # list, one was already ordered (open purchase order). Same product on another order is unrelated.
    monkeypatch.setattr(_FakeVenditSink, "pre_purchase_lines", [
        {"orderReference": "6199106", "optiplyId": 6199106.0, "productId": 33836},
        {"orderReference": "6200477", "optiplyId": "6200477", "productId": 52393},
    ])
    monkeypatch.setattr(_FakeVenditSink, "open_purchase_orders", [
        {"orderReference": None, "optiplyId": "6199106", "details": {"items": [{"productId": 44536}]}},
    ])

    sink = sinks.BuyOrders()
    sink.process_record(
        {
            "id": 6199106,
            "transaction_date": "2026-10-07T13:12:14.000000Z",
            "line_items": [
                {"product_remoteId": 33836, "quantity": 12, "unit_price": 17.18},
                {"product_remoteId": 44536, "quantity": 12, "unit_price": 15.49},
                {"product_remoteId": 52393, "quantity": 12, "unit_price": None},
                {"product_remoteId": 29678, "quantity": 12, "unit_price": 21.57},
            ],
        },
        {},
    )

    assert [payload["items"][0]["productId"] for payload in sink.payloads] == [52393, 29678]
    assert "purchasePriceEx" not in sink.payloads[0]["items"][0]


def test_pre_purchase_orders_maps_unit_price_to_vendit_purchase_price_ex():
    sink = sinks.PrePurchaseOrders()
    payload = sink.preprocess_record(
        {
            "id": "bol-123",
            "productId": 27043329,
            "quantity": 1,
            "unit_price": "0",
            "creationDatetime": "2026-05-13T10:15:00Z",
        },
        {},
    )

    assert payload["items"][0]["purchasePriceEx"] == 0.0
    assert "onetimePurchasePrice" not in payload["items"][0]
    assert "price" not in payload["items"][0]
