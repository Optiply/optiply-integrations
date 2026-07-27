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


class _FakeColleqtiveSink:
    def __init__(self):
        self.config = {
            "store_number": "42",
            "default_no_of_containers": 1,
            "default_target_stock_pool": 7,
        }
        self.logger = _Logger()
        self.payloads = []

    def process_record(self, record, context):
        self.payloads.append(record)


singer_sdk = types.ModuleType("singer_sdk")
singer_sdk_exceptions = types.ModuleType("singer_sdk.exceptions")
singer_sdk_exceptions.FatalAPIError = type("FatalAPIError", (Exception,), {})
singer_sdk.exceptions = singer_sdk_exceptions
sys.modules["singer_sdk"] = singer_sdk
sys.modules["singer_sdk.exceptions"] = singer_sdk_exceptions

fake_client = types.ModuleType("target_colleqtive.client")
fake_client.ColleqtiveSink = _FakeColleqtiveSink
sys.modules["target_colleqtive.client"] = fake_client

sinks = importlib.import_module("target_colleqtive.sinks")


def test_buy_orders_maps_optiply_order_to_colleqtive_delivery_payload():
    sink = sinks.BuyOrders()
    payload = sink.preprocess_record(
        {
            "id": "bo-123",
            "supplierName": "Main Supplier",
            "remoteId": "PO-9001",
            "created_at": "2026-06-25T10:15:00+02:00",
            "expectedDeliveryDate": "2026-06-30T00:00:00Z",
            "supplier_remoteId": "SUP-1",
            "line_items": [
                {"product_remoteId": "SKU-1", "quantity": "2", "barcode": "871234"},
                {"productNumber": "SKU-2", "amount": 3.5, "lineNumber": 9},
            ],
        },
        {},
    )

    assert payload == {
        "orders": [
            {
                "store_number": "42",
                "order_number": "PO-9001",
                "supplier_name": "Main Supplier",
                "no_of_containers": 1,
                "order_lines": [
                    {
                        "quantity": 2.0,
                        "product_number": "SKU-1",
                        "barcode": "871234",
                        "line_number": 1,
                        "target_stock_pool": 7,
                    },
                    {
                        "quantity": 3.5,
                        "product_number": "SKU-2",
                        "line_number": 9,
                        "target_stock_pool": 7,
                    },
                ],
                "supplier_code": "SUP-1",
                "datetime_created": "2026-06-25T08:15:00Z",
                "datetime_expected": "2026-06-30T00:00:00Z",
            }
        ]
    }


def test_buy_orders_skips_bad_records_without_crashing_batch():
    sink = sinks.BuyOrders()

    assert sink.preprocess_record({"id": "bo-missing-lines", "supplierName": "Supplier"}, {}) is None
    assert sink.preprocess_record({"id": "bo-missing-supplier", "line_items": [{"quantity": 1}]}, {}) is None


def test_buy_orders_passes_through_native_colleqtive_payload():
    sink = sinks.BuyOrders()
    payload = {"orders": [{"store_number": "1", "order_number": "A", "supplier_name": "S", "no_of_containers": 1}]}

    assert sink.preprocess_record(payload, {}) == payload


def test_buy_orders_upsert_posts_deliveries_endpoint_with_container_override_header():
    class _Response:
        status_code = 200

        def json(self):
            return {}

    sink = sinks.BuyOrders()
    sink.config["container_override"] = True
    calls = []

    def fake_request_api(method, endpoint, request_data=None, headers=None):
        calls.append(
            {
                "method": method,
                "endpoint": endpoint,
                "request_data": request_data,
                "headers": headers,
            }
        )
        return _Response()

    sink.request_api = fake_request_api
    response_id, success, state = sink.upsert_record(
        {"orders": [{"order_number": "PO-9001"}]},
        {},
    )

    assert success is True
    assert state == {}
    assert response_id == "PO-9001"
    assert calls == [
        {
            "method": "POST",
            "endpoint": "/api/v2/public/orders/deliveries",
            "request_data": {"orders": [{"order_number": "PO-9001"}]},
            "headers": {"container_override": "true"},
        }
    ]
