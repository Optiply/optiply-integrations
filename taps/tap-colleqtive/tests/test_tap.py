"""Regression tests for tap-colleqtive."""

import unittest

from tap_colleqtive.streams import BuyOrdersStream, OrdersStream, ProductsStream, StocksStream
from tap_colleqtive.tap import TapColleqtive


def minimal_config():
    return {
        "api_url": "https://example.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "scope": "api://client-id/.default",
    }


class TapColleqtiveTests(unittest.TestCase):
    def test_discover_streams(self):
        tap = TapColleqtive(config=minimal_config())

        self.assertEqual(
            [stream.name for stream in tap.discover_streams()],
            ["products", "stocks", "orders", "buy_orders"],
        )

    def test_load_state_coerces_legacy_scalar_bookmarks(self):
        tap = TapColleqtive(
            config=minimal_config(),
            state={"bookmarks": {"stocks": "2026-04-01T00:00:00Z"}},
        )

        self.assertEqual(
            tap.state["bookmarks"]["stocks"],
            {
                "replication_key": "last_stock_modified_datetime",
                "replication_key_value": "2026-04-01T00:00:00Z",
            },
        )

    def test_products_unwraps_data_records(self):
        tap = TapColleqtive(config={**minimal_config(), "page_size": 200})
        stream = ProductsStream(tap=tap)

        items = stream._items_from_payload({
            "status": "success",
            "data": {"records": [{"product_number": "P1"}]},
        })

        self.assertEqual(items, [{"product_number": "P1"}])

    def test_stocks_uses_page_and_incremental_params(self):
        tap = TapColleqtive(config={
            **minimal_config(),
            "page_size": 200,
            "start_date": "2026-06-15T06:30:24.557",
        })
        stream = StocksStream(tap=tap)

        params = stream._request_params(None, 1)

        self.assertEqual(params["Page_Size"], 200)
        self.assertEqual(params["Page_Start"], 1)
        self.assertEqual(
            params["last_stock_modified_datetime"],
            "2026-06-15T06:30:24.557",
        )
        self.assertNotIn("last_modified_date", params)
        self.assertNotIn("reason_code", params)
        self.assertEqual(stream.path, "/api/v2/public/storeproducts/stock")

    def test_orders_adds_reason_code_filter_with_end_date(self):
        tap = TapColleqtive(config={
            **minimal_config(),
            "page_size": 200,
            "end_date": "2026-06-16T00:00:00Z",
        })
        stream = OrdersStream(tap=tap)

        params = stream._request_params(None, 3)

        self.assertEqual(params["Page_Size"], 200)
        self.assertEqual(params["Page_Start"], 3)
        self.assertEqual(params["to_modified_date"], "2026-06-16T00:00:00Z")
        self.assertEqual(params["reason_code"], 100)
        self.assertEqual(
            stream.path,
            "/api/v2/public/products/stock/storeproductlogs",
        )

    def test_orders_does_not_add_reason_code_without_end_date(self):
        tap = TapColleqtive(config={**minimal_config(), "page_size": 200})
        stream = OrdersStream(tap=tap)

        params = stream._request_params(None, 3)

        self.assertNotIn("reason_code", params)
        self.assertNotIn("to_modified_date", params)

    def test_stocks_uses_actual_stock_response_schema_and_bookmark_alias(self):
        tap = TapColleqtive(config=minimal_config())
        stream = StocksStream(tap=tap)

        record = stream._normalize_record({
            "id": 6191,
            "store_number": "3101",
            "product_number": "4250435790540",
            "category_code": "wc-cat-170",
            "category_name": "The Bastard accessoires",
            "product_description": "Gietijzeren steelpan met deksel Petromax - 2 Liter",
            "most_likely_quantity": 0.0,
            "most_likely_datetime": "2026-06-15T06:30:37.8",
            "most_likely_pool_1": 0.0,
            "forecast_array": [{"period": 1, "quantity": 2}],
            "barcode": "4250435790540",
            "is_stock": True,
            "is_replenishment": False,
        })

        self.assertEqual(record["id"], 6191)
        self.assertEqual(record["category_name"], "The Bastard accessoires")
        self.assertEqual(record["most_likely_quantity"], 0.0)
        self.assertEqual(record["most_likely_pool_1"], 0.0)
        self.assertEqual(record["is_stock"], True)
        self.assertEqual(record["is_replenishment"], False)
        self.assertEqual(
            record["last_stock_modified_datetime"],
            "2026-06-15T06:30:37.8",
        )
        self.assertEqual(record["forecast_array"], "[{\"period\": 1, \"quantity\": 2}]")
        self.assertNotIn("stock", record)
        self.assertNotIn("quantity", record)

    def test_orders_stringifies_counting_lines_remaining_stockpool(self):
        tap = TapColleqtive(config=minimal_config())
        stream = OrdersStream(tap=tap)

        record = stream._normalize_record({
            "id": 1,
            "updated_on": "2026-04-01T12:00:00Z",
            "counting_lines_remaining_stockpool": {"pool": 1},
        })

        self.assertEqual(record["last_modified_date"], "2026-04-01T12:00:00Z")
        self.assertEqual(record["counting_lines_remaining_stockpool"], "{\"pool\": 1}")

    def test_buy_orders_stringifies_order_lines(self):
        tap = TapColleqtive(config=minimal_config())
        stream = BuyOrdersStream(tap=tap)

        record = stream._normalize_record({
            "order_number": "BO1",
            "store_number": "S1",
            "updated_on": "2026-04-01T12:00:00Z",
            "order_lines": [{"product_number": "P1"}],
        })

        self.assertEqual(record["last_modified_date"], "2026-04-01T12:00:00Z")
        self.assertEqual(record["order_lines"], "[{\"product_number\": \"P1\"}]")


if __name__ == "__main__":
    unittest.main()
