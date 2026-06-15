"""Stream definitions for tap-colleqtive."""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Optional

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream

from tap_colleqtive.client import ColleqtiveClient
from tap_colleqtive.schemas import (
    BUY_ORDER_FIELDS,
    PRODUCT_FIELDS,
    STOCK_FIELDS,
    STOCK_LOG_FIELDS,
    _field_schema,
)

logger = logging.getLogger(__name__)


def _json_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


class ColleqtiveStream(Stream):
    """Base class for Colleqtive public API streams."""

    path = ""
    primary_keys: list[str] = []
    replication_key: Optional[str] = None
    replication_method = "FULL_TABLE"
    page_start_param = "Page_Start"
    page_size_param = "Page_Size"
    records_key = "list"
    schema_fields: dict[str, th.JSONTypeHelper] = {}
    json_string_fields: set[str] = set()
    _client: Optional[ColleqtiveClient] = None

    @property
    def client(self) -> ColleqtiveClient:
        if self._client is None:
            self._client = ColleqtiveClient(self.config, logger_=self.logger)
        return self._client

    @property
    def page_size(self) -> int:
        raw_value = self.config.get("page_size", 200)
        try:
            page_size = int(raw_value)
        except (TypeError, ValueError):
            logger.warning("Invalid page_size=%r. Falling back to 200.", raw_value)
            page_size = 200
        return max(page_size, 1)

    def _request(self, path: str, params: Optional[dict] = None):
        return self.client.request(path, params=params)

    def _page_params(self, page_start: int) -> dict[str, Any]:
        return {
            self.page_size_param: self.page_size,
            self.page_start_param: page_start,
        }

    def _incremental_filter(self, context: Optional[dict]) -> Optional[str]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            return str(start_replication)
        if self.config.get("start_date"):
            return str(self.config["start_date"])
        return None

    def _base_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        params = self._page_params(page_start)

        if self.replication_key:
            replication_value = self._incremental_filter(context)
            if replication_value:
                params[self.replication_key] = replication_value

        store_number = self.config.get("store_number")
        if store_number:
            params["store_number"] = store_number

        return params

    def _items_from_payload(self, payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if not isinstance(payload, dict):
            return []

        if isinstance(payload.get("data"), dict):
            return self._items_from_payload(payload["data"])
        if isinstance(payload.get("data"), list):
            return self._items_from_payload(payload["data"])

        for key in (self.records_key, "records", "list", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        return []

    def _normalize_record(self, row: dict) -> dict:
        record = {field: row.get(field) for field in self.schema_fields}
        for field in self.json_string_fields:
            record[field] = _json_string(row.get(field))

        if self.replication_key and not record.get(self.replication_key):
            record[self.replication_key] = (
                row.get("last_modified_date")
                or row.get("last_stock_modified_datetime")
                or row.get("most_likely_datetime")
                or row.get("updated_on")
                or row.get("datetime_created")
            )

        return record

    def _request_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        return self._base_params(context, page_start)

    def _write_state_message(self) -> None:
        """Clean partitions from state to avoid bloat."""
        try:
            tap_state = getattr(getattr(self, "_tap", None), "state", {}) or {}
            for stream_state in tap_state.get("bookmarks", {}).values():
                stream_state.pop("partitions", None)
            super()._write_state_message()
        except Exception as exc:
            self.logger.warning("Error writing state message: %s", exc)

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page_start = 1

        while True:
            params = self._request_params(context, page_start)
            payload = self._request(self.path, params=params).json()
            items = self._items_from_payload(payload)

            for row in items:
                yield self._normalize_record(row)

            if len(items) < self.page_size:
                break

            page_start += 1


class ProductsStream(ColleqtiveStream):
    """Colleqtive products.

    FULL_TABLE because this endpoint does not expose last_modified_date in the
    request or response shape returned by the v2 public API.
    """

    name = "products"
    path = "/api/v2/public/products"
    primary_keys = ["product_number"]
    replication_method = "FULL_TABLE"
    records_key = "records"
    schema_fields = PRODUCT_FIELDS
    json_string_fields = {
        "promo_stores_allowed",
        "promo_stores_not_allowed",
        "units",
        "stores_allowed",
        "stores_not_allowed",
        "countries_allowed",
        "countries_not_allowed",
        "set_product",
        "free_fields",
    }
    schema = _field_schema(schema_fields)


class StocksStream(ColleqtiveStream):
    """Colleqtive current stock."""

    name = "stocks"
    path = "/api/v2/public/storeproducts/stock"
    primary_keys = ["id"]
    replication_key = "last_stock_modified_datetime"
    replication_method = "INCREMENTAL"
    records_key = "list"
    schema_fields = STOCK_FIELDS
    json_string_fields = {
        "ordering_datetimes",
        "delivery_datetimes",
        "forecast_array",
        "history_array",
        "average_array",
    }
    schema = _field_schema(schema_fields)


class OrdersStream(ColleqtiveStream):
    """Colleqtive order changes."""

    name = "orders"
    path = "/api/v2/public/products/stock/storeproductlogs"
    primary_keys = ["id"]
    replication_key = "last_modified_date"
    replication_method = "INCREMENTAL"
    records_key = "list"
    schema_fields = STOCK_LOG_FIELDS
    json_string_fields = {"counting_lines_remaining_stockpool"}
    schema = _field_schema(schema_fields)

    def _request_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        params = super()._request_params(context, page_start)
        if self.config.get("end_date"):
            params["to_modified_date"] = str(self.config["end_date"])
            params["reason_code"] = 100
        return params


class BuyOrdersStream(ColleqtiveStream):
    """Colleqtive delivery buy orders."""

    name = "buy_orders"
    path = "/api/v2/public/orders/deliveries"
    primary_keys = ["order_number", "store_number"]
    replication_key = "last_modified_date"
    replication_method = "INCREMENTAL"
    records_key = "list"
    schema_fields = BUY_ORDER_FIELDS
    json_string_fields = {"order_lines"}
    schema = _field_schema(schema_fields)
