"""Colleqtive target sink classes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from singer_sdk.exceptions import FatalAPIError

from target_colleqtive.client import ColleqtiveSink


def _first_present(*values: Any) -> Any:
    """Return the first value that is not None or empty string."""
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _as_string(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    return str(value)


def _as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    return int(value)


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)


def _normalize_datetime(value: Any) -> Optional[str]:
    """Normalize datetimes to API-friendly UTC ISO-8601 strings."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return str(value)

    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat(timespec="seconds") + "Z"


def _loads_items(value: Any) -> list:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, list):
        return value
    return [value]


class BuyOrders(ColleqtiveSink):
    """POST Optiply buyOrders to Colleqtive as delivery orders."""

    endpoint = "/api/v2/public/orders/deliveries"
    name = "BuyOrders"

    def _get_line_items(self, record: dict) -> list:
        return _loads_items(
            _first_present(
                record.get("line_items"),
                record.get("lineItems"),
                record.get("order_lines"),
                record.get("orderLines"),
                record.get("lines"),
            )
        )

    def _build_order_line(self, line_item: dict, line_number: int) -> Optional[dict]:
        quantity = _as_float(
            _first_present(
                line_item.get("quantity"),
                line_item.get("amount"),
                line_item.get("qty"),
            )
        )
        if quantity is None:
            self.logger.warning("Skipping Colleqtive delivery line with no quantity: %s", line_item)
            return None

        line: Dict[str, Any] = {"quantity": quantity}

        product_number = _as_string(
            _first_present(
                line_item.get("product_number"),
                line_item.get("productNumber"),
                line_item.get("product_remoteId"),
                line_item.get("productRemoteId"),
                line_item.get("productId"),
                line_item.get("product_id"),
                line_item.get("skuCode"),
                line_item.get("sku"),
                line_item.get("articleCode"),
                line_item.get("article_code"),
            )
        )
        if product_number:
            line["product_number"] = product_number

        optional_string_fields = {
            "barcode": ("barcode", "ean", "eanCode", "ean_code"),
            "description": ("description", "name"),
            "container_barcode": ("container_barcode", "containerBarcode"),
            "secondary_order_number": ("secondary_order_number", "secondaryOrderNumber"),
            "customer_order_number": ("customer_order_number", "customerOrderNumber"),
            "customer_order_shipment_number": (
                "customer_order_shipment_number",
                "customerOrderShipmentNumber",
            ),
        }
        for target_key, source_keys in optional_string_fields.items():
            value = _as_string(_first_present(*(line_item.get(key) for key in source_keys)))
            if value:
                line[target_key] = value

        optional_int_fields = {
            "line_number": ("line_number", "lineNumber"),
            "target_stock_pool": ("target_stock_pool", "targetStockPool"),
            "secondary_order_line_number": (
                "secondary_order_line_number",
                "secondaryOrderLineNumber",
            ),
            "customer_order_line_number": (
                "customer_order_line_number",
                "customerOrderLineNumber",
            ),
            "customer_order_shipment_number_line_number": (
                "customer_order_shipment_number_line_number",
                "customerOrderShipmentNumberLineNumber",
            ),
        }
        for target_key, source_keys in optional_int_fields.items():
            value = _as_int(_first_present(*(line_item.get(key) for key in source_keys)))
            if value is not None:
                line[target_key] = value

        if "line_number" not in line:
            line["line_number"] = line_number

        if "target_stock_pool" not in line:
            default_stock_pool = _as_int(self.config.get("default_target_stock_pool"))
            if default_stock_pool is not None:
                line["target_stock_pool"] = default_stock_pool

        return line

    def _build_order_lines(self, line_items: Iterable[dict]) -> list:
        lines = []
        for index, line_item in enumerate(line_items, start=1):
            if not isinstance(line_item, dict):
                self.logger.warning("Skipping non-object Colleqtive delivery line: %s", line_item)
                continue
            line = self._build_order_line(line_item, index)
            if line:
                lines.append(line)
        return lines

    def _build_order(self, record: dict) -> Optional[dict]:
        store_number = _as_string(
            _first_present(
                record.get("store_number"),
                record.get("storeNumber"),
                self.config.get("store_number"),
            )
        )
        order_number = _as_string(
            _first_present(
                record.get("order_number"),
                record.get("orderNumber"),
                record.get("remoteId"),
                record.get("remote_id"),
                record.get("reference"),
                record.get("id"),
            )
        )
        supplier_name = _as_string(
            _first_present(
                record.get("supplier_name"),
                record.get("supplierName"),
                record.get("supplier"),
                record.get("supplierName_remote"),
                self.config.get("default_supplier_name"),
            )
        )
        no_of_containers = _as_int(
            _first_present(
                record.get("no_of_containers"),
                record.get("noOfContainers"),
                self.config.get("default_no_of_containers"),
                1,
            )
        )

        missing = []
        if not store_number:
            missing.append("store_number")
        if not order_number:
            missing.append("order_number")
        if not supplier_name:
            missing.append("supplier_name")
        if no_of_containers is None:
            missing.append("no_of_containers")
        if missing:
            self.logger.warning(
                "Skipping Colleqtive delivery order %s; missing required fields: %s",
                record.get("id") or record.get("remoteId"),
                ", ".join(missing),
            )
            return None

        order = {
            "store_number": store_number,
            "order_number": order_number,
            "supplier_name": supplier_name,
            "no_of_containers": no_of_containers,
        }

        order_lines = self._build_order_lines(self._get_line_items(record))
        if order_lines:
            order["order_lines"] = order_lines
        else:
            self.logger.warning(
                "Skipping Colleqtive delivery order %s; no valid order_lines",
                order_number,
            )
            return None

        optional_string_fields = {
            "tracing_url": ("tracing_url", "tracingUrl"),
            "supplier_code": ("supplier_code", "supplierCode", "supplier_remoteId"),
            "description": ("description", "remarks", "note"),
        }
        for target_key, source_keys in optional_string_fields.items():
            value = _as_string(_first_present(*(record.get(key) for key in source_keys)))
            if value:
                order[target_key] = value

        optional_int_fields = {
            "order_type": ("order_type", "orderType"),
            "reason_code": ("reason_code", "reasonCode"),
        }
        for target_key, source_keys in optional_int_fields.items():
            value = _as_int(_first_present(*(record.get(key) for key in source_keys)))
            if value is not None:
                order[target_key] = value

        if "order_type" not in order:
            default_order_type = _as_int(self.config.get("default_order_type"))
            if default_order_type is not None:
                order["order_type"] = default_order_type

        datetime_created = _normalize_datetime(
            _first_present(
                record.get("datetime_created"),
                record.get("datetimeCreated"),
                record.get("created_at"),
                record.get("createdAt"),
                record.get("placed"),
            )
        )
        if datetime_created:
            order["datetime_created"] = datetime_created

        datetime_expected = _normalize_datetime(
            _first_present(
                record.get("datetime_expected"),
                record.get("datetimeExpected"),
                record.get("expected_delivery_date"),
                record.get("expectedDeliveryDate"),
                record.get("deliveryDate"),
            )
        )
        if datetime_expected:
            order["datetime_expected"] = datetime_expected

        return order

    def preprocess_record(self, record: dict, context: dict) -> Optional[dict]:
        """Build the Colleqtive deliveries POST body."""
        if not record:
            return None
        if "orders" in record:
            return record

        order = self._build_order(record)
        if not order:
            return None
        return {"orders": [order]}

    def upsert_record(self, record: dict, context: dict):
        """POST a delivery order payload to Colleqtive."""
        state_updates = {}
        if not record:
            return None, False, state_updates

        headers = {"container_override": str(bool(self.config.get("container_override", False))).lower()}

        try:
            response = self.request_api(
                "POST",
                endpoint=self.endpoint,
                request_data=record,
                headers=headers,
            )
        except FatalAPIError as exc:
            state_updates["error"] = str(exc)
            return None, False, state_updates

        response_id = None
        if response.status_code in (200, 201, 202, 204):
            try:
                response_payload = response.json()
            except (json.JSONDecodeError, ValueError):
                response_payload = {}
            if isinstance(response_payload, dict):
                response_id = _first_present(
                    response_payload.get("id"),
                    response_payload.get("order_number"),
                    response_payload.get("orderNumber"),
                )

        if not response_id:
            orders = record.get("orders") or []
            if orders:
                response_id = orders[0].get("order_number")

        return response_id, True, state_updates
