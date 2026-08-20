"""ItsPerfect stream definitions."""

# pyright: reportAssignmentType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

from typing import Any, ClassVar

from tap_itsperfect.client import ItsPerfectStream


def nullable(type_name: str) -> list[str]:
    return [type_name, "null"]


def object_schema(
    properties: dict[str, Any], required: list[str] | None = None
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": True,
        "properties": properties,
    }
    if required:
        schema["required"] = required
    return schema


ID = {"type": "integer"}
STRING = {"type": "string"}
NULLABLE_ID = {"type": nullable("integer")}
NULLABLE_STRING = {"type": nullable("string")}
NULLABLE_NUMBER = {"type": nullable("number")}
NULLABLE_DECIMAL = {"type": ["number", "string", "null"]}
NULLABLE_CODE = {"type": ["integer", "string", "null"]}
REFERENCE = {
    "type": nullable("object"),
    "additionalProperties": True,
    "properties": {"id": NULLABLE_ID},
}


class ProductsStream(ItsPerfectStream):
    name = "products"
    path = "/items"
    primary_keys: ClassVar[list[str]] = list(("id",))
    replication_key = "last_update_timestamp"
    query_params: ClassVar[dict[str, Any]] = dict(includes="colors,barcodes")
    schema = object_schema(
        {
            "id": ID,
            "status": NULLABLE_CODE,
            "type": NULLABLE_CODE,
            "item_number": NULLABLE_STRING,
            "item_name": NULLABLE_STRING,
            "vendor": REFERENCE,
            "brand": REFERENCE,
            "quality": REFERENCE,
            "season": REFERENCE,
            "composition": {
                "type": ["array", "string", "null"],
                "items": {},
            },
            "sales_price": NULLABLE_DECIMAL,
            "purchase_price": NULLABLE_DECIMAL,
            "colors": {"type": nullable("array"), "items": {"type": "object"}},
            "barcodes": {"type": nullable("array"), "items": {"type": "object"}},
            "last_update_timestamp": STRING,
        },
        required=["id", "last_update_timestamp"],
    )


class StocksStream(ItsPerfectStream):
    name = "stocks"
    path = "/stock"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "warehouse": REFERENCE,
            "item": REFERENCE,
            "color": REFERENCE,
            "size": REFERENCE,
            "barcode": NULLABLE_STRING,
            "physical_stock": NULLABLE_STRING,
            "available_stock": NULLABLE_STRING,
            "economical_stock": NULLABLE_STRING,
            "virtual_stock_reservation": NULLABLE_STRING,
            "availability_timestamp": NULLABLE_STRING,
        }
    )


class StoresStream(ItsPerfectStream):
    name = "stores"
    path = "/stores"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "store": NULLABLE_STRING,
            "active": NULLABLE_CODE,
            "warehouse": REFERENCE,
        }
    )


class WarehousesStream(ItsPerfectStream):
    name = "warehouses"
    path = "/warehouses"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "warehouse": NULLABLE_STRING,
            "active": NULLABLE_CODE,
        }
    )


class VendorsStream(ItsPerfectStream):
    name = "vendors"
    path = "/vendors"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "vendor": NULLABLE_STRING,
            "active": NULLABLE_CODE,
            "currency": REFERENCE,
            "language": NULLABLE_STRING,
        }
    )


class SalesOrdersStream(ItsPerfectStream):
    name = "sales_orders"
    path = "/sales_orders"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "type": NULLABLE_CODE,
            "status": NULLABLE_STRING,
            "authorized": NULLABLE_CODE,
            "date": NULLABLE_STRING,
            "delivery_date": NULLABLE_STRING,
            "warehouse": REFERENCE,
            "store": REFERENCE,
            "customer": REFERENCE,
            "reference": NULLABLE_STRING,
            "quantity": NULLABLE_NUMBER,
            "date_cancelled": NULLABLE_STRING,
            "date_shipped": NULLABLE_STRING,
            "last_update_timestamp": NULLABLE_STRING,
        }
    )

    def post_process(self, row: dict, context=None):
        status = str(row.get("status"))
        if status not in {"1", "2"}:
            return None
        row["status"] = status
        return super().post_process(row, context)

    def get_child_context(self, record: dict, context=None):
        return {"sales_order_id": record["id"]}


class SalesOrderLinesStream(ItsPerfectStream):
    name = "sales_order_lines"
    path = "/sales_orders/{sales_order_id}/lines"
    parent_stream_type = SalesOrdersStream
    primary_keys: ClassVar[list[str]] = list(("sales_order_id", "id"))
    schema = object_schema(
        {
            "sales_order_id": ID,
            "id": ID,
            "item": REFERENCE,
            "color": REFERENCE,
            "size": REFERENCE,
            "barcode": NULLABLE_STRING,
            "quantity": NULLABLE_STRING,
            "quantity_ordered": NULLABLE_STRING,
            "quantity_to_ship": NULLABLE_STRING,
            "quantity_shipped": NULLABLE_STRING,
            "quantity_returned": NULLABLE_STRING,
            "price_rcy": NULLABLE_STRING,
            "discount_percentage": NULLABLE_STRING,
            "delivery_date": NULLABLE_STRING,
        }
    )

    def post_process(self, row: dict, context=None):
        assert context is not None
        row["sales_order_id"] = context["sales_order_id"]
        return row


class PurchaseOrdersStream(ItsPerfectStream):
    name = "purchase_orders"
    path = "/purchase_orders"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "type": NULLABLE_CODE,
            "status": NULLABLE_CODE,
            "authorized": NULLABLE_CODE,
            "vendor": REFERENCE,
            "warehouse": REFERENCE,
            "date": NULLABLE_STRING,
            "eta": NULLABLE_STRING,
            "expected_receipt_date": NULLABLE_STRING,
            "reference": NULLABLE_STRING,
            "last_update_timestamp": NULLABLE_STRING,
        }
    )

    def get_child_context(self, record: dict, context=None):
        return {"purchase_order_id": record["id"]}


class PurchaseOrderLinesStream(ItsPerfectStream):
    name = "purchase_order_lines"
    path = "/purchase_orders/{purchase_order_id}/lines"
    parent_stream_type = PurchaseOrdersStream
    primary_keys: ClassVar[list[str]] = list(("purchase_order_id", "id"))
    schema = object_schema(
        {
            "purchase_order_id": ID,
            "id": ID,
            "item": REFERENCE,
            "color": REFERENCE,
            "size": REFERENCE,
            "barcode": NULLABLE_STRING,
            "quantity": NULLABLE_STRING,
            "quantity_ordered": NULLABLE_STRING,
            "quantity_to_receive": NULLABLE_STRING,
            "quantity_received": NULLABLE_STRING,
            "price_rcy": NULLABLE_STRING,
        }
    )

    def post_process(self, row: dict, context=None):
        assert context is not None
        row["purchase_order_id"] = context["purchase_order_id"]
        return row


class PutsStream(ItsPerfectStream):
    name = "puts"
    path = "/puts"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "status": NULLABLE_CODE,
            "date": NULLABLE_STRING,
            "expected_receipt_date": NULLABLE_STRING,
            "date_received": NULLABLE_STRING,
            "order_type": NULLABLE_STRING,
            "warehouse": REFERENCE,
            "quantity": NULLABLE_NUMBER,
            "reference": NULLABLE_STRING,
            "last_update_timestamp": NULLABLE_STRING,
        }
    )

    def get_child_context(self, record: dict, context=None):
        return {"put_id": record["id"]}


class PutLinesStream(ItsPerfectStream):
    name = "put_lines"
    path = "/puts/{put_id}/lines"
    parent_stream_type = PutsStream
    primary_keys: ClassVar[list[str]] = list(("put_id", "id"))
    schema = object_schema(
        {
            "put_id": ID,
            "id": ID,
            "order_id": ID,
            "item": REFERENCE,
            "color": REFERENCE,
            "size": REFERENCE,
            "barcode": NULLABLE_STRING,
            "quantity": NULLABLE_STRING,
            "quantity_received": NULLABLE_STRING,
        }
    )

    def post_process(self, row: dict, context=None):
        assert context is not None
        row["put_id"] = context["put_id"]
        return row


class QualitiesStream(ItsPerfectStream):
    name = "qualities"
    path = "/qualities"
    primary_keys: ClassVar[list[str]] = list(("id",))
    schema = object_schema(
        {
            "id": ID,
            "quality": NULLABLE_STRING,
            "active": NULLABLE_CODE,
        }
    )

    def get_child_context(self, record: dict, context=None):
        return {"quality_id": record["id"]}


class QualityCompositionsStream(ItsPerfectStream):
    name = "quality_compositions"
    path = "/qualities/{quality_id}/composition"
    parent_stream_type = QualitiesStream
    primary_keys: ClassVar[list[str]] = list(("quality_id", "id"))
    schema = object_schema(
        {
            "quality_id": ID,
            "id": ID,
            "active": NULLABLE_CODE,
            "sequence": NULLABLE_CODE,
        }
    )

    def post_process(self, row: dict, context=None):
        assert context is not None
        row["quality_id"] = context["quality_id"]
        return row
