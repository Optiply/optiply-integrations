"""ItsPerfect tap entry point."""

# pyright: reportMissingImports=false

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_itsperfect.streams import (
    ProductsStream,
    PurchaseOrderLinesStream,
    PurchaseOrdersStream,
    PutLinesStream,
    PutsStream,
    QualitiesStream,
    QualityCompositionsStream,
    SalesOrderLinesStream,
    SalesOrdersStream,
    StocksStream,
    StoresStream,
    VendorsStream,
    WarehousesStream,
)


class TapItsPerfect(Tap):
    """Read-only Singer tap for the ItsPerfect v3 API."""

    name = "tap-itsperfect"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="Tenant base URL, for example https://tenant.itsperfect.it.",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="ItsPerfect API username.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="ItsPerfect API password.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00Z",
            description="Lower bound for the products last_update_timestamp filter.",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=250,
            description="Records per page; ItsPerfect documents a maximum of 250.",
        ),
        th.Property(
            "request_timeout_seconds",
            th.NumberType,
            default=60,
            description="Per-request timeout in seconds.",
        ),
    ).to_dict()
    config_jsonschema["properties"]["username"]["secret"] = True
    config_jsonschema["properties"]["password"]["secret"] = True

    def discover_streams(self):
        return [
            ProductsStream(tap=self),
            StocksStream(tap=self),
            StoresStream(tap=self),
            WarehousesStream(tap=self),
            VendorsStream(tap=self),
            SalesOrdersStream(tap=self),
            SalesOrderLinesStream(tap=self),
            PurchaseOrdersStream(tap=self),
            PurchaseOrderLinesStream(tap=self),
            PutsStream(tap=self),
            PutLinesStream(tap=self),
            QualitiesStream(tap=self),
            QualityCompositionsStream(tap=self),
        ]
