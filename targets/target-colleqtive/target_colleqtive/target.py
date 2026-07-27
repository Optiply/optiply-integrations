"""Colleqtive target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_colleqtive.sinks import BuyOrders


class TargetColleqtive(TargetHotglue):
    """Singer target for exporting Optiply buyOrders to Colleqtive."""

    name = "target-colleqtive"
    SINK_TYPES = [BuyOrders]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            default="https://bbq-test.colleqtive.net",
            description="Base URL for the Colleqtive public API",
        ),
        th.Property(
            "token_url",
            th.StringType,
            required=False,
            default=(
                "https://login.microsoftonline.com/"
                "ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token"
            ),
            description="OAuth2 client credentials token endpoint",
        ),
        th.Property("client_id", th.StringType, required=False),
        th.Property("client_secret", th.StringType, required=False),
        th.Property("scope", th.StringType, required=False),
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="Optional pre-fetched bearer token. Prefer client credentials in HotGlue.",
        ),
        th.Property(
            "store_number",
            th.StringType,
            required=False,
            description="Default Colleqtive store_number for exported delivery orders",
        ),
        th.Property(
            "default_supplier_name",
            th.StringType,
            required=False,
            description="Fallback supplier_name when the buyOrder record has no supplier name",
        ),
        th.Property(
            "default_no_of_containers",
            th.IntegerType,
            required=False,
            default=1,
            description="Fallback no_of_containers for Colleqtive delivery orders",
        ),
        th.Property(
            "default_order_type",
            th.IntegerType,
            required=False,
            description="Optional default order_type for Colleqtive delivery orders",
        ),
        th.Property(
            "default_target_stock_pool",
            th.IntegerType,
            required=False,
            description="Optional fallback target_stock_pool for order lines",
        ),
        th.Property(
            "container_override",
            th.BooleanType,
            required=False,
            default=False,
            description="Send the Colleqtive container_override header on delivery POSTs",
        ),
        th.Property(
            "request_timeout_seconds",
            th.NumberType,
            required=False,
            default=120,
        ),
    ).to_dict()


if __name__ == "__main__":
    TargetColleqtive.cli()
