"""ItsPerfect target entry point."""

# pyright: reportAssignmentType=false, reportIncompatibleMethodOverride=false

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_itsperfect.sinks import BuyOrdersSink


class TargetItsPerfect(TargetHotglue):
    """Optiply BuyOrders to ItsPerfect purchase orders."""

    name = "target-itsperfect"
    SINK_TYPES = [BuyOrdersSink]
    MAX_PARALLELISM = 1
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="ItsPerfect tenant base URL.",
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
            "warehouse_id",
            th.IntegerType,
            required=True,
            description="ItsPerfect warehouse ID for exported purchase orders.",
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

    # ponytail: serial writes avoid reference races; add keyed locks if throughput matters.
    def get_sink_class(self, stream_name: str):
        if stream_name.lower() != "buyorders":
            raise ValueError(f"Unsupported stream: {stream_name}")
        return BuyOrdersSink
