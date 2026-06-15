"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json
from pathlib import Path

import jsonschema
from singer_sdk.testing import get_standard_tap_tests

Path("config.json").write_text(json.dumps({"sync_endpoints": False}))

from tap_exact.tap import TapExact

SAMPLE_CONFIG = {
    "refresh_token": "test-refresh-token",
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "current_division": "123456",
    "use_stock_multiple_warehouses": True,
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapExact, config=SAMPLE_CONFIG)
    for test in tests:
        if test.__name__ == "_test_stream_connections":
            # Avoid live Exact OAuth calls in unit tests.
            continue
        test()


def test_hotglue_minimal_discover_config_does_not_require_warehouse_lookup():
    """HotGlue discover only passes base config, so stream construction must be offline."""
    tap = TapExact(
        config={"start_date": "2010-01-01T00:00:00Z", "sync_endpoints": True},
        validate_config=False,
    )

    stream_names = [stream.name for stream in tap.discover_streams()]

    assert "reporting_balance" in stream_names


def test_reporting_balance_stream_is_discovered_with_all_documented_fields():
    """Validate ReportingBalance discovery metadata without live Exact auth."""
    tap = TapExact(config=SAMPLE_CONFIG)
    stream = next(
        stream for stream in tap.discover_streams() if stream.name == "reporting_balance"
    )

    expected_fields = [
        "ID",
        "Amount",
        "AmountCredit",
        "AmountDebit",
        "BalanceType",
        "CostCenterCode",
        "CostCenterDescription",
        "CostUnitCode",
        "CostUnitDescription",
        "Count",
        "Division",
        "GLAccount",
        "GLAccountCode",
        "GLAccountDescription",
        "ReportingPeriod",
        "ReportingYear",
        "Status",
        "Type",
    ]

    assert stream.path == "/financial/ReportingBalance"
    assert stream.primary_keys == ["ID"]
    assert stream.replication_key is None
    assert stream.select.split(",") == expected_fields
    assert list(stream.schema["properties"].keys()) == expected_fields


def test_reporting_balance_accepts_exact_stringified_numeric_fields():
    """Exact can return ReportingBalance numeric fields as strings."""
    tap = TapExact(config=SAMPLE_CONFIG)
    stream = next(
        stream for stream in tap.discover_streams() if stream.name == "reporting_balance"
    )
    record = {
        "ID": "balance-row-1",
        "Amount": "58700",
        "AmountCredit": "0",
        "AmountDebit": "58700",
        "BalanceType": "B",
        "CostCenterCode": None,
        "CostCenterDescription": None,
        "CostUnitCode": None,
        "CostUnitDescription": None,
        "Count": "1",
        "Division": "123456",
        "GLAccount": "00000000-0000-0000-0000-000000000000",
        "GLAccountCode": "1000",
        "GLAccountDescription": "Balance account",
        "ReportingPeriod": "1",
        "ReportingYear": "2026",
        "Status": "20",
        "Type": "10",
    }

    jsonschema.validate(record, stream.schema)


# TODO: Create additional tests as appropriate for your tap.
