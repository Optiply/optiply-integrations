"""ItsPerfect BuyOrders sink."""

# pyright: reportAssignmentType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from hotglue_etl_exceptions import InvalidPayloadError

from target_itsperfect.client import AmbiguousWriteError, ItsPerfectSink


class BuyOrdersSink(ItsPerfectSink):
    """Create replay-safe ItsPerfect purchase orders with lines."""

    name = "BuyOrders"
    endpoint = "/purchase_orders"

    def process_record(self, record: dict, context: dict) -> None:
        """Persist ambiguous references and make their retries lookup-only."""
        if not self.latest_state:
            self.init_state()

        try:
            mapped = self.preprocess_record(record, context)
        except Exception as error:
            self.update_state(
                self._build_record_error_state(error, record=record),
            )
            return

        state = self.latest_state
        if state is None:
            raise RuntimeError("ItsPerfect target state failed to initialize.")
        record_hash = self.build_record_hash(mapped)
        states = state["bookmarks"][self.name]
        successful = next(
            (
                state
                for state in states
                if state.get("hash") == record_hash and state.get("success")
            ),
            None,
        )
        if successful:
            self.update_state(successful, is_duplicate=True)
            return

        reference = str(mapped["reference"])
        ambiguous = next(
            (
                state
                for state in states
                if not state.get("success")
                and state.get("ambiguous_reference") == reference
            ),
            None,
        )
        try:
            if ambiguous:
                remote_id, existing = self._reconcile_ambiguous(mapped)
            else:
                remote_id, success, updates = self.upsert_record(mapped, context)
                if not success:
                    raise AmbiguousWriteError(
                        "ItsPerfect purchase order was not durably confirmed."
                    )
                existing = bool(updates.get("existing"))
        except Exception as error:
            ambiguous_failure = ambiguous is not None
            if isinstance(error, AmbiguousWriteError):
                ambiguous_failure = True
            identifiers = (
                {"ambiguous_reference": reference}
                if ambiguous_failure
                else None
            )
            self.update_state(
                self._build_record_error_state(
                    error,
                    record_hash=record_hash,
                    identifiers=identifiers,
                )
            )
            return

        self.update_state(
            {"success": True, "hash": record_hash, "id": remote_id},
            is_duplicate=existing,
        )

    def preprocess_record(self, record: dict, context: dict) -> dict:
        order_id = self._required(record, "id", "optiplyId", "optiply_id")
        supplier_id = self._positive_int(
            self._required(
                record,
                "targetSupplierId",
                "supplier_remoteId",
                "customer_id",
            ),
            "supplier remote ID",
        )
        warehouse_id = self._positive_int(
            self.config.get("warehouse_id"),
            "configured warehouse ID",
        )
        order_date = self._date(
            self._required(record, "transaction_date", "order_date", "date"),
            "order date",
        )
        reference = f"OPTIPLY-{order_id}"

        payload: dict[str, Any] = {
            "vendor": {"id": supplier_id},
            "warehouse": {"id": warehouse_id},
            "reference": reference,
            "date": order_date,
            "lines": self._lines(record.get("line_items")),
        }
        return payload

    def upsert_record(self, record: dict, context: dict):
        remote_id, existing = self.ensure_purchase_order(record)
        return remote_id, True, {"existing": existing}

    def _lines(self, value: Any) -> list[dict[str, Any]]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise InvalidPayloadError("BuyOrders line_items is not valid JSON.") from exc
        if not isinstance(value, list) or not value:
            raise InvalidPayloadError("BuyOrders requires at least one line item.")

        lines = []
        for line in value:
            if not isinstance(line, dict):
                raise InvalidPayloadError("BuyOrders line item must be an object.")
            item_id = self._required(line, "product_remoteId", "product_id", "item_id")
            quantity = self._positive_decimal(
                self._required(line, "quantity", "amount", "qty"),
                "line quantity",
            )
            mapped: dict[str, Any] = {
                "item_id": str(item_id),
                "quantity": self._json_number(quantity),
            }
            price = self._first_present(
                line.get("purchase_price"),
                line.get("unit_price"),
                line.get("price"),
            )
            if price is not None:
                mapped["price"] = self._decimal_string(price, "line purchase price")
            lines.append(mapped)
        return lines

    @staticmethod
    def _first_present(*values: Any) -> Any:
        return next((value for value in values if value not in (None, "")), None)

    @classmethod
    def _required(cls, record: dict, *keys: str) -> Any:
        value = cls._first_present(*(record.get(key) for key in keys))
        if value is None:
            raise InvalidPayloadError(f"BuyOrders requires one of: {', '.join(keys)}.")
        return value

    @staticmethod
    def _positive_int(value: Any, label: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise InvalidPayloadError(f"BuyOrders {label} must be an integer.") from exc
        if parsed <= 0:
            raise InvalidPayloadError(f"BuyOrders {label} must be positive.")
        return parsed

    @staticmethod
    def _positive_decimal(value: Any, label: str) -> Decimal:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise InvalidPayloadError(f"BuyOrders {label} must be numeric.") from exc
        if not parsed.is_finite() or parsed <= 0:
            raise InvalidPayloadError(f"BuyOrders {label} must be positive.")
        return parsed

    @staticmethod
    def _json_number(value: Decimal) -> int | float:
        try:
            return int(value) if value == value.to_integral() else float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise InvalidPayloadError(
                "BuyOrders line quantity is outside the supported numeric range."
            ) from exc

    @classmethod
    def _decimal_string(cls, value: Any, label: str) -> str:
        parsed = cls._positive_decimal(value, label)
        return format(parsed, "f")

    @staticmethod
    def _date(value: Any, label: str) -> str:
        if isinstance(value, datetime):
            parsed = value
            if parsed.tzinfo:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if not isinstance(value, str) or not value.strip():
            raise InvalidPayloadError(f"BuyOrders {label} must be an ISO date.")
        normalized = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).date().isoformat()
        except ValueError:
            try:
                return date.fromisoformat(normalized).isoformat()
            except ValueError as exc:
                raise InvalidPayloadError(
                    f"BuyOrders {label} must be an ISO date."
                ) from exc
