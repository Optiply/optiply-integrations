"""ItsPerfect authentication, readback, and replay-safe purchase-order writes."""

from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any

import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueSink


class AmbiguousWriteError(FatalAPIError):
    """Raised when a write outcome cannot be proven safely."""


class ItsPerfectSink(HotglueSink):
    """Shared ItsPerfect v3 target client."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = requests.Session()

    @property
    def base_url(self) -> str:
        return str(self.config["api_url"]).rstrip("/") + "/api/v3"

    @property
    def request_timeout(self) -> int:
        try:
            return max(1, int(float(self.config.get("request_timeout_seconds", 60))))
        except (TypeError, ValueError):
            return 60

    def _access_token(self) -> str:
        token = getattr(self._target, "_itsperfect_token", None)
        expires_at = getattr(self._target, "_itsperfect_token_expires_at", 0.0)
        if token and time.time() < expires_at - 30:
            return str(token)
        return self._authenticate()

    def _authenticate(self) -> str:
        missing = [
            key
            for key in ("api_url", "username", "password")
            if not self.config.get(key)
        ]
        if missing:
            raise FatalAPIError(
                f"Missing required ItsPerfect config: {', '.join(missing)}"
            )

        url = f"{str(self.config['api_url']).rstrip('/')}/api/v3/authentication"
        for attempt in range(1, 5):
            try:
                response = self._session.post(
                    url,
                    json={
                        "username": self.config["username"],
                        "password": self.config["password"],
                    },
                    headers={"Accept": "application/json"},
                    timeout=self.request_timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == 4:
                    raise RetriableAPIError(
                        "ItsPerfect authentication network failure."
                    ) from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code == 401:
                raise InvalidCredentialsError(
                    "ItsPerfect authentication failed (401)."
                )
            if response.status_code == 403:
                raise FatalAPIError("ItsPerfect authentication forbidden (403).")
            if response.status_code == 429 or response.status_code in (
                500,
                502,
                503,
                504,
            ):
                if attempt == 4:
                    raise RetriableAPIError(
                        f"ItsPerfect authentication remained unavailable ({response.status_code})."
                    )
                self._sleep_before_retry(attempt, response)
                continue
            if 400 <= response.status_code < 500:
                raise FatalAPIError(
                    f"ItsPerfect authentication failed ({response.status_code})."
                )

            payload = self._json(response, "authentication")
            token = payload.get("token") if isinstance(payload, dict) else None
            if not token:
                raise InvalidCredentialsError(
                    "ItsPerfect authentication response omitted the token."
                )
            try:
                expires_in = max(60, int(payload.get("expires_in", 1800)))
            except (TypeError, ValueError):
                expires_in = 1800
            setattr(self._target, "_itsperfect_token", token)
            setattr(
                self._target,
                "_itsperfect_token_expires_at",
                time.time() + expires_in,
            )
            return str(token)

        raise RetriableAPIError("ItsPerfect authentication attempts exhausted.")

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token()}",
        }

    def _get(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        refreshed = False
        for attempt in range(1, 5):
            try:
                response = self._session.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers=self._headers(),
                    timeout=self.request_timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == 4:
                    raise RetriableAPIError(
                        f"ItsPerfect GET network failure for {path}."
                    ) from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code == 401 and not refreshed:
                setattr(self._target, "_itsperfect_token", None)
                setattr(self._target, "_itsperfect_token_expires_at", 0.0)
                self._authenticate()
                refreshed = True
                continue
            if response.status_code == 401:
                raise InvalidCredentialsError(
                    "ItsPerfect request failed authentication (401)."
                )
            if response.status_code == 403:
                raise FatalAPIError(f"ItsPerfect GET forbidden for {path} (403).")
            if response.status_code == 429 or response.status_code in (
                500,
                502,
                503,
                504,
            ):
                if attempt == 4:
                    raise RetriableAPIError(
                        f"ItsPerfect GET remained unavailable for {path} ({response.status_code})."
                    )
                self._sleep_before_retry(attempt, response)
                continue
            if response.status_code != 200:
                raise FatalAPIError(
                    f"ItsPerfect GET failed for {path} ({response.status_code})."
                )
            return response

        raise RetriableAPIError(f"ItsPerfect GET attempts exhausted for {path}.")

    def lookup_purchase_order(self, reference: str) -> dict[str, Any] | None:
        response = self._get(
            "/purchase_orders",
            params={
                "reference": reference,
                "includes": "lines",
                "limit": 2,
                "page": 1,
            },
        )
        try:
            current_page = int(response.headers["X-Pagination-Current-Page"])
            page_count = int(response.headers["X-Pagination-Page-Count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise FatalAPIError(
                "ItsPerfect purchase order lookup omitted valid pagination headers."
            ) from exc
        if current_page != 1 or page_count not in (0, 1):
            raise FatalAPIError(
                "ItsPerfect purchase order lookup was not a unique first page."
            )
        payload = self._json(response, "purchase order lookup")
        if not isinstance(payload, list):
            raise FatalAPIError(
                "ItsPerfect purchase order lookup returned a non-list response."
            )
        matches = [
            record
            for record in payload
            if isinstance(record, dict) and str(record.get("reference")) == reference
        ]
        if len(matches) > 1:
            raise FatalAPIError(
                "ItsPerfect contains duplicate purchase orders for the integration reference."
            )
        return matches[0] if matches else None

    def get_purchase_order_lines(self, remote_id: int) -> list[dict[str, Any]]:
        response = self._get(f"/purchase_orders/{remote_id}/lines")
        payload = self._json(response, "purchase order lines")
        if not isinstance(payload, list):
            raise FatalAPIError(
                "ItsPerfect purchase order lines returned a non-list response."
            )
        return [record for record in payload if isinstance(record, dict)]

    def ensure_purchase_order(
        self,
        payload: dict[str, Any],
    ) -> tuple[int, bool]:
        reference = str(payload["reference"])
        existing = self.lookup_purchase_order(reference)
        if existing:
            remote_id = self._remote_id(existing)
            self._verify_purchase_order(payload, existing, remote_id)
            return remote_id, True

        response = self._post_purchase_order_once(payload)
        if response is None:
            return self._reconcile_ambiguous(payload)

        try:
            response_payload = self._json(response, "purchase order create")
            remote_id = int(response_payload["insertId"])
        except (FatalAPIError, KeyError, TypeError, ValueError) as exc:
            return self._reconcile_ambiguous(payload, exc)

        confirmed = self._wait_for_purchase_order(reference)
        if not confirmed:
            raise AmbiguousWriteError(
                "ItsPerfect accepted the purchase order but readback did not confirm it."
            )
        confirmed_id = self._remote_id(confirmed)
        if confirmed_id != remote_id:
            raise AmbiguousWriteError(
                "ItsPerfect create and readback returned different purchase order IDs."
            )
        try:
            self._verify_purchase_order(payload, confirmed, confirmed_id)
        except FatalAPIError as exc:
            raise AmbiguousWriteError(
                "ItsPerfect accepted the purchase order but readback content differs."
            ) from exc
        return confirmed_id, False

    def _post_purchase_order_once(
        self,
        payload: dict[str, Any],
    ) -> requests.Response | None:
        refreshed = False
        while True:
            try:
                response = self._session.post(
                    f"{self.base_url}/purchase_orders",
                    json=payload,
                    headers=self._headers(),
                    timeout=self.request_timeout,
                )
            except (requests.ConnectionError, requests.Timeout):
                return None

            if response.status_code == 401 and not refreshed:
                setattr(self._target, "_itsperfect_token", None)
                setattr(self._target, "_itsperfect_token_expires_at", 0.0)
                self._authenticate()
                refreshed = True
                continue
            if response.status_code == 401:
                raise InvalidCredentialsError(
                    "ItsPerfect purchase order create failed authentication (401)."
                )
            if response.status_code == 403:
                raise FatalAPIError(
                    "ItsPerfect purchase order create forbidden (403)."
                )
            if 200 <= response.status_code < 300:
                return response
            if response.status_code == 429 or response.status_code in (
                500,
                502,
                503,
                504,
            ):
                return None
            raise FatalAPIError(
                f"ItsPerfect purchase order create failed ({response.status_code})."
            )

    def _reconcile_ambiguous(
        self,
        payload: dict[str, Any],
        cause: Exception | None = None,
    ) -> tuple[int, bool]:
        existing = self._wait_for_purchase_order(str(payload["reference"]))
        if not existing:
            raise AmbiguousWriteError(
                "ItsPerfect purchase order write outcome is ambiguous; replay was stopped."
            ) from cause
        remote_id = self._remote_id(existing)
        self._verify_purchase_order(payload, existing, remote_id)
        return remote_id, True

    def _wait_for_purchase_order(self, reference: str) -> dict[str, Any] | None:
        for attempt in range(1, 4):
            existing = self.lookup_purchase_order(reference)
            if existing:
                return existing
            if attempt < 3:
                time.sleep(0.5 * attempt)
        return None

    def _verify_purchase_order(
        self,
        expected: dict[str, Any],
        actual: dict[str, Any],
        remote_id: int,
    ) -> None:
        if str(actual.get("reference")) != str(expected["reference"]):
            raise FatalAPIError(
                "ItsPerfect purchase order reference does not match the replay."
            )
        for field in ("vendor", "warehouse"):
            actual_value = actual.get(field)
            actual_id = actual_value.get("id") if isinstance(actual_value, dict) else None
            if str(actual_id) != str(expected[field]["id"]):
                raise FatalAPIError(
                    f"Existing ItsPerfect purchase order {field} differs; updates are unsupported."
                )
        if "date" in expected and str(actual.get("date")) != str(expected["date"]):
            raise FatalAPIError(
                "Existing ItsPerfect purchase order date differs; updates are unsupported."
            )

        actual_lines = actual.get("lines")
        if not isinstance(actual_lines, list):
            actual_lines = self.get_purchase_order_lines(remote_id)
        if not self._lines_match(expected["lines"], actual_lines):
            raise FatalAPIError(
                "Existing ItsPerfect purchase order lines differ; updates are unsupported."
            )

    def _lines_match(
        self,
        expected_lines: list[dict[str, Any]],
        actual_lines: list[dict[str, Any]],
    ) -> bool:
        remaining = []
        for line in actual_lines:
            item = line.get("item")
            item_id = item.get("id") if isinstance(item, dict) else line.get("item_id")
            quantity = line.get("quantity_ordered", line.get("quantity"))
            price = self._first_present(
                line.get("purchase_price"),
                line.get("price_rcy"),
            )
            remaining.append(
                (
                    str(item_id),
                    self._decimal(quantity),
                    self._decimal(price) if price not in (None, "") else None,
                )
            )

        expected = sorted(expected_lines, key=lambda line: "price" not in line)
        for line in expected:
            item_id = str(line.get("item_id"))
            quantity = self._decimal(line.get("quantity"))
            price = (
                self._decimal(line.get("price"))
                if line.get("price") not in (None, "")
                else None
            )
            match = next(
                (
                    index
                    for index, candidate in enumerate(remaining)
                    if candidate[0] == item_id
                    and candidate[1] == quantity
                    and (price is None or candidate[2] == price)
                ),
                None,
            )
            if match is None:
                return False
            remaining.pop(match)
        return not remaining

    @staticmethod
    def _first_present(*values: Any) -> Any:
        return next((value for value in values if value not in (None, "")), None)

    @staticmethod
    def _remote_id(record: dict[str, Any]) -> int:
        try:
            return int(record["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise FatalAPIError(
                "ItsPerfect purchase order lookup omitted a valid ID."
            ) from exc

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise FatalAPIError(
                "ItsPerfect purchase order contains an invalid decimal value."
            ) from exc

    @staticmethod
    def _json(response: requests.Response, operation: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise FatalAPIError(
                f"ItsPerfect {operation} returned malformed JSON."
            ) from exc

    def _sleep_before_retry(
        self,
        attempt: int,
        response: requests.Response | None = None,
    ) -> None:
        delay = None
        if response is not None:
            raw = response.headers.get("Retry-After")
            if raw:
                try:
                    delay = float(raw)
                except (TypeError, ValueError):
                    try:
                        delay = parsedate_to_datetime(raw).timestamp() - time.time()
                    except (TypeError, ValueError, OverflowError):
                        delay = None
        if delay is None:
            delay = 2 ** (attempt - 1)
        try:
            time.sleep(max(0.0, min(float(delay), 60.0)))
        except (TypeError, ValueError, OverflowError) as exc:
            raise RetriableAPIError(
                "ItsPerfect returned an invalid retry delay."
            ) from exc
