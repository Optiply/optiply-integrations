"""Shared ItsPerfect authentication, retries, and page-number pagination."""

from __future__ import annotations

import time
from email.utils import parsedate_to_datetime
from typing import Any, ClassVar

import backoff  # pyright: ignore[reportMissingImports]
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream


class ItsPerfectStream(RESTStream):
    """Base stream for the read-only ItsPerfect v3 API."""

    records_jsonpath = "$[*]"
    query_params: ClassVar[dict[str, Any]] = dict()

    @property
    def url_base(self) -> str:
        return str(self.config["api_url"]).rstrip("/") + "/api/v3"

    @property
    def authenticator(self):
        return None

    @property
    def http_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._access_token()}",
        }

    @property
    def timeout(self) -> int:
        try:
            return max(1, int(float(self.config.get("request_timeout_seconds", 60))))
        except (TypeError, ValueError):
            return 60

    def request_decorator(self, func):
        return backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
            max_tries=5,
            factor=1,
            jitter=backoff.full_jitter,
        )(func)

    def _request(self, prepared_request, context):
        """Refresh one expired resource token, then let a second 401 fail."""
        try:
            return super()._request(prepared_request, context)
        except InvalidCredentialsError:
            setattr(self._tap, "_itsperfect_token", None)
            setattr(self._tap, "_itsperfect_token_expires_at", 0.0)
            prepared_request.headers["Authorization"] = (
                f"Bearer {self._authenticate()}"
            )
            return super()._request(prepared_request, context)

    def _access_token(self) -> str:
        token = getattr(self._tap, "_itsperfect_token", None)
        expires_at = getattr(self._tap, "_itsperfect_token_expires_at", 0.0)
        if token and time.time() < expires_at - 30:
            return str(token)
        return self._authenticate()

    @backoff.on_exception(
        backoff.expo,
        (
            RetriableAPIError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ),
        max_tries=5,
        factor=1,
        jitter=backoff.full_jitter,
    )
    def _authenticate(self) -> str:
        missing = [key for key in ("api_url", "username", "password") if not self.config.get(key)]
        if missing:
            raise FatalAPIError(f"Missing required ItsPerfect config: {', '.join(missing)}")

        response = requests.post(
            f"{str(self.config['api_url']).rstrip('/')}/api/v3/authentication",
            json={
                "username": self.config["username"],
                "password": self.config["password"],
            },
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )

        if response.status_code == 401:
            raise InvalidCredentialsError("ItsPerfect authentication failed (401).")
        if response.status_code == 403:
            raise FatalAPIError("ItsPerfect authentication forbidden (403).")
        if response.status_code == 429 or response.status_code >= 500:
            self._sleep_for_retry_after(response)
            raise RetriableAPIError(
                f"ItsPerfect authentication temporarily failed ({response.status_code})."
            )
        if 400 <= response.status_code < 500:
            raise FatalAPIError(
                f"ItsPerfect authentication failed ({response.status_code})."
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise FatalAPIError("ItsPerfect authentication returned malformed JSON.") from exc

        token = payload.get("token") if isinstance(payload, dict) else None
        if not token:
            raise InvalidCredentialsError(
                "ItsPerfect authentication response did not contain a token."
            )

        try:
            expires_in = max(60, int(payload.get("expires_in", 1800)))
        except (TypeError, ValueError):
            expires_in = 1800
        setattr(self._tap, "_itsperfect_token", token)
        setattr(self._tap, "_itsperfect_token_expires_at", time.time() + expires_in)
        return str(token)

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 401:
            raise InvalidCredentialsError("ItsPerfect request failed authentication (401).")
        if response.status_code == 403:
            raise FatalAPIError(f"ItsPerfect forbids access to {self.path} (403).")
        if response.status_code == 204:
            return
        if response.status_code == 404:
            if self.parent_stream_type:
                return
            raise FatalAPIError(f"ItsPerfect endpoint not found for {self.path} (404).")
        if response.status_code == 429 or response.status_code in (500, 502, 503, 504):
            self._sleep_for_retry_after(response)
            raise RetriableAPIError(
                f"ItsPerfect temporary error ({response.status_code}) for {self.path}."
            )
        if 400 <= response.status_code < 500:
            raise FatalAPIError(
                f"ItsPerfect permanent error ({response.status_code}) for {self.path}."
            )

    def _sleep_for_retry_after(self, response: requests.Response) -> None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return
        try:
            delay = float(raw)
        except (TypeError, ValueError):
            try:
                delay = parsedate_to_datetime(raw).timestamp() - time.time()
            except (TypeError, ValueError, OverflowError):
                return
        time.sleep(max(0.0, min(delay, 60.0)))

    def parse_response(self, response: requests.Response):
        if response.status_code == 204 or (
            response.status_code == 404 and self.parent_stream_type
        ):
            return
        if response.status_code == 404:
            raise FatalAPIError(f"ItsPerfect endpoint not found for {self.path} (404).")
        try:
            payload = response.json()
        except ValueError as exc:
            raise FatalAPIError(f"ItsPerfect returned malformed JSON for {self.path}.") from exc
        if not isinstance(payload, list):
            raise FatalAPIError(
                f"ItsPerfect returned {type(payload).__name__}, expected a list for {self.path}."
            )
        yield from payload

    def get_next_page_token(self, response, previous_token):
        if response.status_code == 204 or (
            response.status_code == 404 and self.parent_stream_type
        ):
            return None
        if response.status_code == 404:
            raise FatalAPIError(f"ItsPerfect endpoint not found for {self.path} (404).")

        try:
            payload = response.json()
        except ValueError as exc:
            raise FatalAPIError(f"ItsPerfect returned malformed JSON for {self.path}.") from exc
        if not isinstance(payload, list):
            raise FatalAPIError(
                f"ItsPerfect returned {type(payload).__name__}, expected a list for {self.path}."
            )

        try:
            current = int(response.headers["X-Pagination-Current-Page"])
            page_count = int(response.headers["X-Pagination-Page-Count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise FatalAPIError(
                f"ItsPerfect omitted valid pagination headers for {self.path}."
            ) from exc

        try:
            expected_page = int(previous_token or 1)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(
                f"ItsPerfect received an invalid pagination token for {self.path}."
            ) from exc
        if current != expected_page:
            raise FatalAPIError(
                f"ItsPerfect returned page {current}, expected {expected_page} for {self.path}."
            )
        if current < 1 or page_count < 0 or (payload and page_count < current):
            raise FatalAPIError(f"ItsPerfect returned invalid pagination bounds for {self.path}.")
        if not payload:
            if page_count > current:
                raise FatalAPIError(
                    f"ItsPerfect returned an empty page before the final page for {self.path}."
                )
            return None
        if current >= page_count:
            return None
        return current + 1

    def get_url_params(self, context, next_page_token):
        try:
            page = int(next_page_token or 1)
        except (TypeError, ValueError) as exc:
            raise FatalAPIError(
                f"ItsPerfect received an invalid pagination token for {self.path}."
            ) from exc
        params = {
            "limit": self._configured_page_size(),
            "page": page,
            **self.query_params,
        }
        if self.replication_key:
            marker = self.get_starting_replication_key_value(context) or self.config.get(
                "start_date"
            )
            if marker:
                # The query separator supplies '=': key 'timestamp>' becomes 'timestamp>='.
                params[f"{self.replication_key}>"] = str(marker)
        return params

    def post_process(self, row: dict, context=None) -> dict | None:
        if self.replication_key and row.get(self.replication_key) in (None, ""):
            raise FatalAPIError(
                f"ItsPerfect {self.name} record omitted replication key "
                f"{self.replication_key!r}."
            )
        return row

    def _configured_page_size(self) -> int:
        try:
            return max(1, min(int(self.config.get("page_size", 250)), 250))
        except (TypeError, ValueError):
            return 250
