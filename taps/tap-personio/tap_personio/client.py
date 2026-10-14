"""Shared request, pagination, retry, and incremental logic for Personio v2 streams."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlparse

import backoff
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream

from tap_personio.auth import PersonioAuthenticator

SCHEMAS_DIR = Path(__file__).parent / "schemas"
API_URL = "https://api.personio.de"
APP_ID = "OPTIPLY"
REQUEST_TIMEOUT = (10, 120)
MAX_RETRY_AFTER_SECONDS = 120
ERROR_BODY_LIMIT = 500


def format_timestamp(value: datetime) -> str:
    """Render a UTC timestamp the way every Personio v2 filter accepts it."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class PersonioStream(RESTStream):
    """Personio v2 list endpoint: `{"_data": [...], "_meta": {"links": {"next": {"href": ...}}}}`."""

    url_base = API_URL
    records_jsonpath = "$._data[*]"
    page_size: int | None = None
    declared_replication_method = "FULL_TABLE"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.forced_replication_method = self.declared_replication_method
        self._seen_cursors: set[str] = set()

    @property
    def schema_filepath(self) -> Path:
        return SCHEMAS_DIR / f"{self.name}.json"

    @property
    def authenticator(self) -> PersonioAuthenticator:
        """One token per tap run, shared by every stream."""
        return self._tap.get_authenticator(self)

    @property
    def http_headers(self) -> dict:
        return {"Accept": "application/json", "X-Personio-App-ID": APP_ID}

    @property
    def timeout(self) -> tuple[int, int]:
        return REQUEST_TIMEOUT

    def _get_state_partition_context(self, context: dict | None) -> dict | None:
        # Full-table child streams keep no bookmark, so one state entry per parent
        # record (every person, every absence period ever synced) would only grow.
        if self.parent_stream_type and self.replication_method == "FULL_TABLE":
            return None
        return super()._get_state_partition_context(context)

    # Requests and retries

    def validate_response(self, response: requests.Response) -> None:
        status = response.status_code
        if status == 401:
            raise InvalidCredentialsError(
                f"Personio rejected the access token for {self._error_path(response)} (401)."
            )
        if status == 429:
            self._wait_retry_after(response)
            raise RetriableAPIError(self._error_message(response), response)
        if status >= 500:
            raise RetriableAPIError(self._error_message(response), response)
        if status == 403:
            raise FatalAPIError(
                f"{self._error_message(response)} The Personio API credential lacks the scope "
                f"for stream '{self.name}'; grant it in Personio or deselect the stream."
            )
        if status >= 400:
            raise FatalAPIError(self._error_message(response))
        if status != 200:
            raise FatalAPIError(f"Unexpected {status} from Personio for {self._error_path(response)}")

    def _wait_retry_after(self, response: requests.Response) -> None:
        header = response.headers.get("Retry-After")
        if header is None:
            return
        try:
            seconds = float(header)
        except ValueError:
            return
        time.sleep(min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS))

    @staticmethod
    def _error_path(response: requests.Response) -> str:
        return urlparse(response.url).path

    def _error_message(self, response: requests.Response) -> str:
        return (
            f"{response.status_code} {response.reason} from Personio for "
            f"{self._error_path(response)}: {response.text[:ERROR_BODY_LIMIT]}"
        )

    def backoff_wait_generator(self):
        return backoff.expo(factor=2, max_value=60)

    def backoff_max_tries(self) -> int:
        return 5

    # Pagination

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        try:
            body = response.json()
        except ValueError as ex:
            raise FatalAPIError(f"Non-JSON Personio response for {self._error_path(response)}") from ex
        if not isinstance(body, dict) or not isinstance(body.get("_data"), list):
            raise FatalAPIError(f"Malformed Personio response for {self._error_path(response)}")
        yield from body["_data"]

    def get_next_page_token(self, response: requests.Response, previous_token: Any) -> str | None:
        if previous_token is None:
            self._seen_cursors = set()
        body = response.json()
        meta = body.get("_meta") or {}
        href = ((meta.get("links") or {}).get("next") or {}).get("href")
        if not href:
            return None
        cursor = dict(parse_qsl(urlparse(href).query)).get("cursor")
        if not cursor:
            # Recruiting endpoints return a cursor-less `next` link on their last page.
            # Only a page shorter than the requested limit proves the list is exhausted.
            if self.page_size and len(body["_data"]) < self.page_size:
                return None
            raise FatalAPIError(f"Personio next link has no cursor for {self._error_path(response)}")
        if cursor in self._seen_cursors:
            raise RuntimeError(f"Personio returned an already visited cursor for stream '{self.name}'.")
        self._seen_cursors.add(cursor)
        return href

    def get_url_params(self, context: dict | None, next_page_token: Any) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if self.page_size:
            params["limit"] = self.page_size
        params.update(self.get_filter_params(context))
        if next_page_token:
            params.update(parse_qsl(urlparse(next_page_token).query))
        return params

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        """Per-stream query filters, sent on every page."""
        return {}


class IncrementalPersonioStream(PersonioStream):
    """Stream filtered server-side by its `updated_at` marker.

    The lower bound is the bookmark (or `start_date`). Records changed after this
    sync started are skipped so the bookmark never passes data that has not been
    read yet; they are picked up by the next sync.
    """

    #: Server filter for the lower bound and whether it is exclusive (`.gt`).
    lower_bound_param: str = "updated_at.gte"
    lower_bound_exclusive: bool = False
    #: Server filter for the upper bound, if the endpoint allows combining it.
    upper_bound_param: str | None = "updated_at.lte"
    declared_replication_method = "INCREMENTAL"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sync_started_at = datetime.now(timezone.utc).replace(microsecond=0)

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        start = self.get_starting_timestamp(context) or self.get_config_start_date()
        if self.lower_bound_exclusive:
            # `.gt` is exclusive at second precision: step back 1s so ties with the
            # bookmark are re-read (duplicates are deduplicated by primary key).
            start = start - timedelta(seconds=1)
        params = {self.lower_bound_param: format_timestamp(start)}
        if self.upper_bound_param:
            params[self.upper_bound_param] = format_timestamp(self.sync_started_at)
        return params

    def replication_value(self, row: dict) -> str | None:
        return row.get(self.replication_key)

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        value = self.replication_value(row)
        if value is None:
            raise FatalAPIError(
                f"Record {row.get('id')!r} in stream '{self.name}' has no {self.replication_key}."
            )
        if parse_timestamp(value) > self.sync_started_at:
            return None
        return row


class PerPersonStream(PersonioStream):
    """One listing per Personio person.

    The person IDs come from a full person listing made once per run (see
    `TapPersonio.person_ids`), not from the `persons` stream, so `persons` can be
    incremental without hiding changes to records that do not bump a person's
    `updated_at`.
    """

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for person_id in self._tap.person_ids():
            person_context = {"person_id": person_id}
            for row in self.request_records(person_context):
                row = self.post_process(row, person_context)
                if row is not None:
                    yield row

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row = super().post_process(row, context)
        if row is not None:
            row["person_id"] = context["person_id"]
        return row


def months_before(day: date, months: int) -> date:
    """First day of the month `months` calendar months before `day`'s month."""
    index = day.year * 12 + (day.month - 1) - months
    return date(index // 12, index % 12 + 1, 1)


def month_windows(start: datetime, end: datetime) -> Iterable[dict[str, str]]:
    """Calendar-month `start_date`/`end_date` windows from `start`'s month through `end`'s."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
        yield {
            "start_date": f"{year:04d}-{month:02d}-01",
            "end_date": f"{year:04d}-{month:02d}-{last_day:02d}",
        }
        year, month = next_year, next_month
