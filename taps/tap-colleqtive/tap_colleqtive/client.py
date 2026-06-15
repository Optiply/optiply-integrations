"""HTTP client for the Colleqtive public API."""

from __future__ import annotations

import logging
import time
from email.utils import parsedate_to_datetime
from typing import Optional

import backoff
import requests

try:
    from hotglue_singer_sdk.exceptions import InvalidCredentialsError
except ImportError:
    class InvalidCredentialsError(Exception):
        """Raised when API credentials are invalid."""


logger = logging.getLogger(__name__)


class RetryableAPIError(Exception):
    """Raised for errors that should trigger backoff retry."""


class ColleqtiveClient:
    """Authenticated HTTP client for the Colleqtive public API."""

    _access_token: Optional[str] = None
    _token_expires_at = 0.0
    _next_request_at = 0.0

    def __init__(self, config: dict, *, logger_: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger_ or logger
        self._session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Accept": "application/json",
                "Content-Type": "application/json",
            })
        return self._session

    @property
    def base_url(self) -> str:
        return self.config.get("api_url", "https://bbq-test.colleqtive.net").rstrip("/")

    @property
    def requests_per_second(self) -> float:
        raw_value = self.config.get("requests_per_second", 4)
        try:
            requests_per_second = float(raw_value)
        except (TypeError, ValueError):
            self.logger.warning(
                "Invalid requests_per_second=%r. Falling back to 4 requests/second.",
                raw_value,
            )
            requests_per_second = 4.0
        return max(requests_per_second, 0.1)

    @property
    def request_timeout(self) -> float:
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

    def _apply_client_throttle(self) -> None:
        min_interval = 1.0 / self.requests_per_second
        now = time.monotonic()
        sleep_for = ColleqtiveClient._next_request_at - now
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.monotonic()
        ColleqtiveClient._next_request_at = max(
            ColleqtiveClient._next_request_at,
            now,
        ) + min_interval

    @staticmethod
    def _delay_from_retry_after(retry_after: Optional[str]) -> Optional[float]:
        if not retry_after:
            return None
        try:
            return max(float(retry_after), 0.0)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None
            return max(retry_at.timestamp() - time.time(), 0.0)

    def _token_is_valid(self) -> bool:
        return bool(self._access_token) and time.time() < self._token_expires_at - 60

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, RetryableAPIError),
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def _fetch_access_token(self) -> str:
        response = requests.post(
            self.config.get(
                "token_url",
                "https://login.microsoftonline.com/"
                "ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token",
            ),
            data={
                "grant_type": "client_credentials",
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "scope": self.config["scope"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.request_timeout,
        )

        if response.status_code in (429, 500, 502, 503, 504):
            if response.status_code == 429:
                delay = self._delay_from_retry_after(response.headers.get("Retry-After")) or 30.0
                self.logger.warning("Token endpoint rate limited (429). Sleeping %.1fs.", delay)
                time.sleep(delay)
            raise RetryableAPIError(f"Token endpoint error ({response.status_code})")
        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )

        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise InvalidCredentialsError("Authentication failed: token response has no access_token")

        expires_in = payload.get("expires_in", 3600)
        try:
            expires_in_seconds = int(expires_in)
        except (TypeError, ValueError):
            expires_in_seconds = 3600

        ColleqtiveClient._access_token = token
        ColleqtiveClient._token_expires_at = time.time() + expires_in_seconds
        return token

    def _get_access_token(self) -> str:
        if self._token_is_valid():
            return str(self._access_token)
        return self._fetch_access_token()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, RetryableAPIError),
        max_tries=8,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def request(
        self,
        path: str,
        params: Optional[dict] = None,
        *,
        retry_auth: bool = True,
    ) -> requests.Response:
        """Issue an authenticated GET request."""
        self._apply_client_throttle()
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            headers={"Authorization": f"Bearer {self._get_access_token()}"},
            timeout=self.request_timeout,
        )

        if response.status_code == 401 and retry_auth:
            ColleqtiveClient._access_token = None
            return self.request(path, params=params, retry_auth=False)
        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )
        if response.status_code == 429:
            delay = self._delay_from_retry_after(response.headers.get("Retry-After")) or 30.0
            self.logger.warning("Rate limited (429). Sleeping %.1fs.", delay)
            time.sleep(delay)
            raise RetryableAPIError("Rate limited (429)")
        if response.status_code >= 500:
            raise RetryableAPIError(
                f"Server error ({response.status_code}): {response.text[:300]}"
            )
        if response.status_code >= 400:
            self.logger.error(
                "GET %s -> %d: %s",
                response.url,
                response.status_code,
                (response.text or "")[:500],
            )
        response.raise_for_status()
        return response
