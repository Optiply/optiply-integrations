"""Colleqtive target sink base class."""

from __future__ import annotations

import time
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional

import requests
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueSink

from target_colleqtive.auth import ColleqtiveAuthenticator


class ColleqtiveSink(HotglueSink):
    """Base sink for Colleqtive public API requests."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._authenticator = ColleqtiveAuthenticator(self.config)
        self.logger.info(
            "Initialized %s sink for stream '%s'",
            self.__class__.__name__,
            stream_name,
        )

    @property
    def base_url(self) -> str:
        return self.config.get("api_url", "https://bbq-test.colleqtive.net").rstrip("/")

    @property
    def request_timeout(self) -> float:
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

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

    @property
    def http_headers(self) -> Dict[str, str]:
        return self._authenticator.get_headers()

    def preprocess_record(self, record: dict, context: dict) -> Optional[dict]:
        return record

    def process_record(self, record: dict, context: dict) -> None:
        preprocessed = self.preprocess_record(record, context)
        if preprocessed is None:
            return
        super().process_record(preprocessed, context)

    def request_api(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        request_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        retry_auth: bool = True,
    ) -> requests.Response:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        full_url = f"{self.base_url}{endpoint}"
        request_headers = self.http_headers.copy()
        if headers:
            request_headers.update(headers)

        for attempt in range(1, 6):
            try:
                response = requests.request(
                    method.upper(),
                    full_url,
                    params=params,
                    headers=request_headers,
                    json=request_data,
                    timeout=self.request_timeout,
                )
            except requests.exceptions.RequestException as exc:
                if attempt == 5:
                    raise FatalAPIError(f"Request to {full_url} failed: {exc}") from exc
                time.sleep(min(2 ** attempt, 30))
                continue

            if response.status_code == 401 and retry_auth:
                ColleqtiveAuthenticator._access_token = None
                return self.request_api(
                    method,
                    endpoint,
                    params=params,
                    request_data=request_data,
                    headers=headers,
                    retry_auth=False,
                )

            if response.status_code == 429 and attempt < 5:
                delay = self._delay_from_retry_after(response.headers.get("Retry-After")) or 30.0
                self.logger.warning("Colleqtive rate limited (429). Sleeping %.1fs.", delay)
                time.sleep(delay)
                continue

            if response.status_code >= 500 and attempt < 5:
                time.sleep(min(2 ** attempt, 30))
                continue

            self.validate_response(response)
            return response

        raise FatalAPIError(f"Request to {full_url} failed after retries")

    def validate_response(self, response: requests.Response) -> None:
        super().validate_response(response)
