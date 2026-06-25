"""Authentication handling for the Colleqtive public API."""

from __future__ import annotations

import logging
import time
from typing import Dict

import requests
from requests.exceptions import RequestException
from singer_sdk.exceptions import FatalAPIError

logger = logging.getLogger(__name__)


class ColleqtiveAuthenticator:
    """OAuth2 client-credentials authenticator for Colleqtive."""

    _access_token = None
    _token_expires_at = 0.0

    def __init__(self, config: Dict) -> None:
        self.config = config

    @property
    def request_timeout(self) -> float:
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

    def _token_is_valid(self) -> bool:
        return bool(self._access_token) and time.time() < self._token_expires_at - 60

    def _fetch_access_token(self) -> str:
        token_url = self.config.get(
            "token_url",
            "https://login.microsoftonline.com/"
            "ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token",
        )
        missing = [
            key
            for key in ("client_id", "client_secret", "scope")
            if not self.config.get(key)
        ]
        if missing:
            raise FatalAPIError(
                "Missing required Colleqtive auth config: " + ", ".join(missing)
            )

        try:
            response = requests.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config["client_id"],
                    "client_secret": self.config["client_secret"],
                    "scope": self.config["scope"],
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.request_timeout,
            )
        except RequestException as exc:
            raise FatalAPIError(f"Colleqtive token request failed: {exc}") from exc

        if response.status_code == 401:
            raise FatalAPIError(
                f"Colleqtive authentication failed (401): {response.text[:300]}"
            )
        if response.status_code >= 400:
            raise FatalAPIError(
                f"Colleqtive token endpoint error ({response.status_code}): "
                f"{response.text[:300]}"
            )

        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise FatalAPIError("Colleqtive token response has no access_token")

        try:
            expires_in = int(payload.get("expires_in", 3600))
        except (TypeError, ValueError):
            expires_in = 3600

        ColleqtiveAuthenticator._access_token = token
        ColleqtiveAuthenticator._token_expires_at = time.time() + expires_in
        logger.info("Successfully obtained Colleqtive access token")
        return token

    def get_access_token(self) -> str:
        configured_token = self.config.get("access_token") or self.config.get("token")
        if configured_token:
            return configured_token
        if self._token_is_valid():
            return str(self._access_token)
        return self._fetch_access_token()

    def get_headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.get_access_token()}",
        }
