"""Personio OAuth 2.0 client-credentials authentication."""

from __future__ import annotations

import backoff
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers._util import utc_now

TOKEN_URL = "https://api.personio.de/v2/auth/token"
TOKEN_TIMEOUT = (10, 30)
TOKEN_MAX_TRIES = 5


class PersonioAuthenticator(OAuthAuthenticator):
    """Mints a bearer token with the client-credentials grant (`POST /v2/auth/token`).

    Personio tokens are valid for 24 hours; the base class refreshes them
    120 seconds before `expires_in` elapses. The token is kept in memory only.
    """

    @property
    def oauth_request_body(self) -> dict:
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    def update_access_token_locally(self) -> None:
        request_time = utc_now()
        token_json = self._request_token()
        self.access_token = token_json["access_token"]
        self.expires_in = int(token_json["expires_in"]) + int(request_time.timestamp())
        self.last_refreshed = request_time
        self.logger.info("Personio access token obtained.")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=TOKEN_MAX_TRIES,
        factor=2,
        max_value=60,
    )
    def _request_token(self) -> dict:
        response = requests.post(
            self.auth_endpoint, data=self.oauth_request_payload, timeout=TOKEN_TIMEOUT
        )
        status = response.status_code
        if status == 429 or status >= 500:
            raise RetriableAPIError(f"Personio token endpoint returned {status}", response)
        if status in (400, 401, 403):
            raise InvalidCredentialsError(
                f"Personio rejected the client credentials ({status}): {_oauth_error(response)}"
            )
        if status != 200:
            raise FatalAPIError(f"Personio token endpoint returned unexpected status {status}")
        try:
            token_json = response.json()
        except ValueError as ex:
            raise FatalAPIError("Personio token response is not JSON") from ex
        if not isinstance(token_json, dict) or not token_json.get("access_token") or not token_json.get("expires_in"):
            raise FatalAPIError("Personio token response is missing access_token/expires_in")
        return token_json


def _oauth_error(response: requests.Response) -> str:
    """Return the OAuth `error`/`error_description` fields only; never the request."""
    try:
        body = response.json()
    except ValueError:
        return response.reason or "no error body"
    if not isinstance(body, dict):
        return response.reason or "no error body"
    return " - ".join(str(body[k]) for k in ("error", "error_description") if body.get(k)) or (
        response.reason or "no error body"
    )
