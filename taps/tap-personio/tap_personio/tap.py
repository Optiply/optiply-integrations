"""Personio tap class."""

from __future__ import annotations

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_personio.auth import TOKEN_URL, PersonioAuthenticator
from tap_personio.streams import STREAM_TYPES, PersonDirectory


class TapPersonio(Tap):
    """Singer tap for the Personio API v2."""

    name = "tap-personio"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = {
        "type": "object",
        "properties": {
            "client_id": {
                "type": "string",
                "minLength": 1,
                "description": "Personio API credential client ID.",
            },
            "client_secret": {
                "type": "string",
                "minLength": 1,
                "secret": True,
                "description": "Personio API credential client secret.",
            },
            "start_date": {
                "type": "string",
                "format": "date-time",
                "description": "Lower bound for incremental streams on the first run and "
                "first month read for compensations.",
            },
            "compensations_lookback_months": {
                "type": "integer",
                "minimum": 0,
                "default": 3,
                "description": "Months before the last compensations window that each run "
                "re-reads, to catch back-dated changes.",
            },
        },
        "required": ["client_id", "client_secret", "start_date"],
    }

    _authenticator: PersonioAuthenticator | None = None
    _person_ids: list[str] | None = None

    def get_authenticator(self, stream: Stream) -> PersonioAuthenticator:
        if self._authenticator is None:
            self._authenticator = PersonioAuthenticator(stream=stream, auth_endpoint=TOKEN_URL)
        return self._authenticator

    def person_ids(self) -> list[str]:
        """Every person's ID, listed once per run for the per-person streams."""
        if self._person_ids is None:
            self._person_ids = [row["id"] for row in PersonDirectory(tap=self).request_records(None)]
        return self._person_ids

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapPersonio.cli()
