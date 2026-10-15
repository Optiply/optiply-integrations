"""Credential-free tests: every HTTP call goes through a fake transport."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_personio.client import month_windows, months_before
from tap_personio.tap import TapPersonio

CONFIG = {"client_id": "test-id", "client_secret": "test-secret", "start_date": "2024-01-01T00:00:00Z"}
API = "https://api.personio.de"
TOKEN = {"access_token": "papi-test-token", "token_type": "Bearer", "expires_in": 86400}


class FakeAPI:
    """Routes requests by path to queued responses and records every call."""

    def __init__(self) -> None:
        self.routes: dict[str, list] = {"/v2/auth/token": [(200, TOKEN, {})]}
        self.calls: list[requests.PreparedRequest] = []

    def add(self, path: str, *responses) -> None:
        self.routes[path] = list(responses)

    def send(self, adapter, request, **kwargs):
        self.calls.append(request)
        path = urlparse(request.url).path
        queue = self.routes.get(path)
        if not queue:
            raise AssertionError(f"Unexpected request {request.method} {request.url}")
        entry = queue.pop(0) if len(queue) > 1 else queue[0]
        status, body, headers = entry(request) if callable(entry) else entry
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        response.headers.update({"Content-Type": "application/json", **headers})
        response.url = request.url
        response.request = request
        response.reason = "fake"
        return response

    def calls_to(self, path: str) -> list[requests.PreparedRequest]:
        return [c for c in self.calls if urlparse(c.url).path == path]


def query(request: requests.PreparedRequest) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


def page(records: list[dict], next_cursor: str | None = None, path: str = "/v2/x") -> tuple:
    links = {"self": {"href": f"{API}{path}"}}
    if next_cursor:
        links["next"] = {"href": f"{API}{path}?cursor={next_cursor}"}
    return (200, {"_data": records, "_meta": {"links": links}}, {})


@pytest.fixture
def api(monkeypatch):
    fake = FakeAPI()
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", lambda adapter, request, **kw: fake.send(adapter, request, **kw))
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    fake.sleeps = sleeps
    return fake


def make_tap(streams: set[str], state: dict | None = None, config: dict | None = None) -> TapPersonio:
    """Tap with only `streams` selected."""
    cfg = config or CONFIG
    catalog = TapPersonio(config=cfg).catalog_dict
    for entry in catalog["streams"]:
        for md in entry["metadata"]:
            if md["breadcrumb"] == []:
                md["metadata"]["selected"] = entry["tap_stream_id"] in streams
    return TapPersonio(config=cfg, catalog=catalog, state=state or {})


def sync(tap: TapPersonio, out: io.StringIO) -> list[dict]:
    with redirect_stdout(out):
        tap.sync_all()
    return parse_messages(out)


def parse_messages(out: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


def run_tap(streams: set[str], state: dict | None = None, config: dict | None = None) -> list[dict]:
    """Sync only `streams` and return the Singer messages written to stdout."""
    return sync(make_tap(streams, state, config), io.StringIO())


def records(messages: list[dict], stream: str) -> list[dict]:
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


def last_state(messages: list[dict]) -> dict:
    return [m for m in messages if m["type"] == "STATE"][-1]["value"]


# Discovery


def test_discovery_exposes_every_stream_with_keys_and_replication():
    catalog = TapPersonio(config=CONFIG).catalog_dict
    by_name = {s["tap_stream_id"]: s for s in catalog["streams"]}
    assert len(by_name) == 25
    incremental = {
        name
        for name, s in by_name.items()
        if next(m for m in s["metadata"] if m["breadcrumb"] == [])["metadata"]["forced-replication-method"]
        == "INCREMENTAL"
    }
    assert incremental == {
        "absence_periods",
        "attendance_periods",
        "compensations",
        "employments",
        "persons",
        "recruiting_applications",
        "recruiting_candidates",
        "recruiting_jobs",
    }
    for name, stream in by_name.items():
        assert stream["key_properties"], name
        for key in stream["key_properties"]:
            assert key in stream["schema"]["properties"], (name, key)


# Auth and HTTP


def test_token_is_minted_once_and_sent_on_every_stream(api):
    api.add("/v2/cost-centers", page([{"id": "1", "name": "A"}]))
    api.add("/v2/workplaces", page([{"id": "2", "name": "B"}]))
    run_tap({"cost_centers", "workplaces"})
    token_calls = api.calls_to("/v2/auth/token")
    assert len(token_calls) == 1
    assert parse_qs(token_calls[0].body) == {
        "grant_type": ["client_credentials"],
        "client_id": ["test-id"],
        "client_secret": ["test-secret"],
    }
    for call in api.calls_to("/v2/cost-centers") + api.calls_to("/v2/workplaces"):
        assert call.headers["Authorization"] == "Bearer papi-test-token"
        assert call.headers["X-Personio-App-ID"] == "OPTIPLY"


@pytest.mark.parametrize("status", [400, 401])
def test_rejected_client_credentials_raise_invalid_credentials_without_retry(api, status):
    api.add("/v2/auth/token", (status, {"error": "invalid_client", "error_description": "bad secret"}, {}))
    with pytest.raises(InvalidCredentialsError, match="invalid_client - bad secret"):
        run_tap({"cost_centers"})
    assert len(api.calls_to("/v2/auth/token")) == 1


def test_token_endpoint_5xx_is_retried(api):
    api.add("/v2/auth/token", (503, {}, {}), (200, TOKEN, {}))
    api.add("/v2/cost-centers", page([{"id": "1", "name": "A"}]))
    assert len(records(run_tap({"cost_centers"}), "cost_centers")) == 1
    assert len(api.calls_to("/v2/auth/token")) == 2


def test_data_401_is_invalid_credentials_and_not_retried(api):
    api.add("/v2/cost-centers", (401, {"error": "unauthorized"}, {}))
    with pytest.raises(InvalidCredentialsError):
        run_tap({"cost_centers"})
    assert len(api.calls_to("/v2/cost-centers")) == 1


def test_403_names_missing_scope_and_is_not_retried(api):
    api.add("/v2/compensations/types", (403, {"error": "forbidden"}, {}))
    with pytest.raises(FatalAPIError, match="lacks the scope for stream 'compensation_types'"):
        run_tap({"compensation_types"})
    assert len(api.calls_to("/v2/compensations/types")) == 1


def test_400_is_fatal_with_response_detail(api):
    api.add("/v2/cost-centers", (400, {"errors": [{"title": "bad filter"}]}, {}))
    with pytest.raises(FatalAPIError, match="bad filter"):
        run_tap({"cost_centers"})
    assert len(api.calls_to("/v2/cost-centers")) == 1


def test_429_honours_bounded_retry_after_then_succeeds(api):
    api.add(
        "/v2/cost-centers",
        (429, {}, {"Retry-After": "7"}),
        (429, {}, {"Retry-After": "9999"}),
        (429, {}, {}),
        page([{"id": "1", "name": "A"}]),
    )
    assert len(records(run_tap({"cost_centers"}), "cost_centers")) == 1
    assert 7.0 in api.sleeps
    assert 120 in api.sleeps  # 9999s capped
    assert all(s <= 120 for s in api.sleeps)


def test_persistent_5xx_gives_up_after_bounded_attempts(api):
    api.add("/v2/cost-centers", (502, {}, {}))
    with pytest.raises(Exception, match="502"):
        run_tap({"cost_centers"})
    assert len(api.calls_to("/v2/cost-centers")) == 5
    assert all(s <= 60 for s in api.sleeps)


def test_malformed_body_is_fatal(api):
    api.add("/v2/cost-centers", (200, {"data": []}, {}))
    with pytest.raises(FatalAPIError, match="Malformed"):
        run_tap({"cost_centers"})


# Pagination


def test_pagination_follows_cursor_and_keeps_filters(api):
    api.add(
        "/v2/org-units",
        lambda r: page([{"id": f"{query(r)['type']}-1", "type": query(r)["type"], "name": "x"}], "c2", "/v2/org-units")
        if "cursor" not in query(r)
        else page([{"id": f"{query(r)['type']}-2", "type": query(r)["type"], "name": "y"}], None, "/v2/org-units"),
    )
    got = records(run_tap({"org_units"}), "org_units")
    assert sorted(r["id"] for r in got) == ["department-1", "department-2", "team-1", "team-2"]
    second_pages = [query(c) for c in api.calls_to("/v2/org-units") if "cursor" in query(c)]
    assert second_pages == [
        {"type": "department", "limit": "100", "cursor": "c2"},
        {"type": "team", "limit": "100", "cursor": "c2"},
    ]


def test_empty_first_page_terminates(api):
    api.add("/v2/cost-centers", page([]))
    assert records(run_tap({"cost_centers"}), "cost_centers") == []
    assert len(api.calls_to("/v2/cost-centers")) == 1


def _cursorless_next(records: list[dict], path: str) -> tuple:
    status, body, headers = page(records, None, path)
    body["_meta"]["links"]["next"] = {"href": f"{API}{path}"}
    return status, body, headers


def _application(id_: str) -> dict:
    return {"id": id_, "updated_at": {"date-time": "2024-03-01T10:00:00Z", "timezone": "UTC"}}


def test_cursorless_next_link_on_short_last_page_ends_pagination(api):
    path = "/v2/recruiting/applications"
    full = [_application(str(i)) for i in range(200)]
    api.add(path, page(full, "c2", path), _cursorless_next([_application("last")], path))
    rows = records(run_tap({"recruiting_applications"}), "recruiting_applications")
    assert len(rows) == 201
    assert len(api.calls_to(path)) == 2


def test_cursorless_next_link_on_full_page_fails(api):
    path = "/v2/recruiting/applications"
    api.add(path, _cursorless_next([_application(str(i)) for i in range(200)], path))
    with pytest.raises(FatalAPIError, match="no cursor"):
        run_tap({"recruiting_applications"})


def test_repeated_cursor_is_rejected(api):
    api.add(
        "/v2/cost-centers",
        page([{"id": "1", "name": "A"}], "c1", "/v2/cost-centers"),
        page([{"id": "2", "name": "B"}], "c2", "/v2/cost-centers"),
        page([{"id": "3", "name": "C"}], "c1", "/v2/cost-centers"),
    )
    with pytest.raises(RuntimeError, match="already visited cursor"):
        run_tap({"cost_centers"})


# Incremental


def _absence(id_: str, updated_at: str) -> dict:
    return {"id": id_, "updated_at": updated_at, "person": {"id": "p1"}}


def test_incremental_first_run_bounds_window_and_bookmarks_max_emitted(api):
    future = "2999-01-01T00:00:00Z"
    api.add(
        "/v2/absence-periods",
        page([_absence("a1", "2024-03-01T10:00:00Z"), _absence("a2", future), _absence("a3", "2024-02-01T00:00:00Z")]),
    )
    messages = run_tap({"absence_periods"})
    params = query(api.calls_to("/v2/absence-periods")[0])
    assert params["updated_at.gte"] == "2024-01-01T00:00:00Z"
    assert datetime.fromisoformat(params["updated_at.lte"].replace("Z", "+00:00")) <= datetime.now(timezone.utc)
    assert [r["id"] for r in records(messages, "absence_periods")] == ["a1", "a3"]
    bookmark = last_state(messages)["bookmarks"]["absence_periods"]
    assert bookmark["replication_key_value"] == "2024-03-01T10:00:00Z"


def test_incremental_second_run_resumes_from_bookmark(api):
    api.add("/v2/attendance-periods", page([]))
    state = {"bookmarks": {"attendance_periods": {"replication_key": "updated_at", "replication_key_value": "2024-05-05T05:05:05Z"}}}
    messages = run_tap({"attendance_periods"}, state=state)
    assert query(api.calls_to("/v2/attendance-periods")[0])["updated_at.gte"] == "2024-05-05T05:05:05Z"
    assert last_state(messages)["bookmarks"]["attendance_periods"]["replication_key_value"] == "2024-05-05T05:05:05Z"


def test_failed_page_does_not_advance_bookmark(api):
    api.add(
        "/v2/absence-periods",
        page([_absence("a1", "2024-03-01T10:00:00Z")], "c2", "/v2/absence-periods"),
        (400, {"error": "boom"}, {}),
    )
    state = {"bookmarks": {"absence_periods": {"replication_key": "updated_at", "replication_key_value": "2024-01-15T00:00:00Z"}}}
    tap = make_tap({"absence_periods"}, state=state)
    out = io.StringIO()
    with pytest.raises(FatalAPIError):
        sync(tap, out)
    messages = parse_messages(out)
    assert [r["id"] for r in records(messages, "absence_periods")] == ["a1"]
    assert [m for m in messages if m["type"] == "STATE"] == []
    assert tap.state["bookmarks"]["absence_periods"]["replication_key_value"] == "2024-01-15T00:00:00Z"


def test_recruiting_uses_exclusive_lower_bound_with_lookback_and_nested_updated_at(api):
    api.add(
        "/v2/recruiting/jobs",
        page([{"id": "j1", "name": "Dev", "updated_at": {"date-time": "2024-06-01T12:00:00Z", "timezone": "UTC"}}]),
    )
    state = {"bookmarks": {"recruiting_jobs": {"replication_key": "updated_at_date_time", "replication_key_value": "2024-06-01T12:00:00Z"}}}
    messages = run_tap({"recruiting_jobs"}, state=state)
    params = query(api.calls_to("/v2/recruiting/jobs")[0])
    assert params["updated_at.gt"] == "2024-06-01T11:59:59Z"
    assert "updated_at.lt" not in params
    assert records(messages, "recruiting_jobs")[0]["updated_at_date_time"] == "2024-06-01T12:00:00Z"


def test_record_without_replication_value_fails_closed(api):
    api.add("/v2/recruiting/candidates", page([{"id": "c1", "first_name": "A"}]))
    with pytest.raises(FatalAPIError, match="has no updated_at"):
        run_tap({"recruiting_candidates"})


# Parent/child


def test_per_person_streams_cover_every_person_while_persons_is_incremental(api):
    def persons(r):
        if "updated_at.gt" in query(r):  # the incremental `persons` stream
            return page([{"id": "p2", "updated_at": "2024-06-02T00:00:00Z"}])
        return page([{"id": "p1"}, {"id": "p2"}])  # the unfiltered person directory

    api.add("/v2/persons", persons)
    api.add("/v2/persons/p1/employments", page([{"id": "e1", "updated_at": "2024-06-03T00:00:00Z"}]))
    api.add("/v2/persons/p2/employments", page([]))
    api.add("/v2/document-management/documents", lambda r: page([{"id": f"d-{query(r)['owner_id']}"}]))
    state = {
        "bookmarks": {
            "persons": {"replication_key": "updated_at", "replication_key_value": "2024-06-01T00:00:00Z"},
            "employments": {"replication_key": "updated_at", "replication_key_value": "2024-06-01T00:00:00Z"},
        }
    }
    messages = run_tap({"persons", "employments", "documents"}, state=state)

    assert [r["id"] for r in records(messages, "persons")] == ["p2"]
    # p1 did not change as a person, but its employment did: it is still read.
    assert [(r["id"], r["person_id"]) for r in records(messages, "employments")] == [("e1", "p1")]
    for call in api.calls_to("/v2/persons/p1/employments") + api.calls_to("/v2/persons/p2/employments"):
        assert query(call)["updated_at.gt"] == "2024-05-31T23:59:59Z"
    assert {(r["id"], r["person_id"]) for r in records(messages, "documents")} == {("d-p1", "p1"), ("d-p2", "p2")}
    assert len([c for c in api.calls_to("/v2/persons") if "updated_at.gt" not in query(c)]) == 1
    bookmarks = last_state(messages)["bookmarks"]
    assert bookmarks["persons"]["replication_key_value"] == "2024-06-02T00:00:00Z"
    assert bookmarks["employments"]["replication_key_value"] == "2024-06-03T00:00:00Z"
    assert "partitions" not in bookmarks.get("documents", {})


def test_stale_full_table_catalog_reads_incremental_streams_in_full(api):
    # Catalog saved before persons/employments became incremental (Hotglue job iizdomh6wm4n0oik321pg).
    api.add("/v2/persons", page([{"id": "p1", "updated_at": "2024-06-02T00:00:00Z"}]))
    api.add("/v2/persons/p1/employments", page([{"id": "e1"}]))
    tap = make_tap({"persons", "employments"})
    catalog = tap.catalog_dict
    for entry in catalog["streams"]:
        if entry["tap_stream_id"] in {"persons", "employments"}:
            entry.pop("replication_key", None)
            entry["replication_method"] = "FULL_TABLE"
    tap = TapPersonio(config=CONFIG, catalog=catalog, state={})
    messages = sync(tap, io.StringIO())

    assert [r["id"] for r in records(messages, "persons")] == ["p1"]
    assert [(r["id"], r["person_id"]) for r in records(messages, "employments")] == [("e1", "p1")]
    for call in api.calls_to("/v2/persons") + api.calls_to("/v2/persons/p1/employments"):
        assert not any(k.startswith("updated_at") for k in query(call))


def test_employee_attributes_define_custom_attribute_ids(api):
    api.add(
        "/v1/company/employees/attributes",
        (200, {"success": True, "data": [
            {"key": "hire_date", "label": "Hire date", "type": "date", "universal_id": "hire_date", "options": []},
            {"key": "dynamic_2", "label": "Marital status", "type": "list", "universal_id": None, "options": ["married", "single"]},
        ]}, {}),
    )
    messages = run_tap({"employee_attributes"})
    rows = {r["key"]: r for r in records(messages, "employee_attributes")}
    assert rows["dynamic_2"]["label"] == "Marital status"
    assert rows["dynamic_2"]["options"] == ["married", "single"]
    assert rows.keys() == {"hire_date", "dynamic_2"}
    assert len(api.calls_to("/v1/company/employees/attributes")) == 1


def test_employee_attributes_unsuccessful_body_fails(api):
    api.add("/v1/company/employees/attributes", (200, {"success": False, "error": {"message": "nope"}}, {}))
    with pytest.raises(FatalAPIError, match="Malformed Personio response"):
        run_tap({"employee_attributes"})


def test_project_members_get_composite_identity(api):
    api.add("/v2/projects", page([{"id": "pr1"}]))
    api.add("/v2/projects/pr1/members", page([{"person": {"id": "p9"}}]))
    member = records(run_tap({"project_members"}), "project_members")[0]
    assert (member["project_id"], member["person_id"]) == ("pr1", "p9")


def test_breakdowns_only_for_absence_periods_in_window(api):
    api.add(
        "/v2/absence-periods",
        page([_absence("a1", "2024-03-01T10:00:00Z"), _absence("late", "2999-01-01T00:00:00Z")]),
    )
    api.add("/v2/absence-periods/a1/breakdowns", page([{"date": "2024-03-01", "effective_duration": {"value": 1}}]))
    rows = records(run_tap({"absence_periods", "absence_period_breakdowns"}), "absence_period_breakdowns")
    assert [(r["absence_period_id"], r["date"]) for r in rows] == [("a1", "2024-03-01")]
    assert api.calls_to("/v2/absence-periods/late/breakdowns") == []


# Compensations


def test_month_windows_cover_calendar_months_across_year_end():
    windows = list(month_windows(datetime(2023, 11, 20, tzinfo=timezone.utc), datetime(2024, 2, 3, tzinfo=timezone.utc)))
    assert windows == [
        {"start_date": "2023-11-01", "end_date": "2023-11-30"},
        {"start_date": "2023-12-01", "end_date": "2023-12-31"},
        {"start_date": "2024-01-01", "end_date": "2024-01-31"},
        {"start_date": "2024-02-01", "end_date": "2024-02-29"},
    ]


def test_compensations_first_run_reads_from_start_date_and_bookmarks_last_window(api):
    api.add("/v2/compensations", lambda r: page([{"id": f"c-{query(r)['start_date']}"}]))
    start = datetime.now(timezone.utc).replace(day=1)
    config = {**CONFIG, "start_date": start.strftime("%Y-%m-%dT00:00:00Z")}
    messages = run_tap({"compensations"}, config=config)
    expected = start.strftime("%Y-%m-01")
    assert [(r["id"], r["window_start_date"]) for r in records(messages, "compensations")] == [(f"c-{expected}", expected)]
    assert query(api.calls_to("/v2/compensations")[0])["start_date"] == expected
    assert last_state(messages)["bookmarks"]["compensations"]["replication_key_value"] == expected


def test_compensations_resume_from_bookmark_minus_lookback(api):
    api.add("/v2/compensations", lambda r: page([{"id": f"c-{query(r)['start_date']}"}]))
    current = datetime.now(timezone.utc).date().replace(day=1)
    state = {"bookmarks": {"compensations": {"replication_key": "window_start_date", "replication_key_value": current.isoformat()}}}
    config = {**CONFIG, "compensations_lookback_months": 2}
    messages = run_tap({"compensations"}, state=state, config=config)
    requested = [query(c)["start_date"] for c in api.calls_to("/v2/compensations")]
    assert requested == [months_before(current, n).isoformat() for n in (2, 1, 0)]
    assert last_state(messages)["bookmarks"]["compensations"]["replication_key_value"] == current.isoformat()
