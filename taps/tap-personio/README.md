# tap-personio

Singer tap for the [Personio API v2](https://developer.personio.de/reference/introduction), built on `hotglue-singer-sdk`. Personio is an HR system, so this tap feeds no Optiply inventory entity. It extracts every readable v2 business resource for internal use.

## Config

| Key | Required | Description |
| --- | --- | --- |
| `client_id` | yes | Personio API credential client ID. |
| `client_secret` | yes (secret) | Personio API credential client secret. |
| `start_date` | yes | Lower bound for incremental streams on the first run, and the first month read for `compensations`. |
| `compensations_lookback_months` | no (default 3) | Months before the last `compensations` window that each run re-reads, to catch back-dated changes. |

Auth uses the OAuth 2.0 client-credentials grant (`POST /v2/auth/token`). The tap mints one token per run, keeps it in memory, and shares it across all streams. Each Personio API credential grants read scopes per resource. Deselect any stream whose scope the credential lacks: a 403 response fails the run and names the stream.

See `config.json.example`.

## Streams

| Stream | Endpoint | Primary key | Replication | Notes |
| --- | --- | --- | --- | --- |
| `persons` | `/v2/persons` | `id` | incremental `updated_at` | `updated_at.gt`. |
| `employments` | `/v2/persons/{id}/employments` | `id` | incremental `updated_at` | Read for every person (see below), filtered with `updated_at.gt`. Adds `person_id`. |
| `documents` | `/v2/document-management/documents?owner_id=` | `id` | full table | Metadata only, read for every person. Personio filters documents only by `created_at`, which would miss edits. Adds `person_id`. |
| `compensations` | `/v2/compensations` | `id` | incremental `window_start_date` | Calendar-month windows (the API maximum). Compensations have no change timestamp; each record gets the `window_start_date` it was read for, and each run re-reads from `compensations_lookback_months` before the last window through the current month. An active compensation appears in every month it applies to. |
| `compensation_types` | `/v2/compensations/types` | `id` | full table | |
| `absence_types` | `/v2/absence-types` | `id` | full table | |
| `absence_periods` | `/v2/absence-periods` | `id` | incremental `updated_at` | `updated_at.gte` / `.lte`. |
| `absence_period_breakdowns` | `/v2/absence-periods/{id}/breakdowns` | `absence_period_id`, `date` | full table, child | Re-read for every changed absence period. |
| `attendance_periods` | `/v2/attendance-periods` | `id` | incremental `updated_at` | `updated_at.gte` / `.lte`. |
| `projects` | `/v2/projects` | `id` | full table | |
| `project_members` | `/v2/projects/{id}/members` | `project_id`, `person_id` | full table, child | |
| `org_units` | `/v2/org-units` | `type`, `id` | full table | One partition each for `department` and `team` (`type` is a required filter). |
| `legal_entities` | `/v2/legal-entities` | `id` | full table | |
| `cost_centers` | `/v2/cost-centers` | `id` | full table | |
| `workplaces` | `/v2/workplaces` | `id` | full table | |
| `jobs` | `/v2/jobs` | `id` | full table | |
| `salary_bands` | `/v2/salary-bands` | `id` | full table | |
| `reports` | `/v2/reports` | `id` | full table | Report definitions only. |
| `report_attributes` | `/v2/reports/attributes` | `name` | full table | |
| `recruiting_applications` | `/v2/recruiting/applications` | `id` | incremental `updated_at_date_time` | `updated_at` is an object; its `date-time` is copied to `updated_at_date_time`. |
| `recruiting_application_stage_transitions` | `/v2/recruiting/applications/{id}/stage-transitions` | `application_id`, `entered_at` | full table, child | Personio gives transitions no ID. |
| `recruiting_candidates` | `/v2/recruiting/candidates` | `id` | incremental `updated_at` | |
| `recruiting_jobs` | `/v2/recruiting/jobs` | `id` | incremental `updated_at_date_time` | Same nested `updated_at` as applications. |
| `recruiting_categories` | `/v2/recruiting/categories` | `id` | full table | |

Not extracted:

- Webhooks and their events/activity. These are integration configuration, the webhook list includes signing `token` secrets, and events/activity are 30-day delivery logs.
- Document file downloads (binary content).
- Report result rows (`/v2/reports/{id}`). Each report has its own column set, and rows have no stable identity.
- Single-record `GET /{id}` endpoints. The list endpoints return the same resources.
- Deletes. The v2 list endpoints expose no deleted-record feed, so deleted records are not emitted.

## Behavior

- **Pagination:** the tap follows `_meta.links.next.href` cursors and sends the stream's filters on every page. Recruiting endpoints return a `next` link without a cursor on their last page; that ends the listing only when the page is shorter than the requested limit, otherwise the stream fails. A cursor that repeats within one listing fails the stream, and so does a body without a `_data` list.
- **Incremental sync:** the lower bound is the bookmark, or `start_date` on the first run. Records updated after the sync started are skipped and picked up next run, so the bookmark never passes unread data. Recruiting endpoints only allow an exclusive `updated_at.gt` with no upper bound, so the tap re-reads 1 second before the bookmark and drops later records client-side. A record without its replication value fails the stream.
- **Per-person streams:** `employments` and `documents` list every person once per run (unfiltered `/v2/persons`, not the `persons` stream), so a change to an employment or document is read even when the person's own `updated_at` did not change.
- **Full-table child streams:** these keep no per-parent state entries.
- **Deletes:** incremental streams never report deleted records, because Personio has no deleted-record feed.
- **HTTP errors:**
  - 401 raises `InvalidCredentialsError` and is not retried.
  - 400, 403, 404 and other 4xx responses are fatal.
  - 429 waits for `Retry-After` (capped at 120 s) and then backs off.
  - 429, 5xx and network errors are retried up to 5 attempts, with exponential backoff, full jitter and at most 60 s between tries.
  - Requests time out after 10 s to connect and 120 s to read.
- **Headers:** every request sends `X-Personio-App-ID: OPTIPLY`.
- **Rate limits:** the token endpoint allows 150 requests per minute (the tap requests one token per run). Personio does not document limits for data endpoints, so 429s are handled by the retry policy above.

## Development

```bash
uv sync
uv run pytest
uv run tap-personio --config config.json --discover > catalog.json
uv run tap-personio --config config.json --catalog catalog.json
```

The JSON schemas in `tap_personio/schemas/` were generated from Personio's per-endpoint OpenAPI 3 definitions (`developer.personio.de/reference/*.md`). In the generated schemas every field is nullable, enums are dropped, `oneOf` objects are merged, and the tap adds the parent-ID and derived fields listed above.
