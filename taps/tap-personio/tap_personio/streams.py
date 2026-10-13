"""Personio v2 stream definitions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from tap_personio.client import IncrementalPersonioStream, PersonioStream, month_windows

# People


class PersonsStream(PersonioStream):
    """Full table: every person is the parent of `employments` and `documents`, whose
    changes are not documented to bump the person's `updated_at`."""

    name = "persons"
    path = "/v2/persons"
    primary_keys = ["id"]
    page_size = 50

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"person_id": record["id"]}


class EmploymentsStream(PersonioStream):
    name = "employments"
    parent_stream_type = PersonsStream
    path = "/v2/persons/{person_id}/employments"
    primary_keys = ["id"]
    page_size = 50

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["person_id"] = context["person_id"]
        return row


class DocumentsStream(PersonioStream):
    """Document metadata per person (file downloads are not extracted)."""

    name = "documents"
    parent_stream_type = PersonsStream
    path = "/v2/document-management/documents"
    primary_keys = ["id"]
    page_size = 200

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        return {"owner_id": context["person_id"]}

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["person_id"] = context["person_id"]
        return row


class CompensationsStream(PersonioStream):
    """Payroll compensations, read in calendar-month windows (the API's maximum)
    from `start_date` through the current month, every run."""

    name = "compensations"
    path = "/v2/compensations"
    primary_keys = ["id"]
    page_size = 100

    def get_paging_windows(self, context: dict | None) -> list[dict[str, str]]:
        return list(month_windows(self.get_config_start_date(), datetime.now(timezone.utc)))

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        return {"start_date": context["start_date"], "end_date": context["end_date"]}


class CompensationTypesStream(PersonioStream):
    name = "compensation_types"
    path = "/v2/compensations/types"
    primary_keys = ["id"]
    page_size = 100


# Absences and attendances


class AbsenceTypesStream(PersonioStream):
    name = "absence_types"
    path = "/v2/absence-types"
    primary_keys = ["id"]
    page_size = 100


class AbsencePeriodsStream(IncrementalPersonioStream):
    name = "absence_periods"
    path = "/v2/absence-periods"
    primary_keys = ["id"]
    replication_key = "updated_at"
    page_size = 100

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"absence_period_id": record["id"]}


class AbsencePeriodBreakdownsStream(PersonioStream):
    """Daily breakdowns, re-read whenever their absence period changed."""

    name = "absence_period_breakdowns"
    parent_stream_type = AbsencePeriodsStream
    path = "/v2/absence-periods/{absence_period_id}/breakdowns"
    primary_keys = ["absence_period_id", "date"]
    page_size = 28

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["absence_period_id"] = context["absence_period_id"]
        return row


class AttendancePeriodsStream(IncrementalPersonioStream):
    name = "attendance_periods"
    path = "/v2/attendance-periods"
    primary_keys = ["id"]
    replication_key = "updated_at"
    page_size = 100


class ProjectsStream(PersonioStream):
    name = "projects"
    path = "/v2/projects"
    primary_keys = ["id"]
    page_size = 200

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"project_id": record["id"]}


class ProjectMembersStream(PersonioStream):
    name = "project_members"
    parent_stream_type = ProjectsStream
    path = "/v2/projects/{project_id}/members"
    primary_keys = ["project_id", "person_id"]
    page_size = 200

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["project_id"] = context["project_id"]
        row["person_id"] = row["person"]["id"]
        return row


# Organization


class OrgUnitsStream(PersonioStream):
    """`type` is a required filter; Personio currently supports these two."""

    name = "org_units"
    path = "/v2/org-units"
    primary_keys = ["type", "id"]
    page_size = 100
    partitions = [{"type": "department"}, {"type": "team"}]

    def get_filter_params(self, context: dict | None) -> dict[str, Any]:
        return {"type": context["type"]}


class LegalEntitiesStream(PersonioStream):
    name = "legal_entities"
    path = "/v2/legal-entities"
    primary_keys = ["id"]
    page_size = 100


class CostCentersStream(PersonioStream):
    name = "cost_centers"
    path = "/v2/cost-centers"
    primary_keys = ["id"]
    page_size = 100


class WorkplacesStream(PersonioStream):
    name = "workplaces"
    path = "/v2/workplaces"
    primary_keys = ["id"]
    page_size = 100


class JobsStream(PersonioStream):
    name = "jobs"
    path = "/v2/jobs"
    primary_keys = ["id"]
    page_size = 100


class SalaryBandsStream(PersonioStream):
    name = "salary_bands"
    path = "/v2/salary-bands"
    primary_keys = ["id"]
    page_size = 100


# Reporting


class ReportsStream(PersonioStream):
    name = "reports"
    path = "/v2/reports"
    primary_keys = ["id"]
    page_size = 100


class ReportAttributesStream(PersonioStream):
    name = "report_attributes"
    path = "/v2/reports/attributes"
    primary_keys = ["name"]


# Recruiting


class _NestedUpdatedAtStream(IncrementalPersonioStream):
    """Recruiting applications and jobs return `updated_at` as
    `{"date-time": ..., "timezone": ...}`; it is lifted to `updated_at_date_time`."""

    replication_key = "updated_at_date_time"

    def replication_value(self, row: dict) -> str | None:
        row["updated_at_date_time"] = (row.get("updated_at") or {}).get("date-time")
        return row["updated_at_date_time"]


class RecruitingApplicationsStream(_NestedUpdatedAtStream):
    name = "recruiting_applications"
    path = "/v2/recruiting/applications"
    primary_keys = ["id"]
    page_size = 200
    # `updated_at.gt` and `updated_at.lt` are mutually exclusive on recruiting endpoints.
    lower_bound_param = "updated_at.gt"
    lower_bound_exclusive = True
    upper_bound_param = None

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"application_id": record["id"]}


class RecruitingApplicationStageTransitionsStream(PersonioStream):
    """Stage history, re-read whenever its application changed. Personio gives
    transitions no id; identity is the application and the entry time."""

    name = "recruiting_application_stage_transitions"
    parent_stream_type = RecruitingApplicationsStream
    path = "/v2/recruiting/applications/{application_id}/stage-transitions"
    primary_keys = ["application_id", "entered_at"]

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["application_id"] = context["application_id"]
        return row


class RecruitingCandidatesStream(IncrementalPersonioStream):
    name = "recruiting_candidates"
    path = "/v2/recruiting/candidates"
    primary_keys = ["id"]
    replication_key = "updated_at"
    page_size = 200
    lower_bound_param = "updated_at.gt"
    lower_bound_exclusive = True
    upper_bound_param = None


class RecruitingJobsStream(_NestedUpdatedAtStream):
    name = "recruiting_jobs"
    path = "/v2/recruiting/jobs"
    primary_keys = ["id"]
    page_size = 200
    lower_bound_param = "updated_at.gt"
    lower_bound_exclusive = True
    upper_bound_param = None


class RecruitingCategoriesStream(PersonioStream):
    name = "recruiting_categories"
    path = "/v2/recruiting/categories"
    primary_keys = ["id"]


STREAM_TYPES = [
    PersonsStream,
    EmploymentsStream,
    DocumentsStream,
    CompensationsStream,
    CompensationTypesStream,
    AbsenceTypesStream,
    AbsencePeriodsStream,
    AbsencePeriodBreakdownsStream,
    AttendancePeriodsStream,
    ProjectsStream,
    ProjectMembersStream,
    OrgUnitsStream,
    LegalEntitiesStream,
    CostCentersStream,
    WorkplacesStream,
    JobsStream,
    SalaryBandsStream,
    ReportsStream,
    ReportAttributesStream,
    RecruitingApplicationsStream,
    RecruitingApplicationStageTransitionsStream,
    RecruitingCandidatesStream,
    RecruitingJobsStream,
    RecruitingCategoriesStream,
]
