"""Pydantic models for request/response validation."""

from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator


# ---------- Tables ----------
class TableIn(BaseModel):
    name: str
    src_system_id: int
    src_schema: str
    src_table: str
    tgt_system_id: int
    tgt_schema: str | None = None
    tgt_table: str | None = None
    compare_mode: Literal["except_all", "primary_key", "hash"] = "except_all"
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    include_columns: list[str] = Field(default_factory=list)
    exclude_columns: list[str] = Field(default_factory=list)
    options: dict = Field(default_factory=dict)
    config_overrides: dict | None = None
    is_active: bool = True

    @field_validator("name", "src_schema", "src_table")
    @classmethod
    def not_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty")
        return v.strip()


class TableUpdate(BaseModel):
    name: str | None = None
    src_system_id: int | None = None
    src_schema: str | None = None
    src_table: str | None = None
    tgt_system_id: int | None = None
    tgt_schema: str | None = None
    tgt_table: str | None = None
    compare_mode: Literal["except_all", "primary_key", "hash"] | None = None
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    include_columns: list[str] | None = None
    exclude_columns: list[str] | None = None
    options: dict | None = None
    config_overrides: dict | None = None
    is_active: bool | None = None
    version: int


class BulkTableItem(BaseModel):
    name: str | None = None
    src_schema: str
    src_table: str
    tgt_schema: str | None = None
    tgt_table: str | None = None
    schedule_name: str | None = None
    compare_mode: Literal["except_all", "primary_key", "hash"] | None = "except_all"
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    include_columns: list[str] | None = None
    exclude_columns: list[str] | None = None
    config_overrides: dict | None = None
    is_active: bool | None = True
    tags: list[str] | None = None
    src_system_name: str | None = None
    tgt_system_name: str | None = None


class BulkTableRequest(BaseModel):
    src_system_id: int
    tgt_system_id: int
    items: list[BulkTableItem]


# ---------- Queries ----------
class QueryIn(BaseModel):
    name: str
    src_system_id: int
    tgt_system_id: int
    sql: str
    compare_mode: Literal["except_all", "primary_key", "hash"] = "except_all"
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    options: dict = Field(default_factory=dict)
    config_overrides: dict | None = None
    is_active: bool = True

    @field_validator("name", "sql")
    @classmethod
    def not_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty")
        return v.strip() if info.field_name == "name" else v


class QueryUpdate(BaseModel):
    name: str | None = None
    src_system_id: int | None = None
    tgt_system_id: int | None = None
    sql: str | None = None
    compare_mode: Literal["except_all", "primary_key", "hash"] | None = None
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    options: dict | None = None
    config_overrides: dict | None = None
    is_active: bool | None = None
    version: int


class BulkQueryItem(BaseModel):
    name: str | None = None
    sql: str
    schedule_name: str | None = None
    compare_mode: Literal["except_all", "primary_key", "hash"] | None = "except_all"
    pk_columns: list[str] | None = None
    watermark_filter: str | None = None
    config_overrides: dict | None = None
    is_active: bool | None = True
    tags: list[str] | None = None
    src_system_name: str | None = None
    tgt_system_name: str | None = None


class BulkQueryRequest(BaseModel):
    src_system_id: int
    tgt_system_id: int
    items: list[BulkQueryItem]


# ---------- Schedules ----------
class ScheduleIn(BaseModel):
    name: str
    cron_expr: str
    timezone: str = "UTC"
    enabled: bool = True
    max_concurrency: int = 4
    backfill_policy: Literal["none", "catch_up", "skip_missed"] = "none"

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate timezone is a valid IANA timezone"""
        try:
            ZoneInfo(v)
            return v
        except Exception:
            raise ValueError(
                f"Invalid timezone '{v}'. Must be a valid IANA timezone (e.g., 'America/New_York', 'Europe/London', 'UTC')"
            ) from None


class ScheduleUpdate(BaseModel):
    name: str | None = None
    cron_expr: str | None = None
    timezone: str | None = None
    enabled: bool | None = None
    max_concurrency: int | None = None
    backfill_policy: Literal["none", "catch_up", "skip_missed"] | None = None
    last_run_at: str | None = None
    next_run_at: str | None = None
    version: int

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str | None) -> str | None:
        """Validate timezone is a valid IANA timezone"""
        if v is None:
            return v
        try:
            ZoneInfo(v)
            return v
        except Exception:
            raise ValueError(
                f"Invalid timezone '{v}'. Must be a valid IANA timezone (e.g., 'America/New_York', 'Europe/London', 'UTC')"
            ) from None


class BindingIn(BaseModel):
    schedule_id: int
    entity_type: Literal["table", "compare_query"]
    entity_id: int


# ---------- Triggers ----------
class TriggerIn(BaseModel):
    source: Literal["manual", "schedule", "bulk_job", "notebook"] = "manual"
    schedule_id: int | None = None
    entity_type: Literal["table", "compare_query"]
    entity_id: int
    requested_by: str | None = None
    priority: int = 100
    params: dict = Field(default_factory=dict)


class BulkRepairRequest(BaseModel):
    trigger_ids: list[int]


class BulkTriggerRequest(BaseModel):
    entity_type: str
    entity_ids: list[int]


# ---------- Systems ----------
class SystemIn(BaseModel):
    name: str
    kind: str
    catalog: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    secret_scope: str | None = "livevalidator"
    user_secret_key: str | None = None
    pass_secret_key: str | None = None
    jdbc_string: str | None = None
    driver_connector: str | None = None
    concurrency: int = -1
    max_rows: int | None = None
    options: dict = Field(default_factory=dict)
    is_active: bool = True


class SystemUpdate(BaseModel):
    name: str | None = None
    kind: str | None = None
    catalog: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    secret_scope: str | None = None
    user_secret_key: str | None = None
    pass_secret_key: str | None = None
    jdbc_string: str | None = None
    driver_connector: str | None = None
    concurrency: int | None = None
    max_rows: int | None = None
    options: dict | None = None
    is_active: bool | None = None
    version: int


# ---------- Type Transformations ----------
class TypeTransformationIn(BaseModel):
    system_a_id: int
    system_b_id: int
    system_a_function: str
    system_b_function: str


class TypeTransformationUpdate(BaseModel):
    system_a_function: str | None = None
    system_b_function: str | None = None
    version: int


class ValidatePythonCode(BaseModel):
    code: str


# ---------- Dashboards ----------
class DashboardIn(BaseModel):
    name: str
    project: str = "General"

    @field_validator("name")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()


class DashboardUpdate(BaseModel):
    name: str | None = None
    project: str | None = None
    time_range_preset: str | None = None
    time_range_from: str | None = None
    time_range_to: str | None = None
    version: int


class ChartIn(BaseModel):
    name: str
    filters: dict = Field(default_factory=dict)
    sort_order: int = 0

    @field_validator("name")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()


class ChartUpdate(BaseModel):
    name: str | None = None
    filters: dict | None = None
    sort_order: int | None = None


class ChartReorder(BaseModel):
    chart_ids: list[int]
