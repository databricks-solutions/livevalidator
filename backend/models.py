"""Pydantic models for request/response validation."""

from typing import Optional, Literal
from pydantic import BaseModel, Field, field_validator
from zoneinfo import ZoneInfo


# ---------- Tables ----------
class TableIn(BaseModel):
    name: str
    src_system_id: int
    src_schema: Optional[str] = None
    src_table: Optional[str] = None
    tgt_system_id: int
    tgt_schema: Optional[str] = None
    tgt_table: Optional[str] = None
    compare_mode: Literal['except_all','primary_key','hash'] = 'except_all'
    pk_columns: Optional[list[str]] = None
    watermark_column: Optional[str] = None
    include_columns: list[str] = Field(default_factory=list)
    exclude_columns: list[str] = Field(default_factory=list)
    options: dict = Field(default_factory=dict)
    is_active: bool = True
    updated_by: str


class TableUpdate(BaseModel):
    name: Optional[str] = None
    src_system_id: Optional[int] = None
    src_schema: Optional[str] = None
    src_table: Optional[str] = None
    tgt_system_id: Optional[int] = None
    tgt_schema: Optional[str] = None
    tgt_table: Optional[str] = None
    compare_mode: Optional[Literal['except_all','primary_key','hash']] = None
    pk_columns: Optional[list[str]] = None
    watermark_column: Optional[str] = None
    include_columns: Optional[list[str]] = None
    exclude_columns: Optional[list[str]] = None
    options: Optional[dict] = None
    is_active: Optional[bool] = None
    updated_by: str
    version: int


class BulkTableItem(BaseModel):
    name: Optional[str] = None
    src_schema: str
    src_table: str
    tgt_schema: Optional[str] = None
    tgt_table: Optional[str] = None
    schedule_name: str
    compare_mode: Optional[Literal['except_all','primary_key','hash']] = 'except_all'
    pk_columns: Optional[list[str]] = None
    watermark_column: Optional[str] = None
    include_columns: Optional[list[str]] = None
    exclude_columns: Optional[list[str]] = None
    is_active: Optional[bool] = True


class BulkTableRequest(BaseModel):
    src_system_id: int
    tgt_system_id: int
    items: list[BulkTableItem]
    updated_by: str


# ---------- Queries ----------
class QueryIn(BaseModel):
    name: str
    src_system_id: int
    tgt_system_id: int
    sql: str
    compare_mode: Literal['except_all','primary_key','hash'] = 'except_all'
    pk_columns: Optional[list[str]] = None
    options: dict = Field(default_factory=dict)
    is_active: bool = True
    updated_by: str


class QueryUpdate(BaseModel):
    name: Optional[str] = None
    src_system_id: Optional[int] = None
    tgt_system_id: Optional[int] = None
    sql: Optional[str] = None
    compare_mode: Optional[Literal['except_all','primary_key','hash']] = None
    pk_columns: Optional[list[str]] = None
    options: Optional[dict] = None
    is_active: Optional[bool] = None
    updated_by: str
    version: int


class BulkQueryItem(BaseModel):
    name: Optional[str] = None
    sql: str
    schedule_name: str
    compare_mode: Optional[Literal['except_all','primary_key','hash']] = 'except_all'
    pk_columns: Optional[list[str]] = None
    is_active: Optional[bool] = True


class BulkQueryRequest(BaseModel):
    src_system_id: int
    tgt_system_id: int
    items: list[BulkQueryItem]
    updated_by: str


# ---------- Schedules ----------
class ScheduleIn(BaseModel):
    name: str
    cron_expr: str
    timezone: str = 'UTC'
    enabled: bool = True
    max_concurrency: int = 4
    backfill_policy: Literal['none','catch_up','skip_missed'] = 'none'
    updated_by: str
    
    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate timezone is a valid IANA timezone"""
        try:
            ZoneInfo(v)
            return v
        except Exception:
            raise ValueError(f"Invalid timezone '{v}'. Must be a valid IANA timezone (e.g., 'America/New_York', 'Europe/London', 'UTC')")


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    cron_expr: Optional[str] = None
    timezone: Optional[str] = None
    enabled: Optional[bool] = None
    max_concurrency: Optional[int] = None
    backfill_policy: Optional[Literal['none','catch_up','skip_missed']] = None
    last_run_at: Optional[str] = None
    next_run_at: Optional[str] = None
    updated_by: str
    version: int
    
    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone is a valid IANA timezone"""
        if v is None:
            return v
        try:
            ZoneInfo(v)
            return v
        except Exception:
            raise ValueError(f"Invalid timezone '{v}'. Must be a valid IANA timezone (e.g., 'America/New_York', 'Europe/London', 'UTC')")


class BindingIn(BaseModel):
    schedule_id: int
    entity_type: Literal['table', 'compare_query']
    entity_id: int


# ---------- Triggers ----------
class TriggerIn(BaseModel):
    source: Literal['manual', 'schedule', 'bulk_job'] = 'manual'
    schedule_id: Optional[int] = None
    entity_type: Literal['table', 'compare_query']
    entity_id: int
    requested_by: str
    priority: int = 100
    params: dict = Field(default_factory=dict)


# ---------- Systems ----------
class SystemIn(BaseModel):
    name: str
    kind: str
    catalog: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user_secret_key: Optional[str] = None
    pass_secret_key: Optional[str] = None
    jdbc_string: Optional[str] = None
    concurrency: int = -1
    options: dict = Field(default_factory=dict)
    is_active: bool = True
    updated_by: str


class SystemUpdate(BaseModel):
    name: Optional[str] = None
    kind: Optional[str] = None
    catalog: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user_secret_key: Optional[str] = None
    pass_secret_key: Optional[str] = None
    jdbc_string: Optional[str] = None
    concurrency: Optional[int] = None
    options: Optional[dict] = None
    is_active: Optional[bool] = None
    updated_by: str
    version: int


# ---------- Type Transformations ----------
class TypeTransformationIn(BaseModel):
    system_a_id: int
    system_b_id: int
    system_a_function: str
    system_b_function: str
    updated_by: str


class TypeTransformationUpdate(BaseModel):
    system_a_function: Optional[str] = None
    system_b_function: Optional[str] = None
    updated_by: str
    version: int


class ValidatePythonCode(BaseModel):
    code: str
