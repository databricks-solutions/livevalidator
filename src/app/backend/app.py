import os
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import available_timezones
from contextvars import ContextVar

from fastapi import FastAPI, APIRouter, HTTPException, Response, Request
from databricks.sdk import WorkspaceClient
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import asyncpg

from backend.db import fetch, fetchrow, fetchval, execute
from backend.models import (
    TableIn, TableUpdate, BulkTableItem, BulkTableRequest,
    QueryIn, QueryUpdate, BulkQueryItem, BulkQueryRequest,
    ScheduleIn, ScheduleUpdate, BindingIn,
    TriggerIn, BulkRepairRequest, BulkTriggerRequest, SystemIn, SystemUpdate,
    TypeTransformationIn, TypeTransformationUpdate, ValidatePythonCode,
    DashboardIn, DashboardUpdate, ChartIn, ChartUpdate, ChartReorder
)
from backend.default_transformations import get_default_transformation

def serialize_row(row) -> dict:
    """Convert a database row to a JSON-serializable dict (handles datetime)."""
    if row is None:
        return None
    result = dict(row)
    for k, v in result.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
    return result


# ---------- Job Launch Helpers ----------

async def get_enriched_trigger(trigger_id: int) -> dict | None:
    """
    Get trigger with full entity details for job launch.
    Returns enriched dict with all fields needed by validation job.
    """
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
    if not trigger:
        return None
    
    # Fetch entity details
    if trigger['entity_type'] == 'table':
        entity = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", trigger['entity_id'])
    else:
        entity = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", trigger['entity_id'])
    
    if not entity:
        return None
    
    # Fetch system names
    src_system = await fetchrow("SELECT * FROM control.systems WHERE id=$1", entity['src_system_id'])
    tgt_system = await fetchrow("SELECT * FROM control.systems WHERE id=$1", entity['tgt_system_id'])
    
    # Build enriched result
    result = dict(entity)
    result["id"] = trigger["id"]
    result["entity_type"] = trigger["entity_type"]
    result["entity_id"] = trigger["entity_id"]
    result["src_system_id"] = entity["src_system_id"]
    result["tgt_system_id"] = entity["tgt_system_id"]
    if trigger["entity_type"] == 'table':
        result["source_table"] = f"{entity['src_schema'].strip()}.{entity['src_table'].strip()}"
        result["target_table"] = f"{entity['tgt_schema'].strip()}.{entity['tgt_table'].strip()}"
    result["watermark_expr"] = entity.get('watermark_filter', '')
    result["src_system_name"] = src_system["name"] if src_system else "unknown"
    result["tgt_system_name"] = tgt_system["name"] if tgt_system else "unknown"
    result["config_overrides"] = entity.get("config_overrides")
    
    return result


async def check_system_concurrency(src_system_id: int, tgt_system_id: int) -> tuple[bool, str]:
    """
    Check if job can be launched based on system concurrency limits.
    Each system is checked independently against its own limit.
    Returns (can_launch, reason).
    """
    # Get system concurrency limits
    src_system = await fetchrow("SELECT name, concurrency FROM control.systems WHERE id=$1", src_system_id)
    tgt_system = await fetchrow("SELECT name, concurrency FROM control.systems WHERE id=$1", tgt_system_id)
    
    src_limit = src_system["concurrency"] if src_system else -1
    tgt_limit = tgt_system["concurrency"] if tgt_system else -1
    
    # If both unlimited, no need to check
    if src_limit == -1 and tgt_limit == -1:
        return True, ""
    
    # Get running counts per system
    rows = await fetch("""
        WITH running_tables AS (
            SELECT d.src_system_id, d.tgt_system_id
            FROM control.triggers t
            JOIN control.datasets d ON t.entity_id = d.id
            WHERE t.status = 'running' AND t.entity_type = 'table'
        ),
        running_queries AS (
            SELECT q.src_system_id, q.tgt_system_id
            FROM control.triggers t
            JOIN control.compare_queries q ON t.entity_id = q.id
            WHERE t.status = 'running' AND t.entity_type = 'compare_query'
        ),
        all_running AS (
            SELECT src_system_id as system_id FROM running_tables
            UNION ALL SELECT tgt_system_id FROM running_tables
            UNION ALL SELECT src_system_id FROM running_queries
            UNION ALL SELECT tgt_system_id FROM running_queries
        )
        SELECT system_id, COUNT(*) as count FROM all_running
        WHERE system_id IN ($1, $2) GROUP BY system_id
    """, src_system_id, tgt_system_id)
    
    running_counts = {row['system_id']: int(row['count']) for row in rows}
    
    # Check each system independently against its own limit
    src_running = running_counts.get(src_system_id, 0)
    tgt_running = running_counts.get(tgt_system_id, 0)
    
    if src_limit != -1 and src_running >= src_limit:
        src_name = src_system["name"] if src_system else f"System {src_system_id}"
        return False, f"{src_name} at capacity ({src_running}/{src_limit})"
    
    if tgt_limit != -1 and tgt_running >= tgt_limit:
        tgt_name = tgt_system["name"] if tgt_system else f"System {tgt_system_id}"
        return False, f"{tgt_name} at capacity ({tgt_running}/{tgt_limit})"
    
    return True, ""


async def launch_validation_job(trigger_id: int) -> dict:
    """
    Launch a Databricks validation job for the given trigger.
    Returns dict with run_id and run_url on success.
    Raises HTTPException on failure.
    """
    enriched = await get_enriched_trigger(trigger_id)
    if not enriched:
        raise HTTPException(status_code=404, detail="Trigger or entity not found")
    
    # Fetch validation config and apply overrides
    config_row = await fetchrow("SELECT * FROM control.validation_config WHERE id = 1")
    resolved_config = dict(config_row) if config_row else {
        "downgrade_unicode": False,
        "replace_special_char": [],
        "extra_replace_regex": ""
    }
    if enriched.get("config_overrides"):
        resolved_config.update(enriched["config_overrides"])
    
    # Build job parameters (same as job_sentinel)
    is_table = enriched["entity_type"] == "table"
    params = {
        "trigger_id": str(trigger_id),
        "name": enriched.get("name", ""),
        "source_system_name": str(enriched["src_system_name"]),
        "target_system_name": str(enriched["tgt_system_name"]),
        "backend_api_url": os.environ.get("DATABRICKS_APP_URL", ""),
        "source_table": enriched.get("source_table", "") if is_table else "",
        "target_table": enriched.get("target_table", "") if is_table else "",
        "sql": enriched.get("sql", "") if not is_table else "",
        "watermark_expr": enriched.get("watermark_expr", "") or "",
        "compare_mode": enriched.get("compare_mode", "except_all"),
        "pk_columns": json.dumps(enriched.get("pk_columns") or []),
        "include_columns": json.dumps(enriched.get("include_columns") or []),
        "exclude_columns": json.dumps(enriched.get("exclude_columns") or []),
        "options": json.dumps(enriched.get("options") or {}),
        "downgrade_unicode": str(resolved_config.get("downgrade_unicode", False)).lower(),
        "replace_special_char": json.dumps(resolved_config.get("replace_special_char", [])),
        "extra_replace_regex": resolved_config.get("extra_replace_regex", "")
    }
    
    # Get job ID from environment
    job_id = os.environ.get("VALIDATION_JOB_ID")
    if not job_id:
        raise HTTPException(status_code=500, detail="VALIDATION_JOB_ID not configured")
    
    try:
        # WorkspaceClient auto-picks up app service principal creds
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=int(job_id), job_parameters=params)
        run_url = f"{w.config.host}/jobs/{job_id}/runs/{run.run_id}"
        
        # Update trigger with run info
        await execute("""
            UPDATE control.triggers 
            SET status = 'running',
                started_at = now(),
                databricks_run_id = $2,
                databricks_run_url = $3
            WHERE id = $1
        """, trigger_id, str(run.run_id), run_url)
        
        return {"run_id": run.run_id, "run_url": run_url}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to launch job: {str(e)}")


app = FastAPI(title="LiveValidator Control Plane API", version="0.1")

# Keep API isolated under /api so SPA routing can own "/"
api = APIRouter(prefix="/api")

# Context var to store current user email
_current_user_email: ContextVar[str] = ContextVar("current_user_email", default="system")

# Middleware to extract user email from headers and auto-create user entries
@app.middleware("http")
async def user_email_middleware(request: Request, call_next):
    # For Databricks: use x-forwarded-email header
    # For local dev: default to local-admin@localhost
    email = request.headers.get("x-forwarded-email", "local-admin@localhost")
    _current_user_email.set(email)
    
    # Auto-create user entry with default role if not exists
    # (Skip for non-API routes like static files and admin endpoints)
    if request.url.path.startswith("/api") and not request.url.path.startswith("/api/admin"):
        try:
            await ensure_user_exists(email)
        except Exception as e:
            # Don't block requests if user creation fails
            print(f"[warn] Failed to auto-create user {email}: {e}")
    
    response = await call_next(request)
    return response

# If frontend and API are same-origin in prod, you can tighten allow_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Global Exception Handlers ----------
@app.exception_handler(asyncpg.exceptions.UndefinedTableError)
async def handle_undefined_table(request: Request, exc: asyncpg.exceptions.UndefinedTableError):
    """Catch database table not found errors and direct user to setup."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database not initialized",
            "action": "setup_required",
            "message": "Please go to the Setup tab and click 'Initialize Database'"
        }
    )

@app.exception_handler(asyncpg.exceptions.UndefinedObjectError)
async def handle_undefined_object(request: Request, exc: asyncpg.exceptions.UndefinedObjectError):
    """Catch missing role/object errors (e.g., 'role apprunner does not exist')."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database role not configured",
            "action": "setup_required",
            "message": f"Database setup required: {exc}. Please ensure the database role exists (run grants.sql)."
        }
    )

@app.exception_handler(asyncpg.exceptions.InvalidCatalogNameError)
async def handle_invalid_catalog(request: Request, exc: asyncpg.exceptions.InvalidCatalogNameError):
    """Catch database doesn't exist errors."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database not found",
            "action": "setup_required",
            "message": f"Database setup required: {exc}. Please ensure the database exists."
        }
    )

@app.exception_handler(asyncpg.exceptions.InvalidPasswordError)
async def handle_invalid_password(request: Request, exc: asyncpg.exceptions.InvalidPasswordError):
    """Catch authentication errors."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database authentication failed",
            "action": "setup_required",
            "message": "Database authentication failed. Please check DB_DSN credentials."
        }
    )

@app.exception_handler(asyncpg.exceptions.CannotConnectNowError)
async def handle_cannot_connect(request: Request, exc: asyncpg.exceptions.CannotConnectNowError):
    """Catch connection errors."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Cannot connect to database",
            "action": "setup_required", 
            "message": f"Cannot connect to database: {exc}. Please check database availability."
        }
    )

@app.exception_handler(asyncpg.exceptions.PostgresConnectionError)
async def handle_postgres_connection_error(request: Request, exc: asyncpg.exceptions.PostgresConnectionError):
    """Catch-all for postgres connection errors."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database connection error",
            "action": "setup_required",
            "message": f"Database connection error: {exc}. Please check database configuration."
        }
    )

@app.exception_handler(OSError)
async def handle_os_error(request: Request, exc: OSError):
    """Catch network/OS errors (connection refused, host unreachable, etc.)."""
    if "Connect call failed" in str(exc) or "Connection refused" in str(exc):
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Cannot reach database server",
                "action": "setup_required",
                "message": f"Cannot reach database server: {exc}. Please check network connectivity and database host."
            }
        )
    raise exc  # Re-raise if not a connection error

@app.exception_handler(asyncpg.exceptions.UniqueViolationError)
async def handle_unique_violation(request: Request, exc: asyncpg.exceptions.UniqueViolationError):
    """Catch duplicate key errors and return friendly message."""
    detail = str(exc)
    # Extract the constraint name and value for a cleaner message
    if "already exists" in detail:
        return JSONResponse(
            status_code=409,
            content={
                "detail": "A record with this name already exists",
                "error": "duplicate_name",
                "message": detail
            }
        )
    return JSONResponse(
        status_code=409,
        content={"detail": "Duplicate record", "message": detail}
    )

@app.exception_handler(asyncpg.exceptions.ForeignKeyViolationError)
async def handle_foreign_key_violation(request: Request, exc: asyncpg.exceptions.ForeignKeyViolationError):
    """Catch foreign key errors (e.g., invalid system ID)."""
    detail = str(exc)
    return JSONResponse(
        status_code=400,
        content={
            "detail": "Invalid reference",
            "error": "invalid_foreign_key",
            "message": "One or more referenced records do not exist (e.g., system ID)"
        }
    )

# ---------- Helpers ----------
def get_user_email() -> str:
    """Get current user email from context (set by middleware)"""
    return _current_user_email.get()


async def get_default_user_role() -> str:
    """Get the default user role from app config"""
    row = await fetchrow("SELECT value FROM control.app_config WHERE key = 'default_user_role'")
    return row['value'] if row else 'CAN_MANAGE'


async def ensure_user_exists(email: str):
    """Ensure user exists in user_roles table, create with default role if not"""
    exists = await fetchrow("SELECT 1 FROM control.user_roles WHERE user_email = $1", email)
    if not exists:
        default_role = await get_default_user_role()
        await execute("""
            INSERT INTO control.user_roles (user_email, role, assigned_by, assigned_at)
            VALUES ($1, $2, 'system', NOW())
            ON CONFLICT (user_email) DO NOTHING
        """, email, default_role)


async def get_user_role(email: str) -> str:
    """Get user role, uses default from config if user not in table"""
    row = await fetchrow("SELECT role FROM control.user_roles WHERE user_email = $1", email)
    if row:
        return row['role']
    # Fallback (should rarely happen since middleware auto-creates)
    return await get_default_user_role()


async def can_edit_object(email: str, object_type: str, object_id: int) -> bool:
    """
    Check if user can edit specific object based on their role and ownership.
    
    Rules:
    - CAN_VIEW: Cannot edit anything
    - CAN_RUN: Can edit tables/queries/schedules they created
    - CAN_EDIT: Can edit any table/query/schedule (but not systems/type_transformations)
    - CAN_MANAGE: Can edit everything
    """
    role = await get_user_role(email)
    
    # CAN_VIEW can't edit anything
    if role == 'CAN_VIEW':
        return False
    
    # CAN_MANAGE can edit everything
    if role == 'CAN_MANAGE':
        return True
    
    # CAN_EDIT can edit tables/queries/schedules but not systems/type_transformations
    if role == 'CAN_EDIT':
        return object_type in ['tables', 'queries', 'schedules']
    
    # CAN_RUN can only edit their own creations
    if role == 'CAN_RUN':
        if object_type not in ['tables', 'queries', 'schedules']:
            return False
        
        # Check if they're the creator
        table_map = {
            'tables': 'datasets',
            'queries': 'compare_queries',
            'schedules': 'schedules'
        }
        db_table = table_map.get(object_type)
        if not db_table:
            return False
            
        row = await fetchrow(f"SELECT created_by FROM control.{db_table} WHERE id = $1", object_id)
        return row and row['created_by'] == email
    
    return False


async def require_role(*allowed_roles: str):
    """Check if user has one of the allowed roles, raise 403 if not"""
    email = get_user_email()
    role = await get_user_role(email)
    if role not in allowed_roles:
        raise HTTPException(
            403, 
            f"Access denied. This action requires one of these roles: {', '.join(allowed_roles)}. Your role: {role}"
        )


async def row_or_404(sql: str, *args):
    row = await fetchrow(sql, *args)
    if not row:
        raise HTTPException(404, "not found")
    return dict(row)


@api.get("/secrets")
async def question():
    return (os.environ.get("DATABRICKS_CLIENT_ID"), os.environ.get("DATABRICKS_CLIENT_SECRET"))


@api.get("/current_user")
async def get_current_user():
    """Get the current authenticated user's email and role"""
    # Get email from context (set by middleware)
    email = get_user_email()
    
    # Look up role using shared function
    role = await get_user_role(email)
    
    return {"email": email, "role": role}

# ---------- Tables ----------
@api.get("/tables")
async def list_tables(q: Optional[str] = None):
    if q:
        rows = await fetch("""
            SELECT 
                d.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                vh.error_message as last_run_error,
                vh.row_count_match as last_run_row_count_match,
                vh.rows_different as last_run_rows_different,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'table' AND et.entity_id = d.id),
                    '[]'::json
                ) as tags,
                COALESCE(
                    (SELECT json_agg(s.name ORDER BY s.name)
                     FROM control.schedule_bindings sb
                     JOIN control.schedules s ON sb.schedule_id = s.id
                     WHERE sb.entity_type = 'table' AND sb.entity_id = d.id),
                    '[]'::json
                ) as schedules
            FROM control.datasets d
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at, error_message, row_count_match, rows_different
                FROM control.validation_history
                WHERE entity_type = 'table' AND entity_id = d.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE (d.name ILIKE $1 OR $1 = '')
            ORDER BY d.name
        """, f"%{q}%")
    else:
        rows = await fetch("""
            SELECT 
                d.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                vh.error_message as last_run_error,
                vh.row_count_match as last_run_row_count_match,
                vh.rows_different as last_run_rows_different,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'table' AND et.entity_id = d.id),
                    '[]'::json
                ) as tags,
                COALESCE(
                    (SELECT json_agg(s.name ORDER BY s.name)
                     FROM control.schedule_bindings sb
                     JOIN control.schedules s ON sb.schedule_id = s.id
                     WHERE sb.entity_type = 'table' AND sb.entity_id = d.id),
                    '[]'::json
                ) as schedules
            FROM control.datasets d
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at, error_message, row_count_match, rows_different
                FROM control.validation_history
                WHERE entity_type = 'table' AND entity_id = d.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            ORDER BY d.name
        """)
    return [dict(r) for r in rows]

@api.post("/tables")
async def create_table(body: TableIn):
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    # Validate systems exist
    src_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.src_system_id)
    if not src_sys:
        raise HTTPException(400, f"Source system ID {body.src_system_id} does not exist")
    tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.tgt_system_id)
    if not tgt_sys:
        raise HTTPException(400, f"Target system ID {body.tgt_system_id} does not exist")
    
    # Check for duplicate name
    existing = await fetchrow("SELECT id FROM control.datasets WHERE name = $1", body.name)
    if existing:
        raise HTTPException(409, f"A table with name '{body.name}' already exists")
    
    row = await fetchrow("""
        INSERT INTO control.datasets (
          name, src_system_id, src_schema, src_table,
          tgt_system_id, tgt_schema, tgt_table,
          compare_mode, pk_columns, watermark_filter, include_columns, exclude_columns,
          options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$11,$12, $13,$14,$15,$15
        ) RETURNING *
    """,
    body.name, body.src_system_id, body.src_schema, body.src_table,
    body.tgt_system_id, body.tgt_schema, body.tgt_table,
    body.compare_mode, body.pk_columns, body.watermark_filter, body.include_columns, body.exclude_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, user_email)
    return dict(row)

@api.get("/tables/{id}")
async def get_table(id: int):
    return await row_or_404("SELECT * FROM control.datasets WHERE id=$1", id)

@api.put("/tables/{id}")
async def update_table(id: int, body: TableUpdate):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'tables', id):
        raise HTTPException(403, "You don't have permission to edit this table")
    
    # If renaming, check for duplicate name (exclude current record)
    if body.name:
        existing = await fetchrow("SELECT id FROM control.datasets WHERE name = $1 AND id != $2", body.name, id)
        if existing:
            raise HTTPException(409, f"A table with name '{body.name}' already exists")
    
    # Validate systems exist if being changed
    if body.src_system_id:
        src_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.src_system_id)
        if not src_sys:
            raise HTTPException(400, f"Source system ID {body.src_system_id} does not exist")
    if body.tgt_system_id:
        tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.tgt_system_id)
        if not tgt_sys:
            raise HTTPException(400, f"Target system ID {body.tgt_system_id} does not exist")
    
    row = await fetchrow("""
        UPDATE control.datasets SET
          name = COALESCE($2, name),
          src_system_id = COALESCE($3, src_system_id),
          src_schema    = COALESCE($4, src_schema),
          src_table     = COALESCE($5, src_table),
          tgt_system_id = COALESCE($6, tgt_system_id),
          tgt_schema    = COALESCE($7, tgt_schema),
          tgt_table     = COALESCE($8, tgt_table),
          compare_mode  = COALESCE($9, compare_mode),
          pk_columns    = COALESCE($10, pk_columns),
          watermark_filter = COALESCE($11, watermark_filter),
          include_columns  = COALESCE($12, include_columns),
          exclude_columns  = COALESCE($13, exclude_columns),
          options = COALESCE($14, options),
          is_active = COALESCE($15, is_active),
          updated_by = $16,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$17
        RETURNING *
    """,
    id, body.name, body.src_system_id, body.src_schema, body.src_table,
    body.tgt_system_id, body.tgt_schema, body.tgt_table,
    body.compare_mode, body.pk_columns, body.watermark_filter, body.include_columns, body.exclude_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, user_email, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/tables/{id}")
async def delete_table(id: int):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'tables', id):
        raise HTTPException(403, "You don't have permission to delete this table")
    
    await execute("DELETE FROM control.datasets WHERE id=$1", id)
    return {"ok": True}

@api.post("/tables/bulk")
async def bulk_create_tables(body: BulkTableRequest):
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    results = {"created": [], "updated": [], "errors": []}
    
    for idx, item in enumerate(body.items):
        try:
            # Apply defaults
            name = item.name or f"{item.src_schema}.{item.src_table}"
            tgt_schema = item.tgt_schema or item.src_schema
            tgt_table = item.tgt_table or item.src_table
            
            # Resolve system IDs - per-row system names override request-level IDs
            src_system_id = body.src_system_id
            tgt_system_id = body.tgt_system_id
            
            if item.src_system_name:
                src_sys = await fetchrow("SELECT id FROM control.systems WHERE name = $1", item.src_system_name)
                if not src_sys:
                    raise ValueError(f"Source system '{item.src_system_name}' not found")
                src_system_id = src_sys['id']
            
            if item.tgt_system_name:
                tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE name = $1", item.tgt_system_name)
                if not tgt_sys:
                    raise ValueError(f"Target system '{item.tgt_system_name}' not found")
                tgt_system_id = tgt_sys['id']
            
            # Check if exists
            existing = await fetchrow("SELECT id, version FROM control.datasets WHERE name=$1", name)
            
            if existing:
                # Update existing
                row = await fetchrow("""
                    UPDATE control.datasets SET
                      src_system_id = $2,
                      src_schema = $3,
                      src_table = $4,
                      tgt_system_id = $5,
                      tgt_schema = $6,
                      tgt_table = $7,
                      compare_mode = $8,
                      pk_columns = $9,
                      watermark_filter = $10,
                      include_columns = $11,
                      exclude_columns = $12,
                      is_active = $13,
                      updated_by = $14,
                      updated_at = now(),
                      version = version + 1
                    WHERE id=$1
                    RETURNING *
                """,
                existing['id'], src_system_id, item.src_schema, item.src_table,
                tgt_system_id, tgt_schema, tgt_table,
                item.compare_mode, item.pk_columns, item.watermark_filter,
                item.include_columns or [], item.exclude_columns or [],
                item.is_active, user_email)
                results["updated"].append({"row": idx + 1, "name": name, "data": dict(row)})
            else:
                # Create new
                row = await fetchrow("""
                    INSERT INTO control.datasets (
                      name, src_system_id, src_schema, src_table,
                      tgt_system_id, tgt_schema, tgt_table,
                      compare_mode, pk_columns, watermark_filter,
                      include_columns, exclude_columns,
                      is_active, created_by, updated_by
                    ) VALUES (
                      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14
                    ) RETURNING *
                """,
                name, src_system_id, item.src_schema, item.src_table,
                tgt_system_id, tgt_schema, tgt_table,
                item.compare_mode, item.pk_columns, item.watermark_filter,
                item.include_columns or [], item.exclude_columns or [],
                item.is_active, user_email)
                
                # Bind to schedule (if provided)
                if item.schedule_name:
                    schedule = await fetchrow("SELECT id FROM control.schedules WHERE name=$1", item.schedule_name)
                    if schedule:
                        await execute("""
                            INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                            VALUES ($1, 'table', $2)
                            ON CONFLICT DO NOTHING
                        """, schedule['id'], row['id'])
                
                # Apply tags if provided
                if item.tags:
                    for tag_name in item.tags:
                        tag_name = tag_name.strip()
                        if not tag_name:
                            continue
                        # Get or create tag
                        tag = await fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
                        if not tag:
                            tag = await fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)
                        # Associate tag with entity
                        await execute("""
                            INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                            VALUES ('table', $1, $2)
                            ON CONFLICT DO NOTHING
                        """, row['id'], tag['id'])
                
                results["created"].append({"row": idx + 1, "name": name, "data": dict(row)})
        except Exception as e:
            results["errors"].append({"row": idx + 1, "error": str(e)})
    
    return results

# ---------- Compare Queries ----------
@api.get("/queries")
async def list_queries(q: Optional[str] = None):
    if q:
        rows = await fetch("""
            SELECT 
                cq.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                vh.error_message as last_run_error,
                vh.row_count_match as last_run_row_count_match,
                vh.rows_different as last_run_rows_different,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'query' AND et.entity_id = cq.id),
                    '[]'::json
                ) as tags,
                COALESCE(
                    (SELECT json_agg(s.name ORDER BY s.name)
                     FROM control.schedule_bindings sb
                     JOIN control.schedules s ON sb.schedule_id = s.id
                     WHERE sb.entity_type = 'compare_query' AND sb.entity_id = cq.id),
                    '[]'::json
                ) as schedules
            FROM control.compare_queries cq
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at, error_message, row_count_match, rows_different
                FROM control.validation_history
                WHERE entity_type = 'compare_query' AND entity_id = cq.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE cq.name ILIKE $1
            ORDER BY cq.name
        """, f"%{q}%")
    else:
        rows = await fetch("""
            SELECT 
                cq.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                vh.error_message as last_run_error,
                vh.row_count_match as last_run_row_count_match,
                vh.rows_different as last_run_rows_different,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'query' AND et.entity_id = cq.id),
                    '[]'::json
                ) as tags,
                COALESCE(
                    (SELECT json_agg(s.name ORDER BY s.name)
                     FROM control.schedule_bindings sb
                     JOIN control.schedules s ON sb.schedule_id = s.id
                     WHERE sb.entity_type = 'compare_query' AND sb.entity_id = cq.id),
                    '[]'::json
                ) as schedules
            FROM control.compare_queries cq
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at, error_message, row_count_match, rows_different
                FROM control.validation_history
                WHERE entity_type = 'compare_query' AND entity_id = cq.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            ORDER BY cq.name
        """)
    return [dict(r) for r in rows]

@api.post("/queries")
async def create_query(body: QueryIn):
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    # Validate systems exist
    src_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.src_system_id)
    if not src_sys:
        raise HTTPException(400, f"Source system ID {body.src_system_id} does not exist")
    tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.tgt_system_id)
    if not tgt_sys:
        raise HTTPException(400, f"Target system ID {body.tgt_system_id} does not exist")
    
    # Check for duplicate name
    existing = await fetchrow("SELECT id FROM control.compare_queries WHERE name = $1", body.name)
    if existing:
        raise HTTPException(409, f"A query with name '{body.name}' already exists")
    
    row = await fetchrow("""
        INSERT INTO control.compare_queries (
          name, src_system_id, tgt_system_id, sql,
          compare_mode, pk_columns, watermark_filter,
          options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$10
        ) RETURNING *
    """,
    body.name, body.src_system_id, body.tgt_system_id, body.sql,
    body.compare_mode, body.pk_columns, body.watermark_filter,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, user_email)
    return dict(row)

@api.get("/queries/{id}")
async def get_query(id: int):
    return await row_or_404("SELECT * FROM control.compare_queries WHERE id=$1", id)

@api.put("/queries/{id}")
async def update_query(id: int, body: QueryUpdate):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'queries', id):
        raise HTTPException(403, "You don't have permission to edit this query")
    
    # If renaming, check for duplicate name (exclude current record)
    if body.name:
        existing = await fetchrow("SELECT id FROM control.compare_queries WHERE name = $1 AND id != $2", body.name, id)
        if existing:
            raise HTTPException(409, f"A query with name '{body.name}' already exists")
    
    # Validate systems exist if being changed
    if body.src_system_id:
        src_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.src_system_id)
        if not src_sys:
            raise HTTPException(400, f"Source system ID {body.src_system_id} does not exist")
    if body.tgt_system_id:
        tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE id = $1", body.tgt_system_id)
        if not tgt_sys:
            raise HTTPException(400, f"Target system ID {body.tgt_system_id} does not exist")
    
    row = await fetchrow("""
        UPDATE control.compare_queries SET
          name = COALESCE($2, name),
          src_system_id = COALESCE($3, src_system_id),
          tgt_system_id = COALESCE($4, tgt_system_id),
          sql           = COALESCE($5, sql),
          compare_mode  = COALESCE($6, compare_mode),
          pk_columns    = COALESCE($7, pk_columns),
          watermark_filter = COALESCE($8, watermark_filter),
          options = COALESCE($9, options),
          is_active = COALESCE($10, is_active),
          updated_by = $11,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$12
        RETURNING *
    """,
    id, body.name, body.src_system_id, body.tgt_system_id, body.sql,
    body.compare_mode, body.pk_columns, body.watermark_filter,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, user_email, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/queries/{id}")
async def delete_query(id: int):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'queries', id):
        raise HTTPException(403, "You don't have permission to delete this query")
    
    await execute("DELETE FROM control.compare_queries WHERE id=$1", id)
    return {"ok": True}

@api.post("/queries/bulk")
async def bulk_create_queries(body: BulkQueryRequest):
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    results = {"created": [], "updated": [], "errors": []}
    
    for idx, item in enumerate(body.items):
        try:
            # Apply defaults
            name = item.name or f"Query {idx + 1}"
            
            # Resolve system IDs - per-row system names override request-level IDs
            src_system_id = body.src_system_id
            tgt_system_id = body.tgt_system_id
            
            if item.src_system_name:
                src_sys = await fetchrow("SELECT id FROM control.systems WHERE name = $1", item.src_system_name)
                if not src_sys:
                    raise ValueError(f"Source system '{item.src_system_name}' not found")
                src_system_id = src_sys['id']
            
            if item.tgt_system_name:
                tgt_sys = await fetchrow("SELECT id FROM control.systems WHERE name = $1", item.tgt_system_name)
                if not tgt_sys:
                    raise ValueError(f"Target system '{item.tgt_system_name}' not found")
                tgt_system_id = tgt_sys['id']
            
            # Check if exists
            existing = await fetchrow("SELECT id, version FROM control.compare_queries WHERE name=$1", name)
            
            if existing:
                # Update existing
                row = await fetchrow("""
                    UPDATE control.compare_queries SET
                      src_system_id = $2,
                      sql = $3,
                      tgt_system_id = $4,
                      compare_mode = $5,
                      pk_columns = $6,
                      watermark_filter = $7,
                      is_active = $8,
                      updated_by = $9,
                      updated_at = now(),
                      version = version + 1
                    WHERE id=$1
                    RETURNING *
                """,
                existing['id'], src_system_id, item.sql, tgt_system_id,
                item.compare_mode, item.pk_columns, item.watermark_filter,
                item.is_active, user_email)
                results["updated"].append({"row": idx + 1, "name": name, "data": dict(row)})
            else:
                # Create new
                row = await fetchrow("""
                    INSERT INTO control.compare_queries (
                      name, src_system_id, sql, tgt_system_id,
                      compare_mode, pk_columns, watermark_filter,
                      is_active, created_by, updated_by
                    ) VALUES (
                      $1,$2,$3,$4,$5,$6,$7,$8,$9,$9
                    ) RETURNING *
                """,
                name, src_system_id, item.sql, tgt_system_id,
                item.compare_mode, item.pk_columns, item.watermark_filter,
                item.is_active, user_email)
                
                # Bind to schedule (if provided)
                if item.schedule_name:
                    schedule = await fetchrow("SELECT id FROM control.schedules WHERE name=$1", item.schedule_name)
                    if schedule:
                        await execute("""
                            INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                            VALUES ($1, 'compare_query', $2)
                            ON CONFLICT DO NOTHING
                        """, schedule['id'], row['id'])
                
                # Apply tags if provided
                if item.tags:
                    for tag_name in item.tags:
                        tag_name = tag_name.strip()
                        if not tag_name:
                            continue
                        # Get or create tag
                        tag = await fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
                        if not tag:
                            tag = await fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)
                        # Associate tag with entity
                        await execute("""
                            INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                            VALUES ('query', $1, $2)
                            ON CONFLICT DO NOTHING
                        """, row['id'], tag['id'])
                
                results["created"].append({"row": idx + 1, "name": name, "data": dict(row)})
        except Exception as e:
            results["errors"].append({"row": idx + 1, "error": str(e)})
    
    return results

# ---------- Schedules & bindings ----------
@api.get("/timezones")
async def list_timezones():
    """
    Return common IANA timezones for use in schedule configuration.
    Filters to commonly used timezones for better UX.
    """
    # Get all available timezones and filter to common ones
    common_timezones = sorted([
        tz for tz in available_timezones()
        if '/' in tz  # Only include region/city format (excludes legacy aliases)
        and not tz.startswith('Etc/')  # Exclude Etc/* timezones
        and not tz.startswith('SystemV/')  # Exclude SystemV/* timezones
    ])
    
    # Add UTC at the beginning
    return ['UTC'] + common_timezones

@api.get("/schedules")
async def list_schedules():
    rows = await fetch("SELECT * FROM control.schedules ORDER BY name")
    return [dict(r) for r in rows]

@api.post("/schedules")
async def create_schedule(body: ScheduleIn):
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    row = await fetchrow("""
        INSERT INTO control.schedules (name, cron_expr, timezone, enabled, max_concurrency, backfill_policy, created_by, updated_by)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$7) RETURNING *
    """, body.name, body.cron_expr, body.timezone, body.enabled, body.max_concurrency, body.backfill_policy, user_email)
    return dict(row)

@api.put("/schedules/{id}")
async def update_schedule(id: int, body: ScheduleUpdate):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'schedules', id):
        raise HTTPException(403, "You don't have permission to edit this schedule")
    
    # Check if cron_expr or timezone changed - if so, reset next_run_at so sentinel recalculates
    current = await fetchrow("SELECT cron_expr, timezone FROM control.schedules WHERE id=$1", id)
    reset_next_run = current and (
        (body.cron_expr and body.cron_expr != current["cron_expr"]) or
        (body.timezone and body.timezone != current["timezone"])
    )
    
    next_run_at = None if reset_next_run else (
        datetime.fromisoformat(body.next_run_at) if body.next_run_at else None
    )
    
    row = await fetchrow("""
        UPDATE control.schedules SET
          name = COALESCE($2, name),
          cron_expr = COALESCE($3, cron_expr),
          timezone  = COALESCE($4, timezone),
          enabled   = COALESCE($5, enabled),
          max_concurrency = COALESCE($6, max_concurrency),
          backfill_policy = COALESCE($7, backfill_policy),
          last_run_at = COALESCE($8, last_run_at),
          next_run_at = $9,
          updated_by = $10,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$11
        RETURNING *
    """, 
    id, body.name, body.cron_expr, body.timezone, body.enabled, body.max_concurrency, body.backfill_policy, 
    datetime.fromisoformat(body.last_run_at) if body.last_run_at else None, 
    next_run_at, user_email, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.schedules WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": serialize_row(current) if current else None})
    return serialize_row(row)

@api.delete("/schedules/{id}")
async def delete_schedule(id: int):
    user_email = get_user_email()
    if not await can_edit_object(user_email, 'schedules', id):
        raise HTTPException(403, "You don't have permission to delete this schedule")
    
    await execute("DELETE FROM control.schedules WHERE id=$1", id)
    return {"ok": True}

@api.post("/bindings")
async def bind_schedule(body: BindingIn):
    id_ = await fetchval("""
        INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
        VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id
    """, body.schedule_id, body.entity_type, body.entity_id)
    return {"id": id_}

@api.get("/bindings/{entity_type}/{entity_id}")
async def list_bindings(entity_type: str, entity_id: int):
    rows = await fetch("SELECT * FROM control.schedule_bindings WHERE entity_type=$1 AND entity_id=$2", entity_type, entity_id)
    return [dict(r) for r in rows]

@api.get("/bindings/all")
async def list_all_bindings():
    """Bulk fetch all bindings - avoids N+1 queries from frontend."""
    rows = await fetch("SELECT * FROM control.schedule_bindings ORDER BY entity_type, entity_id")
    return [dict(r) for r in rows]

@api.get("/bindings_by_sched/{schedule_id}")
async def list_bindings_by_schedule(schedule_id: int):
    rows = await fetch("SELECT * FROM control.schedule_bindings WHERE schedule_id=$1", schedule_id)
    return [dict(r) for r in rows]

@api.delete("/bindings/{id}")
async def delete_binding(id: int):
    await execute("DELETE FROM control.schedule_bindings WHERE id=$1", id)
    return {"ok": True}

# ---------- Trigger now ----------
def get_databricks_run_statuses(run_ids: list[int]) -> dict[int, dict]:
    """Query Databricks for run statuses. Returns {run_id: {status, failed, error}}."""
    if not run_ids:
        return {}
    
    results = {}
    try:
        w = WorkspaceClient()
        for run_id in run_ids:
            try:
                run = w.jobs.get_run(run_id=run_id)
                state = run.state
                # life_cycle_state: PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR
                # result_state (when terminated): SUCCESS, FAILED, TIMEDOUT, CANCELED, etc.
                life_cycle = state.life_cycle_state.value if state.life_cycle_state else None
                result = state.result_state.value if state.result_state else None
                
                is_failed = result in ('FAILED', 'TIMEDOUT', 'CANCELED', 'MAXIMUM_CONCURRENT_RUNS_REACHED') or life_cycle == 'INTERNAL_ERROR'
                is_done = life_cycle in ('TERMINATED', 'SKIPPED', 'INTERNAL_ERROR')
                
                results[run_id] = {
                    "life_cycle_state": life_cycle,
                    "result_state": result,
                    "failed": is_failed,
                    "done": is_done,
                    "state_message": state.state_message if state.state_message else None
                }
            except Exception as e:
                # Run might not exist or be accessible
                results[run_id] = {"failed": True, "done": True, "error": str(e)}
    except Exception:
        pass  # If WorkspaceClient fails, return empty - don't break the endpoint
    
    return results


@api.get("/triggers")
async def list_triggers(status: Optional[str] = None):
    """
    Get active triggers (queued/running only).
    Used by UI to show current queue state.
    """
    if status:
        rows = await fetch("""
            SELECT t.*, 
                   CASE t.entity_type 
                     WHEN 'table' THEN d.name
                     WHEN 'compare_query' THEN q.name
                   END as entity_name,
                   COALESCE(
                       (SELECT json_agg(tg.name ORDER BY tg.name)
                        FROM control.entity_tags et
                        JOIN control.tags tg ON et.tag_id = tg.id
                        WHERE et.entity_type = CASE t.entity_type 
                            WHEN 'table' THEN 'table'
                            WHEN 'compare_query' THEN 'query'
                        END AND et.entity_id = t.entity_id),
                       '[]'::json
                   ) as entity_tags
            FROM control.triggers t
            LEFT JOIN control.datasets d ON t.entity_type = 'table' AND t.entity_id = d.id
            LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND t.entity_id = q.id
            WHERE t.status = $1
            ORDER BY t.priority ASC, t.id ASC
        """, status)
    else:
        rows = await fetch("""
            SELECT t.*, 
                   CASE t.entity_type 
                     WHEN 'table' THEN d.name
                     WHEN 'compare_query' THEN q.name
                   END as entity_name,
                   COALESCE(
                       (SELECT json_agg(tg.name ORDER BY tg.name)
                        FROM control.entity_tags et
                        JOIN control.tags tg ON et.tag_id = tg.id
                        WHERE et.entity_type = CASE t.entity_type 
                            WHEN 'table' THEN 'table'
                            WHEN 'compare_query' THEN 'query'
                        END AND et.entity_id = t.entity_id),
                       '[]'::json
                   ) as entity_tags
            FROM control.triggers t
            LEFT JOIN control.datasets d ON t.entity_type = 'table' AND t.entity_id = d.id
            LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND t.entity_id = q.id
            ORDER BY t.status, t.priority ASC, t.id ASC
        """)
    
    # Check Databricks run status for running triggers
    running_run_ids = [
        int(r['databricks_run_id']) for r in rows 
        if r['status'] == 'running' and r['databricks_run_id']
    ]
    run_statuses = get_databricks_run_statuses(running_run_ids)
    
    # Serialize and enrich with run status
    results = []
    for r in rows:
        row = serialize_row(r)
        run_id = r['databricks_run_id']
        if run_id and int(run_id) in run_statuses:
            row['databricks_run_status'] = run_statuses[int(run_id)]
        results.append(row)
    
    return results

@api.post("/triggers")
async def create_trigger(body: TriggerIn):
    """
    Create a new validation trigger and attempt immediate launch.
    Called by UI "Run Now" button.
    
    Flow:
    1. Validate entity exists
    2. Check for duplicate active triggers
    3. Check concurrency limits
       - If exceeded: create as 'queued' (sentinel or manual retry will handle)
       - If OK: launch job, create as 'running' with run_id
    """
    user_email = get_user_email()
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    # Validate entity exists
    if body.entity_type == 'table':
        entity = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", body.entity_id)
    else:
        entity = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", body.entity_id)
    
    if not entity:
        raise HTTPException(status_code=404, detail=f"{body.entity_type} not found")
    
    # Check for duplicate active trigger
    existing = await fetchrow("""
        SELECT id FROM control.triggers 
        WHERE entity_type = $1 AND entity_id = $2 AND status IN ('queued', 'running')
    """, body.entity_type, body.entity_id)
    
    if existing:
        raise HTTPException(status_code=409, detail="Validation already queued/running for this entity")
    
    # Check concurrency limits
    can_launch, reason = await check_system_concurrency(entity['src_system_id'], entity['tgt_system_id'])
    
    if not can_launch:
        # Create trigger as queued - sentinel or manual retry will handle
        row = await fetchrow("""
            INSERT INTO control.triggers (
                source, schedule_id, entity_type, entity_id,
                priority, requested_by, requested_at, params, status
            ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7, 'queued')
            RETURNING *
        """, body.source, body.schedule_id, body.entity_type, body.entity_id,
             body.priority, user_email, json.dumps(body.params) if isinstance(body.params, (dict, list)) else body.params)
        result = serialize_row(row)
        result["queued_reason"] = reason
        return result
    
    # Create trigger as running (we'll launch immediately)
    row = await fetchrow("""
        INSERT INTO control.triggers (
            source, schedule_id, entity_type, entity_id,
            priority, requested_by, requested_at, params, status, started_at
        ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7, 'running', now())
        RETURNING *
    """, body.source, body.schedule_id, body.entity_type, body.entity_id,
         body.priority, user_email, json.dumps(body.params) if isinstance(body.params, (dict, list)) else body.params)
    
    trigger_id = row['id']
    
    try:
        # Launch the job
        run_info = await launch_validation_job(trigger_id)
        result = serialize_row(row)
        result["databricks_run_id"] = run_info["run_id"]
        result["databricks_run_url"] = run_info["run_url"]
        return result
    except HTTPException as e:
        # Launch failed - delete the trigger and propagate error
        await execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
        raise e
    except Exception as e:
        # Unexpected error - delete trigger and raise
        await execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
        raise HTTPException(status_code=500, detail=f"Failed to launch job: {str(e)}")

@api.post("/triggers/bulk")
async def create_triggers_bulk(triggers: list[TriggerIn]):
    """
    Create multiple validation triggers in one transaction.
    Uses bulk INSERT with conflict detection - skips duplicates silently.
    """
    if not triggers:
        return {"created": []}
    
    user_email = get_user_email()
    
    # Extract arrays for each column
    sources = [t.source for t in triggers]
    schedule_ids = [t.schedule_id for t in triggers]
    entity_types = [t.entity_type for t in triggers]
    entity_ids = [t.entity_id for t in triggers]
    priorities = [t.priority for t in triggers]
    requested_bys = [t.requested_by or user_email for t in triggers]
    params_json = [json.dumps(t.params) if isinstance(t.params, (dict, list)) else t.params for t in triggers]
    
    # Bulk INSERT using unnest() with JOINs to filter active entities
    rows = await fetch("""
        INSERT INTO control.triggers (
            source, schedule_id, entity_type, entity_id,
            priority, requested_by, requested_at, params
        )
        SELECT t.source, t.schedule_id, t.entity_type, t.entity_id, t.priority, t.requested_by, now(), t.params::jsonb
        FROM unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::int[], $6::text[], $7::text[])
            AS t(source, schedule_id, entity_type, entity_id, priority, requested_by, params)
        -- Entity must be marked as active
        LEFT JOIN control.datasets d ON t.entity_type = 'table' AND d.id = t.entity_id AND d.is_active = TRUE
        LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND q.id = t.entity_id AND q.is_active = TRUE
        -- Entity must be not already in the queue
        WHERE NOT EXISTS (
            SELECT 1 FROM control.triggers tr
            WHERE tr.entity_type = t.entity_type 
            AND tr.entity_id = t.entity_id 
            AND tr.status IN ('queued', 'running')
        )
        AND (d.id IS NOT NULL OR q.id IS NOT NULL)  
        RETURNING *
    """, sources, schedule_ids, entity_types, entity_ids, priorities, requested_bys, params_json)
    
    return {"created": [dict(r) for r in rows]}


@api.post("/triggers/bulk-create")
async def bulk_create_triggers(body: BulkTriggerRequest):
    """
    Bulk create triggers with status 'running' (to prevent race with sentinel).
    Returns immediately - caller should then launch jobs via bulk-launch.
    """
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    if not body.entity_ids:
        return {"created": [], "skipped": 0}
    
    user_email = get_user_email()
    entity_type = body.entity_type
    
    # Bulk INSERT with status='running' to prevent sentinel race condition
    rows = await fetch("""
        INSERT INTO control.triggers (
            source, entity_type, entity_id, status,
            priority, requested_by, requested_at
        )
        SELECT 'manual', $1, t.entity_id, 'running', 100, $2, now()
        FROM unnest($3::bigint[]) AS t(entity_id)
        -- Entity must be active
        LEFT JOIN control.datasets d ON $1 = 'table' AND d.id = t.entity_id AND d.is_active = TRUE
        LEFT JOIN control.compare_queries q ON $1 = 'compare_query' AND q.id = t.entity_id AND q.is_active = TRUE
        -- Not already in queue
        WHERE NOT EXISTS (
            SELECT 1 FROM control.triggers tr
            WHERE tr.entity_type = $1 
            AND tr.entity_id = t.entity_id 
            AND tr.status IN ('queued', 'running')
        )
        AND (d.id IS NOT NULL OR q.id IS NOT NULL)
        RETURNING id, entity_id
    """, entity_type, user_email, body.entity_ids)
    
    created_ids = [r['id'] for r in rows]
    skipped = len(body.entity_ids) - len(created_ids)
    
    return {"created": created_ids, "skipped": skipped}

@api.delete("/triggers/{id}")
async def cancel_trigger(id: int):
    """Cancel a queued or running trigger."""
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    
    await execute("DELETE FROM control.triggers WHERE id=$1", id)
    return {"ok": True}


@api.post("/triggers/{id}/launch")
async def launch_trigger(id: int):
    """
    Manually launch a trigger.
    Accepts 'queued' or 'running' (without databricks_run_id) triggers.
    """
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    
    # Accept 'queued' or 'running' without a run yet (bulk-created awaiting launch)
    if trigger['status'] == 'running' and trigger['databricks_run_id']:
        return {"launched": False, "reason": "Already running"}
    if trigger['status'] not in ('queued', 'running'):
        raise HTTPException(status_code=400, detail=f"Trigger cannot be launched (status: {trigger['status']})")
    
    # Get entity to check concurrency
    if trigger['entity_type'] == 'table':
        entity = await fetchrow("SELECT src_system_id, tgt_system_id FROM control.datasets WHERE id=$1", trigger['entity_id'])
    else:
        entity = await fetchrow("SELECT src_system_id, tgt_system_id FROM control.compare_queries WHERE id=$1", trigger['entity_id'])
    
    if not entity:
        # Entity was deleted, clean up orphan
        await execute("DELETE FROM control.triggers WHERE id=$1", id)
        raise HTTPException(status_code=404, detail="Entity no longer exists")
    
    # Check concurrency
    can_launch, reason = await check_system_concurrency(entity['src_system_id'], entity['tgt_system_id'])
    if not can_launch:
        return {"launched": False, "reason": reason}
    
    try:
        run_info = await launch_validation_job(id)
        return {"launched": True, "run_id": run_info["run_id"], "run_url": run_info["run_url"]}
    except HTTPException as e:
        return {"launched": False, "reason": e.detail}
    except Exception as e:
        return {"launched": False, "reason": str(e)}


@api.post("/triggers/bulk-launch")
async def bulk_launch_triggers(body: dict):
    """
    Attempt to launch multiple queued triggers.
    Respects concurrency limits - launches as many as possible.
    Returns results per trigger.
    """
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    trigger_ids = body.get("trigger_ids", [])
    if not trigger_ids:
        return {"results": []}
    
    results = []
    for trigger_id in trigger_ids:
        trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            results.append({"id": trigger_id, "launched": False, "reason": "Not found"})
            continue
        
        if trigger['status'] != 'queued':
            results.append({"id": trigger_id, "launched": False, "reason": f"Not queued (status: {trigger['status']})"})
            continue
        
        # Get entity
        if trigger['entity_type'] == 'table':
            entity = await fetchrow("SELECT src_system_id, tgt_system_id FROM control.datasets WHERE id=$1", trigger['entity_id'])
        else:
            entity = await fetchrow("SELECT src_system_id, tgt_system_id FROM control.compare_queries WHERE id=$1", trigger['entity_id'])
        
        if not entity:
            await execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
            results.append({"id": trigger_id, "launched": False, "reason": "Entity no longer exists"})
            continue
        
        # Check concurrency
        can_launch, reason = await check_system_concurrency(entity['src_system_id'], entity['tgt_system_id'])
        if not can_launch:
            results.append({"id": trigger_id, "launched": False, "reason": reason})
            continue
        
        try:
            run_info = await launch_validation_job(trigger_id)
            results.append({"id": trigger_id, "launched": True, "run_id": run_info["run_id"], "run_url": run_info["run_url"]})
        except Exception as e:
            results.append({"id": trigger_id, "launched": False, "reason": str(e)})
    
    return {"results": results}


@api.post("/triggers/{id}/repair")
async def repair_trigger_run(id: int):
    """
    Repair a failed Databricks run for a trigger.
    Uses repair_run to re-run all failed tasks.
    """
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    
    if not trigger['databricks_run_id']:
        raise HTTPException(status_code=400, detail="No Databricks run associated with this trigger")
    
    job_id = os.environ.get("VALIDATION_JOB_ID")
    if not job_id:
        raise HTTPException(status_code=500, detail="VALIDATION_JOB_ID not configured")
    
    try:
        w = WorkspaceClient()
        run_id = int(trigger['databricks_run_id'])
        
        # Get run info with repair history and resolved values
        run_info = w.jobs.get_run(run_id=run_id, include_history=True, include_resolved_values=True)
        
        # Extract latest_repair_id from repair history (required for subsequent repairs)
        latest_repair_id = None
        if run_info.repair_history:
            latest_repair_id = run_info.repair_history[-1].id
        
        # Extract original job_parameters to pass to repair
        original_params = None
        if run_info.job_parameters:
            original_params = {p.name: p.value for p in run_info.job_parameters}
        
        # Repair the run with rerun_all_failed_tasks and original parameters
        repair_waiter = w.jobs.repair_run(
            run_id=run_id,
            rerun_all_failed_tasks=True,
            latest_repair_id=latest_repair_id,
            job_parameters=original_params
        )
        
        # Get the repair_id from the response (Wait object has .response with the immediate result)
        repair_id = repair_waiter.response.repair_id if hasattr(repair_waiter, 'response') and repair_waiter.response else None
        
        # Fetch updated run info to get the new run_page_url
        updated_run = w.jobs.get_run(run_id=run_id)
        new_run_url = updated_run.run_page_url if updated_run.run_page_url else trigger['databricks_run_url']
        
        # Update trigger with new URL and reset status
        await execute("""
            UPDATE control.triggers 
            SET status = 'running', started_at = now(), databricks_run_url = $2
            WHERE id = $1
        """, id, new_run_url)
        
        return {
            "repaired": True, 
            "run_id": trigger['databricks_run_id'],
            "repair_id": repair_id,
            "run_url": new_run_url
        }
    
    except Exception as e:
        error_msg = str(e)
        # Check for common errors
        if "INVALID_STATE" in error_msg or "in progress" in error_msg.lower():
            return {"repaired": False, "reason": "Run is still in progress. Wait for it to complete before repairing."}
        if "not found" in error_msg.lower():
            return {"repaired": False, "reason": "Databricks run not found. It may have been deleted."}
        return {"repaired": False, "reason": error_msg}


@api.post("/triggers/bulk-repair")
async def bulk_repair_triggers(body: BulkRepairRequest):
    """Repair multiple failed triggers."""
    await require_role('CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE')
    
    results = []
    w = WorkspaceClient()
    
    for trigger_id in body.trigger_ids:
        try:
            trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
            if not trigger:
                results.append({"id": trigger_id, "repaired": False, "reason": "Not found"})
                continue
            
            if not trigger['databricks_run_id']:
                results.append({"id": trigger_id, "repaired": False, "reason": "No run ID"})
                continue
            
            run_id = int(trigger['databricks_run_id'])
            run_info = w.jobs.get_run(run_id=run_id, include_history=True, include_resolved_values=True)
            
            latest_repair_id = None
            if run_info.repair_history:
                latest_repair_id = run_info.repair_history[-1].id
            
            original_params = None
            if run_info.job_parameters:
                original_params = {p.name: p.value for p in run_info.job_parameters}
            
            repair_waiter = w.jobs.repair_run(
                run_id=run_id,
                rerun_all_failed_tasks=True,
                latest_repair_id=latest_repair_id,
                job_parameters=original_params
            )
            
            updated_run = w.jobs.get_run(run_id=run_id)
            new_run_url = updated_run.run_page_url or trigger['databricks_run_url']
            
            await execute("""
                UPDATE control.triggers 
                SET status = 'running', started_at = now(), databricks_run_url = $2
                WHERE id = $1
            """, trigger_id, new_run_url)
            
            results.append({"id": trigger_id, "repaired": True})
        except Exception as e:
            results.append({"id": trigger_id, "repaired": False, "reason": str(e)})
    
    return {"results": results}


@api.get("/queue-status")
async def get_queue_status():
    """
    Get queue statistics for dashboard.
    Returns counts by status and recent activity.
    """
    stats = await fetchrow("""
        SELECT 
            COUNT(*) FILTER (WHERE status = 'queued') as queued,
            COUNT(*) FILTER (WHERE status = 'running') as running,
            COUNT(*) as total_active
        FROM control.triggers
    """)
    
    recent = await fetchrow("""
        SELECT 
            COUNT(*) FILTER (WHERE status = 'succeeded') as succeeded,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COUNT(*) as total_completed
        FROM control.validation_history
        WHERE finished_at > now() - interval '1 hour'
    """)
    
    return {
        "active": dict(stats) if stats else {"queued": 0, "running": 0, "total_active": 0},
        "recent_1h": dict(recent) if recent else {"succeeded": 0, "failed": 0, "total_completed": 0}
    }

@api.get("/triggers/running-per-system")
async def get_running_per_system():
    """
    Get count of running validations per system (source and target).
    Used by JobSentinel for concurrency control.
    Single efficient query with JOINs.
    """
    rows = await fetch("""
        WITH running_tables AS (
            SELECT t.id, d.src_system_id, d.tgt_system_id
            FROM control.triggers t
            JOIN control.datasets d ON t.entity_id = d.id
            WHERE t.status = 'running' AND t.entity_type = 'table'
        ),
        running_queries AS (
            SELECT t.id, q.src_system_id, q.tgt_system_id
            FROM control.triggers t
            JOIN control.compare_queries q ON t.entity_id = q.id
            WHERE t.status = 'running' AND t.entity_type = 'compare_query'
        ),
        all_running AS (
            SELECT src_system_id as system_id FROM running_tables
            UNION ALL
            SELECT tgt_system_id as system_id FROM running_tables
            UNION ALL
            SELECT src_system_id as system_id FROM running_queries
            UNION ALL
            SELECT tgt_system_id as system_id FROM running_queries
        )
        SELECT system_id, COUNT(*) as count
        FROM all_running
        GROUP BY system_id
    """)
    
    # Convert to dict for easy lookup
    return {row['system_id']: int(row['count']) for row in rows}

# ---------- Validation History ----------

def _transform_pk_samples_to_legacy(sample_differences: dict | str | None) -> dict | None:
    """
    Transform NEW flat pk_sample format to OLD nested format for frontend compatibility.
    
    NEW format: {"mode": "primary_key", "pk_columns": [...], "samples": [
        {"pk_col": val, ".system": "source", "diff_col": val}, ...
    ]}
    
    OLD format: {"mode": "primary_key", "pk_columns": [...], "samples": [
        {"pk": {pk_col: val}, "differences": [{"column": ..., "source_value": ..., "target_value": ...}]}
    ]}
    
    TODO: Remove after March 2026 when all records use old format or frontend updated
    """
    if sample_differences is None:
        return None
    
    if isinstance(sample_differences, str):
        try:
            sample_differences = json.loads(sample_differences)
        except (json.JSONDecodeError, TypeError):
            return sample_differences
    
    # Only transform primary_key mode with new format
    if not isinstance(sample_differences, dict) or sample_differences.get("mode") != "primary_key":
        return sample_differences
    
    samples = sample_differences.get("samples", [])
    if not samples or not isinstance(samples[0], dict):
        return sample_differences
    
    # Detect NEW format by presence of ".system" key
    if ".system" not in samples[0]:
        return sample_differences  # Already in old format
    
    pk_columns = sample_differences.get("pk_columns", [])
    
    # Group rows by PK, pair source/target
    grouped: dict[str, dict] = {}
    system_names: list[str] = []
    for row in samples:
        system = row.get(".system", "")
        if system and system not in system_names:
            system_names.append(system)
        pk_key = "|".join(str(row.get(pk, "")) for pk in pk_columns)
        if pk_key not in grouped:
            grouped[pk_key] = {"pk": {pk: row.get(pk) for pk in pk_columns}, "source": None, "target": None}
        if len(system_names) >= 1 and system == system_names[0]:
            grouped[pk_key]["source"] = row
        else:
            grouped[pk_key]["target"] = row
    
    # Build legacy format
    legacy_samples = []
    for pk_key, data in grouped.items():
        src, tgt = data["source"], data["target"]
        if not src or not tgt:
            continue
        differences = []
        for col in src.keys():
            if col in (".system", *pk_columns):
                continue
            differences.append({
                "column": col,
                "source_value": src.get(col),
                "target_value": tgt.get(col)
            })
        legacy_samples.append({"pk": data["pk"], "differences": differences})
    
    return {
        "mode": "primary_key",
        "pk_columns": pk_columns,
        "samples": legacy_samples
    }

@api.get("/validation-history")
async def list_validation_history(
    # Pagination
    limit: int = 100,
    offset: int = 0,
    # Filters
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    entity_name: Optional[str] = None,
    status: Optional[str] = None,
    schedule_id: Optional[int] = None,
    source_system: Optional[str] = None,
    target_system: Optional[str] = None,
    tags: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days_back: int = 0,
    # Sorting
    sort_by: str = "requested_at",
    sort_dir: str = "desc",
):
    """
    Get validation history with server-side filters and pagination.
    Returns { data: [...], total: N, limit: N, offset: N }
    """
    from datetime import datetime
    
    conditions = []
    params = []
    param_idx = 1
    
    # Date range filters (date_from/date_to take precedence over days_back)
    if date_from:
        conditions.append(f"vh.requested_at >= ${param_idx}")
        # Parse ISO string to datetime
        dt = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
        params.append(dt)
        param_idx += 1
    elif days_back > 0:
        conditions.append(f"vh.requested_at >= NOW() - INTERVAL '{days_back} days'")
    
    if date_to:
        conditions.append(f"vh.requested_at <= ${param_idx}")
        dt = datetime.fromisoformat(date_to.replace('Z', '+00:00'))
        params.append(dt)
        param_idx += 1
    
    if entity_type:
        conditions.append(f"vh.entity_type = ${param_idx}")
        params.append(entity_type)
        param_idx += 1
    
    if entity_id:
        conditions.append(f"vh.entity_id = ${param_idx}")
        params.append(entity_id)
        param_idx += 1
    
    if entity_name:
        conditions.append(f"vh.entity_name ILIKE ${param_idx}")
        params.append(f"%{entity_name}%")
        param_idx += 1
    
    if status:
        conditions.append(f"vh.status = ${param_idx}")
        params.append(status)
        param_idx += 1
    
    if schedule_id:
        conditions.append(f"vh.schedule_id = ${param_idx}")
        params.append(schedule_id)
        param_idx += 1
    
    if source_system:
        conditions.append(f"vh.source_system_name = ${param_idx}")
        params.append(source_system)
        param_idx += 1
    
    if target_system:
        conditions.append(f"vh.target_system_name = ${param_idx}")
        params.append(target_system)
        param_idx += 1
    
    # Tag filtering (AND logic - must have all specified tags)
    tag_join = ""
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        if tag_list:
            tag_join = """
                JOIN (
                    SELECT et.entity_type, et.entity_id
                    FROM control.entity_tags et
                    JOIN control.tags t ON et.tag_id = t.id
                    WHERE t.name = ANY($%d)
                    GROUP BY et.entity_type, et.entity_id
                    HAVING COUNT(DISTINCT t.name) = $%d
                ) tf ON (
                    (vh.entity_type = 'table' AND tf.entity_type = 'table' AND tf.entity_id = vh.entity_id)
                    OR (vh.entity_type = 'compare_query' AND tf.entity_type = 'query' AND tf.entity_id = vh.entity_id)
                )
            """ % (param_idx, param_idx + 1)
            params.append(tag_list)
            params.append(len(tag_list))
            param_idx += 2
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Validate sort column to prevent SQL injection
    valid_sort_cols = {
        "requested_at": "vh.requested_at",
        "entity_name": "vh.entity_name",
        "entity_type": "vh.entity_type",
        "status": "vh.status",
        "duration": "vh.duration_seconds",
        "systems": "vh.source_system_name",
        "row_counts": "vh.row_count_source",
        "differences": "vh.rows_different",
    }
    sort_col = valid_sort_cols.get(sort_by, "vh.requested_at")
    sort_direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
    
    # Count total and status breakdown for matching records
    stats_row = await fetchrow(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE vh.status = 'succeeded') as succeeded,
            COUNT(*) FILTER (WHERE vh.status = 'failed') as failed,
            COUNT(*) FILTER (WHERE vh.status = 'error') as errors
        FROM control.validation_history vh
        {tag_join}
        {where_clause}
    """, *params)
    total = stats_row['total'] if stats_row else 0
    stats = {
        "total": total,
        "succeeded": stats_row['succeeded'] if stats_row else 0,
        "failed": stats_row['failed'] if stats_row else 0,
        "errors": stats_row['errors'] if stats_row else 0,
    }
    
    # Fetch paginated results
    rows = await fetch(f"""
        SELECT 
            vh.id, vh.trigger_id, vh.entity_type, vh.entity_id, vh.entity_name,
            vh.source, vh.schedule_id, vh.requested_by, vh.requested_at,
            vh.started_at, vh.finished_at, vh.duration_seconds,
            vh.source_system_name, vh.target_system_name,
            vh.source_table, vh.target_table, vh.pk_columns,
            vh.status, vh.schema_match, vh.row_count_match,
            vh.row_count_source, vh.row_count_target,
            vh.rows_compared, vh.rows_different, vh.difference_pct,
            vh.compare_mode, vh.error_message, vh.databricks_run_url
        FROM control.validation_history vh
        {tag_join}
        {where_clause}
        ORDER BY {sort_col} {sort_direction}
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """, *params, limit, offset)
    
    results = [dict(r) for r in rows]
    
    # Batch fetch tags for all unique entities
    if results:
        table_ids = list(set(r['entity_id'] for r in results if r['entity_type'] == 'table'))
        query_ids = list(set(r['entity_id'] for r in results if r['entity_type'] == 'compare_query'))
        
        tags_by_entity: dict[tuple[str, int], list] = {}
        
        if table_ids:
            tag_rows = await fetch("""
                SELECT et.entity_id, json_agg(t.name ORDER BY t.name) as tags
                FROM control.entity_tags et
                JOIN control.tags t ON et.tag_id = t.id
                WHERE et.entity_type = 'table' AND et.entity_id = ANY($1)
                GROUP BY et.entity_id
            """, table_ids)
            for tr in tag_rows:
                tags_by_entity[('table', tr['entity_id'])] = tr['tags'] or []
        
        if query_ids:
            tag_rows = await fetch("""
                SELECT et.entity_id, json_agg(t.name ORDER BY t.name) as tags
                FROM control.entity_tags et
                JOIN control.tags t ON et.tag_id = t.id
                WHERE et.entity_type = 'query' AND et.entity_id = ANY($1)
                GROUP BY et.entity_id
            """, query_ids)
            for tr in tag_rows:
                tags_by_entity[('query', tr['entity_id'])] = tr['tags'] or []
        
        for r in results:
            tag_key = ('table', r['entity_id']) if r['entity_type'] == 'table' else ('query', r['entity_id'])
            r['tags'] = tags_by_entity.get(tag_key, [])
    
    return {"data": results, "total": total, "limit": limit, "offset": offset, "stats": stats}

@api.get("/validation-history/{id}")
async def get_validation_detail(id: int):
    """Get full validation details including sample differences."""
    row = await fetchrow("SELECT * FROM control.validation_history WHERE id=$1", id)
    if not row:
        raise HTTPException(status_code=404, detail="Validation not found")
    result = dict(row)
    result["sample_differences"] = _transform_pk_samples_to_legacy(result.get("sample_differences"))
    return result

@api.get("/validation-history/entity/{entity_type}/{entity_id}/latest")
async def get_latest_validation(entity_type: str, entity_id: int):
    """
    Get most recent validation for a specific table/query.
    Used to display "Last Run" status in UI.
    """
    row = await fetchrow("""
        SELECT * FROM control.validation_history
        WHERE entity_type = $1 AND entity_id = $2
        ORDER BY finished_at DESC
        LIMIT 1
    """, entity_type, entity_id)
    
    if not row:
        return None
    result = dict(row)
    result["sample_differences"] = _transform_pk_samples_to_legacy(result.get("sample_differences"))
    return result

@api.post("/validation-history")
async def create_validation_history(body: dict):
    """
    Called by Databricks workflow at completion to record results.
    Also deletes the corresponding trigger from active queue.
    """   
    # if trigger-id was blank, it was a manual job so we return early
    if not body.get('trigger_id'):
        return 

    # Get missing fields from trigger if not provided by job 
    trigger = await fetchrow("SELECT databricks_run_url, databricks_run_id, entity_id, requested_at FROM control.triggers WHERE id=$1", body['trigger_id'])
    if not trigger:
        raise HTTPException(status_code=404, detail=f"Trigger '{body['trigger_id']}' not found")

    databricks_run_url = body.get('databricks_run_url') or trigger['databricks_run_url']
    databricks_run_id = body.get('databricks_run_id') or trigger['databricks_run_id']
    entity_id = body.get('entity_id') or trigger['entity_id']
    requested_by = "system" # trigger['requested_by'] - once we have users and logins set up, add this in
    requested_at = trigger['requested_at']
    
    row = await fetchrow("""
        INSERT INTO control.validation_history (
            trigger_id, entity_type, entity_id, entity_name,
            source, schedule_id, requested_by, requested_at,
            started_at, finished_at,
            source_system_id, target_system_id,
            source_system_name, target_system_name,
            source_table, target_table, sql_query,
            compare_mode, pk_columns, exclude_columns,
            status, schema_match, schema_details,
            row_count_source, row_count_target, row_count_match,
            rows_compared, rows_matched, rows_different,
            sample_differences, error_message, error_details,
            databricks_run_id, databricks_run_url, full_result
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29,
            $30, $31, $32, $33, $34, $35
        ) RETURNING id
    """,
        body['trigger_id'], body['entity_type'], entity_id, body['entity_name'],
        body['source'], body.get('schedule_id'), requested_by, requested_at,
        datetime.fromisoformat(body['started_at']), datetime.fromisoformat(body['finished_at']),
        body['source_system_id'], body['target_system_id'],
        body['source_system_name'], body['target_system_name'],
        body.get('source_table'), body.get('target_table'), body.get('sql_query'),
        body['compare_mode'], body.get('pk_columns'), body.get('exclude_columns'),
        body['status'], body.get('schema_match'), json.dumps(body.get('schema_details', {})),
        body.get('row_count_source'), body.get('row_count_target'), body.get('row_count_match'),
        body.get('rows_compared'), body.get('rows_matched'), body.get('rows_different'),
        json.dumps(body.get('sample_differences', [])), body.get('error_message'), 
        json.dumps(body.get('error_details', {})),
        databricks_run_id, databricks_run_url, json.dumps(body.get('full_result', {}))
    )
    
    # Delete from active queue
    await execute("DELETE FROM control.triggers WHERE id=$1", body['trigger_id'])
    
    return {"id": row['id'], "ok": True}

@api.patch("/validation-history/{id}")
async def update_validation_history(id: int, body: dict):
    """
    Update specific fields on a validation history record.
    Used for post-processing like PK analysis of sample_differences.
    """
    # Only allow updating specific fields
    allowed_fields = {
        'sample_differences', 'status', 'error_message', 'error_details',
        'rows_compared', 'rows_matched', 'rows_different'
    }
    
    # Filter to only allowed fields that are present in body
    updates = {k: v for k, v in body.items() if k in allowed_fields}
    
    if not updates:
        raise HTTPException(400, f"No valid fields to update. Allowed: {allowed_fields}")
    
    # Build dynamic UPDATE query
    set_clauses = []
    params = []
    param_idx = 1
    
    for field, value in updates.items():
        if field in ('sample_differences', 'error_details'):
            # JSON fields need serialization
            set_clauses.append(f"{field} = ${param_idx}::jsonb")
            params.append(json.dumps(value) if value is not None else None)
        else:
            set_clauses.append(f"{field} = ${param_idx}")
            params.append(value)
        param_idx += 1
    
    params.append(id)
    
    result = await execute(f"""
        UPDATE control.validation_history 
        SET {', '.join(set_clauses)}
        WHERE id = ${param_idx}
    """, *params)
    
    if result == "UPDATE 0":
        raise HTTPException(404, f"Validation history record {id} not found")
    
    return {"id": id, "ok": True, "updated_fields": list(updates.keys())}


@api.post("/tables/{id}/fetch-lineage")
async def fetch_lineage_for_table(id: int, system: str = "source"):
    """
    Start a Databricks lineage job for a configured table (dataset).
    Lineage is available when the source or target system is Databricks.
    Pass ?system=source (default) or ?system=target to choose which side to query.
    The job will PATCH the lineage result back to this dataset when done.
    """
    row = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", id)
    if not row:
        raise HTTPException(status_code=404, detail="Table not found")

    if system not in ("source", "target"):
        raise HTTPException(status_code=400, detail="system must be 'source' or 'target'")

    system_id = row["src_system_id"] if system == "source" else row["tgt_system_id"]
    chosen_system = await fetchrow("SELECT id, kind, catalog FROM control.systems WHERE id=$1", system_id)
    if not chosen_system:
        raise HTTPException(status_code=404, detail=f"{system.capitalize()} system not found")
    if chosen_system["kind"] != "Databricks":
        raise HTTPException(
            status_code=400,
            detail=f"Lineage is only available for Databricks systems ({system} system is {chosen_system['kind']})"
        )

    schema_col = "src_schema" if system == "source" else "tgt_schema"
    table_col = "src_table" if system == "source" else "tgt_table"
    schema_val = (row.get(schema_col) or "").strip()
    table_val = (row.get(table_col) or "").strip()
    if not schema_val or not table_val:
        raise HTTPException(
            status_code=400,
            detail=f"{system.capitalize()} schema and table are required for lineage"
        )

    catalog = (chosen_system.get("catalog") or "").strip()
    if not catalog:
        raise HTTPException(status_code=400, detail=f"{system.capitalize()} system has no catalog configured")

    table_name = f"{catalog}.{schema_val}.{table_val}"

    await execute("UPDATE control.datasets SET lineage = NULL WHERE id=$1", id)

    job_id = os.environ.get("LINEAGE_JOB_ID")
    if not job_id:
        raise HTTPException(status_code=500, detail="LINEAGE_JOB_ID not configured")

    backend_url = os.environ.get("DATABRICKS_APP_URL", "").rstrip("/")
    if not backend_url:
        raise HTTPException(status_code=500, detail="DATABRICKS_APP_URL not configured")

    params = {
        "table_name": table_name,
        "catalog_name": catalog,
        "backend_api_url": backend_url,
        "entity_type": "table",
        "entity_id": str(id),
    }
    try:
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=int(job_id), job_parameters=params)
        run_url = f"{w.config.host}/jobs/{job_id}/runs/{run.run_id}"
        return {"ok": True, "message": "Lineage fetch started", "run_id": run.run_id, "run_url": run_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start lineage job: {str(e)}")


@api.patch("/tables/{id}/lineage")
async def update_table_lineage(id: int, body: dict):
    """
    Update the lineage JSONB field on a dataset. Called by the fetch-lineage job.
    Body: {"lineage": [...]}
    """
    lineage = body.get("lineage")
    await execute(
        "UPDATE control.datasets SET lineage = $1::jsonb WHERE id = $2",
        json.dumps(lineage) if lineage is not None else None, id
    )
    return {"ok": True}


@api.post("/queries/{id}/fetch-lineage")
async def fetch_lineage_for_query(id: int):
    """
    Lineage fetch for queries is not supported at this time.
    Only tables (datasets) can fetch lineage.
    """
    raise HTTPException(
        status_code=400,
        detail="Lineage is only supported for tables at this time. Query lineage is not available."
    )


@api.patch("/queries/{id}/lineage")
async def update_query_lineage(id: int, body: dict):
    """
    Update the lineage JSONB field on a compare_query. Called by the fetch-lineage job.
    Body: {"lineage": [...]}
    """
    lineage = body.get("lineage")
    await execute(
        "UPDATE control.compare_queries SET lineage = $1::jsonb WHERE id = $2",
        json.dumps(lineage) if lineage is not None else None, id
    )
    return {"ok": True}


@api.delete("/validation-history")
async def delete_validation_history(body: dict):
    """
    Bulk delete validation history records by IDs.
    Body: {"ids": [1, 2, 3, ...]}
    """
    ids = body.get('ids', [])
    if not ids:
        raise HTTPException(400, "No IDs provided")
    
    # Delete the records
    result = await execute(
        "DELETE FROM control.validation_history WHERE id = ANY($1)",
        ids
    )
    
    return {"deleted_count": len(ids), "ok": True}

# ---------- Validation Configuration ----------
@api.get("/validation-config")
async def get_validation_config():
    """Get global validation configuration"""
    row = await fetchrow("SELECT * FROM control.validation_config WHERE id = 1")
    if not row:
        # Return defaults if not initialized
        return {
            "downgrade_unicode": False,
            "replace_special_char": [],
            "extra_replace_regex": ""
        }
    return dict(row)

@api.put("/validation-config")
async def update_validation_config(body: dict):
    """Update global validation configuration"""
    await execute("""
        UPDATE control.validation_config 
        SET downgrade_unicode = $1,
            replace_special_char = $2,
            extra_replace_regex = $3,
            updated_by = $4,
            updated_at = now()
        WHERE id = 1
    """, 
        body.get('downgrade_unicode', False),
        body.get('replace_special_char', []),
        body.get('extra_replace_regex', ''),
        body.get('updated_by', 'user@company.com')
    )
    
    # Return updated config
    return await get_validation_config()

# ---------- Type Transformations ----------
@api.get("/type-transformations")
async def list_type_transformations():
    """Get all type transformations with system details"""
    rows = await fetch("""
        SELECT 
            tt.*,
            sa.name as system_a_name,
            sa.kind as system_a_kind,
            sb.name as system_b_name,
            sb.kind as system_b_kind
        FROM control.type_transformations tt
        JOIN control.systems sa ON tt.system_a_id = sa.id
        JOIN control.systems sb ON tt.system_b_id = sb.id
        ORDER BY sa.name, sb.name
    """)
    return [dict(r) for r in rows]

@api.get("/type-transformations/default/{system_kind}")
async def get_default_transformation_for_system(system_kind: str):
    """Get default transformation function for a system type"""
    return {
        "system_kind": system_kind,
        "function": get_default_transformation(system_kind)
    }

@api.get("/type-transformations/for-validation/{system_a_id}/{system_b_id}")
async def get_type_transformation_for_validation(system_a_id: int, system_b_id: int):
    """
    Get type transformation for a validation job (non-directional, with defaults).
    Returns empty strings if no transformation exists, allowing the job to handle defaults.
    """
    # Normalize to match storage order
    min_id = min(system_a_id, system_b_id)
    max_id = max(system_a_id, system_b_id)
    is_swapped = system_a_id != min_id
    
    row = await fetchrow("""
        SELECT 
            tt.*,
            sa.name as system_a_name,
            sa.kind as system_a_kind,
            sb.name as system_b_name,
            sb.kind as system_b_kind
        FROM control.type_transformations tt
        JOIN control.systems sa ON tt.system_a_id = sa.id
        JOIN control.systems sb ON tt.system_b_id = sb.id
        WHERE tt.system_a_id = $1 AND tt.system_b_id = $2
    """, min_id, max_id)
    
    if not row:
        # No transformation defined - return empty functions
        sys_a = await fetchrow("SELECT name, kind FROM control.systems WHERE id = $1", system_a_id)
        sys_b = await fetchrow("SELECT name, kind FROM control.systems WHERE id = $1", system_b_id)
        
        return {
            "exists": False,
            "system_a_id": system_a_id,
            "system_b_id": system_b_id,
            "system_a_name": sys_a['name'] if sys_a else None,
            "system_a_kind": sys_a['kind'] if sys_a else None,
            "system_b_name": sys_b['name'] if sys_b else None,
            "system_b_kind": sys_b['kind'] if sys_b else None,
            "system_a_function": "",
            "system_b_function": ""
        }
    
    # Map stored order to requested order
    if is_swapped:
        system_a_func, system_b_func = row['system_b_function'], row['system_a_function']
        system_a_name, system_b_name = row['system_b_name'], row['system_a_name']
        system_a_kind, system_b_kind = row['system_b_kind'], row['system_a_kind']
    else:
        system_a_func, system_b_func = row['system_a_function'], row['system_b_function']
        system_a_name, system_b_name = row['system_a_name'], row['system_b_name']
        system_a_kind, system_b_kind = row['system_a_kind'], row['system_b_kind']
    
    return {
        "exists": True,
        "system_a_id": system_a_id,
        "system_b_id": system_b_id,
        "system_a_name": system_a_name,
        "system_a_kind": system_a_kind,
        "system_b_name": system_b_name,
        "system_b_kind": system_b_kind,
        "system_a_function": system_a_func,
        "system_b_function": system_b_func
    }

@api.get("/type-transformations/{system_a_id}/{system_b_id}")
async def get_type_transformation(system_a_id: int, system_b_id: int):
    """Get type transformation for a system pair (non-directional)"""
    # Normalize to match storage order
    system_a = min(system_a_id, system_b_id)
    system_b = max(system_a_id, system_b_id)
    
    row = await fetchrow("""
        SELECT 
            tt.*,
            sa.name as system_a_name,
            sa.kind as system_a_kind,
            sb.name as system_b_name,
            sb.kind as system_b_kind
        FROM control.type_transformations tt
        JOIN control.systems sa ON tt.system_a_id = sa.id
        JOIN control.systems sb ON tt.system_b_id = sb.id
        WHERE tt.system_a_id = $1 AND tt.system_b_id = $2
    """, system_a, system_b)
    
    if not row:
        raise HTTPException(404, "Type transformation not found for this system pair")
    return dict(row)

@api.post("/type-transformations")
async def create_type_transformation(body: TypeTransformationIn):
    """Create a new type transformation for a system pair"""
    user_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    print(f"\n[BACKEND CREATE] Received from frontend:")
    print(f"  body.system_a_id = {body.system_a_id}")
    print(f"  body.system_b_id = {body.system_b_id}")
    print(f"  body.system_a_function = '{body.system_a_function[:50] if body.system_a_function else 'EMPTY'}'...")
    print(f"  body.system_b_function = '{body.system_b_function[:50] if body.system_b_function else 'EMPTY'}'...")
    
    # Normalize the pair to avoid duplicates
    system_a = min(body.system_a_id, body.system_b_id)
    system_b = max(body.system_a_id, body.system_b_id)
    
    print(f"[BACKEND CREATE] After normalization: system_a={system_a}, system_b={system_b}")
    print(f"[BACKEND CREATE] Checking: body.system_a_id ({body.system_a_id}) == system_a ({system_a})? {body.system_a_id == system_a}")
    
    # Swap functions if IDs were swapped
    if body.system_a_id == system_a:
        func_a, func_b = body.system_a_function, body.system_b_function
        print(f"[BACKEND CREATE] NO SWAP - using functions as-is")
    else:
        func_a, func_b = body.system_b_function, body.system_a_function
        print(f"[BACKEND CREATE] SWAPPED - func_a gets body.system_b_function, func_b gets body.system_a_function")
    
    print(f"[BACKEND CREATE] Will store:")
    print(f"  system_a_id={system_a}, system_a_function='{func_a[:50] if func_a else 'EMPTY'}'...")
    print(f"  system_b_id={system_b}, system_b_function='{func_b[:50] if func_b else 'EMPTY'}'...\n")
    
    # Check if systems exist
    sys_a = await fetchrow("SELECT id, kind FROM control.systems WHERE id = $1", system_a)
    sys_b = await fetchrow("SELECT id, kind FROM control.systems WHERE id = $1", system_b)
    
    if not sys_a or not sys_b:
        raise HTTPException(404, "One or both systems not found")
    
    try:
        row = await fetchrow("""
            INSERT INTO control.type_transformations 
                (system_a_id, system_b_id, system_a_function, system_b_function, updated_by)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """, system_a, system_b, func_a, func_b, user_email)
        
        return dict(row)
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "Type transformation already exists for this system pair")

@api.put("/type-transformations/{system_a_id}/{system_b_id}")
async def update_type_transformation(system_a_id: int, system_b_id: int, body: TypeTransformationUpdate):
    """Update type transformation for a system pair"""
    user_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    # Normalize the pair
    system_a = min(system_a_id, system_b_id)
    system_b = max(system_a_id, system_b_id)
    is_swapped = system_a_id != system_a
    
    # Get current version
    current = await fetchrow("""
        SELECT * FROM control.type_transformations 
        WHERE system_a_id = $1 AND system_b_id = $2
    """, system_a, system_b)
    
    if not current:
        raise HTTPException(404, "Type transformation not found")
    
    if current['version'] != body.version:
        raise HTTPException(409, "Version conflict - refresh and try again")
    
    # Build update query dynamically based on provided fields
    # If IDs were swapped, swap which function updates which column
    updates = []
    params = []
    param_idx = 1
    
    if body.system_a_function is not None:
        col = "system_b_function" if is_swapped else "system_a_function"
        updates.append(f"{col} = ${param_idx}")
        params.append(body.system_a_function)
        param_idx += 1
    
    if body.system_b_function is not None:
        col = "system_a_function" if is_swapped else "system_b_function"
        updates.append(f"{col} = ${param_idx}")
        params.append(body.system_b_function)
        param_idx += 1
    
    updates.append(f"updated_by = ${param_idx}")
    params.append(user_email)
    param_idx += 1
    
    updates.append(f"updated_at = now()")
    updates.append(f"version = version + 1")
    
    # Add WHERE clause params
    params.extend([system_a, system_b])
    
    row = await fetchrow(f"""
        UPDATE control.type_transformations
        SET {', '.join(updates)}
        WHERE system_a_id = ${param_idx} AND system_b_id = ${param_idx + 1}
        RETURNING *
    """, *params)
    
    return dict(row)

@api.delete("/type-transformations/{system_a_id}/{system_b_id}")
async def delete_type_transformation(system_a_id: int, system_b_id: int):
    """Delete type transformation for a system pair"""
    await require_role('CAN_MANAGE')
    
    system_a = min(system_a_id, system_b_id)
    system_b = max(system_a_id, system_b_id)
    
    result = await execute("""
        DELETE FROM control.type_transformations
        WHERE system_a_id = $1 AND system_b_id = $2
    """, system_a, system_b)
    
    return {"ok": True}

@api.post("/validate-python")
async def validate_python_code(body: ValidatePythonCode):
    """Validate Python code syntax and type hints"""
    import ast
    import tempfile
    import subprocess
    
    code = body.code
    errors = []
    
    # 1. Syntax validation
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append({
            "type": "syntax",
            "message": f"Syntax error at line {e.lineno}: {e.msg}",
            "line": e.lineno
        })
        return {"valid": False, "errors": errors}
    
    # 2. Check for function definition
    try:
        tree = ast.parse(code)
        functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
        
        if not functions:
            errors.append({
                "type": "structure",
                "message": "No function definition found. Must define a function named 'transform_columns'.",
                "line": 1
            })
        else:
            func = functions[0]
            if func.name != "transform_columns":
                errors.append({
                    "type": "structure",
                    "message": f"Function must be named 'transform_columns', found '{func.name}'",
                    "line": func.lineno
                })
            
            # Check function signature
            if len(func.args.args) != 2:
                errors.append({
                    "type": "signature",
                    "message": "Function must accept exactly 2 parameters: (column_name: str, data_type: str)",
                    "line": func.lineno
                })
    except Exception as e:
        errors.append({
            "type": "validation",
            "message": f"Validation error: {str(e)}",
            "line": 1
        })
    
    # 3. Type checking with mypy (optional, only if no syntax errors)
    if not errors:
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write(code)
                f.flush()
                temp_path = f.name
            
            result = subprocess.run(
                ['mypy', '--strict', '--no-error-summary', temp_path],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode != 0:
                # Parse mypy output
                for line in result.stdout.split('\n'):
                    if line.strip() and ':' in line:
                        parts = line.split(':', 3)
                        if len(parts) >= 4:
                            try:
                                line_num = int(parts[1])
                                message = parts[3].strip()
                                errors.append({
                                    "type": "type_hint",
                                    "message": message,
                                    "line": line_num
                                })
                            except (ValueError, IndexError):
                                pass
            
            os.unlink(temp_path)
        except subprocess.TimeoutExpired:
            errors.append({
                "type": "timeout",
                "message": "Type checking timed out",
                "line": 1
            })
        except FileNotFoundError:
            # mypy not installed, skip type checking
            pass
        except Exception as e:
            # Don't fail validation if mypy check fails
            pass
    
    return {
        "valid": len(errors) == 0,
        "errors": errors
    }

# ---------- Worker Helper Endpoints ----------
@api.get("/triggers/next")
async def get_next_trigger(worker_id: str = "worker-default"):
    """
    Worker polls this endpoint to get next trigger to execute.
    Uses SKIP LOCKED for atomic claiming.
    Automatically cleans up orphaned triggers and retries.
    """
    max_retries = 50  # Prevent infinite loop if something goes wrong
    
    for _ in range(max_retries):
        row = await fetchrow("""
            UPDATE control.triggers
            SET status = 'running',
                worker_id = $1,
                locked_at = now(),
                started_at = COALESCE(started_at, now()),
                attempts = attempts + 1
            WHERE id = (
                SELECT id FROM control.triggers
                WHERE status = 'queued'
                ORDER BY priority ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """, worker_id)
        
        if not row:
            return None  # Queue is truly empty
        
        # Fetch full entity details
        if row['entity_type'] == 'table':
            entity = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", row['entity_id'])
        else:
            entity = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", row['entity_id'])
        
        if not entity:
            # Entity was deleted, clean up orphaned trigger and retry
            await execute("DELETE FROM control.triggers WHERE id=$1", row['id'])
            print(f"[cleanup] Removed orphaned trigger {row['id']} (entity {row['entity_type']} #{row['entity_id']} no longer exists)")
            continue  # Try next trigger
        
        # Found a valid trigger, break out of retry loop
        break
    else:
        # Exhausted retries (unlikely, but safety net)
        return None
    
    # add system names for extra user transparency
    src_source_info = await fetchrow("SELECT * FROM control.systems WHERE id=$1", entity['src_system_id'])
    tgt_source_info = await fetchrow("SELECT * FROM control.systems WHERE id=$1", entity['tgt_system_id'])

    # Convert to dict so we can modify it
    result = dict(entity)
    result["id"] = row["id"]
    result["entity_type"] = row["entity_type"]
    if row["entity_type"] == 'table':
        result["source_table"] = f"{entity['src_schema'].strip()}.{entity['src_table'].strip()}"
        result["target_table"] = f"{entity['tgt_schema'].strip()}.{entity['tgt_table'].strip()}"
    result["watermark_expr"] = entity['watermark_filter']
    result["src_system_name"] = src_source_info["name"]
    result["tgt_system_name"] = tgt_source_info["name"]
    
    return result

@api.put("/triggers/{id}/update-run-id")
async def update_trigger_run_id(id: int, body: dict):
    """Worker calls this after launching Databricks job to record run ID."""
    await execute("""
        UPDATE control.triggers 
        SET databricks_run_id = $2, databricks_run_url = $3
        WHERE id = $1
    """, id, body['run_id'], body.get('run_url'))
    return {"ok": True}

@api.put("/triggers/{id}/release")
async def release_trigger(id: int):
    """
    Release a claimed trigger back to the queue.
    Used when concurrency limits prevent immediate launch.
    """
    await execute("""
        UPDATE control.triggers 
        SET status = 'queued',
            worker_id = NULL,
            locked_at = NULL
        WHERE id = $1 AND status = 'running'
    """, id)
    return {"ok": True}

@api.put("/triggers/{id}/fail")
async def fail_trigger(id: int, body: dict):
    """
    Worker calls this if it fails to launch the job (before Databricks runs).
    Records failure in history and removes from queue.
    """
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    
    # Record minimal history entry for the failure
    await execute("""
        INSERT INTO control.validation_history (
            trigger_id, entity_type, entity_id, entity_name,
            source, requested_by, requested_at, started_at, finished_at,
            source_system_id, target_system_id,
            source_system_name, target_system_name,
            compare_mode, status, error_message, error_details, databricks_run_id, databricks_run_url
        ) SELECT 
            $1, t.entity_type, t.entity_id, 
            CASE t.entity_type WHEN 'table' THEN d.name ELSE q.name END,
            t.source, t.requested_by, t.requested_at, t.started_at, now(),
            COALESCE(d.src_system_id, q.src_system_id),
            COALESCE(d.tgt_system_id, q.tgt_system_id),
            src.name, tgt.name,
            COALESCE(d.compare_mode, q.compare_mode),
            $2, $3, $4, t.databricks_run_id, t.databricks_run_url
        FROM control.triggers t
        LEFT JOIN control.datasets d ON t.entity_type = 'table' AND t.entity_id = d.id
        LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND t.entity_id = q.id
        LEFT JOIN control.systems src ON COALESCE(d.src_system_id, q.src_system_id) = src.id
        LEFT JOIN control.systems tgt ON COALESCE(d.tgt_system_id, q.tgt_system_id) = tgt.id
        WHERE t.id = $1
    """, id, body.get('status', 'error'), body.get('error_message', 'Worker failed to launch job'), json.dumps(body.get('error_details', {})))
    
    # Remove from queue
    await execute("DELETE FROM control.triggers WHERE id=$1", id)
    return {"ok": True}

# ---------- Tags ----------
@api.get("/tags")
async def list_tags():
    """Get all tags."""
    rows = await fetch("SELECT * FROM control.tags ORDER BY name")
    return [dict(r) for r in rows]

@api.post("/tags")
async def create_tag(body: dict):
    """Create a new tag (or return existing if name already exists)."""
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Tag name is required")
    
    # Check if tag already exists
    existing = await fetchrow("SELECT * FROM control.tags WHERE name = $1", name)
    if existing:
        return dict(existing)
    
    # Create new tag
    row = await fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING *", name)
    return dict(row)

@api.get("/tags/entity/{entity_type}/{entity_id}")
async def get_entity_tags(entity_type: str, entity_id: int):
    """Get all tags for a specific entity."""
    rows = await fetch("""
        SELECT t.id, t.name
        FROM control.tags t
        JOIN control.entity_tags et ON et.tag_id = t.id
        WHERE et.entity_type = $1 AND et.entity_id = $2
        ORDER BY t.name
    """, entity_type, entity_id)
    return [dict(r) for r in rows]

@api.post("/tags/entity/{entity_type}/{entity_id}")
async def set_entity_tags(entity_type: str, entity_id: int, body: dict):
    """Set tags for an entity (replaces existing tags)."""
    tag_names = body.get("tags", [])
    
    # Delete existing tags for this entity
    await execute("DELETE FROM control.entity_tags WHERE entity_type = $1 AND entity_id = $2", entity_type, entity_id)
    
    # Create tags if they don't exist and associate with entity
    for tag_name in tag_names:
        tag_name = tag_name.strip()
        if not tag_name:
            continue
        
        # Get or create tag
        tag = await fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
        if not tag:
            tag = await fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)
        
        # Associate tag with entity
        await execute("""
            INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
        """, entity_type, entity_id, tag['id'])
    
    # Clean up unused tags
    await execute("""
        DELETE FROM control.tags
        WHERE id NOT IN (SELECT DISTINCT tag_id FROM control.entity_tags)
    """)
    
    return {"ok": True}

@api.post("/tags/entity/bulk-add")
async def bulk_add_tags(body: dict):
    """Add tags to multiple entities."""
    entity_type = body.get("entity_type")
    entity_ids = body.get("entity_ids", [])
    tag_names = body.get("tags", [])
    
    if not entity_type or not entity_ids or not tag_names:
        raise HTTPException(status_code=400, detail="entity_type, entity_ids, and tags are required")
    
    for tag_name in tag_names:
        tag_name = tag_name.strip()
        if not tag_name:
            continue
        
        # Get or create tag
        tag = await fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
        if not tag:
            tag = await fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)
        
        # Associate tag with all entities
        for entity_id in entity_ids:
            await execute("""
                INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """, entity_type, entity_id, tag['id'])
    
    return {"ok": True}

@api.post("/tags/entity/bulk-remove")
async def bulk_remove_tags(body: dict):
    """Remove tags from multiple entities."""
    entity_type = body.get("entity_type")
    entity_ids = body.get("entity_ids", [])
    tag_names = body.get("tags", [])
    
    if not entity_type or not entity_ids or not tag_names:
        raise HTTPException(status_code=400, detail="entity_type, entity_ids, and tags are required")
    
    for tag_name in tag_names:
        tag_name = tag_name.strip()
        if not tag_name:
            continue
        
        # Get tag
        tag = await fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
        if not tag:
            continue
        
        # Remove association from all entities
        await execute("""
            DELETE FROM control.entity_tags
            WHERE entity_type = $1 AND entity_id = ANY($2) AND tag_id = $3
        """, entity_type, entity_ids, tag['id'])
    
    # Clean up unused tags
    await execute("""
        DELETE FROM control.tags
        WHERE id NOT IN (SELECT DISTINCT tag_id FROM control.entity_tags)
    """)
    
    return {"ok": True}

# ---------- Systems ----------
@api.get("/systems")
async def list_systems():
    rows = await fetch("SELECT * FROM control.systems WHERE is_active ORDER BY name")
    return [dict(r) for r in rows]

@api.post("/systems")
async def create_system(body: SystemIn):
    user_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    row = await fetchrow("""
        INSERT INTO control.systems (
          name, kind, catalog, host, port, database, secret_scope, user_secret_key, pass_secret_key, jdbc_string, driver_connector,
          concurrency, max_rows, options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, $12,$13,$14,$15,$16,$16
        ) RETURNING *
    """,
    body.name, body.kind, 
    body.catalog.strip() if body.catalog else None, 
    body.host.strip() if body.host else None, 
    body.port, 
    body.database.strip() if body.database else None,
    body.secret_scope.strip() if body.secret_scope else 'livevalidator',
    body.user_secret_key.strip() if body.user_secret_key else None, 
    body.pass_secret_key.strip() if body.pass_secret_key else None, 
    body.jdbc_string.strip() if body.jdbc_string else None,
    body.driver_connector.strip() if body.driver_connector else None,
    body.concurrency,
    body.max_rows,
    json.dumps(body.options) if body.options else '{}', body.is_active, user_email)
    return dict(row)

@api.get("/systems/{id}")
async def get_system(id: int):
    return await row_or_404("SELECT * FROM control.systems WHERE id=$1", id)

@api.get("/systems/name/{name}")
async def get_system_by_name(name: str):
    return await row_or_404("SELECT * FROM control.systems WHERE name=$1", name)

@api.put("/systems/{id}")
async def update_system(id: int, body: SystemUpdate):
    user_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    row = await fetchrow("""
        UPDATE control.systems SET
          name = COALESCE($2, name),
          kind = COALESCE($3, kind),
          catalog = COALESCE($4, catalog),
          host = COALESCE($5, host),
          port = COALESCE($6, port),
          database = COALESCE($7, database),
          secret_scope = COALESCE($8, secret_scope),
          user_secret_key = COALESCE($9, user_secret_key),
          pass_secret_key = COALESCE($10, pass_secret_key),
          jdbc_string = COALESCE($11, jdbc_string),
          driver_connector = COALESCE($12, driver_connector),
          concurrency = COALESCE($13, concurrency),
          max_rows = COALESCE($14, max_rows),
          options = COALESCE($15, options),
          is_active = COALESCE($16, is_active),
          updated_by = $17,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$18
        RETURNING *
    """,
    id, body.name, body.kind, body.catalog, body.host, body.port, body.database,
    body.secret_scope, body.user_secret_key, body.pass_secret_key, body.jdbc_string, body.driver_connector, body.concurrency,
    body.max_rows,
    json.dumps(body.options) if body.options else '{}', body.is_active, user_email, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.systems WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/systems/{id}")
async def delete_system(id: int):
    await require_role('CAN_MANAGE')
    await execute("DELETE FROM control.systems WHERE id=$1", id)
    return {"ok": True}

# ---------- User Role Management (Admin Only) ----------
@api.get("/admin/users")
async def list_user_roles():
    """List all users and their assigned roles"""
    await require_role('CAN_MANAGE')
    rows = await fetch("""
        SELECT user_email, role 
        FROM control.user_roles 
        ORDER BY user_email ASC
    """)
    return [dict(r) for r in rows]


@api.put("/admin/users/{user_email}/role")
async def set_user_role(user_email: str, role: str):
    """Set or update a user's role"""
    admin_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    # Validate role
    valid_roles = ['CAN_VIEW', 'CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE']
    if role not in valid_roles:
        raise HTTPException(400, f"Invalid role. Must be one of: {', '.join(valid_roles)}")
    
    # Update role
    await execute("""
        UPDATE control.user_roles 
        SET role = $2, assigned_by = $3, assigned_at = NOW()
        WHERE user_email = $1
    """, user_email, role, admin_email)
    
    return {"user_email": user_email, "role": role}


@api.delete("/admin/users/{user_email}/role")
async def delete_user_role(user_email: str):
    """Remove user's role assignment (reverts to default)"""
    await require_role('CAN_MANAGE')
    
    await execute("DELETE FROM control.user_roles WHERE user_email = $1", user_email)
    return {"user_email": user_email, "message": "Role removed, user will now have default role"}


# ---------- App Configuration (Admin Only) ----------
@api.get("/admin/config")
async def get_app_config():
    """Get all application configuration"""
    await require_role('CAN_MANAGE')
    rows = await fetch("SELECT key, value, description FROM control.app_config ORDER BY key")
    return [dict(r) for r in rows]


@api.put("/admin/config/{key}")
async def update_app_config(key: str, value: str):
    """Update a specific config value"""
    admin_email = get_user_email()
    await require_role('CAN_MANAGE')
    
    # Validate specific configs
    if key == 'default_user_role':
        valid_roles = ['CAN_VIEW', 'CAN_RUN', 'CAN_EDIT', 'CAN_MANAGE']
        if value not in valid_roles:
            raise HTTPException(400, f"Invalid role. Must be one of: {', '.join(valid_roles)}")
    
    # Update config
    await execute("""
        UPDATE control.app_config 
        SET value = $2, updated_by = $3, updated_at = NOW()
        WHERE key = $1
    """, key, value, admin_email)
    
    return {"key": key, "value": value}

# ---------- Setup / Database Reset ----------
@api.post("/setup/initialize-database")
async def initialize_database():
    """
    Initial setup: Creates schema and tables from DDL (safe, idempotent).
    """
    sql_dir = Path(__file__).resolve().parent / "sql"
    ddl_file = sql_dir / "ddl.sql"
    grants_file = sql_dir / "grants.sql"
    
    if not ddl_file.exists():
        raise HTTPException(status_code=500, detail=f"DDL file not found: {ddl_file}")
    
    # Read DDL and grants
    ddl_sql = ddl_file.read_text()
    grants_sql = grants_file.read_text() if grants_file.exists() else ""
    
    try:
        # Execute DDL (idempotent - uses IF NOT EXISTS)
        await execute(ddl_sql)
        
        # Execute grants (if present)
        if grants_sql:
            try:
                await execute(grants_sql)
            except Exception as e:
                # Grants might fail in local dev (user doesn't exist), that's ok
                print(f"[warn] Grants failed (might be ok in local dev): {e}")
        
        return {"ok": True, "message": "Database initialized successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database initialization failed: {str(e)}")

@api.post("/setup/reset-database")
async def reset_database():
    """
    ⚠️ DESTRUCTIVE: Drops all tables and recreates them from DDL.
    """
    sql_dir = Path(__file__).resolve().parent / "sql"
    drop_tables_file = sql_dir / "drop_tables.sql"
    ddl_file = sql_dir / "ddl.sql"
    
    if not drop_tables_file.exists():
        raise HTTPException(status_code=500, detail=f"Drop tables file not found: {drop_tables_file}")
    if not ddl_file.exists():
        raise HTTPException(status_code=500, detail=f"DDL file not found: {ddl_file}")
    
    # Read SQL files
    drop_tables_sql = drop_tables_file.read_text()
    ddl_sql = ddl_file.read_text()
    
    try:
        # 1. Drop all tables (but keep schema)
        await execute(drop_tables_sql)
        
        # 2. Execute DDL (recreate tables)
        await execute(ddl_sql)
        
        return {"ok": True, "message": "Database reset successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database reset failed: {str(e)}")

# ---------- Dashboards ----------

@api.get("/dashboards")
async def list_dashboards():
    """
    List dashboards visible to the current user.
    Visibility: own dashboards (any project) + dashboards in non-General projects (published).
    """
    user_email = get_user_email()
    rows = await fetch("""
        SELECT d.*,
            (SELECT COUNT(*) FROM control.dashboard_charts dc WHERE dc.dashboard_id = d.id) as chart_count
        FROM control.dashboards d
        WHERE d.created_by = $1 OR d.project != 'General'
        ORDER BY d.project, d.updated_at DESC
    """, user_email)
    return [serialize_row(r) for r in rows]


@api.get("/dashboards/projects")
async def list_projects():
    """Return distinct project names for autocomplete."""
    rows = await fetch("""
        SELECT DISTINCT project FROM control.dashboards
        WHERE project != 'General'
        ORDER BY project
    """)
    return [r['project'] for r in rows]


@api.get("/dashboards/{id}")
async def get_dashboard(id: int):
    """Get a dashboard with its charts."""
    user_email = get_user_email()
    dashboard = await fetchrow("SELECT * FROM control.dashboards WHERE id=$1", id)
    if not dashboard:
        raise HTTPException(404, "Dashboard not found")

    d = serialize_row(dashboard)
    if d['project'] == 'General' and d['created_by'] != user_email:
        raise HTTPException(403, "This dashboard is private")

    charts = await fetch(
        "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", id
    )
    d['charts'] = [serialize_row(c) for c in charts]
    return d


@api.post("/dashboards")
async def create_dashboard(body: DashboardIn):
    """Create a new dashboard with a default 'Overall' chart."""
    user_email = get_user_email()
    dashboard = await fetchrow("""
        INSERT INTO control.dashboards (name, project, created_by, updated_by)
        VALUES ($1, $2, $3, $3)
        RETURNING *
    """, body.name, body.project, user_email)

    await execute("""
        INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
        VALUES ($1, 'Overall', 0, '{}'::jsonb)
    """, dashboard['id'])

    d = serialize_row(dashboard)
    charts = await fetch(
        "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id",
        dashboard['id']
    )
    d['charts'] = [serialize_row(c) for c in charts]
    return d


@api.put("/dashboards/{id}")
async def update_dashboard(id: int, body: DashboardUpdate):
    """Update dashboard metadata with optimistic locking."""
    user_email = get_user_email()

    existing = await fetchrow("SELECT * FROM control.dashboards WHERE id=$1", id)
    if not existing:
        raise HTTPException(404, "Dashboard not found")
    if existing['created_by'] != user_email:
        role = await fetchrow("SELECT role FROM control.user_roles WHERE user_email=$1", user_email)
        if not role or role['role'] != 'CAN_MANAGE':
            raise HTTPException(403, "Only the dashboard creator or CAN_MANAGE users can update this dashboard")

    row = await fetchrow("""
        UPDATE control.dashboards SET
            name = COALESCE($2, name),
            project = COALESCE($3, project),
            time_range_preset = COALESCE($4, time_range_preset),
            time_range_from = COALESCE($5::timestamptz, time_range_from),
            time_range_to = COALESCE($6::timestamptz, time_range_to),
            updated_by = $7,
            updated_at = now(),
            version = version + 1
        WHERE id=$1 AND version=$8
        RETURNING *
    """, id, body.name, body.project, body.time_range_preset,
        body.time_range_from, body.time_range_to, user_email, body.version)

    if not row:
        current = await fetchrow("SELECT * FROM control.dashboards WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={
            "error": "version_conflict",
            "current": serialize_row(current) if current else None
        })
    return serialize_row(row)


@api.delete("/dashboards/{id}")
async def delete_dashboard(id: int):
    """Delete a dashboard. Only the creator or CAN_MANAGE users."""
    user_email = get_user_email()
    existing = await fetchrow("SELECT * FROM control.dashboards WHERE id=$1", id)
    if not existing:
        raise HTTPException(404, "Dashboard not found")
    if existing['created_by'] != user_email:
        role = await fetchrow("SELECT role FROM control.user_roles WHERE user_email=$1", user_email)
        if not role or role['role'] != 'CAN_MANAGE':
            raise HTTPException(403, "Only the dashboard creator or CAN_MANAGE users can delete this dashboard")
    await execute("DELETE FROM control.dashboards WHERE id=$1", id)
    return {"ok": True}


@api.post("/dashboards/{id}/clone")
async def clone_dashboard(id: int, body: dict = {}):
    """Clone a dashboard and all its charts."""
    user_email = get_user_email()
    source = await fetchrow("SELECT * FROM control.dashboards WHERE id=$1", id)
    if not source:
        raise HTTPException(404, "Dashboard not found")

    clone_name = body.get('name') or f"{source['name']} (Copy)"
    clone_project = body.get('project') or 'General'

    new_dash = await fetchrow("""
        INSERT INTO control.dashboards (name, project, time_range_preset, time_range_from, time_range_to, created_by, updated_by)
        VALUES ($1, $2, $3, $4, $5, $6, $6)
        RETURNING *
    """, clone_name, clone_project, source['time_range_preset'],
        source['time_range_from'], source['time_range_to'], user_email)

    charts = await fetch(
        "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", id
    )
    for chart in charts:
        src_filters = chart['filters'] if isinstance(chart['filters'], dict) else {}
        await execute("""
            INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
            VALUES ($1, $2, $3, $4::jsonb)
        """, new_dash['id'], chart['name'], chart['sort_order'], json.dumps(src_filters))

    d = serialize_row(new_dash)
    new_charts = await fetch(
        "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id",
        new_dash['id']
    )
    d['charts'] = [serialize_row(c) for c in new_charts]
    return d


# -- Dashboard Charts --

@api.post("/dashboards/{id}/charts")
async def add_chart(id: int, body: ChartIn):
    """Add a chart to a dashboard."""
    user_email = get_user_email()
    dashboard = await fetchrow("SELECT created_by FROM control.dashboards WHERE id=$1", id)
    if not dashboard:
        raise HTTPException(404, "Dashboard not found")
    if dashboard['created_by'] != user_email:
        await require_role('CAN_MANAGE')

    row = await fetchrow("""
        INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING *
    """, id, body.name, body.sort_order, json.dumps(body.filters))

    await execute("UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", id, user_email)
    return serialize_row(row)


@api.put("/dashboards/{id}/charts/reorder")
async def reorder_charts(id: int, body: ChartReorder):
    """Bulk update chart sort_order based on provided order."""
    user_email = get_user_email()
    dashboard = await fetchrow("SELECT created_by FROM control.dashboards WHERE id=$1", id)
    if not dashboard:
        raise HTTPException(404, "Dashboard not found")
    if dashboard['created_by'] != user_email:
        await require_role('CAN_MANAGE')

    for idx, chart_id in enumerate(body.chart_ids):
        await execute("""
            UPDATE control.dashboard_charts SET sort_order=$2, updated_at=now()
            WHERE id=$1 AND dashboard_id=$3
        """, chart_id, idx, id)

    await execute("UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", id, user_email)
    return {"ok": True}


@api.put("/dashboards/{id}/charts/{chart_id}")
async def update_chart(id: int, chart_id: int, body: ChartUpdate):
    """Update a chart's name, filters, or sort_order."""
    user_email = get_user_email()
    dashboard = await fetchrow("SELECT created_by FROM control.dashboards WHERE id=$1", id)
    if not dashboard:
        raise HTTPException(404, "Dashboard not found")
    if dashboard['created_by'] != user_email:
        await require_role('CAN_MANAGE')

    chart = await fetchrow("SELECT * FROM control.dashboard_charts WHERE id=$1 AND dashboard_id=$2", chart_id, id)
    if not chart:
        raise HTTPException(404, "Chart not found")

    filters_json = json.dumps(body.filters) if body.filters is not None else None
    row = await fetchrow("""
        UPDATE control.dashboard_charts SET
            name = COALESCE($2, name),
            filters = COALESCE($3::jsonb, filters),
            sort_order = COALESCE($4, sort_order),
            updated_at = now()
        WHERE id=$1
        RETURNING *
    """, chart_id, body.name, filters_json, body.sort_order)

    await execute("UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", id, user_email)
    return serialize_row(row)


@api.delete("/dashboards/{id}/charts/{chart_id}")
async def delete_chart(id: int, chart_id: int):
    """Remove a chart from a dashboard."""
    user_email = get_user_email()
    dashboard = await fetchrow("SELECT created_by FROM control.dashboards WHERE id=$1", id)
    if not dashboard:
        raise HTTPException(404, "Dashboard not found")
    if dashboard['created_by'] != user_email:
        await require_role('CAN_MANAGE')

    result = await execute("DELETE FROM control.dashboard_charts WHERE id=$1 AND dashboard_id=$2", chart_id, id)
    await execute("UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", id, user_email)
    return {"ok": True}


# ---------- Wire API ----------
app.include_router(api)

# ---------- Robust SPA mounting (works from backend/ or anywhere) ----------
def _pick_frontend_dir() -> Optional[Path]:
    base = Path(__file__).resolve().parent
    env = os.getenv("FRONTEND_DIR")
    candidates = []

    if env:
        candidates.append(Path(env))

    # Common layouts:
    # repo/
    #   backend/app.py
    #   frontend/dist  (Vite)
    #   frontend/build (CRA)
    candidates += [
        base / ".." / "frontend" / "dist",   # Vite (sibling)
        base / ".." / "frontend" / "build",  # CRA (sibling)
        base / "frontend" / "dist",          # Vite (nested)
        base / "frontend" / "build",         # CRA (nested)
        base / ".." / "dist",                # direct sibling dist
        base / ".." / "build",               # direct sibling build
        base / "dist",
        base / "build",
    ]

    for p in candidates:
        p = p.resolve()
        if (p / "index.html").exists():
            print(f"[info] Serving SPA from: {p}")
            return p

    print("[warn] Could not find a built frontend. Looked in:")
    for p in candidates:
        print(f"  - {p}")
    return None

_FRONTEND_DIR = _pick_frontend_dir()

if _FRONTEND_DIR:
    # Serve static files (JS/CSS/assets) and index.html at "/"
    app.mount("/", StaticFiles(directory=str(_FRONTEND_DIR), html=True), name="spa")

    assets_dir = _FRONTEND_DIR / "assets"
    static_dir = _FRONTEND_DIR / "static"

    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
        print(f"[info] Mounted /assets -> {assets_dir}")

    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
        print(f"[info] Mounted /static -> {static_dir}")

    # SPA history fallback for client-side routes (e.g., /settings, /queries/123)
    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str):
        index_path = _FRONTEND_DIR / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        raise HTTPException(status_code=404, detail="index.html not found")

    # Optional: silence favicon 404s if missing
    @app.get("/favicon.ico")
    def favicon():
        ico = _FRONTEND_DIR / "favicon.ico"
        return FileResponse(str(ico)) if ico.exists() else Response(status_code=204)
else:
    @app.get("/")
    def _missing_build():
        return {
            "error": "frontend_build_not_found",
            "hint": "Set FRONTEND_DIR or run `npm run build` in your frontend and place index.html under one of the common locations."
        }
