import os
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import available_timezones

from fastapi import FastAPI, APIRouter, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import asyncpg

from backend.db import fetch, fetchrow, fetchval, execute
from backend.models import (
    TableIn, TableUpdate, BulkTableItem, BulkTableRequest,
    QueryIn, QueryUpdate, BulkQueryItem, BulkQueryRequest,
    ScheduleIn, ScheduleUpdate, BindingIn,
    TriggerIn, SystemIn, SystemUpdate,
    TypeTransformationIn, TypeTransformationUpdate, ValidatePythonCode
)
from backend.default_transformations import get_default_transformation

app = FastAPI(title="LiveValidator Control Plane API", version="0.1")

# Keep API isolated under /api so SPA routing can own "/"
api = APIRouter(prefix="/api")

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
    """
    Catch database table not found errors and direct user to setup.
    """
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database not initialized",
            "action": "setup_required",
            "message": "Please go to the Setup tab and click 'Initialize Database'"
        }
)

# ---------- Helpers ----------
async def row_or_404(sql: str, *args):
    row = await fetchrow(sql, *args)
    if not row:
        raise HTTPException(404, "not found")
    return dict(row)


@api.get("/secrets")
async def question():
    return (os.environ.get("DATABRICKS_CLIENT_ID"), os.environ.get("DATABRICKS_CLIENT_SECRET"))

# ---------- Tables ----------
@api.get("/tables")
async def list_tables(q: str | None = None):
    if q:
        rows = await fetch("""
            SELECT 
                d.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'table' AND et.entity_id = d.id),
                    '[]'::json
                ) as tags
            FROM control.datasets d
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at
                FROM control.validation_history
                WHERE entity_type = 'table' AND entity_id = d.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE d.is_active AND (d.name ILIKE $1 OR $1 = '')
            ORDER BY d.name
        """, f"%{q}%")
    else:
        rows = await fetch("""
            SELECT 
                d.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'table' AND et.entity_id = d.id),
                    '[]'::json
                ) as tags
            FROM control.datasets d
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at
                FROM control.validation_history
                WHERE entity_type = 'table' AND entity_id = d.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE d.is_active
            ORDER BY d.name
        """)
    return [dict(r) for r in rows]

@api.post("/tables")
async def create_table(body: TableIn):
    row = await fetchrow("""
        INSERT INTO control.datasets (
          name, src_system_id, src_schema, src_table,
          tgt_system_id, tgt_schema, tgt_table,
          compare_mode, pk_columns, watermark_column, include_columns, exclude_columns,
          options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$11,$12, $13,$14,$15,$15
        ) RETURNING *
    """,
    body.name, body.src_system_id, body.src_schema, body.src_table,
    body.tgt_system_id, body.tgt_schema, body.tgt_table,
    body.compare_mode, body.pk_columns, body.watermark_column, body.include_columns, body.exclude_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by)
    return dict(row)

@api.get("/tables/{id}")
async def get_table(id: int):
    return await row_or_404("SELECT * FROM control.datasets WHERE id=$1", id)

@api.put("/tables/{id}")
async def update_table(id: int, body: TableUpdate):
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
          watermark_column = COALESCE($11, watermark_column),
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
    body.compare_mode, body.pk_columns, body.watermark_column, body.include_columns, body.exclude_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/tables/{id}")
async def delete_table(id: int):
    await execute("DELETE FROM control.datasets WHERE id=$1", id)
    return {"ok": True}

@api.post("/tables/bulk")
async def bulk_create_tables(body: BulkTableRequest):
    results = {"created": [], "updated": [], "errors": []}
    
    for idx, item in enumerate(body.items):
        try:
            # Apply defaults
            name = item.name or f"{item.src_schema}.{item.src_table}"
            tgt_schema = item.tgt_schema or item.src_schema
            tgt_table = item.tgt_table or item.src_table
            
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
                      watermark_column = $10,
                      include_columns = $11,
                      exclude_columns = $12,
                      is_active = $13,
                      updated_by = $14,
                      updated_at = now(),
                      version = version + 1
                    WHERE id=$1
                    RETURNING *
                """,
                existing['id'], body.src_system_id, item.src_schema, item.src_table,
                body.tgt_system_id, tgt_schema, tgt_table,
                item.compare_mode, item.pk_columns, item.watermark_column,
                item.include_columns or [], item.exclude_columns or [],
                item.is_active, body.updated_by)
                results["updated"].append({"row": idx + 1, "name": name, "data": dict(row)})
            else:
                # Create new
                row = await fetchrow("""
                    INSERT INTO control.datasets (
                      name, src_system_id, src_schema, src_table,
                      tgt_system_id, tgt_schema, tgt_table,
                      compare_mode, pk_columns, watermark_column,
                      include_columns, exclude_columns,
                      is_active, created_by, updated_by
                    ) VALUES (
                      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14
                    ) RETURNING *
                """,
                name, body.src_system_id, item.src_schema, item.src_table,
                body.tgt_system_id, tgt_schema, tgt_table,
                item.compare_mode, item.pk_columns, item.watermark_column,
                item.include_columns or [], item.exclude_columns or [],
                item.is_active, body.updated_by)
                
                # Bind to schedule
                schedule = await fetchrow("SELECT id FROM control.schedules WHERE name=$1", item.schedule_name)
                if schedule:
                    await execute("""
                        INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                        VALUES ($1, 'table', $2)
                        ON CONFLICT DO NOTHING
                    """, schedule['id'], row['id'])
                
                results["created"].append({"row": idx + 1, "name": name, "data": dict(row)})
        except Exception as e:
            results["errors"].append({"row": idx + 1, "error": str(e)})
    
    return results

# ---------- Compare Queries ----------
@api.get("/queries")
async def list_queries(q: str | None = None):
    if q:
        rows = await fetch("""
            SELECT 
                cq.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'query' AND et.entity_id = cq.id),
                    '[]'::json
                ) as tags
            FROM control.compare_queries cq
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at
                FROM control.validation_history
                WHERE entity_type = 'compare_query' AND entity_id = cq.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE cq.is_active AND cq.name ILIKE $1
            ORDER BY cq.name
        """, f"%{q}%")
    else:
        rows = await fetch("""
            SELECT 
                cq.*,
                vh.id as last_run_id,
                vh.status as last_run_status,
                vh.finished_at as last_run_timestamp,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et
                     JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = 'query' AND et.entity_id = cq.id),
                    '[]'::json
                ) as tags
            FROM control.compare_queries cq
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at
                FROM control.validation_history
                WHERE entity_type = 'compare_query' AND entity_id = cq.id
                ORDER BY finished_at DESC
                LIMIT 1
            ) vh ON true
            WHERE cq.is_active
            ORDER BY cq.name
        """)
    return [dict(r) for r in rows]

@api.post("/queries")
async def create_query(body: QueryIn):
    row = await fetchrow("""
        INSERT INTO control.compare_queries (
          name, src_system_id, tgt_system_id, sql,
          compare_mode, pk_columns,
          options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4, $5,$6, $7,$8,$9,$9
        ) RETURNING *
    """,
    body.name, body.src_system_id, body.tgt_system_id, body.sql,
    body.compare_mode, body.pk_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by)
    return dict(row)

@api.get("/queries/{id}")
async def get_query(id: int):
    return await row_or_404("SELECT * FROM control.compare_queries WHERE id=$1", id)

@api.put("/queries/{id}")
async def update_query(id: int, body: QueryUpdate):
    row = await fetchrow("""
        UPDATE control.compare_queries SET
          name = COALESCE($2, name),
          src_system_id = COALESCE($3, src_system_id),
          tgt_system_id = COALESCE($4, tgt_system_id),
          sql           = COALESCE($5, sql),
          compare_mode  = COALESCE($6, compare_mode),
          pk_columns    = COALESCE($7, pk_columns),
          options = COALESCE($8, options),
          is_active = COALESCE($9, is_active),
          updated_by = $10,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$11
        RETURNING *
    """,
    id, body.name, body.src_system_id, body.tgt_system_id, body.sql,
    body.compare_mode, body.pk_columns,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/queries/{id}")
async def delete_query(id: int):
    await execute("DELETE FROM control.compare_queries WHERE id=$1", id)
    return {"ok": True}

@api.post("/queries/bulk")
async def bulk_create_queries(body: BulkQueryRequest):
    results = {"created": [], "updated": [], "errors": []}
    
    for idx, item in enumerate(body.items):
        try:
            # Apply defaults
            name = item.name or f"Query {idx + 1}"
            
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
                      is_active = $7,
                      updated_by = $8,
                      updated_at = now(),
                      version = version + 1
                    WHERE id=$1
                    RETURNING *
                """,
                existing['id'], body.src_system_id, item.sql, body.tgt_system_id,
                item.compare_mode, item.pk_columns, 
                item.is_active, body.updated_by)
                results["updated"].append({"row": idx + 1, "name": name, "data": dict(row)})
            else:
                # Create new
                row = await fetchrow("""
                    INSERT INTO control.compare_queries (
                      name, src_system_id, sql, tgt_system_id,
                      compare_mode, pk_columns,
                      is_active, created_by, updated_by
                    ) VALUES (
                      $1,$2,$3,$4,$5,$6,$7,$8,$8
                    ) RETURNING *
                """,
                name, body.src_system_id, item.sql, body.tgt_system_id,
                item.compare_mode, item.pk_columns,
                item.is_active, body.updated_by)
                
                # Bind to schedule
                schedule = await fetchrow("SELECT id FROM control.schedules WHERE name=$1", item.schedule_name)
                if schedule:
                    await execute("""
                        INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                        VALUES ($1, 'compare_query', $2)
                        ON CONFLICT DO NOTHING
                    """, schedule['id'], row['id'])
                
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
    row = await fetchrow("""
        INSERT INTO control.schedules (name, cron_expr, timezone, enabled, max_concurrency, backfill_policy, created_by, updated_by)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$7) RETURNING *
    """, body.name, body.cron_expr, body.timezone, body.enabled, body.max_concurrency, body.backfill_policy, body.updated_by)
    return dict(row)

@api.put("/schedules/{id}")
async def update_schedule(id: int, body: ScheduleUpdate):
    row = await fetchrow("""
        UPDATE control.schedules SET
          name = COALESCE($2, name),
          cron_expr = COALESCE($3, cron_expr),
          timezone  = COALESCE($4, timezone),
          enabled   = COALESCE($5, enabled),
          max_concurrency = COALESCE($6, max_concurrency),
          backfill_policy = COALESCE($7, backfill_policy),
          last_run_at = COALESCE($8, last_run_at),
          next_run_at = COALESCE($9, next_run_at),
          updated_by = $10,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$11
        RETURNING *
    """, 
    id, body.name, body.cron_expr, body.timezone, body.enabled, body.max_concurrency, body.backfill_policy, 
    datetime.fromisoformat(body.last_run_at) if body.last_run_at else None, 
    datetime.fromisoformat(body.next_run_at) if body.next_run_at else None, 
    body.updated_by, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.schedules WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/schedules/{id}")
async def delete_schedule(id: int):
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

@api.get("/bindings_by_sched/{schedule_id}")
async def list_bindings_by_schedule(schedule_id: int):
    rows = await fetch("SELECT * FROM control.schedule_bindings WHERE schedule_id=$1", schedule_id)
    return [dict(r) for r in rows]

@api.delete("/bindings/{id}")
async def delete_binding(id: int):
    await execute("DELETE FROM control.schedule_bindings WHERE id=$1", id)
    return {"ok": True}

# ---------- Trigger now ----------
@api.get("/triggers")
async def list_triggers(status: str | None = None):
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
                   END as entity_name
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
                   END as entity_name
            FROM control.triggers t
            LEFT JOIN control.datasets d ON t.entity_type = 'table' AND t.entity_id = d.id
            LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND t.entity_id = q.id
            ORDER BY t.status, t.priority ASC, t.id ASC
        """)
    return [dict(r) for r in rows]

@api.post("/triggers")
async def create_trigger(body: TriggerIn):
    """
    Create a new validation trigger.
    Called by UI "Run Now" button or by scheduler.
    """
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
    
    # Create trigger
    row = await fetchrow("""
        INSERT INTO control.triggers (
            source, schedule_id, entity_type, entity_id,
            priority, requested_by, requested_at, params
        ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7)
        RETURNING *
    """, body.source, body.schedule_id, body.entity_type, body.entity_id,
         body.priority, body.requested_by, json.dumps(body.params) if isinstance(body.params, (dict, list)) else body.params)
    
    return dict(row)

@api.post("/triggers/bulk")
async def create_triggers_bulk(triggers: list[TriggerIn]):
    """
    Create multiple validation triggers in one transaction.
    Uses bulk INSERT with conflict detection - skips duplicates silently.
    """
    if not triggers:
        return {"created": []}
    
    # Extract arrays for each column
    sources = [t.source for t in triggers]
    schedule_ids = [t.schedule_id for t in triggers]
    entity_types = [t.entity_type for t in triggers]
    entity_ids = [t.entity_id for t in triggers]
    priorities = [t.priority for t in triggers]
    requested_bys = [t.requested_by for t in triggers]
    params_json = [json.dumps(t.params) if isinstance(t.params, (dict, list)) else t.params for t in triggers]
    
    # Bulk INSERT using unnest() - clean and efficient
    rows = await fetch("""
        INSERT INTO control.triggers (
            source, schedule_id, entity_type, entity_id,
            priority, requested_by, requested_at, params
        )
        SELECT source, schedule_id, entity_type, entity_id, priority, requested_by, now(), params::jsonb
        FROM unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::int[], $6::text[], $7::text[])
            AS t(source, schedule_id, entity_type, entity_id, priority, requested_by, params)
        WHERE NOT EXISTS (
            SELECT 1 FROM control.triggers
            WHERE entity_type = t.entity_type 
            AND entity_id = t.entity_id 
            AND status IN ('queued', 'running')
        )
        RETURNING *
    """, sources, schedule_ids, entity_types, entity_ids, priorities, requested_bys, params_json)
    
    return {"created": [dict(r) for r in rows]}

@api.delete("/triggers/{id}")
async def cancel_trigger(id: int):
    """Cancel a queued trigger (can't cancel running)."""
    trigger = await fetchrow("SELECT * FROM control.triggers WHERE id=$1", id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    
    if trigger['status'] == 'running':
        raise HTTPException(status_code=400, detail="Cannot cancel running trigger")
    
    await execute("DELETE FROM control.triggers WHERE id=$1", id)
    return {"ok": True}

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
@api.get("/validation-history")
async def list_validation_history(
    entity_type: str | None = None,
    entity_id: int | None = None,
    status: str | None = None,
    schedule_id: int | None = None,
    days_back: int = 30,
    limit: int = 100,
    offset: int = 0
):
    """
    Get validation history with filters.
    Used by History UI view and entity detail pages.
    """
    conditions = []
    params = []
    param_idx = 1
    
    # Filter by time range
    if days_back > 0:
        conditions.append(f"vh.requested_at >= NOW() - INTERVAL '{days_back} days'")
    
    if entity_type:
        conditions.append(f"vh.entity_type = ${param_idx}")
        params.append(entity_type)
        param_idx += 1
    
    if entity_id:
        conditions.append(f"vh.entity_id = ${param_idx}")
        params.append(entity_id)
        param_idx += 1
    
    if status:
        conditions.append(f"vh.status = ${param_idx}")
        params.append(status)
        param_idx += 1
    
    if schedule_id:
        conditions.append(f"vh.schedule_id = ${param_idx}")
        params.append(schedule_id)
        param_idx += 1
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    rows = await fetch(f"""
        SELECT 
            vh.id, vh.trigger_id, vh.entity_type, vh.entity_id, vh.entity_name,
            vh.source, vh.schedule_id, vh.requested_by, vh.requested_at,
            vh.started_at, vh.finished_at, vh.duration_seconds,
            vh.source_system_name, vh.target_system_name,
            vh.status, vh.schema_match, vh.row_count_match,
            vh.row_count_source, vh.row_count_target,
            vh.rows_compared, vh.rows_different, vh.difference_pct,
            vh.error_message, vh.databricks_run_url,
            COALESCE(
                (SELECT json_agg(t.name ORDER BY t.name)
                 FROM control.entity_tags et
                 JOIN control.tags t ON et.tag_id = t.id
                 WHERE et.entity_type = CASE 
                     WHEN vh.entity_type = 'table' THEN 'table'
                     WHEN vh.entity_type = 'compare_query' THEN 'query'
                     ELSE vh.entity_type
                 END AND et.entity_id = vh.entity_id),
                '[]'::json
            ) as tags
        FROM control.validation_history vh
        {where_clause}
        ORDER BY vh.finished_at DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """, *params, limit, offset)
    
    return [dict(r) for r in rows]

@api.get("/validation-history/{id}")
async def get_validation_detail(id: int):
    """Get full validation details including sample differences."""
    row = await fetchrow("SELECT * FROM control.validation_history WHERE id=$1", id)
    if not row:
        raise HTTPException(status_code=404, detail="Validation not found")
    return dict(row)

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
    return dict(row)

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
        raise Exception(f"Trigger '{body['trigger_id']}' not found")

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
        """, system_a, system_b, func_a, func_b, body.updated_by)
        
        return dict(row)
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "Type transformation already exists for this system pair")

@api.put("/type-transformations/{system_a_id}/{system_b_id}")
async def update_type_transformation(system_a_id: int, system_b_id: int, body: TypeTransformationUpdate):
    """Update type transformation for a system pair"""
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
    params.append(body.updated_by)
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
    """
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
        return None
    
    # Fetch full entity details
    if row['entity_type'] == 'table':
        entity = await fetchrow("SELECT * FROM control.datasets WHERE id=$1", row['entity_id'])
    else:
        entity = await fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", row['entity_id'])
    
    if not entity:
        # Entity was deleted, clean up trigger
        await execute("DELETE FROM control.triggers WHERE id=$1", row['id'])
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
            compare_mode, status, error_message, databricks_run_id, databricks_run_url
        ) SELECT 
            $1, t.entity_type, t.entity_id, 
            CASE t.entity_type WHEN 'table' THEN d.name ELSE q.name END,
            t.source, t.requested_by, t.requested_at, t.started_at, now(),
            COALESCE(d.src_system_id, q.src_system_id),
            COALESCE(d.tgt_system_id, q.tgt_system_id),
            src.name, tgt.name,
            COALESCE(d.compare_mode, q.compare_mode),
            $2, $3, t.databricks_run_id, t.databricks_run_url
        FROM control.triggers t
        LEFT JOIN control.datasets d ON t.entity_type = 'table' AND t.entity_id = d.id
        LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND t.entity_id = q.id
        LEFT JOIN control.systems src ON COALESCE(d.src_system_id, q.src_system_id) = src.id
        LEFT JOIN control.systems tgt ON COALESCE(d.tgt_system_id, q.tgt_system_id) = tgt.id
        WHERE t.id = $1
    """, id, body.get('status', 'Error'), body.get('error', 'Worker failed to launch job'))
    
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
    row = await fetchrow("""
        INSERT INTO control.systems (
          name, kind, catalog, host, port, database, user_secret_key, pass_secret_key, jdbc_string,
          concurrency, options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9, $10,$11,$12,$13,$13
        ) RETURNING *
    """,
    body.name, body.kind, body.catalog.strip(), body.host.strip(), body.port, body.database.strip(), 
    body.user_secret_key.strip(), body.pass_secret_key.strip(), body.jdbc_string.strip(), body.concurrency,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by)
    return dict(row)

@api.get("/systems/{id}")
async def get_system(id: int):
    return await row_or_404("SELECT * FROM control.systems WHERE id=$1", id)

@api.get("/systems/name/{name}")
async def get_system_by_name(name: str):
    return await row_or_404("SELECT * FROM control.systems WHERE name=$1", name)

@api.put("/systems/{id}")
async def update_system(id: int, body: SystemUpdate):
    row = await fetchrow("""
        UPDATE control.systems SET
          name = COALESCE($2, name),
          kind = COALESCE($3, kind),
          catalog = COALESCE($4, catalog),
          host = COALESCE($5, host),
          port = COALESCE($6, port),
          database = COALESCE($7, database),
          user_secret_key = COALESCE($8, user_secret_key),
          pass_secret_key = COALESCE($9, pass_secret_key),
          jdbc_string = COALESCE($10, jdbc_string),
          concurrency = COALESCE($11, concurrency),
          options = COALESCE($12, options),
          is_active = COALESCE($13, is_active),
          updated_by = $14,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$15
        RETURNING *
    """,
    id, body.name, body.kind, body.catalog, body.host, body.port, body.database, 
    body.user_secret_key, body.pass_secret_key, body.jdbc_string, body.concurrency,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by, body.version)
    if not row:
        current = await fetchrow("SELECT * FROM control.systems WHERE id=$1", id)
        raise HTTPException(status_code=409, detail={"error":"version_conflict", "current": dict(current) if current else None})
    return dict(row)

@api.delete("/systems/{id}")
async def delete_system(id: int):
    await execute("DELETE FROM control.systems WHERE id=$1", id)
    return {"ok": True}

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
