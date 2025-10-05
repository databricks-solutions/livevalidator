import os
import json
from pathlib import Path
from typing import Optional, Literal

from fastapi import FastAPI, APIRouter, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
import asyncpg

from backend.db import fetch, fetchrow, fetchval, execute

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

# ---------- Schemas ----------
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
    # partial update + optimistic token
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

class ScheduleIn(BaseModel):
    name: str
    cron_expr: str
    timezone: str = 'UTC'
    enabled: bool = True
    max_concurrency: int = 4
    backfill_policy: Literal['none','catch_up','skip_missed'] = 'none'
    updated_by: str

class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    cron_expr: Optional[str] = None
    timezone: Optional[str] = None
    enabled: Optional[bool] = None
    max_concurrency: Optional[int] = None
    backfill_policy: Optional[Literal['none','catch_up','skip_missed']] = None
    updated_by: str
    version: int

class BindingIn(BaseModel):
    schedule_id: int
    entity_type: Literal['dataset','compare_query']
    entity_id: int

class TriggerIn(BaseModel):
    entity_type: Literal['dataset','compare_query']
    entity_id: int
    requested_by: str
    priority: int = 100
    params: dict = Field(default_factory=dict)

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
    options: Optional[dict] = None
    is_active: Optional[bool] = None
    updated_by: str
    version: int

# ---------- Helpers ----------
async def row_or_404(sql: str, *args):
    row = await fetchrow(sql, *args)
    if not row:
        raise HTTPException(404, "not found")
    return dict(row)

# ---------- Tables ----------
@api.get("/tables")
async def list_tables(q: str | None = None):
    if q:
        rows = await fetch("""
            SELECT * FROM control.datasets
            WHERE is_active AND (name ILIKE $1 OR $1 = '')
            ORDER BY name
        """, f"%{q}%")
    else:
        rows = await fetch("SELECT * FROM control.datasets WHERE is_active ORDER BY name")
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
                        VALUES ($1, 'dataset', $2)
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
        rows = await fetch(
            "SELECT * FROM control.compare_queries WHERE is_active AND name ILIKE $1 ORDER BY name",
            f"%{q}%"
        )
    else:
        rows = await fetch("SELECT * FROM control.compare_queries WHERE is_active ORDER BY name")
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
          updated_by = $8,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$9
        RETURNING *
    """, id, body.name, body.cron_expr, body.timezone, body.enabled, body.max_concurrency, body.backfill_policy, body.updated_by, body.version)
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

@api.delete("/bindings/{id}")
async def delete_binding(id: int):
    await execute("DELETE FROM control.schedule_bindings WHERE id=$1", id)
    return {"ok": True}

# ---------- Trigger now ----------
@api.post("/triggers")
async def trigger_now(t: TriggerIn):
    row = await fetchrow("""
        INSERT INTO control.triggers (source, schedule_id, entity_type, entity_id, priority, requested_by, params)
        VALUES ('manual', NULL, $1, $2, $3, $4, $5)
        RETURNING *
    """, t.entity_type, t.entity_id, t.priority, t.requested_by, json.dumps(t.params) if isinstance(t.params, (dict, list)) else t.params)
    return dict(row)

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
          options, is_active, created_by, updated_by
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9, $10,$11,$12,$12
        ) RETURNING *
    """,
    body.name, body.kind, body.catalog, body.host, body.port, body.database, 
    body.user_secret_key, body.pass_secret_key, body.jdbc_string,
    json.dumps(body.options) if isinstance(body.options, (dict, list)) else body.options, body.is_active, body.updated_by)
    return dict(row)

@api.get("/systems/{id}")
async def get_system(id: int):
    return await row_or_404("SELECT * FROM control.systems WHERE id=$1", id)

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
          options = COALESCE($11, options),
          is_active = COALESCE($12, is_active),
          updated_by = $13,
          updated_at = now(),
          version = version + 1
        WHERE id=$1 AND version=$14
        RETURNING *
    """,
    id, body.name, body.kind, body.catalog, body.host, body.port, body.database, 
    body.user_secret_key, body.pass_secret_key, body.jdbc_string,
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
