"""LiveValidator Control Plane API - Main application."""

import os
from pathlib import Path

import asyncpg
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.dependencies import set_current_user_email
from backend.routers import (
    admin_router,
    dashboards_router,
    misc_router,
    queries_router,
    schedules_router,
    setup_router,
    systems_router,
    tables_router,
    tags_router,
    triggers_router,
    type_transformations_router,
    validation_config_router,
    validation_history_router,
)
from backend.services.users_service import UsersService

app = FastAPI(title="LiveValidator Control Plane API", version="0.1")


# ---------- Middleware ----------
@app.middleware("http")
async def user_email_middleware(request: Request, call_next):
    """Extract user email from headers and auto-create user entries."""
    email = request.headers.get("x-forwarded-email", "local-admin@localhost")
    set_current_user_email(email)

    if request.url.path.startswith("/api") and not request.url.path.startswith("/api/admin"):
        try:
            from backend.dependencies import get_db

            db = await get_db()
            users = UsersService(db)
            await users.ensure_user_exists(email)
        except Exception as e:
            print(f"[warn] Failed to auto-create user {email}: {e}")

    response = await call_next(request)
    return response


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
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database setup required",
            "action": "setup_required",
            "message": "Database needs initialization or upgrade. Please go to the Setup tab and click 'Initialize Database'.",
        },
    )


@app.exception_handler(asyncpg.exceptions.UndefinedObjectError)
async def handle_undefined_object(request: Request, exc: asyncpg.exceptions.UndefinedObjectError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database role not configured",
            "action": "setup_required",
            "message": f"Database setup required: {exc}. Please ensure the database role exists (run grants.sql).",
        },
    )


@app.exception_handler(asyncpg.exceptions.InvalidCatalogNameError)
async def handle_invalid_catalog(request: Request, exc: asyncpg.exceptions.InvalidCatalogNameError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database not found",
            "action": "setup_required",
            "message": f"Database setup required: {exc}. Please ensure the database exists.",
        },
    )


@app.exception_handler(asyncpg.exceptions.InvalidPasswordError)
async def handle_invalid_password(request: Request, exc: asyncpg.exceptions.InvalidPasswordError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database authentication failed",
            "action": "setup_required",
            "message": "Database authentication failed. Please check DB_DSN credentials.",
        },
    )


@app.exception_handler(asyncpg.exceptions.CannotConnectNowError)
async def handle_cannot_connect(request: Request, exc: asyncpg.exceptions.CannotConnectNowError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Cannot connect to database",
            "action": "setup_required",
            "message": f"Cannot connect to database: {exc}. Please check database availability.",
        },
    )


@app.exception_handler(asyncpg.exceptions.PostgresConnectionError)
async def handle_postgres_connection_error(request: Request, exc: asyncpg.exceptions.PostgresConnectionError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database connection error",
            "action": "setup_required",
            "message": f"Database connection error: {exc}. Please check database configuration.",
        },
    )


@app.exception_handler(OSError)
async def handle_os_error(request: Request, exc: OSError):
    if "Connect call failed" in str(exc) or "Connection refused" in str(exc):
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Cannot reach database server",
                "action": "setup_required",
                "message": f"Cannot reach database server: {exc}. Please check network connectivity and database host.",
            },
        )
    raise exc


@app.exception_handler(asyncpg.exceptions.UniqueViolationError)
async def handle_unique_violation(request: Request, exc: asyncpg.exceptions.UniqueViolationError):
    detail = str(exc)
    if "already exists" in detail:
        return JSONResponse(
            status_code=409,
            content={"detail": "A record with this name already exists", "error": "duplicate_name", "message": detail},
        )
    return JSONResponse(status_code=409, content={"detail": "Duplicate record", "message": detail})


@app.exception_handler(asyncpg.exceptions.ForeignKeyViolationError)
async def handle_foreign_key_violation(request: Request, exc: asyncpg.exceptions.ForeignKeyViolationError):
    return JSONResponse(
        status_code=400,
        content={
            "detail": "Invalid reference",
            "error": "invalid_foreign_key",
            "message": "One or more referenced records do not exist (e.g., system ID)",
        },
    )


# ---------- Wire API Routers ----------
app.include_router(tables_router, prefix="/api")
app.include_router(queries_router, prefix="/api")
app.include_router(schedules_router, prefix="/api")
app.include_router(triggers_router, prefix="/api")
app.include_router(systems_router, prefix="/api")
app.include_router(validation_history_router, prefix="/api")
app.include_router(dashboards_router, prefix="/api")
app.include_router(tags_router, prefix="/api")
app.include_router(type_transformations_router, prefix="/api")
app.include_router(validation_config_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(setup_router, prefix="/api")
app.include_router(misc_router, prefix="/api")


# ---------- Robust SPA mounting ----------
def _pick_frontend_dir() -> Path | None:
    base = Path(__file__).resolve().parent
    env = os.getenv("FRONTEND_DIR")
    candidates = []

    if env:
        candidates.append(Path(env))

    candidates += [
        base / ".." / "frontend" / "dist",
        base / ".." / "frontend" / "build",
        base / "frontend" / "dist",
        base / "frontend" / "build",
        base / ".." / "dist",
        base / ".." / "build",
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
    app.mount("/", StaticFiles(directory=str(_FRONTEND_DIR), html=True), name="spa")

    assets_dir = _FRONTEND_DIR / "assets"
    static_dir = _FRONTEND_DIR / "static"

    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
        print(f"[info] Mounted /assets -> {assets_dir}")

    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
        print(f"[info] Mounted /static -> {static_dir}")

    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str):
        index_path = _FRONTEND_DIR / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="index.html not found")

    @app.get("/favicon.ico")
    def favicon():
        ico = _FRONTEND_DIR / "favicon.ico"
        return FileResponse(str(ico)) if ico.exists() else Response(status_code=204)
else:

    @app.get("/")
    def _missing_build():
        return {
            "error": "frontend_build_not_found",
            "hint": "Set FRONTEND_DIR or run `npm run build` in your frontend and place index.html under one of the common locations.",
        }
