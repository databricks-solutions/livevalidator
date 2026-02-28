"""Systems service for connection targets/engines."""

import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.utils import raise_version_conflict

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class SystemsService:
    """Handles system (connection target) CRUD operations."""

    def __init__(self, db: "DBSession", user_email: str):
        self.db = db
        self.user_email = user_email

    async def list_systems(self) -> list[dict]:
        """Get all active systems."""
        return await self.db.fetch("SELECT * FROM control.systems WHERE is_active ORDER BY name")

    async def get_system(self, system_id: int) -> dict:
        """Get a system by ID."""
        row = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", system_id)
        if not row:
            raise HTTPException(404, "not found")
        return row

    async def get_system_by_name(self, name: str) -> dict:
        """Get a system by name."""
        row = await self.db.fetchrow("SELECT * FROM control.systems WHERE name=$1", name)
        if not row:
            raise HTTPException(404, "not found")
        return row

    async def create_system(self, data: dict) -> dict:
        """Create a new system."""
        row = await self.db.fetchrow(
            """
            INSERT INTO control.systems (
              name, kind, catalog, host, port, database, secret_scope, user_secret_key, pass_secret_key, jdbc_string, driver_connector,
              concurrency, max_rows, options, is_active, created_by, updated_by
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, $12,$13,$14,$15,$16,$16
            ) RETURNING *
        """,
            data["name"],
            data["kind"],
            data.get("catalog", "").strip() if data.get("catalog") else None,
            data.get("host", "").strip() if data.get("host") else None,
            data.get("port"),
            data.get("database", "").strip() if data.get("database") else None,
            (data.get("secret_scope", "").strip() or "livevalidator") if data.get("secret_scope") else "livevalidator",
            data.get("user_secret_key", "").strip() if data.get("user_secret_key") else None,
            data.get("pass_secret_key", "").strip() if data.get("pass_secret_key") else None,
            data.get("jdbc_string", "").strip() if data.get("jdbc_string") else None,
            data.get("driver_connector", "").strip() if data.get("driver_connector") else None,
            data.get("concurrency", -1),
            data.get("max_rows"),
            json.dumps(data.get("options", {})),
            data.get("is_active", True),
            self.user_email,
        )
        return row

    async def update_system(self, system_id: int, data: dict) -> dict:
        """Update a system with optimistic locking."""
        row = await self.db.fetchrow(
            """
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
            system_id,
            data.get("name"),
            data.get("kind"),
            data.get("catalog"),
            data.get("host"),
            data.get("port"),
            data.get("database"),
            data.get("secret_scope"),
            data.get("user_secret_key"),
            data.get("pass_secret_key"),
            data.get("jdbc_string"),
            data.get("driver_connector"),
            data.get("concurrency"),
            data.get("max_rows"),
            json.dumps(data["options"]) if data.get("options") else None,
            data.get("is_active"),
            self.user_email,
            data["version"],
        )

        if not row:
            current = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", system_id)
            raise_version_conflict(current)
        return row

    async def delete_system(self, system_id: int) -> dict:
        """Delete a system."""
        await self.db.execute("DELETE FROM control.systems WHERE id=$1", system_id)
        return {"ok": True}
