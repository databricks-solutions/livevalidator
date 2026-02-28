"""Schedules and bindings service."""

from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import available_timezones

from backend.utils import raise_version_conflict, serialize_row

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class SchedulesService:
    """Handles schedule CRUD and schedule-entity bindings."""

    def __init__(self, db: "DBSession", user_email: str):
        self.db = db
        self.user_email = user_email

    def list_timezones(self) -> list[str]:
        """Return common IANA timezones for use in schedule configuration."""
        common_timezones = sorted(
            [
                tz
                for tz in available_timezones()
                if "/" in tz and not tz.startswith("Etc/") and not tz.startswith("SystemV/")
            ]
        )
        return ["UTC"] + common_timezones

    async def list_schedules(self) -> list[dict]:
        """Get all schedules."""
        rows = await self.db.fetch("SELECT * FROM control.schedules ORDER BY name")
        return [serialize_row(r) for r in rows]

    async def create_schedule(self, data: dict) -> dict:
        """Create a new schedule."""
        row = await self.db.fetchrow(
            """
            INSERT INTO control.schedules (name, cron_expr, timezone, enabled, max_concurrency, backfill_policy, created_by, updated_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$7) RETURNING *
        """,
            data["name"],
            data["cron_expr"],
            data.get("timezone", "UTC"),
            data.get("enabled", True),
            data.get("max_concurrency", 4),
            data.get("backfill_policy", "none"),
            self.user_email,
        )
        return serialize_row(row)

    async def update_schedule(self, schedule_id: int, data: dict) -> dict:
        """Update a schedule with optimistic locking."""
        current = await self.db.fetchrow("SELECT cron_expr, timezone FROM control.schedules WHERE id=$1", schedule_id)
        reset_next_run = current and (
            (data.get("cron_expr") and data["cron_expr"] != current["cron_expr"])
            or (data.get("timezone") and data["timezone"] != current["timezone"])
        )

        next_run_at = (
            None
            if reset_next_run
            else (datetime.fromisoformat(data["next_run_at"]) if data.get("next_run_at") else None)
        )

        row = await self.db.fetchrow(
            """
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
            schedule_id,
            data.get("name"),
            data.get("cron_expr"),
            data.get("timezone"),
            data.get("enabled"),
            data.get("max_concurrency"),
            data.get("backfill_policy"),
            datetime.fromisoformat(data["last_run_at"]) if data.get("last_run_at") else None,
            next_run_at,
            self.user_email,
            data["version"],
        )

        if not row:
            current = await self.db.fetchrow("SELECT * FROM control.schedules WHERE id=$1", schedule_id)
            raise_version_conflict(current)
        return serialize_row(row)

    async def delete_schedule(self, schedule_id: int) -> dict:
        """Delete a schedule."""
        await self.db.execute("DELETE FROM control.schedules WHERE id=$1", schedule_id)
        return {"ok": True}

    async def create_binding(self, schedule_id: int, entity_type: str, entity_id: int) -> dict:
        """Bind a schedule to an entity."""
        id_ = await self.db.fetchval(
            """
            INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
            VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id
        """,
            schedule_id,
            entity_type,
            entity_id,
        )
        return {"id": id_}

    async def list_bindings(self, entity_type: str, entity_id: int) -> list[dict]:
        """Get bindings for an entity."""
        return await self.db.fetch(
            "SELECT * FROM control.schedule_bindings WHERE entity_type=$1 AND entity_id=$2", entity_type, entity_id
        )

    async def list_all_bindings(self) -> list[dict]:
        """Bulk fetch all bindings."""
        return await self.db.fetch("SELECT * FROM control.schedule_bindings ORDER BY entity_type, entity_id")

    async def list_bindings_by_schedule(self, schedule_id: int) -> list[dict]:
        """Get bindings for a schedule."""
        return await self.db.fetch("SELECT * FROM control.schedule_bindings WHERE schedule_id=$1", schedule_id)

    async def delete_binding(self, binding_id: int) -> dict:
        """Delete a binding."""
        await self.db.execute("DELETE FROM control.schedule_bindings WHERE id=$1", binding_id)
        return {"ok": True}
