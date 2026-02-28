"""Dashboards service."""

import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.utils import raise_version_conflict, serialize_row

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class DashboardsService:
    """Handles dashboard CRUD and chart management."""

    def __init__(self, db: "DBSession", user_email: str):
        self.db = db
        self.user_email = user_email

    async def _check_dashboard_access(self, dashboard_id: int, require_owner: bool = False) -> dict:
        """Check if user can access/modify a dashboard."""
        dashboard = await self.db.fetchrow("SELECT * FROM control.dashboards WHERE id=$1", dashboard_id)
        if not dashboard:
            raise HTTPException(404, "Dashboard not found")

        d = serialize_row(dashboard)
        if d["project"] == "General" and d["created_by"] != self.user_email:
            raise HTTPException(403, "This dashboard is private")

        if require_owner and d["created_by"] != self.user_email:
            role = await self.db.fetchrow("SELECT role FROM control.user_roles WHERE user_email=$1", self.user_email)
            if not role or role["role"] != "CAN_MANAGE":
                raise HTTPException(403, "Only the dashboard creator or CAN_MANAGE users can modify this dashboard")

        return d

    async def list_dashboards(self) -> list[dict]:
        """List dashboards visible to the current user."""
        rows = await self.db.fetch(
            """
            SELECT d.*,
                (SELECT COUNT(*) FROM control.dashboard_charts dc WHERE dc.dashboard_id = d.id) as chart_count
            FROM control.dashboards d
            WHERE d.created_by = $1 OR d.project != 'General'
            ORDER BY d.project, d.updated_at DESC
        """,
            self.user_email,
        )
        return [serialize_row(r) for r in rows]

    async def list_projects(self) -> list[str]:
        """Return distinct project names for autocomplete."""
        rows = await self.db.fetch("""
            SELECT DISTINCT project FROM control.dashboards
            WHERE project != 'General'
            ORDER BY project
        """)
        return [r["project"] for r in rows]

    async def get_dashboard(self, dashboard_id: int) -> dict:
        """Get a dashboard with its charts."""
        d = await self._check_dashboard_access(dashboard_id)

        charts = await self.db.fetch(
            "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", dashboard_id
        )
        d["charts"] = [serialize_row(c) for c in charts]
        return d

    async def create_dashboard(self, name: str, project: str = "General") -> dict:
        """Create a new dashboard with a default 'Overall' chart."""
        dashboard = await self.db.fetchrow(
            """
            INSERT INTO control.dashboards (name, project, created_by, updated_by)
            VALUES ($1, $2, $3, $3)
            RETURNING *
        """,
            name,
            project,
            self.user_email,
        )

        await self.db.execute(
            """
            INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
            VALUES ($1, 'Overall', 0, '{}'::jsonb)
        """,
            dashboard["id"],
        )

        d = serialize_row(dashboard)
        charts = await self.db.fetch(
            "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", dashboard["id"]
        )
        d["charts"] = [serialize_row(c) for c in charts]
        return d

    async def update_dashboard(self, dashboard_id: int, data: dict) -> dict:
        """Update dashboard metadata with optimistic locking."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)

        row = await self.db.fetchrow(
            """
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
        """,
            dashboard_id,
            data.get("name"),
            data.get("project"),
            data.get("time_range_preset"),
            data.get("time_range_from"),
            data.get("time_range_to"),
            self.user_email,
            data["version"],
        )

        if not row:
            current = await self.db.fetchrow("SELECT * FROM control.dashboards WHERE id=$1", dashboard_id)
            raise_version_conflict(current)
        return serialize_row(row)

    async def delete_dashboard(self, dashboard_id: int) -> dict:
        """Delete a dashboard."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)
        await self.db.execute("DELETE FROM control.dashboards WHERE id=$1", dashboard_id)
        return {"ok": True}

    async def clone_dashboard(self, dashboard_id: int, name: str | None = None, project: str | None = None) -> dict:
        """Clone a dashboard and all its charts."""
        source = await self.db.fetchrow("SELECT * FROM control.dashboards WHERE id=$1", dashboard_id)
        if not source:
            raise HTTPException(404, "Dashboard not found")

        clone_name = name or f"{source['name']} (Copy)"
        clone_project = project or "General"

        new_dash = await self.db.fetchrow(
            """
            INSERT INTO control.dashboards (name, project, time_range_preset, time_range_from, time_range_to, created_by, updated_by)
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            RETURNING *
        """,
            clone_name,
            clone_project,
            source["time_range_preset"],
            source["time_range_from"],
            source["time_range_to"],
            self.user_email,
        )

        charts = await self.db.fetch(
            "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", dashboard_id
        )
        for chart in charts:
            src_filters = chart["filters"] if isinstance(chart["filters"], dict) else {}
            await self.db.execute(
                """
                INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
                VALUES ($1, $2, $3, $4::jsonb)
            """,
                new_dash["id"],
                chart["name"],
                chart["sort_order"],
                json.dumps(src_filters),
            )

        d = serialize_row(new_dash)
        new_charts = await self.db.fetch(
            "SELECT * FROM control.dashboard_charts WHERE dashboard_id=$1 ORDER BY sort_order, id", new_dash["id"]
        )
        d["charts"] = [serialize_row(c) for c in new_charts]
        return d

    async def add_chart(self, dashboard_id: int, name: str, filters: dict | None = None, sort_order: int = 0) -> dict:
        """Add a chart to a dashboard."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)

        row = await self.db.fetchrow(
            """
            INSERT INTO control.dashboard_charts (dashboard_id, name, sort_order, filters)
            VALUES ($1, $2, $3, $4::jsonb)
            RETURNING *
        """,
            dashboard_id,
            name,
            sort_order,
            json.dumps(filters or {}),
        )

        await self.db.execute(
            "UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", dashboard_id, self.user_email
        )
        return serialize_row(row)

    async def reorder_charts(self, dashboard_id: int, chart_ids: list[int]) -> dict:
        """Bulk update chart sort_order based on provided order."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)

        for idx, chart_id in enumerate(chart_ids):
            await self.db.execute(
                """
                UPDATE control.dashboard_charts SET sort_order=$2, updated_at=now()
                WHERE id=$1 AND dashboard_id=$3
            """,
                chart_id,
                idx,
                dashboard_id,
            )

        await self.db.execute(
            "UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", dashboard_id, self.user_email
        )
        return {"ok": True}

    async def update_chart(self, dashboard_id: int, chart_id: int, data: dict) -> dict:
        """Update a chart's name, filters, or sort_order."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)

        chart = await self.db.fetchrow(
            "SELECT * FROM control.dashboard_charts WHERE id=$1 AND dashboard_id=$2", chart_id, dashboard_id
        )
        if not chart:
            raise HTTPException(404, "Chart not found")

        filters_json = json.dumps(data["filters"]) if data.get("filters") is not None else None
        row = await self.db.fetchrow(
            """
            UPDATE control.dashboard_charts SET
                name = COALESCE($2, name),
                filters = COALESCE($3::jsonb, filters),
                sort_order = COALESCE($4, sort_order),
                updated_at = now()
            WHERE id=$1
            RETURNING *
        """,
            chart_id,
            data.get("name"),
            filters_json,
            data.get("sort_order"),
        )

        await self.db.execute(
            "UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", dashboard_id, self.user_email
        )
        return serialize_row(row)

    async def delete_chart(self, dashboard_id: int, chart_id: int) -> dict:
        """Remove a chart from a dashboard."""
        await self._check_dashboard_access(dashboard_id, require_owner=True)

        await self.db.execute(
            "DELETE FROM control.dashboard_charts WHERE id=$1 AND dashboard_id=$2", chart_id, dashboard_id
        )
        await self.db.execute(
            "UPDATE control.dashboards SET updated_at=now(), updated_by=$2 WHERE id=$1", dashboard_id, self.user_email
        )
        return {"ok": True}
