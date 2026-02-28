"""Tables (datasets) service."""

import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.utils import raise_version_conflict

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class TablesService:
    """Handles table (dataset) CRUD and bulk operations."""

    def __init__(self, db: "DBSession", user_email: str):
        self.db = db
        self.user_email = user_email

    async def list_tables(self, search: str | None = None) -> list[dict]:
        """Get all tables with last run status and tags."""
        if search:
            rows = await self.db.fetch(
                """
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
            """,
                f"%{search}%",
            )
        else:
            rows = await self.db.fetch("""
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
        return rows

    async def get_table(self, table_id: int) -> dict:
        """Get a table by ID."""
        row = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", table_id)
        if not row:
            raise HTTPException(404, "not found")
        return row

    async def create_table(self, data: dict) -> dict:
        """Create a new table."""
        src_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["src_system_id"])
        if not src_sys:
            raise HTTPException(400, f"Source system ID {data['src_system_id']} does not exist")
        tgt_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["tgt_system_id"])
        if not tgt_sys:
            raise HTTPException(400, f"Target system ID {data['tgt_system_id']} does not exist")

        existing = await self.db.fetchrow("SELECT id FROM control.datasets WHERE name = $1", data["name"])
        if existing:
            raise HTTPException(409, f"A table with name '{data['name']}' already exists")

        options = data.get("options", {})
        row = await self.db.fetchrow(
            """
            INSERT INTO control.datasets (
              name, src_system_id, src_schema, src_table,
              tgt_system_id, tgt_schema, tgt_table,
              compare_mode, pk_columns, watermark_filter, include_columns, exclude_columns,
              options, is_active, created_by, updated_by
            ) VALUES (
              $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$11,$12, $13,$14,$15,$15
            ) RETURNING *
        """,
            data["name"],
            data["src_system_id"],
            data["src_schema"],
            data["src_table"],
            data["tgt_system_id"],
            data.get("tgt_schema"),
            data.get("tgt_table"),
            data.get("compare_mode", "except_all"),
            data.get("pk_columns"),
            data.get("watermark_filter"),
            data.get("include_columns", []),
            data.get("exclude_columns", []),
            json.dumps(options) if isinstance(options, (dict, list)) else options,
            data.get("is_active", True),
            self.user_email,
        )
        return row

    async def update_table(self, table_id: int, data: dict) -> dict:
        """Update a table with optimistic locking."""
        if data.get("name"):
            existing = await self.db.fetchrow(
                "SELECT id FROM control.datasets WHERE name = $1 AND id != $2", data["name"], table_id
            )
            if existing:
                raise HTTPException(409, f"A table with name '{data['name']}' already exists")

        if data.get("src_system_id"):
            src_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["src_system_id"])
            if not src_sys:
                raise HTTPException(400, f"Source system ID {data['src_system_id']} does not exist")
        if data.get("tgt_system_id"):
            tgt_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["tgt_system_id"])
            if not tgt_sys:
                raise HTTPException(400, f"Target system ID {data['tgt_system_id']} does not exist")

        options = data.get("options")
        row = await self.db.fetchrow(
            """
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
            table_id,
            data.get("name"),
            data.get("src_system_id"),
            data.get("src_schema"),
            data.get("src_table"),
            data.get("tgt_system_id"),
            data.get("tgt_schema"),
            data.get("tgt_table"),
            data.get("compare_mode"),
            data.get("pk_columns"),
            data.get("watermark_filter"),
            data.get("include_columns"),
            data.get("exclude_columns"),
            json.dumps(options) if isinstance(options, (dict, list)) else options,
            data.get("is_active"),
            self.user_email,
            data["version"],
        )

        if not row:
            current = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", table_id)
            raise_version_conflict(current)
        return row

    async def delete_table(self, table_id: int) -> dict:
        """Delete a table."""
        await self.db.execute("DELETE FROM control.datasets WHERE id=$1", table_id)
        return {"ok": True}

    async def bulk_create_tables(self, src_system_id: int, tgt_system_id: int, items: list[dict]) -> dict:
        """Bulk create/update tables."""
        results = {"created": [], "updated": [], "errors": []}

        for idx, item in enumerate(items):
            try:
                name = item.get("name") or f"{item['src_schema']}.{item['src_table']}"
                tgt_schema = item.get("tgt_schema") or item["src_schema"]
                tgt_table = item.get("tgt_table") or item["src_table"]

                actual_src_system_id = src_system_id
                actual_tgt_system_id = tgt_system_id

                if item.get("src_system_name"):
                    src_sys = await self.db.fetchrow(
                        "SELECT id FROM control.systems WHERE name = $1", item["src_system_name"]
                    )
                    if not src_sys:
                        raise ValueError(f"Source system '{item['src_system_name']}' not found")
                    actual_src_system_id = src_sys["id"]

                if item.get("tgt_system_name"):
                    tgt_sys = await self.db.fetchrow(
                        "SELECT id FROM control.systems WHERE name = $1", item["tgt_system_name"]
                    )
                    if not tgt_sys:
                        raise ValueError(f"Target system '{item['tgt_system_name']}' not found")
                    actual_tgt_system_id = tgt_sys["id"]

                existing = await self.db.fetchrow("SELECT id, version FROM control.datasets WHERE name=$1", name)

                if existing:
                    row = await self.db.fetchrow(
                        """
                        UPDATE control.datasets SET
                          src_system_id = $2, src_schema = $3, src_table = $4,
                          tgt_system_id = $5, tgt_schema = $6, tgt_table = $7,
                          compare_mode = $8, pk_columns = $9, watermark_filter = $10,
                          include_columns = $11, exclude_columns = $12, is_active = $13,
                          updated_by = $14, updated_at = now(), version = version + 1
                        WHERE id=$1 RETURNING *
                    """,
                        existing["id"],
                        actual_src_system_id,
                        item["src_schema"],
                        item["src_table"],
                        actual_tgt_system_id,
                        tgt_schema,
                        tgt_table,
                        item.get("compare_mode", "except_all"),
                        item.get("pk_columns"),
                        item.get("watermark_filter"),
                        item.get("include_columns") or [],
                        item.get("exclude_columns") or [],
                        item.get("is_active", True),
                        self.user_email,
                    )
                    results["updated"].append({"row": idx + 1, "name": name, "data": row})
                else:
                    row = await self.db.fetchrow(
                        """
                        INSERT INTO control.datasets (
                          name, src_system_id, src_schema, src_table,
                          tgt_system_id, tgt_schema, tgt_table,
                          compare_mode, pk_columns, watermark_filter,
                          include_columns, exclude_columns, is_active, created_by, updated_by
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14) RETURNING *
                    """,
                        name,
                        actual_src_system_id,
                        item["src_schema"],
                        item["src_table"],
                        actual_tgt_system_id,
                        tgt_schema,
                        tgt_table,
                        item.get("compare_mode", "except_all"),
                        item.get("pk_columns"),
                        item.get("watermark_filter"),
                        item.get("include_columns") or [],
                        item.get("exclude_columns") or [],
                        item.get("is_active", True),
                        self.user_email,
                    )

                    if item.get("schedule_name"):
                        schedule = await self.db.fetchrow(
                            "SELECT id FROM control.schedules WHERE name=$1", item["schedule_name"]
                        )
                        if schedule:
                            await self.db.execute(
                                """
                                INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                                VALUES ($1, 'table', $2) ON CONFLICT DO NOTHING
                            """,
                                schedule["id"],
                                row["id"],
                            )

                    if item.get("tags"):
                        for tag_name in item["tags"]:
                            tag_name = tag_name.strip()
                            if not tag_name:
                                continue
                            tag = await self.db.fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
                            if not tag:
                                tag = await self.db.fetchrow(
                                    "INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name
                                )
                            await self.db.execute(
                                """
                                INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                                VALUES ('table', $1, $2) ON CONFLICT DO NOTHING
                            """,
                                row["id"],
                                tag["id"],
                            )

                    results["created"].append({"row": idx + 1, "name": name, "data": row})
            except Exception as e:
                results["errors"].append({"row": idx + 1, "error": str(e)})

        return results

    async def update_lineage(self, table_id: int, lineage: dict | list | None) -> dict:
        """Update the lineage JSONB field on a dataset."""
        await self.db.execute(
            "UPDATE control.datasets SET lineage = $1::jsonb WHERE id = $2",
            json.dumps(lineage) if lineage is not None else None,
            table_id,
        )
        return {"ok": True}
