"""Compare queries service."""

import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.utils import raise_version_conflict

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class QueriesService:
    """Handles compare query CRUD and bulk operations."""

    def __init__(self, db: "DBSession", user_email: str):
        self.db = db
        self.user_email = user_email

    async def list_queries(self, search: str | None = None) -> list[dict]:
        """Get all queries with last run status and tags."""
        if search:
            rows = await self.db.fetch(
                """
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
            """,
                f"%{search}%",
            )
        else:
            rows = await self.db.fetch("""
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
        return rows

    async def get_query(self, query_id: int) -> dict:
        """Get a query by ID."""
        row = await self.db.fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", query_id)
        if not row:
            raise HTTPException(404, "not found")
        return row

    async def create_query(self, data: dict) -> dict:
        """Create a new compare query."""
        src_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["src_system_id"])
        if not src_sys:
            raise HTTPException(400, f"Source system ID {data['src_system_id']} does not exist")
        tgt_sys = await self.db.fetchrow("SELECT id FROM control.systems WHERE id = $1", data["tgt_system_id"])
        if not tgt_sys:
            raise HTTPException(400, f"Target system ID {data['tgt_system_id']} does not exist")

        existing = await self.db.fetchrow("SELECT id FROM control.compare_queries WHERE name = $1", data["name"])
        if existing:
            raise HTTPException(409, f"A query with name '{data['name']}' already exists")

        options = data.get("options", {})
        row = await self.db.fetchrow(
            """
            INSERT INTO control.compare_queries (
              name, src_system_id, tgt_system_id, sql,
              compare_mode, pk_columns, watermark_filter,
              options, is_active, created_by, updated_by
            ) VALUES (
              $1,$2,$3,$4, $5,$6,$7, $8,$9,$10,$10
            ) RETURNING *
        """,
            data["name"],
            data["src_system_id"],
            data["tgt_system_id"],
            data["sql"],
            data.get("compare_mode", "except_all"),
            data.get("pk_columns"),
            data.get("watermark_filter"),
            json.dumps(options) if isinstance(options, (dict, list)) else options,
            data.get("is_active", True),
            self.user_email,
        )
        return row

    async def update_query(self, query_id: int, data: dict) -> dict:
        """Update a query with optimistic locking."""
        if data.get("name"):
            existing = await self.db.fetchrow(
                "SELECT id FROM control.compare_queries WHERE name = $1 AND id != $2", data["name"], query_id
            )
            if existing:
                raise HTTPException(409, f"A query with name '{data['name']}' already exists")

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
            query_id,
            data.get("name"),
            data.get("src_system_id"),
            data.get("tgt_system_id"),
            data.get("sql"),
            data.get("compare_mode"),
            data.get("pk_columns"),
            data.get("watermark_filter"),
            json.dumps(options) if isinstance(options, (dict, list)) else options,
            data.get("is_active"),
            self.user_email,
            data["version"],
        )

        if not row:
            current = await self.db.fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", query_id)
            raise_version_conflict(current)
        return row

    async def delete_query(self, query_id: int) -> dict:
        """Delete a query."""
        await self.db.execute("DELETE FROM control.compare_queries WHERE id=$1", query_id)
        return {"ok": True}

    async def bulk_create_queries(self, src_system_id: int, tgt_system_id: int, items: list[dict]) -> dict:
        """Bulk create/update queries."""
        results = {"created": [], "updated": [], "errors": []}

        for idx, item in enumerate(items):
            try:
                name = item.get("name") or f"Query {idx + 1}"

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

                existing = await self.db.fetchrow("SELECT id, version FROM control.compare_queries WHERE name=$1", name)

                if existing:
                    row = await self.db.fetchrow(
                        """
                        UPDATE control.compare_queries SET
                          src_system_id = $2, sql = $3, tgt_system_id = $4,
                          compare_mode = $5, pk_columns = $6, watermark_filter = $7,
                          is_active = $8, updated_by = $9, updated_at = now(), version = version + 1
                        WHERE id=$1 RETURNING *
                    """,
                        existing["id"],
                        actual_src_system_id,
                        item["sql"],
                        actual_tgt_system_id,
                        item.get("compare_mode", "except_all"),
                        item.get("pk_columns"),
                        item.get("watermark_filter"),
                        item.get("is_active", True),
                        self.user_email,
                    )
                    results["updated"].append({"row": idx + 1, "name": name, "data": row})
                else:
                    row = await self.db.fetchrow(
                        """
                        INSERT INTO control.compare_queries (
                          name, src_system_id, sql, tgt_system_id,
                          compare_mode, pk_columns, watermark_filter, is_active, created_by, updated_by
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9) RETURNING *
                    """,
                        name,
                        actual_src_system_id,
                        item["sql"],
                        actual_tgt_system_id,
                        item.get("compare_mode", "except_all"),
                        item.get("pk_columns"),
                        item.get("watermark_filter"),
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
                                VALUES ($1, 'compare_query', $2) ON CONFLICT DO NOTHING
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
                                VALUES ('query', $1, $2) ON CONFLICT DO NOTHING
                            """,
                                row["id"],
                                tag["id"],
                            )

                    results["created"].append({"row": idx + 1, "name": name, "data": row})
            except Exception as e:
                results["errors"].append({"row": idx + 1, "error": str(e)})

        return results

    async def update_lineage(self, query_id: int, lineage: dict | list | None) -> dict:
        """Update the lineage JSONB field on a query."""
        await self.db.execute(
            "UPDATE control.compare_queries SET lineage = $1::jsonb WHERE id = $2",
            json.dumps(lineage) if lineage is not None else None,
            query_id,
        )
        return {"ok": True}
