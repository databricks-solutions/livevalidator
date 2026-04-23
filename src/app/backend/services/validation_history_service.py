"""Validation history service."""

import json
from datetime import datetime
from typing import TYPE_CHECKING

from fastapi import HTTPException

if TYPE_CHECKING:
    from backend.dependencies import DBSession
    from backend.services.databricks_service import DatabricksService


def _transform_pk_samples_to_legacy(sample_differences: dict | str | None) -> dict | None:
    """Transform NEW flat pk_sample format to OLD nested format for frontend compatibility."""
    if sample_differences is None:
        return None

    if isinstance(sample_differences, str):
        try:
            sample_differences = json.loads(sample_differences)
        except (json.JSONDecodeError, TypeError):
            return sample_differences

    if not isinstance(sample_differences, dict) or sample_differences.get("mode") != "primary_key":
        return sample_differences

    samples = sample_differences.get("samples", [])
    if not samples or not isinstance(samples[0], dict):
        return sample_differences

    if ".system" not in samples[0]:
        return sample_differences

    pk_columns = sample_differences.get("pk_columns", [])

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

    legacy_samples = []
    for _pk_key, data in grouped.items():
        src, tgt = data["source"], data["target"]
        if not src or not tgt:
            continue
        differences = []
        for col in src.keys():
            if col in (".system", *pk_columns):
                continue
            differences.append({"column": col, "source_value": src.get(col), "target_value": tgt.get(col)})
        legacy_samples.append({"pk": data["pk"], "differences": differences})

    return {"mode": "primary_key", "pk_columns": pk_columns, "samples": legacy_samples}


class ValidationHistoryService:
    """Handles validation history queries and management."""

    def __init__(self, db: "DBSession", databricks: "DatabricksService | None" = None):
        self.db = db
        self._databricks = databricks

    @property
    def databricks(self) -> "DatabricksService":
        if self._databricks is None:
            from backend.services.databricks_service import DatabricksService

            self._databricks = DatabricksService()
        return self._databricks

    async def list_validation_history(
        self,
        limit: int = 100,
        offset: int = 0,
        entity_type: str | None = None,
        entity_id: int | None = None,
        entity_name: str | None = None,
        status: str | None = None,
        schedule_id: int | None = None,
        source_system: str | None = None,
        target_system: str | None = None,
        tags: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        days_back: int = 0,
        sort_by: str = "requested_at",
        sort_dir: str = "desc",
    ) -> dict:
        """Get validation history with filters and pagination."""
        conditions = []
        params = []
        param_idx = 1

        if date_from:
            conditions.append(f"vh.requested_at >= ${param_idx}")
            dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
            params.append(dt)
            param_idx += 1
        elif days_back > 0:
            conditions.append(f"vh.requested_at >= NOW() - INTERVAL '{days_back} days'")

        if date_to:
            conditions.append(f"vh.requested_at <= ${param_idx}")
            dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
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

        tag_join = ""
        if tags:
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]
            if tag_list:
                tag_join = f"""
                    JOIN (
                        SELECT et.entity_type, et.entity_id
                        FROM control.entity_tags et
                        JOIN control.tags t ON et.tag_id = t.id
                        WHERE t.name = ANY(${param_idx})
                        GROUP BY et.entity_type, et.entity_id
                        HAVING COUNT(DISTINCT t.name) = ${param_idx + 1}
                    ) tf ON (
                        (vh.entity_type = 'table' AND tf.entity_type = 'table' AND tf.entity_id = vh.entity_id)
                        OR (vh.entity_type = 'compare_query' AND tf.entity_type = 'query' AND tf.entity_id = vh.entity_id)
                    )
                """
                params.append(tag_list)
                params.append(len(tag_list))
                param_idx += 2

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

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

        stats_row = await self.db.fetchrow(
            f"""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE vh.status = 'succeeded') as succeeded,
                COUNT(*) FILTER (WHERE vh.status = 'failed') as failed,
                COUNT(*) FILTER (WHERE vh.status = 'error') as errors
            FROM control.validation_history vh
            {tag_join}
            {where_clause}
        """,
            *params,
        )
        total = stats_row["total"] if stats_row else 0
        stats = {
            "total": total,
            "succeeded": stats_row["succeeded"] if stats_row else 0,
            "failed": stats_row["failed"] if stats_row else 0,
            "errors": stats_row["errors"] if stats_row else 0,
        }

        rows = await self.db.fetch(
            f"""
            SELECT
                vh.id, vh.trigger_id, vh.entity_type, vh.entity_id, vh.entity_name,
                vh.source, vh.schedule_id, vh.requested_by, vh.requested_at,
                vh.started_at, vh.finished_at, vh.duration_seconds,
                vh.source_system_name, vh.target_system_name,
                vh.source_table, vh.target_table, vh.pk_columns,
                vh.status, vh.schema_match, vh.row_count_match,
                vh.row_count_source, vh.row_count_target,
                vh.rows_compared, vh.rows_different, vh.difference_pct,
                vh.compare_mode, vh.error_message, vh.databricks_run_url,
                (vh.sample_differences->>'unique_count_source')::bigint as unique_count_source,
                (vh.sample_differences->>'unique_count_target')::bigint as unique_count_target
            FROM control.validation_history vh
            {tag_join}
            {where_clause}
            ORDER BY {sort_col} {sort_direction}
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """,
            *params,
            limit,
            offset,
        )

        results = rows

        if results:
            table_ids = list(set(r["entity_id"] for r in results if r["entity_type"] == "table"))
            query_ids = list(set(r["entity_id"] for r in results if r["entity_type"] == "compare_query"))

            tags_by_entity: dict[tuple[str, int], list] = {}

            if table_ids:
                tag_rows = await self.db.fetch(
                    """
                    SELECT et.entity_id, json_agg(t.name ORDER BY t.name) as tags
                    FROM control.entity_tags et
                    JOIN control.tags t ON et.tag_id = t.id
                    WHERE et.entity_type = 'table' AND et.entity_id = ANY($1)
                    GROUP BY et.entity_id
                """,
                    table_ids,
                )
                for tr in tag_rows:
                    tags_by_entity[("table", tr["entity_id"])] = tr["tags"] or []

            if query_ids:
                tag_rows = await self.db.fetch(
                    """
                    SELECT et.entity_id, json_agg(t.name ORDER BY t.name) as tags
                    FROM control.entity_tags et
                    JOIN control.tags t ON et.tag_id = t.id
                    WHERE et.entity_type = 'query' AND et.entity_id = ANY($1)
                    GROUP BY et.entity_id
                """,
                    query_ids,
                )
                for tr in tag_rows:
                    tags_by_entity[("query", tr["entity_id"])] = tr["tags"] or []

            for r in results:
                tag_key = ("table", r["entity_id"]) if r["entity_type"] == "table" else ("query", r["entity_id"])
                r["tags"] = tags_by_entity.get(tag_key, [])

        return {"data": results, "total": total, "limit": limit, "offset": offset, "stats": stats}

    async def get_validation_detail(self, validation_id: int) -> dict:
        """Get full validation details including sample differences."""
        row = await self.db.fetchrow("SELECT * FROM control.validation_history WHERE id=$1", validation_id)
        if not row:
            raise HTTPException(status_code=404, detail="Validation not found")
        result = dict(row)
        result["sample_differences"] = _transform_pk_samples_to_legacy(result.get("sample_differences"))
        return result

    async def get_latest_validation(self, entity_type: str, entity_id: int) -> dict | None:
        """Get most recent validation for a specific table/query."""
        row = await self.db.fetchrow(
            """
            SELECT * FROM control.validation_history
            WHERE entity_type = $1 AND entity_id = $2
            ORDER BY finished_at DESC
            LIMIT 1
        """,
            entity_type,
            entity_id,
        )

        if not row:
            return None
        result = dict(row)
        result["sample_differences"] = _transform_pk_samples_to_legacy(result.get("sample_differences"))
        return result

    async def create_validation_history(self, data: dict) -> dict | None:
        """Called by Databricks workflow at completion to record results."""
        if not data.get("trigger_id"):
            return None

        trigger = await self.db.fetchrow(
            "SELECT databricks_run_url, databricks_run_id, entity_id, requested_at FROM control.triggers WHERE id=$1",
            data["trigger_id"],
        )
        if not trigger:
            raise HTTPException(status_code=404, detail=f"Trigger '{data['trigger_id']}' not found")

        databricks_run_url = data.get("databricks_run_url") or trigger["databricks_run_url"]
        databricks_run_id = data.get("databricks_run_id") or trigger["databricks_run_id"]
        entity_id = data.get("entity_id") or trigger["entity_id"]
        requested_by = "system"
        requested_at = trigger["requested_at"]

        row = await self.db.fetchrow(
            """
            INSERT INTO control.validation_history (
                trigger_id, entity_type, entity_id, entity_name,
                source, schedule_id, requested_by, requested_at,
                started_at, finished_at,
                source_system_id, target_system_id,
                source_system_name, target_system_name,
                source_table, target_table, src_sql_query, tgt_sql_query,
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
                $30, $31, $32, $33, $34, $35, $36
            ) RETURNING id
        """,
            data["trigger_id"],
            data["entity_type"],
            entity_id,
            data["entity_name"],
            data["source"],
            data.get("schedule_id"),
            requested_by,
            requested_at,
            datetime.fromisoformat(data["started_at"]),
            datetime.fromisoformat(data["finished_at"]),
            data["source_system_id"],
            data["target_system_id"],
            data["source_system_name"],
            data["target_system_name"],
            data.get("source_table"),
            data.get("target_table"),
            data.get("src_sql_query"),
            data.get("tgt_sql_query"),
            data["compare_mode"],
            data.get("pk_columns"),
            data.get("exclude_columns"),
            data["status"],
            data.get("schema_match"),
            json.dumps(data.get("schema_details", {})),
            data.get("row_count_source"),
            data.get("row_count_target"),
            data.get("row_count_match"),
            data.get("rows_compared"),
            data.get("rows_matched"),
            data.get("rows_different"),
            json.dumps(data.get("sample_differences", [])),
            data.get("error_message"),
            json.dumps(data.get("error_details", {})),
            databricks_run_id,
            databricks_run_url,
            json.dumps(data.get("full_result", {})),
        )

        await self.db.execute("DELETE FROM control.triggers WHERE id=$1", data["trigger_id"])

        return {"id": row["id"], "ok": True}

    async def update_validation_history(self, validation_id: int, data: dict) -> dict:
        """Update specific fields on a validation history record."""
        allowed_fields = {
            "sample_differences",
            "status",
            "error_message",
            "error_details",
            "rows_compared",
            "rows_matched",
            "rows_different",
        }

        updates = {k: v for k, v in data.items() if k in allowed_fields}

        if not updates:
            raise HTTPException(400, f"No valid fields to update. Allowed: {allowed_fields}")

        set_clauses = []
        params = []
        param_idx = 1

        for field, value in updates.items():
            if field in ("sample_differences", "error_details"):
                set_clauses.append(f"{field} = ${param_idx}::jsonb")
                params.append(json.dumps(value) if value is not None else None)
            else:
                set_clauses.append(f"{field} = ${param_idx}")
                params.append(value)
            param_idx += 1

        params.append(validation_id)

        result = await self.db.execute(
            f"""
            UPDATE control.validation_history
            SET {", ".join(set_clauses)}
            WHERE id = ${param_idx}
        """,
            *params,
        )

        if result == "UPDATE 0":
            raise HTTPException(404, f"Validation history record {validation_id} not found")

        return {"id": validation_id, "ok": True, "updated_fields": list(updates.keys())}

    async def delete_validation_history(self, ids: list[int]) -> dict:
        """Bulk delete validation history records by IDs."""
        if not ids:
            raise HTTPException(400, "No IDs provided")

        await self.db.execute("DELETE FROM control.validation_history WHERE id = ANY($1)", ids)

        return {"deleted_count": len(ids), "ok": True}

    async def fetch_lineage_for_table(self, table_id: int, system: str = "source") -> dict:
        """Start a Databricks lineage job for a configured table."""
        row = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", table_id)
        if not row:
            raise HTTPException(status_code=404, detail="Table not found")

        if system not in ("source", "target"):
            raise HTTPException(status_code=400, detail="system must be 'source' or 'target'")

        system_id = row["src_system_id"] if system == "source" else row["tgt_system_id"]
        chosen_system = await self.db.fetchrow("SELECT id, kind, catalog FROM control.systems WHERE id=$1", system_id)
        if not chosen_system:
            raise HTTPException(status_code=404, detail=f"{system.capitalize()} system not found")
        if chosen_system["kind"] != "Databricks":
            raise HTTPException(
                status_code=400,
                detail=f"Lineage is only available for Databricks systems ({system} system is {chosen_system['kind']})",
            )

        schema_col = "src_schema" if system == "source" else "tgt_schema"
        table_col = "src_table" if system == "source" else "tgt_table"
        schema_val = (row.get(schema_col) or "").strip()
        table_val = (row.get(table_col) or "").strip()
        if not schema_val or not table_val:
            raise HTTPException(
                status_code=400, detail=f"{system.capitalize()} schema and table are required for lineage"
            )

        catalog = (chosen_system.get("catalog") or "").strip()
        if not catalog:
            raise HTTPException(status_code=400, detail=f"{system.capitalize()} system has no catalog configured")

        table_name = f"{catalog}.{schema_val}.{table_val}"

        await self.db.execute("UPDATE control.datasets SET lineage = NULL WHERE id=$1", table_id)

        job_id = self.databricks.get_lineage_job_id()
        if not job_id:
            raise HTTPException(status_code=500, detail="LINEAGE_JOB_ID not configured")

        backend_url = self.databricks.get_backend_url().rstrip("/")
        if not backend_url:
            raise HTTPException(status_code=500, detail="DATABRICKS_APP_URL not configured")

        params = {
            "table_name": table_name,
            "catalog_name": catalog,
            "backend_api_url": backend_url,
            "entity_type": "table",
            "entity_id": str(table_id),
        }
        try:
            run_id, run_url = self.databricks.launch_job(int(job_id), params)
            return {"ok": True, "message": "Lineage fetch started", "run_id": run_id, "run_url": run_url}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to start lineage job: {str(e)}") from e
