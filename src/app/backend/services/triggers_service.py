"""Triggers service for validation job management."""

import json
import os
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.utils import serialize_row

if TYPE_CHECKING:
    from backend.dependencies import DBSession
    from backend.services.databricks_service import DatabricksService


class TriggersService:
    """Handles trigger CRUD, job launching, and queue management."""

    def __init__(self, db: "DBSession", user_email: str, databricks: "DatabricksService | None" = None):
        self.db = db
        self.user_email = user_email
        self._databricks = databricks

    @property
    def databricks(self) -> "DatabricksService":
        if self._databricks is None:
            from backend.services.databricks_service import DatabricksService

            self._databricks = DatabricksService()
        return self._databricks

    async def get_enriched_trigger(self, trigger_id: int) -> dict | None:
        """Get trigger with full entity details for job launch."""
        trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            return None

        if trigger["entity_type"] == "table":
            entity = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", trigger["entity_id"])
        else:
            entity = await self.db.fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", trigger["entity_id"])

        if not entity:
            return None

        src_system = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", entity["src_system_id"])
        tgt_system = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", entity["tgt_system_id"])

        result = dict(entity)
        result["id"] = trigger["id"]
        result["entity_type"] = trigger["entity_type"]
        result["entity_id"] = trigger["entity_id"]
        result["src_system_id"] = entity["src_system_id"]
        result["tgt_system_id"] = entity["tgt_system_id"]
        if trigger["entity_type"] == "table":
            result["source_table"] = f"{entity['src_schema'].strip()}.{entity['src_table'].strip()}"
            result["target_table"] = f"{entity['tgt_schema'].strip()}.{entity['tgt_table'].strip()}"
        result["watermark_expr"] = entity.get("watermark_filter", "")
        result["src_system_name"] = src_system["name"] if src_system else "unknown"
        result["tgt_system_name"] = tgt_system["name"] if tgt_system else "unknown"
        result["config_overrides"] = entity.get("config_overrides")

        return result

    async def check_system_concurrency(self, src_system_id: int, tgt_system_id: int) -> tuple[bool, str]:
        """Check if job can be launched based on system concurrency limits."""
        src_system = await self.db.fetchrow("SELECT name, concurrency FROM control.systems WHERE id=$1", src_system_id)
        tgt_system = await self.db.fetchrow("SELECT name, concurrency FROM control.systems WHERE id=$1", tgt_system_id)

        src_limit = src_system["concurrency"] if src_system else -1
        tgt_limit = tgt_system["concurrency"] if tgt_system else -1

        if src_limit == -1 and tgt_limit == -1:
            return True, ""

        rows = await self.db.fetch(
            """
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
        """,
            src_system_id,
            tgt_system_id,
        )

        running_counts = {row["system_id"]: int(row["count"]) for row in rows}

        src_running = running_counts.get(src_system_id, 0)
        tgt_running = running_counts.get(tgt_system_id, 0)

        if src_limit != -1 and src_running >= src_limit:
            src_name = src_system["name"] if src_system else f"System {src_system_id}"
            return False, f"{src_name} at capacity ({src_running}/{src_limit})"

        if tgt_limit != -1 and tgt_running >= tgt_limit:
            tgt_name = tgt_system["name"] if tgt_system else f"System {tgt_system_id}"
            return False, f"{tgt_name} at capacity ({tgt_running}/{tgt_limit})"

        return True, ""

    async def launch_validation_job(self, trigger_id: int) -> dict:
        """Launch a Databricks validation job for the given trigger."""
        enriched = await self.get_enriched_trigger(trigger_id)
        if not enriched:
            raise HTTPException(status_code=404, detail="Trigger or entity not found")

        config_row = await self.db.fetchrow("SELECT * FROM control.validation_config WHERE id = 1")
        resolved_config = (
            dict(config_row)
            if config_row
            else {"downgrade_unicode": False, "replace_special_char": [], "extra_replace_regex": ""}
        )
        if enriched.get("config_overrides"):
            resolved_config.update(enriched["config_overrides"])

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
            "extra_replace_regex": resolved_config.get("extra_replace_regex", ""),
        }

        job_id = self.databricks.get_validation_job_id()
        if not job_id:
            raise HTTPException(status_code=500, detail="VALIDATION_JOB_ID not configured")

        try:
            run_id, run_url = self.databricks.launch_job(int(job_id), params)

            await self.db.execute(
                """
                UPDATE control.triggers
                SET status = 'running', started_at = now(), databricks_run_id = $2, databricks_run_url = $3
                WHERE id = $1
            """,
                trigger_id,
                str(run_id),
                run_url,
            )

            return {"run_id": run_id, "run_url": run_url}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to launch job: {str(e)}") from e

    async def list_triggers(self, status: str | None = None) -> list[dict]:
        """Get active triggers with optional status filter."""
        if status:
            rows = await self.db.fetch(
                """
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
            """,
                status,
            )
        else:
            rows = await self.db.fetch("""
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

        running_run_ids = [
            int(r["databricks_run_id"]) for r in rows if r["status"] == "running" and r.get("databricks_run_id")
        ]
        run_statuses = self.databricks.get_run_statuses(running_run_ids) if running_run_ids else {}

        results = []
        for r in rows:
            row = serialize_row(r)
            run_id = r.get("databricks_run_id")
            if run_id and int(run_id) in run_statuses:
                row["databricks_run_status"] = run_statuses[int(run_id)]
            results.append(row)

        return results

    async def create_trigger(self, data: dict) -> dict:
        """Create a new validation trigger and attempt immediate launch."""
        if data["entity_type"] == "table":
            entity = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", data["entity_id"])
        else:
            entity = await self.db.fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", data["entity_id"])

        if not entity:
            raise HTTPException(status_code=404, detail=f"{data['entity_type']} not found")

        existing = await self.db.fetchrow(
            """
            SELECT id FROM control.triggers
            WHERE entity_type = $1 AND entity_id = $2 AND status IN ('queued', 'running')
        """,
            data["entity_type"],
            data["entity_id"],
        )

        if existing:
            raise HTTPException(status_code=409, detail="Validation already queued/running for this entity")

        can_launch, reason = await self.check_system_concurrency(entity["src_system_id"], entity["tgt_system_id"])

        if not can_launch:
            row = await self.db.fetchrow(
                """
                INSERT INTO control.triggers (
                    source, schedule_id, entity_type, entity_id,
                    priority, requested_by, requested_at, params, status
                ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7, 'queued')
                RETURNING *
            """,
                data.get("source", "manual"),
                data.get("schedule_id"),
                data["entity_type"],
                data["entity_id"],
                data.get("priority", 100),
                self.user_email,
                json.dumps(data.get("params", {}))
                if isinstance(data.get("params"), (dict, list))
                else data.get("params", "{}"),
            )
            result = serialize_row(row)
            result["queued_reason"] = reason
            return result

        row = await self.db.fetchrow(
            """
            INSERT INTO control.triggers (
                source, schedule_id, entity_type, entity_id,
                priority, requested_by, requested_at, params, status, started_at
            ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7, 'running', now())
            RETURNING *
        """,
            data.get("source", "manual"),
            data.get("schedule_id"),
            data["entity_type"],
            data["entity_id"],
            data.get("priority", 100),
            self.user_email,
            json.dumps(data.get("params", {}))
            if isinstance(data.get("params"), (dict, list))
            else data.get("params", "{}"),
        )

        trigger_id = row["id"]

        try:
            run_info = await self.launch_validation_job(trigger_id)
            result = serialize_row(row)
            result["databricks_run_id"] = run_info["run_id"]
            result["databricks_run_url"] = run_info["run_url"]
            return result
        except HTTPException as e:
            await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
            raise e
        except Exception as e:
            await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
            raise HTTPException(status_code=500, detail=f"Failed to launch job: {str(e)}") from e

    async def create_triggers_bulk(self, triggers: list[dict]) -> dict:
        """Create multiple validation triggers in one transaction."""
        if not triggers:
            return {"created": []}

        sources = [t.get("source", "manual") for t in triggers]
        schedule_ids = [t.get("schedule_id") for t in triggers]
        entity_types = [t["entity_type"] for t in triggers]
        entity_ids = [t["entity_id"] for t in triggers]
        priorities = [t.get("priority", 100) for t in triggers]
        requested_bys = [t.get("requested_by") or self.user_email for t in triggers]
        params_json = [
            json.dumps(t.get("params", {})) if isinstance(t.get("params"), (dict, list)) else t.get("params", "{}")
            for t in triggers
        ]

        rows = await self.db.fetch(
            """
            INSERT INTO control.triggers (
                source, schedule_id, entity_type, entity_id,
                priority, requested_by, requested_at, params
            )
            SELECT t.source, t.schedule_id, t.entity_type, t.entity_id, t.priority, t.requested_by, now(), t.params::jsonb
            FROM unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::int[], $6::text[], $7::text[])
                AS t(source, schedule_id, entity_type, entity_id, priority, requested_by, params)
            LEFT JOIN control.datasets d ON t.entity_type = 'table' AND d.id = t.entity_id AND d.is_active = TRUE
            LEFT JOIN control.compare_queries q ON t.entity_type = 'compare_query' AND q.id = t.entity_id AND q.is_active = TRUE
            WHERE NOT EXISTS (
                SELECT 1 FROM control.triggers tr
                WHERE tr.entity_type = t.entity_type
                AND tr.entity_id = t.entity_id
                AND tr.status IN ('queued', 'running')
            )
            AND (d.id IS NOT NULL OR q.id IS NOT NULL)
            RETURNING *
        """,
            sources,
            schedule_ids,
            entity_types,
            entity_ids,
            priorities,
            requested_bys,
            params_json,
        )

        return {"created": [serialize_row(r) for r in rows]}

    async def bulk_create_triggers(self, entity_type: str, entity_ids: list[int]) -> dict:
        """Bulk create triggers with status 'running'."""
        if not entity_ids:
            return {"created": [], "skipped": 0}

        rows = await self.db.fetch(
            """
            INSERT INTO control.triggers (
                source, entity_type, entity_id, status,
                priority, requested_by, requested_at
            )
            SELECT 'manual', $1, t.entity_id, 'running', 100, $2, now()
            FROM unnest($3::bigint[]) AS t(entity_id)
            LEFT JOIN control.datasets d ON $1 = 'table' AND d.id = t.entity_id AND d.is_active = TRUE
            LEFT JOIN control.compare_queries q ON $1 = 'compare_query' AND q.id = t.entity_id AND q.is_active = TRUE
            WHERE NOT EXISTS (
                SELECT 1 FROM control.triggers tr
                WHERE tr.entity_type = $1
                AND tr.entity_id = t.entity_id
                AND tr.status IN ('queued', 'running')
            )
            AND (d.id IS NOT NULL OR q.id IS NOT NULL)
            RETURNING id, entity_id
        """,
            entity_type,
            self.user_email,
            entity_ids,
        )

        created_ids = [r["id"] for r in rows]
        skipped = len(entity_ids) - len(created_ids)

        return {"created": created_ids, "skipped": skipped}

    async def cancel_trigger(self, trigger_id: int) -> dict:
        """Cancel a queued or running trigger."""
        trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
        return {"ok": True}

    async def launch_trigger(self, trigger_id: int) -> dict:
        """Manually launch a trigger."""
        trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        if trigger["status"] == "running" and trigger.get("databricks_run_id"):
            return {"launched": False, "reason": "Already running"}
        if trigger["status"] not in ("queued", "running"):
            raise HTTPException(status_code=400, detail=f"Trigger cannot be launched (status: {trigger['status']})")

        if trigger["entity_type"] == "table":
            entity = await self.db.fetchrow(
                "SELECT src_system_id, tgt_system_id FROM control.datasets WHERE id=$1", trigger["entity_id"]
            )
        else:
            entity = await self.db.fetchrow(
                "SELECT src_system_id, tgt_system_id FROM control.compare_queries WHERE id=$1", trigger["entity_id"]
            )

        if not entity:
            await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
            raise HTTPException(status_code=404, detail="Entity no longer exists")

        can_launch, reason = await self.check_system_concurrency(entity["src_system_id"], entity["tgt_system_id"])
        if not can_launch:
            return {"launched": False, "reason": reason}

        try:
            run_info = await self.launch_validation_job(trigger_id)
            return {"launched": True, "run_id": run_info["run_id"], "run_url": run_info["run_url"]}
        except HTTPException as e:
            return {"launched": False, "reason": e.detail}
        except Exception as e:
            return {"launched": False, "reason": str(e)}

    async def bulk_launch_triggers(self, trigger_ids: list[int]) -> dict:
        """Attempt to launch multiple queued triggers."""
        if not trigger_ids:
            return {"results": []}

        results = []
        for trigger_id in trigger_ids:
            trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
            if not trigger:
                results.append({"id": trigger_id, "launched": False, "reason": "Not found"})
                continue

            if trigger["status"] != "queued":
                results.append(
                    {"id": trigger_id, "launched": False, "reason": f"Not queued (status: {trigger['status']})"}
                )
                continue

            if trigger["entity_type"] == "table":
                entity = await self.db.fetchrow(
                    "SELECT src_system_id, tgt_system_id FROM control.datasets WHERE id=$1", trigger["entity_id"]
                )
            else:
                entity = await self.db.fetchrow(
                    "SELECT src_system_id, tgt_system_id FROM control.compare_queries WHERE id=$1", trigger["entity_id"]
                )

            if not entity:
                await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
                results.append({"id": trigger_id, "launched": False, "reason": "Entity no longer exists"})
                continue

            can_launch, reason = await self.check_system_concurrency(entity["src_system_id"], entity["tgt_system_id"])
            if not can_launch:
                results.append({"id": trigger_id, "launched": False, "reason": reason})
                continue

            try:
                run_info = await self.launch_validation_job(trigger_id)
                results.append(
                    {"id": trigger_id, "launched": True, "run_id": run_info["run_id"], "run_url": run_info["run_url"]}
                )
            except Exception as e:
                results.append({"id": trigger_id, "launched": False, "reason": str(e)})

        return {"results": results}

    async def repair_trigger(self, trigger_id: int) -> dict:
        """Repair a failed Databricks run for a trigger."""
        trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        if not trigger.get("databricks_run_id"):
            raise HTTPException(status_code=400, detail="No Databricks run associated with this trigger")

        try:
            repair_info = self.databricks.repair_run(int(trigger["databricks_run_id"]))
            new_run_url = repair_info.get("run_url") or trigger["databricks_run_url"]

            await self.db.execute(
                """
                UPDATE control.triggers
                SET status = 'running', started_at = now(), databricks_run_url = $2
                WHERE id = $1
            """,
                trigger_id,
                new_run_url,
            )

            return {
                "repaired": True,
                "run_id": trigger["databricks_run_id"],
                "repair_id": repair_info.get("repair_id"),
                "run_url": new_run_url,
            }
        except Exception as e:
            error_msg = str(e)
            if "INVALID_STATE" in error_msg or "in progress" in error_msg.lower():
                return {
                    "repaired": False,
                    "reason": "Run is still in progress. Wait for it to complete before repairing.",
                }
            if "not found" in error_msg.lower():
                return {"repaired": False, "reason": "Databricks run not found. It may have been deleted."}
            return {"repaired": False, "reason": error_msg}

    async def bulk_repair_triggers(self, trigger_ids: list[int]) -> dict:
        """Repair multiple failed triggers."""
        results = []

        for trigger_id in trigger_ids:
            try:
                trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
                if not trigger:
                    results.append({"id": trigger_id, "repaired": False, "reason": "Not found"})
                    continue

                if not trigger.get("databricks_run_id"):
                    results.append({"id": trigger_id, "repaired": False, "reason": "No run ID"})
                    continue

                repair_info = self.databricks.repair_run(int(trigger["databricks_run_id"]))
                new_run_url = repair_info.get("run_url") or trigger["databricks_run_url"]

                await self.db.execute(
                    """
                    UPDATE control.triggers
                    SET status = 'running', started_at = now(), databricks_run_url = $2
                    WHERE id = $1
                """,
                    trigger_id,
                    new_run_url,
                )

                results.append({"id": trigger_id, "repaired": True})
            except Exception as e:
                results.append({"id": trigger_id, "repaired": False, "reason": str(e)})

        return {"results": results}

    async def get_queue_status(self) -> dict:
        """Get queue statistics for dashboard."""
        stats = await self.db.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'queued') as queued,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                COUNT(*) as total_active
            FROM control.triggers
        """)

        recent = await self.db.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'succeeded') as succeeded,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) as total_completed
            FROM control.validation_history
            WHERE finished_at > now() - interval '1 hour'
        """)

        return {
            "active": stats if stats else {"queued": 0, "running": 0, "total_active": 0},
            "recent_1h": recent if recent else {"succeeded": 0, "failed": 0, "total_completed": 0},
        }

    async def get_running_per_system(self) -> dict[int, int]:
        """Get count of running validations per system."""
        rows = await self.db.fetch("""
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
                UNION ALL SELECT tgt_system_id as system_id FROM running_tables
                UNION ALL SELECT src_system_id as system_id FROM running_queries
                UNION ALL SELECT tgt_system_id as system_id FROM running_queries
            )
            SELECT system_id, COUNT(*) as count
            FROM all_running GROUP BY system_id
        """)

        return {row["system_id"]: int(row["count"]) for row in rows}

    async def get_next_trigger(self, worker_id: str) -> dict | None:
        """Worker polls this to get next trigger to execute."""
        max_retries = 50

        for _ in range(max_retries):
            row = await self.db.fetchrow(
                """
                UPDATE control.triggers
                SET status = 'running', worker_id = $1, locked_at = now(),
                    started_at = COALESCE(started_at, now()), attempts = attempts + 1
                WHERE id = (
                    SELECT id FROM control.triggers
                    WHERE status = 'queued'
                    ORDER BY priority ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
            """,
                worker_id,
            )

            if not row:
                return None

            if row["entity_type"] == "table":
                entity = await self.db.fetchrow("SELECT * FROM control.datasets WHERE id=$1", row["entity_id"])
            else:
                entity = await self.db.fetchrow("SELECT * FROM control.compare_queries WHERE id=$1", row["entity_id"])

            if not entity:
                await self.db.execute("DELETE FROM control.triggers WHERE id=$1", row["id"])
                continue

            break
        else:
            return None

        src_source_info = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", entity["src_system_id"])
        tgt_source_info = await self.db.fetchrow("SELECT * FROM control.systems WHERE id=$1", entity["tgt_system_id"])

        result = dict(entity)
        result["id"] = row["id"]
        result["entity_type"] = row["entity_type"]
        if row["entity_type"] == "table":
            result["source_table"] = f"{entity['src_schema'].strip()}.{entity['src_table'].strip()}"
            result["target_table"] = f"{entity['tgt_schema'].strip()}.{entity['tgt_table'].strip()}"
        result["watermark_expr"] = entity["watermark_filter"]
        result["src_system_name"] = src_source_info["name"]
        result["tgt_system_name"] = tgt_source_info["name"]

        return result

    async def update_trigger_run_id(self, trigger_id: int, run_id: str, run_url: str | None) -> dict:
        """Worker calls this after launching Databricks job to record run ID."""
        await self.db.execute(
            """
            UPDATE control.triggers
            SET databricks_run_id = $2, databricks_run_url = $3
            WHERE id = $1
        """,
            trigger_id,
            run_id,
            run_url,
        )
        return {"ok": True}

    async def release_trigger(self, trigger_id: int) -> dict:
        """Release a claimed trigger back to the queue."""
        await self.db.execute(
            """
            UPDATE control.triggers
            SET status = 'queued', worker_id = NULL, locked_at = NULL
            WHERE id = $1 AND status = 'running'
        """,
            trigger_id,
        )
        return {"ok": True}

    async def fail_trigger(self, trigger_id: int, status: str, error_message: str, error_details: dict | None) -> dict:
        """Worker calls this if it fails to launch the job."""
        trigger = await self.db.fetchrow("SELECT * FROM control.triggers WHERE id=$1", trigger_id)
        if not trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        await self.db.execute(
            """
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
        """,
            trigger_id,
            status,
            error_message,
            json.dumps(error_details or {}),
        )

        await self.db.execute("DELETE FROM control.triggers WHERE id=$1", trigger_id)
        return {"ok": True}
