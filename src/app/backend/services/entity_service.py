"""Unified entity service for tables and queries."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel

from backend.models import QueryIn, QueryUpdate, TableIn, TableUpdate
from backend.utils import raise_version_conflict

if TYPE_CHECKING:
    from backend.dependencies import DBSession

EntityType = Literal["table", "query"]


class EntityService:
    """Unified service for table and query CRUD operations."""

    def __init__(self, db: DBSession, user_email: str, entity_type: EntityType):
        self.db = db
        self.user_email = user_email
        self.entity_type = entity_type
        self.db_table = "control.datasets" if entity_type == "table" else "control.compare_queries"
        self.binding_type = "table" if entity_type == "table" else "compare_query"
        self.label = entity_type
        self.create_model = TableIn if entity_type == "table" else QueryIn
        self.update_model = TableUpdate if entity_type == "table" else QueryUpdate
        self.columns = list(self.create_model.model_fields.keys())

    def _get_value(self, data: dict, key: str, model: type[BaseModel]) -> Any:
        """Get value from data, applying defaults from model and JSON serialization."""
        field = model.model_fields[key]
        default = field.default_factory() if field.default_factory else field.default
        val = data.get(key, default)
        if key in ("options", "config_overrides") and isinstance(val, (dict, list)):
            val = json.dumps(val)
        return val

    def _get_values(self, data: dict, model: type[BaseModel]) -> list[Any]:
        """Extract values from data dict based on model fields."""
        return [self._get_value(data, key, model) for key in self.columns]

    def _build_insert_sql(self) -> str:
        """Build INSERT SQL from model fields."""
        cols = self.columns + ["created_by", "updated_by"]
        n = len(self.columns)
        placeholders = [f"${i + 1}" for i in range(n)] + [f"${n + 1}", f"${n + 1}"]
        return f"INSERT INTO {self.db_table} ({', '.join(cols)}) VALUES ({', '.join(placeholders)}) RETURNING *"

    def _build_update_sql(self) -> str:
        """Build UPDATE SQL from model fields with COALESCE for partial updates."""
        sets = [f"{col}=COALESCE(${i + 2},{col})" for i, col in enumerate(self.columns)]
        n = len(self.columns)
        sets += [f"updated_by=${n + 2}", "updated_at=now()", "version=version+1"]
        return f"UPDATE {self.db_table} SET {', '.join(sets)} WHERE id=$1 AND version=${n + 3} RETURNING *"

    def _default_name(self, item: dict, idx: int) -> str:
        if self.entity_type == "table":
            return item.get("name") or f"{item['src_schema']}.{item['src_table']}"
        return item.get("name") or f"Query {idx + 1}"

    async def _require_system(self, system_id: int, label: str) -> None:
        """Raise 400 if system doesn't exist."""
        if not await self.db.fetchrow("SELECT id FROM control.systems WHERE id=$1", system_id):
            raise HTTPException(400, f"{label} system ID {system_id} does not exist")

    async def _get_system_id_by_name(self, name: str, label: str) -> int:
        """Get system ID by name, raise ValueError if not found."""
        row = await self.db.fetchrow("SELECT id FROM control.systems WHERE name=$1", name)
        if not row:
            raise ValueError(f"{label} system '{name}' not found")
        return row["id"]

    async def _require_name_unique(self, name: str, exclude_id: int | None = None) -> None:
        """Raise 409 if name already exists."""
        if exclude_id:
            existing = await self.db.fetchrow(
                f"SELECT id FROM {self.db_table} WHERE name=$1 AND id!=$2", name, exclude_id
            )
        else:
            existing = await self.db.fetchrow(f"SELECT id FROM {self.db_table} WHERE name=$1", name)
        if existing:
            raise HTTPException(409, f"A {self.label} with name '{name}' already exists")

    async def list(self, search: str | None = None) -> list[dict]:
        """List entities with last run status, tags, and schedules."""
        where = "WHERE e.name ILIKE $1" if search else ""
        sql = f"""
            SELECT e.*,
                vh.id as last_run_id, vh.status as last_run_status,
                vh.finished_at as last_run_timestamp, vh.error_message as last_run_error,
                vh.row_count_match as last_run_row_count_match, vh.rows_different as last_run_rows_different,
                COALESCE(
                    (SELECT json_agg(t.name ORDER BY t.name)
                     FROM control.entity_tags et JOIN control.tags t ON et.tag_id = t.id
                     WHERE et.entity_type = '{self.label}' AND et.entity_id = e.id),
                    '[]'::json
                ) as tags,
                COALESCE(
                    (SELECT json_agg(s.name ORDER BY s.name)
                     FROM control.schedule_bindings sb JOIN control.schedules s ON sb.schedule_id = s.id
                     WHERE sb.entity_type = '{self.binding_type}' AND sb.entity_id = e.id),
                    '[]'::json
                ) as schedules
            FROM {self.db_table} e
            LEFT JOIN LATERAL (
                SELECT id, status, finished_at, error_message, row_count_match, rows_different
                FROM control.validation_history
                WHERE entity_type = '{self.binding_type}' AND entity_id = e.id
                ORDER BY finished_at DESC LIMIT 1
            ) vh ON true
            {where}
            ORDER BY e.name
        """
        if search:
            return await self.db.fetch(sql, f"%{search}%")
        return await self.db.fetch(sql)

    async def get(self, entity_id: int) -> dict:
        """Get entity by ID."""
        row = await self.db.fetchrow(f"SELECT * FROM {self.db_table} WHERE id=$1", entity_id)
        if not row:
            raise HTTPException(404, "not found")
        return row

    async def create(self, data: dict) -> dict:
        """Create a new entity."""
        await self._require_system(data["src_system_id"], "Source")
        await self._require_system(data["tgt_system_id"], "Target")
        await self._require_name_unique(data["name"])
        return await self.db.fetchrow(
            self._build_insert_sql(), *self._get_values(data, self.create_model), self.user_email
        )

    async def update(self, entity_id: int, data: dict) -> dict:
        """Update entity with optimistic locking."""
        if data.get("name"):
            await self._require_name_unique(data["name"], exclude_id=entity_id)
        if data.get("src_system_id"):
            await self._require_system(data["src_system_id"], "Source")
        if data.get("tgt_system_id"):
            await self._require_system(data["tgt_system_id"], "Target")

        row = await self.db.fetchrow(
            self._build_update_sql(),
            entity_id,
            *self._get_values(data, self.update_model),
            self.user_email,
            data["version"],
        )
        if not row:
            current = await self.db.fetchrow(f"SELECT * FROM {self.db_table} WHERE id=$1", entity_id)
            raise_version_conflict(current)
        return row

    async def delete(self, entity_id: int) -> dict:
        """Delete entity."""
        await self.db.execute(f"DELETE FROM {self.db_table} WHERE id=$1", entity_id)
        return {"ok": True}

    async def update_lineage(self, entity_id: int, lineage: dict | list | None) -> dict:
        """Update lineage JSONB field."""
        await self.db.execute(
            f"UPDATE {self.db_table} SET lineage=$1::jsonb WHERE id=$2",
            json.dumps(lineage) if lineage is not None else None,
            entity_id,
        )
        return {"ok": True}

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk operations
    # ─────────────────────────────────────────────────────────────────────────

    async def _resolve_system_ids(self, item: dict, default_src: int, default_tgt: int) -> tuple[int, int]:
        src_id = (
            await self._get_system_id_by_name(item["src_system_name"], "Source")
            if item.get("src_system_name")
            else default_src
        )
        tgt_id = (
            await self._get_system_id_by_name(item["tgt_system_name"], "Target")
            if item.get("tgt_system_name")
            else default_tgt
        )
        return src_id, tgt_id

    def _parse_schedule_names(self, item: dict) -> list[str]:
        names = item.get("schedule_names") or []
        if not names and item.get("schedule_name"):
            names = [s.strip() for s in item["schedule_name"].split(",") if s.strip()]
        return names

    async def _bind_schedules(self, entity_id: int, sched_names: list[str], clear: bool = False) -> None:
        if clear:
            await self.db.execute(
                "DELETE FROM control.schedule_bindings WHERE entity_type=$1 AND entity_id=$2",
                self.binding_type,
                entity_id,
            )
        for name in sched_names:
            sched = await self.db.fetchrow("SELECT id FROM control.schedules WHERE name=$1", name)
            if sched:
                await self.db.execute(
                    """INSERT INTO control.schedule_bindings (schedule_id, entity_type, entity_id)
                    VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
                    sched["id"],
                    self.binding_type,
                    entity_id,
                )

    async def _bind_tags(self, entity_id: int, tags: list[str]) -> None:
        for tag_name in tags:
            tag_name = tag_name.strip()
            if not tag_name:
                continue
            tag = await self.db.fetchrow("SELECT id FROM control.tags WHERE name=$1", tag_name)
            if not tag:
                tag = await self.db.fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)
            await self.db.execute(
                """INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
                self.label,
                entity_id,
                tag["id"],
            )

    async def _bulk_insert(self, item: dict, name: str, src_id: int, tgt_id: int) -> dict:
        data = {**item, "name": name, "src_system_id": src_id, "tgt_system_id": tgt_id}
        if self.entity_type == "table":
            data.setdefault("tgt_schema", item.get("src_schema"))
            data.setdefault("tgt_table", item.get("src_table"))
        # Bulk doesn't use options column
        cols = [c for c in self.columns if c != "options"] + ["created_by", "updated_by"]
        vals = [self._get_value(data, c, self.create_model) for c in self.columns if c != "options"] + [self.user_email]
        n = len(vals)
        placeholders = [f"${i + 1}" for i in range(n)] + [f"${n}"]
        sql = f"INSERT INTO {self.db_table} ({', '.join(cols)}) VALUES ({', '.join(placeholders)}) RETURNING *"
        return await self.db.fetchrow(sql, *vals)

    async def _bulk_update(self, entity_id: int, item: dict, src_id: int, tgt_id: int) -> dict:
        data = {**item, "src_system_id": src_id, "tgt_system_id": tgt_id}
        if self.entity_type == "table":
            data.setdefault("tgt_schema", item.get("src_schema"))
            data.setdefault("tgt_table", item.get("src_table"))
        # Bulk update: skip name and options
        skip = {"name", "options"}
        sets, vals = [], [entity_id]
        for i, col in enumerate(c for c in self.columns if c not in skip):
            sets.append(f"{col}=${i + 2}")
            vals.append(self._get_value(data, col, self.update_model))
        idx = len(vals) + 1
        sets += [f"updated_by=${idx}", "updated_at=now()", "version=version+1"]
        vals.append(self.user_email)
        sql = f"UPDATE {self.db_table} SET {', '.join(sets)} WHERE id=$1 RETURNING *"
        return await self.db.fetchrow(sql, *vals)

    async def bulk_create(self, src_system_id: int, tgt_system_id: int, items: list[dict]) -> dict:
        """Bulk create/update entities."""
        results: dict = {"created": [], "updated": [], "errors": []}

        for idx, item in enumerate(items):
            try:
                name = self._default_name(item, idx)
                src_id, tgt_id = await self._resolve_system_ids(item, src_system_id, tgt_system_id)
                sched_names = self._parse_schedule_names(item)
                existing = await self.db.fetchrow(f"SELECT id FROM {self.db_table} WHERE name=$1", name)

                if existing:
                    row = await self._bulk_update(existing["id"], item, src_id, tgt_id)
                    if sched_names:
                        await self._bind_schedules(row["id"], sched_names, clear=True)
                    results["updated"].append({"row": idx + 1, "name": name, "data": row})
                else:
                    row = await self._bulk_insert(item, name, src_id, tgt_id)
                    await self._bind_schedules(row["id"], sched_names)
                    if item.get("tags"):
                        await self._bind_tags(row["id"], item["tags"])
                    results["created"].append({"row": idx + 1, "name": name, "data": row})
            except Exception as e:
                results["errors"].append({"row": idx + 1, "error": str(e)})

        return results
