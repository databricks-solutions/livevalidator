"""Tags service for entity tagging."""

from typing import TYPE_CHECKING

from fastapi import HTTPException

if TYPE_CHECKING:
    from backend.dependencies import DBSession


class TagsService:
    """Handles tag CRUD and entity-tag associations."""

    def __init__(self, db: "DBSession"):
        self.db = db

    async def list_tags(self) -> list[dict]:
        """Get all tags."""
        return await self.db.fetch("SELECT * FROM control.tags ORDER BY name")

    async def create_tag(self, name: str) -> dict:
        """Create a new tag (or return existing if name already exists)."""
        name = name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="Tag name is required")

        existing = await self.db.fetchrow("SELECT * FROM control.tags WHERE name = $1", name)
        if existing:
            return existing

        row = await self.db.fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING *", name)
        return row

    async def get_entity_tags(self, entity_type: str, entity_id: int) -> list[dict]:
        """Get all tags for a specific entity."""
        return await self.db.fetch(
            """
            SELECT t.id, t.name
            FROM control.tags t
            JOIN control.entity_tags et ON et.tag_id = t.id
            WHERE et.entity_type = $1 AND et.entity_id = $2
            ORDER BY t.name
        """,
            entity_type,
            entity_id,
        )

    async def get_tag_entities(self, tag_id: int) -> list[dict]:
        """Get all entities (with names) associated with a tag."""
        if not await self.db.fetchrow("SELECT id FROM control.tags WHERE id = $1", tag_id):
            raise HTTPException(status_code=404, detail="Tag not found")

        return await self.db.fetch(
            """
            SELECT et.entity_type, et.entity_id,
                CASE et.entity_type
                    WHEN 'table' THEN d.name
                    WHEN 'query' THEN cq.name
                END AS name
            FROM control.entity_tags et
            LEFT JOIN control.datasets d
                ON et.entity_type = 'table' AND et.entity_id = d.id
            LEFT JOIN control.compare_queries cq
                ON et.entity_type = 'query' AND et.entity_id = cq.id
            WHERE et.tag_id = $1
            ORDER BY et.entity_type, name
        """,
            tag_id,
        )

    async def set_entity_tags(self, entity_type: str, entity_id: int, tag_names: list[str]) -> dict:
        """Set tags for an entity (replaces existing tags)."""
        await self.db.execute(
            "DELETE FROM control.entity_tags WHERE entity_type = $1 AND entity_id = $2", entity_type, entity_id
        )

        for tag_name in tag_names:
            tag_name = tag_name.strip()
            if not tag_name:
                continue

            tag = await self.db.fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
            if not tag:
                tag = await self.db.fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)

            await self.db.execute(
                """
                INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """,
                entity_type,
                entity_id,
                tag["id"],
            )

        await self._cleanup_unused_tags()
        return {"ok": True}

    async def bulk_add_tags(self, entity_type: str, entity_ids: list[int], tag_names: list[str]) -> dict:
        """Add tags to multiple entities."""
        if not entity_type or not entity_ids or not tag_names:
            raise HTTPException(status_code=400, detail="entity_type, entity_ids, and tags are required")

        for tag_name in tag_names:
            tag_name = tag_name.strip()
            if not tag_name:
                continue

            tag = await self.db.fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
            if not tag:
                tag = await self.db.fetchrow("INSERT INTO control.tags (name) VALUES ($1) RETURNING id", tag_name)

            for entity_id in entity_ids:
                await self.db.execute(
                    """
                    INSERT INTO control.entity_tags (entity_type, entity_id, tag_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                """,
                    entity_type,
                    entity_id,
                    tag["id"],
                )

        return {"ok": True}

    async def bulk_remove_tags(self, entity_type: str, entity_ids: list[int], tag_names: list[str]) -> dict:
        """Remove tags from multiple entities."""
        if not entity_type or not entity_ids or not tag_names:
            raise HTTPException(status_code=400, detail="entity_type, entity_ids, and tags are required")

        for tag_name in tag_names:
            tag_name = tag_name.strip()
            if not tag_name:
                continue

            tag = await self.db.fetchrow("SELECT id FROM control.tags WHERE name = $1", tag_name)
            if not tag:
                continue

            await self.db.execute(
                """
                DELETE FROM control.entity_tags
                WHERE entity_type = $1 AND entity_id = ANY($2) AND tag_id = $3
            """,
                entity_type,
                entity_ids,
                tag["id"],
            )

        await self._cleanup_unused_tags()
        return {"ok": True}

    async def _cleanup_unused_tags(self) -> None:
        """Remove tags that are not associated with any entity."""
        await self.db.execute("""
            DELETE FROM control.tags
            WHERE id NOT IN (SELECT DISTINCT tag_id FROM control.entity_tags)
        """)
