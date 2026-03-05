"""Validation configuration service."""

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.dependencies import DBSession

DEFAULT_CONFIG: dict[str, Any] = {
    "downgrade_unicode": False,
    "replace_special_char": [],
    "extra_replace_regex": "",
    "skip_row_validation": False,
    "max_sample_rows": 100,
}


def _parse_settings(settings: Any) -> dict[str, Any]:
    """Parse settings from DB - handles both string and dict."""
    if settings is None:
        return {}
    if isinstance(settings, str):
        return json.loads(settings)
    return dict(settings)


class ValidationConfigService:
    """Handles global and entity-level validation configuration."""

    def __init__(self, db: "DBSession", user_email: str = "system"):
        self.db = db
        self.user_email = user_email

    async def get_validation_config(self) -> dict[str, Any]:
        """Get global config with defaults applied."""
        row = await self.db.fetchrow(
            "SELECT settings FROM control.config WHERE scope='global' AND scope_id IS NULL"
        )
        return {**DEFAULT_CONFIG, **_parse_settings(row["settings"] if row else None)}

    async def update_validation_config(self, data: dict) -> dict[str, Any]:
        """Update global config (merges with existing)."""
        current = await self.get_validation_config()
        merged = {**current, **data}
        await self.db.execute(
            """
            INSERT INTO control.config (scope, scope_id, settings, updated_by, updated_at)
            VALUES ('global', NULL, $1::jsonb, $2, now())
            ON CONFLICT (scope, COALESCE(scope_id, -1))
            DO UPDATE SET settings = $1::jsonb, updated_by = $2, updated_at = now()
            """,
            json.dumps(merged),
            self.user_email,
        )
        return await self.get_validation_config()

    async def get_effective_config(self, entity_type: str, entity_id: int) -> dict[str, Any]:
        """Get merged config: defaults -> global -> entity override."""
        global_cfg = await self.get_validation_config()

        override = await self.db.fetchrow(
            "SELECT settings FROM control.config WHERE scope=$1 AND scope_id=$2",
            entity_type,
            entity_id,
        )
        if override:
            return {**global_cfg, **_parse_settings(override["settings"])}

        # Fallback: check deprecated config_overrides column for backward compat
        if entity_type == "table":
            entity = await self.db.fetchrow(
                "SELECT config_overrides FROM control.datasets WHERE id=$1", entity_id
            )
        else:
            entity = await self.db.fetchrow(
                "SELECT config_overrides FROM control.compare_queries WHERE id=$1", entity_id
            )

        if entity and entity.get("config_overrides"):
            return {**global_cfg, **_parse_settings(entity["config_overrides"])}

        return global_cfg
