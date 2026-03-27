"""Tests for backend/services/validation_config_service.py."""

import json

from backend.services.validation_config_service import DEFAULT_CONFIG, ValidationConfigService
from tests.backend.conftest import MockDBSession


class TestGetValidationConfig:
    async def test_returns_config_with_defaults(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"settings": json.dumps({"downgrade_unicode": True})})
        service = ValidationConfigService(mock_db)
        result = await service.get_validation_config()
        assert result["downgrade_unicode"] is True
        assert result["skip_row_validation"] is False  # Default applied

    async def test_returns_defaults_when_no_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = ValidationConfigService(mock_db)
        result = await service.get_validation_config()
        assert result == DEFAULT_CONFIG


class TestUpdateValidationConfig:
    async def test_updates_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"settings": json.dumps({"downgrade_unicode": True})})
        service = ValidationConfigService(mock_db, "test@test.com")
        result = await service.update_validation_config({"downgrade_unicode": True})
        assert result is not None
        execute_calls = mock_db.get_calls("execute")
        assert any("INSERT INTO control.config" in call[0] for call in execute_calls)


class TestGetEffectiveConfig:
    async def test_merges_entity_override(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"settings": json.dumps({"downgrade_unicode": False})},  # global config
            {"config_overrides": json.dumps({"downgrade_unicode": True})},  # entity override
        )
        service = ValidationConfigService(mock_db)
        result = await service.get_effective_config("table", 1)
        assert result["downgrade_unicode"] is True

    async def test_returns_global_when_no_override(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"settings": json.dumps({"downgrade_unicode": True})},  # global config
            {"config_overrides": None},  # entity has no override
        )
        service = ValidationConfigService(mock_db)
        result = await service.get_effective_config("table", 1)
        assert result["downgrade_unicode"] is True
