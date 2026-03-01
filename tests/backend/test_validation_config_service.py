"""Tests for backend/services/validation_config_service.py."""

from backend.services.validation_config_service import ValidationConfigService
from tests.backend.conftest import MockDBSession


class TestGetValidationConfig:
    async def test_returns_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({
            "downgrade_unicode": True,
            "replace_special_char": ["7F", "?"],
            "extra_replace_regex": "",
        })
        service = ValidationConfigService(mock_db)
        result = await service.get_validation_config()
        assert result["downgrade_unicode"] is True

    async def test_returns_defaults_when_no_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = ValidationConfigService(mock_db)
        result = await service.get_validation_config()
        assert result["downgrade_unicode"] is False
        assert result["replace_special_char"] == []


class TestUpdateValidationConfig:
    async def test_updates_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({
            "downgrade_unicode": True,
            "replace_special_char": [],
            "extra_replace_regex": "",
        })
        service = ValidationConfigService(mock_db, "test@test.com")
        result = await service.update_validation_config({"downgrade_unicode": True})
        assert result is not None
        execute_calls = mock_db.get_calls("execute")
        assert any("UPDATE control.validation_config" in call[0] for call in execute_calls)
