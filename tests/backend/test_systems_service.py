"""Tests for backend/services/systems_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.systems_service import SystemsService
from tests.backend.conftest import MockDBSession


class TestListSystems:
    async def test_returns_active_systems(self, mock_db: MockDBSession, sample_system):
        mock_db.set_fetch_results([sample_system])
        service = SystemsService(mock_db, "test@test.com")
        result = await service.list_systems()
        assert len(result) == 1
        assert result[0]["name"] == "test-system"


class TestGetSystem:
    async def test_returns_system_by_id(self, mock_db: MockDBSession, sample_system):
        mock_db.set_fetchrow_results(sample_system)
        service = SystemsService(mock_db, "test@test.com")
        result = await service.get_system(1)
        assert result["id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = SystemsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.get_system(999)
        assert exc_info.value.status_code == 404


class TestGetSystemByName:
    async def test_returns_system_by_name(self, mock_db: MockDBSession, sample_system):
        mock_db.set_fetchrow_results(sample_system)
        service = SystemsService(mock_db, "test@test.com")
        result = await service.get_system_by_name("test-system")
        assert result["name"] == "test-system"

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = SystemsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.get_system_by_name("nonexistent")
        assert exc_info.value.status_code == 404


class TestCreateSystem:
    async def test_creates_system(self, mock_db: MockDBSession, sample_system):
        mock_db.set_fetchrow_results(sample_system)
        service = SystemsService(mock_db, "test@test.com")
        result = await service.create_system({
            "name": "new-system",
            "kind": "Databricks",
            "catalog": "test",
        })
        assert result["name"] == "test-system"


class TestUpdateSystem:
    async def test_updates_system(self, mock_db: MockDBSession, sample_system):
        updated = {**sample_system, "name": "updated-name", "version": 2}
        mock_db.set_fetchrow_results(updated)
        service = SystemsService(mock_db, "test@test.com")
        result = await service.update_system(1, {"name": "updated-name", "version": 1})
        assert result["name"] == "updated-name"

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_system):
        mock_db.set_fetchrow_results(None, sample_system)  # update fails, then get current
        service = SystemsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.update_system(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["error"] == "version_conflict"


class TestDeleteSystem:
    async def test_deletes_system(self, mock_db: MockDBSession):
        service = SystemsService(mock_db, "test@test.com")
        result = await service.delete_system(1)
        assert result["ok"] is True
        execute_calls = mock_db.get_calls("execute")
        assert any("DELETE FROM control.systems" in call[0] for call in execute_calls)
