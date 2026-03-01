"""Tests for backend/services/users_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.users_service import UsersService
from tests.backend.conftest import MockDBSession


class TestGetDefaultUserRole:
    async def test_returns_value_from_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"value": "CAN_VIEW"})
        service = UsersService(mock_db)
        result = await service.get_default_user_role()
        assert result == "CAN_VIEW"

    async def test_returns_can_manage_when_no_config(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = UsersService(mock_db)
        result = await service.get_default_user_role()
        assert result == "CAN_MANAGE"


class TestEnsureUserExists:
    async def test_does_nothing_if_user_exists(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"1": 1})  # user exists
        service = UsersService(mock_db)
        await service.ensure_user_exists("test@test.com")
        # Should only have one fetchrow call, no execute
        assert len(mock_db.get_calls("execute")) == 0

    async def test_creates_user_if_not_exists(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None, {"value": "CAN_EDIT"})  # user doesn't exist, then get default role
        service = UsersService(mock_db)
        await service.ensure_user_exists("new@test.com")
        execute_calls = mock_db.get_calls("execute")
        assert len(execute_calls) == 1
        assert "INSERT INTO control.user_roles" in execute_calls[0][0]


class TestGetUserRole:
    async def test_returns_role_from_db(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_EDIT"})
        service = UsersService(mock_db)
        result = await service.get_user_role("test@test.com")
        assert result == "CAN_EDIT"

    async def test_returns_default_if_no_role(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None, {"value": "CAN_VIEW"})  # no role, then config
        service = UsersService(mock_db)
        result = await service.get_user_role("test@test.com")
        assert result == "CAN_VIEW"


class TestCanEditObject:
    async def test_can_view_cannot_edit(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_VIEW"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "tables", 1)
        assert result is False

    async def test_can_manage_can_edit_anything(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_MANAGE"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "systems", 1)
        assert result is True

    async def test_can_edit_can_edit_tables(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_EDIT"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "tables", 1)
        assert result is True

    async def test_can_edit_cannot_edit_systems(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_EDIT"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "systems", 1)
        assert result is False

    async def test_can_run_can_edit_own_table(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_RUN"}, {"created_by": "test@test.com"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "tables", 1)
        assert result is True

    async def test_can_run_cannot_edit_others_table(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_RUN"}, {"created_by": "other@test.com"})
        service = UsersService(mock_db)
        result = await service.can_edit_object("test@test.com", "tables", 1)
        assert result is False


class TestRequireRole:
    async def test_passes_with_allowed_role(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_MANAGE"})
        service = UsersService(mock_db)
        await service.require_role("test@test.com", "CAN_EDIT", "CAN_MANAGE")

    async def test_raises_403_without_allowed_role(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"role": "CAN_VIEW"})
        service = UsersService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.require_role("test@test.com", "CAN_EDIT", "CAN_MANAGE")
        assert exc_info.value.status_code == 403


class TestListUsers:
    async def test_returns_all_users(self, mock_db: MockDBSession):
        mock_db.set_fetch_results([
            {"user_email": "a@test.com", "role": "CAN_VIEW"},
            {"user_email": "b@test.com", "role": "CAN_MANAGE"},
        ])
        service = UsersService(mock_db)
        result = await service.list_users()
        assert len(result) == 2


class TestSetUserRole:
    async def test_updates_role(self, mock_db: MockDBSession):
        service = UsersService(mock_db)
        result = await service.set_user_role("test@test.com", "CAN_EDIT", "admin@test.com")
        assert result["user_email"] == "test@test.com"
        assert result["role"] == "CAN_EDIT"

    async def test_rejects_invalid_role(self, mock_db: MockDBSession):
        service = UsersService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.set_user_role("test@test.com", "INVALID_ROLE", "admin@test.com")
        assert exc_info.value.status_code == 400


class TestDeleteUserRole:
    async def test_deletes_role(self, mock_db: MockDBSession):
        service = UsersService(mock_db)
        result = await service.delete_user_role("test@test.com")
        assert "message" in result
        execute_calls = mock_db.get_calls("execute")
        assert len(execute_calls) == 1
        assert "DELETE FROM control.user_roles" in execute_calls[0][0]
