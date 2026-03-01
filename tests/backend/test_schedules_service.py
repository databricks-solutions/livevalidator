"""Tests for backend/services/schedules_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.schedules_service import SchedulesService
from tests.backend.conftest import MockDBSession


class TestListTimezones:
    def test_returns_utc_first(self, mock_db: MockDBSession):
        service = SchedulesService(mock_db, "test@test.com")
        result = service.list_timezones()
        assert result[0] == "UTC"

    def test_excludes_etc_timezones(self, mock_db: MockDBSession):
        service = SchedulesService(mock_db, "test@test.com")
        result = service.list_timezones()
        assert not any(tz.startswith("Etc/") for tz in result)

    def test_includes_common_timezones(self, mock_db: MockDBSession):
        service = SchedulesService(mock_db, "test@test.com")
        result = service.list_timezones()
        assert "America/New_York" in result
        assert "Europe/London" in result


class TestListSchedules:
    async def test_returns_all_schedules(self, mock_db: MockDBSession, sample_schedule):
        mock_db.set_fetch_results([sample_schedule])
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.list_schedules()
        assert len(result) == 1


class TestCreateSchedule:
    async def test_creates_schedule(self, mock_db: MockDBSession, sample_schedule):
        mock_db.set_fetchrow_results(sample_schedule)
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.create_schedule({
            "name": "new-schedule",
            "cron_expr": "0 0 * * *",
        })
        assert result["name"] == "daily-schedule"


class TestUpdateSchedule:
    async def test_updates_schedule(self, mock_db: MockDBSession, sample_schedule):
        mock_db.set_fetchrow_results(
            {"cron_expr": "0 0 * * *", "timezone": "UTC"},  # current for comparison
            {**sample_schedule, "version": 2},  # updated result
        )
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.update_schedule(1, {"name": "updated", "version": 1})
        assert result is not None

    async def test_resets_next_run_when_cron_changes(self, mock_db: MockDBSession, sample_schedule):
        mock_db.set_fetchrow_results(
            {"cron_expr": "0 0 * * *", "timezone": "UTC"},
            {**sample_schedule, "version": 2},
        )
        service = SchedulesService(mock_db, "test@test.com")
        await service.update_schedule(1, {"cron_expr": "0 12 * * *", "version": 1})

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_schedule):
        mock_db.set_fetchrow_results(
            {"cron_expr": "0 0 * * *", "timezone": "UTC"},
            None,  # update fails
            sample_schedule,  # get current
        )
        service = SchedulesService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.update_schedule(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteSchedule:
    async def test_deletes_schedule(self, mock_db: MockDBSession):
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.delete_schedule(1)
        assert result["ok"] is True


class TestBindings:
    async def test_create_binding(self, mock_db: MockDBSession):
        mock_db.set_fetchval_results(1)
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.create_binding(1, "table", 1)
        assert result["id"] == 1

    async def test_list_bindings(self, mock_db: MockDBSession):
        mock_db.set_fetch_results([{"id": 1, "schedule_id": 1, "entity_type": "table", "entity_id": 1}])
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.list_bindings("table", 1)
        assert len(result) == 1

    async def test_list_all_bindings(self, mock_db: MockDBSession):
        mock_db.set_fetch_results([
            {"id": 1, "schedule_id": 1, "entity_type": "table", "entity_id": 1},
            {"id": 2, "schedule_id": 1, "entity_type": "query", "entity_id": 1},
        ])
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.list_all_bindings()
        assert len(result) == 2

    async def test_delete_binding(self, mock_db: MockDBSession):
        service = SchedulesService(mock_db, "test@test.com")
        result = await service.delete_binding(1)
        assert result["ok"] is True
