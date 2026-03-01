"""Tests for backend/services/triggers_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.triggers_service import TriggersService
from tests.backend.conftest import MockDBSession, MockDatabricksService


class TestCheckSystemConcurrency:
    async def test_allows_when_no_limits(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"name": "sys1", "concurrency": -1},
            {"name": "sys2", "concurrency": -1},
        )
        mock_db.set_fetch_results([])
        service = TriggersService(mock_db, "test@test.com")
        can_launch, reason = await service.check_system_concurrency(1, 2)
        assert can_launch is True
        assert reason == ""

    async def test_blocks_when_at_capacity(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"name": "sys1", "concurrency": 2},
            {"name": "sys2", "concurrency": -1},
        )
        mock_db.set_fetch_results([{"system_id": 1, "count": 2}])
        service = TriggersService(mock_db, "test@test.com")
        can_launch, reason = await service.check_system_concurrency(1, 2)
        assert can_launch is False
        assert "at capacity" in reason


class TestListTriggers:
    async def test_returns_triggers(self, mock_db: MockDBSession, mock_databricks: MockDatabricksService):
        mock_db.set_fetch_results([{
            "id": 1,
            "entity_type": "table",
            "entity_id": 1,
            "entity_name": "test.table",
            "status": "queued",
            "databricks_run_id": None,
            "entity_tags": [],
        }])
        service = TriggersService(mock_db, "test@test.com", mock_databricks)
        result = await service.list_triggers()
        assert len(result) == 1

    async def test_filters_by_status(self, mock_db: MockDBSession, mock_databricks: MockDatabricksService):
        mock_db.set_fetch_results([])
        service = TriggersService(mock_db, "test@test.com", mock_databricks)
        result = await service.list_triggers(status="running")
        assert len(result) == 0


class TestCreateTrigger:
    async def test_creates_trigger(
        self, mock_db: MockDBSession, sample_table, sample_system
    ):
        """Test trigger creation (without launching to avoid complex mock setup)."""
        mock_db.set_fetchrow_results(
            sample_table,  # entity exists
            None,  # no existing trigger
            {"name": "sys1", "concurrency": 1},  # src system with limit
            {"name": "sys2", "concurrency": -1},  # tgt system
            {"id": 1, "entity_type": "table", "entity_id": 1, "status": "queued"},  # created
        )
        mock_db.set_fetch_results([{"system_id": 1, "count": 1}])  # at capacity - will queue
        service = TriggersService(mock_db, "test@test.com")
        result = await service.create_trigger({
            "entity_type": "table",
            "entity_id": 1,
        })
        assert "id" in result or "queued_reason" in result

    async def test_queues_when_at_capacity(
        self, mock_db: MockDBSession, mock_databricks: MockDatabricksService, sample_table, sample_system
    ):
        mock_db.set_fetchrow_results(
            sample_table,  # entity exists
            None,  # no existing trigger
            {"name": "sys1", "concurrency": 1},  # src system with limit
            {"name": "sys2", "concurrency": -1},  # tgt system
            {"id": 1, "entity_type": "table", "entity_id": 1, "status": "queued"},  # created
        )
        mock_db.set_fetch_results([{"system_id": 1, "count": 1}])  # at capacity
        service = TriggersService(mock_db, "test@test.com", mock_databricks)
        result = await service.create_trigger({
            "entity_type": "table",
            "entity_id": 1,
        })
        assert "queued_reason" in result

    async def test_rejects_duplicate_trigger(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            sample_table,  # entity exists
            {"id": 99},  # existing trigger
        )
        service = TriggersService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.create_trigger({
                "entity_type": "table",
                "entity_id": 1,
            })
        assert exc_info.value.status_code == 409

    async def test_rejects_nonexistent_entity(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = TriggersService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.create_trigger({
                "entity_type": "table",
                "entity_id": 999,
            })
        assert exc_info.value.status_code == 404


class TestCancelTrigger:
    async def test_cancels_trigger(self, mock_db: MockDBSession, sample_trigger):
        mock_db.set_fetchrow_results(sample_trigger)
        service = TriggersService(mock_db, "test@test.com")
        result = await service.cancel_trigger(1)
        assert result["ok"] is True

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = TriggersService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.cancel_trigger(999)
        assert exc_info.value.status_code == 404


class TestRepairTrigger:
    async def test_repairs_trigger(self, mock_db: MockDBSession, mock_databricks: MockDatabricksService):
        mock_db.set_fetchrow_results({"id": 1, "databricks_run_id": "123", "databricks_run_url": "http://test"})
        service = TriggersService(mock_db, "test@test.com", mock_databricks)
        result = await service.repair_trigger(1)
        assert result["repaired"] is True

    async def test_raises_400_if_no_run_id(self, mock_db: MockDBSession, mock_databricks: MockDatabricksService):
        mock_db.set_fetchrow_results({"id": 1, "databricks_run_id": None})
        service = TriggersService(mock_db, "test@test.com", mock_databricks)
        with pytest.raises(HTTPException) as exc_info:
            await service.repair_trigger(1)
        assert exc_info.value.status_code == 400


class TestGetQueueStatus:
    async def test_returns_status(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"queued": 5, "running": 2, "total_active": 7},
            {"succeeded": 10, "failed": 1, "total_completed": 11},
        )
        service = TriggersService(mock_db, "test@test.com")
        result = await service.get_queue_status()
        assert "active" in result
        assert "recent_1h" in result


class TestWorkerEndpoints:
    async def test_get_next_trigger(self, mock_db: MockDBSession, sample_table, sample_system):
        mock_db.set_fetchrow_results(
            {"id": 1, "entity_type": "table", "entity_id": 1},  # next trigger
            sample_table,  # entity
            sample_system,  # src system
            sample_system,  # tgt system
        )
        service = TriggersService(mock_db, "test@test.com")
        result = await service.get_next_trigger("worker-1")
        assert result is not None

    async def test_get_next_trigger_returns_none_when_empty(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = TriggersService(mock_db, "test@test.com")
        result = await service.get_next_trigger("worker-1")
        assert result is None

    async def test_update_trigger_run_id(self, mock_db: MockDBSession):
        service = TriggersService(mock_db, "test@test.com")
        result = await service.update_trigger_run_id(1, "12345", "http://url")
        assert result["ok"] is True

    async def test_release_trigger(self, mock_db: MockDBSession):
        service = TriggersService(mock_db, "test@test.com")
        result = await service.release_trigger(1)
        assert result["ok"] is True

    async def test_fail_trigger(self, mock_db: MockDBSession, sample_trigger):
        mock_db.set_fetchrow_results(sample_trigger)
        service = TriggersService(mock_db, "test@test.com")
        result = await service.fail_trigger(1, "error", "Something went wrong", None)
        assert result["ok"] is True


