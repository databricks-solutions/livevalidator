"""Tests for backend/services/validation_history_service.py."""

from datetime import datetime

import pytest
from fastapi import HTTPException

from backend.services.validation_history_service import ValidationHistoryService
from tests.backend.conftest import MockDBSession


@pytest.fixture
def sample_validation() -> dict:
    return {
        "id": 1,
        "trigger_id": 1,
        "entity_type": "table",
        "entity_id": 1,
        "entity_name": "test.table",
        "source": "manual",
        "schedule_id": None,
        "requested_by": "test@test.com",
        "requested_at": datetime(2024, 1, 15, 10, 0, 0),
        "started_at": datetime(2024, 1, 15, 10, 0, 5),
        "finished_at": datetime(2024, 1, 15, 10, 1, 0),
        "duration_seconds": 55,
        "source_system_name": "Source",
        "target_system_name": "Target",
        "source_table": "source.table",
        "target_table": "target.table",
        "pk_columns": [],
        "status": "succeeded",
        "schema_match": True,
        "row_count_match": True,
        "row_count_source": 100,
        "row_count_target": 100,
        "rows_compared": 100,
        "rows_different": 0,
        "difference_pct": 0.0,
        "compare_mode": "except_all",
        "error_message": None,
        "databricks_run_url": "https://databricks.com/run/123",
    }


class TestListValidationHistory:
    async def test_returns_results_with_stats(self, mock_db: MockDBSession, sample_validation):
        mock_db.set_fetchrow_results({"total": 1, "succeeded": 1, "failed": 0, "errors": 0})
        mock_db.set_fetch_results([sample_validation], [])  # results then tags
        service = ValidationHistoryService(mock_db)
        result = await service.list_validation_history()
        assert "data" in result
        assert "stats" in result
        assert result["stats"]["total"] == 1

    async def test_filters_by_entity_type(self, mock_db: MockDBSession, sample_validation):
        mock_db.set_fetchrow_results({"total": 1, "succeeded": 1, "failed": 0, "errors": 0})
        mock_db.set_fetch_results([sample_validation], [])
        service = ValidationHistoryService(mock_db)
        result = await service.list_validation_history(entity_type="table")
        assert len(result["data"]) == 1


class TestGetValidationDetail:
    async def test_returns_validation(self, mock_db: MockDBSession, sample_validation):
        mock_db.set_fetchrow_results({**sample_validation, "sample_differences": None})
        service = ValidationHistoryService(mock_db)
        result = await service.get_validation_detail(1)
        assert result["id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = ValidationHistoryService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.get_validation_detail(999)
        assert exc_info.value.status_code == 404


class TestGetLatestValidation:
    async def test_returns_latest(self, mock_db: MockDBSession, sample_validation):
        mock_db.set_fetchrow_results({**sample_validation, "sample_differences": None})
        service = ValidationHistoryService(mock_db)
        result = await service.get_latest_validation("table", 1)
        assert result["id"] == 1

    async def test_returns_none_when_no_history(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = ValidationHistoryService(mock_db)
        result = await service.get_latest_validation("table", 999)
        assert result is None


class TestDeleteValidationHistory:
    async def test_deletes_records(self, mock_db: MockDBSession):
        service = ValidationHistoryService(mock_db)
        result = await service.delete_validation_history([1, 2, 3])
        assert result["ok"] is True
        assert result["deleted_count"] == 3

    async def test_raises_400_for_empty_ids(self, mock_db: MockDBSession):
        service = ValidationHistoryService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.delete_validation_history([])
        assert exc_info.value.status_code == 400


class TestUpdateValidationHistory:
    async def test_updates_record(self, mock_db: MockDBSession):
        mock_db.set_execute_results("UPDATE 1")
        service = ValidationHistoryService(mock_db)
        result = await service.update_validation_history(1, {"status": "failed"})
        assert result["ok"] is True
        assert "status" in result["updated_fields"]

    async def test_raises_400_for_invalid_fields(self, mock_db: MockDBSession):
        service = ValidationHistoryService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.update_validation_history(1, {"invalid_field": "value"})
        assert exc_info.value.status_code == 400
