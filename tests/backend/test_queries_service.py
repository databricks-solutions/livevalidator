"""Tests for backend/services/queries_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.queries_service import QueriesService
from tests.backend.conftest import MockDBSession


class TestListQueries:
    async def test_returns_all_queries(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetch_results([sample_query])
        service = QueriesService(mock_db, "test@test.com")
        result = await service.list_queries()
        assert len(result) == 1

    async def test_filters_by_search(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetch_results([sample_query])
        service = QueriesService(mock_db, "test@test.com")
        result = await service.list_queries(search="test")
        assert len(result) == 1


class TestGetQuery:
    async def test_returns_query_by_id(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(sample_query)
        service = QueriesService(mock_db, "test@test.com")
        result = await service.get_query(1)
        assert result["id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = QueriesService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.get_query(999)
        assert exc_info.value.status_code == 404


class TestCreateQuery:
    async def test_creates_query(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            {"id": 1},  # src system exists
            {"id": 2},  # tgt system exists
            None,  # name doesn't exist
            sample_query,  # created
        )
        service = QueriesService(mock_db, "test@test.com")
        result = await service.create_query({
            "name": "new-query",
            "src_system_id": 1,
            "tgt_system_id": 2,
            "sql": "SELECT 1",
        })
        assert result["id"] == 1

    async def test_rejects_invalid_src_system(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = QueriesService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.create_query({
                "name": "test",
                "src_system_id": 999,
                "tgt_system_id": 1,
                "sql": "SELECT 1",
            })
        assert exc_info.value.status_code == 400

    async def test_rejects_duplicate_name(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"id": 1},
            {"id": 2},
            {"id": 99},  # name exists
        )
        service = QueriesService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.create_query({
                "name": "existing",
                "src_system_id": 1,
                "tgt_system_id": 2,
                "sql": "SELECT 1",
            })
        assert exc_info.value.status_code == 409


class TestUpdateQuery:
    async def test_updates_query(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            None,  # name check (no conflict)
            {**sample_query, "version": 2},  # updated result
        )
        service = QueriesService(mock_db, "test@test.com")
        result = await service.update_query(1, {"name": "updated", "version": 1})
        assert result is not None

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            None,  # name check
            None,  # update fails
            sample_query,  # get current
        )
        service = QueriesService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.update_query(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteQuery:
    async def test_deletes_query(self, mock_db: MockDBSession):
        service = QueriesService(mock_db, "test@test.com")
        result = await service.delete_query(1)
        assert result["ok"] is True


class TestBulkCreateQueries:
    async def test_creates_multiple_queries(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(None, sample_query)
        service = QueriesService(mock_db, "test@test.com")
        result = await service.bulk_create_queries(1, 2, [
            {"sql": "SELECT 1"},
        ])
        assert "created" in result


class TestUpdateLineage:
    async def test_updates_lineage(self, mock_db: MockDBSession):
        service = QueriesService(mock_db, "test@test.com")
        result = await service.update_lineage(1, {"upstream": ["table1"]})
        assert result["ok"] is True
