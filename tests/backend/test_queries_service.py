"""Tests for EntityService with entity_type='query'."""

import pytest
from fastapi import HTTPException

from backend.services.entity_service import EntityService
from tests.backend.conftest import MockDBSession


class TestListQueries:
    async def test_returns_all_queries(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetch_results([sample_query])
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.list()
        assert len(result) == 1

    async def test_filters_by_search(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetch_results([sample_query])
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.list(search="test")
        assert len(result) == 1


class TestGetQuery:
    async def test_returns_query_by_id(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(sample_query)
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.get(1)
        assert result["id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.get(999)
        assert exc_info.value.status_code == 404


class TestCreateQuery:
    async def test_creates_query(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            {"id": 1},  # src system exists
            {"id": 2},  # tgt system exists
            None,  # name doesn't exist
            sample_query,  # created
        )
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.create({
            "name": "new-query",
            "src_system_id": 1,
            "tgt_system_id": 2,
            "sql": "SELECT 1",
        })
        assert result["id"] == 1

    async def test_rejects_invalid_src_system(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
                "name": "test",
                "src_system_id": 999,
                "tgt_system_id": 1,
                "sql": "SELECT 1",
            })
        assert exc_info.value.status_code == 400

    async def test_rejects_invalid_tgt_system(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1}, None)  # src exists, tgt doesn't
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
                "name": "test",
                "src_system_id": 1,
                "tgt_system_id": 999,
                "sql": "SELECT 1",
            })
        assert exc_info.value.status_code == 400
        assert "Target system" in str(exc_info.value.detail)

    async def test_rejects_duplicate_name(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"id": 1},
            {"id": 2},
            {"id": 99},  # name exists
        )
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
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
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.update(1, {"name": "updated", "version": 1})
        assert result is not None

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            None,  # name check
            None,  # update fails
            sample_query,  # get current
        )
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.update(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409

    async def test_rejects_duplicate_name_on_update(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 99})  # another query has this name
        service = EntityService(mock_db, "test@test.com", "query")
        with pytest.raises(HTTPException) as exc_info:
            await service.update(1, {"name": "taken", "version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteQuery:
    async def test_deletes_query(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.delete(1)
        assert result["ok"] is True


class TestBulkCreateQueries:
    async def test_creates_multiple_queries(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(None, sample_query)
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.bulk_create(1, 2, [
            {"sql": "SELECT 1"},
        ])
        assert "created" in result

    async def test_updates_existing_queries(self, mock_db: MockDBSession, sample_query):
        mock_db.set_fetchrow_results(
            {"id": 1, "version": 1},  # query exists
            sample_query,  # updated
        )
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.bulk_create(1, 2, [
            {"sql": "SELECT 1"},
        ])
        assert "updated" in result


class TestUpdateLineage:
    async def test_updates_lineage(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.update_lineage(1, {"upstream": ["table1"]})
        assert result["ok"] is True

    async def test_clears_lineage_with_none(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "query")
        result = await service.update_lineage(1, None)
        assert result["ok"] is True
