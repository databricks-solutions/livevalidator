"""Tests for EntityService with entity_type='table'."""

import pytest
from fastapi import HTTPException

from backend.services.entity_service import EntityService
from tests.backend.conftest import MockDBSession


class TestListTables:
    async def test_returns_all_tables(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetch_results([sample_table])
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.list()
        assert len(result) == 1

    async def test_filters_by_search(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetch_results([sample_table])
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.list(search="test")
        assert len(result) == 1


class TestGetTable:
    async def test_returns_table_by_id(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(sample_table)
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.get(1)
        assert result["id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.get(999)
        assert exc_info.value.status_code == 404


class TestCreateTable:
    async def test_creates_table(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            {"id": 1},  # src system exists
            {"id": 2},  # tgt system exists
            None,  # name doesn't exist
            sample_table,  # created
        )
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.create({
            "name": "new.table",
            "src_system_id": 1,
            "src_schema": "schema",
            "src_table": "table",
            "tgt_system_id": 2,
        })
        assert result["id"] == 1

    async def test_rejects_invalid_src_system(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)  # src system doesn't exist
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
                "name": "test",
                "src_system_id": 999,
                "src_schema": "s",
                "src_table": "t",
                "tgt_system_id": 1,
            })
        assert exc_info.value.status_code == 400
        assert "Source system" in str(exc_info.value.detail)

    async def test_rejects_invalid_tgt_system(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1}, None)  # src exists, tgt doesn't
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
                "name": "test",
                "src_system_id": 1,
                "src_schema": "s",
                "src_table": "t",
                "tgt_system_id": 999,
            })
        assert exc_info.value.status_code == 400
        assert "Target system" in str(exc_info.value.detail)

    async def test_rejects_duplicate_name(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            {"id": 1},  # src exists
            {"id": 2},  # tgt exists
            {"id": 99},  # name already exists
        )
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.create({
                "name": "existing",
                "src_system_id": 1,
                "src_schema": "s",
                "src_table": "t",
                "tgt_system_id": 2,
            })
        assert exc_info.value.status_code == 409


class TestUpdateTable:
    async def test_updates_table(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            None,  # name check (no conflict)
            {**sample_table, "version": 2},  # updated result
        )
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.update(1, {"name": "updated", "version": 1})
        assert result is not None

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            None,  # name check
            None,  # update fails
            sample_table,  # get current
        )
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.update(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409

    async def test_rejects_duplicate_name_on_update(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results({"id": 99})  # another table has this name
        service = EntityService(mock_db, "test@test.com", "table")
        with pytest.raises(HTTPException) as exc_info:
            await service.update(1, {"name": "taken", "version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteTable:
    async def test_deletes_table(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.delete(1)
        assert result["ok"] is True


class TestBulkCreateTables:
    async def test_creates_multiple_tables(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            None,  # first table doesn't exist
            sample_table,  # created
        )
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.bulk_create(1, 2, [
            {"src_schema": "s1", "src_table": "t1"},
        ])
        assert "created" in result

    async def test_updates_existing_tables(self, mock_db: MockDBSession, sample_table):
        mock_db.set_fetchrow_results(
            {"id": 1, "version": 1},  # table exists
            sample_table,  # updated
        )
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.bulk_create(1, 2, [
            {"src_schema": "s1", "src_table": "t1"},
        ])
        assert "updated" in result


class TestUpdateLineage:
    async def test_updates_lineage(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.update_lineage(1, {"upstream": ["table1"]})
        assert result["ok"] is True

    async def test_clears_lineage_with_none(self, mock_db: MockDBSession):
        service = EntityService(mock_db, "test@test.com", "table")
        result = await service.update_lineage(1, None)
        assert result["ok"] is True
