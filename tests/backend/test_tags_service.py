"""Tests for backend/services/tags_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.tags_service import TagsService
from tests.backend.conftest import MockDBSession


class TestListTags:
    async def test_returns_all_tags(self, mock_db: MockDBSession):
        mock_db.set_fetch_results([{"id": 1, "name": "tag1"}, {"id": 2, "name": "tag2"}])
        service = TagsService(mock_db)
        result = await service.list_tags()
        assert len(result) == 2
        assert result[0]["name"] == "tag1"


class TestCreateTag:
    async def test_creates_new_tag(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None, {"id": 1, "name": "new-tag"})  # not exists, then created
        service = TagsService(mock_db)
        result = await service.create_tag("new-tag")
        assert result["name"] == "new-tag"

    async def test_returns_existing_tag(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1, "name": "existing-tag"})
        service = TagsService(mock_db)
        result = await service.create_tag("existing-tag")
        assert result["id"] == 1

    async def test_rejects_empty_name(self, mock_db: MockDBSession):
        service = TagsService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.create_tag("")
        assert exc_info.value.status_code == 400

    async def test_rejects_whitespace_only(self, mock_db: MockDBSession):
        service = TagsService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.create_tag("   ")
        assert exc_info.value.status_code == 400


class TestGetEntityTags:
    async def test_returns_entity_tags(self, mock_db: MockDBSession):
        mock_db.set_fetch_results([{"id": 1, "name": "tag1"}, {"id": 2, "name": "tag2"}])
        service = TagsService(mock_db)
        result = await service.get_entity_tags("table", 1)
        assert len(result) == 2


class TestSetEntityTags:
    async def test_replaces_existing_tags(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1}, {"id": 2})  # tag lookups
        service = TagsService(mock_db)
        result = await service.set_entity_tags("table", 1, ["tag1", "tag2"])
        assert result["ok"] is True
        # Should have delete call first
        execute_calls = mock_db.get_calls("execute")
        assert any("DELETE FROM control.entity_tags" in call[0] for call in execute_calls)

    async def test_creates_new_tags_if_needed(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None, {"id": 99})  # tag doesn't exist, then created
        service = TagsService(mock_db)
        await service.set_entity_tags("table", 1, ["new-tag"])
        fetchrow_calls = mock_db.get_calls("fetchrow")
        assert any("INSERT INTO control.tags" in call[0] for call in fetchrow_calls)

    async def test_skips_empty_tag_names(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1})  # one valid tag found
        service = TagsService(mock_db)
        result = await service.set_entity_tags("table", 1, ["valid", "", "  "])
        # Should return ok even when some tags are skipped
        assert result["ok"] is True


class TestBulkAddTags:
    async def test_adds_tags_to_multiple_entities(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1})  # tag exists
        service = TagsService(mock_db)
        result = await service.bulk_add_tags("table", [1, 2, 3], ["tag1"])
        assert result["ok"] is True

    async def test_rejects_empty_params(self, mock_db: MockDBSession):
        service = TagsService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.bulk_add_tags("", [], [])
        assert exc_info.value.status_code == 400


class TestBulkRemoveTags:
    async def test_removes_tags_from_entities(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results({"id": 1})  # tag exists
        service = TagsService(mock_db)
        result = await service.bulk_remove_tags("table", [1, 2], ["tag1"])
        assert result["ok"] is True
        execute_calls = mock_db.get_calls("execute")
        assert any("DELETE FROM control.entity_tags" in call[0] for call in execute_calls)

    async def test_skips_nonexistent_tags(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)  # tag doesn't exist
        service = TagsService(mock_db)
        result = await service.bulk_remove_tags("table", [1], ["nonexistent"])
        assert result["ok"] is True
