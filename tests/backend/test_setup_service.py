"""Tests for backend/services/setup_service.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.services.setup_service import SetupService
from tests.backend.conftest import MockDBSession


class TestInitializeDatabase:
    @patch.object(Path, "exists")
    @patch.object(Path, "read_text")
    async def test_initializes_database(self, mock_read, mock_exists, mock_db: MockDBSession):
        mock_exists.return_value = True
        mock_read.return_value = "CREATE SCHEMA control;"
        service = SetupService(mock_db)
        result = await service.initialize_database()
        assert result["ok"] is True
        assert "initialized" in result["message"]

    @patch.object(Path, "exists")
    async def test_raises_when_ddl_not_found(self, mock_exists, mock_db: MockDBSession):
        mock_exists.return_value = False
        service = SetupService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.initialize_database()
        assert exc_info.value.status_code == 500
        assert "DDL file not found" in str(exc_info.value.detail)


class TestResetDatabase:
    @patch.object(Path, "exists")
    @patch.object(Path, "read_text")
    async def test_resets_database(self, mock_read, mock_exists, mock_db: MockDBSession):
        mock_exists.return_value = True
        mock_read.return_value = "DROP TABLE test;"
        service = SetupService(mock_db)
        result = await service.reset_database()
        assert result["ok"] is True
        assert "reset" in result["message"]

    @patch.object(Path, "exists")
    async def test_raises_when_drop_file_not_found(self, mock_exists, mock_db: MockDBSession):
        mock_exists.side_effect = [False]  # drop_tables.sql not found
        service = SetupService(mock_db)
        with pytest.raises(HTTPException) as exc_info:
            await service.reset_database()
        assert exc_info.value.status_code == 500
