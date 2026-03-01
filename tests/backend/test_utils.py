"""Tests for backend/utils.py."""

from datetime import datetime

import pytest
from fastapi import HTTPException

from backend.utils import raise_version_conflict, serialize_row


class TestSerializeRow:
    def test_none_input(self):
        assert serialize_row(None) is None

    def test_empty_dict(self):
        assert serialize_row({}) == {}

    def test_simple_dict(self):
        row = {"id": 1, "name": "test"}
        assert serialize_row(row) == {"id": 1, "name": "test"}

    def test_datetime_conversion(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        row = {"id": 1, "created_at": dt}
        result = serialize_row(row)
        assert result["id"] == 1
        assert result["created_at"] == "2024-01-15T10:30:00"

    def test_multiple_datetimes(self):
        dt1 = datetime(2024, 1, 15, 10, 30, 0)
        dt2 = datetime(2024, 2, 20, 15, 45, 30)
        row = {"created_at": dt1, "updated_at": dt2, "name": "test"}
        result = serialize_row(row)
        assert result["created_at"] == "2024-01-15T10:30:00"
        assert result["updated_at"] == "2024-02-20T15:45:30"
        assert result["name"] == "test"

    def test_preserves_original(self):
        original = {"id": 1, "name": "test"}
        serialize_row(original)
        assert original == {"id": 1, "name": "test"}


class TestRaiseVersionConflict:
    def test_raises_409(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_version_conflict({"id": 1, "version": 5})
        assert exc_info.value.status_code == 409

    def test_includes_error_type(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_version_conflict({"id": 1})
        assert exc_info.value.detail["error"] == "version_conflict"

    def test_includes_current_record(self):
        current = {"id": 1, "version": 5, "name": "test"}
        with pytest.raises(HTTPException) as exc_info:
            raise_version_conflict(current)
        assert exc_info.value.detail["current"]["id"] == 1
        assert exc_info.value.detail["current"]["version"] == 5

    def test_serializes_datetime_in_current(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        current = {"id": 1, "updated_at": dt}
        with pytest.raises(HTTPException) as exc_info:
            raise_version_conflict(current)
        assert exc_info.value.detail["current"]["updated_at"] == "2024-01-15T10:30:00"

    def test_handles_none_current(self):
        with pytest.raises(HTTPException) as exc_info:
            raise_version_conflict(None)
        assert exc_info.value.detail["current"] is None
