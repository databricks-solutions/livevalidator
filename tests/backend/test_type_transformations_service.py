"""Tests for backend/services/type_transformations_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.type_transformations_service import TypeTransformationsService
from tests.backend.conftest import MockDBSession


@pytest.fixture
def sample_transformation() -> dict:
    return {
        "id": 1,
        "system_a_id": 1,
        "system_b_id": 2,
        "system_a_function": "def transform_columns(column_name: str, data_type: str) -> str:\n    return column_name",
        "system_b_function": "def transform_columns(column_name: str, data_type: str) -> str:\n    return column_name",
        "system_a_name": "Source",
        "system_a_kind": "Databricks",
        "system_b_name": "Target",
        "system_b_kind": "Postgres",
        "updated_by": "test@test.com",
        "version": 1,
    }


class TestListTypeTransformations:
    async def test_returns_all(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetch_results([sample_transformation])
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.list_type_transformations()
        assert len(result) == 1


class TestGetDefaultTransformationForSystem:
    def test_returns_default_for_databricks(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = service.get_default_transformation_for_system("Databricks")
        assert result["system_kind"] == "Databricks"
        assert "function" in result


class TestGetTypeTransformation:
    async def test_returns_transformation(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetchrow_results(sample_transformation)
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.get_type_transformation(1, 2)
        assert result["system_a_id"] == 1

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = TypeTransformationsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.get_type_transformation(1, 2)
        assert exc_info.value.status_code == 404


class TestGetTypeTransformationForValidation:
    async def test_returns_existing(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetchrow_results(sample_transformation)
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.get_type_transformation_for_validation(1, 2)
        assert result["exists"] is True

    async def test_returns_default_when_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(
            None,  # no transformation
            {"name": "Source", "kind": "Databricks"},
            {"name": "Target", "kind": "Postgres"},
        )
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.get_type_transformation_for_validation(1, 2)
        assert result["exists"] is False


class TestCreateTypeTransformation:
    async def test_creates_transformation(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetchrow_results(
            {"id": 1, "kind": "Databricks"},
            {"id": 2, "kind": "Postgres"},
            sample_transformation,
        )
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.create_type_transformation({
            "system_a_id": 1,
            "system_b_id": 2,
            "system_a_function": "def transform_columns(c, t): return c",
            "system_b_function": "def transform_columns(c, t): return c",
        })
        assert result is not None


class TestUpdateTypeTransformation:
    async def test_updates_transformation(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetchrow_results(
            sample_transformation,  # current
            {**sample_transformation, "version": 2},  # updated
        )
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.update_type_transformation(1, 2, {"version": 1})
        assert result is not None

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_transformation):
        mock_db.set_fetchrow_results({**sample_transformation, "version": 5})
        service = TypeTransformationsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.update_type_transformation(1, 2, {"version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteTypeTransformation:
    async def test_deletes(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = await service.delete_type_transformation(1, 2)
        assert result["ok"] is True


class TestValidatePythonCode:
    def test_valid_code(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        code = "def transform_columns(column_name: str, data_type: str) -> str:\n    return column_name"
        result = service.validate_python_code(code)
        assert result["valid"] is True

    def test_invalid_syntax(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = service.validate_python_code("def bad(")
        assert result["valid"] is False

    def test_wrong_function_name(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        code = "def wrong_name(column_name: str, data_type: str) -> str:\n    return column_name"
        result = service.validate_python_code(code)
        assert result["valid"] is False
        assert any("transform_columns" in e["message"] for e in result["errors"])

    def test_missing_function(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        result = service.validate_python_code("x = 1")
        assert result["valid"] is False

    def test_wrong_parameter_count(self, mock_db: MockDBSession):
        service = TypeTransformationsService(mock_db, "test@test.com")
        code = "def transform_columns(column_name: str) -> str:\n    return column_name"
        result = service.validate_python_code(code)
        assert result["valid"] is False
