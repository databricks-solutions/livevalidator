"""Pytest fixtures for backend unit tests."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


class MockDBSession:
    """Mock database session for unit tests."""

    def __init__(self):
        self._fetch_results: list[list[dict]] = []
        self._fetchrow_results: list[dict | None] = []
        self._fetchval_results: list[Any] = []
        self._execute_results: list[str] = []
        self._call_log: list[tuple[str, str, tuple]] = []

    def set_fetch_results(self, *results: list[dict]) -> None:
        """Set results for fetch() calls (in order)."""
        self._fetch_results = list(results)

    def set_fetchrow_results(self, *results: dict | None) -> None:
        """Set results for fetchrow() calls (in order)."""
        self._fetchrow_results = list(results)

    def set_fetchval_results(self, *results: Any) -> None:
        """Set results for fetchval() calls (in order)."""
        self._fetchval_results = list(results)

    def set_execute_results(self, *results: str) -> None:
        """Set results for execute() calls (in order)."""
        self._execute_results = list(results)

    @property
    def calls(self) -> list[tuple[str, str, tuple]]:
        """Return all logged calls as (method, sql, args)."""
        return self._call_log

    def get_calls(self, method: str | None = None) -> list[tuple[str, tuple]]:
        """Get calls filtered by method, returns (sql, args)."""
        if method is None:
            return [(sql, args) for _, sql, args in self._call_log]
        return [(sql, args) for m, sql, args in self._call_log if m == method]

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        self._call_log.append(("fetch", sql, args))
        if self._fetch_results:
            return self._fetch_results.pop(0)
        return []

    async def fetchrow(self, sql: str, *args: Any) -> dict | None:
        self._call_log.append(("fetchrow", sql, args))
        if self._fetchrow_results:
            return self._fetchrow_results.pop(0)
        return None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self._call_log.append(("fetchval", sql, args))
        if self._fetchval_results:
            return self._fetchval_results.pop(0)
        return None

    async def execute(self, sql: str, *args: Any) -> str:
        self._call_log.append(("execute", sql, args))
        if self._execute_results:
            return self._execute_results.pop(0)
        return "UPDATE 1"


class MockDatabricksService:
    """Mock Databricks service for unit tests."""

    def __init__(self):
        self.launch_job_result: tuple[int, str] = (12345, "https://databricks.com/run/12345")
        self.get_run_status_result: dict = {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "failed": False,
            "done": True,
        }
        self.repair_run_result: dict = {"repair_id": 999, "run_url": "https://databricks.com/run/12345"}
        self._call_log: list[tuple[str, tuple, dict]] = []

    @property
    def host(self) -> str:
        return "https://mock-databricks.com"

    def launch_job(self, job_id: int, params: dict) -> tuple[int, str]:
        self._call_log.append(("launch_job", (job_id,), params))
        return self.launch_job_result

    def get_run_status(self, run_id: int) -> dict:
        self._call_log.append(("get_run_status", (run_id,), {}))
        return self.get_run_status_result

    def get_run_statuses(self, run_ids: list[int]) -> dict[int, dict]:
        self._call_log.append(("get_run_statuses", (run_ids,), {}))
        return {rid: self.get_run_status_result for rid in run_ids}

    def repair_run(self, run_id: int) -> dict:
        self._call_log.append(("repair_run", (run_id,), {}))
        return self.repair_run_result

    @staticmethod
    def get_validation_job_id() -> str | None:
        return "123"

    @staticmethod
    def get_lineage_job_id() -> str | None:
        return "456"

    @staticmethod
    def get_backend_url() -> str:
        return "https://mock-backend.com"


@pytest.fixture
def mock_db() -> MockDBSession:
    """Provide a fresh MockDBSession for each test."""
    return MockDBSession()


@pytest.fixture
def mock_databricks() -> MockDatabricksService:
    """Provide a fresh MockDatabricksService for each test."""
    return MockDatabricksService()


# Common test data fixtures
@pytest.fixture
def sample_system() -> dict:
    return {
        "id": 1,
        "name": "test-system",
        "kind": "Databricks",
        "catalog": "test_catalog",
        "host": None,
        "port": None,
        "database": None,
        "secret_scope": "livevalidator",
        "user_secret_key": None,
        "pass_secret_key": None,
        "jdbc_string": None,
        "driver_connector": None,
        "concurrency": -1,
        "max_rows": None,
        "options": "{}",
        "is_active": True,
        "created_by": "test@test.com",
        "updated_by": "test@test.com",
        "version": 1,
    }


@pytest.fixture
def sample_table() -> dict:
    return {
        "id": 1,
        "name": "test.table",
        "src_system_id": 1,
        "src_schema": "test_schema",
        "src_table": "test_table",
        "tgt_system_id": 2,
        "tgt_schema": "test_schema",
        "tgt_table": "test_table",
        "compare_mode": "except_all",
        "pk_columns": [],
        "watermark_filter": None,
        "include_columns": [],
        "exclude_columns": [],
        "options": "{}",
        "is_active": True,
        "created_by": "test@test.com",
        "updated_by": "test@test.com",
        "version": 1,
    }


@pytest.fixture
def sample_query() -> dict:
    return {
        "id": 1,
        "name": "test-query",
        "src_system_id": 1,
        "tgt_system_id": 2,
        "sql": "SELECT * FROM test",
        "compare_mode": "except_all",
        "pk_columns": [],
        "watermark_filter": None,
        "options": "{}",
        "is_active": True,
        "created_by": "test@test.com",
        "updated_by": "test@test.com",
        "version": 1,
    }


@pytest.fixture
def sample_schedule() -> dict:
    return {
        "id": 1,
        "name": "daily-schedule",
        "cron_expr": "0 0 * * *",
        "timezone": "UTC",
        "enabled": True,
        "max_concurrency": 4,
        "backfill_policy": "none",
        "created_by": "test@test.com",
        "updated_by": "test@test.com",
        "version": 1,
    }


@pytest.fixture
def sample_trigger() -> dict:
    return {
        "id": 1,
        "source": "manual",
        "schedule_id": None,
        "entity_type": "table",
        "entity_id": 1,
        "status": "queued",
        "priority": 100,
        "requested_by": "test@test.com",
        "databricks_run_id": None,
        "databricks_run_url": None,
    }


@pytest.fixture
def sample_user_role() -> dict:
    return {
        "user_email": "test@test.com",
        "role": "CAN_MANAGE",
        "assigned_by": "admin@test.com",
    }
