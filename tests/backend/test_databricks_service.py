"""Tests for backend/services/databricks_service.py."""

import os
from unittest.mock import MagicMock, patch

import pytest

from backend.services.databricks_service import DatabricksService


class MockClient:
    """Mock Databricks client for testing."""

    def __init__(self):
        self.config = MagicMock()
        self.config.host = "https://test.databricks.com"
        self.jobs = MagicMock()


class TestLaunchJob:
    def test_launches_job_with_params(self):
        mock_client = MockClient()
        mock_run = MagicMock()
        mock_run.run_id = 12345
        mock_client.jobs.run_now.return_value = mock_run

        service = DatabricksService(mock_client)
        run_id, run_url = service.launch_job(999, {"key": "value"})

        assert run_id == 12345
        assert "12345" in run_url
        mock_client.jobs.run_now.assert_called_once()


class TestGetRunStatus:
    def test_returns_status_dict(self):
        mock_client = MockClient()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "TERMINATED"
        mock_run.state.result_state.value = "SUCCESS"
        mock_run.state.state_message = None
        mock_client.jobs.get_run.return_value = mock_run

        service = DatabricksService(mock_client)
        status = service.get_run_status(12345)

        assert status["life_cycle_state"] == "TERMINATED"
        assert status["result_state"] == "SUCCESS"
        assert status["done"] is True
        assert status["failed"] is False

    def test_handles_running_state(self):
        mock_client = MockClient()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "RUNNING"
        mock_run.state.result_state = None
        mock_run.state.state_message = None
        mock_client.jobs.get_run.return_value = mock_run

        service = DatabricksService(mock_client)
        status = service.get_run_status(12345)

        assert status["life_cycle_state"] == "RUNNING"
        assert status["done"] is False

    def test_detects_failed_state(self):
        mock_client = MockClient()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "TERMINATED"
        mock_run.state.result_state.value = "FAILED"
        mock_run.state.state_message = "Task failed"
        mock_client.jobs.get_run.return_value = mock_run

        service = DatabricksService(mock_client)
        status = service.get_run_status(12345)

        assert status["failed"] is True
        assert status["done"] is True


class TestGetRunStatuses:
    def test_returns_multiple_statuses(self):
        mock_client = MockClient()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "TERMINATED"
        mock_run.state.result_state.value = "SUCCESS"
        mock_run.state.state_message = None
        mock_client.jobs.get_run.return_value = mock_run

        service = DatabricksService(mock_client)
        statuses = service.get_run_statuses([1, 2, 3])

        assert len(statuses) == 3
        assert all(s["done"] for s in statuses.values())

    def test_returns_empty_for_empty_list(self):
        mock_client = MockClient()
        service = DatabricksService(mock_client)
        statuses = service.get_run_statuses([])
        assert statuses == {}


class TestRepairRun:
    def test_repairs_run(self):
        mock_client = MockClient()
        mock_run_info = MagicMock()
        mock_run_info.repair_history = None
        mock_run_info.job_parameters = None

        mock_repair = MagicMock()
        mock_repair.response = MagicMock()
        mock_repair.response.repair_id = 999

        mock_updated_run = MagicMock()
        mock_updated_run.run_page_url = "https://databricks.com/run/123"

        mock_client.jobs.get_run.side_effect = [mock_run_info, mock_updated_run]
        mock_client.jobs.repair_run.return_value = mock_repair

        service = DatabricksService(mock_client)
        result = service.repair_run(12345)

        assert result["repair_id"] == 999
        assert "run_url" in result


class TestStaticMethods:
    @patch.dict(os.environ, {"VALIDATION_JOB_ID": "123"})
    def test_get_validation_job_id(self):
        assert DatabricksService.get_validation_job_id() == "123"

    @patch.dict(os.environ, {"LINEAGE_JOB_ID": "456"})
    def test_get_lineage_job_id(self):
        assert DatabricksService.get_lineage_job_id() == "456"

    @patch.dict(os.environ, {"DATABRICKS_APP_URL": "https://backend.com"})
    def test_get_backend_url(self):
        assert DatabricksService.get_backend_url() == "https://backend.com"

    @patch.dict(os.environ, {}, clear=True)
    def test_get_backend_url_default(self):
        result = DatabricksService.get_backend_url()
        assert result == ""


class TestHost:
    def test_returns_host_from_client(self):
        mock_client = MockClient()
        service = DatabricksService(mock_client)
        assert service.host == "https://test.databricks.com"
