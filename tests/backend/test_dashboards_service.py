"""Tests for backend/services/dashboards_service.py."""

import pytest
from fastapi import HTTPException

from backend.services.dashboards_service import DashboardsService
from tests.backend.conftest import MockDBSession


@pytest.fixture
def sample_dashboard() -> dict:
    return {
        "id": 1,
        "name": "Test Dashboard",
        "project": "Test Project",
        "layout": "{}",
        "created_by": "test@test.com",
        "updated_by": "test@test.com",
        "version": 1,
    }


@pytest.fixture
def sample_chart() -> dict:
    return {
        "id": 1,
        "dashboard_id": 1,
        "chart_type": "bar",
        "config": "{}",
        "position": 0,
    }


class TestListDashboards:
    async def test_returns_all_dashboards(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetch_results([sample_dashboard])
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.list_dashboards()
        assert len(result) == 1


class TestGetDashboard:
    async def test_returns_dashboard_with_charts(self, mock_db: MockDBSession, sample_dashboard, sample_chart):
        mock_db.set_fetchrow_results(sample_dashboard)
        mock_db.set_fetch_results([sample_chart])
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.get_dashboard(1)
        assert result["id"] == 1
        assert "charts" in result

    async def test_raises_404_if_not_found(self, mock_db: MockDBSession):
        mock_db.set_fetchrow_results(None)
        service = DashboardsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.get_dashboard(999)
        assert exc_info.value.status_code == 404


class TestCreateDashboard:
    async def test_creates_dashboard(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(sample_dashboard)
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.create_dashboard({"name": "New Dashboard"})
        assert result["name"] == "Test Dashboard"


class TestUpdateDashboard:
    async def test_updates_dashboard(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(
            sample_dashboard,  # for access check
            {**sample_dashboard, "version": 2},  # updated result
        )
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.update_dashboard(1, {"name": "Updated", "version": 1})
        assert result is not None

    async def test_raises_409_on_version_conflict(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(
            sample_dashboard,  # for access check
            None,  # update fails
            sample_dashboard,  # get current
        )
        service = DashboardsService(mock_db, "test@test.com")
        with pytest.raises(HTTPException) as exc_info:
            await service.update_dashboard(1, {"name": "new", "version": 1})
        assert exc_info.value.status_code == 409


class TestDeleteDashboard:
    async def test_deletes_dashboard(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(sample_dashboard)
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.delete_dashboard(1)
        assert result["ok"] is True


class TestChartOperations:
    async def test_add_chart(self, mock_db: MockDBSession, sample_dashboard, sample_chart):
        mock_db.set_fetchrow_results(sample_dashboard, sample_chart)
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.add_chart(1, {"chart_type": "bar", "config": {}})
        assert result["id"] == 1

    async def test_update_chart(self, mock_db: MockDBSession, sample_dashboard, sample_chart):
        mock_db.set_fetchrow_results(
            sample_dashboard,  # access check
            sample_chart,  # existing chart
            {**sample_chart, "config": '{"updated": true}'},  # updated result
        )
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.update_chart(1, 1, {"config": {"updated": True}})
        assert result is not None

    async def test_delete_chart(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(sample_dashboard)
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.delete_chart(1, 1)
        assert result["ok"] is True

    async def test_reorder_charts(self, mock_db: MockDBSession, sample_dashboard):
        mock_db.set_fetchrow_results(sample_dashboard)
        service = DashboardsService(mock_db, "test@test.com")
        result = await service.reorder_charts(1, [3, 1, 2])
        assert result["ok"] is True
