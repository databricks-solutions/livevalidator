from __future__ import annotations

from dataclasses import dataclass
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession


@dataclass
class BackendAPIClient:
    backend_api_url: str | None = None
    _w: WorkspaceClient | None = None

    def get_workspace_client(self) -> WorkspaceClient:
        """Lazy initialization of WorkspaceClient singleton."""
        if self._w is None:
            spark = SparkSession.getActiveSession()
            self._w = WorkspaceClient(
                host=spark.conf.get("spark.databricks.workspaceUrl"),
                client_id=dbutils.secrets.get(scope="livevalidator", key="lv-app-id"),
                client_secret=dbutils.secrets.get(scope="livevalidator", key="lv-app-secret"),
            )
        return self._w

    def _serialize_value(self, val: Any) -> Any:
        """Convert non-JSON-serializable objects to serializable formats."""
        match val:
            case datetime() | date():
                return val.isoformat()
            case Decimal():
                return float(val)
            case _ if hasattr(val, "item"):  # numpy scalar
                return val.item()
            case _:
                return val

    def _serialize_data(self, data: Any) -> Any:
        """Recursively serialize nested dicts/lists for JSON."""
        match data:
            case dict():
                return {k: self._serialize_data(v) for k, v in data.items()}
            case list():
                return [self._serialize_data(item) for item in data]
            case _:
                return self._serialize_value(data)

    def api_call(self, method: str, endpoint: str, data: dict | None = None) -> dict:
        """Call backend API with Databricks authentication. Reads backend_api_url from spark conf."""
        if self.backend_api_url is None:
            raise ValueError("backend_api_url is not set")

        url: str = f"{self.backend_api_url}{endpoint}"
        headers: dict[str, str] = self.get_workspace_client().config.authenticate()
        serialized_data: dict | None = self._serialize_data(data) if data else None
        response: requests.Response = requests.request(
            method, url, json=serialized_data, headers=headers, timeout=30
        )
        response.raise_for_status()
        return response.json()
