from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import Any
import base64
from urllib.parse import urlparse
import re
from time import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession


@dataclass
class BackendAPIClient:
    backend_api_url: str | None = None
    _w: WorkspaceClient | None = None
    _token_expires_at: float = 0

    def _host(self) -> str:
        host = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl")
        return (host if host.startswith("https://") else f"https://{host}").rstrip("/")

    @property
    def app_name(self) -> str:
        if not self.backend_api_url:
            raise ValueError("backend_api_url is not set")
        host = (urlparse(self.backend_api_url).hostname or "").split(".")[0]
        return re.sub(r"-\d+$", "", host)  # strip trailing workspace id

    def _notebook_token(self) -> str:
        return (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .apiToken()
            .get()
        )

    @cached_property
    def app_client_id(self) -> str:
        w = WorkspaceClient(host=self._host(), token=self._notebook_token(), auth_type="pat")
        return w.apps.get(self.app_name).oauth2_app_client_id

    def get_workspace_client(self) -> WorkspaceClient:
        now = time()
        if self._w is None or now >= self._token_expires_at - 60:
            r = requests.post(
                f"{self._host()}/oidc/v1/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token": self._notebook_token(),
                    "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
                    "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                    "scope": "all-apis",
                    "audience": self.app_client_id,
                },
                timeout=30,
            )
            r.raise_for_status()
            token_data = r.json()
            self._w = WorkspaceClient(host=self._host(), token=r.json()["access_token"], auth_type="pat")
            self._token_expires_at = now + token_data.get("expires_in", 3600)
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
            case bytes() | bytearray():
                return base64.b64encode(val).decode("ascii")
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

    def api_call(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict[str, str] | None = None,
        allow_failure: bool = False,
    ) -> dict:
        """Call backend API with Databricks authentication. Reads backend_api_url from spark conf."""
        if self.backend_api_url is None:
            raise ValueError("backend_api_url is not set")

        url: str = f"{self.backend_api_url}{endpoint}"
        headers: dict[str, str] = self.get_workspace_client().config.authenticate()
        serialized_data: dict | None = self._serialize_data(data) if data else None
        use_json: bool = method.upper() != "GET" and serialized_data is not None
        try:
            response: requests.Response = requests.request(
                method,
                url,
                json=serialized_data if use_json else None,
                params=params,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if allow_failure:
                print(f"[WARNING] API call failed: {exc}")
                return {"error": str(exc)}
            else:
                raise
