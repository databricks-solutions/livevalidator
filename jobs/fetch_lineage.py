# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch Table Lineage
# MAGIC Fetches upstream (and optionally downstream) lineage for a table/view via Databricks Lineage API and posts result to the backend.

# COMMAND ----------

import json
import requests
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

# Widgets: table_name (catalog.schema.table), catalog_name, backend_api_url, entity_type, entity_id
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("backend_api_url", "")
dbutils.widgets.text("entity_type", "")
dbutils.widgets.text("entity_id", "")

table_name = dbutils.widgets.get("table_name").strip()
catalog_name = dbutils.widgets.get("catalog_name").strip()
backend_api_url = dbutils.widgets.get("backend_api_url").strip()
entity_type = dbutils.widgets.get("entity_type").strip()
entity_id = dbutils.widgets.get("entity_id").strip()

if not table_name or not catalog_name:
    raise ValueError("table_name and catalog_name are required")

# COMMAND ----------

# Get workspace URL and auth (same pattern as backend_api_client for calling Databricks APIs)
spark = SparkSession.getActiveSession()
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
if not workspace_url.startswith("http"):
    workspace_url = "https://" + workspace_url
databricks_host = workspace_url.rstrip("/")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient(
    host=databricks_host,
    client_id=dbutils.secrets.get(scope="livevalidator", key="lv-app-id"),
    client_secret=dbutils.secrets.get(scope="livevalidator", key="lv-app-secret"),
)
auth_headers = w.config.authenticate()

# COMMAND ----------

def get_table_lineage_response(view_name: str) -> requests.Response:
    url = f"{databricks_host}/api/2.0/lineage-tracking/table-lineage"
    payload = {"table_name": view_name, "include_entity_lineage": True}
    headers = {**auth_headers, "Content-Type": "application/json"}
    return requests.get(url, headers=headers, json=payload, timeout=30)

# COMMAND ----------

ALLOWED_TABLE_TYPES = {"TABLE", "VIEW", "PERSISTED_VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"}

def fetch_lineage(view_name: str, next_name: str, level: int, direction: str, visited: set) -> list:
    response = get_table_lineage_response(next_name)
    if response.status_code != 200:
        return []

    lineage_json = response.json()
    results = []
    entities = lineage_json.get(direction, [])

    for entity in entities:
        table_info_dict = dict(entity.get("tableInfo", {}))
        if not table_info_dict.get("name"):
            continue

        table_type = (table_info_dict.get("table_type") or "").upper()
        if table_type and table_type not in ALLOWED_TABLE_TYPES:
            continue

        if table_info_dict.get("catalog_name") != catalog_name:
            continue

        obj_name = table_info_dict.get("name")
        next_table = f"{table_info_dict['catalog_name']}.{table_info_dict['schema_name']}.{obj_name}"

        if next_table in visited:
            continue
        visited.add(next_table)

        table_info_dict["parent_name"] = view_name if level == 0 else next_name
        table_info_dict.pop("lineage_timestamp", None)
        table_info_dict["direction"] = direction
        table_info_dict["level"] = level
        table_info_dict["object_name"] = table_info_dict.pop("name", None)
        table_info_dict["view_name"] = view_name
        results.append(table_info_dict)

        results.extend(fetch_lineage(view_name, next_table, level + 1, direction, visited))

    return results

# COMMAND ----------

visited = {table_name}
table_info = []
for direction in ["upstreams"]:
    table_info.extend(fetch_lineage(table_name, table_name, 0, direction, visited))

# Normalize for JSON (ordered cols for display)
ordered_cols = ["view_name", "level", "parent_name", "catalog_name", "schema_name", "object_name", "object_type"]
lineage_payload = []
for row in table_info:
    mapped = {}
    for k in ordered_cols:
        if k == "object_type":
            raw_type = (row.get("table_type") or "").upper()
            mapped[k] = "VIEW" if raw_type == "PERSISTED_VIEW" else raw_type
        elif k in row:
            mapped[k] = row[k]
    lineage_payload.append(mapped)

# Fetch the entity's own object type from Unity Catalog
entity_object_type = None
try:
    uc_resp = requests.get(
        f"{databricks_host}/api/2.1/unity-catalog/tables/{table_name}",
        headers={**auth_headers, "Content-Type": "application/json"},
        timeout=15,
    )
    if uc_resp.status_code == 200:
        raw = (uc_resp.json().get("table_type") or "").upper()
        entity_object_type = "VIEW" if raw == "PERSISTED_VIEW" else raw
except Exception:
    pass

# COMMAND ----------

lineage_data = {"entity_object_type": entity_object_type, "items": lineage_payload}

if backend_api_url and entity_type and entity_id:
    from backend_api_client import BackendAPIClient
    client = BackendAPIClient(backend_api_url=backend_api_url)
    # Map entity_type to API path: "table" -> /api/tables, "query" -> /api/queries
    type_path = "tables" if entity_type == "table" else "queries"
    client.api_call("PATCH", f"/api/{type_path}/{entity_id}/lineage", {"lineage": lineage_data})
    print(f"Posted lineage ({len(lineage_payload)} nodes, entity_type={entity_object_type}) to {entity_type} {entity_id}")
else:
    print(f"Lineage ({len(lineage_payload)} nodes, entity_type={entity_object_type}):")
    for r in lineage_payload:
        print(r)
