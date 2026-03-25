# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch Table Lineage
# MAGIC
# MAGIC Given a fully-qualified table/view name (catalog.schema.table), this notebook:
# MAGIC 1. Calls the Databricks Lineage API to discover the **immediate upstream** tables/views.
# MAGIC 2. For each discovered upstream, **recursively** calls the same API to discover *its* upstreams,
# MAGIC    building a full dependency tree (depth-first).
# MAGIC 3. Flattens the tree into a list of nodes, each annotated with its depth level and parent.
# MAGIC 4. Posts the result to the LiveValidator backend so the UI can display the lineage graph.
# MAGIC
# MAGIC Lineage is scoped to the specified catalog only; cross-catalog dependencies are excluded.

# COMMAND ----------

import json
import requests
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, "/Workspace" + os.path.dirname(_nb_path))

# --- Input parameters (passed as Databricks job widgets) ---
# table_name:       the root table to fetch lineage for, e.g. "prod.finance.revenue_summary"
# catalog_name:     only keep upstream nodes within this catalog (filter out cross-catalog refs)
# backend_api_url:  LiveValidator backend URL to POST results to (optional; prints if empty)
# entity_type/id:   identifies the LiveValidator entity (table or query) this lineage belongs to
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

# --- Authenticate with the Databricks workspace ---
# Uses the service principal stored in the "livevalidator" secret scope to generate
# OAuth headers for calling Databricks REST APIs (Lineage + Unity Catalog).
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
    """Single API call: ask Databricks for the immediate upstream/downstream neighbors of one table."""
    url = f"{databricks_host}/api/2.0/lineage-tracking/table-lineage"
    # include_entity_lineage=True asks the API to also return the notebooks/jobs/pipelines
    # that connect the tables (not used by this script today, but ensures complete results).
    payload = {"table_name": view_name, "include_entity_lineage": True}
    headers = {**auth_headers, "Content-Type": "application/json"}
    return requests.get(url, headers=headers, json=payload, timeout=30)

# COMMAND ----------

ALLOWED_TABLE_TYPES = {"TABLE", "VIEW", "PERSISTED_VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"}

def fetch_lineage(view_name: str, next_name: str, level: int, direction: str, visited: set) -> list:
    """
    Recursively walk the lineage graph starting from `next_name`.

    Algorithm (depth-first traversal):
      1. Call the Lineage API for `next_name` to get its immediate neighbors in `direction`.
      2. For each neighbor returned:
         a. Skip if it's not a TABLE/VIEW type, outside our catalog, or already visited.
         b. Record it as a lineage node (with level, parent, direction metadata).
         c. Recurse into that neighbor at level+1 to discover *its* upstreams.
      3. The `visited` set prevents cycles (e.g. A -> B -> A).

    Args:
        view_name:  the original root table (stays constant across recursion; used for labeling)
        next_name:  the table we're currently expanding (changes each recursive call)
        level:      depth from the root (0 = direct upstream of root, 1 = upstream of upstream, ...)
        direction:  "upstreams" or "downstreams" — which neighbors to follow
        visited:    shared set of fully-qualified table names already seen (prevents cycles)

    Returns:
        Flat list of all discovered lineage nodes (depth-first order).
    """
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

        # Filter: only keep standard table/view types
        table_type = (table_info_dict.get("table_type") or "").upper()
        if table_type and table_type not in ALLOWED_TABLE_TYPES:
            continue

        # Filter: only keep tables within the target catalog
        if table_info_dict.get("catalog_name") != catalog_name:
            continue

        obj_name = table_info_dict.get("name")
        next_table = f"{table_info_dict['catalog_name']}.{table_info_dict['schema_name']}.{obj_name}"

        # Cycle detection: skip if we've already visited this table
        if next_table in visited:
            continue
        visited.add(next_table)

        # Annotate the node with traversal metadata before adding to results
        table_info_dict["parent_name"] = view_name if level == 0 else next_name
        table_info_dict.pop("lineage_timestamp", None)
        table_info_dict["direction"] = direction
        table_info_dict["level"] = level
        table_info_dict["object_name"] = table_info_dict.pop("name", None)
        table_info_dict["view_name"] = view_name
        results.append(table_info_dict)

        # Recurse: discover this node's own upstreams (one level deeper)
        results.extend(fetch_lineage(view_name, next_table, level + 1, direction, visited))

    return results

# COMMAND ----------

# --- Kick off the recursive traversal ---
# Start from the root table and walk upstream. The visited set is seeded with the root
# itself so we never "discover" it as its own dependency.
#
# Example: if table_name = "prod.finance.revenue_summary", the traversal might produce:
#   level 0: prod.finance.daily_totals        (direct upstream of revenue_summary)
#   level 0: prod.finance.exchange_rates       (direct upstream of revenue_summary)
#   level 1: prod.raw.transactions             (upstream of daily_totals)
#   level 1: prod.raw.fx_feed                  (upstream of exchange_rates)
#   level 2: ...                               (and so on, until no more upstreams)
visited = {table_name}
table_info = []
for direction in ["upstreams"]:
    table_info.extend(fetch_lineage(table_name, table_name, 0, direction, visited))

# --- Normalize into a clean JSON payload ---
# Pick only the columns the frontend/backend care about, in a consistent order.
# Also normalize "PERSISTED_VIEW" -> "VIEW" so the UI doesn't have to handle both.
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

# --- Look up the root table's own type (TABLE vs VIEW) via Unity Catalog ---
# The lineage API only tells us about *other* tables; we separately fetch metadata
# for the root entity so the UI can display its type (e.g. show a VIEW icon).
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

# --- Post results to the LiveValidator backend (or print for local debugging) ---
lineage_data = {"entity_object_type": entity_object_type, "items": lineage_payload}

if backend_api_url and entity_type and entity_id:
    from backend_api_client import BackendAPIClient
    client = BackendAPIClient(backend_api_url=backend_api_url)
    type_path = "tables" if entity_type == "table" else "queries"
    client.api_call("PATCH", f"/api/{type_path}/{entity_id}/lineage", {"lineage": lineage_data})
    print(f"Posted lineage ({len(lineage_payload)} nodes, entity_type={entity_object_type}) to {entity_type} {entity_id}")
else:
    # No backend configured — just print the lineage tree (useful for interactive runs)
    print(f"Lineage ({len(lineage_payload)} nodes, entity_type={entity_object_type}):")
    for r in lineage_payload:
        print(r)
