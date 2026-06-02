# Databricks notebook source
# MAGIC %md
# MAGIC # LiveValidator - Validation Workflow
# MAGIC Validates schema, row counts, and row-level differences between source and target systems.

# COMMAND ----------

import json
import os
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime

from databricks.sdk.runtime import dbutils
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, xxhash64, countDistinct
from pyspark.sql.types import DecimalType, NullType, StringType

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, "/Workspace" + os.path.dirname(_nb_path))

from backend_api_client import BackendAPIClient
from data_reader import get_connection_info, get_type_transformations, read_count, read_data
from exceptall_analysis import run_except_all_count_analysis
from pk_analysis import null_safe_join, run_pk_analysis, run_pk_count_analysis
from transformation_options import downgrade_unicode

# COMMAND ----------

# Define parameters
dbutils.widgets.text("trigger_id", "")
dbutils.widgets.text("name", "")
dbutils.widgets.text("source_system_name", "")
dbutils.widgets.text("target_system_name", "")
dbutils.widgets.text("backend_api_url", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("source_sql", "")
dbutils.widgets.text("target_sql", "")
dbutils.widgets.text("watermark_expr", "")
dbutils.widgets.text("compare_mode", "except_all")
dbutils.widgets.text("pk_columns", "")
dbutils.widgets.text("include_columns", "[]")
dbutils.widgets.text("exclude_columns", "[]")
dbutils.widgets.text("persist_catalog", "live_validator_data")
dbutils.widgets.text("options", "")
dbutils.widgets.text("config", "{}")

# COMMAND ----------

# Parse parameters
trigger_id: str | None = dbutils.widgets.get("trigger_id") or None
name: str = dbutils.widgets.get("name")
source_system_name: str = dbutils.widgets.get("source_system_name")
target_system_name: str = dbutils.widgets.get("target_system_name")
backend_api_url: str = dbutils.widgets.get("backend_api_url")
source_table: str | None = dbutils.widgets.get("source_table") or None
target_table: str | None = dbutils.widgets.get("target_table") or None
source_sql: str | None = dbutils.widgets.get("source_sql") or None
target_sql_raw: str | None = dbutils.widgets.get("target_sql") or None
target_sql: str | None = target_sql_raw.strip() if (target_sql_raw and target_sql_raw.strip()) else source_sql
watermark_expr: str | None = dbutils.widgets.get("watermark_expr") or None
compare_mode: str = dbutils.widgets.get("compare_mode")
pk_columns: list[str] = json.loads(dbutils.widgets.get("pk_columns") or "[]")
include_columns: list[str] = json.loads(dbutils.widgets.get("include_columns") or "[]")
exclude_columns: list[str] = json.loads(dbutils.widgets.get("exclude_columns") or "[]")
persist_catalog: str | None = dbutils.widgets.get("persist_catalog") or None
options: dict | str = json.loads(dbutils.widgets.get("options") or "{}")
column_overrides: dict[str, dict[str, str]] | None = options.get("column_overrides") or None

# sanitize column names
pk_columns = [c.lower() for c in pk_columns if c]
include_columns = [c.lower() for c in include_columns if c]
exclude_columns = [c.lower() for c in exclude_columns if c]

# Parse unified config
config: dict = json.loads(dbutils.widgets.get("config") or "{}")
skip_row_validation: bool = config.get("skip_row_validation", False)
max_sample_rows: int = config.get("max_sample_rows", 10)
row_count_tolerance: float = config.get("row_count_tolerance", 0)
row_value_tolerance: float = config.get("row_value_tolerance", 0)
downgrade_unicode_enabled: bool = config.get("downgrade_unicode", False)
replace_special_char: list[str] = config.get("replace_special_char", [])
extra_replace_regex: str = config.get("extra_replace_regex", "")

# Set up client for the backend REST API calls
client = BackendAPIClient(backend_api_url=backend_api_url)

print(f"Starting: {name} (trigger_id={trigger_id or 'manual'})")

# COMMAND ----------
# DBTITLE 1,Schema and Count Validation

def validate_schema(src_df: DataFrame, tgt_df: DataFrame, exclude: list[str]) -> dict:
    """Compare column names between source and target"""
    src_cols: set[str] = set(c for c in src_df.columns if c not in exclude)
    tgt_cols: set[str] = set(c for c in tgt_df.columns if c not in exclude)
    
    match: bool = src_cols == tgt_cols
    print(f"\tSchema {'matches' if match else 'does not match'}, source: {len(src_cols)}, target: {len(tgt_cols)} columns")
    
    return {
        "schema_match": match,
        "schema_details": {
            "columns_matched": list(src_cols & tgt_cols),
            "columns_missing": list(src_cols - tgt_cols),
            "columns_extra": list(tgt_cols - src_cols)
        }
    }

def validate_counts(
    src_conn: dict, tgt_conn: dict,
    src_table: str | None, tgt_table: str | None,
    src_query: str | None, tgt_query: str | None, watermark: str | None,
    row_count_tolerance: float,
) -> dict[str, int | bool]:
    """Compare row counts using pushed-down COUNT(*)"""
    src_count: int = read_count(src_conn, src_table, src_query, watermark)
    tgt_count: int = read_count(tgt_conn, tgt_table, tgt_query, watermark)
    match: bool = (
        abs(src_count - tgt_count) / src_count <= row_count_tolerance / 100 
        if (row_count_tolerance and src_count)
        else src_count == tgt_count
        )
    
    print(f"\tRow counts {'match' if match else 'do not match'}: source={src_count}, target={tgt_count}")
    if src_count != tgt_count and match:
        print(f"\tWithin tolerance: {row_count_tolerance}%, was {abs(src_count - tgt_count) / src_count * 100:.5f}%")
    
    return {
        "rows_compared": src_count,
        "row_count_source": src_count,
        "row_count_target": tgt_count,
        "row_count_match": match
    }

def exclude_cols(df: DataFrame, exclude: list[str]) -> DataFrame:
    """Exclude columns from the dataframe"""
    return df.select([c for c in df.columns if c not in exclude])

# COMMAND ----------
# DBTITLE 1,Row-Level Validation Functions

def persist_obj(obj: DataFrame, name: str, suffix: str, catalog: str = persist_catalog) -> DataFrame:
    """Cache a DataFrame in the LiveValidator data catalog"""

    if not os.environ.get("IS_SERVERLESS"):
        return obj.persist(StorageLevel.MEMORY_AND_DISK)

    spark: SparkSession = SparkSession.getActiveSession()
    sanitized_name: str = name.replace('.', '__').replace(' ', '').replace('-', '_')
    persisted_name: str = f"{catalog}.entities.`{sanitized_name}__{suffix}`"
    configs: dict[str, str] = {
        "overwriteSchema": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.feature.timestampNtz": "supported"
        }
    # Coerce void (NullType) columns so Parquet has physical backing for Photon
    void_cols = {f.name for f in obj.schema.fields if isinstance(f.dataType, NullType)}
    if void_cols:
        obj = obj.select(*[col(c).cast(StringType()) if c in void_cols else col(c) for c in obj.columns])
    obj.write.format("delta").mode("overwrite").options(**configs).saveAsTable(persisted_name)
    return spark.read.table(persisted_name)

def conform_types(src_df: DataFrame, tgt_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Align column order and resolve type mismatches (decimal precision → min of both sides). If types mismatch, cast to the source system's types."""
    tgt_df = tgt_df.select(src_df.columns)
    src_types = {f.name: f.dataType for f in src_df.schema.fields}
    casts = {
        f.name: DecimalType(min(src_types[f.name].precision, f.dataType.precision), min(src_types[f.name].scale, f.dataType.scale))
        if isinstance(src_types.get(f.name), DecimalType) and isinstance(f.dataType, DecimalType) else src_types[f.name]
        for f in tgt_df.schema.fields if f.name in src_types and src_types[f.name] != f.dataType
    }
    apply = lambda df: df.select(*[col(c).cast(casts[c]) if c in casts else col(c) for c in df.columns])
    return (apply(src_df), apply(tgt_df)) if casts else (src_df, tgt_df)

def run_except_all(src_df: DataFrame, tgt_df: DataFrame) -> DataFrame:
    """Find rows in source not in target using EXCEPT ALL"""
    return src_df.exceptAll(tgt_df)

def run_pk_compare(src_df: DataFrame, tgt_df: DataFrame, pk: list[str], how: str) -> DataFrame:
    """Find rows with PK matches but different values using hash comparison"""
    def rowhash_exact(df: DataFrame) -> DataFrame:
        cols: list = [col(c) for c in df.columns]
        return df.withColumn("__hash__", xxhash64(*cols))

    src_hash: DataFrame = rowhash_exact(src_df)
    tgt_hash: DataFrame = rowhash_exact(tgt_df)

    joined: DataFrame = null_safe_join(src_hash, tgt_hash, pk, how).select(
        *pk,
        src_hash["__hash__"].alias("h_lhs"),
        tgt_hash["__hash__"].alias("h_rhs")
    )

    return joined.filter(
        (col("h_lhs") != col("h_rhs")) | col("h_lhs").isNull() | col("h_rhs").isNull()
    ).drop("h_lhs", "h_rhs", "__hash__")

def validate_rows(src_df: DataFrame, tgt_df: DataFrame, mode: str, row_count_match: bool, max_sample_rows: int) -> dict:
    """Row-level validation - returns diff count and samples"""

    src_df, tgt_df = conform_types(src_df, tgt_df)
    how: str = "leftouter" if row_count_match else "inner"
    comparison_func: Callable = run_except_all if mode == "except_all" else lambda s, t: run_pk_compare(s, t, pk_columns, how)    
    diff_df: DataFrame = comparison_func(src_df, tgt_df)
    diff_count: int = diff_df.count()
    
    if diff_count == 0:
        return {"rows_different": 0, "sample_differences": []}

    # Try unicode downgrade if enabled
    if downgrade_unicode_enabled:
        print(f"Found {diff_count} mismatches, retrying with unicode downgraded...")
        src_df = downgrade_unicode(src_df, replace_special_char, extra_replace_regex)
        tgt_df = downgrade_unicode(tgt_df, replace_special_char, extra_replace_regex)

        # persist source and target data to avoid recomputation
        src_df = persist_obj(src_df, name, "src")
        tgt_df = persist_obj(tgt_df, name, "tgt")

        # compare and persist
        diff_df = comparison_func(src_df, tgt_df)
        diff_df = persist_obj(diff_df, name, "diff")
        diff_count = diff_df.count()
        
        if diff_count == 0:
            return {"rows_different": 0, "sample_differences": []}
    
    print(f"Found {diff_count} differences, extracting sample")
    sample_df: DataFrame = diff_df.limit(max_sample_rows)
    sample_df = persist_obj(sample_df, name, "samples")
    
    sample_dicts: list[dict] = [row.asDict() for row in sample_df.collect()]
    
    return {
        "rows_different": diff_count,
        "sample_differences": sample_dicts,
        "src_df": src_df,
        "tgt_df": tgt_df,
        "sample_df": sample_df,
        "diff_df": diff_df
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Validation Logic

# COMMAND ----------

try:
    # Initialize result with metadata
    result: dict = {
        "trigger_id": int(trigger_id) if trigger_id else None,
        "entity_type": "table" if source_table else "compare_query",
        "entity_name": name,
        "source": "manual" if not trigger_id else "schedule",
        "requested_by": "system",
        "started_at": datetime.now(UTC).isoformat(),
        "status": "succeeded",
        "source_table": source_table,
        "target_table": target_table,
        "src_sql_query": source_sql,
        "tgt_sql_query": target_sql_raw.strip() if target_sql_raw and target_sql_raw.strip() else None,
        "compare_mode": compare_mode,
        "pk_columns": pk_columns,
        "exclude_columns": exclude_columns
    }

    # Validate parameters
    if compare_mode not in ["except_all", "primary_key"]:
        raise ValueError(f"Unsupported compare_mode: {compare_mode}. Must be either 'except_all' or 'primary_key'")
    
    # Step 1: Connect to systems
    src_conn: dict = get_connection_info(source_system_name, client)
    tgt_conn: dict = get_connection_info(target_system_name, client)
    
    result["source_system_id"] = src_conn["system"]["id"]
    result["target_system_id"] = tgt_conn["system"]["id"]
    result["source_system_name"] = source_system_name
    result["target_system_name"] = target_system_name
    
    # Step 2: Read data with type transformations
    print("Reading data...")
    src_xform_func, tgt_xform_func = get_type_transformations(src_conn["system"]["id"], tgt_conn["system"]["id"], client)
    src_df: DataFrame = read_data(
        src_conn, table=source_table, query=source_sql, watermark_expr=watermark_expr,
        type_mapping_func=src_xform_func, column_overrides=column_overrides,
    )
    tgt_df: DataFrame = read_data(
        tgt_conn, table=target_table, query=target_sql, watermark_expr=watermark_expr,
        type_mapping_func=tgt_xform_func, column_overrides=column_overrides,
    )
    
    # Step 3: Validate schema
    print("Validating schema...")
    schema_result: dict = validate_schema(src_df, tgt_df, exclude_columns)
    result.update(schema_result)
    
    # Step 4: Validate counts
    print("Validating counts...")
    count_result: dict[str, int | bool] = validate_counts(
        src_conn,
        tgt_conn,
        source_table,
        target_table,
        source_sql,
        target_sql,
        watermark_expr,
        row_count_tolerance,
    )
    result.update(count_result)

    # Step 5: Apply max_rows limit if configured
    result["source_was_limited"] = False
    if src_conn["system"]["max_rows"] and count_result["rows_compared"] > src_conn["system"]["max_rows"]:
        print(f"Limiting source system {source_system_name} for row value check...")
        src_df = src_df.limit(src_conn["system"]["max_rows"])
        result["rows_compared"] = src_conn["system"]["max_rows"]
        result["source_was_limited"] = True
    if tgt_conn["system"]["max_rows"]:
        print(f"Ignoring target system max row limit of '{tgt_conn['system']['max_rows']}', can only be applied to source system...")
    
    src_df = exclude_cols(src_df, exclude_columns)
    tgt_df = exclude_cols(tgt_df, exclude_columns)
    
    # persist to avoid re-pulling data which could have changed
    src_df = persist_obj(src_df, name, "src")
    tgt_df = persist_obj(tgt_df, name, "tgt")

    # Step 6: Row-level validation (only if counts match and not skipped)
    if (count_result["row_count_match"] or compare_mode == "primary_key") and not skip_row_validation:
        # Validate PK columns exist and are unique for primary_key mode
        if compare_mode == "primary_key":
            if not set(pk_columns).issubset(set(src_df.columns)):
                raise ValueError(f"PK columns {pk_columns} not found in source columns {list(src_df.columns)}")

            # check the backend to see if this PK is vetted
            entity_type: str = "table" if source_table else "compare_query"
            status: dict = client.api_call(
                "GET", "/api/pk-vetted", params={"entity_type": entity_type, "entity_name": name}, allow_failure=True
            )
            if not status.get("pk_vetted"):
                print("Vetting the primary key...")
                duplicate_pk: list[Row] = tgt_df.groupBy(*pk_columns).count().filter(col("count") > 1).limit(1).collect()
                if duplicate_pk:
                    raise ValueError(f"PK not unique: {duplicate_pk[0].asDict()}")
                print("\tSuccess, updating backend...")
                client.api_call("POST", "/api/pk-vetted/vet", {"entity_type": entity_type, "entity_name": name}, allow_failure=True)
        
        print(f"Validating rows using {compare_mode}...")
        row_result: dict = validate_rows(src_df, tgt_df, compare_mode, count_result["row_count_match"], max_sample_rows)
        result.update(row_result)
        result["rows_matched"] = max(result["rows_compared"] - result["rows_different"], 0)
        perc_diff = abs(result["rows_compared"] - result["rows_matched"]) / result["rows_compared"] * 100 if result["rows_compared"] else 0
        result["rows_success"] = perc_diff <= row_value_tolerance if row_value_tolerance else result["rows_different"] == 0
        if not result["rows_success"]:
            print(f"\tPercent difference: {perc_diff:.5f}%")
    else:
        result.update({"rows_compared": None, "rows_matched": None, "rows_different": None, "rows_success": None, "src_df": src_df, "tgt_df": tgt_df, "sample_df": None, "diff_df": None})

    # Step 7: Determine final status
    if (result["rows_success"] and result["row_count_match"]) or (skip_row_validation and result["row_count_match"]):
        print("[SUCCESS] Validation passed")
    else:
        rows_diff: int | None = result.get("rows_different")
        print(f"[FAILURE] Schema: {result['schema_match']}, Count: {result['row_count_match']}, Diffs: {rows_diff if rows_diff is not None else 'N/A'}")
        result["status"] = "failed"

    result["finished_at"] = datetime.now(UTC).isoformat()

except Exception as e:
    print(f"[ERROR] Unexpected failure: {traceback.format_exc()}")
    result.update({
        "status": "error",
        "error_message": str(e),
        "error_details": {"type": type(e).__name__},
        "rows_compared": None,
        "rows_matched": None,
        "rows_different": None
    })
    
    if trigger_id:
        client.api_call("PUT", f"/api/triggers/{trigger_id}/fail", {
            "status": result["status"],
            "error_message": str(e),
            "error_details": {"type": type(e).__name__}
        })
    raise Exception(result["error_message"])

# COMMAND ----------

# Report results to API
print("Reporting results...")

serde_result: dict = result.copy()
if serde_result.get('src_df'):
    src_df = serde_result.pop('src_df')
    tgt_df = serde_result.pop('tgt_df')
    sample_df = serde_result.pop('sample_df')
    diff_df = serde_result.pop('diff_df')

history_response: dict = client.api_call("POST", "/api/validation-history", serde_result)

if result["status"] == "succeeded":
    dbutils.notebook.exit("Validation passed")

history_id: int | None = history_response.get("id") if history_response else None
if not history_id:
    dbutils.notebook.exit("Finished")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Except All Post Analysis

# COMMAND ----------

# Compute unique row counts on count mismatch
unique_counts: dict = {}
if not result["row_count_match"]:
    print("Computing unique row counts...")
    unique_src = src_df.select(countDistinct(xxhash64(*[col(c) for c in src_df.columns]))).collect()[0][0]
    unique_tgt = tgt_df.select(countDistinct(xxhash64(*[col(c) for c in tgt_df.columns]))).collect()[0][0]
    unique_counts = {"unique_count_source": unique_src, "unique_count_target": unique_tgt}
    print(f"Unique rows: source={unique_src}, target={unique_tgt}")

if compare_mode == "except_all" and history_id:
    print("Running except_all analysis...")
    except_all_analysis = run_except_all_count_analysis(result)
    if except_all_analysis:
        except_all_analysis.update(unique_counts)
        client.api_call("PATCH", f"/api/validation-history/{history_id}", {"sample_differences": except_all_analysis})
        print(f"Updated validation history {history_id} with except_all analysis")
    reason = "Row count mismatch" if not result["row_count_match"] else "Row value mismatch"
    if result["row_count_match"]:
        sample_df.display()
    dbutils.notebook.exit(f"Validation failed - {reason}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## PK Post Analysis

# COMMAND ----------
# Display sample PKs that mismatched
if result.get("sample_df"):
    result["sample_df"].display()

# COMMAND ----------
# Update validation history with PK analysis
pk_sample_differences: dict | None = run_pk_analysis(result)
if pk_sample_differences:
    client.api_call("PATCH", f"/api/validation-history/{history_id}", {"sample_differences": pk_sample_differences})
    print(f"Updated validation history {history_id} with PK analysis ({len(pk_sample_differences['samples'])} samples)")

# COMMAND ----------
if pk_sample_differences:
    dbutils.notebook.exit("Validation failed, pk analysis completed.")

# COMMAND ----------
# No diffs found, proceeding with row count mismatch analysis
if not result["row_count_match"]:
    pk_count_analysis = run_pk_count_analysis(result)
    if pk_count_analysis and history_id:
        pk_count_analysis.update(unique_counts)
        client.api_call("PATCH", f"/api/validation-history/{history_id}", {"sample_differences": pk_count_analysis})
        if not pk_count_analysis.get("skipped"):
            print(f"Updated validation history {history_id} with PK count analysis")
