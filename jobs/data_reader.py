import os
import re
import sys
from collections.abc import Callable
from typing import Any

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame, SparkSession

_jobs_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.path.abspath(".")
sys.path.insert(0, _jobs_dir)

from backend_api_client import BackendAPIClient
from jdbc_reader import JDBCReader
from teradata_columns import teradata_columns


def get_connection_info(system_name: str, backend_client: BackendAPIClient) -> dict:
    """Fetch system and prepare connection info"""
    system: dict = backend_client.api_call("GET", f"/api/systems/name/{system_name}")

    match system["kind"]:
        case "Databricks":
            return {"type": "catalog", "catalog": system.get("catalog"), "system": system}
        case "Teradata":
            # teradata always needs host/username/password because it uses the python library teradatasql to run `HELP COLUMN ...` command
            user: str | None = dbutils.secrets.get(system.get("secret_scope"), system["user_secret_key"]) if system.get("user_secret_key") else None
            password: str | None = dbutils.secrets.get(system.get("secret_scope"), system["pass_secret_key"]) if system.get("pass_secret_key") else None
            return {
                "type": "jdbc", "method": system.get("jdbc_method"), "system": system,
                "host": system.get("host"), "username": user, "password": password}
        case _:
            return {"type": "jdbc", "method": system.get("jdbc_method"), "system": system}


def get_type_transformations(
    source_system_id: int, target_system_id: int, backend_client: BackendAPIClient
) -> tuple[str, str]:
    """Fetch type transformation functions for a system pair. Empty strings mean no transformation."""
    data: dict = backend_client.api_call(
        "GET", f"/api/type-transformations/for-validation/{source_system_id}/{target_system_id}"
    )
    return data.get("system_a_function", ""), data.get("system_b_function", "")


def format_watermark_expr(expr: str, kind: str) -> str:
    """Rewrite backtick-quoted identifiers to the target system's native quoting. Also uppercase for Oracle/Netezza"""
    if kind in ("Databricks", "MySQL"):
        return expr

    def _requote(m: re.Match[str]) -> str:
        name = m.group(1)
        if kind in ("SQLServer", "Synapse"):
            return f"[{name.replace(']', ']]')}]"
        if kind in ("Oracle", "Netezza"):
            name = name.upper()
        return f'"{name.replace('"', '""')}"'

    return re.sub(r"`([^`]*)`", _requote, expr)


def get_column_types(conn: dict, table: str) -> list[tuple[str, str]]:
    """Get column names and types for a table"""
    spark: SparkSession = SparkSession.getActiveSession()
    tbl_parts: list[str] = table.split(".")
    catalog: str | None
    schema: str
    tbl: str
    if len(tbl_parts) == 2:
        schema, tbl = tbl_parts
        catalog = None
    elif len(tbl_parts) == 3:
        catalog, schema, tbl = tbl_parts
    else:
        raise ValueError("Table must have format 'catalog.schema.table' or 'schema.table' for type mapping.")

    query_columns: str
    match conn["system"]["kind"]:
        case "Databricks":
            table_schema = spark.read.table(f"{catalog}.{schema}.{tbl}").schema
            return [(col.name, str(col.dataType)) for col in table_schema.fields]
        case "Teradata":
            return teradata_columns(conn["host"], conn["username"], conn["password"], schema=schema, tbl=tbl)
        case "Oracle":
            query_columns = f"""
            SELECT column_name, data_type FROM all_tab_columns
            WHERE table_name = '{tbl.upper()}' AND owner = '{schema.upper()}'
            """
        case "Netezza" | "SQLServer" | "Synapse" | "MySQL" | "Postgres" | "Redshift" | "Snowflake":
            query_columns = f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE UPPER(table_name) = '{tbl.upper()}' AND UPPER(table_schema) = '{schema.upper()}'
            """
            if catalog:
                query_columns += f" AND UPPER(TABLE_CATALOG) = '{catalog.upper()}'"
        case _:
            raise ValueError(f"Unsupported system type: {conn['system']['kind']}")

    column_df: DataFrame = JDBCReader(conn).query(query_columns)
    return [(row[0], row[1]) for row in column_df.collect()]


def generate_read_query(
    conn: dict, table: str, type_mapping_func: str, column_overrides: dict[str, dict[str, str]] | None = None
) -> str:
    """Generate the query to read data with type transformations applied.
    column_overrides: {col_name_lower: {system_id_str: sql_expression}}
    """
    print(f"Mapping types for system: '{conn['system']['name']}' with type mapping function: \n{type_mapping_func or '(none)'}")

    if column_overrides:
        sid: str = str(conn["system"]["id"])
        applied: dict[str, str] = {col: by_sys[sid] for col, by_sys in column_overrides.items() if sid in by_sys}
        co: str = "\n  ".join(f"{col} -> {expr}" for col, expr in applied.items())
        print(f"Column overrides:\n  {co}")

    transform_columns: Callable[[str, str], str] | None = None
    if type_mapping_func and type_mapping_func.strip():
        namespace: dict[str, Any] = {}
        exec(type_mapping_func, namespace)
        transform_columns = namespace["transform_columns"]

    col_types: list[tuple[str, str]] = get_column_types(conn, table)
    col_types = [(n, t) for n, t in col_types if n and (n.strip() if isinstance(n, str) else True)]

    cast_columns: list[str] = []
    for name, data_type in col_types:
        override_expr: str | None = (column_overrides or {}).get(name.lower(), {}).get(str(conn["system"]["id"]))
        if override_expr:
            cast_columns.append(f"{override_expr} AS {name}")
        elif transform_columns:
            cast_columns.append(transform_columns(name, data_type))
        else:
            cast_columns.append(name)
    return f"SELECT {', '.join(cast_columns)} FROM {table}"


def read_count(
    conn: dict, table: str | None = None, query: str | None = None, watermark_expr: str | None = None
) -> int:
    """Get row count from system using pushed-down COUNT(*)"""
    spark: SparkSession = SparkSession.getActiveSession()
    is_databricks: bool = conn["system"]["kind"] == "Databricks"

    if watermark_expr:
        watermark_expr = format_watermark_expr(watermark_expr, conn["system"]["kind"])

    if query:
        if is_databricks:
            if conn["type"] == "catalog":
                spark.sql(f"USE CATALOG `{conn['catalog']}`;")
        count_query = f"SELECT COUNT(*) as cnt FROM ({query.replace(';', '')}) _subq"
    else:
        tbl = f"`{conn['catalog']}`.{table}" if is_databricks else table
        where = f" WHERE {watermark_expr}" if watermark_expr else ""
        count_query = f"SELECT COUNT(*) as cnt FROM {tbl}{where}"

    if conn["type"] == "jdbc":
        return JDBCReader(conn).query(count_query).collect()[0]["cnt"]
    return spark.sql(count_query).collect()[0]["cnt"]


def lowercase_cols(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def read_data(
    conn: dict,
    table: str | None = None,
    query: str | None = None,
    watermark_expr: str | None = None,
    type_mapping_func: str | None = None,
    column_overrides: dict[str, dict[str, str]] | None = None,
) -> DataFrame:
    """Read data from system (Databricks catalog or JDBC)"""
    spark: SparkSession = SparkSession.getActiveSession()
    is_databricks: bool = conn["system"]["kind"] == "Databricks"
    jdbc_reader: JDBCReader = JDBCReader(conn)

    if watermark_expr:
        watermark_expr = format_watermark_expr(watermark_expr, conn["system"]["kind"])

    df: DataFrame
    if query:
        if watermark_expr:
            print(f"Ignoring watermark expression for 'query' entity: {watermark_expr}")
        if is_databricks:
            if conn["type"] == "catalog":
                spark.sql(f"USE CATALOG `{conn['catalog']}`;")

        df = spark.sql(query) if is_databricks else jdbc_reader.query(query)
        return lowercase_cols(df)

    if is_databricks:
        table = f"`{conn['catalog']}`.{table}"
        # invalidate the disk cache, may fix some of the caching issues we see
        if not os.environ.get("IS_SERVERLESS"):
            try:
                spark.sql(f"REFRESH TABLE {table}")
                spark.sql(f"UNCACHE TABLE {table}")
            except Exception as e:
                print(f"Issue with REFRESH or UNCACHE: {e}")

    watermark_clause: str = f" WHERE {watermark_expr}" if watermark_expr else ""
    has_transforms: bool = bool(type_mapping_func and type_mapping_func.strip()) or bool(column_overrides)
    read_query: str = (
        generate_read_query(conn, table, type_mapping_func or "", column_overrides)
        if has_transforms
        else f"SELECT * FROM {table}"
    )
    read_query += watermark_clause

    if conn["type"] == "jdbc":
        jdbc_reader.detect_partition_info(table)
        df = jdbc_reader.query(read_query)
        return lowercase_cols(df)

    df = spark.sql(read_query)
    return lowercase_cols(df)
