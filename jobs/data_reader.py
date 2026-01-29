from collections.abc import Callable
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from databricks.sdk.runtime import dbutils

import sys
import os
sys.path.append(os.path.abspath('.'))
from backend_api_client import BackendAPIClient


def get_connection_info(system_name: str, backend_client: BackendAPIClient) -> dict:
    """Fetch system and prepare connection info"""
    system: dict = backend_client.api_call("GET", f"/api/systems/name/{system_name}")
    
    if system["kind"] == "Databricks":
        return {"type": "catalog", "catalog": system.get("catalog"), "system": system}
    
    jdbc_str: str
    if system["jdbc_string"]:
        jdbc_str = system["jdbc_string"]
    else:
        match system["kind"]:
            case "Teradata":
                jdbc_str = f"jdbc:teradata://{system['host']}"
            case "Oracle":
                jdbc_str = f"jdbc:oracle:thin:@//{system['host']}:{system['port']}/{system['database']}"
            case "SQLServer":
                jdbc_str = f"jdbc:sqlserver://{system['host']}:{system['port']};databaseName={system['database']};encrypt=true;trustServerCertificate=true"
            case _:
                jdbc_str = f"jdbc:{system['kind'].lower()}://{system['host']}:{system['port']}/{system['database']}"
        print(f"Generated {system['kind']} JDBC string: {jdbc_str}")

    scope: str = system.get("secret_scope") or "livevalidator"
    return {
        "type": "jdbc",
        "jdbc_string": jdbc_str,
        "username": dbutils.secrets.get(scope, system["user_secret_key"]) if system.get("user_secret_key") else None,
        "password": dbutils.secrets.get(scope, system["pass_secret_key"]) if system.get("pass_secret_key") else None,
        "system": system
    }

def get_type_transformations(source_system_id: int, target_system_id: int, backend_client: BackendAPIClient) -> tuple[str, str]:
    """Fetch type transformation functions for a system pair. Empty strings mean no transformation."""
    data: dict = backend_client.api_call("GET", f"/api/type-transformations/for-validation/{source_system_id}/{target_system_id}")
    return data.get('system_a_function', ''), data.get('system_b_function', '')

def query_jdbc(conn_info: dict, query: str) -> DataFrame:
    """Execute a JDBC query and return DataFrame"""
    spark: SparkSession = SparkSession.getActiveSession()
    driver: str | None = conn_info["system"].get("driver_connector")
    if not driver:
        raise ValueError(f"JDBC driver not set for system: {conn_info['system']['name']}")
    
    return spark.read.format("jdbc") \
        .option("url", conn_info["jdbc_string"]).option("driver", driver).option("query", query) \
        .option("user", conn_info.get("username")).option("password", conn_info.get("password")).load()

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
        raise ValueError(f"Table must have format 'catalog.schema.table' or 'schema.table' for type mapping.")

    query_columns: str
    match conn["system"]["kind"]:
        case "Databricks":
            table_schema = spark.read.table(f"{catalog}.{schema}.{tbl}").schema
            return [(col.name, str(col.dataType)) for col in table_schema.fields]
        case "Teradata":
            query_columns = f"HELP COLUMN {schema.upper()}.{tbl.upper()}.*"
        case "Oracle":
            query_columns = f"""
            SELECT column_name, data_type FROM all_tab_columns
            WHERE table_name = '{tbl.upper()}' AND owner = '{schema.upper()}'
            """
        case "Netezza" | "SQLServer" | "MySQL" | "Postgres" | "Snowflake":
            query_columns = f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE UPPER(table_name) = '{tbl.upper()}' AND UPPER(table_schema) = '{schema.upper()}'
            """
            if catalog:
                query_columns += f" AND UPPER(TABLE_CATALOG) = '{catalog.upper()}'"
        case _:
            raise ValueError(f"Unsupported system type: {conn['system']['kind']}")

    column_df: DataFrame = query_jdbc(conn, query_columns)
    return [(row[0], row[1]) for row in column_df.collect()]

def generate_read_query(conn: dict, table: str, type_mapping_func: str) -> str:
    """Generate the query to read data with type transformations applied"""
    print(f"Mapping types for system: '{conn['system']['name']}' with type mapping function: \n{type_mapping_func}")
    namespace: dict[str, Any] = {}
    exec(type_mapping_func, namespace)
    transform_columns: Callable[[str, str], str] = namespace['transform_columns']

    col_types: list[tuple[str, str]] = get_column_types(conn, table)
    cast_columns: list[str] = [transform_columns(name, data_type) for name, data_type in col_types] if col_types else ["*"]
    return f"SELECT {', '.join(cast_columns)} FROM {table}"

def read_count(
    conn: dict,
    table: str | None = None,
    query: str | None = None,
    watermark_expr: str | None = None
) -> int:
    """Get row count from system using pushed-down COUNT(*)"""
    spark: SparkSession = SparkSession.getActiveSession()
    is_databricks: bool = conn["system"]["kind"] == "Databricks"

    if query:
        count_query = f"SELECT COUNT(*) as cnt FROM ({query.replace(';','')}) _subq"
    else:
        tbl = f"`{conn['catalog']}`.{table}" if is_databricks else table
        where = f" WHERE {watermark_expr}" if watermark_expr else ""
        count_query = f"SELECT COUNT(*) as cnt FROM {tbl}{where}"

    if conn["type"] == "jdbc":
        return query_jdbc(conn, count_query).collect()[0]["cnt"]
    return spark.sql(count_query).collect()[0]["cnt"]


def read_data(
    conn: dict, 
    table: str | None = None, 
    query: str | None = None, 
    watermark_expr: str | None = None, 
    type_mapping_func: str | None = None
) -> DataFrame:
    """Read data from system (Databricks catalog or JDBC)"""
    spark: SparkSession = SparkSession.getActiveSession()
    is_databricks: bool = conn["system"]["kind"] == "Databricks"

    if query:
        if watermark_expr:
            print(f"Ignoring watermark expression for 'query' entity: {watermark_expr}")
        if is_databricks:
            if conn["type"] == "catalog":
                spark.sql(f"USE CATALOG `{conn['catalog']}`;")
            return spark.sql(query)
        else:
            return query_jdbc(conn, query)
    
    # Table mode - may need type mapping
    if is_databricks:
        table = f"`{conn['catalog']}`.{table}"

    watermark_clause: str = f" WHERE {watermark_expr}" if watermark_expr else ""
    read_query: str = generate_read_query(conn, table, type_mapping_func) if type_mapping_func and type_mapping_func.strip() else f"SELECT * FROM {table}"
    read_query += watermark_clause

    if conn["type"] == "jdbc":
        return query_jdbc(conn, read_query)
    
    return spark.sql(read_query)
