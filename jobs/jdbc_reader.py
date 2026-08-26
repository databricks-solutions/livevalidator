import json
import os
import sys
from dataclasses import dataclass

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame, DataFrameReader, SparkSession

_jobs_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.path.abspath(".")
sys.path.insert(0, _jobs_dir)

@dataclass
class PartitionInfo:
    MIN_PARTITIONS = 4
    MAX_PARTITIONS = 12
    PARTITION_RANGE_DIVISOR = 1_000_000

    column: str
    lower: int
    upper: int

    @property
    def num_partitions(self) -> int:
        return min(self.MAX_PARTITIONS, max(self.MIN_PARTITIONS, (self.upper - self.lower) // self.PARTITION_RANGE_DIVISOR))


class JDBCReader:
    def __init__(self, conn_info: dict):
        self.conn = conn_info
        self.spark: SparkSession = SparkSession.getActiveSession()
        self.partition_info: PartitionInfo | None = None


    def _add_extra_options(self, reader: DataFrameReader) -> DataFrameReader:
        options = self.conn["system"].get("options") or {}
        if isinstance(options, str):
            options = json.loads(options)
        for key, val in options.get("jdbc", {}).items():
            reader = reader.option(key, val)
        return reader

    @property
    def _direct_reader(self) -> DataFrameReader:
        system: dict = self.conn["system"]

        if not system.get("driver_connector"):
            raise ValueError(f"JDBC driver not set for system: {system['name']}")

        jdbc_str: str = system["jdbc_string"]
        if not jdbc_str:
            match system["kind"]:
                case "Teradata":
                    jdbc_str = f"jdbc:teradata://{system['host']}"
                case "Oracle":
                    jdbc_str = f"jdbc:oracle:thin:@//{system['host']}:{system['port']}/{system['database']}"
                case "SQLServer":
                    jdbc_str = f"jdbc:sqlserver://{system['host']}:{system['port']};databaseName={system['database']};encrypt=true;trustServerCertificate=true"
                case "Synapse":
                    jdbc_str = f"jdbc:sqlserver://{system['host']}:{system['port']};database={system['database']};encrypt=true;trustServerCertificate=false"
                case "Redshift":
                    jdbc_str = f"jdbc:redshift://{system['host']}:{system['port']}/{system['database']}"
                case _:
                    jdbc_str = f"jdbc:{system['kind'].lower()}://{system['host']}:{system['port']}/{system['database']}"
            print(f"Generated {system['kind']} JDBC string: {jdbc_str}")

        scope: str = system.get("secret_scope") or "livevalidator"
        user: str | None = dbutils.secrets.get(scope, system["user_secret_key"]) if system.get("user_secret_key") else None
        password: str | None = dbutils.secrets.get(scope, system["pass_secret_key"]) if system.get("pass_secret_key") else None

        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_str)
            .option("driver", system["driver_connector"])
            .option("user", user)
            .option("password", password)
        )

    @property
    def _uc_jdbc_reader(self) -> DataFrameReader:
        return (
            self.spark.read.format("jdbc")
            .option("databricks.connection", self.conn["system"]["uc_connection_name"])
        )

    def _read_uc_connection(self, query: str) -> DataFrame:
        uc_conn_name: str = self.conn["system"]["uc_connection_name"]
        return self.spark.sql(f"SELECT * FROM remote_query('{uc_conn_name}', query => '{query}')")


    def detect_partition_info(self, table: str) -> None:
        """Probe the source system for partition column bounds. Extend this match block to support additional sources."""
        if self.conn["method"] == "uc_connection":
            return None

        match self.conn["system"]["kind"]:
            case "SQLServer" | "Synapse":
                from sql_server_columns import sqlserver_partition_info

                self.partition_info = sqlserver_partition_info(table, lambda q: self.query(q))


    def _add_partition_column(self, raw_query: str) -> str:
        """Add partition column to query if needed. If it is a SELECT * FROM query then the column is already there."""
        from_pos: int = raw_query.upper().index(" FROM ")
        parallel_query: str = raw_query[:from_pos] + f", [{self.partition_info.column}] AS __lv_pk__" + raw_query[from_pos:]
        self.partition_info.column = "__lv_pk__"
        print(f"[Auto-Partition] Parallel JDBC read: {self.partition_info.num_partitions} partitions")
        return parallel_query


    def _safe_partition_reader(self, reader: DataFrameReader, query: str) -> DataFrame | None:
        try:
            if "SELECT * FROM" not in query:
                query = self._add_partition_column(query)
            reader = (
                reader.option("dbtable", f"({query}) AS _t")
                .option("partitionColumn", self.partition_info.column)
                .option("lowerBound", self.partition_info.lower)
                .option("upperBound", self.partition_info.upper)
                .option("numPartitions", self.partition_info.num_partitions)
            )
            return reader.load().drop("__lv_pk__")
        except Exception as e:
            print(f"[WARN] Parallel read failed ({e}), falling back to single connection")
            return None


    def query(self, query: str) -> DataFrame:
        """Execute a JDBC query and return DataFrame.

        When *self.partition_info* is provided, switches from single-connection
        ``.option("query", ...)`` to parallel ``.option("dbtable", ...)`` with
        Spark JDBC partitioning.
        """

        match self.conn["method"]:
            case "direct":
                reader = self._direct_reader
            case "uc_jdbc_connection":
                reader = self._uc_jdbc_reader
            case "uc_connection":
                return self._read_uc_connection(query)
            case _:
                raise ValueError(f"Unsupported JDBC method: {self.conn['method']}")

        reader = self._add_extra_options(reader)

        if self.partition_info:
            df: DataFrame | None = self._safe_partition_reader(reader, query)
            if df is not None:
                return df

        reader = reader.option("query", query)
        return reader.load()

