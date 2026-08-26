"""Unit tests for JDBCReader and PartitionInfo."""
import sys
from unittest.mock import MagicMock, patch

# Mock pyspark and databricks before importing
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.runtime"] = MagicMock()

from jobs.jdbc_reader import JDBCReader, PartitionInfo


def _conn(method: str = "direct", kind: str = "SQLServer", **extras) -> dict:
    system = {"kind": kind, "name": "test", **extras}
    return {"method": method, "system": system}


def _reader(method: str = "direct", partition_info: PartitionInfo | None = None, **extras) -> JDBCReader:
    r = JDBCReader(_conn(method, **extras))
    r.spark = MagicMock()
    r.partition_info = partition_info
    return r


# ---------------------------------------------------------------------------
# PartitionInfo
# ---------------------------------------------------------------------------
class TestPartitionInfo:
    def test_num_partitions_min(self):
        assert PartitionInfo("col", 0, 100).num_partitions == 4

    def test_num_partitions_max(self):
        assert PartitionInfo("col", 0, 100_000_000).num_partitions == 12

    def test_num_partitions_scales(self):
        assert PartitionInfo("col", 0, 5_000_000).num_partitions == 5


# ---------------------------------------------------------------------------
# _add_partition_column
# ---------------------------------------------------------------------------
class TestAddPartitionColumn:
    def test_explicit_columns_adds_alias(self):
        p = PartitionInfo(column="order_id", lower=1, upper=1000)
        r = _reader(partition_info=p)
        result = r._add_partition_column("SELECT col1, col2 FROM schema.table")

        assert "[order_id] AS __lv_pk__" in result
        assert p.column == "__lv_pk__"

    def test_case_insensitive_from(self):
        p = PartitionInfo(column="pk", lower=1, upper=100)
        r = _reader(partition_info=p)
        result = r._add_partition_column("select col1, col2 from schema.table")

        assert "[pk] AS __lv_pk__" in result
        assert p.column == "__lv_pk__"

    def test_column_inserted_before_from(self):
        p = PartitionInfo(column="pk", lower=0, upper=100)
        r = _reader(partition_info=p)
        result = r._add_partition_column("SELECT a, b, c FROM mytable")

        assert result == "SELECT a, b, c, [pk] AS __lv_pk__ FROM mytable"


# ---------------------------------------------------------------------------
# _add_extra_options
# ---------------------------------------------------------------------------
class TestAddExtraOptions:
    def test_applies_jdbc_options(self):
        r = _reader(options='{"jdbc": {"fetchsize": "1000", "batchsize": "500"}}')
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader

        r._add_extra_options(mock_reader)

        mock_reader.option.assert_any_call("fetchsize", "1000")
        mock_reader.option.assert_any_call("batchsize", "500")

    def test_no_options_is_noop(self):
        r = _reader()
        mock_reader = MagicMock()
        result = r._add_extra_options(mock_reader)
        assert result is mock_reader

    def test_handles_dict_options(self):
        r = _reader(options={"jdbc": {"fetchsize": "100"}})
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader

        r._add_extra_options(mock_reader)

        mock_reader.option.assert_called_once_with("fetchsize", "100")


# ---------------------------------------------------------------------------
# query() dispatch
# ---------------------------------------------------------------------------
class TestQueryDispatch:
    def test_direct_uses_direct_reader(self):
        r = _reader("direct")
        mock_df = MagicMock()
        r.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value = (
            MagicMock(option=MagicMock(return_value=MagicMock(load=MagicMock(return_value=mock_df))))
        )

        with patch.object(JDBCReader, "_direct_reader", new_callable=lambda: property(lambda self: MagicMock(
            option=MagicMock(return_value=MagicMock(load=MagicMock(return_value=mock_df)))
        ))):
            result = r.query("SELECT 1")
        assert result is mock_df

    def test_uc_connection_uses_spark_sql(self):
        r = _reader("uc_connection", uc_connection_name="my_conn")
        mock_df = MagicMock()
        r.spark.sql.return_value = mock_df

        result = r.query("SELECT 1")

        r.spark.sql.assert_called_once()
        assert "remote_query" in r.spark.sql.call_args[0][0]
        assert result is mock_df

    def test_unsupported_method_raises(self):
        r = _reader("bogus_method")
        try:
            r.query("SELECT 1")
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "bogus_method" in str(e)


# ---------------------------------------------------------------------------
# _safe_partition_reader
# ---------------------------------------------------------------------------
class TestSafePartitionReader:
    def test_returns_df_on_success(self):
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader(partition_info=p)
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value.drop.return_value = mock_df

        result = r._safe_partition_reader(mock_reader, "SELECT * FROM t")
        assert result is mock_df

    def test_returns_none_on_exception(self):
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader(partition_info=p)
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.side_effect = Exception("Connection reset")

        result = r._safe_partition_reader(mock_reader, "SELECT * FROM t")
        assert result is None

    def test_select_star_skips_add_partition_column(self):
        """SELECT * queries should NOT call _add_partition_column."""
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader(partition_info=p)
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value.drop.return_value = MagicMock()

        with patch.object(r, "_add_partition_column") as mock_apc:
            r._safe_partition_reader(mock_reader, "SELECT * FROM t")
            mock_apc.assert_not_called()

    def test_non_star_calls_add_partition_column(self):
        """Explicit column queries SHOULD call _add_partition_column."""
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader(partition_info=p)
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value.drop.return_value = MagicMock()

        with patch.object(r, "_add_partition_column", return_value="SELECT a, [id] AS __lv_pk__ FROM t") as mock_apc:
            r._safe_partition_reader(mock_reader, "SELECT a FROM t")
            mock_apc.assert_called_once()


# ---------------------------------------------------------------------------
# query() fallback behavior
# ---------------------------------------------------------------------------
class TestQueryFallback:
    def test_falls_back_when_partition_read_fails(self):
        """If _safe_partition_reader returns None, query() falls back to non-partitioned read."""
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader("direct", partition_info=p)
        fallback_df = MagicMock()

        with (
            patch.object(JDBCReader, "_direct_reader", new_callable=lambda: property(lambda self: MagicMock(
                option=MagicMock(return_value=MagicMock(load=MagicMock(return_value=fallback_df)))
            ))),
            patch.object(r, "_safe_partition_reader", return_value=None),
            patch.object(r, "_add_extra_options", side_effect=lambda rd: rd),
        ):
            result = r.query("SELECT 1")
        assert result is fallback_df

    def test_uses_partition_read_when_available(self):
        """If _safe_partition_reader succeeds, query() returns its result."""
        p = PartitionInfo(column="id", lower=0, upper=1000)
        r = _reader("direct", partition_info=p)
        partitioned_df = MagicMock()

        with (
            patch.object(JDBCReader, "_direct_reader", new_callable=lambda: property(lambda self: MagicMock())),
            patch.object(r, "_safe_partition_reader", return_value=partitioned_df),
            patch.object(r, "_add_extra_options", side_effect=lambda rd: rd),
        ):
            result = r.query("SELECT 1")
        assert result is partitioned_df

    def test_no_partition_info_skips_partition_read(self):
        """Without partition_info, query() goes straight to non-partitioned read."""
        r = _reader("direct")
        plain_df = MagicMock()

        with (
            patch.object(JDBCReader, "_direct_reader", new_callable=lambda: property(lambda self: MagicMock(
                option=MagicMock(return_value=MagicMock(load=MagicMock(return_value=plain_df)))
            ))),
            patch.object(r, "_add_extra_options", side_effect=lambda rd: rd),
        ):
            result = r.query("SELECT 1")
        assert result is plain_df


# ---------------------------------------------------------------------------
# detect_partition_info
# ---------------------------------------------------------------------------
class TestDetectPartitionInfo:
    def test_skips_for_uc_connection(self):
        r = _reader("uc_connection")
        r.detect_partition_info("dbo.table")
        assert r.partition_info is None

    @patch("sql_server_columns.sqlserver_partition_info")
    def test_delegates_to_sqlserver(self, mock_ss):
        mock_ss.return_value = PartitionInfo("id", 1, 1000)
        r = _reader("direct", kind="SQLServer")
        r.detect_partition_info("dbo.table")
        assert r.partition_info is not None
        assert r.partition_info.column == "id"

    @patch("sql_server_columns.sqlserver_partition_info")
    def test_delegates_to_sqlserver_for_synapse(self, mock_ss):
        mock_ss.return_value = PartitionInfo("id", 1, 1000)
        r = _reader("direct", kind="Synapse")
        r.detect_partition_info("dbo.table")
        assert r.partition_info is not None
        assert r.partition_info.column == "id"


# ---------------------------------------------------------------------------
# _direct_reader JDBC string generation
# ---------------------------------------------------------------------------
class TestDirectReaderUrl:
    def _url(self, kind: str, **extras) -> str:
        r = _reader(
            "direct",
            kind=kind,
            jdbc_string=None,
            driver_connector="com.example.Driver",
            host="myhost",
            port=1433,
            database="mydb",
            **extras,
        )
        r._direct_reader
        url_call = r.spark.read.format.return_value.option.call_args_list[0]
        return url_call[0][1]

    def test_sqlserver_url(self):
        assert self._url("SQLServer") == (
            "jdbc:sqlserver://myhost:1433;databaseName=mydb;encrypt=true;trustServerCertificate=true"
        )

    def test_synapse_url(self):
        assert self._url("Synapse") == (
            "jdbc:sqlserver://myhost:1433;database=mydb;encrypt=true;trustServerCertificate=false"
        )

    def test_explicit_jdbc_string_wins(self):
        r = _reader(
            "direct",
            kind="Synapse",
            jdbc_string="jdbc:sqlserver://custom:1433;database=other",
            driver_connector="com.example.Driver",
        )
        r._direct_reader
        url_call = r.spark.read.format.return_value.option.call_args_list[0]
        assert url_call[0][1] == "jdbc:sqlserver://custom:1433;database=other"
