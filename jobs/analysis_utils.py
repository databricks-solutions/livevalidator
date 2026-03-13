"""Shared analysis utilities for validation jobs."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, TimestampType, DateType, StringType


def summarize_df(df: DataFrame, pk_columns: list[str]) -> list[dict]:
    """
    Compute summary stats per column, preserving column order.
    
    Returns list of dicts (one per column) to preserve order:
    [{"name": "col1", "type": "numeric", "min": ..., "max": ..., "nulls": ..., "is_pk": True}, ...]
    """
    from pyspark.sql.functions import col, min as spark_min, max as spark_max, countDistinct, sum as spark_sum, when  # noqa: PLC0415
    
    pk_set: set[str] = set(pk_columns)
    stats: list[dict] = []
    for field in df.schema.fields:
        name, dtype = field.name, str(field.dataType)
        is_pk = name in pk_set
        
        null_expr = spark_sum(when(col(name).isNull(), 1).otherwise(0))
        
        if any(t in dtype for t in ["Int", "Long", "Double", "Decimal", "Float", "Short"]):
            agg = df.agg(spark_min(name), spark_max(name), null_expr).collect()[0]
            stats.append({"name": name, "type": "numeric", "is_pk": is_pk,
                          "min": agg[0], "max": agg[1], "nulls": int(agg[2] or 0)})
        elif any(t in dtype for t in ["Timestamp", "Date"]):
            agg = df.agg(spark_min(name), spark_max(name), null_expr).collect()[0]
            stats.append({"name": name, "type": "time", "is_pk": is_pk,
                          "min": agg[0].isoformat() if agg[0] else None,
                          "max": agg[1].isoformat() if agg[1] else None,
                          "nulls": int(agg[2] or 0)})
        else:
            agg = df.agg(countDistinct(name), null_expr).collect()[0]
            stats.append({"name": name, "type": "string", "is_pk": is_pk,
                          "cardinality": int(agg[0] or 0), "nulls": int(agg[1] or 0)})
    return stats


def compare_summaries(source_summary: list[dict], target_summary: list[dict]) -> list[dict]:
    """Compare two summaries and return only columns with differences."""
    target_dict = {s["name"]: s for s in target_summary}
    diffs = []
    
    for src in source_summary:
        name = src["name"]
        tgt = target_dict.pop(name, None)
        
        if not tgt:
            diffs.append({"column": name, "source": src, "target": None, "difference_type": "missing_in_target"})
            continue
        
        if src["type"] != tgt["type"]:
            diffs.append({"column": name, "source": src, "target": tgt, "difference_type": "type_mismatch"})
        elif src["type"] in ("numeric", "time"):
            if (src.get("min"), src.get("max"), src.get("nulls")) != (tgt.get("min"), tgt.get("max"), tgt.get("nulls")):
                diffs.append({"column": name, "source": src, "target": tgt, "difference_type": "stats_differ"})
        elif src["type"] == "string":
            if (src.get("cardinality"), src.get("nulls")) != (tgt.get("cardinality"), tgt.get("nulls")):
                diffs.append({"column": name, "source": src, "target": tgt, "difference_type": "stats_differ"})
    
    # Columns in target but not in source
    for name, tgt in target_dict.items():
        diffs.append({"column": name, "source": None, "target": tgt, "difference_type": "missing_in_source"})
    
    return diffs
