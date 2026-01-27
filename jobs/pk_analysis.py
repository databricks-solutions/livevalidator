from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compare_pk_samples(
    src_rows: list[dict],
    tgt_rows: list[dict],
    pk_columns: list[str],
    source_system_name: str,
    target_system_name: str
) -> list[dict] | None:
    """
    Pure Python comparison of source/target rows - no Spark dependency.
    
    Args:
        src_rows: Source rows as list of dicts
        tgt_rows: Target rows as list of dicts  
        pk_columns: Primary key column names
        source_system_name: Name of source system
        target_system_name: Name of target system
    
    Returns:
        List of mismatch dicts with .system key, or None if row counts don't match
    """
    if len(src_rows) != len(tgt_rows):
        return None
    
    zipped_samples = zip(
        sorted(src_rows, key=lambda item: [item[pk] for pk in pk_columns]),
        sorted(tgt_rows, key=lambda item: [item[pk] for pk in pk_columns])
    )
    
    return [
        {**{pk: src[pk] for pk in pk_columns}, **item}
        for src, tgt in zipped_samples
        for item in [
            {".system": source_system_name, **{k: v for k, v in src.items() if v != tgt[k]}},
            {".system": target_system_name, **{k: tgt[k] for k, v in src.items() if v != tgt[k]}}
        ]
    ]


def null_safe_join(lhs: DataFrame, rhs: DataFrame, keys: list[str], how: str = "inner") -> DataFrame:
    """Join DataFrames with null-safe key comparison"""
    from pyspark.sql.functions import col, coalesce, lit  # noqa: PLC0415
    jk: list[str] = [f"__k{i}" for i in range(len(keys))]
    null_sentinel: str = "live_validator_null_placeholder"
    
    def with_join_keys(df: DataFrame) -> DataFrame:
        nulls_replaced = [coalesce(col(k).cast("string"), lit(null_sentinel)).alias(jk[i]) for i, k in enumerate(keys)]
        return df.select("*", *nulls_replaced)

    return with_join_keys(lhs).join(with_join_keys(rhs).drop(*keys), jk, how).drop(*jk)


def summarize_df(df: DataFrame, pk_columns: list[str]) -> list[dict]:
    """
    Compute summary stats per column, preserving column order.
    
    Returns list of dicts (one per column) to preserve order:
    [{"name": "col1", "type": "numeric", "min": ..., "max": ..., "nulls": ..., "is_pk": True}, ...]
    """
    from pyspark.sql.functions import col, min as spark_min, max as spark_max, countDistinct, sum as spark_sum, when  # noqa: PLC0415
    
    pk_set: set[str] = {pk.lower() for pk in pk_columns}
    stats: list[dict] = []
    for field in df.schema.fields:
        name, dtype = field.name, str(field.dataType)
        is_pk = name.lower() in pk_set
        
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


def run_except_all_count_analysis(result: dict) -> dict | None:
    """
    Analyze row count mismatch for except_all mode.
    
    Compares summary statistics between source and target tables,
    showing only columns where stats differ. Reuses except_all results
    from the validation run.
    
    Args:
        result: Validation result dict containing src_df, tgt_df,
                sample_differences (from except_all), row_count_source, row_count_target
    
    Returns:
        Analysis dict with column differences and sample rows, or None if not applicable
    """
    src_df: DataFrame = result.get("src_df")
    tgt_df: DataFrame = result.get("tgt_df")
    row_count_source: int = result.get("row_count_source", 0)
    row_count_target: int = result.get("row_count_target", 0)
    except_all_samples: list[dict] = result.get("sample_differences", [])
    rows_different: int = result.get("rows_different", 0)
    
    if not all([src_df, tgt_df]):
        return None
    
    print("Analyzing column statistics for except_all row count mismatch...")
    
    # Calculate summary stats for both tables
    source_summary = summarize_df(src_df, [])  # No PKs in except_all mode
    target_summary = summarize_df(tgt_df, [])
    
    # Create lookup dict for target summary by column name
    target_summary_dict = {col["name"]: col for col in target_summary}
    
    # Compare and filter to columns with differences
    column_differences = []
    for src_col in source_summary:
        col_name = src_col["name"]
        tgt_col = target_summary_dict.get(col_name)
        
        if not tgt_col:
            # Column missing in target
            column_differences.append({
                "column": col_name,
                "source": src_col,
                "target": None,
                "difference_type": "missing_in_target"
            })
            continue
        
        # Check if stats differ
        differs = False
        if src_col["type"] == tgt_col["type"]:
            if src_col["type"] in ["numeric", "time"]:
                differs = (src_col.get("min") != tgt_col.get("min") or 
                          src_col.get("max") != tgt_col.get("max") or
                          src_col.get("nulls") != tgt_col.get("nulls"))
            elif src_col["type"] == "string":
                differs = (src_col.get("cardinality") != tgt_col.get("cardinality") or
                          src_col.get("nulls") != tgt_col.get("nulls"))
        else:
            differs = True  # Type mismatch
        
        if differs:
            column_differences.append({
                "column": col_name,
                "source": src_col,
                "target": tgt_col,
                "difference_type": "stats_differ"
            })
    
    # Check for columns in target but not source
    source_cols = {col["name"] for col in source_summary}
    for tgt_col in target_summary:
        if tgt_col["name"] not in source_cols:
            column_differences.append({
                "column": tgt_col["name"],
                "source": None,
                "target": tgt_col,
                "difference_type": "missing_in_source"
            })
    
    print(f"Found {len(column_differences)} columns with differing statistics")
    
    # Get except_all samples - handle both dict (bidirectional) and list formats
    if isinstance(except_all_samples, dict) and "in_source_not_target" in except_all_samples:
        # Already in bidirectional format from validate_rows
        in_source_not_target_samples = except_all_samples.get("in_source_not_target", {}).get("samples", [])
        in_target_not_source_samples = except_all_samples.get("in_target_not_source", {}).get("samples", [])
        in_target_not_source_count = except_all_samples.get("in_target_not_source", {}).get("count", 0)
        print(f"Using bidirectional samples from validation run")
    else:
        # Old format - simple list of samples from source not in target
        in_source_not_target_samples = except_all_samples if isinstance(except_all_samples, list) else []
        
        # Get the reverse: rows in target not in source
        in_target_not_source_df = tgt_df.exceptAll(src_df)
        in_target_not_source_count = in_target_not_source_df.count()
        in_target_not_source_samples = [r.asDict() for r in in_target_not_source_df.limit(10).collect()]
    
    print(f"In source not in target: {rows_different} rows")
    print(f"In target not in source: {in_target_not_source_count} rows")
    
    return {
        "mode": "row_count_mismatch_except_all",
        "source_row_count": row_count_source,
        "target_row_count": row_count_target,
        "column_differences": column_differences,
        "in_source_not_target": {
            "count": rows_different,
            "samples": in_source_not_target_samples[:10] if in_source_not_target_samples else []  # Limit to 10
        },
        "in_target_not_source": {
            "count": in_target_not_source_count,
            "samples": in_target_not_source_samples[:10] if in_target_not_source_samples else []  # Limit to 10
        }
    }


def run_pk_count_analysis(result: dict) -> dict | None:
    """
    Analyze row count mismatch using FULL OUTER JOIN on PKs.
    
    Args:
        result: Validation result dict containing src_df, tgt_df, pk_columns,
                row_count_source, row_count_target, source_was_limited
    
    Returns:
        Analysis dict or None if skipped/not applicable
    """
    from pyspark.sql.functions import col  # noqa: PLC0415
    
    pk_columns: list[str] = result.get("pk_columns", [])
    src_df: DataFrame = result.get("src_df")
    tgt_df: DataFrame = result.get("tgt_df")
    source_was_limited: bool = result.get("source_was_limited", False)
    row_count_source: int = result.get("row_count_source", 0)
    row_count_target: int = result.get("row_count_target", 0)
    
    if not all([src_df, tgt_df, pk_columns]):
        return None
    
    # Skip if source was limited and source < target (unreliable results)
    if source_was_limited and row_count_source < row_count_target:
        print("Skipping analysis: source was limited and source_count < target_count")
        return {"mode": "row_count_mismatch", "skipped": True, "reason": "source_limited_and_fewer"}
    
    # Select only PK columns for the join to avoid column name collisions
    src_pks = src_df.select(*pk_columns)
    tgt_pks = tgt_df.select(*pk_columns)
    
    # FULL OUTER JOIN on PKs
    # Rename target PKs to avoid collision
    tgt_pk_aliases = {pk: f"_tgt_{pk}" for pk in pk_columns}
    tgt_pks_renamed = tgt_pks.select(*[col(pk).alias(tgt_pk_aliases[pk]) for pk in pk_columns])
    
    join_cond = [col(pk).eqNullSafe(col(tgt_pk_aliases[pk])) for pk in pk_columns]
    joined = src_pks.join(tgt_pks_renamed, on=join_cond, how="full")
    
    # Missing in target: source PKs exist, target PKs are null
    missing_in_target_df = joined.filter(col(tgt_pk_aliases[pk_columns[0]]).isNull()).select(*pk_columns)
    # Missing in source: target PKs exist, source PKs are null  
    missing_in_source_df = joined.filter(col(pk_columns[0]).isNull()).select(*[col(tgt_pk_aliases[pk]).alias(pk) for pk in pk_columns])
    
    # Join back to get full rows for samples and stats
    missing_in_target_full = null_safe_join(src_df, missing_in_target_df, pk_columns, "inner")
    missing_in_source_full = null_safe_join(tgt_df, missing_in_source_df, pk_columns, "inner")
    
    # Counts
    missing_in_target_count = missing_in_target_full.count()
    missing_in_source_count = missing_in_source_full.count()
    
    print(f"Missing in target: {missing_in_target_count}, Missing in source: {missing_in_source_count}")
    
    # Summary stats
    missing_in_target_summary = summarize_df(missing_in_target_full, pk_columns) if missing_in_target_count > 0 else []
    missing_in_source_summary = summarize_df(missing_in_source_full, pk_columns) if missing_in_source_count > 0 else []
    
    # Samples (10 each)
    missing_in_target_samples = [r.asDict() for r in missing_in_target_full.limit(10).collect()]
    missing_in_source_samples = [r.asDict() for r in missing_in_source_full.limit(10).collect()]
    
    return {
        "mode": "row_count_mismatch",
        "skipped": False,
        "pk_columns": pk_columns,
        "missing_in_target": {
            "count": missing_in_target_count,
            "summary": missing_in_target_summary,
            "samples": missing_in_target_samples
        },
        "missing_in_source": {
            "count": missing_in_source_count,
            "summary": missing_in_source_summary,
            "samples": missing_in_source_samples
        }
    }


def run_pk_analysis(result: dict) -> dict | None:
    """
    Analyze PK mismatches and return formatted sample differences.
    
    Args:
        result: Validation result dict containing src_df, tgt_df, sample_df, 
                pk_columns, source_system_name, target_system_name
    
    Returns:
        pk_sample_differences dict or None if analysis not applicable
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from pyspark.sql.functions import broadcast  # noqa: PLC0415
    
    spark = SparkSession.getActiveSession()
    
    pk_columns: list[str] = result.get("pk_columns", [])
    src_df: DataFrame = result.get("src_df")
    tgt_df: DataFrame = result.get("tgt_df")
    sample_df: DataFrame = result.get("sample_df")
    source_system_name: str = result.get("source_system_name", "")
    target_system_name: str = result.get("target_system_name", "")
    
    if not all([src_df, tgt_df, sample_df, pk_columns]):
        return None
    
    # Collect from Spark DataFrames
    src_rows: list[dict] = [r.asDict() for r in null_safe_join(src_df, broadcast(sample_df), pk_columns).collect()]
    tgt_rows: list[dict] = [r.asDict() for r in null_safe_join(tgt_df, broadcast(sample_df), pk_columns).collect()]
    
    # Pure Python comparison
    mismatch_samples = compare_pk_samples(src_rows, tgt_rows, pk_columns, source_system_name, target_system_name)
    
    if mismatch_samples is None:
        print("Found inconsistencies when matching primary keys. One or more PKs may be invalid.")
        return None

    if mismatch_samples:
        mismatch_df: DataFrame = spark.createDataFrame(mismatch_samples)
        mismatch_df.display()
    print(mismatch_samples)

    return {
        "mode": "primary_key",
        "pk_columns": pk_columns,
        "samples": mismatch_samples
    }
