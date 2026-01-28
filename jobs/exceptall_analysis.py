"""Post-processing functions for except_all validation mode."""

from pyspark.sql import DataFrame
from analysis_utils import summarize_df


def run_except_all_count_analysis(result: dict) -> dict | None:
    """
    Analyze row count mismatch for except_all mode.

    Compares summary statistics between source and target tables,
    showing only columns where stats differ. Reuses already-computed
    "in source not in target" results and adds "in target not in source".

    Args:
        result: Validation result dict containing src_df, tgt_df,
                row_count_source, row_count_target, source_was_limited, rows_different,
                sample_differences (already computed in source not in target direction)

    Returns:
        Analysis dict with mode/data structure, or None if not applicable
    """
    src_df: DataFrame = result.get("src_df")
    tgt_df: DataFrame = result.get("tgt_df")
    source_was_limited: bool = result.get("source_was_limited", False)
    row_count_source: int = result.get("row_count_source", 0)
    row_count_target: int = result.get("row_count_target", 0)
    rows_different: int = result.get("rows_different", 0)
    
    # Extract already-computed "in source not in target" samples
    existing_samples: dict = result.get("sample_differences", {})
    in_source_not_target_samples: list = existing_samples.get("data", {}).get("samples", [])

    if not all([src_df, tgt_df]):
        return None
    
    # Skip if source was limited and source < target (unreliable results)
    if source_was_limited and row_count_source < row_count_target:
        print("Skipping analysis: source was limited and source_count < target_count")
        return {
            "mode": "row_count_mismatch_except_all",
            "data": {
                "skipped": True,
                "reason": "source_limited_and_fewer"
            }
        }
    
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
    
    # Reuse already-computed "in source not in target" (from initial validation)
    # Only compute the reverse direction: "in target not in source"
    in_target_not_source_df = tgt_df.exceptAll(src_df)
    in_target_not_source_count = in_target_not_source_df.count()
    in_target_not_source_samples = [r.asDict() for r in in_target_not_source_df.limit(10).collect()]
    
    print(f"In source not in target: {rows_different} rows (reused from initial validation)")
    print(f"In target not in source: {in_target_not_source_count} rows")
    
    return {
        "mode": "row_count_mismatch_except_all",
        "data": {
            "source_row_count": row_count_source,
            "target_row_count": row_count_target,
            "column_differences": column_differences,
            "in_source_not_target": {
                "count": rows_different,
                "samples": in_source_not_target_samples[:10]  # Reuse from initial validation
            },
            "in_target_not_source": {
                "count": in_target_not_source_count,
                "samples": in_target_not_source_samples[:10]
            }
        }
    }
