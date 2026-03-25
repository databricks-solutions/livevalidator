"""Post-processing functions for except_all validation mode."""

from pyspark.sql import DataFrame

import sys
import os
_jobs_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.path.abspath('.')
sys.path.insert(0, _jobs_dir)

from analysis_utils import summarize_df, compare_summaries


def run_except_all_count_analysis(result: dict) -> dict | None:
    """
    Analyze row count mismatch for except_all mode.

    Compares summary statistics between diff DataFrames (src-tgt vs tgt-src),
    showing only columns where stats differ.
    """
    src_df: DataFrame = result.get("src_df")
    tgt_df: DataFrame = result.get("tgt_df")
    source_was_limited: bool = result.get("source_was_limited", False)
    row_count_source: int = result.get("row_count_source", 0)
    row_count_target: int = result.get("row_count_target", 0)
    row_count_match: bool = result.get("row_count_match", False)
    in_src_not_tgt_count: int = result.get("rows_different", 0)
    src_tgt_diff_df: DataFrame | None = result.get("diff_df")
    in_src_not_tgt_samples: list = result.get("sample_differences", [])

    if not all([src_df, tgt_df]):
        return None

    if source_was_limited and row_count_source < row_count_target:
        return {"mode": "row_count_mismatch_except_all", 
                "data": {"skipped": True, "reason": "source_limited_and_fewer"}}

    # Compute diffs if needed
    if not row_count_match and src_tgt_diff_df is None:
        src_tgt_diff_df = src_df.exceptAll(tgt_df).cache()
        in_src_not_tgt_count = src_tgt_diff_df.count()
        in_src_not_tgt_samples = [r.asDict() for r in src_tgt_diff_df.limit(10).collect()]

    tgt_src_diff_df = tgt_df.exceptAll(src_df).cache()
    in_tgt_not_src_count = tgt_src_diff_df.count()
    in_tgt_not_src_samples = [r.asDict() for r in tgt_src_diff_df.limit(10).collect()]

    # Compare summaries of the two diff DataFrames
    column_differences = compare_summaries(
        summarize_df(src_tgt_diff_df, []),
        summarize_df(tgt_src_diff_df, [])
    )

    src_tgt_diff_df.unpersist()
    tgt_src_diff_df.unpersist()
    src_df.unpersist(), tgt_df.unpersist()

    return {
        "mode": "row_count_mismatch_except_all",
        "data": {
            "column_differences": column_differences,
            "in_source_not_target": {"count": in_src_not_tgt_count, "samples": in_src_not_tgt_samples[:10]},
            "in_target_not_source": {"count": in_tgt_not_src_count, "samples": in_tgt_not_src_samples[:10]}
        }
}
    