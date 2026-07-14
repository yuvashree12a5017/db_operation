"""Multi-dataset action implementations (section 14).

Implemented in this build: merge-join, union, anti-join, exception-extract,
reconciliation, delta-detection, snapshot-diff, and cross-validate.
Remaining multi-dataset actions are registered but stubbed.

reconciliation/delta-detection/snapshot-diff/cross-validate all write a
single computed detail column across the full joined dataset; by default
that column has a fixed PRD-style name (recon_status, change_type,
change_status, validation_status), but `output.target_column` lets the
caller rename/choose it explicitly -- this is the primary use case the
universal target-column feature was built for.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from common.processors.action_engine.models import OutputSpec
from common.processors.action_engine.polars_utils import (
    build_condition_expr,
    validate_identifier,
    value_mismatch_expr,
)


def action_merge_join(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    left, right = inputs[0], inputs[1]
    left_cols = [validate_identifier(pair["left"]) for pair in config["on"]]
    right_cols = [validate_identifier(pair["right"]) for pair in config["on"]]
    suffix = config.get("suffix", "_right")
    return left.join(right, left_on=left_cols, right_on=right_cols, how=config["type"], suffix=suffix)


def action_union(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    align_schema = config.get("align_schema", False)
    result = pl.concat(inputs, how="diagonal" if align_schema else "vertical")
    if config["type"] == "union":
        result = result.unique()
    return result


def action_anti_join(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    left, right = inputs[0], inputs[1]
    on = config["on"]
    if on and isinstance(on[0], dict):
        left_cols = [validate_identifier(pair["left"]) for pair in on]
        right_cols = [validate_identifier(pair["right"]) for pair in on]
    else:
        left_cols = right_cols = [validate_identifier(c) for c in on]

    if output and output.mode == "flag_column":
        target = validate_identifier(output.target_column)
        marker = right.select(right_cols).unique().with_columns(pl.lit(True).alias("_matched_marker"))
        joined = left.join(marker, left_on=left_cols, right_on=right_cols, how="left")
        return joined.with_columns(pl.col("_matched_marker").is_null().alias(target)).drop(
            "_matched_marker"
        )

    return left.join(right, left_on=left_cols, right_on=right_cols, how="anti")


def action_exception_extract(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    lf = inputs[0]
    for extra in inputs[1:]:
        lf = pl.concat([lf, extra], how="diagonal")
    expr = build_condition_expr(config["condition"])

    if output and output.mode == "flag_column":
        target = validate_identifier(output.target_column)
        return lf.with_columns(expr.alias(target))

    output_mode = config.get("output_mode", "exceptions_only")
    return lf.filter(expr) if output_mode == "exceptions_only" else lf.filter(~expr)


def action_reconciliation(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    src, tgt = inputs[0], inputs[1]
    keys = [validate_identifier(k) for k in config["keys"]]
    compare_columns = [validate_identifier(c) for c in config.get("compare_columns", [])]
    tolerance = config.get("tolerance", 0.0)
    status_col = validate_identifier(output.target_column) if (output and output.mode == "flag_column") else "recon_status"

    src_m = src.with_columns(pl.lit(True).alias("_src_marker"))
    tgt_m = tgt.with_columns(pl.lit(True).alias("_tgt_marker"))
    joined = src_m.join(tgt_m, on=keys, how="full", suffix="_tgt", coalesce=True)

    missing_in_target = pl.col("_src_marker") & pl.col("_tgt_marker").is_null()
    missing_in_source = pl.col("_tgt_marker") & pl.col("_src_marker").is_null()
    any_mismatch = _combine_mismatches(compare_columns, tolerance)

    status_expr = (
        pl.when(missing_in_target)
        .then(pl.lit("MISSING_IN_TARGET"))
        .when(missing_in_source)
        .then(pl.lit("MISSING_IN_SOURCE"))
        .when(any_mismatch)
        .then(pl.lit("MISMATCHED"))
        .otherwise(pl.lit("MATCHED"))
    )
    return joined.with_columns(status_expr.alias(status_col)).drop(["_src_marker", "_tgt_marker"])


def action_delta_detection(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    old, new = inputs[0], inputs[1]
    keys = [validate_identifier(k) for k in config["keys"]]
    compare_columns = [validate_identifier(c) for c in config.get("compare_columns", [])]
    change_col = validate_identifier(output.target_column) if (output and output.mode == "flag_column") else "change_type"

    old_m = old.with_columns(pl.lit(True).alias("_old_marker"))
    new_m = new.with_columns(pl.lit(True).alias("_new_marker"))
    joined = old_m.join(new_m, on=keys, how="full", suffix="_new", coalesce=True)

    inserted = pl.col("_new_marker") & pl.col("_old_marker").is_null()
    deleted = pl.col("_old_marker") & pl.col("_new_marker").is_null()
    any_mismatch = _combine_mismatches(compare_columns, 0.0, right_suffix="_new")

    change_expr = (
        pl.when(inserted)
        .then(pl.lit("INSERT"))
        .when(deleted)
        .then(pl.lit("DELETE"))
        .when(any_mismatch)
        .then(pl.lit("UPDATE"))
        .otherwise(pl.lit("UNCHANGED"))
    )
    return joined.with_columns(change_expr.alias(change_col)).drop(["_old_marker", "_new_marker"])


def action_snapshot_diff(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    d1, d2 = inputs[0], inputs[1]
    keys = [validate_identifier(k) for k in config["keys"]]
    compare_columns = [validate_identifier(c) for c in config.get("compare_columns", [])]
    status_col = validate_identifier(output.target_column) if (output and output.mode == "flag_column") else "change_status"

    d1_m = d1.with_columns(pl.lit(True).alias("_d1_marker"))
    d2_m = d2.with_columns(pl.lit(True).alias("_d2_marker"))
    joined = d1_m.join(d2_m, on=keys, how="full", suffix="_d2", coalesce=True)

    added = pl.col("_d2_marker") & pl.col("_d1_marker").is_null()
    removed = pl.col("_d1_marker") & pl.col("_d2_marker").is_null()
    any_mismatch = _combine_mismatches(compare_columns, 0.0, right_suffix="_d2")

    status_expr = (
        pl.when(added)
        .then(pl.lit("ADDED"))
        .when(removed)
        .then(pl.lit("REMOVED"))
        .when(any_mismatch)
        .then(pl.lit("CHANGED"))
        .otherwise(pl.lit("UNCHANGED"))
    )
    return joined.with_columns(status_expr.alias(status_col)).drop(["_d1_marker", "_d2_marker"])


def action_cross_validate(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    left, right = inputs[0], inputs[1]
    keys = [validate_identifier(k) for k in config["keys"]]
    joined = left.join(right, on=keys, how="inner", suffix="_right")

    rule_exprs = [
        _rule_pass_expr(
            validate_identifier(rule["left_column"]),
            validate_identifier(rule["right_column"]),
            rule.get("op", "eq"),
            rule.get("tolerance", 0.0),
        )
        for rule in config["rules"]
    ]
    overall_pass = rule_exprs[0]
    for expr in rule_exprs[1:]:
        overall_pass = overall_pass & expr

    status_col = validate_identifier(output.target_column) if (output and output.mode == "flag_column") else "validation_status"
    status_expr = pl.when(overall_pass).then(pl.lit("PASS")).otherwise(pl.lit("FAIL"))
    return joined.with_columns(status_expr.alias(status_col))


def _combine_mismatches(compare_columns: List[str], tolerance: float, right_suffix: str = "_tgt") -> pl.Expr:
    if not compare_columns:
        return pl.lit(False)
    exprs = [value_mismatch_expr(c, tolerance, right_suffix) for c in compare_columns]
    combined = exprs[0]
    for e in exprs[1:]:
        combined = combined | e
    return combined


def _rule_pass_expr(left_col: str, right_col: str, op: str, tolerance: float) -> pl.Expr:
    left, right = pl.col(left_col), pl.col(right_col)
    if op == "eq":
        return (left - right).abs() <= tolerance if tolerance else left == right
    if op == "neq":
        return (left - right).abs() > tolerance if tolerance else left != right
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    if op == "lte":
        return left <= right
    raise ValueError(f"Unsupported cross-validate op: '{op}'")


def _not_implemented(name: str):
    def _stub(
        inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
    ) -> pl.LazyFrame:
        raise NotImplementedError(f"'{name}' is not implemented in this build")

    return _stub


action_aggregate_multi = _not_implemented("action-aggregate-multi")
action_rolling_join = _not_implemented("action-rolling-join")
