"""Dataset-level action implementations (section 13).

Implemented in this build: filter, sort, deduplicate, aggregate,
sample-limit, format-convert, schema-validate, and row-hash.
Remaining dataset actions are registered but stubbed.

Every action function has the signature
`(inputs: List[pl.LazyFrame], config: Dict, output: Optional[OutputSpec]) -> pl.LazyFrame`.
Actions listed in `FLAG_COLUMN_SUPPORTED_ACTIONS` branch on
`output.mode == "flag_column"` to enrich the input in place instead of
reshaping/filtering it -- see models.OutputSpec for the contract.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from common.processors.action_engine.models import OutputSpec
from common.processors.action_engine.polars_utils import (
    build_condition_expr,
    cast_expr,
    row_hash_expr,
    validate_identifier,
)


def action_filter_dataset(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    lf = inputs[0]
    expr = build_condition_expr(config["condition"])
    if output and output.mode == "flag_column":
        target = validate_identifier(output.target_column)
        return lf.with_columns(expr.alias(target))
    return lf.filter(expr)


def action_sort_dataset(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    columns = [validate_identifier(c) for c in config["columns"]]
    order = config.get("order", "asc")
    nulls_last = config.get("nulls_last", True)
    return inputs[0].sort(columns, descending=(order == "desc"), nulls_last=nulls_last)


def action_deduplicate(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    subset = [validate_identifier(c) for c in config["subset"]]
    keep = config.get("keep", "first")
    return inputs[0].unique(subset=subset, keep=keep)


def action_aggregate(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    group_by = [validate_identifier(c) for c in config["group_by"]]
    aggs = []
    for metric in config["metrics"]:
        column = validate_identifier(metric["column"])
        agg = metric["agg"]
        alias = metric.get("alias", f"{column}_{agg}")
        aggs.append(getattr(pl.col(column), agg)().alias(alias))
    return inputs[0].group_by(group_by).agg(aggs)


def action_sample_limit(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    lf = inputs[0]
    if config.get("sample_fraction") is not None:
        return lf.collect().sample(fraction=config["sample_fraction"], seed=config.get("seed")).lazy()
    return lf.limit(config["limit"])


def action_format_convert(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    lf = inputs[0]
    columns = config["columns"]
    if output and output.mode == "flag_column":
        if len(columns) != 1:
            raise ValueError(
                "output.mode='flag_column' on action-format-convert requires exactly one "
                "entry in config.columns (ambiguous target otherwise)"
            )
        spec = columns[0]
        target = validate_identifier(output.target_column)
        expr = cast_expr(spec["name"], spec["type"], spec.get("format"), spec.get("strict", True))
        return lf.with_columns(expr.alias(target))

    exprs = []
    for spec in columns:
        column = validate_identifier(spec["name"])
        expr = cast_expr(column, spec["type"], spec.get("format"), spec.get("strict", True))
        exprs.append(expr.alias(column))
    return lf.with_columns(exprs)


def action_schema_validate(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    lf = inputs[0]
    expected_schema = config["schema"]
    fail_on_error = config.get("fail_on_error", True)
    actual_columns = set(lf.collect_schema().names())
    missing = [c for c in expected_schema if c not in actual_columns]

    if output and output.mode == "flag_column":
        target = validate_identifier(output.target_column)
        detail = f"MISSING:{','.join(missing)}" if missing else "OK"
        return lf.with_columns(pl.lit(detail).alias(target))

    if missing and fail_on_error:
        raise ValueError(f"Schema validation failed, missing columns: {missing}")
    return lf


def action_row_hash(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    # action-row-hash is inherently a "write a computed detail into a named
    # column" action, so its own config.target_column is authoritative;
    # output.target_column (if set) is honored as a convenience override.
    target_column = validate_identifier(
        (output.target_column if output and output.target_column else config["target_column"])
    )
    algo = config.get("algo", "sha256")
    return inputs[0].with_columns(row_hash_expr(config["columns"], algo).alias(target_column))


def action_write_staging(
    inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
) -> pl.LazyFrame:
    # Persistence to the staging table/schema named in config is performed by
    # the engine via common.processors.action_engine.db_sink.TargetWriter;
    # this action just forwards the dataset unchanged downstream.
    return inputs[0]


def _not_implemented(name: str):
    def _stub(
        inputs: List[pl.LazyFrame], config: Dict[str, Any], output: Optional[OutputSpec] = None
    ) -> pl.LazyFrame:
        raise NotImplementedError(f"'{name}' is not implemented in this build")

    return _stub


action_flatten_json = _not_implemented("action-flatten-json")
action_explode_array = _not_implemented("action-explode-array")
action_pivot_unpivot = _not_implemented("action-pivot-unpivot")
action_error_quarantine = _not_implemented("action-error-quarantine")
action_handle_drift = _not_implemented("action-handle-drift")
