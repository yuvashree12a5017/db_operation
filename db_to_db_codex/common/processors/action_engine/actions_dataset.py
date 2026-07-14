"""Dataset-level action implementations from the PRD MVP."""
from __future__ import annotations

from typing import Any

import polars as pl

from common.processors.action_engine.polars_utils import build_condition_expr, row_hash_expr, validate_identifier


def _one(inputs: list[pl.LazyFrame]) -> pl.LazyFrame:
    if len(inputs) != 1:
        raise ValueError("This action requires exactly one input dataset")
    return inputs[0]


def action_filter_dataset(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    return _one(inputs).filter(build_condition_expr(config["condition"]))


def action_sort_dataset(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    columns = [validate_identifier(column) for column in config["columns"]]
    return _one(inputs).sort(columns, descending=config["order"] == "desc", nulls_last=config["nulls_last"])


def action_deduplicate(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    subset = [validate_identifier(column) for column in config["subset"]]
    return _one(inputs).unique(subset=subset, keep=config["keep"])


def action_aggregate(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    metrics = []
    for metric in config["metrics"]:
        column = validate_identifier(metric["column"])
        alias = validate_identifier(metric.get("alias") or f"{column}_{metric['agg']}")
        metrics.append(getattr(pl.col(column), metric["agg"])().alias(alias))
    groups = [validate_identifier(column) for column in config["group_by"]]
    return _one(inputs).group_by(groups).agg(metrics) if groups else _one(inputs).select(metrics)


def action_sample_limit(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    frame = _one(inputs)
    if "sample_fraction" in config:
        return frame.collect().sample(fraction=config["sample_fraction"], seed=config.get("seed")).lazy()
    return frame.limit(config["limit"])


def action_schema_validate(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    frame = _one(inputs)
    expected = config["schema"]
    actual = set(frame.collect_schema().names())
    missing = sorted(set(expected) - actual)
    if missing and config["fail_on_error"]:
        raise ValueError(f"Schema validation failed; missing columns: {missing}")
    return frame


def action_row_hash(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    target = validate_identifier(config["target_column"])
    return _one(inputs).with_columns(row_hash_expr(config["columns"], config["algo"]).alias(target))


def action_write_staging(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    return _one(inputs)


def _not_implemented(name: str):
    def stub(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
        raise NotImplementedError(f"'{name}' is registered but not implemented in this build")
    return stub


action_flatten_json = _not_implemented("action-flatten-json")
action_explode_array = _not_implemented("action-explode-array")
action_pivot_unpivot = _not_implemented("action-pivot-unpivot")
action_format_convert = _not_implemented("action-format-convert")
action_error_quarantine = _not_implemented("action-error-quarantine")
action_handle_drift = _not_implemented("action-handle-drift")
