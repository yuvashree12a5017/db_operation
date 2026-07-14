"""Multi-dataset action implementations from the PRD MVP."""
from __future__ import annotations

from typing import Any

import polars as pl

from common.processors.action_engine.polars_utils import validate_identifier


def action_merge_join(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    if len(inputs) != 2:
        raise ValueError("action-merge-join requires exactly two inputs")
    left_on = [validate_identifier(pair["left"]) for pair in config["on"]]
    right_on = [validate_identifier(pair["right"]) for pair in config["on"]]
    how = "full" if config["type"] == "outer" else config["type"]
    return inputs[0].join(inputs[1], left_on=left_on, right_on=right_on, how=how, suffix=config["suffix"])


def action_union(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    if len(inputs) < 2:
        raise ValueError("action-union requires at least two inputs")
    result = pl.concat(inputs, how="diagonal" if config["align_schema"] else "vertical")
    return result.unique() if config["type"] == "union" else result


def action_anti_join(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
    if len(inputs) != 2:
        raise ValueError("action-anti-join requires exactly two inputs")
    on = config["on"]
    if isinstance(on[0], dict):
        left_on = [validate_identifier(pair["left"]) for pair in on]
        right_on = [validate_identifier(pair["right"]) for pair in on]
    else:
        left_on = right_on = [validate_identifier(column) for column in on]
    return inputs[0].join(inputs[1], left_on=left_on, right_on=right_on, how="anti")


def _not_implemented(name: str):
    def stub(inputs: list[pl.LazyFrame], config: dict[str, Any]) -> pl.LazyFrame:
        raise NotImplementedError(f"'{name}' is registered but not implemented in this build")
    return stub


action_reconciliation = _not_implemented("action-reconciliation")
action_delta_detection = _not_implemented("action-delta-detection")
action_exception_extract = _not_implemented("action-exception-extract")
action_cross_validate = _not_implemented("action-cross-validate")
action_aggregate_multi = _not_implemented("action-aggregate-multi")
action_rolling_join = _not_implemented("action-rolling-join")
action_snapshot_diff = _not_implemented("action-snapshot-diff")
