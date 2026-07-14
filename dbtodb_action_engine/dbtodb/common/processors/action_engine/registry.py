"""ACTION_REGISTRY and lookup helpers (section 19/20).

Maps every action name in the contract to its implementation, including
the not-yet-implemented ones so the registry stays complete ("all actions
represented in registry even if implemented behind a feature flag").
"""
from __future__ import annotations

from typing import Callable, Dict

from common.processors.action_engine import actions_dataset as ds
from common.processors.action_engine import actions_multi as multi
from common.processors.action_engine.models import FLAG_COLUMN_SUPPORTED_ACTIONS, MVP_ACTIONS

ACTION_REGISTRY: Dict[str, Callable] = {
    "action-filter-dataset": ds.action_filter_dataset,
    "action-sort-dataset": ds.action_sort_dataset,
    "action-deduplicate": ds.action_deduplicate,
    "action-aggregate": ds.action_aggregate,
    "action-sample-limit": ds.action_sample_limit,
    "action-flatten-json": ds.action_flatten_json,
    "action-explode-array": ds.action_explode_array,
    "action-pivot-unpivot": ds.action_pivot_unpivot,
    "action-format-convert": ds.action_format_convert,
    "action-schema-validate": ds.action_schema_validate,
    "action-row-hash": ds.action_row_hash,
    "action-write-staging": ds.action_write_staging,
    "action-error-quarantine": ds.action_error_quarantine,
    "action-handle-drift": ds.action_handle_drift,
    "action-merge-join": multi.action_merge_join,
    "action-union": multi.action_union,
    "action-reconciliation": multi.action_reconciliation,
    "action-delta-detection": multi.action_delta_detection,
    "action-exception-extract": multi.action_exception_extract,
    "action-cross-validate": multi.action_cross_validate,
    "action-aggregate-multi": multi.action_aggregate_multi,
    "action-rolling-join": multi.action_rolling_join,
    "action-anti-join": multi.action_anti_join,
    "action-snapshot-diff": multi.action_snapshot_diff,
}


def get_action(action_name: str) -> Callable:
    try:
        return ACTION_REGISTRY[action_name]
    except KeyError as exc:
        raise ValueError(f"Unknown action '{action_name}'") from exc


def is_implemented(action_name: str) -> bool:
    return action_name in MVP_ACTIONS


def supports_flag_column(action_name: str) -> bool:
    return action_name in FLAG_COLUMN_SUPPORTED_ACTIONS
