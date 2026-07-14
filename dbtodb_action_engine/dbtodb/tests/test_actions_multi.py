"""Unit tests for the multi-dataset actions, including output.mode='flag_column'."""
from __future__ import annotations

import polars as pl

from common.processors.action_engine.actions_multi import (
    action_anti_join,
    action_cross_validate,
    action_delta_detection,
    action_exception_extract,
    action_merge_join,
    action_reconciliation,
    action_snapshot_diff,
    action_union,
)
from common.processors.action_engine.models import OutputSpec


def test_merge_join_inner():
    left = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).lazy()
    right = pl.DataFrame({"user_id": [1, 2], "balance": [10, 20]}).lazy()
    config = {"type": "inner", "on": [{"left": "id", "right": "user_id"}]}
    result = action_merge_join([left, right], config, None).collect()
    assert result.height == 2
    assert "balance" in result.columns


def test_union_all_and_distinct():
    a = pl.DataFrame({"id": [1, 2]}).lazy()
    b = pl.DataFrame({"id": [2, 3]}).lazy()
    all_result = action_union([a, b], {"type": "union_all"}, None).collect()
    assert all_result.height == 4
    distinct_result = action_union([a, b], {"type": "union"}, None).collect()
    assert distinct_result.height == 3


def test_anti_join_replace_mode_returns_only_unmatched():
    left = pl.DataFrame({"id": [1, 2, 3]}).lazy()
    right = pl.DataFrame({"id": [2]}).lazy()
    result = action_anti_join([left, right], {"on": ["id"]}, None).collect()
    assert sorted(result["id"].to_list()) == [1, 3]


def test_anti_join_flag_column_keeps_all_rows():
    left = pl.DataFrame({"id": [1, 2, 3]}).lazy()
    right = pl.DataFrame({"id": [2]}).lazy()
    output = OutputSpec(mode="flag_column", target_column="missing_in_target")
    result = action_anti_join([left, right], {"on": ["id"]}, output).collect().sort("id")
    assert result.height == 3
    assert result["missing_in_target"].to_list() == [True, False, True]


def test_exception_extract_default_returns_exceptions_only():
    df = pl.DataFrame({"id": [1, 2, 3], "amount": [-5, 10, -1]}).lazy()
    config = {"condition": {"column": "amount", "op": "lt", "value": 0}, "output_mode": "exceptions_only"}
    result = action_exception_extract([df], config, None).collect()
    assert sorted(result["id"].to_list()) == [1, 3]


def test_exception_extract_flag_column_keeps_all_rows():
    df = pl.DataFrame({"id": [1, 2, 3], "amount": [-5, 10, -1]}).lazy()
    config = {"condition": {"column": "amount", "op": "lt", "value": 0}}
    output = OutputSpec(mode="flag_column", target_column="is_exception")
    result = action_exception_extract([df], config, output).collect().sort("id")
    assert result.height == 3
    assert result["is_exception"].to_list() == [True, False, True]


def test_reconciliation_default_column_name_and_statuses():
    src = pl.DataFrame({"payment_id": [1, 2, 3], "amount": [100.0, 50.0, 75.0]}).lazy()
    tgt = pl.DataFrame({"payment_id": [1, 2, 4], "amount": [100.0, 999.0, 1.0]}).lazy()
    config = {"keys": ["payment_id"], "compare_columns": ["amount"], "tolerance": 0.01}
    result = action_reconciliation([src, tgt], config, None).collect().sort("payment_id")
    statuses = dict(zip(result["payment_id"].to_list(), result["recon_status"].to_list()))
    assert statuses[1] == "MATCHED"
    assert statuses[2] == "MISMATCHED"
    assert statuses[3] == "MISSING_IN_TARGET"
    assert statuses[4] == "MISSING_IN_SOURCE"


def test_reconciliation_custom_target_column_via_output_spec():
    src = pl.DataFrame({"payment_id": [1], "amount": [100.0]}).lazy()
    tgt = pl.DataFrame({"payment_id": [1], "amount": [100.0]}).lazy()
    config = {"keys": ["payment_id"], "compare_columns": ["amount"]}
    output = OutputSpec(mode="flag_column", target_column="payment_recon_status")
    result = action_reconciliation([src, tgt], config, output).collect()
    assert "payment_recon_status" in result.columns
    assert "recon_status" not in result.columns
    assert result["payment_recon_status"].to_list() == ["MATCHED"]


def test_delta_detection_insert_update_delete_unchanged():
    old = pl.DataFrame({"customer_id": [1, 2, 3], "balance": [10, 20, 30]}).lazy()
    new = pl.DataFrame({"customer_id": [1, 2, 4], "balance": [10, 999, 40]}).lazy()
    config = {"keys": ["customer_id"], "compare_columns": ["balance"]}
    result = action_delta_detection([old, new], config, None).collect().sort("customer_id")
    changes = dict(zip(result["customer_id"].to_list(), result["change_type"].to_list()))
    assert changes[1] == "UNCHANGED"
    assert changes[2] == "UPDATE"
    assert changes[3] == "DELETE"
    assert changes[4] == "INSERT"


def test_snapshot_diff_added_removed_changed_unchanged():
    d1 = pl.DataFrame({"id": [1, 2, 3], "status": ["A", "A", "A"]}).lazy()
    d2 = pl.DataFrame({"id": [1, 2, 4], "status": ["A", "B", "A"]}).lazy()
    config = {"keys": ["id"], "compare_columns": ["status"]}
    result = action_snapshot_diff([d1, d2], config, None).collect().sort("id")
    changes = dict(zip(result["id"].to_list(), result["change_status"].to_list()))
    assert changes[1] == "UNCHANGED"
    assert changes[2] == "CHANGED"
    assert changes[3] == "REMOVED"
    assert changes[4] == "ADDED"


def test_cross_validate_pass_fail():
    orders = pl.DataFrame({"order_id": [1, 2], "order_total": [100.0, 50.0]}).lazy()
    payments = pl.DataFrame({"order_id": [1, 2], "payment_total": [100.0, 999.0]}).lazy()
    config = {
        "keys": ["order_id"],
        "rules": [{"left_column": "order_total", "right_column": "payment_total", "op": "eq", "tolerance": 0.01}],
    }
    result = action_cross_validate([orders, payments], config, None).collect().sort("order_id")
    assert result["validation_status"].to_list() == ["PASS", "FAIL"]
