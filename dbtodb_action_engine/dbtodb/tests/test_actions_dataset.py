"""Unit tests for the dataset-level actions, including output.mode='flag_column'."""
from __future__ import annotations

import polars as pl
import pytest

from common.processors.action_engine.actions_dataset import (
    action_deduplicate,
    action_filter_dataset,
    action_format_convert,
    action_row_hash,
    action_schema_validate,
    action_sort_dataset,
)
from common.processors.action_engine.models import OutputSpec


def _users():
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "status": ["ACTIVE", "INACTIVE", "ACTIVE"],
            "balance": [1500, 200, 999],
        }
    ).lazy()


def test_filter_dataset_replace_mode_drops_rows():
    condition = {"column": "status", "op": "eq", "value": "ACTIVE"}
    result = action_filter_dataset([_users()], {"condition": condition}, None).collect()
    assert result["id"].to_list() == [1, 3]


def test_filter_dataset_flag_column_keeps_all_rows_and_adds_flag():
    condition = {"column": "status", "op": "eq", "value": "ACTIVE"}
    output = OutputSpec(mode="flag_column", target_column="is_active")
    result = action_filter_dataset([_users()], {"condition": condition}, output).collect()
    assert result.height == 3
    assert result["is_active"].to_list() == [True, False, True]


def test_sort_dataset():
    result = action_sort_dataset(
        [_users()], {"columns": ["balance"], "order": "desc"}, None
    ).collect()
    assert result["id"].to_list() == [1, 3, 2]


def test_deduplicate():
    df = pl.DataFrame({"id": [1, 1, 2], "v": ["a", "b", "c"]}).lazy()
    result = action_deduplicate([df], {"subset": ["id"], "keep": "first"}, None).collect()
    assert result.height == 2


def test_row_hash_adds_target_column_from_config():
    result = action_row_hash(
        [_users()],
        {"columns": ["id", "status"], "target_column": "row_hash"},
        None,
    ).collect()
    assert "row_hash" in result.columns
    assert result["row_hash"].null_count() == 0


def test_row_hash_output_target_column_overrides_config():
    output = OutputSpec(mode="flag_column", target_column="hash_override")
    result = action_row_hash(
        [_users()],
        {"columns": ["id", "status"], "target_column": "row_hash"},
        output,
    ).collect()
    assert "hash_override" in result.columns
    assert "row_hash" not in result.columns


def test_schema_validate_replace_mode_raises_on_missing():
    with pytest.raises(ValueError):
        action_schema_validate(
            [_users()], {"schema": {"nonexistent": "string"}, "fail_on_error": True}, None
        )


def test_schema_validate_flag_column_never_raises_and_reports_detail():
    output = OutputSpec(mode="flag_column", target_column="schema_check")
    result = action_schema_validate(
        [_users()], {"schema": {"nonexistent": "string"}, "fail_on_error": True}, output
    ).collect()
    assert result.height == 3
    assert set(result["schema_check"].to_list()) == {"MISSING:nonexistent"}


def test_format_convert_replace_mode_casts_in_place():
    df = pl.DataFrame({"amount": ["1", "2", "3"]}).lazy()
    result = action_format_convert(
        [df], {"columns": [{"name": "amount", "type": "integer"}]}, None
    ).collect()
    assert result["amount"].dtype == pl.Int64


def test_format_convert_flag_column_writes_new_column():
    df = pl.DataFrame({"amount": ["1", "2", "3"]}).lazy()
    output = OutputSpec(mode="flag_column", target_column="amount_int")
    result = action_format_convert(
        [df], {"columns": [{"name": "amount", "type": "integer"}]}, output
    ).collect()
    assert result["amount"].dtype == pl.Utf8
    assert result["amount_int"].dtype == pl.Int64


def test_format_convert_flag_column_rejects_multiple_columns():
    df = pl.DataFrame({"a": ["1"], "b": ["2"]}).lazy()
    output = OutputSpec(mode="flag_column", target_column="x")
    config = {
        "columns": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "integer"},
        ]
    }
    with pytest.raises(ValueError):
        action_format_convert([df], config, output)
