"""Contract-level tests for the universal output.target_column feature."""
from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from common.processors.action_engine.engine import ActionExecutionEngine
from common.processors.action_engine.models import ActionEnvelope, OutputSpec


def test_output_spec_requires_target_column_in_flag_mode():
    with pytest.raises(ValidationError):
        OutputSpec(mode="flag_column")


def test_output_spec_replace_mode_needs_no_target_column():
    spec = OutputSpec(mode="replace")
    assert spec.target_column is None


def test_action_envelope_rejects_unknown_output_field():
    with pytest.raises(ValidationError):
        ActionEnvelope(
            action="action-filter-dataset",
            name="x",
            input=["src"],
            config={"condition": {"column": "a", "op": "eq", "value": 1}},
            output={"mode": "flag_column", "target_column": "flag", "bogus": True},
        )


def test_engine_rejects_flag_column_for_unsupported_action():
    engine = ActionExecutionEngine(correlation_id="test")
    engine._datasets["src"] = pl.DataFrame({"id": [1, 2]}).lazy()
    action = ActionEnvelope(
        action="action-sort-dataset",
        name="sorted",
        input=["src"],
        config={"columns": ["id"]},
        output=OutputSpec(mode="flag_column", target_column="whatever"),
    )
    with pytest.raises(ValueError, match="does not support output.mode='flag_column'"):
        engine._run_action(action, sequence_number=0, include_audit=False)


def test_engine_accepts_flag_column_for_supported_action():
    engine = ActionExecutionEngine(correlation_id="test")
    engine._datasets["src"] = pl.DataFrame({"id": [1, 2], "status": ["A", "B"]}).lazy()
    action = ActionEnvelope(
        action="action-filter-dataset",
        name="flagged",
        input=["src"],
        config={"condition": {"column": "status", "op": "eq", "value": "A"}},
        output=OutputSpec(mode="flag_column", target_column="is_a"),
    )
    engine._run_action(action, sequence_number=0, include_audit=False)
    result = engine._datasets["flagged"].collect()
    assert result.height == 2
    assert result["is_a"].to_list() == [True, False]
