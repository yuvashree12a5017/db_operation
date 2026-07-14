import polars as pl
import pytest

from common.processors.action_engine.models import TargetColumnMapping
from common.processors.action_engine.target_mapper import apply_target_mappings


def test_maps_renames_literals_and_operations():
    frame = pl.DataFrame(
        {
            "id": [1],
            "first": ["Ada"],
            "last": ["Lovelace"],
            "amount": [10.0],
            "credit": [2.5],
        }
    ).lazy()
    mappings = [
        TargetColumnMapping(target_column="customer_id", source_column="id", nullable=False),
        TargetColumnMapping(
            target_column="full_name",
            operation={"type": "concat", "columns": ["first", "last"], "separator": " "},
        ),
        TargetColumnMapping(
            target_column="total", operation={"type": "add", "columns": ["amount", "credit"]}
        ),
        TargetColumnMapping(target_column="source_system", literal="codex"),
    ]

    result = apply_target_mappings(frame, mappings)

    assert result.columns == ["customer_id", "full_name", "total", "source_system"]
    assert result.row(0) == (1, "Ada Lovelace", 12.5, "codex")


def test_missing_mapping_source_fails_before_write():
    mapping = TargetColumnMapping(target_column="customer_id", source_column="missing")
    with pytest.raises(ValueError, match="missing columns"):
        apply_target_mappings(pl.DataFrame({"id": [1]}).lazy(), [mapping])


def test_non_nullable_mapping_rejects_nulls():
    mapping = TargetColumnMapping(target_column="customer_id", source_column="id", nullable=False)
    with pytest.raises(ValueError, match="non-nullable"):
        apply_target_mappings(pl.DataFrame({"id": [None]}).lazy(), [mapping])
