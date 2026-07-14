"""Action registry plus strict action-specific configuration validation."""
from __future__ import annotations

from typing import Any, Callable, Literal

from pydantic import Field, model_validator

from common.processors.action_engine.models import MVP_ACTIONS, StrictModel


class ActionCondition(StrictModel):
    column: str
    op: Literal["eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "is_not_null"]
    value: Any | None = None


class ActionConditionGroup(StrictModel):
    and_: list["Condition"] | None = Field(default=None, alias="and")
    or_: list["Condition"] | None = Field(default=None, alias="or")

    @model_validator(mode="after")
    def exactly_one_group(self):
        if (self.and_ is None) == (self.or_ is None):
            raise ValueError("Condition group requires exactly one of 'and' or 'or'")
        values = self.and_ if self.and_ is not None else self.or_
        if not values:
            raise ValueError("Condition group cannot be empty")
        return self


Condition = ActionCondition | ActionConditionGroup
ActionConditionGroup.model_rebuild()


class FilterConfig(StrictModel):
    condition: Condition


class SortConfig(StrictModel):
    columns: list[str] = Field(min_length=1)
    order: Literal["asc", "desc"] = "asc"
    nulls_last: bool = True


class DeduplicateConfig(StrictModel):
    subset: list[str] = Field(min_length=1)
    keep: Literal["first", "last", "any", "none"] = "first"


class Metric(StrictModel):
    column: str
    agg: Literal["sum", "min", "max", "mean", "median", "count", "n_unique", "first", "last"]
    alias: str | None = None


class AggregateConfig(StrictModel):
    group_by: list[str]
    metrics: list[Metric] = Field(min_length=1)


class SampleConfig(StrictModel):
    limit: int | None = Field(default=None, ge=1)
    sample_fraction: float | None = Field(default=None, gt=0, le=1)
    seed: int | None = None

    @model_validator(mode="after")
    def one_mode(self):
        if (self.limit is None) == (self.sample_fraction is None):
            raise ValueError("Specify exactly one of limit or sample_fraction")
        return self


class SchemaValidateConfig(StrictModel):
    schema_: dict[str, str] = Field(alias="schema")
    fail_on_error: bool = True
    error_column: str | None = None


class RowHashConfig(StrictModel):
    columns: list[str] = Field(min_length=1)
    target_column: str
    algo: Literal["sha256"] = "sha256"


class JoinPair(StrictModel):
    left: str
    right: str


class MergeJoinConfig(StrictModel):
    type: Literal["inner", "left", "right", "outer", "full"]
    on: list[JoinPair] = Field(min_length=1)
    suffix: str = "_right"


class UnionConfig(StrictModel):
    type: Literal["union", "union_all"]
    align_schema: bool = False


class AntiJoinConfig(StrictModel):
    on: list[Any] = Field(min_length=1)


class FlattenConfig(StrictModel):
    column: str
    prefix: str = ""
    drop_original: bool = False


class ExplodeConfig(StrictModel):
    column: str


class PivotConfig(StrictModel):
    mode: Literal["pivot", "unpivot"]
    index: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    values: list[str] = Field(default_factory=list)
    agg: str | None = None


class FormatColumn(StrictModel):
    name: str
    type: str
    format: str | None = None
    strict: bool = True


class FormatConfig(StrictModel):
    columns: list[FormatColumn] = Field(min_length=1)


class StagingConfig(StrictModel):
    schema_: str | None = Field(default=None, alias="schema")
    table: str
    mode: Literal["append", "overwrite"] = "overwrite"


class QuarantineConfig(StrictModel):
    error_column: str
    schema_: str | None = Field(default=None, alias="schema")
    table: str
    include_valid_output: bool = True


class DriftConfig(StrictModel):
    mode: Literal["add_missing_columns", "drop_extra_columns", "fail"]
    expected_schema: dict[str, str]
    default_value: Any | None = None


class CompareConfig(StrictModel):
    keys: list[str] = Field(min_length=1)
    compare_columns: list[str] | None = None
    tolerance: float | None = None


class ExceptionConfig(StrictModel):
    condition: Condition
    output_mode: str


class CrossValidateConfig(StrictModel):
    keys: list[str]
    rules: list[dict[str, Any]]


class AggregateMultiConfig(StrictModel):
    union_type: Literal["union", "union_all"]
    group_by: list[str]
    metrics: list[Metric]


class RollingJoinConfig(StrictModel):
    on: str
    right_on: str | None = None
    by: str | list[str] | None = None
    strategy: Literal["backward", "forward", "nearest"] = "backward"
    tolerance: str | int | float | None = None


ACTION_CONFIG_MODELS = {
    "action-filter-dataset": FilterConfig,
    "action-sort-dataset": SortConfig,
    "action-deduplicate": DeduplicateConfig,
    "action-aggregate": AggregateConfig,
    "action-sample-limit": SampleConfig,
    "action-flatten-json": FlattenConfig,
    "action-explode-array": ExplodeConfig,
    "action-pivot-unpivot": PivotConfig,
    "action-format-convert": FormatConfig,
    "action-schema-validate": SchemaValidateConfig,
    "action-row-hash": RowHashConfig,
    "action-write-staging": StagingConfig,
    "action-error-quarantine": QuarantineConfig,
    "action-handle-drift": DriftConfig,
    "action-merge-join": MergeJoinConfig,
    "action-union": UnionConfig,
    "action-reconciliation": CompareConfig,
    "action-delta-detection": CompareConfig,
    "action-exception-extract": ExceptionConfig,
    "action-cross-validate": CrossValidateConfig,
    "action-aggregate-multi": AggregateMultiConfig,
    "action-rolling-join": RollingJoinConfig,
    "action-anti-join": AntiJoinConfig,
    "action-snapshot-diff": CompareConfig,
}


def validate_action_config(action: str, config: dict[str, Any]) -> dict[str, Any]:
    return ACTION_CONFIG_MODELS[action].model_validate(config).model_dump(
        exclude_none=True, by_alias=True
    )


def _registry() -> dict[str, Callable]:
    from common.processors.action_engine import actions_dataset as ds
    from common.processors.action_engine import actions_multi as multi

    return {
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


def get_action(action: str) -> Callable:
    return _registry()[action]


def is_implemented(action: str) -> bool:
    return action in MVP_ACTIONS
