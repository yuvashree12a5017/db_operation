"""Pydantic request/action/response contracts for the DB-to-DB Action Engine.

Adapted from the original DB-to-DB Federated Action Engine PRD (sections
7-11, 15, 20), extended with a universal `output` directive on every action
envelope (see `OutputSpec` below) so a caller can explicitly choose the
target column an action writes its computed detail/result into, instead of
always replacing the whole downstream dataset.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

ActionName = Literal[
    "action-filter-dataset",
    "action-sort-dataset",
    "action-deduplicate",
    "action-aggregate",
    "action-sample-limit",
    "action-flatten-json",
    "action-explode-array",
    "action-pivot-unpivot",
    "action-format-convert",
    "action-schema-validate",
    "action-row-hash",
    "action-write-staging",
    "action-error-quarantine",
    "action-handle-drift",
    "action-merge-join",
    "action-union",
    "action-reconciliation",
    "action-delta-detection",
    "action-exception-extract",
    "action-cross-validate",
    "action-aggregate-multi",
    "action-rolling-join",
    "action-anti-join",
    "action-snapshot-diff",
]

# Actions fully implemented in this build. Everything else in ActionName is
# registered so the API surface/validation behaves correctly, but calling it
# raises a 400 "not implemented" error.
MVP_ACTIONS = {
    "action-filter-dataset",
    "action-sort-dataset",
    "action-deduplicate",
    "action-aggregate",
    "action-sample-limit",
    "action-format-convert",
    "action-schema-validate",
    "action-row-hash",
    "action-merge-join",
    "action-union",
    "action-anti-join",
    "action-exception-extract",
    "action-reconciliation",
    "action-delta-detection",
    "action-snapshot-diff",
    "action-cross-validate",
}

# Actions that understand output.mode="flag_column": instead of reshaping /
# filtering the dataset, they enrich every input row with a computed detail
# value written into output.target_column and keep all input rows/columns.
FLAG_COLUMN_SUPPORTED_ACTIONS = {
    "action-filter-dataset",
    "action-schema-validate",
    "action-format-convert",
    "action-anti-join",
    "action-exception-extract",
    "action-reconciliation",
    "action-delta-detection",
    "action-snapshot-diff",
    "action-cross-validate",
}


class ConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sqlalchemy_url: str


class FilterCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column: str
    op: Literal["eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "is_not_null"]
    value: Optional[Any] = None


class ConditionGroup(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    and_: Optional[List["Condition"]] = Field(default=None, alias="and")
    or_: Optional[List["Condition"]] = Field(default=None, alias="or")


Condition = Union[FilterCondition, ConditionGroup]
ConditionGroup.model_rebuild()


class DatasetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    schema_: str = Field(alias="schema")
    table: str
    columns: Optional[List[str]] = None
    filters: Optional[List[FilterCondition]] = None
    batch_size: int = 10000


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    type: Literal["postgres", "oracle", "sqlserver", "mysql", "generic"]
    connection: ConnectionConfig
    dataset: DatasetSpec


class TargetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    type: Literal["postgres", "oracle", "sqlserver", "mysql", "generic"]
    connection: ConnectionConfig
    schema_: Optional[str] = Field(default=None, alias="schema")
    table: str
    write_mode: Literal["append", "overwrite", "merge"]
    merge_keys: Optional[List[str]] = None
    create_if_missing: bool = False


class ExecutionOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    parallel_load: bool = True
    max_rows_per_source: Optional[int] = None
    fail_mode: Literal["fail_fast", "tolerant"] = "fail_fast"
    collect_metrics: bool = True
    include_action_audit: bool = True
    final_dataset: Optional[str] = None


class OutputSpec(BaseModel):
    """Where an action's computed result/detail should be written.

    mode="replace" (default) is the original PRD behavior: the action's
    natural output becomes the new dataset named by `ActionEnvelope.name`.

    mode="flag_column" asks the action to instead enrich its input dataset
    in place -- keep every input row and column, and add one new column,
    named `target_column`, holding the action's computed detail for that
    row (e.g. a filter match flag, a reconciliation status, a delta
    change_type, a converted value). Only actions listed in
    `FLAG_COLUMN_SUPPORTED_ACTIONS` accept this mode.
    """

    model_config = ConfigDict(extra="forbid")
    mode: Literal["replace", "flag_column"] = "replace"
    target_column: Optional[str] = None

    @model_validator(mode="after")
    def _require_target_column_for_flag_mode(self) -> "OutputSpec":
        if self.mode == "flag_column" and not self.target_column:
            raise ValueError("output.target_column is required when output.mode='flag_column'")
        return self


class ActionEnvelope(BaseModel):
    """Section 11/20: universal action envelope, dispatched per-action to a typed config."""

    model_config = ConfigDict(extra="forbid")
    action: ActionName
    name: str
    input: List[str]
    config: Dict[str, Any]
    output: Optional[OutputSpec] = None


class DbToDbRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["1.0"]
    sources: List[SourceConfig]
    actions: List[ActionEnvelope]
    target: TargetConfig
    options: ExecutionOptions = ExecutionOptions()
    audit_message: Optional[Dict[str, Any]] = None
    node_runId: str


class DbToDbResponse(BaseModel):
    """Section 15 response contract."""

    status: Literal["OK"] = "OK"
    records_written: int
    duration_ms: int
    target_table: str
    source_count: int
    action_count: int
    final_dataset: str
    failed_stage: Optional[str] = None
