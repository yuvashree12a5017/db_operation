"""Strict request, action, target mapping, and response contracts."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


def _identifier(value: str, field: str = "identifier") -> str:
    import re

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {field}: '{value}'")
    return value


class ConnectionConfig(StrictModel):
    sqlalchemy_url: str = Field(min_length=1)


class FilterCondition(StrictModel):
    column: str
    op: Literal["eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "is_not_null"]
    value: Any | None = None


class DatasetSpec(StrictModel):
    schema_: str | None = Field(default=None, alias="schema")
    table: str
    columns: list[str] | None = None
    filters: list[FilterCondition] | None = None
    batch_size: int = Field(default=10_000, ge=1)

    @model_validator(mode="after")
    def validate_identifiers(self):
        if self.schema_:
            _identifier(self.schema_, "schema")
        _identifier(self.table, "table")
        for column in self.columns or []:
            _identifier(column, "column")
        return self


DatabaseType = Literal["postgres", "oracle", "sqlserver", "mysql", "generic"]


class SourceConfig(StrictModel):
    name: str
    type: DatabaseType
    connection: ConnectionConfig
    dataset: DatasetSpec

    @model_validator(mode="after")
    def validate_name(self):
        _identifier(self.name, "source name")
        return self


class TargetOperation(StrictModel):
    """Safe operation evaluated in Polars; no expression text or SQL is accepted."""

    type: Literal[
        "concat", "coalesce", "add", "subtract", "multiply", "divide",
        "upper", "lower", "trim", "cast",
    ]
    columns: list[str] = Field(min_length=1)
    separator: str = ""
    data_type: Literal["string", "integer", "float", "boolean", "date", "datetime"] | None = None

    @model_validator(mode="after")
    def validate_shape(self):
        for column in self.columns:
            _identifier(column, "operation column")
        unary = {"upper", "lower", "trim", "cast"}
        binary = {"subtract", "divide"}
        if self.type in unary and len(self.columns) != 1:
            raise ValueError(f"Operation '{self.type}' requires exactly one column")
        if self.type in binary and len(self.columns) != 2:
            raise ValueError(f"Operation '{self.type}' requires exactly two columns")
        if self.type == "cast" and not self.data_type:
            raise ValueError("cast requires data_type")
        if self.type != "cast" and self.data_type is not None:
            raise ValueError("data_type is only valid for cast")
        return self


class TargetColumnMapping(StrictModel):
    target_column: str
    source_column: str | None = None
    literal: Any | None = None
    operation: TargetOperation | None = None
    nullable: bool = True

    @model_validator(mode="before")
    @classmethod
    def validate_one_provider(cls, data: Any):
        if isinstance(data, dict):
            providers = sum(key in data for key in ("source_column", "literal", "operation"))
            if providers != 1:
                raise ValueError("Specify exactly one of source_column, literal, or operation")
        return data

    @model_validator(mode="after")
    def validate_identifiers(self):
        _identifier(self.target_column, "target column")
        if self.source_column:
            _identifier(self.source_column, "source column")
        return self


class TargetConfig(StrictModel):
    type: DatabaseType
    connection: ConnectionConfig
    schema_: str | None = Field(default=None, alias="schema")
    table: str
    write_mode: Literal["append", "overwrite", "merge"]
    merge_keys: list[str] | None = None
    create_if_missing: bool = False
    column_mappings: list[TargetColumnMapping] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_target(self):
        if self.schema_:
            _identifier(self.schema_, "target schema")
        _identifier(self.table, "target table")
        names = [mapping.target_column for mapping in self.column_mappings]
        if len(names) != len(set(names)):
            raise ValueError("target.column_mappings contains duplicate target_column values")
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError("merge_keys is required when write_mode is 'merge'")
        for key in self.merge_keys or []:
            _identifier(key, "merge key")
            if key not in names:
                raise ValueError(f"Merge key '{key}' is not present in column_mappings")
        return self


class ExecutionOptions(StrictModel):
    parallel_load: bool = True
    max_rows_per_source: int | None = Field(default=None, ge=1)
    fail_mode: Literal["fail_fast", "tolerant"] = "fail_fast"
    collect_metrics: bool = True
    include_action_audit: bool = True
    final_dataset: str | None = None


ActionName = Literal[
    "action-filter-dataset", "action-sort-dataset", "action-deduplicate",
    "action-aggregate", "action-sample-limit", "action-flatten-json",
    "action-explode-array", "action-pivot-unpivot", "action-format-convert",
    "action-schema-validate", "action-row-hash", "action-write-staging",
    "action-error-quarantine", "action-handle-drift", "action-merge-join",
    "action-union", "action-reconciliation", "action-delta-detection",
    "action-exception-extract", "action-cross-validate", "action-aggregate-multi",
    "action-rolling-join", "action-anti-join", "action-snapshot-diff",
]

MVP_ACTIONS = {
    "action-filter-dataset", "action-sort-dataset", "action-deduplicate",
    "action-aggregate", "action-sample-limit", "action-merge-join", "action-union",
    "action-anti-join", "action-row-hash", "action-schema-validate",
}


class ActionEnvelope(StrictModel):
    action: ActionName
    name: str
    input: list[str] = Field(min_length=1)
    config: dict[str, Any]

    @model_validator(mode="after")
    def validate_envelope(self):
        _identifier(self.name, "action name")
        for item in self.input:
            _identifier(item, "action input")
        from common.processors.action_engine.registry import validate_action_config

        self.config = validate_action_config(self.action, self.config)
        return self


class DbToDbRequest(StrictModel):
    version: Literal["1.0"]
    sources: list[SourceConfig] = Field(min_length=1)
    actions: list[ActionEnvelope] = Field(min_length=1)
    target: TargetConfig
    options: ExecutionOptions = Field(default_factory=ExecutionOptions)
    audit_message: dict[str, Any] | None = None
    node_runId: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_plan(self):
        source_names = [source.name for source in self.sources]
        action_names = [action.name for action in self.actions]
        if len(source_names) != len(set(source_names)):
            raise ValueError("Source names must be unique")
        if len(action_names) != len(set(action_names)):
            raise ValueError("Action names must be unique")
        if set(source_names) & set(action_names):
            raise ValueError("Action names cannot shadow source names")
        available = set(source_names)
        for action in self.actions:
            missing = set(action.input) - available
            if missing:
                raise ValueError(f"Action '{action.name}' references unavailable datasets: {sorted(missing)}")
            available.add(action.name)
        final_name = self.options.final_dataset or self.actions[-1].name
        if final_name not in available:
            raise ValueError(f"final_dataset '{final_name}' is unavailable")
        return self


class DbToDbResponse(StrictModel):
    status: Literal["OK"] = "OK"
    records_written: int
    duration_ms: int
    target_table: str
    source_count: int
    action_count: int
    final_dataset: str
    mapped_columns: list[str]
    failed_stage: str | None = None
