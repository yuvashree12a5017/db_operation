"""Explicitly project the final dataset into caller-declared target columns."""
from __future__ import annotations

from functools import reduce
from operator import add, mul

import polars as pl

from common.processors.action_engine.models import TargetColumnMapping, TargetOperation
from common.processors.action_engine.polars_utils import validate_identifier

_DTYPES = {
    "string": pl.String,
    "integer": pl.Int64,
    "float": pl.Float64,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime,
}


def apply_target_mappings(
    frame: pl.LazyFrame, mappings: list[TargetColumnMapping]
) -> pl.DataFrame:
    available = set(frame.collect_schema().names())
    expressions: list[pl.Expr] = []
    for mapping in mappings:
        required = _required_columns(mapping)
        missing = required - available
        if missing:
            raise ValueError(
                f"Target column '{mapping.target_column}' references missing columns: {sorted(missing)}"
            )
        expression = _mapping_expression(mapping).alias(mapping.target_column)
        expressions.append(expression)
    result = frame.select(expressions).collect()
    for mapping in mappings:
        if not mapping.nullable and result[mapping.target_column].null_count():
            raise ValueError(f"Target column '{mapping.target_column}' is non-nullable but contains nulls")
    return result


def _required_columns(mapping: TargetColumnMapping) -> set[str]:
    if mapping.source_column:
        return {mapping.source_column}
    if mapping.operation:
        return set(mapping.operation.columns)
    return set()


def _mapping_expression(mapping: TargetColumnMapping) -> pl.Expr:
    if mapping.source_column is not None:
        return pl.col(validate_identifier(mapping.source_column))
    if mapping.operation is not None:
        return _operation_expression(mapping.operation)
    return pl.lit(mapping.literal)


def _operation_expression(operation: TargetOperation) -> pl.Expr:
    columns = [pl.col(validate_identifier(name)) for name in operation.columns]
    if operation.type == "concat":
        return pl.concat_str([column.cast(pl.String) for column in columns], separator=operation.separator)
    if operation.type == "coalesce":
        return pl.coalesce(columns)
    if operation.type == "add":
        return reduce(add, columns)
    if operation.type == "subtract":
        return columns[0] - columns[1]
    if operation.type == "multiply":
        return reduce(mul, columns)
    if operation.type == "divide":
        return columns[0] / columns[1]
    if operation.type == "upper":
        return columns[0].cast(pl.String).str.to_uppercase()
    if operation.type == "lower":
        return columns[0].cast(pl.String).str.to_lowercase()
    if operation.type == "trim":
        return columns[0].cast(pl.String).str.strip_chars()
    if operation.type == "cast":
        return columns[0].cast(_DTYPES[operation.data_type], strict=True)
    raise ValueError(f"Unsupported target operation: '{operation.type}'")
