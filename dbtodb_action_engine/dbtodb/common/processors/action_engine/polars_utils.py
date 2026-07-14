"""Condition parsing, identifier validation, type casting, and detail-column
comparison helpers for the action engine.

Global validation rule: all identifiers must be validated before
SQLAlchemy/Polars query construction, and raw SQL transformation text is
prohibited - only structured filters and typed action configs are accepted.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional

import polars as pl

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_OP_MAP = {
    "eq": lambda c, v: c == v,
    "neq": lambda c, v: c != v,
    "gt": lambda c, v: c > v,
    "gte": lambda c, v: c >= v,
    "lt": lambda c, v: c < v,
    "lte": lambda c, v: c <= v,
    "in": lambda c, v: c.is_in(v),
    "not_in": lambda c, v: ~c.is_in(v),
    "is_null": lambda c, v: c.is_null(),
    "is_not_null": lambda c, v: c.is_not_null(),
}

_TYPE_MAP = {
    "string": pl.Utf8,
    "int": pl.Int64,
    "integer": pl.Int64,
    "float": pl.Float64,
    "decimal": pl.Float64,
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "timestamp": pl.Datetime,
}


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: '{name}'")
    return name


def build_condition_expr(condition: Dict[str, Any]) -> pl.Expr:
    """Build a polars expression from a structured condition object.

    Supports a single {"column", "op", "value"} leaf, or {"and": [...]} /
    {"or": [...]} groups of leaves/groups (section 8 FilterCondition shape).
    """
    if "and" in condition:
        exprs = [build_condition_expr(c) for c in condition["and"]]
        expr = exprs[0]
        for e in exprs[1:]:
            expr = expr & e
        return expr
    if "or" in condition:
        exprs = [build_condition_expr(c) for c in condition["or"]]
        expr = exprs[0]
        for e in exprs[1:]:
            expr = expr | e
        return expr

    column = validate_identifier(condition["column"])
    op = condition["op"]
    value = condition.get("value")
    if op not in _OP_MAP:
        raise ValueError(f"Unsupported operator: '{op}'")
    return _OP_MAP[op](pl.col(column), value)


def row_hash_expr(columns: List[str], algo: str = "sha256") -> pl.Expr:
    if algo != "sha256":
        raise ValueError(f"Unsupported hash algorithm: '{algo}'")
    for col in columns:
        validate_identifier(col)

    concat = pl.concat_str(
        [pl.col(c).cast(pl.Utf8).fill_null("") for c in columns], separator="|"
    )
    # Polars has no native sha256, so map_elements with hashlib is used to
    # honor the action-row-hash contract's default algo.
    return concat.map_elements(
        lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest(), return_dtype=pl.Utf8
    )


def cast_expr(column: str, type_name: str, format_: Optional[str] = None, strict: bool = True) -> pl.Expr:
    """Build a cast/parse expression for action-format-convert."""
    dtype = _TYPE_MAP.get(type_name)
    if dtype is None:
        raise ValueError(f"Unsupported type: '{type_name}'")
    col = pl.col(validate_identifier(column))
    if type_name == "date" and format_:
        return col.str.strptime(pl.Date, format_, strict=strict)
    if type_name in ("datetime", "timestamp") and format_:
        return col.str.strptime(pl.Datetime, format_, strict=strict)
    return col.cast(dtype, strict=strict)


def value_mismatch_expr(column: str, tolerance: float = 0.0, right_suffix: str = "_tgt") -> pl.Expr:
    """True where `column` differs between a joined left/right pair.

    Assumes the join produced `column` (left) and `{column}{right_suffix}`
    (right). Nulls on exactly one side always count as a mismatch; nulls on
    both sides count as a match. Used by action-reconciliation,
    action-delta-detection, and action-snapshot-diff.
    """
    left = pl.col(validate_identifier(column))
    right = pl.col(f"{column}{right_suffix}")
    both_null = left.is_null() & right.is_null()
    one_null = left.is_null() != right.is_null()
    if tolerance:
        value_differs = (left - right).abs() > tolerance
    else:
        value_differs = left != right
    return (~both_null) & (one_null | value_differs.fill_null(False))
