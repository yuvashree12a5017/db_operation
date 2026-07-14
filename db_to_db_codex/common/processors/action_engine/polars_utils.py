"""Validated Polars expressions shared by actions and target mapping."""
from __future__ import annotations

import hashlib
import re
from functools import reduce
from operator import and_, or_
from typing import Any

import polars as pl

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: '{name}'")
    return name


def build_condition_expr(condition: dict[str, Any]) -> pl.Expr:
    if set(condition) == {"and"}:
        if not condition["and"]:
            raise ValueError("'and' condition must not be empty")
        return reduce(and_, (build_condition_expr(item) for item in condition["and"]))
    if set(condition) == {"or"}:
        if not condition["or"]:
            raise ValueError("'or' condition must not be empty")
        return reduce(or_, (build_condition_expr(item) for item in condition["or"]))
    allowed = {"column", "op", "value"}
    if not set(condition) <= allowed or not {"column", "op"} <= set(condition):
        raise ValueError("Invalid structured condition")
    column = pl.col(validate_identifier(condition["column"]))
    value = condition.get("value")
    operations = {
        "eq": lambda: column == value,
        "neq": lambda: column != value,
        "gt": lambda: column > value,
        "gte": lambda: column >= value,
        "lt": lambda: column < value,
        "lte": lambda: column <= value,
        "in": lambda: column.is_in(value),
        "not_in": lambda: ~column.is_in(value),
        "is_null": column.is_null,
        "is_not_null": column.is_not_null,
    }
    if condition["op"] not in operations:
        raise ValueError(f"Unsupported condition operator: '{condition['op']}'")
    return operations[condition["op"]]()


def row_hash_expr(columns: list[str], algo: str = "sha256") -> pl.Expr:
    if algo != "sha256":
        raise ValueError(f"Unsupported hash algorithm: '{algo}'")
    values = [pl.col(validate_identifier(column)).cast(pl.String).fill_null("") for column in columns]
    return pl.concat_str(values, separator="|").map_elements(
        lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest(),
        return_dtype=pl.String,
    )
