"""Source loading into Polars LazyFrame (section 19: db_loader.py).

Builds a structured, identifier-validated SELECT against the configured
source table/columns/filters and materializes the result as a Polars
LazyFrame, per section 18 ("all schema/table/column identifiers must be
validated before SQLAlchemy query construction").
"""
from __future__ import annotations

import polars as pl
from sqlalchemy import MetaData, Table, select
from sqlalchemy.ext.asyncio import create_async_engine

from common.processors.action_engine.models import FilterCondition, SourceConfig
from common.processors.action_engine.polars_utils import validate_identifier

_SQL_OP_MAP = {
    "eq": lambda col, v: col == v,
    "neq": lambda col, v: col != v,
    "gt": lambda col, v: col > v,
    "gte": lambda col, v: col >= v,
    "lt": lambda col, v: col < v,
    "lte": lambda col, v: col <= v,
    "in": lambda col, v: col.in_(v),
    "not_in": lambda col, v: col.not_in(v),
    "is_null": lambda col, v: col.is_(None),
    "is_not_null": lambda col, v: col.is_not(None),
}


class SourceLoader:
    """Loads a configured source dataset into a Polars LazyFrame via async SQLAlchemy."""

    def __init__(self, resolved_url: str):
        self._engine = create_async_engine(resolved_url)

    async def load(self, source: SourceConfig) -> pl.LazyFrame:
        schema = validate_identifier(source.dataset.schema_)
        table_name = validate_identifier(source.dataset.table)
        columns = source.dataset.columns

        async with self._engine.connect() as conn:
            metadata = MetaData(schema=schema)
            table: Table = await conn.run_sync(
                lambda sync_conn: Table(table_name, metadata, autoload_with=sync_conn)
            )
            cols = [table.c[validate_identifier(c)] for c in columns] if columns else list(table.c)
            query = select(*cols)
            for condition in source.dataset.filters or []:
                query = query.where(_build_sql_filter(table, condition))

            result = await conn.execute(query)
            rows = result.fetchall()
            col_names = [c.name for c in cols]

        data = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
        return pl.DataFrame(data).lazy()

    async def aclose(self) -> None:
        await self._engine.dispose()


def _build_sql_filter(table: Table, condition: FilterCondition):
    column = validate_identifier(condition.column)
    col = table.c[column]
    op = condition.op
    if op not in _SQL_OP_MAP:
        raise ValueError(f"Unsupported operator: '{op}'")
    return _SQL_OP_MAP[op](col, condition.value)
