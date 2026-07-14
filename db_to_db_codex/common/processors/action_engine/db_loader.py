"""Structured, async SQLAlchemy source loading into Polars."""
from __future__ import annotations

import polars as pl
from sqlalchemy import MetaData, Table, select
from sqlalchemy.ext.asyncio import create_async_engine

from common.processors.action_engine.models import FilterCondition, SourceConfig
from common.processors.action_engine.polars_utils import validate_identifier


class SourceLoader:
    def __init__(self, resolved_url: str):
        self._engine = create_async_engine(resolved_url)

    async def load(self, source: SourceConfig, max_rows: int | None = None) -> pl.LazyFrame:
        schema = validate_identifier(source.dataset.schema_) if source.dataset.schema_ else None
        table_name = validate_identifier(source.dataset.table)
        async with self._engine.connect() as connection:
            metadata = MetaData(schema=schema)
            table: Table = await connection.run_sync(
                lambda sync: Table(table_name, metadata, autoload_with=sync)
            )
            columns = (
                [table.c[validate_identifier(name)] for name in source.dataset.columns]
                if source.dataset.columns else list(table.c)
            )
            query = select(*columns)
            for condition in source.dataset.filters or []:
                query = query.where(_sql_filter(table, condition))
            if max_rows:
                query = query.limit(max_rows)
            result = await connection.execute(query)
            rows = result.fetchall()
        data = {column.name: [row[index] for row in rows] for index, column in enumerate(columns)}
        return pl.DataFrame(data).lazy()

    async def aclose(self) -> None:
        await self._engine.dispose()


def _sql_filter(table: Table, condition: FilterCondition):
    column = table.c[validate_identifier(condition.column)]
    value = condition.value
    operations = {
        "eq": lambda: column == value,
        "neq": lambda: column != value,
        "gt": lambda: column > value,
        "gte": lambda: column >= value,
        "lt": lambda: column < value,
        "lte": lambda: column <= value,
        "in": lambda: column.in_(value),
        "not_in": lambda: column.not_in(value),
        "is_null": lambda: column.is_(None),
        "is_not_null": lambda: column.is_not(None),
    }
    return operations[condition.op]()
