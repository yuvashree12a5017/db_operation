"""Final target writer with mandatory explicit column projection."""
from __future__ import annotations

import polars as pl
from sqlalchemy import create_engine, inspect

from common.processors.action_engine.models import TargetConfig
from common.processors.action_engine.polars_utils import validate_identifier
from common.processors.action_engine.target_mapper import apply_target_mappings


class TargetWriter:
    def prepare(self, frame: pl.LazyFrame, target: TargetConfig) -> pl.DataFrame:
        return apply_target_mappings(frame, target.column_mappings)

    def write(self, frame: pl.LazyFrame, target: TargetConfig) -> tuple[int, list[str]]:
        dataframe = self.prepare(frame, target)
        if target.write_mode == "merge":
            raise NotImplementedError("merge write_mode is merge-ready but not implemented in this build")
        schema = validate_identifier(target.schema_) if target.schema_ else None
        table = validate_identifier(target.table)
        qualified = f"{schema}.{table}" if schema else table
        engine = create_engine(target.connection.sqlalchemy_url)
        try:
            if not target.create_if_missing and not inspect(engine).has_table(table, schema=schema):
                raise ValueError(
                    f"Target table '{qualified}' does not exist and create_if_missing is false"
                )
            dataframe.write_database(
                table_name=qualified,
                connection=engine,
                if_table_exists="append" if target.write_mode == "append" else "replace",
            )
        finally:
            engine.dispose()
        return dataframe.height, dataframe.columns
