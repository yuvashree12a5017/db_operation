"""Target and staging write wrappers (section 19: db_sink.py).

Writes a materialized Polars DataFrame to the target database table per the
append/overwrite write_mode contract (section 9). "merge" is not implemented
in this build.
"""
from __future__ import annotations

from typing import Any, Dict

import polars as pl

from common.processors.action_engine.models import ConnectionConfig, TargetConfig
from common.processors.action_engine.polars_utils import validate_identifier


class TargetWriter:
    """Writes a materialized Polars DataFrame to a target database table."""

    def write(self, df: pl.DataFrame, target: TargetConfig) -> int:
        schema = validate_identifier(target.schema_) if target.schema_ else None
        table = validate_identifier(target.table)
        qualified_table = f"{schema}.{table}" if schema else table

        if target.write_mode == "merge":
            raise NotImplementedError("merge write_mode is not implemented in this build")

        if_table_exists = "append" if target.write_mode == "append" else "replace"
        df.write_database(
            table_name=qualified_table,
            connection=target.connection.sqlalchemy_url,
            if_table_exists=if_table_exists,
        )
        return df.height

    def write_staging(self, df: pl.DataFrame, connection_url: str, config: Dict[str, Any]) -> int:
        staging_target = TargetConfig(
            type="generic",
            connection=ConnectionConfig(sqlalchemy_url=connection_url),
            schema=config["schema"],
            table=config["table"],
            write_mode="append" if config.get("mode", "overwrite") == "append" else "overwrite",
            create_if_missing=True,
        )
        return self.write(df, staging_target)
