"""ActionExecutionEngine and dataset store (section 19: engine.py).

Sequentially executes a validated action plan against a dict[str, LazyFrame]
dataset store, publishing audit/metrics events and mapping failures to HTTP
status codes. Validates `action.output` (the target-column directive)
before dispatch: `output.mode="flag_column"` is only accepted for actions
listed in FLAG_COLUMN_SUPPORTED_ACTIONS.
"""
from __future__ import annotations

import time
from typing import Dict, List

import polars as pl
from fastapi import HTTPException

from common.audit import publish_action_audit, publish_operation_audit
from common.metrics import record_action_latency
from common.processors.action_engine.db_loader import SourceLoader
from common.processors.action_engine.db_sink import TargetWriter
from common.processors.action_engine.models import ActionEnvelope, DbToDbRequest, DbToDbResponse
from common.processors.action_engine.registry import get_action, is_implemented, supports_flag_column


class ActionExecutionEngine:
    """Sequentially executes a validated action plan against named Polars datasets."""

    def __init__(self, correlation_id: str):
        self._datasets: Dict[str, pl.LazyFrame] = {}
        self._correlation_id = correlation_id

    async def run(self, request: DbToDbRequest, resolved_source_urls: Dict[str, str]) -> DbToDbResponse:
        start = time.monotonic()
        loaders: List[SourceLoader] = []

        try:
            for source in request.sources:
                loader = SourceLoader(resolved_source_urls[source.name])
                loaders.append(loader)
                self._datasets[source.name] = await loader.load(source)

            for sequence_number, action in enumerate(request.actions):
                self._run_action(action, sequence_number, request.options.include_action_audit)

            final_name = request.options.final_dataset or request.actions[-1].name
            if final_name not in self._datasets:
                raise KeyError(final_name)
            final_df = self._datasets[final_name].collect()

            records_written = TargetWriter().write(final_df, request.target)

            duration_ms = int((time.monotonic() - start) * 1000)
            publish_operation_audit(
                "OPERATION_COMPLETED",
                correlation_id=self._correlation_id,
                node_runid=request.node_runId,
                records_written=records_written,
                duration_ms=duration_ms,
                final_dataset=final_name,
            )
            return DbToDbResponse(
                records_written=records_written,
                duration_ms=duration_ms,
                target_table=request.target.table,
                source_count=len(request.sources),
                action_count=len(request.actions),
                final_dataset=final_name,
            )
        except HTTPException:
            raise
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid dataset reference: {exc}") from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            publish_operation_audit(
                "OPERATION_FAILED",
                correlation_id=self._correlation_id,
                node_runid=request.node_runId,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise HTTPException(status_code=500, detail=f"Operation failed: {exc}") from exc
        finally:
            for loader in loaders:
                await loader.aclose()

    def _run_action(self, action: ActionEnvelope, sequence_number: int, include_audit: bool) -> None:
        if not is_implemented(action.action):
            raise NotImplementedError(
                f"'{action.action}' is registered but not implemented in this build"
            )

        if action.output and action.output.mode == "flag_column" and not supports_flag_column(action.action):
            raise ValueError(
                f"action '{action.name}' ({action.action}) does not support "
                "output.mode='flag_column'"
            )

        func = get_action(action.action)
        try:
            inputs = [self._datasets[name] for name in action.input]
        except KeyError as exc:
            raise KeyError(f"action '{action.name}' references unknown dataset {exc}") from exc

        if include_audit:
            publish_action_audit(
                "ACTION_STARTED",
                action=action.action,
                action_name=action.name,
                input=action.input,
                sequence_number=sequence_number,
                correlation_id=self._correlation_id,
            )

        start = time.monotonic()
        try:
            output = func(inputs, action.config, action.output)
        except Exception as exc:
            if include_audit:
                publish_action_audit(
                    "ACTION_FAILED",
                    action=action.action,
                    action_name=action.name,
                    failed_stage=action.name,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    correlation_id=self._correlation_id,
                )
            raise

        duration_ms = int((time.monotonic() - start) * 1000)
        record_action_latency(action.action, duration_ms)
        self._datasets[action.name] = output
        if include_audit:
            publish_action_audit(
                "ACTION_COMPLETED",
                action=action.action,
                action_name=action.name,
                output_dataset=action.name,
                duration_ms=duration_ms,
                correlation_id=self._correlation_id,
            )
