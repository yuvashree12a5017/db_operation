"""Sequential action orchestration with optional concurrent source loading."""
from __future__ import annotations

import asyncio
import time

import polars as pl
from fastapi import HTTPException

from common.audit import publish_audit_event
from common.metrics import record_action_latency, record_batch, record_error, record_success
from common.processors.action_engine.db_loader import SourceLoader
from common.processors.action_engine.db_sink import TargetWriter
from common.processors.action_engine.models import ActionEnvelope, DbToDbRequest, DbToDbResponse
from common.processors.action_engine.registry import get_action, is_implemented


class ActionExecutionEngine:
    def __init__(self, correlation_id: str):
        self._datasets: dict[str, pl.LazyFrame] = {}
        self._correlation_id = correlation_id

    async def run(self, request: DbToDbRequest, resolved_source_urls: dict[str, str]) -> DbToDbResponse:
        started = time.monotonic()
        loaders = {source.name: SourceLoader(resolved_source_urls[source.name]) for source in request.sources}
        stage = "source_load"
        publish_audit_event(
            "OPERATION_STARTED", operation_type="dbtodb", source_count=len(request.sources),
            action_count=len(request.actions), target=request.target.table,
            correlation_id=self._correlation_id, node_runId=request.node_runId,
        )
        try:
            async def load(source):
                source_started = time.monotonic()
                frame = await loaders[source.name].load(source, request.options.max_rows_per_source)
                self._datasets[source.name] = frame
                count = frame.select(pl.len()).collect().item()
                publish_audit_event(
                    "SOURCE_READ", source_name=source.name, table=source.dataset.table,
                    record_count=count, duration_ms=int((time.monotonic() - source_started) * 1000),
                    correlation_id=self._correlation_id, node_runId=request.node_runId,
                )

            if request.options.parallel_load:
                await asyncio.gather(*(load(source) for source in request.sources))
            else:
                for source in request.sources:
                    await load(source)

            for sequence, action in enumerate(request.actions):
                stage = action.name
                self._run_action(action, sequence, request)

            final_name = request.options.final_dataset or request.actions[-1].name
            stage = "target_write"
            records, columns = TargetWriter().write(self._datasets[final_name], request.target)
            qualified = f"{request.target.schema_}.{request.target.table}" if request.target.schema_ else request.target.table
            duration = int((time.monotonic() - started) * 1000)
            publish_audit_event(
                "TARGET_WRITTEN", target_table=qualified, write_mode=request.target.write_mode,
                records_written=records, correlation_id=self._correlation_id, node_runId=request.node_runId,
            )
            publish_audit_event(
                "OPERATION_COMPLETED", records_written=records, duration_ms=duration,
                final_dataset=final_name, correlation_id=self._correlation_id, node_runId=request.node_runId,
            )
            if request.options.collect_metrics:
                record_success("dbtodb")
                record_batch("dbtodb", records)
            return DbToDbResponse(
                records_written=records, duration_ms=duration, target_table=qualified,
                source_count=len(request.sources), action_count=len(request.actions),
                final_dataset=final_name, mapped_columns=columns,
            )
        except (ValueError, KeyError, NotImplementedError) as exc:
            self._audit_failure(request, stage, exc)
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            self._audit_failure(request, stage, exc)
            raise HTTPException(status_code=500, detail=f"Operation failed at '{stage}': {exc}") from exc
        finally:
            await asyncio.gather(*(loader.aclose() for loader in loaders.values()), return_exceptions=True)

    def _run_action(self, action: ActionEnvelope, sequence: int, request: DbToDbRequest) -> None:
        if not is_implemented(action.action):
            raise NotImplementedError(f"'{action.action}' is registered but not implemented in this build")
        if request.options.include_action_audit:
            publish_audit_event(
                "ACTION_STARTED", action=action.action, action_name=action.name, input=action.input,
                sequence_number=sequence, correlation_id=self._correlation_id, node_runId=request.node_runId,
            )
        started = time.monotonic()
        output = get_action(action.action)([self._datasets[name] for name in action.input], action.config)
        self._datasets[action.name] = output
        duration = int((time.monotonic() - started) * 1000)
        if request.options.collect_metrics:
            record_action_latency(action.action, duration)
        if request.options.include_action_audit:
            publish_audit_event(
                "ACTION_COMPLETED", action=action.action, action_name=action.name,
                output_dataset=action.name, duration_ms=duration,
                correlation_id=self._correlation_id, node_runId=request.node_runId,
            )

    def _audit_failure(self, request: DbToDbRequest, stage: str, exc: Exception) -> None:
        record_error("dbtodb")
        publish_audit_event(
            "OPERATION_FAILED", failed_stage=stage, error_type=type(exc).__name__,
            error_message=str(exc), correlation_id=self._correlation_id, node_runId=request.node_runId,
        )
