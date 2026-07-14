"""Small in-process metrics adapter compatible with the Genesis call pattern."""
from __future__ import annotations

import logging
from collections import defaultdict

from fastapi import FastAPI

logger = logging.getLogger("genesis.db_to_db_codex.metrics")
OPERATION_LATENCY: dict[str, list[int]] = defaultdict(list)


def mount_metrics(app: FastAPI) -> None:
    logger.info("Metrics initialized")


def record_action_latency(action: str, duration_ms: int) -> None:
    OPERATION_LATENCY[action].append(duration_ms)


def record_success(name: str) -> None:
    logger.info("metric=success name=%s", name)


def record_error(name: str) -> None:
    logger.info("metric=error name=%s", name)


def record_batch(name: str, records: int) -> None:
    logger.info("metric=batch name=%s records=%s", name, records)
