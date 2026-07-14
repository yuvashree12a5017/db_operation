"""Metrics recorder for the db-to-db action service (section 16).

Standalone placeholder for the shared Genesis common.metrics module.
"""
from __future__ import annotations

import logging
from typing import Dict, List

from fastapi import FastAPI

logger = logging.getLogger("genesis.dbtodb.metrics")

OPERATION_LATENCY: Dict[str, List[int]] = {}


def mount_metrics(app: FastAPI) -> None:
    logger.info("Metrics mounted for db-to-db action service")


def record_action_latency(action: str, duration_ms: int) -> None:
    OPERATION_LATENCY.setdefault(action, []).append(duration_ms)


def record_success(name: str) -> None:
    logger.info("metric=success name=%s", name)


def record_error(name: str) -> None:
    logger.info("metric=error name=%s", name)
