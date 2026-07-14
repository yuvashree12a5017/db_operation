"""Structured audit logging hooks; replace with Genesis Kafka audit in deployment."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI

logger = logging.getLogger("genesis.db_to_db_codex.audit")


def setup_audit(app: FastAPI) -> None:
    logger.info("DB-to-DB audit subsystem initialized")


def publish_audit_event(event: str, **metadata: Any) -> None:
    logger.info("audit_event=%s metadata=%s", event, metadata)
