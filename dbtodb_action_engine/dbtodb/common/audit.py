"""Audit event publisher for the db-to-db action service (section 16).

Standalone placeholder for the shared Genesis common.audit module. If the
real Genesis common package is available in the deployment environment,
replace this import target with it instead.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI

logger = logging.getLogger("genesis.dbtodb.audit")


def setup_audit(app: FastAPI) -> None:
    logger.info("Audit subsystem initialized for db-to-db action service")


def publish_action_audit(event: str, **metadata: Any) -> None:
    logger.info("audit_event=%s metadata=%s", event, metadata)


def publish_operation_audit(event: str, **metadata: Any) -> None:
    logger.info("audit_event=%s metadata=%s", event, metadata)
