"""Correlation ID middleware (section 16: correlation_id required in audit metadata).

Standalone placeholder for the shared Genesis correlation middleware.
"""
from __future__ import annotations

import contextvars
import uuid

from fastapi import FastAPI, Request

_correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def add_correlation_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _set_correlation_id(request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-Id", str(uuid.uuid4()))
        _correlation_id_var.set(correlation_id)
        response = await call_next(request)
        response.headers["X-Correlation-Id"] = correlation_id
        return response


def get_correlation_id() -> str:
    return _correlation_id_var.get() or str(uuid.uuid4())
