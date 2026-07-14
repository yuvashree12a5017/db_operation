"""Request correlation ID propagation."""
from __future__ import annotations

import contextvars
import uuid

from fastapi import FastAPI, Request

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")


def add_correlation_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation(request: Request, call_next):
        value = request.headers.get("X-Correlation-Id") or str(uuid.uuid4())
        _correlation_id.set(value)
        response = await call_next(request)
        response.headers["X-Correlation-Id"] = value
        return response


def get_correlation_id() -> str:
    return _correlation_id.get() or str(uuid.uuid4())
