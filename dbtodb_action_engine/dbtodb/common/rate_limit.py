"""Rate limiting via the existing Genesis slowapi pattern (section 18).

Standalone placeholder for common.rate_limit.
"""
from __future__ import annotations

from fastapi import FastAPI
from slowapi import Limiter
from slowapi.util import get_remote_address

STRICT_RATE_LIMIT = "30/minute"

limiter = Limiter(key_func=get_remote_address)


def add_rate_limiting(app: FastAPI) -> None:
    app.state.limiter = limiter
