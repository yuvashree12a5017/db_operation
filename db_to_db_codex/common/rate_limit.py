"""Rate-limiting configuration."""
from fastapi import FastAPI
from slowapi import Limiter
from slowapi.util import get_remote_address

STRICT_RATE_LIMIT = "30/minute"
limiter = Limiter(key_func=get_remote_address)


def add_rate_limiting(app: FastAPI) -> None:
    app.state.limiter = limiter
