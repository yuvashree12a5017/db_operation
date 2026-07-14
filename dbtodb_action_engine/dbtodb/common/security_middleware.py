"""Request size limiting and CORS (section 18 security requirements).

Standalone placeholder for common.security_middleware.
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

MAX_REQUEST_BYTES = 5 * 1024 * 1024


def add_request_size_limit(app: FastAPI, max_bytes: int = MAX_REQUEST_BYTES) -> None:
    @app.middleware("http")
    async def _limit_body_size(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > max_bytes:
            return JSONResponse(status_code=413, content={"detail": "Request body too large"})
        return await call_next(request)


def add_cors_middleware(app: FastAPI) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
