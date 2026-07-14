"""Request-size and CORS middleware."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

MAX_REQUEST_BYTES = 5 * 1024 * 1024


def add_request_size_limit(app: FastAPI, max_bytes: int = MAX_REQUEST_BYTES) -> None:
    @app.middleware("http")
    async def limit_body(request: Request, call_next):
        value = request.headers.get("content-length")
        if value:
            try:
                if int(value) > max_bytes:
                    return JSONResponse(status_code=413, content={"detail": "Request body too large"})
            except ValueError:
                return JSONResponse(status_code=400, content={"detail": "Invalid Content-Length"})
        return await call_next(request)


def add_cors_middleware(app: FastAPI) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
