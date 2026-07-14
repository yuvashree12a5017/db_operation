"""FastAPI service, endpoint, middleware, lifespan, orchestration (section 19).

Exposes POST /export/dbtodb and GET /health. Auth, security middleware,
correlation, rate limiting, audit, and metrics follow the Genesis framework
alignment pattern.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request

from common.audit import setup_audit
from common.auth import verify_jwt_or_basic
from common.correlation_middleware import add_correlation_middleware, get_correlation_id
from common.creds import resolve_connection_url
from common.metrics import mount_metrics
from common.processors.action_engine.engine import ActionExecutionEngine
from common.processors.action_engine.models import DbToDbRequest, DbToDbResponse
from common.rate_limit import STRICT_RATE_LIMIT, add_rate_limiting, limiter
from common.security_middleware import add_cors_middleware, add_request_size_limit


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_audit(app)
    mount_metrics(app)
    yield


app = FastAPI(
    title="Genesis DB-to-DB Action Engine",
    description=(
        "Reads datasets from multiple heterogeneous relational databases, "
        "executes a declarative action plan -- including per-action "
        "target-column directives -- and writes the materialized result "
        "into a target database."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

add_request_size_limit(app)
add_cors_middleware(app)
add_correlation_middleware(app)
add_rate_limiting(app)


@app.get("/health")
async def health() -> dict:
    return {"status": "OK"}


@app.post("/export/dbtodb", response_model=DbToDbResponse)
@limiter.limit(STRICT_RATE_LIMIT)
async def export_dbtodb(
    request: Request,
    payload: DbToDbRequest,
    user: str = Depends(verify_jwt_or_basic),
) -> DbToDbResponse:
    resolved_source_urls = {
        source.name: resolve_connection_url(source.connection.sqlalchemy_url)
        for source in payload.sources
    }
    engine = ActionExecutionEngine(correlation_id=get_correlation_id())
    return await engine.run(payload, resolved_source_urls)
