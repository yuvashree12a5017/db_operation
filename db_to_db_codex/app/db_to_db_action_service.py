"""Genesis DB-to-DB Codex FastAPI service."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

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
    title="Genesis DB-to-DB Federated Action Engine - Codex",
    description="Federated dataset actions with mandatory explicit target-column mappings.",
    version="2.0.0",
    lifespan=lifespan,
)
add_request_size_limit(app)
add_cors_middleware(app)
add_correlation_middleware(app)
add_rate_limiting(app)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=400, content={"detail": exc.errors()})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "OK", "version": "2.0.0"}


@app.post("/export/dbtodb", response_model=DbToDbResponse)
@limiter.limit(STRICT_RATE_LIMIT)
async def export_dbtodb(
    request: Request,
    payload: DbToDbRequest,
    user: str = Depends(verify_jwt_or_basic),
) -> DbToDbResponse:
    source_urls = {
        source.name: resolve_connection_url(source.connection.sqlalchemy_url)
        for source in payload.sources
    }
    resolved_target = payload.target.model_copy(
        update={
            "connection": payload.target.connection.model_copy(
                update={"sqlalchemy_url": resolve_connection_url(payload.target.connection.sqlalchemy_url)}
            )
        }
    )
    resolved_payload = payload.model_copy(update={"target": resolved_target})
    return await ActionExecutionEngine(get_correlation_id()).run(resolved_payload, source_urls)
