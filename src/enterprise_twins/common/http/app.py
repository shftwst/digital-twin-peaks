from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from enterprise_twins.common.http.context import RequestContextMiddleware, current_request
from enterprise_twins.common.http.errors import ApiError, ErrorBody, ErrorCode, ErrorEnvelope
from enterprise_twins.common.http.health import RuntimeStatus, health_router
from enterprise_twins.common.ids import new_id


def create_app(
    name: str,
    capabilities: Sequence[str],
    status: RuntimeStatus,
    routers: Sequence[APIRouter] = (),
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    app = FastAPI(title=name, version="0.1.0", openapi_version="3.1.0", lifespan=lifespan)
    app.state.capabilities = capabilities
    app.add_middleware(RequestContextMiddleware, epoch=status.current_epoch)
    app.include_router(health_router(status))
    for router in routers:
        app.include_router(router)

    @app.exception_handler(ApiError)
    async def api_error(_request: Request, error: ApiError) -> JSONResponse:
        context = current_request.get()
        request_id = context.request_id if context else new_id("req")
        body = ErrorEnvelope(
            error=ErrorBody(
                code=error.code,
                message=error.message,
                retryable=error.retryable,
                requestId=request_id,
                details=error.details,
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=error.status_code)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await status.current_epoch()
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request: Request, error: RequestValidationError) -> JSONResponse:
        context = current_request.get()
        request_id = context.request_id if context else new_id("req")
        body = ErrorEnvelope(
            error=ErrorBody(
                code=ErrorCode.INVALID_REQUEST,
                message="request validation failed",
                requestId=request_id,
                details={
                    "errors": error.errors(),
                },
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=422)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await status.current_epoch()
        return response

    return app
