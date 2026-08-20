from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from enterprise_twins.common.http.context import (
    RequestContext,
    RequestContextMiddleware,
    current_request,
)
from enterprise_twins.common.http.errors import ApiError, ErrorBody, ErrorCode, ErrorEnvelope
from enterprise_twins.common.http.health import RuntimeStatus, health_router
from enterprise_twins.common.http.openapi import business_openapi
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
    app.openapi = business_openapi(app)  # type: ignore[method-assign]

    def request_context(request: Request) -> RequestContext | None:
        context = current_request.get()
        if context is not None:
            return context
        state_context = getattr(request.state, "request_context", None)
        return state_context if isinstance(state_context, RequestContext) else None

    async def response_epoch(request: Request) -> str:
        context = request_context(request)
        if context is not None and context.response_epoch is not None:
            return context.response_epoch
        return await status.current_epoch()

    @app.exception_handler(ApiError)
    async def api_error(request: Request, error: ApiError) -> JSONResponse:
        context = request_context(request)
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
        response.headers["X-Scenario-Epoch"] = await response_epoch(request)
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        context = request_context(request)
        request_id = context.request_id if context else new_id("req")
        try:
            validation_errors = error.errors(  # type: ignore[call-arg]
                include_url=False, include_context=False, include_input=False
            )
        except TypeError:
            validation_errors = [
                {key: value for key, value in item.items() if key not in {"ctx", "input", "url"}}
                for item in error.errors()
            ]
        body = ErrorEnvelope(
            error=ErrorBody(
                code=ErrorCode.INVALID_REQUEST,
                message="request validation failed",
                requestId=request_id,
                details={
                    "errors": validation_errors,
                },
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=422)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await response_epoch(request)
        return response

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException) -> JSONResponse:
        context = request_context(request)
        request_id = context.request_id if context else new_id("req")
        code_by_status = {
            400: ErrorCode.INVALID_REQUEST,
            401: ErrorCode.UNAUTHENTICATED,
            403: ErrorCode.FORBIDDEN,
            404: ErrorCode.NOT_FOUND,
            409: ErrorCode.CONFLICT,
            412: ErrorCode.PRECONDITION_FAILED,
            429: ErrorCode.RATE_LIMITED,
            503: ErrorCode.TEMPORARILY_UNAVAILABLE,
        }
        code = code_by_status.get(error.status_code, ErrorCode.INTERNAL_ERROR)
        body = ErrorEnvelope(
            error=ErrorBody(
                code=code,
                message=str(error.detail),
                retryable=error.status_code == 429 or error.status_code >= 500,
                requestId=request_id,
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=error.status_code)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await response_epoch(request)
        return response

    @app.exception_handler(Exception)
    async def internal_error(request: Request, _error: Exception) -> JSONResponse:
        context = request_context(request)
        request_id = context.request_id if context else new_id("req")
        body = ErrorEnvelope(
            error=ErrorBody(
                code=ErrorCode.INTERNAL_ERROR,
                message="internal server error",
                requestId=request_id,
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=500)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await response_epoch(request)
        return response

    return app
