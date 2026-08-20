from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from enterprise_twins.common.http.errors import ErrorBody, ErrorCode, ErrorEnvelope
from enterprise_twins.common.ids import new_id


@dataclass(slots=True)
class RequestContext:
    request_id: str
    correlation_id: str
    traceparent: str | None
    response_epoch: str | None = None


current_request: ContextVar[RequestContext | None] = ContextVar("current_request", default=None)


def bind_response_epoch(epoch: str) -> None:
    context = current_request.get()
    if context is not None:
        context.response_epoch = epoch


class RequestContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object, epoch: Callable[[], Awaitable[str]]) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self.epoch = epoch

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = new_id("req")
        correlation_id = request.headers.get("X-Correlation-Id")
        correlation_error: str | None = None
        if request.url.path.startswith("/v1/") and not correlation_id:
            correlation_error = "X-Correlation-Id is required"
        elif correlation_id is not None and len(correlation_id) > 128:
            correlation_error = "X-Correlation-Id is too long"
        if correlation_error is not None:
            body = ErrorEnvelope(
                error=ErrorBody(
                    code=ErrorCode.INVALID_REQUEST,
                    message=correlation_error,
                    requestId=request_id,
                )
            )
            response: Response = JSONResponse(body.model_dump(mode="json"), status_code=400)
            response.headers["X-Request-Id"] = request_id
            response.headers["X-Scenario-Epoch"] = await self.epoch()
            return response
        context = RequestContext(
            request_id, correlation_id or request_id, request.headers.get("traceparent")
        )
        request.state.request_context = context
        token = current_request.set(context)
        try:
            response = await call_next(request)
        finally:
            current_request.reset(token)
        response.headers["X-Request-Id"] = request_id
        if "X-Scenario-Epoch" not in response.headers:
            response.headers["X-Scenario-Epoch"] = context.response_epoch or await self.epoch()
        if context.traceparent:
            response.headers["traceparent"] = context.traceparent
        return response
