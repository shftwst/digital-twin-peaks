from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from enterprise_twins.common.http.errors import ErrorBody, ErrorCode, ErrorEnvelope
from enterprise_twins.common.ids import new_id


@dataclass(frozen=True, slots=True)
class RequestContext:
    request_id: str
    correlation_id: str
    traceparent: str | None


current_request: ContextVar[RequestContext | None] = ContextVar("current_request", default=None)


class RequestContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object, epoch: Callable[[], Awaitable[str]]) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self.epoch = epoch

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = new_id("req")
        correlation_id = request.headers.get("X-Correlation-Id")
        if request.url.path.startswith("/v1/") and not correlation_id:
            body = ErrorEnvelope(
                error=ErrorBody(
                    code=ErrorCode.INVALID_REQUEST,
                    message="X-Correlation-Id is required",
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
        token = current_request.set(context)
        try:
            response = await call_next(request)
        finally:
            current_request.reset(token)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await self.epoch()
        if context.traceparent:
            response.headers["traceparent"] = context.traceparent
        return response
