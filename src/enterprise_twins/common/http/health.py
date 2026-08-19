from typing import Protocol

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.responses import Response


class RuntimeStatus(Protocol):
    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        raise NotImplementedError


def health_router(status: RuntimeStatus) -> APIRouter:
    router = APIRouter()

    @router.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @router.get("/health/ready", response_model=None)
    async def ready() -> Response:
        is_ready, checks = await status.readiness()
        body = {"status": "ready" if is_ready else "not_ready", "checks": checks}
        return JSONResponse(body, status_code=200 if is_ready else 503)

    return router
