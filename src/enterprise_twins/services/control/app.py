from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings


class ControlStatus:
    def __init__(self, repository: ControlRepository) -> None:
        self.repository = repository

    async def current_epoch(self) -> str:
        return (await self.repository.state()).active_epoch

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.repository.state()
            await self.repository.now()
        except RuntimeError:
            return False, {"database": "not_ready", "clock": "not_ready"}
        ready = state.mode == "active"
        return ready, {"database": "ready", "clock": "ready", "scenario": state.mode}


def create_control_app(
    factory: async_sessionmaker[AsyncSession], settings: ControlSettings
) -> FastAPI:
    repository = ControlRepository(factory)
    return create_app(
        "Twin Control",
        ("scenario:reset", "time:write", "faults:write", "diagnostics:read"),
        ControlStatus(repository),
        (control_router(repository, settings),),
    )
