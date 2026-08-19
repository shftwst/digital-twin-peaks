from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.faults import FaultRepository, fault_router
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings


class ControlStatus:
    def __init__(self, repository: ControlRepository) -> None:
        self.repository = repository

    async def current_epoch(self) -> str:
        try:
            return (await self.repository.state()).active_epoch
        except (OSError, RuntimeError, SQLAlchemyError):  # fmt: skip
            return "none"

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.repository.state()
            await self.repository.now()
        except (OSError, RuntimeError, SQLAlchemyError):  # fmt: skip
            return False, {"database": "not_ready", "clock": "not_ready"}
        ready = state.mode == "active"
        return ready, {"database": "ready", "clock": "ready", "scenario": state.mode}


def create_control_app(
    factory: async_sessionmaker[AsyncSession], settings: ControlSettings
) -> FastAPI:
    repository = ControlRepository(factory)
    faults = FaultRepository(factory)
    return create_app(
        "Twin Control",
        ("scenario:reset", "time:write", "faults:write", "diagnostics:read"),
        ControlStatus(repository),
        (control_router(repository, settings), fault_router(faults, settings)),
    )
