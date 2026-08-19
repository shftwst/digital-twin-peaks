from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ResetRequest
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.faults import FaultRepository, fault_router
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.reset import (
    ControlResetStore,
    DirectoryBundleLoader,
    HttpParticipantClient,
    ParticipantClient,
    ResetCoordinator,
    reset_router,
)
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


def build_control_app(
    factory: async_sessionmaker[AsyncSession],
    settings: ControlSettings,
    coordinator: ResetCoordinator,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    repository = ControlRepository(factory)
    faults = FaultRepository(factory)
    return create_app(
        "Twin Control",
        ("scenario:reset", "time:write", "faults:write", "diagnostics:read"),
        ControlStatus(repository),
        (
            control_router(repository, settings),
            fault_router(faults, settings),
            reset_router(coordinator, repository, settings),
        ),
        lifespan,
    )


def create_from_env() -> FastAPI:
    settings = ControlSettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    participants: dict[str, ParticipantClient] = {
        name: HttpParticipantClient(url, settings.participant_token, http_client)
        for name, url in settings.participants.items()
    }
    store = ControlResetStore(factory)
    coordinator = ResetCoordinator(
        participants,
        DirectoryBundleLoader(settings.scenario_root),
        store.begin,
        store.commit,
        store.fail,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        repository = ControlRepository(factory)
        try:
            await repository.state()
        except RuntimeError:
            await coordinator.reset(
                ResetRequest(
                    scenarioId=settings.bootstrap_scenario,
                    version=settings.bootstrap_version,
                )
            )
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return build_control_app(factory, settings, coordinator, lifespan)
