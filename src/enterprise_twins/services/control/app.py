import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ResetRequest
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.faults import FaultRepository, fault_router
from enterprise_twins.services.control.repository import (
    ControlRepository,
    ScenarioStateMissingError,
)
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
            state, _now = await self.repository.status_snapshot()
        except (OSError, RuntimeError, SQLAlchemyError):  # fmt: skip
            return False, {"database": "not_ready", "clock": "not_ready"}
        ready = state.mode == "active" and state.pending_epoch is None
        return ready, {"database": "ready", "clock": "ready", "scenario": state.mode}


async def bootstrap_scenario(
    repository: ControlRepository,
    coordinator: ResetCoordinator,
    request: ResetRequest,
    *,
    timeout_seconds: float = 30.0,
    retry_delay_seconds: float = 0.25,
) -> None:
    try:
        state = await repository.state()
    except ScenarioStateMissingError:
        state = None
    if state is not None:
        cleanup_recovery_required = (
            state.mode in {"finalizing", "error"}
            and state.pending_epoch is not None
            and state.active_epoch == state.pending_epoch
        )
        abort_recovery_required = (
            state.mode in {"preparing", "aborting"}
            and state.pending_epoch is not None
            and state.active_epoch != state.pending_epoch
        )
        if not cleanup_recovery_required and not abort_recovery_required:
            return
    else:
        abort_recovery_required = False

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        try:
            if state is None or abort_recovery_required:
                await coordinator.reset(request)
            else:
                await coordinator.recover_pending_cleanup()
            return
        except (ApiError, httpx.TransportError) as error:
            if isinstance(error, ApiError) and not (
                error.code == ErrorCode.TEMPORARILY_UNAVAILABLE
                and error.details.get("phase") in {"cleanup", "abort"}
            ):
                raise
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise
            await asyncio.sleep(min(retry_delay_seconds, remaining))


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
        store.finalize,
        store.pending_cleanup,
        store.finalize_abort,
        store.pending_abort,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        repository = ControlRepository(factory)
        try:
            await bootstrap_scenario(
                repository,
                coordinator,
                ResetRequest(
                    scenarioId=settings.bootstrap_scenario,
                    version=settings.bootstrap_version,
                ),
            )
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return build_control_app(factory, settings, coordinator, lifespan)
