from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.relay.api import relay_router
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings


class RelayStatus:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("Relay scenario is not initialised")
            return state

    async def current_epoch(self) -> str:
        try:
            return (await self.state()).active_epoch
        except OSError, RuntimeError, SQLAlchemyError:
            return "none"

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.state()
        except OSError, SQLAlchemyError:
            return False, {"database": "not_ready", "scenario": "unavailable"}
        except RuntimeError:
            return False, {"database": "not_ready", "scenario": "uninitialised"}
        return state.mode == "active", {"database": "ready", "scenario": state.mode}


def create_from_env() -> FastAPI:
    settings = RelaySettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    repository = RelayRepository(factory, settings.allowed_targets)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return create_app(
        "Event Relay integration API",
        (),
        RelayStatus(factory),
        (relay_router(repository, control, settings),),
        lifespan,
    )
