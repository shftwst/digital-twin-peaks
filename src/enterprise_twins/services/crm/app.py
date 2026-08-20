from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.audit import DatabaseAuthDecisionRecorder
from enterprise_twins.common.auth.scenario import ScenarioAccess
from enterprise_twins.common.auth.verifier import BearerAuthenticator, JwtVerifier
from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.events.publisher import DispatcherSupervisor, OutboxDispatcher
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError
from enterprise_twins.services.crm.api import crm_router
from enterprise_twins.services.crm.repository import CustomerRepository
from enterprise_twins.services.crm.service import CrmControl, CrmService
from enterprise_twins.services.crm.settings import CrmSettings


class CrmStatus:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        control: CrmControl,
        *,
        dispatcher: DispatcherSupervisor | None = None,
        relay: RelayClient | None = None,
    ) -> None:
        self.factory = factory
        self.control = control
        self.dispatcher = dispatcher
        self.relay = relay

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("CRM scenario is not initialised")
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
        checks = {"database": "ready", "scenario": state.mode, "control": "not_ready"}
        if state.mode != "active":
            return False, checks
        try:
            control_epoch = await self.control.ready_epoch()
        except ApiError, OSError, RuntimeError:
            return False, checks
        if control_epoch != state.active_epoch:
            checks["control"] = "epoch_mismatch"
            return False, checks
        checks["control"] = "ready"
        if self.dispatcher is not None and self.relay is not None:
            checks["dispatcher"] = "ready" if self.dispatcher.is_ready() else "not_ready"
            checks["relay"] = "not_ready"
            try:
                relay_epoch = await self.relay.ready_epoch()
            except ApiError, OSError, RuntimeError:
                return False, checks
            if relay_epoch != state.active_epoch:
                checks["relay"] = "epoch_mismatch"
                return False, checks
            checks["relay"] = "ready"
            if checks["dispatcher"] != "ready":
                return False, checks
        return True, checks


def create_crm_app(
    factory: async_sessionmaker[AsyncSession],
    settings: CrmSettings,
    control: CrmControl,
    verifier: JwtVerifier,
    relay: RelayClient | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
    dispatcher: DispatcherSupervisor | None = None,
) -> FastAPI:
    authenticator = BearerAuthenticator(
        verifier,
        DatabaseAuthDecisionRecorder("crm", factory, control),
    )
    repository = CustomerRepository(factory, settings.cursor_secret)
    service = CrmService(factory, control)
    return create_app(
        "CRM twin",
        ("crm:read", "crm:notes:write", "webhooks:manage"),
        CrmStatus(
            factory,
            control,
            dispatcher=dispatcher,
            relay=relay if dispatcher is not None else None,
        ),
        (
            crm_router(
                repository,
                service,
                authenticator,
                relay,
                ScenarioAccess("CRM", factory),
            ),
        ),
        lifespan,
    )


def create_from_env() -> FastAPI:
    settings = CrmSettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    verifier = JwtVerifier(
        settings.identity_issuer_aliases,
        settings.identity_audience,
        settings.identity_jwks_url,
        control,
        http_client,
    )
    relay = RelayClient(settings.relay_url, "crm", settings.relay_token, http_client)
    dispatcher = OutboxDispatcher(factory, relay)
    supervisor = DispatcherSupervisor(dispatcher)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        supervisor.start()
        try:
            yield
        finally:
            await supervisor.stop()
            await http_client.aclose()
            await engine.dispose()

    return create_crm_app(
        factory,
        settings,
        control,
        verifier,
        relay,
        lifespan,
        supervisor,
    )
