import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress

import httpx
import jwt
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.audit import DatabaseAuthDecisionRecorder
from enterprise_twins.common.auth.scenario import ScenarioAccess
from enterprise_twins.common.auth.verifier import BearerAuthenticator, JwtVerifier
from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.events.publisher import OutboxDispatcher
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError
from enterprise_twins.services.identity.api import identity_router
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.repository import IdentityControl, IdentityRepository
from enterprise_twins.services.identity.settings import IdentitySettings


class IdentityStatus:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        control: IdentityControl,
    ) -> None:
        self.factory = factory
        self.control = control

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("identity scenario is not initialised")
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
        return True, checks


def create_identity_app(
    factory: async_sessionmaker[AsyncSession],
    settings: IdentitySettings,
    control: IdentityControl,
    relay: RelayClient | None = None,
    verification_http: httpx.AsyncClient | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    issuer = TokenIssuer(
        settings.issuer,
        settings.audience,
        settings.signing_seed,
        settings.token_ttl_seconds,
    )
    jwt_http = verification_http or httpx.AsyncClient()
    verifier = JwtVerifier(
        settings.issuer,
        settings.audience,
        f"{settings.issuer}/.well-known/jwks.json",
        control,
        jwt_http,
    )
    verifier.keys[issuer.kid] = jwt.PyJWK.from_dict(issuer.public_jwk, algorithm="EdDSA")
    authenticator = BearerAuthenticator(
        verifier,
        DatabaseAuthDecisionRecorder("identity", factory, control),
    )
    repository = IdentityRepository(factory, settings, issuer, control)
    return create_app(
        "Identity twin",
        ("tokens:issue", "webhooks:manage"),
        IdentityStatus(factory, control),
        (
            identity_router(
                repository,
                issuer,
                settings,
                authenticator,
                relay,
                ScenarioAccess("identity", factory),
            ),
        ),
        lifespan,
    )


def create_from_env() -> FastAPI:
    settings = IdentitySettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    relay = RelayClient(settings.relay_url, "identity", settings.relay_token, http_client)
    dispatcher = OutboxDispatcher(factory, relay)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        async def dispatch() -> None:
            while True:
                try:
                    await dispatcher.run_once()
                except SQLAlchemyError:
                    pass
                await asyncio.sleep(0.05)

        task = asyncio.create_task(dispatch())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await http_client.aclose()
            await engine.dispose()

    return create_identity_app(
        factory,
        settings,
        control,
        relay,
        http_client,
        lifespan,
    )
