from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.context import current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.secrets import secret_matches
from enterprise_twins.services.identity.settings import IdentitySettings


class IdentityControl(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def ready_epoch(self) -> str:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class TokenResult:
    access_token: str
    token_id: str
    scopes: list[str]
    expires_in: int


class IdentityRepository:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: IdentitySettings,
        issuer: TokenIssuer,
        control: IdentityControl,
    ) -> None:
        self.factory = factory
        self.settings = settings
        self.issuer = issuer
        self.control = control

    async def record_denial(
        self,
        client_id: str,
        now: datetime,
        epoch: str,
        *,
        action: str = "identity.authentication.denied",
        actor_id: str | None = None,
        reason: str = "invalid_client_credentials",
    ) -> None:
        context = current_request.get()
        correlation_id = context.correlation_id if context else new_id("corr")
        async with self.factory.begin() as session:
            record_audit(
                session,
                epoch=epoch,
                action=action,
                resource_type="identity_client",
                resource_id=client_id,
                actor_id=actor_id or client_id,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"reason": reason},
            )

    async def authenticate(
        self,
        client_id: str,
        secret: str,
        requested_scopes: list[str],
    ) -> TokenResult:
        decision = await self.control.evaluate_fault(
            FaultProbe(
                targetService="identity",
                operation="identity.token.issue",
                phase=FaultPhase.BEFORE_COMMIT,
                actorId=client_id,
            )
        )
        if decision.effect == FaultEffect.RATE_LIMITED:
            raise ApiError(
                ErrorCode.RATE_LIMITED,
                "token endpoint is rate limited",
                status_code=429,
                retryable=True,
            )
        if decision.effect in {FaultEffect.TEMPORARY_FAILURE, FaultEffect.TIMEOUT}:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "token endpoint is temporarily unavailable",
                status_code=503,
                retryable=True,
            )
        now = await self.control.now()
        epoch = await self.control.current_epoch()
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            if state is None or state.mode != "active" or state.active_epoch != epoch:
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    "identity scenario is not active",
                    status_code=503,
                    retryable=True,
                )
            client = await session.scalar(
                select(IdentityClient).where(
                    IdentityClient.scenario_epoch == epoch,
                    IdentityClient.client_id == client_id,
                    IdentityClient.active.is_(True),
                )
            )
            expected_digest = client.secret_digest if client is not None else "0" * 64
            matches = secret_matches(
                client_id,
                secret,
                self.settings.secret_pepper,
                expected_digest,
            )
            if client is None or not matches:
                await self.record_denial(client_id, now, epoch)
                raise ApiError(
                    ErrorCode.UNAUTHENTICATED,
                    "client credentials are invalid",
                    status_code=401,
                )
            scopes = sorted(set(requested_scopes or client.scopes))
            if not set(scopes).issubset(client.scopes):
                await self.record_denial(
                    client.client_id,
                    now,
                    epoch,
                    action="identity.authorisation.denied",
                    actor_id=client.subject,
                    reason="ungranted_scope",
                )
                raise ApiError(
                    ErrorCode.FORBIDDEN,
                    "requested scope is not granted",
                    status_code=403,
                )
            token, token_id = self.issuer.issue(client, scopes, now, epoch)
            context = current_request.get()
            correlation_id = context.correlation_id if context else token_id
            request_id = context.request_id if context else token_id
            record_audit(
                session,
                epoch=epoch,
                action="identity.token.issued",
                resource_type="identity_client",
                resource_id=client.client_id,
                actor_id=client.subject,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"role": client.role, "tokenId": token_id},
            )
            record_event(
                session,
                epoch=epoch,
                event_type="identity.token.issued",
                source="identity",
                subject=f"identity/{client.subject}",
                resource_version=client.version,
                correlation_id=correlation_id,
                causation_id=request_id,
                occurred_at=now,
                recorded_at=now,
                data={"subject": client.subject, "role": client.role, "tokenId": token_id},
            )
            return TokenResult(token, token_id, scopes, self.settings.token_ttl_seconds)
