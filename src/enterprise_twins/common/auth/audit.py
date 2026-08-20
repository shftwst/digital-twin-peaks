from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import TokenClock
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.publisher import record_audit
from enterprise_twins.common.http.context import bind_response_epoch, current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class DatabaseAuthDecisionRecorder:
    def __init__(
        self,
        service: str,
        factory: async_sessionmaker[AsyncSession],
        control: TokenClock,
    ) -> None:
        self.service = service
        self.factory = factory
        self.control = control

    async def record(
        self,
        principal: Principal | None,
        required_scopes: Sequence[str],
        allowed: bool,
    ) -> None:
        snapshot = await self.control.snapshot()
        now = snapshot.now
        epoch = snapshot.scenario_epoch
        context = current_request.get()
        correlation_id = context.correlation_id if context else "uncorrelated"
        actor_id = principal.subject if principal is not None else "anonymous"
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            if state is not None:
                bind_response_epoch(state.active_epoch)
            if (
                state is None
                or state.mode != "active"
                or state.active_epoch != epoch
                or (principal is not None and principal.scenario_epoch != state.active_epoch)
            ):
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    f"{self.service} scenario is not active",
                    status_code=503,
                    retryable=True,
                )
            record_audit(
                session,
                epoch=epoch,
                action=f"{self.service}.authorisation.{'allowed' if allowed else 'denied'}",
                resource_type="api_request",
                resource_id=context.request_id if context else "unknown",
                actor_id=actor_id,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"requiredScopes": sorted(required_scopes)},
            )
