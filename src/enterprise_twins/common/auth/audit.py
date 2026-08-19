from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import TokenClock
from enterprise_twins.common.events.publisher import record_audit
from enterprise_twins.common.http.context import current_request


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
        now = await self.control.now()
        epoch = await self.control.current_epoch()
        context = current_request.get()
        correlation_id = context.correlation_id if context else "uncorrelated"
        actor_id = principal.subject if principal is not None else "anonymous"
        async with self.factory.begin() as session:
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
