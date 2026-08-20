from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.services.relay.models import (
    Delivery,
    DeliveryAttempt,
    SourceEvent,
    Subscription,
)


class RelayScenarioLoader:
    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        if payload.get("schemaVersion") != "1":
            raise ValueError('Relay schemaVersion must be "1"')
        subscriptions = payload.get("subscriptions", [])
        if subscriptions:
            raise ValueError("platform-contracts Relay seed must start without subscriptions")
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"subscriptions": 0, "events": 0, "deliveries": 0, "attempts": 0},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(
            delete(DeliveryAttempt).where(DeliveryAttempt.scenario_epoch == epoch)
        )
        await session.execute(delete(Delivery).where(Delivery.scenario_epoch == epoch))
        await session.execute(delete(SourceEvent).where(SourceEvent.scenario_epoch == epoch))
        await session.execute(delete(Subscription).where(Subscription.scenario_epoch == epoch))
