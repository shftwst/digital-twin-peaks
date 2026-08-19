from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import AuditRecord, OutboxRecord, ScenarioState
from enterprise_twins.common.events.contracts import EventEnvelope
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError
from enterprise_twins.common.ids import new_id


def record_audit(
    session: AsyncSession,
    *,
    epoch: str,
    action: str,
    resource_type: str,
    resource_id: str,
    actor_id: str,
    correlation_id: str,
    occurred_at: datetime,
    details: dict[str, Any],
) -> AuditRecord:
    record = AuditRecord(
        audit_id=new_id("aud"),
        scenario_epoch=epoch,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_id=actor_id,
        correlation_id=correlation_id,
        occurred_at=occurred_at,
        details=details,
    )
    session.add(record)
    return record


def record_event(
    session: AsyncSession,
    *,
    epoch: str,
    event_type: str,
    source: str,
    subject: str,
    resource_version: int,
    correlation_id: str,
    causation_id: str,
    occurred_at: datetime,
    recorded_at: datetime,
    data: dict[str, Any],
) -> EventEnvelope:
    envelope = EventEnvelope(
        eventId=new_id("evt"),
        eventType=event_type,
        source=source,
        subject=subject,
        resourceVersion=resource_version,
        correlationId=correlation_id,
        causationId=causation_id,
        occurredAt=occurred_at,
        recordedAt=recorded_at,
        data=data,
    )
    session.add(
        OutboxRecord(
            event_id=envelope.event_id,
            scenario_epoch=epoch,
            event_type=event_type,
            envelope=envelope.model_dump(mode="json", by_alias=True),
            published=False,
            publish_attempts=0,
        )
    )
    return envelope


class OutboxDispatcher:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        relay: RelayClient,
    ) -> None:
        self.factory = factory
        self.relay = relay

    async def run_once(self) -> int:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1)
            if state is None or state.mode != "active":
                return 0
            record = await session.scalar(
                select(OutboxRecord)
                .where(
                    OutboxRecord.scenario_epoch == state.active_epoch,
                    OutboxRecord.published.is_(False),
                )
                .order_by(OutboxRecord.event_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if record is None:
                return 0
            record.publish_attempts += 1
            try:
                await self.relay.ingest(EventEnvelope.model_validate(record.envelope))
            except ApiError, httpx.HTTPError:
                return 0
            record.published = True
            record.published_at = datetime.now(UTC)
            return 1
