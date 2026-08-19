from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.db.records import AuditRecord, OutboxRecord
from enterprise_twins.common.events.contracts import EventEnvelope
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
        recordedAt=datetime.now(UTC),
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
