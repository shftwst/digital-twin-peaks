import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

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


class Dispatcher(Protocol):
    async def run_once(self) -> int:
        raise NotImplementedError


class DispatcherSupervisor:
    def __init__(
        self,
        dispatcher: Dispatcher,
        *,
        interval_seconds: float = 0.05,
        freshness_seconds: float = 2.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.dispatcher = dispatcher
        self.interval_seconds = interval_seconds
        self.freshness_seconds = freshness_seconds
        self.monotonic = monotonic
        self.task: asyncio.Task[None] | None = None
        self.last_heartbeat: float | None = None
        self.last_iteration_failed = False

    def start(self) -> asyncio.Task[None]:
        if self.task is not None and not self.task.done():
            raise RuntimeError("dispatcher supervisor is already running")
        self.last_heartbeat = None
        self.last_iteration_failed = False
        self.task = asyncio.create_task(self.run())
        return self.task

    async def run(self) -> None:
        while True:
            try:
                await self.dispatcher.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.last_iteration_failed = True
            else:
                self.last_iteration_failed = False
                self.last_heartbeat = self.monotonic()
            await asyncio.sleep(self.interval_seconds)

    def is_ready(self) -> bool:
        task = self.task
        heartbeat = self.last_heartbeat
        return (
            task is not None
            and not task.done()
            and not self.last_iteration_failed
            and heartbeat is not None
            and self.monotonic() - heartbeat <= self.freshness_seconds
        )

    async def stop(self) -> None:
        task = self.task
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
