from datetime import UTC, datetime

import pytest
from sqlalchemy import String, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base
from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.db.records import AuditRecord, IdempotencyRecord, OutboxRecord
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.errors import ApiError


async def count(factory: async_sessionmaker[AsyncSession], model: type[object]) -> int:
    async with factory() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


class ProbeSourceRecord(Base):
    __tablename__ = "probe_source_records"

    probe_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)


@pytest.mark.asyncio
async def test_same_idempotency_input_replays_original_result(
    db: async_sessionmaker[AsyncSession],
) -> None:
    calls = 0
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "crm.note.create", "idem-1")

    async def work() -> StoredResponse:
        nonlocal calls
        calls += 1
        return StoredResponse(201, {"noteId": "note_1"}, {"ETag": '"1"'})

    async with db.begin() as session:
        first, first_replay = await run_idempotent(
            session, "epoch_1", namespace, {"body": "VIP"}, work
        )
    async with db.begin() as session:
        second, second_replay = await run_idempotent(
            session, "epoch_1", namespace, {"body": "VIP"}, work
        )

    assert first == second
    assert first_replay is False
    assert second_replay is True
    assert calls == 1


@pytest.mark.asyncio
async def test_changed_input_under_same_key_is_conflict(
    db: async_sessionmaker[AsyncSession],
) -> None:
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "crm.note.create", "idem-2")

    async def work() -> StoredResponse:
        return StoredResponse(201, {"noteId": "note_2"}, {})

    async with db.begin() as session:
        await run_idempotent(session, "epoch_1", namespace, {"body": "first"}, work)
    with pytest.raises(ApiError) as raised:
        async with db.begin() as session:
            await run_idempotent(session, "epoch_1", namespace, {"body": "changed"}, work)
    assert raised.value.status_code == 409
    assert raised.value.details == {"operation": "crm.note.create"}


@pytest.mark.asyncio
async def test_source_audit_idempotency_and_event_commit_together(
    db: async_sessionmaker[AsyncSession],
) -> None:
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "probe.create", "idem-3")

    async def work(session: AsyncSession) -> StoredResponse:
        session.add(ProbeSourceRecord(probe_id="probe_1", name="probe"))
        record_audit(
            session,
            epoch="epoch_1",
            action="probe.created",
            resource_type="probe",
            resource_id="probe_1",
            actor_id="actor_test",
            correlation_id="case-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            details={},
        )
        record_event(
            session,
            epoch="epoch_1",
            event_type="probe.created",
            source="probe",
            subject="probe/probe_1",
            resource_version=1,
            correlation_id="case-1",
            causation_id="req-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            recorded_at=datetime(2026, 8, 19, 10, 1, tzinfo=UTC),
            data={"probeId": "probe_1"},
        )
        return StoredResponse(201, {"probeId": "probe_1"}, {})

    async with db.begin() as session:
        result, replay = await run_idempotent(
            session,
            "epoch_1",
            namespace,
            {"name": "probe"},
            lambda: work(session),
        )

    assert result == StoredResponse(201, {"probeId": "probe_1"}, {})
    assert replay is False
    assert await count(db, ProbeSourceRecord) == 1
    assert await count(db, AuditRecord) == 1
    assert await count(db, OutboxRecord) == 1
    assert await count(db, IdempotencyRecord) == 1
    async with db() as session:
        outbox = await session.scalar(select(OutboxRecord))
    assert outbox is not None
    assert outbox.envelope["recordedAt"] == "2026-08-19T10:01:00Z"


@pytest.mark.asyncio
async def test_source_audit_idempotency_and_event_roll_back_together(
    db: async_sessionmaker[AsyncSession],
) -> None:
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "probe.create", "idem-3")

    async def work(session: AsyncSession) -> StoredResponse:
        session.add(ProbeSourceRecord(probe_id="probe_1", name="probe"))
        record_audit(
            session,
            epoch="epoch_1",
            action="probe.created",
            resource_type="probe",
            resource_id="probe_1",
            actor_id="actor_test",
            correlation_id="case-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            details={},
        )
        record_event(
            session,
            epoch="epoch_1",
            event_type="probe.created",
            source="probe",
            subject="probe/probe_1",
            resource_version=1,
            correlation_id="case-1",
            causation_id="req-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            recorded_at=datetime(2026, 8, 19, 10, 1, tzinfo=UTC),
            data={"probeId": "probe_1"},
        )
        return StoredResponse(201, {"probeId": "probe_1"}, {})

    with pytest.raises(RuntimeError, match="force rollback"):
        async with db.begin() as session:
            await run_idempotent(
                session,
                "epoch_1",
                namespace,
                {"name": "probe"},
                lambda: work(session),
            )
            raise RuntimeError("force rollback")

    assert await count(db, ProbeSourceRecord) == 0
    assert await count(db, AuditRecord) == 0
    assert await count(db, OutboxRecord) == 0
    assert await count(db, IdempotencyRecord) == 0
