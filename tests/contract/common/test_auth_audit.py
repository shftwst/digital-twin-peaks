# ruff: noqa: S106

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.audit import DatabaseAuthDecisionRecorder
from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.control.contracts import ClockValue
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import AuditRecord, ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode

NOW = datetime(2026, 8, 19, 10, tzinfo=UTC)


class Control:
    def __init__(
        self,
        *,
        snapshot_time: datetime = NOW,
        snapshot_epoch: str = "epoch_old",
    ) -> None:
        self.snapshot_time = snapshot_time
        self.snapshot_epoch = snapshot_epoch

    async def snapshot(self) -> ClockValue:
        return ClockValue(now=self.snapshot_time, scenarioEpoch=self.snapshot_epoch)

    async def now(self) -> datetime:
        return datetime(2026, 8, 18, 9, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return self.snapshot_epoch


class UnusedLoader:
    async def load(
        self,
        _session: AsyncSession,
        _epoch: str,
        _payload: dict[str, Any],
    ) -> dict[str, object]:
        raise AssertionError("load is not used by reset prepare")

    async def discard(self, _session: AsyncSession, _epoch: str) -> None:
        raise AssertionError("discard is not used by reset prepare")


async def wait_for_lock_waiters(
    db: async_sessionmaker[AsyncSession],
    expected: int,
    tasks: list[asyncio.Task[object]],
) -> bool:
    for _ in range(100):
        async with db() as session:
            waiters = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            )
        if waiters is not None and waiters >= expected:
            return True
        if any(task.done() for task in tasks):
            return False
    return False


@asynccontextmanager
async def advisory_gate(
    db: async_sessionmaker[AsyncSession],
    lock_id: int,
) -> AsyncIterator[None]:
    async with db() as session:
        await session.begin()
        await session.execute(text(f"SELECT pg_advisory_xact_lock({lock_id})"))
        try:
            yield
        finally:
            await session.commit()


@pytest.mark.asyncio
async def test_reset_prepare_waits_for_the_complete_auth_audit_transaction(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_auth_audit_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7401); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_auth_audit_insert "
                "BEFORE INSERT ON audit_records FOR EACH ROW "
                "EXECUTE FUNCTION gate_auth_audit_insert()"
            )
        )
    recorder = DatabaseAuthDecisionRecorder("crm", db, Control())
    participant = ResetParticipant(db, UnusedLoader(), service="crm")

    async with advisory_gate(db, 7401):
        audit_task = asyncio.create_task(recorder.record(None, (), False))
        assert await wait_for_lock_waiters(db, 1, [audit_task]) is True
        prepare_task = asyncio.create_task(participant.prepare("epoch_new"))
        reset_was_fenced = await wait_for_lock_waiters(db, 2, [audit_task, prepare_task])
    await asyncio.gather(audit_task, prepare_task)

    assert reset_was_fenced is True
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        audit_count = await session.scalar(select(func.count()).select_from(AuditRecord))
    assert state is not None
    assert state.mode == "preparing"
    assert audit_count == 0


@pytest.mark.asyncio
async def test_auth_audit_uses_the_atomic_control_snapshot(
    db: async_sessionmaker[AsyncSession],
) -> None:
    snapshot_time = datetime(2026, 8, 19, 10, 5, tzinfo=UTC)
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_new"))

    await DatabaseAuthDecisionRecorder(
        "crm",
        db,
        Control(snapshot_time=snapshot_time, snapshot_epoch="epoch_new"),
    ).record(None, (), False)

    async with db() as session:
        audit = await session.scalar(select(AuditRecord))
    assert audit is not None
    assert audit.scenario_epoch == "epoch_new"
    assert audit.occurred_at == snapshot_time


@pytest.mark.asyncio
async def test_allowed_old_epoch_principal_is_not_audited_in_the_new_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_new"))
    principal = Principal(
        subject="person-support-1",
        actor_type="human",
        role="support_agent",
        scopes=frozenset({"crm:read"}),
        tenant_id="tenant_synthetic",
        token_id="tok_old",
        scenario_epoch="epoch_old",
    )

    with pytest.raises(ApiError) as raised:
        await DatabaseAuthDecisionRecorder("crm", db, Control(snapshot_epoch="epoch_new")).record(
            principal, (), True
        )

    assert raised.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert raised.value.status_code == 503
    async with db() as session:
        audit_count = await session.scalar(select(func.count()).select_from(AuditRecord))
    assert audit_count == 0
