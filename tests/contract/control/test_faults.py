from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import (
    FaultEffect,
    FaultPhase,
    FaultProbe,
    FaultRuleCreate,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.faults import FaultRepository
from enterprise_twins.services.control.models import FaultActivation, VirtualClock


@pytest.mark.asyncio
async def test_rule_matches_then_fires_at_configured_occurrence_once(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))
    repository = FaultRepository(db)
    await repository.create(
        FaultRuleCreate(
            ruleId="crm-note-after-commit",
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            effect=FaultEffect.TIMEOUT,
            actorId="support-agent",
            occurrence=2,
            activationCount=1,
            delayMs=250,
        )
    )
    wrong_actor = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="auditor",
            correlationId="case-1",
        )
    )
    first = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    second = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    exhausted = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )

    assert wrong_actor.effect is None
    assert first.effect is None
    assert second.effect == FaultEffect.TIMEOUT
    assert second.delay_ms == 250
    assert exhausted.effect is None
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(FaultActivation))
    assert count == 1
