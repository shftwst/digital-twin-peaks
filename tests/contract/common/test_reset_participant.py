from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import ParticipantLoadRequest
from enterprise_twins.common.control.participant import ResetParticipant, ScenarioLoader
from enterprise_twins.common.db.records import ScenarioState


class RecordingLoader(ScenarioLoader):
    def __init__(self) -> None:
        self.loaded: dict[str, dict[str, Any]] = {}

    async def load(
        self, session: AsyncSession, epoch: str, payload: dict[str, Any]
    ) -> dict[str, object]:
        self.loaded[epoch] = payload
        return {"schemaVersion": "1", "counts": payload["expectedCounts"]}

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        self.loaded.pop(epoch, None)


@pytest.mark.asyncio
async def test_staged_epoch_is_not_active_until_commit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
    loader = RecordingLoader()
    participant = ResetParticipant(db, loader)
    payload = {"expectedCounts": {"records": 2}}

    await participant.prepare("epoch_new")
    report = await participant.load(
        ParticipantLoadRequest(
            scenarioEpoch="epoch_new",
            scenarioId="platform-contracts",
            scenarioVersion=1,
            randomSeed=7,
            payload=payload,
            checksum=sha256_hex(payload),
        )
    )
    async with db() as session:
        before = await session.get(ScenarioState, 1)
        assert before is not None
        assert before.active_epoch == "epoch_old"
        assert before.mode == "loaded"
    await participant.commit("epoch_new")
    async with db() as session:
        after = await session.get(ScenarioState, 1)
        assert after is not None
        assert after.active_epoch == "epoch_new"
        assert after.mode == "active"
    assert report.checksum == sha256_hex(payload)


@pytest.mark.asyncio
async def test_abort_discards_pending_epoch_and_leaves_service_unhealthy(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
    loader = RecordingLoader()
    participant = ResetParticipant(db, loader)
    await participant.prepare("epoch_new")
    await participant.abort("epoch_new")
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.active_epoch == "epoch_old"
        assert state.mode == "error"
