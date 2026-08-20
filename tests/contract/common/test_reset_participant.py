import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
)
from enterprise_twins.common.control.participant import (
    ResetParticipant,
    ScenarioLoader,
    create_participant_app,
)
from enterprise_twins.common.db.base import Base
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.control.reset import ResetCoordinator, ScenarioBundle

OLD_MANIFEST = "a" * 64
NEW_MANIFEST = "b" * 64


class RecordingLoader(ScenarioLoader):
    def __init__(self) -> None:
        self.loaded: dict[str, dict[str, Any]] = {}
        self.fail_discard_once: str | None = None

    async def load(
        self, session: AsyncSession, epoch: str, payload: dict[str, Any]
    ) -> dict[str, object]:
        self.loaded[epoch] = payload
        return {"schemaVersion": "1", "counts": payload["expectedCounts"]}

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        if self.fail_discard_once == epoch:
            self.fail_discard_once = None
            raise RuntimeError(f"discard {epoch} failed")
        self.loaded.pop(epoch, None)


class FailingCommitParticipant:
    def __init__(self, participant: ResetParticipant) -> None:
        self.participant = participant

    async def prepare(self, epoch: str) -> None:
        await self.participant.prepare(epoch)

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        return await self.participant.load(request)

    async def commit(self, epoch: str) -> None:
        raise RuntimeError("crm commit failed")

    async def abort(self, epoch: str) -> None:
        await self.participant.abort(epoch)

    async def finalize(self, epoch: str) -> None:
        await self.participant.finalize(epoch)


async def add_active_state(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
                scenario_id="old-scenario",
                scenario_version=3,
                random_seed=11,
                manifest_checksum=OLD_MANIFEST,
            )
        )


def load_request(epoch: str = "epoch_new") -> ParticipantLoadRequest:
    payload = {"expectedCounts": {"records": 2}}
    return ParticipantLoadRequest(
        scenarioEpoch=epoch,
        scenarioId="platform-contracts",
        scenarioVersion=1,
        randomSeed=7,
        payload=payload,
        checksum=sha256_hex(payload),
        manifestChecksum=NEW_MANIFEST,
    )


@pytest_asyncio.fixture
async def participant_dbs() -> AsyncIterator[
    tuple[async_sessionmaker[AsyncSession], async_sessionmaker[AsyncSession]]
]:
    url = os.environ.get("TEST_DATABASE_URL")
    if url is None:
        pytest.skip("TEST_DATABASE_URL is required for Compose-only database tests")
    admin = make_engine(url)
    schema_names = [f"participant_{uuid4().hex}" for _ in range(2)]
    async with admin.begin() as connection:
        for schema in schema_names:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engines: list[AsyncEngine] = []
    factories: list[async_sessionmaker[AsyncSession]] = []
    for schema in schema_names:
        engine = create_async_engine(
            url,
            connect_args={"server_settings": {"search_path": schema}},
            pool_pre_ping=True,
        )
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        engines.append(engine)
        factories.append(make_session_factory(engine))
    try:
        yield factories[0], factories[1]
    finally:
        for engine in engines:
            await engine.dispose()
        async with admin.begin() as connection:
            for schema in schema_names:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_staged_metadata_and_old_epoch_remain_until_finalize(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await add_active_state(db)
    loader = RecordingLoader()
    loader.loaded["epoch_old"] = {"expectedCounts": {"records": 1}}
    participant = ResetParticipant(db, loader)
    request = load_request()

    await participant.prepare("epoch_new")
    report = await participant.load(request)
    async with db() as session:
        loaded = await session.get(ScenarioState, 1)
        assert loaded is not None
        assert loaded.active_epoch == "epoch_old"
        assert loaded.scenario_id == "old-scenario"
        assert loaded.scenario_version == 3
        assert loaded.random_seed == 11
        assert loaded.manifest_checksum == OLD_MANIFEST
        assert loaded.pending_scenario_id == "platform-contracts"
        assert loaded.pending_scenario_version == 1
        assert loaded.pending_random_seed == 7
        assert loaded.pending_manifest_checksum == NEW_MANIFEST
        assert loaded.mode == "loaded"

    await participant.commit("epoch_new")
    async with db() as session:
        committed = await session.get(ScenarioState, 1)
        assert committed is not None
        assert committed.active_epoch == "epoch_new"
        assert committed.mode == "committed"
        assert committed.rollback_epoch == "epoch_old"
        assert committed.rollback_scenario_id == "old-scenario"
    assert "epoch_old" in loader.loaded

    await participant.finalize("epoch_new")
    async with db() as session:
        final = await session.get(ScenarioState, 1)
        assert final is not None
        assert final.active_epoch == "epoch_new"
        assert final.scenario_id == "platform-contracts"
        assert final.mode == "active"
        assert final.pending_epoch is None
        assert final.rollback_epoch is None
    assert "epoch_old" not in loader.loaded
    assert report.checksum == sha256_hex(request.payload)


@pytest.mark.asyncio
async def test_abort_before_commit_preserves_active_metadata(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await add_active_state(db)
    loader = RecordingLoader()
    loader.loaded["epoch_old"] = {"expectedCounts": {"records": 1}}
    participant = ResetParticipant(db, loader)

    await participant.prepare("epoch_new")
    await participant.load(load_request())
    await participant.abort("epoch_new")

    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.active_epoch == "epoch_old"
        assert state.scenario_id == "old-scenario"
        assert state.scenario_version == 3
        assert state.random_seed == 11
        assert state.manifest_checksum == OLD_MANIFEST
        assert state.pending_epoch is None
        assert state.pending_scenario_id is None
        assert state.mode == "error"
    assert "epoch_old" in loader.loaded
    assert "epoch_new" not in loader.loaded


@pytest.mark.asyncio
async def test_later_commit_failure_restores_every_participant_epoch_data_and_metadata(
    participant_dbs: tuple[async_sessionmaker[AsyncSession], async_sessionmaker[AsyncSession]],
) -> None:
    identity_db, crm_db = participant_dbs
    await add_active_state(identity_db)
    await add_active_state(crm_db)
    identity_loader = RecordingLoader()
    crm_loader = RecordingLoader()
    old_payload = {"expectedCounts": {"records": 1}}
    identity_loader.loaded["epoch_old"] = old_payload
    crm_loader.loaded["epoch_old"] = old_payload
    identity = ResetParticipant(identity_db, identity_loader, service="identity")
    crm = ResetParticipant(crm_db, crm_loader, service="crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {
                "schemaVersion": "1",
                "expectedCounts": {"records": 2},
                "aliases": {},
            },
            "crm": {
                "schemaVersion": "1",
                "expectedCounts": {"records": 3},
                "aliases": {},
            },
        },
    )
    coordinator = ResetCoordinator.for_test(
        {"identity": identity, "crm": FailingCommitParticipant(crm)}, bundle
    )

    with pytest.raises(RuntimeError, match="crm commit failed"):
        await coordinator.reset(
            ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7)
        )

    for factory, loader in (
        (identity_db, identity_loader),
        (crm_db, crm_loader),
    ):
        async with factory() as session:
            state = await session.get(ScenarioState, 1)
            assert state is not None
            assert state.active_epoch == "epoch_old"
            assert state.scenario_id == "old-scenario"
            assert state.scenario_version == 3
            assert state.random_seed == 11
            assert state.manifest_checksum == OLD_MANIFEST
            assert state.pending_epoch is None
            assert state.rollback_epoch is None
            assert state.mode == "error"
        assert "epoch_old" in loader.loaded
        assert all(epoch == "epoch_old" for epoch in loader.loaded)


@pytest.mark.asyncio
async def test_finalize_is_idempotent_and_retry_safe(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await add_active_state(db)
    loader = RecordingLoader()
    loader.loaded["epoch_old"] = {"expectedCounts": {"records": 1}}
    participant = ResetParticipant(db, loader)
    await participant.prepare("epoch_new")
    await participant.load(load_request())
    await participant.commit("epoch_new")
    loader.fail_discard_once = "epoch_old"

    with pytest.raises(RuntimeError, match="discard epoch_old failed"):
        await participant.finalize("epoch_new")
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.mode == "committed"
        assert state.rollback_epoch == "epoch_old"

    await participant.finalize("epoch_new")
    await participant.finalize("epoch_new")

    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.mode == "active"
        assert state.rollback_epoch is None
    assert "epoch_old" not in loader.loaded


@pytest.mark.asyncio
async def test_participant_readiness_requires_existing_active_state(
    db: async_sessionmaker[AsyncSession],
) -> None:
    participant = ResetParticipant(db, RecordingLoader())
    app = create_participant_app("identity", participant, "participant-token")
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://participant"
    ) as client:
        absent = await client.get("/health/ready")
        await add_active_state(db)
        active = await client.get("/health/ready")
        responses = []
        for mode in ("preparing", "loaded", "committed", "error"):
            async with db.begin() as session:
                state = await session.get(ScenarioState, 1)
                assert state is not None
                state.mode = mode
            responses.append(await client.get("/health/ready"))

    assert absent.status_code == 503
    assert active.status_code == 200
    assert all(response.status_code == 503 for response in responses)


@pytest.mark.asyncio
async def test_private_routes_require_participant_token_and_include_finalize(
    db: async_sessionmaker[AsyncSession],
) -> None:
    loader = RecordingLoader()
    participant = ResetParticipant(db, loader, service="identity")
    app = create_participant_app("identity", participant, "participant-token")
    request = load_request()
    bodies: dict[str, dict[str, object]] = {
        "prepare": {"scenarioEpoch": "epoch_new"},
        "load": request.model_dump(mode="json", by_alias=True),
        "commit": {"scenarioEpoch": "epoch_new"},
        "finalize": {"scenarioEpoch": "epoch_new"},
        "abort": {"scenarioEpoch": "epoch_new"},
    }
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://participant") as client:
        denied = [
            await client.post(f"/internal/v1/reset/{action}", json=body)
            for action, body in bodies.items()
        ]
        headers = {"Authorization": "Bearer participant-token"}
        prepared = await client.post(
            "/internal/v1/reset/prepare", headers=headers, json=bodies["prepare"]
        )
        loaded = await client.post("/internal/v1/reset/load", headers=headers, json=bodies["load"])
        committed = await client.post(
            "/internal/v1/reset/commit", headers=headers, json=bodies["commit"]
        )
        finalized = await client.post(
            "/internal/v1/reset/finalize", headers=headers, json=bodies["finalize"]
        )
        finalized_again = await client.post(
            "/internal/v1/reset/finalize", headers=headers, json=bodies["finalize"]
        )

    assert all(response.status_code == 401 for response in denied)
    assert prepared.status_code == 204
    assert loaded.status_code == 200
    assert committed.status_code == 204
    assert finalized.status_code == 204
    assert finalized_again.status_code == 204
