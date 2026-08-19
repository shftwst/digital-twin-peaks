import asyncio
import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ResetRequest
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.app import build_control_app, create_from_env
from enterprise_twins.services.control.models import ResetRun, VirtualClock
from enterprise_twins.services.control.reset import (
    ControlResetStore,
    ResetCoordinator,
    ScenarioBundle,
)
from enterprise_twins.services.control.settings import ControlSettings


def empty_bundle() -> ScenarioBundle:
    return ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={},
    )


async def initialise_control(db: async_sessionmaker[AsyncSession]) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
                scenario_id="old-scenario",
                scenario_version=3,
                random_seed=11,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 9, tzinfo=UTC)))


@pytest.mark.asyncio
async def test_concurrent_store_begin_rejects_the_second_persisted_reset(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    store_a = ControlResetStore(db)
    store_b = ControlResetStore(db)

    results = await asyncio.gather(
        store_a.begin("epoch_a", empty_bundle(), 7),
        store_b.begin("epoch_b", empty_bundle(), 7),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    errors = [result for result in results if isinstance(result, ApiError)]
    assert len(errors) == 1
    assert errors[0].code == ErrorCode.CONFLICT
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.mode == "preparing"
        assert state.pending_epoch in {"epoch_a", "epoch_b"}


@pytest.mark.asyncio
async def test_control_reset_and_status_routes_require_controller_role(
    db: async_sessionmaker[AsyncSession],
) -> None:
    bundle = empty_bundle()
    store = ControlResetStore(db)
    coordinator = ResetCoordinator(
        {},
        lambda _scenario_id, _version: bundle,
        store.begin,
        store.commit,
        store.fail,
        store.finalize,
    )
    settings = ControlSettings(
        database_url="postgresql+asyncpg://unused",
        controller_token="controller-token",  # noqa: S106
        twin_token="twin-token",  # noqa: S106
    )
    app = build_control_app(db, settings, coordinator)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        denied_reset = await client.post(
            "/control/v1/reset",
            json={"scenarioId": "platform-contracts", "version": 1},
        )
        reset = await client.post(
            "/control/v1/reset",
            headers={"Authorization": "Bearer controller-token"},
            json={"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7},
        )
        denied_status = await client.get(
            "/control/v1/status", headers={"Authorization": "Bearer twin-token"}
        )
        status = await client.get(
            "/control/v1/status", headers={"Authorization": "Bearer controller-token"}
        )

    assert denied_reset.status_code == 401
    assert reset.status_code == 200
    assert denied_status.status_code == 401
    assert status.status_code == 200
    assert status.json()["scenarioEpoch"] == reset.json()["scenarioEpoch"]
    assert status.json()["mode"] == "active"


@pytest.mark.asyncio
async def test_environment_lifespan_does_not_bootstrap_over_existing_state(
    db: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    await initialise_control(db)
    database_url = os.environ["TEST_DATABASE_URL"]
    monkeypatch.setenv("TWINS_CONTROL_DATABASE_URL", database_url)
    monkeypatch.setenv("TWINS_CONTROL_CONTROLLER_TOKEN", "controller-token")
    monkeypatch.setenv("TWINS_CONTROL_TWIN_TOKEN", "twin-token")
    monkeypatch.setenv("TWINS_CONTROL_PARTICIPANTS", "{}")
    monkeypatch.setenv("TWINS_CONTROL_SCENARIO_ROOT", "/path/that/must/not/be/read")
    app = create_from_env()

    async with app.router.lifespan_context(app):
        pass

    async with db() as session:
        state = await session.get(ScenarioState, 1)
        reset_count = await session.scalar(select(func.count()).select_from(ResetRun))
    assert state is not None
    assert state.active_epoch == "epoch_old"
    assert state.scenario_id == "old-scenario"
    assert reset_count == 0


@pytest.mark.asyncio
async def test_store_failure_records_pre_cutover_and_cleanup_distinctly(
    db: async_sessionmaker[AsyncSession],
) -> None:
    store = ControlResetStore(db)
    bundle = empty_bundle()
    await store.begin("epoch_pre", bundle, 7)
    await store.fail("epoch_pre", "pre_cutover")
    await store.begin("epoch_cleanup", bundle, 7)
    await store.commit("epoch_cleanup", bundle, 7)
    await store.fail("epoch_cleanup", "cleanup")

    async with db() as session:
        runs = {
            run.scenario_epoch: run
            for run in await session.scalars(select(ResetRun).order_by(ResetRun.scenario_epoch))
        }
        state = await session.get(ScenarioState, 1)
    assert runs["epoch_pre"].error == "participant reset failed before cutover"
    assert runs["epoch_cleanup"].error == "participant reset cleanup failed"
    assert state is not None
    assert state.active_epoch == "epoch_cleanup"
    assert state.mode == "error"
    assert state.pending_epoch == "epoch_cleanup"

    with pytest.raises(ApiError, match="another reset is active"):
        await store.begin("epoch_replacement", bundle, 7)
    await store.finalize("epoch_cleanup")
    await store.finalize("epoch_cleanup")

    async with db() as session:
        retried = await session.get(ScenarioState, 1)
        cleanup_run = await session.scalar(
            select(ResetRun).where(ResetRun.scenario_epoch == "epoch_cleanup")
        )
    assert retried is not None
    assert retried.mode == "active"
    assert retried.pending_epoch is None
    assert cleanup_run is not None
    assert cleanup_run.state == "committed"
    assert cleanup_run.error is None


def test_reset_request_boundary_still_rejects_unpersistable_seed() -> None:
    with pytest.raises(ValueError):
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=-1)
