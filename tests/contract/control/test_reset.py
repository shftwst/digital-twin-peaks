import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.app import (
    bootstrap_scenario,
    build_control_app,
    create_from_env,
)
from enterprise_twins.services.control.models import ResetRun, VirtualClock
from enterprise_twins.services.control.reset import (
    ControlResetStore,
    DirectoryBundleLoader,
    ResetCoordinator,
    ScenarioBundle,
)
from enterprise_twins.services.control.settings import ControlSettings


class MissingRepository:
    async def state(self) -> ScenarioState:
        from enterprise_twins.services.control.repository import ScenarioStateMissingError

        raise ScenarioStateMissingError("scenario state is absent")


class ExistingRepository:
    def __init__(self, state: ScenarioState | None = None) -> None:
        self.value = state or ScenarioState(
            singleton_id=1,
            mode="active",
            active_epoch="epoch_existing",
        )

    async def state(self) -> ScenarioState:
        return self.value


class BootstrapCoordinator:
    def __init__(
        self,
        failures: list[Exception],
        recovery_failures: list[Exception] | None = None,
    ) -> None:
        self.failures = failures
        self.calls = 0
        self.recovery_failures = recovery_failures or []
        self.recovery_calls = 0

    async def reset(self, _request: ResetRequest) -> object:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return object()

    async def recover_pending_cleanup(self) -> bool:
        self.recovery_calls += 1
        if self.recovery_failures:
            raise self.recovery_failures.pop(0)
        return True


@pytest.mark.asyncio
async def test_bootstrap_retries_transient_participant_startup_failure() -> None:
    coordinator = BootstrapCoordinator(
        [
            httpx.ConnectError(
                "participant is not listening",
                request=httpx.Request("POST", "http://identity-admin:9000"),
            )
        ]
    )

    await bootstrap_scenario(
        MissingRepository(),
        coordinator,
        ResetRequest(scenarioId="platform-contracts", version=1),
        timeout_seconds=1.0,
        retry_delay_seconds=0.0,
    )

    assert coordinator.calls == 2


@pytest.mark.asyncio
async def test_bootstrap_does_not_retry_reset_contract_failure() -> None:
    coordinator = BootstrapCoordinator([RuntimeError("participant report differs")])

    with pytest.raises(RuntimeError, match="participant report differs"):
        await bootstrap_scenario(
            MissingRepository(),
            coordinator,
            ResetRequest(scenarioId="platform-contracts", version=1),
            timeout_seconds=1.0,
            retry_delay_seconds=0.0,
        )

    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_bootstrap_transport_retry_is_bounded() -> None:
    failure = httpx.ConnectError(
        "participant is not listening",
        request=httpx.Request("POST", "http://identity-admin:9000"),
    )
    coordinator = BootstrapCoordinator([failure])

    with pytest.raises(httpx.ConnectError, match="participant is not listening"):
        await bootstrap_scenario(
            MissingRepository(),
            coordinator,
            ResetRequest(scenarioId="platform-contracts", version=1),
            timeout_seconds=0.0,
            retry_delay_seconds=0.0,
        )

    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_bootstrap_skips_reset_when_scenario_state_exists() -> None:
    coordinator = BootstrapCoordinator([])

    await bootstrap_scenario(
        ExistingRepository(),
        coordinator,
        ResetRequest(scenarioId="platform-contracts", version=1),
        timeout_seconds=1.0,
        retry_delay_seconds=0.0,
    )

    assert coordinator.calls == 0
    assert coordinator.recovery_calls == 0


@pytest.mark.asyncio
async def test_bootstrap_recovers_pending_cleanup_without_reseeding() -> None:
    coordinator = BootstrapCoordinator([])
    repository = ExistingRepository(
        ScenarioState(
            singleton_id=1,
            mode="error",
            active_epoch="epoch_pending",
            pending_epoch="epoch_pending",
        )
    )

    await bootstrap_scenario(
        repository,
        coordinator,
        ResetRequest(scenarioId="platform-contracts", version=1),
        timeout_seconds=1.0,
        retry_delay_seconds=0.0,
    )

    assert coordinator.recovery_calls == 1
    assert coordinator.calls == 0


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
        store.pending_cleanup,
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
    assert status.json()["pendingEpoch"] is None
    assert status.json()["recoveryRequired"] is False
    assert reset.json()["randomSeed"] == 7
    assert status.json()["randomSeed"] == 7
    assert status.json()["manifestChecksum"] == reset.json()["manifestChecksum"]
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        run = await session.scalar(
            select(ResetRun).where(ResetRun.scenario_epoch == reset.json()["scenarioEpoch"])
        )
    assert state is not None
    assert state.random_seed == 7
    assert state.manifest_checksum == reset.json()["manifestChecksum"]
    assert run is not None
    assert run.random_seed == 7
    assert run.manifest_checksum == reset.json()["manifestChecksum"]


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
    assert await store.pending_cleanup() == "epoch_cleanup"

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
    assert await store.pending_cleanup() is None


def test_reset_request_boundary_still_rejects_unpersistable_seed() -> None:
    with pytest.raises(ValueError):
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=-1)


def write_catalogue_case(root: Path, case: str) -> None:
    directory = root / "platform-contracts"
    directory.mkdir()
    payload = {"expectedCounts": {}}
    (directory / "identity.json").write_text(json.dumps(payload), encoding="utf-8")
    manifest: dict[str, object] = {
        "scenarioId": "platform-contracts",
        "version": 1,
        "initialTime": "2026-08-19T10:00:00Z",
        "services": {
            "identity": {
                "file": "identity.json",
                "checksum": sha256_hex(payload),
            }
        },
    }
    if case == "missing_manifest":
        return
    if case == "malformed_manifest":
        (directory / "manifest.json").write_text("{sensitive-value", encoding="utf-8")
        return
    if case == "id_mismatch":
        manifest["scenarioId"] = "different-scenario"
    elif case == "path_escape":
        manifest["services"] = {
            "identity": {"file": "../outside.json", "checksum": sha256_hex(payload)}
        }
        (root / "outside.json").write_text(json.dumps(payload), encoding="utf-8")
    elif case == "missing_payload":
        manifest["services"] = {
            "identity": {"file": "missing.json", "checksum": sha256_hex(payload)}
        }
    elif case == "checksum_invalid":
        manifest["services"] = {"identity": {"file": "identity.json", "checksum": "0" * 64}}
    elif case == "time_invalid":
        manifest["initialTime"] = "sensitive-invalid-time"
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "scenario_id", "version", "expected_status", "expected_code"),
    [
        ("absent_scenario", "missing-scenario", 1, 404, "not_found"),
        ("absent_version", "platform-contracts", 2, 404, "not_found"),
        ("missing_manifest", "platform-contracts", 1, 500, "internal_error"),
        ("malformed_manifest", "platform-contracts", 1, 500, "internal_error"),
        ("id_mismatch", "platform-contracts", 1, 500, "internal_error"),
        ("path_escape", "platform-contracts", 1, 500, "internal_error"),
        ("missing_payload", "platform-contracts", 1, 500, "internal_error"),
        ("checksum_invalid", "platform-contracts", 1, 500, "internal_error"),
        ("time_invalid", "platform-contracts", 1, 500, "internal_error"),
    ],
)
async def test_reset_catalogue_errors_use_common_redacted_envelopes_without_state_change(
    db: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    case: str,
    scenario_id: str,
    version: int,
    expected_status: int,
    expected_code: str,
) -> None:
    await initialise_control(db)
    if case != "absent_scenario":
        write_catalogue_case(tmp_path, case)
    store = ControlResetStore(db)
    coordinator = ResetCoordinator(
        {},
        DirectoryBundleLoader(tmp_path),
        store.begin,
        store.commit,
        store.fail,
        store.finalize,
        store.pending_cleanup,
    )
    app = build_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-token",  # noqa: S106
            twin_token="twin-token",  # noqa: S106
        ),
        coordinator,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://control",
    ) as client:
        response = await client.post(
            "/control/v1/reset",
            headers={"Authorization": "Bearer controller-token"},
            json={"scenarioId": scenario_id, "version": version},
        )

    assert response.status_code == expected_status
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["X-Request-Id"].startswith("req_")
    assert response.headers["X-Scenario-Epoch"] == "epoch_old"
    assert response.json()["error"]["code"] == expected_code
    assert response.json()["error"]["retryable"] is False
    assert "sensitive" not in response.text
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        reset_count = await session.scalar(select(func.count()).select_from(ResetRun))
        clock = await session.get(VirtualClock, 1)
    assert state is not None
    assert state.mode == "active"
    assert state.active_epoch == "epoch_old"
    assert state.pending_epoch is None
    assert reset_count == 0
    assert clock is not None
    assert clock.now == datetime(2026, 8, 19, 9, tzinfo=UTC)


@pytest.mark.asyncio
async def test_reset_invalid_scenario_id_is_a_common_422_without_state_change(
    db: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await initialise_control(db)
    store = ControlResetStore(db)
    coordinator = ResetCoordinator(
        {},
        DirectoryBundleLoader(tmp_path),
        store.begin,
        store.commit,
        store.fail,
        store.finalize,
        store.pending_cleanup,
    )
    app = build_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-token",  # noqa: S106
            twin_token="twin-token",  # noqa: S106
        ),
        coordinator,
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/reset",
            headers={"Authorization": "Bearer controller-token"},
            json={"scenarioId": "../escape", "version": 1},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    async with db() as session:
        state = await session.get(ScenarioState, 1)
    assert state is not None
    assert state.active_epoch == "epoch_old"
    assert state.pending_epoch is None


class CleanupParticipant:
    def __init__(self) -> None:
        self.finalize_calls: list[str] = []
        self.failures_remaining = 1

    async def prepare(self, _epoch: str) -> None:
        return None

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        return ParticipantReport(
            service="identity",
            schemaVersion="1",
            counts=request.payload["expectedCounts"],
            checksum=request.checksum,
        )

    async def commit(self, _epoch: str) -> None:
        return None

    async def finalize(self, epoch: str) -> None:
        self.finalize_calls.append(epoch)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("sensitive participant cleanup failure")

    async def abort(self, _epoch: str) -> None:
        raise AssertionError("post-cutover cleanup must not abort")


@pytest.mark.asyncio
async def test_cleanup_failure_returns_503_then_next_reset_recovers_before_new_reset(
    db: async_sessionmaker[AsyncSession],
) -> None:
    participant = CleanupParticipant()
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"expectedCounts": {}}},
    )
    store = ControlResetStore(db)
    coordinator = ResetCoordinator(
        {"identity": participant},
        lambda _scenario_id, _version: bundle,
        store.begin,
        store.commit,
        store.fail,
        store.finalize,
        store.pending_cleanup,
    )
    app = build_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-token",  # noqa: S106
            twin_token="twin-token",  # noqa: S106
        ),
        coordinator,
    )
    headers = {"Authorization": "Bearer controller-token"}
    body = {"scenarioId": "platform-contracts", "version": 1}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        failed = await client.post("/control/v1/reset", headers=headers, json=body)
        pending = await client.get("/control/v1/status", headers=headers)
        unhealthy = await client.get("/health/ready")
        recovered = await client.post("/control/v1/reset", headers=headers, json=body)
        active = await client.get("/control/v1/status", headers=headers)

    assert failed.status_code == 503
    assert failed.json()["error"] == {
        "code": "temporarily_unavailable",
        "message": "reset cleanup is temporarily unavailable",
        "retryable": True,
        "requestId": failed.headers["X-Request-Id"],
        "details": {"phase": "cleanup"},
    }
    assert "sensitive" not in failed.text
    failed_epoch = pending.json()["scenarioEpoch"]
    assert pending.json()["mode"] == "error"
    assert pending.json()["pendingEpoch"] == failed_epoch
    assert pending.json()["recoveryRequired"] is True
    assert unhealthy.status_code == 503
    assert unhealthy.json()["status"] == "not_ready"
    assert recovered.status_code == 200
    recovered_epoch = recovered.json()["scenarioEpoch"]
    assert recovered_epoch != failed_epoch
    assert active.json()["mode"] == "active"
    assert active.json()["pendingEpoch"] is None
    assert active.json()["recoveryRequired"] is False
    assert participant.finalize_calls.count(failed_epoch) == 2
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        runs = {
            item.scenario_epoch: item.state
            for item in await session.scalars(select(ResetRun).order_by(ResetRun.scenario_epoch))
        }
    assert state is not None
    assert state.pending_epoch is None
    assert runs == {failed_epoch: "committed", recovered_epoch: "committed"}
