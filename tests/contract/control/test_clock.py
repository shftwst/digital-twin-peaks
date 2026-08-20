import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ResetRequest
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.app import ControlStatus, create_control_app
from enterprise_twins.services.control.models import VirtualClock
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.reset import (
    ControlResetStore,
    ResetCoordinator,
    ScenarioBundle,
    reset_router,
)
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.control.time import parse_duration


async def wait_for_lock_waiters(
    db: async_sessionmaker[AsyncSession],
    expected: int,
    tasks: list[asyncio.Task[object]],
) -> list[str]:
    for _ in range(100):
        async with db() as session:
            queries = list(
                await session.scalars(
                    text(
                        "SELECT query FROM pg_stat_activity "
                        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                    )
                )
            )
        if len(queries) >= expected:
            return queries
        if any(task.done() for task in tasks):
            break
    raise AssertionError(f"expected {expected} PostgreSQL lock waiter(s)")


@asynccontextmanager
async def lock_clock_table(
    db: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    async with db() as session:
        await session.begin()
        await session.execute(text("LOCK TABLE virtual_clock IN ACCESS EXCLUSIVE MODE"))
        try:
            yield
        finally:
            await session.commit()


class ResetAwareStatus(ControlStatus):
    def __init__(
        self,
        repository: ControlRepository,
        reset_finished: asyncio.Event,
    ) -> None:
        super().__init__(repository)
        self.reset_finished = reset_finished

    async def current_epoch(self) -> str:
        await self.reset_finished.wait()
        return await super().current_epoch()


@pytest.mark.asyncio
async def test_virtual_clock_set_and_advance(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))

    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        denied = await client.get("/control/v1/time")
        current = await client.get(
            "/control/v1/time", headers={"Authorization": "Bearer twin-test-token"}
        )
        advanced = await client.post(
            "/control/v1/time/advance",
            headers={"Authorization": "Bearer controller-test-token"},
            json={"duration": "PT5M"},
        )

    assert denied.status_code == 401
    assert current.json() == {"now": "2026-08-19T10:00:00Z", "scenarioEpoch": "epoch_1"}
    assert advanced.json() == {"now": "2026-08-19T10:05:00Z", "scenarioEpoch": "epoch_1"}
    assert parse_duration("P1DT2H3M4S") == timedelta(days=1, hours=2, minutes=3, seconds=4)


def test_duration_rejects_calendar_units_and_negative_values() -> None:
    with pytest.raises(ValueError):
        parse_duration("P1M")
    with pytest.raises(ValueError):
        parse_duration("-PT1S")


@pytest.mark.asyncio
async def test_put_time_rejects_naive_datetime_with_common_error_envelope(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))

    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.put(
            "/control/v1/time",
            headers={"Authorization": "Bearer controller-test-token"},
            json={"now": "2026-08-19T10:00:00"},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.headers["X-Scenario-Epoch"] == "epoch_1"


@pytest.mark.asyncio
async def test_put_time_normalises_offset_to_utc_before_persisting(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))

    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        updated = await client.put(
            "/control/v1/time",
            headers={"Authorization": "Bearer controller-test-token"},
            json={"now": "2026-08-19T12:00:00+02:00"},
        )
        persisted = await client.get(
            "/control/v1/time", headers={"Authorization": "Bearer twin-test-token"}
        )

    expected = {"now": "2026-08-19T10:00:00Z", "scenarioEpoch": "epoch_1"}
    assert updated.json() == expected
    assert persisted.json() == expected


@pytest.mark.asyncio
async def test_advance_time_rejects_oversized_duration_with_common_error_envelope(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))

    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/time/advance",
            headers={"Authorization": "Bearer controller-test-token"},
            json={"duration": "P999999999999999999999999D"},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.headers["X-Scenario-Epoch"] == "epoch_1"


@pytest.mark.asyncio
async def test_readiness_is_not_ready_when_scenario_or_clock_is_missing(
    db: async_sessionmaker[AsyncSession],
) -> None:
    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        missing_scenario = await client.get("/health/ready")
        async with db.begin() as session:
            session.add(
                ScenarioState(
                    singleton_id=1,
                    mode="active",
                    active_epoch="epoch_1",
                    scenario_id="platform-contracts",
                    scenario_version=1,
                    random_seed=7,
                    manifest_checksum="a" * 64,
                )
            )
        missing_clock = await client.get("/health/ready")

    for response in (missing_scenario, missing_clock):
        assert response.status_code == 503
        assert response.json()["status"] == "not_ready"
        assert response.headers["X-Request-Id"].startswith("req_")
        assert response.headers["X-Scenario-Epoch"]


@pytest.mark.asyncio
async def test_readiness_handles_database_connection_failure() -> None:
    engine = make_engine("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    app = create_control_app(
        make_session_factory(engine),
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://control"
        ) as client:
            response = await client.get("/health/ready")
    finally:
        await engine.dispose()

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.headers["X-Request-Id"].startswith("req_")
    assert response.headers["X-Scenario-Epoch"]


@pytest.mark.asyncio
async def test_readiness_rejects_active_state_with_inconsistent_pending_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                pending_epoch="epoch_unexpected",
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))
    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",  # noqa: S106
            twin_token="twin-test-token",  # noqa: S106
        ),
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["scenario"] == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/control/v1/time", None),
        ("PUT", "/control/v1/time", {"now": "2026-08-19T11:00:00Z"}),
        ("POST", "/control/v1/time/advance", {"duration": "PT5M"}),
    ],
)
async def test_clock_operations_fail_closed_while_the_scenario_is_not_active(
    db: async_sessionmaker[AsyncSession],
    method: str,
    path: str,
    body: dict[str, str] | None,
) -> None:
    initial = datetime(2026, 8, 19, 10, tzinfo=UTC)
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="preparing",
                active_epoch="epoch_old",
                pending_epoch="epoch_new",
            )
        )
        session.add(VirtualClock(singleton_id=1, now=initial))
    settings = ControlSettings(
        database_url="postgresql+asyncpg://unused",
        controller_token="controller-test-token",  # noqa: S106
        twin_token="twin-test-token",  # noqa: S106
    )
    app = create_control_app(db, settings)
    token = "twin-test-token" if method == "GET" else "controller-test-token"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.request(
            method,
            path,
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.json()["error"]["retryable"] is True
    async with db() as session:
        clock = await session.get(VirtualClock, 1)
    assert clock is not None
    assert clock.now == initial


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "body", "token", "expected_now"),
    [
        (
            "GET",
            "/control/v1/time",
            None,
            "twin-test-token",
            "2026-08-19T10:00:00Z",
        ),
        (
            "PUT",
            "/control/v1/time",
            {"now": "2026-08-19T11:00:00Z"},
            "controller-test-token",
            "2026-08-19T11:00:00Z",
        ),
        (
            "POST",
            "/control/v1/time/advance",
            {"duration": "PT5M"},
            "controller-test-token",
            "2026-08-19T10:05:00Z",
        ),
        (
            "GET",
            "/control/v1/status",
            None,
            "controller-test-token",
            "2026-08-19T10:00:00Z",
        ),
    ],
)
async def test_clock_http_snapshot_holds_the_scenario_fence_and_binds_its_header(
    db: async_sessionmaker[AsyncSession],
    method: str,
    path: str,
    body: dict[str, str] | None,
    token: str,
    expected_now: str,
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))
    settings = ControlSettings(
        database_url="postgresql+asyncpg://unused",
        controller_token="controller-test-token",  # noqa: S106
        twin_token="twin-test-token",  # noqa: S106
    )
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 20, 8, tzinfo=UTC),
        payloads={},
    )
    store = ControlResetStore(db)
    coordinator = ResetCoordinator(
        {},
        lambda _scenario_id, _version: bundle,
        store.begin,
        store.commit,
        store.fail,
        store.finalize,
        store.pending_cleanup,
        store.finalize_abort,
        store.pending_abort,
    )
    repository = ControlRepository(db)
    reset_finished = asyncio.Event()
    app = create_app(
        "Control race probe",
        (),
        ResetAwareStatus(repository, reset_finished),
        (
            control_router(repository, settings),
            reset_router(coordinator, repository, settings),
        ),
    )

    async def run_reset() -> object:
        try:
            return await coordinator.reset(
                ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7)
            )
        finally:
            reset_finished.set()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        async with lock_clock_table(db):
            request_task = asyncio.create_task(
                client.request(
                    method,
                    path,
                    headers={"Authorization": f"Bearer {token}"},
                    json=body,
                )
            )
            await wait_for_lock_waiters(db, 1, [request_task])
            reset_task = asyncio.create_task(run_reset())
            wait_queries = await wait_for_lock_waiters(db, 2, [request_task, reset_task])
            reset_waits_on_scenario_state = any(
                "scenario_state" in query.casefold() for query in wait_queries
            )
        response, reset_result = await asyncio.gather(request_task, reset_task)

    assert reset_waits_on_scenario_state is True
    assert response.status_code == 200
    assert response.json()["scenarioEpoch"] == "epoch_old"
    assert response.json()["now"] == expected_now
    assert response.headers["X-Scenario-Epoch"] == "epoch_old"
    assert reset_result.scenario_epoch != "epoch_old"
