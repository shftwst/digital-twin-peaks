import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.crm.app import CrmStatus
from enterprise_twins.services.identity.app import IdentityStatus
from enterprise_twins.services.relay.app import RelayStatus


class ReadyControl:
    def __init__(self, epoch: str = "epoch_1") -> None:
        self.epoch = epoch

    async def ready_epoch(self) -> str:
        return self.epoch


class UnavailableControl:
    async def ready_epoch(self) -> str:
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "Control is temporarily unavailable",
            status_code=503,
            retryable=True,
        )


class DispatcherHealth:
    def __init__(self, ready: bool) -> None:
        self.ready = ready

    def is_ready(self) -> bool:
        return self.ready


class RelayHealth:
    def __init__(self, epoch: str = "epoch_1", *, unavailable: bool = False) -> None:
        self.epoch = epoch
        self.unavailable = unavailable

    async def ready_epoch(self) -> str:
        if self.unavailable:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "private Relay detail",
                status_code=503,
                retryable=True,
            )
        return self.epoch


@pytest.mark.asyncio
@pytest.mark.parametrize("status_type", [IdentityStatus, CrmStatus, RelayStatus])
async def test_service_readiness_requires_matching_control_epoch(
    db: async_sessionmaker[AsyncSession], status_type: type[object]
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))

    status = status_type(db, ReadyControl("epoch_2"))  # type: ignore[call-arg]

    assert await status.readiness() == (  # type: ignore[attr-defined]
        False,
        {"database": "ready", "scenario": "active", "control": "epoch_mismatch"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_type", [IdentityStatus, CrmStatus, RelayStatus])
async def test_service_readiness_reports_control_outage(
    db: async_sessionmaker[AsyncSession], status_type: type[object]
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))

    status = status_type(db, UnavailableControl())  # type: ignore[call-arg]

    assert await status.readiness() == (  # type: ignore[attr-defined]
        False,
        {"database": "ready", "scenario": "active", "control": "not_ready"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_type", [IdentityStatus, CrmStatus])
async def test_source_readiness_requires_live_dispatcher_and_matching_relay_then_recovers(
    db: async_sessionmaker[AsyncSession], status_type: type[object]
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
    dispatcher = DispatcherHealth(False)
    relay = RelayHealth(unavailable=True)
    status = status_type(  # type: ignore[call-arg]
        db,
        ReadyControl(),
        dispatcher=dispatcher,
        relay=relay,
    )

    assert await status.readiness() == (  # type: ignore[attr-defined]
        False,
        {
            "database": "ready",
            "scenario": "active",
            "control": "ready",
            "dispatcher": "not_ready",
            "relay": "not_ready",
        },
    )

    dispatcher.ready = True
    relay.unavailable = False
    assert await status.readiness() == (  # type: ignore[attr-defined]
        True,
        {
            "database": "ready",
            "scenario": "active",
            "control": "ready",
            "dispatcher": "ready",
            "relay": "ready",
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_type", [IdentityStatus, CrmStatus])
async def test_source_readiness_rejects_a_relay_epoch_mismatch(
    db: async_sessionmaker[AsyncSession], status_type: type[object]
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))

    status = status_type(  # type: ignore[call-arg]
        db,
        ReadyControl(),
        dispatcher=DispatcherHealth(True),
        relay=RelayHealth("epoch_2"),
    )

    ready, checks = await status.readiness()  # type: ignore[attr-defined]
    assert ready is False
    assert checks["relay"] == "epoch_mismatch"
