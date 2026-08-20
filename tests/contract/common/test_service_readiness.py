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
