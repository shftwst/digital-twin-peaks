from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.app import create_control_app
from enterprise_twins.services.control.models import VirtualClock
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.control.time import parse_duration


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
