from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.models import VirtualClock


class ScenarioStateMissingError(RuntimeError):
    pass


class ControlRepository:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise ScenarioStateMissingError("control scenario state is not initialised")
            return state

    async def now(self) -> datetime:
        async with self.factory() as session:
            clock = await session.get(VirtualClock, 1)
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            return clock.now

    async def set_time(self, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("virtual time must include a UTC offset")
        value = value.astimezone(UTC)
        async with self.factory.begin() as session:
            clock = await session.scalar(
                select(VirtualClock).where(VirtualClock.singleton_id == 1).with_for_update()
            )
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            clock.now = value
            return clock.now

    async def advance_time(self, amount: timedelta) -> datetime:
        async with self.factory.begin() as session:
            clock = await session.scalar(
                select(VirtualClock).where(VirtualClock.singleton_id == 1).with_for_update()
            )
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            clock.now += amount
            return clock.now
