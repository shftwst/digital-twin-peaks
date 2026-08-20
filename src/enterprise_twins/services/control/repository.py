from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ClockValue
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
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
        return (await self.snapshot()).now

    @staticmethod
    async def locked_state(session: AsyncSession, *, require_active: bool) -> ScenarioState:
        state = await session.scalar(
            select(ScenarioState).where(ScenarioState.singleton_id == 1).with_for_update(read=True)
        )
        if state is None:
            raise ScenarioStateMissingError("control scenario state is not initialised")
        if require_active and state.mode != "active":
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "Control scenario is not active",
                status_code=503,
                retryable=True,
            )
        return state

    @staticmethod
    async def locked_clock(session: AsyncSession, *, write: bool) -> VirtualClock:
        clock = await session.scalar(
            select(VirtualClock)
            .where(VirtualClock.singleton_id == 1)
            .with_for_update(read=not write)
        )
        if clock is None:
            raise RuntimeError("virtual clock is not initialised")
        return clock

    async def snapshot(self) -> ClockValue:
        async with self.factory.begin() as session:
            state = await self.locked_state(session, require_active=True)
            clock = await self.locked_clock(session, write=False)
            return ClockValue(now=clock.now, scenarioEpoch=state.active_epoch)

    async def status_snapshot(self) -> tuple[ScenarioState, datetime]:
        async with self.factory.begin() as session:
            state = await self.locked_state(session, require_active=False)
            clock = await self.locked_clock(session, write=False)
            return state, clock.now

    async def set_time(self, value: datetime) -> ClockValue:
        if value.utcoffset() is None:
            raise ValueError("virtual time must include a UTC offset")
        value = value.astimezone(UTC)
        async with self.factory.begin() as session:
            state = await self.locked_state(session, require_active=True)
            clock = await self.locked_clock(session, write=True)
            clock.now = value
            return ClockValue(now=clock.now, scenarioEpoch=state.active_epoch)

    async def advance_time(self, amount: timedelta) -> ClockValue:
        async with self.factory.begin() as session:
            state = await self.locked_state(session, require_active=True)
            clock = await self.locked_clock(session, write=True)
            clock.now += amount
            return ClockValue(now=clock.now, scenarioEpoch=state.active_epoch)
