from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.context import bind_response_epoch
from enterprise_twins.common.http.errors import ApiError, ErrorCode

Result = TypeVar("Result")


class ScenarioAccess:
    def __init__(
        self,
        service: str,
        factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.service = service
        self.factory = factory

    async def run(
        self,
        expected_epoch: str,
        operation: Callable[[], Awaitable[Result]],
    ) -> Result:
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            if state is not None:
                bind_response_epoch(state.active_epoch)
            if state is None or state.mode != "active" or state.active_epoch != expected_epoch:
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    f"{self.service} scenario is not active",
                    status_code=503,
                    retryable=True,
                )
            return await operation()

    async def require(self, expected_epoch: str) -> None:
        async def accepted() -> None:
            return None

        await self.run(expected_epoch, accepted)
