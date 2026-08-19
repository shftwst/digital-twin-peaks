import os
from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.base import Base
from enterprise_twins.common.db.runtime import make_engine, make_session_factory


@pytest_asyncio.fixture
async def db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    url = os.environ["TEST_DATABASE_URL"]
    engine = make_engine(url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    factory = make_session_factory(engine)
    yield factory
    await engine.dispose()
