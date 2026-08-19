import asyncio
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import inspect, text

from enterprise_twins.common.db.runtime import make_engine
from enterprise_twins.migration_metadata import selected_metadata
from enterprise_twins.migration_runner import upgrade

pytestmark = pytest.mark.integration


def database_url() -> str:
    value = os.environ.get("TEST_DATABASE_URL")
    if value is None:
        pytest.skip("TEST_DATABASE_URL is required for Compose-only migration tests")
    return value


async def clear_identity_schema(url: str) -> None:
    engine = make_engine(url)
    async with engine.begin() as connection:
        await connection.execute(text("DROP TABLE IF EXISTS alembic_version_identity"))
        await connection.run_sync(selected_metadata("identity").drop_all)
    await engine.dispose()


async def identity_schema(url: str) -> tuple[set[str], list[str]]:
    engine = make_engine(url)
    async with engine.connect() as connection:
        tables = set(await connection.run_sync(lambda sync: inspect(sync).get_table_names()))
        versions = list(
            await connection.scalars(text("SELECT version_num FROM alembic_version_identity"))
        )
    await engine.dispose()
    return tables, versions


def test_upgrade_can_run_twice_without_changing_the_schema() -> None:
    url = database_url()
    asyncio.run(clear_identity_schema(url))

    upgrade("identity", url)
    upgrade("identity", url)

    tables, versions = asyncio.run(identity_schema(url))
    assert set(selected_metadata("identity").tables) <= tables
    assert versions == ["0001_platform_contracts"]


def test_concurrent_first_upgrades_are_serialised() -> None:
    url = database_url()
    asyncio.run(clear_identity_schema(url))
    environment = os.environ | {"TWINS_DATABASE_URL": url}

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                lambda _index: subprocess.run(
                    [sys.executable, "-m", "enterprise_twins.migration_runner", "identity"],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=environment,
                ),
                range(4),
            )
        )

    tables, versions = asyncio.run(identity_schema(url))
    assert [(result.returncode, result.stderr) for result in results] == [
        (0, ""),
        (0, ""),
        (0, ""),
        (0, ""),
    ]
    assert set(selected_metadata("identity").tables) <= tables
    assert versions == ["0001_platform_contracts"]
