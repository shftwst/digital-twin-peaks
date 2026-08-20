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

DROP_VERSION_TABLE = {
    "identity": text("DROP TABLE IF EXISTS alembic_version_identity"),
    "relay": text("DROP TABLE IF EXISTS alembic_version_relay"),
}
SELECT_VERSION = {
    "identity": text("SELECT version_num FROM alembic_version_identity"),
    "relay": text("SELECT version_num FROM alembic_version_relay"),
}


def database_url() -> str:
    value = os.environ.get("TEST_DATABASE_URL")
    if value is None:
        pytest.skip("TEST_DATABASE_URL is required for Compose-only migration tests")
    return value


async def clear_service_schema(url: str, service: str) -> None:
    engine = make_engine(url)
    async with engine.begin() as connection:
        await connection.execute(DROP_VERSION_TABLE[service])
        await connection.run_sync(selected_metadata(service).drop_all)
    await engine.dispose()


async def service_schema(url: str, service: str) -> tuple[set[str], list[str]]:
    engine = make_engine(url)
    async with engine.connect() as connection:
        tables = set(await connection.run_sync(lambda sync: inspect(sync).get_table_names()))
        versions = list(await connection.scalars(SELECT_VERSION[service]))
    await engine.dispose()
    return tables, versions


@pytest.mark.parametrize("service", ["identity", "relay"])
def test_upgrade_can_run_twice_without_changing_the_schema(service: str) -> None:
    url = database_url()
    asyncio.run(clear_service_schema(url, service))

    upgrade(service, url)
    upgrade(service, url)

    tables, versions = asyncio.run(service_schema(url, service))
    assert set(selected_metadata(service).tables) <= tables
    assert versions == ["0002_relay_worker_heartbeat"]


def test_relay_upgrade_from_0001_adds_the_complete_worker_heartbeat_table() -> None:
    url = database_url()
    asyncio.run(clear_service_schema(url, "relay"))
    upgrade("relay", url)

    async def restore_0001_shape() -> None:
        engine = make_engine(url)
        async with engine.begin() as connection:
            await connection.execute(text("DROP TABLE relay_worker_heartbeat"))
            await connection.execute(
                text("UPDATE alembic_version_relay SET version_num = '0001_platform_contracts'")
            )
        await engine.dispose()

    asyncio.run(restore_0001_shape())
    upgrade("relay", url)

    async def heartbeat_columns() -> set[str]:
        engine = make_engine(url)
        async with engine.connect() as connection:
            columns = await connection.run_sync(
                lambda sync: inspect(sync).get_columns("relay_worker_heartbeat")
            )
        await engine.dispose()
        return {str(column["name"]) for column in columns}

    tables, versions = asyncio.run(service_schema(url, "relay"))
    assert "relay_worker_heartbeat" in tables
    assert asyncio.run(heartbeat_columns()) == {"singleton_id", "observed_at", "ready"}
    assert versions == ["0002_relay_worker_heartbeat"]


def test_concurrent_first_upgrades_are_serialised() -> None:
    url = database_url()
    asyncio.run(clear_service_schema(url, "relay"))
    environment = os.environ | {"TWINS_DATABASE_URL": url}

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                lambda _index: subprocess.run(
                    [sys.executable, "-m", "enterprise_twins.migration_runner", "relay"],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=environment,
                ),
                range(4),
            )
        )

    tables, versions = asyncio.run(service_schema(url, "relay"))
    assert [(result.returncode, result.stderr) for result in results] == [
        (0, ""),
        (0, ""),
        (0, ""),
        (0, ""),
    ]
    assert set(selected_metadata("relay").tables) <= tables
    assert versions == ["0002_relay_worker_heartbeat"]
