import asyncio

from alembic import context
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from enterprise_twins.migration_metadata import selected_metadata
from enterprise_twins.migration_runner import migration_lock_key


def run_sync_migrations(connection: Connection, service: str) -> None:
    with connection.begin():
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": migration_lock_key(service)},
        )
        context.configure(
            connection=connection,
            target_metadata=selected_metadata(service),
            version_table=f"alembic_version_{service}",
        )
        with context.begin_transaction():
            context.run_migrations()


async def run() -> None:
    service = str(context.config.attributes["migration_service"])
    configuration = context.config.get_section(context.config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = str(context.config.attributes["database_url"])
    engine = async_engine_from_config(configuration, prefix="sqlalchemy.")
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_sync_migrations, service)
    finally:
        await engine.dispose()


asyncio.run(run())
