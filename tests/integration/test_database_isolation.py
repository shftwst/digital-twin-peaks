import os

import asyncpg
import pytest


def _identity_url(database: str) -> str:
    return os.environ.get(
        "IDENTITY_DATABASE_URL",
        f"postgresql://identity_user:identity_local_only@postgres/{database}",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_identity_login_is_limited_to_identity_database() -> None:
    identity = await asyncpg.connect(_identity_url("identity"))
    try:
        result = await identity.fetchrow("select current_user, current_database()")
        assert result["current_user"] == "identity_user"
        assert result["current_database"] == "identity"
    finally:
        await identity.close()

    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await asyncpg.connect(_identity_url("crm"))
