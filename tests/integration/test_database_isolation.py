import os
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest


def _identity_url(database: str) -> str:
    identity_url = os.environ.get("IDENTITY_DATABASE_URL")
    if identity_url is None:
        pytest.skip("IDENTITY_DATABASE_URL is required for the live database check")
    return urlunsplit(urlsplit(identity_url)._replace(path=f"/{database}"))


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
