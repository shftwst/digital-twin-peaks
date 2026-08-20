import os

import httpx
import jwt
import pytest

pytestmark = pytest.mark.integration


def live_business_urls() -> tuple[str, str, str]:
    if os.environ.get("RUN_LIVE_TOPOLOGY_TESTS") != "1":
        pytest.skip("set RUN_LIVE_TOPOLOGY_TESTS=1 to run live discovery tests")
    if os.environ.get("CONTROL_URL"):
        return "http://identity:8000", "http://crm:8000", "http://identity:8000"
    return (
        "http://127.0.0.1:8101",
        "http://127.0.0.1:8102",
        "http://127.0.0.1:8101",
    )


@pytest.mark.asyncio
async def test_discovered_identity_token_works_from_the_current_host_or_container_context() -> None:
    identity_url, crm_url, expected_issuer = live_business_urls()
    async with httpx.AsyncClient(timeout=5.0) as client:
        discovery = await client.get(f"{identity_url}/.well-known/openid-configuration")
        discovery.raise_for_status()
        metadata = discovery.json()
        token_response = await client.post(
            metadata["token_endpoint"],
            data={
                "grant_type": "client_credentials",
                "client_id": "support-agent",
                "client_secret": "support-secret",
                "scope": "crm:read crm:notes:write",
            },
        )
        token_response.raise_for_status()
        token = token_response.json()["access_token"]
        jwks = await client.get(metadata["jwks_uri"])
        me = await client.get(
            f"{identity_url}/v1/me",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Correlation-Id": "discovery-context-identity",
            },
        )
        crm = await client.get(
            f"{crm_url}/v1/customers",
            params={"email": "alex.unique@example.test"},
            headers={
                "Authorization": f"Bearer {token}",
                "X-Correlation-Id": "discovery-context-crm",
            },
        )

    claims = jwt.decode(token, options={"verify_signature": False})
    assert metadata["issuer"] == expected_issuer
    assert metadata["scopes_supported"] == [
        "crm:notes:write",
        "crm:read",
        "webhooks:manage",
    ]
    assert claims["iss"] == expected_issuer
    assert jwks.status_code == 200
    assert jwks.json()["keys"]
    assert me.status_code == 200
    assert me.json()["subject"] == "person-support-1"
    assert crm.status_code == 200
    assert [item["customerId"] for item in crm.json()["items"]] == ["cus_unique"]
