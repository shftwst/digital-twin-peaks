import os

import httpx
import pytest

pytestmark = pytest.mark.integration


def live_settings() -> tuple[str, dict[str, str]]:
    if os.environ.get("RUN_LIVE_TOPOLOGY_TESTS") != "1":
        pytest.skip("set RUN_LIVE_TOPOLOGY_TESTS=1 to run live reset tests")
    control = os.environ.get("CONTROL_URL")
    token = os.environ.get("CONTROL_TOKEN")
    if control is None or token is None:
        pytest.skip("CONTROL_URL and CONTROL_TOKEN are required for live reset tests")
    return control.rstrip("/"), {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_same_seed_has_same_checksum() -> None:
    control, headers = live_settings()
    reset_body = {"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7}
    async with httpx.AsyncClient(timeout=10.0) as client:
        first = await client.post(f"{control}/control/v1/reset", headers=headers, json=reset_body)
        first.raise_for_status()
        second = await client.post(f"{control}/control/v1/reset", headers=headers, json=reset_body)
        second.raise_for_status()

    assert first.json()["manifestChecksum"] == second.json()["manifestChecksum"]
    assert first.json()["scenarioEpoch"] != second.json()["scenarioEpoch"]
    assert {report["service"] for report in second.json()["reports"]} == {
        "identity",
        "crm",
        "relay",
    }


async def support_token(client: httpx.AsyncClient) -> str:
    response = await client.post(
        "http://identity:8000/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "support-secret",
            "scope": "crm:read crm:notes:write",
        },
    )
    response.raise_for_status()
    return str(response.json()["access_token"])


@pytest.mark.asyncio
async def test_reset_clears_created_note() -> None:
    control, control_headers = live_settings()
    reset_body = {"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7}
    async with httpx.AsyncClient(timeout=10.0) as client:
        first_reset = await client.post(
            f"{control}/control/v1/reset",
            headers=control_headers,
            json=reset_body,
        )
        first_reset.raise_for_status()
        token = await support_token(client)
        business_headers = {
            "Authorization": f"Bearer {token}",
            "X-Correlation-Id": "case-reset-test",
        }
        customer = await client.get(
            "http://crm:8000/v1/customers/cus_unique", headers=business_headers
        )
        customer.raise_for_status()
        created = await client.post(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers=business_headers
            | {"Idempotency-Key": "reset-note", "If-Match": customer.headers["ETag"]},
            json={"body": "removed by reset", "association": "account"},
        )
        assert created.status_code == 201
        before = await client.get(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers=business_headers,
        )
        before.raise_for_status()
        assert len(before.json()["items"]) == 1
        second_reset = await client.post(
            f"{control}/control/v1/reset",
            headers=control_headers,
            json=reset_body,
        )
        second_reset.raise_for_status()
        new_token = await support_token(client)
        after = await client.get(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers={
                "Authorization": f"Bearer {new_token}",
                "X-Correlation-Id": "case-reset-test-after",
            },
        )
        after.raise_for_status()

    assert after.json()["items"] == []
