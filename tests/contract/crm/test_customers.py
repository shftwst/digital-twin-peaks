import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import ParticipantLoadRequest
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.crm.app import CrmStatus
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.scenario import CrmScenarioLoader


@pytest.mark.asyncio
async def test_exact_search_returns_zero_one_or_many_in_stable_order_without_ranking(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    unique = await crm_client.get(
        "/v1/customers",
        params={"email": "alex.unique@example.test"},
        headers=support_headers,
    )
    ambiguous = await crm_client.get(
        "/v1/customers",
        params={"email": "SHARED@example.test"},
        headers=support_headers,
    )
    absent = await crm_client.get(
        "/v1/customers",
        params={"externalReference": "missing"},
        headers=support_headers,
    )
    partial = await crm_client.get(
        "/v1/customers",
        params={"email": "shared@"},
        headers=support_headers,
    )
    loyalty = await crm_client.get(
        "/v1/customers",
        params={"identifier": "LOY-2002"},
        headers=support_headers,
    )

    assert unique.status_code == ambiguous.status_code == absent.status_code == 200
    assert partial.status_code == loyalty.status_code == 200
    assert [item["customerId"] for item in unique.json()["items"]] == ["cus_unique"]
    assert [item["customerId"] for item in ambiguous.json()["items"]] == [
        "cus_ambiguous_a",
        "cus_ambiguous_b",
    ]
    assert absent.json()["items"] == []
    assert partial.json()["items"] == []
    assert [item["customerId"] for item in loyalty.json()["items"]] == ["cus_ambiguous_b"]


@pytest.mark.asyncio
async def test_search_pagination_is_stable_and_rejects_a_changed_cursor(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    first = await crm_client.get(
        "/v1/customers",
        params={"limit": 2},
        headers=support_headers,
    )
    cursor = first.json()["nextCursor"]
    second = await crm_client.get(
        "/v1/customers",
        params={"limit": 2, "after": cursor},
        headers=support_headers,
    )
    changed_cursor = cursor[:-1] + ("A" if cursor[-1] != "A" else "B")
    changed = await crm_client.get(
        "/v1/customers",
        params={"after": changed_cursor},
        headers=support_headers,
    )

    assert [item["customerId"] for item in first.json()["items"]] == [
        "cus_ambiguous_a",
        "cus_ambiguous_b",
    ]
    assert [item["customerId"] for item in second.json()["items"]] == ["cus_unique"]
    assert second.json()["nextCursor"] is None
    assert changed.status_code == 422
    assert changed.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_customer_reads_are_active_epoch_scoped_and_versioned(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    current = await crm_client.get(
        "/v1/customers/cus_unique",
        headers=support_headers,
    )
    hidden_search = await crm_client.get(
        "/v1/customers",
        params={"externalReference": "ext-old"},
        headers=support_headers,
    )
    hidden_customer = await crm_client.get(
        "/v1/customers/cus_old_hidden",
        headers=support_headers,
    )
    hidden_notes = await crm_client.get(
        "/v1/customers/cus_old_hidden/notes",
        headers=support_headers,
    )

    assert current.status_code == 200
    assert current.headers["ETag"] == '"1"'
    assert current.headers["X-Resource-Version"] == "1"
    assert hidden_search.json()["items"] == []
    assert hidden_customer.status_code == hidden_notes.status_code == 404


@pytest.mark.asyncio
async def test_business_reads_stop_when_the_local_scenario_is_not_active(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        state = await session.get(ScenarioState, 1, with_for_update=True)
        assert state is not None
        state.mode = "preparing"

    response = await crm_client.get("/v1/customers", headers=support_headers)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"


@pytest.mark.asyncio
async def test_crm_reset_loads_the_new_epoch_and_discards_the_old_epoch_on_finalize(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
                scenario_id="old-scenario",
                scenario_version=2,
                random_seed=3,
                manifest_checksum="a" * 64,
            )
        )
        session.add(
            Customer(
                row_id="crow_old_reset",
                scenario_epoch="epoch_old",
                customer_id="cus_old",
                display_name="Old customer",
                primary_email="old@example.test",
                external_reference="ext-old",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
    participant = ResetParticipant(db, CrmScenarioLoader(), service="crm")
    status = CrmStatus(db)
    payload = {
        "schemaVersion": "1",
        "customers": [
            {
                "customerId": "cus_new",
                "displayName": "New Customer",
                "primaryEmail": "NEW@EXAMPLE.TEST",
                "externalReference": "ext-new",
                "accountStatus": "active",
                "contactMethods": [{"type": "email", "value": "new@example.test", "primary": True}],
                "externalIdentifiers": {"loyalty": "LOY-NEW"},
                "version": 4,
            }
        ],
        "notes": [
            {
                "noteId": "note_seeded",
                "customerId": "cus_new",
                "body": "Seeded account fact",
                "association": "account",
                "createdBy": "person-support-1",
                "createdAt": "2026-08-19T09:00:00Z",
            }
        ],
        "aliases": {"primaryCustomer": "cus_new"},
    }
    request = ParticipantLoadRequest(
        scenarioEpoch="epoch_new",
        scenarioId="platform-contracts",
        scenarioVersion=1,
        randomSeed=7,
        payload=payload,
        checksum=sha256_hex(payload),
        manifestChecksum="b" * 64,
    )

    await participant.prepare("epoch_new")
    report = await participant.load(request)
    await participant.commit("epoch_new")
    assert (await status.readiness())[0] is False
    await participant.finalize("epoch_new")

    assert report.counts == {"customers": 1, "notes": 1}
    assert report.aliases == {"primaryCustomer": "cus_new"}
    assert await status.readiness() == (
        True,
        {"database": "ready", "scenario": "active"},
    )
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        customers = list(await session.scalars(select(Customer)))
        notes = list(await session.scalars(select(CustomerNote)))
    assert state is not None
    assert state.active_epoch == "epoch_new"
    assert state.pending_epoch is None
    assert state.rollback_epoch is None
    assert [customer.customer_id for customer in customers] == ["cus_new"]
    assert customers[0].primary_email == "new@example.test"
    assert [note.note_id for note in notes] == ["note_seeded"]
