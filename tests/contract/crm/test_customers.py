from copy import deepcopy
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import ParticipantLoadRequest
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import AuditRecord, ScenarioState
from enterprise_twins.services.crm.app import CrmStatus
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.scenario import CrmScenarioLoader


def scenario_payload() -> dict[str, object]:
    return {
        "schemaVersion": "1",
        "customers": [
            {
                "customerId": "cus_new",
                "displayName": "New Customer",
                "primaryEmail": "new@example.test",
                "externalReference": "ext-new",
                "accountStatus": "active",
                "contactMethods": [],
                "externalIdentifiers": {"loyalty": "LOY-NEW"},
                "version": 1,
            }
        ],
        "notes": [
            {
                "noteId": "note_new",
                "customerId": "cus_new",
                "body": "New account fact",
                "association": "account",
                "createdBy": "person-support-1",
                "createdAt": "2026-08-19T11:30:00+01:30",
                "version": 1,
            }
        ],
        "aliases": {},
    }


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
async def test_overlong_correlation_id_is_rejected_before_auth_audit_or_crm_read(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    response = await crm_client.get(
        "/v1/customers/cus_unique",
        headers=support_headers | {"X-Correlation-Id": "c" * 129},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert await session_count(db, AuditRecord) == 0


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
    payload_part, signature_part = cursor.split(".")
    malformed = [
        f"{payload_part}*.{signature_part}",
        f"{cursor}.extra",
        f"{payload_part}=.{signature_part}",
    ]
    malformed_responses = [
        await crm_client.get(
            "/v1/customers",
            params={"after": value},
            headers=support_headers,
        )
        for value in malformed
    ]

    assert [item["customerId"] for item in first.json()["items"]] == [
        "cus_ambiguous_a",
        "cus_ambiguous_b",
    ]
    assert [item["customerId"] for item in second.json()["items"]] == ["cus_unique"]
    assert second.json()["nextCursor"] is None
    assert changed.status_code == 422
    assert changed.json()["error"]["code"] == "invalid_request"
    assert [response.status_code for response in malformed_responses] == [422, 422, 422]
    assert all(
        response.json()["error"]["code"] == "invalid_request" for response in malformed_responses
    )


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


async def session_count(
    db: async_sessionmaker[AsyncSession],
    model: type[object],
) -> int:
    async with db() as session:
        return len(list(await session.scalars(select(model))))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_message"),
    [
        (("schemaVersion",), "schemaVersion"),
        (("customers", 0, "version", True), "version"),
        (("customers", 0, "version", 0), "version"),
        (("customers", 0, "version", "1"), "version"),
        (("notes", 0, "version", True), "version"),
        (("notes", 0, "version", 0), "version"),
        (("notes", 0, "version", "1"), "version"),
        (("notes", 0, "createdAt", "2026-08-19"), "createdAt"),
        (("notes", 0, "createdAt", "2026-08-19T10:00:00"), "createdAt"),
        (("notes", 0, "createdAt", "not-a-timestamp"), "createdAt"),
    ],
)
async def test_scenario_loader_rejects_invalid_core_semantics_before_staging(
    db: async_sessionmaker[AsyncSession],
    mutation: tuple[object, ...],
    expected_message: str,
) -> None:
    payload = scenario_payload()
    if mutation == ("schemaVersion",):
        payload["schemaVersion"] = "2"
    else:
        collection = payload[mutation[0]]
        assert isinstance(collection, list)
        item = collection[mutation[1]]
        assert isinstance(item, dict)
        item[mutation[2]] = mutation[3]

    async with db() as session:
        with pytest.raises(ValueError, match=expected_message):
            await CrmScenarioLoader().load(session, "epoch_new", payload)
        assert not session.new


@pytest.mark.asyncio
async def test_scenario_loader_requires_unique_note_ids_and_normalises_created_at(
    db: async_sessionmaker[AsyncSession],
) -> None:
    duplicate = scenario_payload()
    notes = duplicate["notes"]
    assert isinstance(notes, list)
    notes.append(deepcopy(notes[0]))
    async with db() as session:
        with pytest.raises(ValueError, match="note IDs must be unique"):
            await CrmScenarioLoader().load(session, "epoch_new", duplicate)
        assert not session.new

    async with db() as session:
        await CrmScenarioLoader().load(session, "epoch_new", scenario_payload())
        note = next(item for item in session.new if isinstance(item, CustomerNote))
        assert note.created_at == datetime(2026, 8, 19, 10, tzinfo=UTC)


@pytest.mark.asyncio
async def test_invalid_scenario_payload_does_not_activate_or_stage_the_pending_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
            )
        )
        session.add(
            Customer(
                row_id="crow_old",
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
    payload = scenario_payload()
    payload["schemaVersion"] = "2"
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
    with pytest.raises(ValueError, match="schemaVersion"):
        await participant.load(request)

    async with db() as session:
        state = await session.get(ScenarioState, 1)
        customers = list(await session.scalars(select(Customer)))
        notes = list(await session.scalars(select(CustomerNote)))
    assert state is not None
    assert state.mode == "preparing"
    assert state.active_epoch == "epoch_old"
    assert state.pending_epoch == "epoch_new"
    assert [customer.customer_id for customer in customers] == ["cus_old"]
    assert notes == []
