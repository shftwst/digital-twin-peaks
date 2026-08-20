# ruff: noqa: S106

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.scenario import ScenarioAccess
from enterprise_twins.common.auth.verifier import BearerAuthenticator
from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ClockValue,
    FaultDecision,
    ParticipantLoadRequest,
)
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import AuditRecord, ScenarioState
from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.crm.api import crm_router
from enterprise_twins.services.crm.app import CrmStatus
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.repository import CustomerRepository
from enterprise_twins.services.crm.scenario import CrmScenarioLoader
from enterprise_twins.services.crm.service import CrmService


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


async def wait_for_lock_waiters(
    db: async_sessionmaker[AsyncSession],
    expected: int,
    tasks: list[asyncio.Task[object]],
) -> bool:
    for _ in range(100):
        async with db() as session:
            waiters = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            )
        if waiters is not None and waiters >= expected:
            return True
        if any(task.done() for task in tasks):
            return False
    return False


@asynccontextmanager
async def advisory_gate(
    db: async_sessionmaker[AsyncSession],
    lock_id: int,
) -> AsyncIterator[None]:
    async with db() as session:
        await session.begin()
        await session.execute(text(f"SELECT pg_advisory_xact_lock({lock_id})"))
        try:
            yield
        finally:
            await session.commit()


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
@pytest.mark.parametrize("operation", ["search", "get", "list_notes"])
async def test_reset_prepare_waits_for_the_complete_crm_read_transaction(
    db: async_sessionmaker[AsyncSession],
    operation: str,
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
        session.add(
            Customer(
                row_id="crow_read_fence",
                scenario_epoch="epoch_old",
                customer_id="cus_read_fence",
                display_name="Read Fence",
                primary_email="read-fence@example.test",
                external_reference="ext-read-fence",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
        session.add(
            CustomerNote(
                row_id="nrow_read_fence",
                note_id="note_read_fence",
                scenario_epoch="epoch_old",
                customer_id="cus_read_fence",
                body="read fence note",
                association="account",
                created_by="person-support-1",
                created_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
                archived=False,
                version=1,
            )
        )
    repository = CustomerRepository(db, "crm-test-cursor")
    participant = ResetParticipant(db, CrmScenarioLoader(), service="crm")
    engine = db.kw["bind"]

    def gate_customer_read(
        _connection: object,
        cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if "from crm_customers" in statement.casefold():
            cursor.execute("SELECT pg_advisory_xact_lock(7301)")  # type: ignore[attr-defined]

    event.listen(engine.sync_engine, "before_cursor_execute", gate_customer_read)
    try:
        async with advisory_gate(db, 7301):
            if operation == "search":
                read_task = asyncio.create_task(
                    repository.search(
                        email=None,
                        external_reference=None,
                        identifier=None,
                        limit=50,
                        after=None,
                    )
                )
            elif operation == "get":
                read_task = asyncio.create_task(repository.get("cus_read_fence"))
            else:
                read_task = asyncio.create_task(
                    repository.list_notes("cus_read_fence", False, 50, None)
                )
            assert await wait_for_lock_waiters(db, 1, [read_task]) is True
            prepare_task = asyncio.create_task(participant.prepare("epoch_new"))
            reset_was_fenced = await wait_for_lock_waiters(db, 2, [read_task, prepare_task])
        read_result, _ = await asyncio.gather(read_task, prepare_task)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", gate_customer_read)

    assert reset_was_fenced is True
    if operation == "search":
        assert [item.customer_id for item in read_result.items] == ["cus_read_fence"]
    elif operation == "get":
        assert read_result.customer_id == "cus_read_fence"
    else:
        assert [item.note_id for item in read_result.items] == ["note_read_fence"]


@pytest.mark.asyncio
async def test_authenticated_old_epoch_read_released_after_reset_cannot_read_new_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class BarrierRepository(CustomerRepository):
        async def search(self, *args: object, **kwargs: object) -> object:
            entered.set()
            await release.wait()
            return await super().search(*args, **kwargs)  # type: ignore[arg-type]

    class OldPrincipalVerifier:
        async def verify(self, _token: str) -> Principal:
            return Principal(
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=frozenset({"crm:read"}),
                tenant_id="tenant_synthetic",
                token_id="tok_old",
                scenario_epoch="epoch_old",
            )

    class Control:
        epoch = "epoch_old"

        async def snapshot(self) -> ClockValue:
            return ClockValue(
                now=datetime(2026, 8, 19, 10, tzinfo=UTC),
                scenarioEpoch=self.epoch,
            )

        async def now(self) -> datetime:
            return datetime(2026, 8, 19, 10, tzinfo=UTC)

        async def current_epoch(self) -> str:
            return self.epoch

        async def ready_epoch(self) -> str:
            return self.epoch

        async def evaluate_fault(self, _probe: object) -> FaultDecision:
            return FaultDecision()

    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
        session.add(
            Customer(
                row_id="crow_old_barrier",
                scenario_epoch="epoch_old",
                customer_id="cus_old_barrier",
                display_name="Old Barrier",
                primary_email="old-barrier@example.test",
                external_reference="ext-old-barrier",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
    control = Control()
    repository = BarrierRepository(db, "crm-test-cursor")
    app = create_app(
        "CRM epoch barrier probe",
        (),
        CrmStatus(db, control),
        (
            crm_router(
                repository,
                CrmService(db, control),
                BearerAuthenticator(OldPrincipalVerifier()),  # type: ignore[arg-type]
                None,
                ScenarioAccess("CRM", db),
            ),
        ),
    )
    participant = ResetParticipant(db, CrmScenarioLoader(), service="crm")
    payload = scenario_payload()
    request = ParticipantLoadRequest(
        scenarioEpoch="epoch_new",
        scenarioId="platform-contracts",
        scenarioVersion=1,
        randomSeed=7,
        payload=payload,
        checksum=sha256_hex(payload),
        manifestChecksum="b" * 64,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://crm") as client:
        request_task = asyncio.create_task(
            client.get(
                "/v1/customers",
                headers={
                    "Authorization": "Bearer old-token",
                    "X-Correlation-Id": "old-principal-barrier",
                },
            )
        )
        await entered.wait()
        await participant.prepare("epoch_new")
        await participant.load(request)
        await participant.commit("epoch_new")
        await participant.finalize("epoch_new")
        control.epoch = "epoch_new"
        release.set()
        response = await request_task

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.headers["X-Scenario-Epoch"] == "epoch_new"


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
async def test_business_read_reports_control_outage_as_retryable_503(
    crm_harness: object,
) -> None:
    harness = crm_harness
    harness.control.available = False  # type: ignore[attr-defined]

    response = await harness.client.get(  # type: ignore[attr-defined]
        "/v1/customers/cus_unique",
        headers=harness.support_headers,  # type: ignore[attr-defined]
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.json()["error"]["retryable"] is True


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

    class ReadyControl:
        async def ready_epoch(self) -> str:
            return "epoch_new"

    status = CrmStatus(db, ReadyControl())
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
        {"database": "ready", "scenario": "active", "control": "ready"},
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
