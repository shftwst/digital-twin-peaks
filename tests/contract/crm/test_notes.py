import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import (
    AuditRecord,
    IdempotencyRecord,
    OutboxRecord,
    ScenarioState,
)
from enterprise_twins.common.http.errors import ErrorCode
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.scenario import CrmScenarioLoader


class ControlState(Protocol):
    epoch: str
    decision: FaultDecision


class CrmHarness(Protocol):
    client: AsyncClient
    support_headers: dict[str, str]
    future_epoch_headers: dict[str, str]
    control: ControlState


async def count(
    db: async_sessionmaker[AsyncSession],
    model: type[object],
) -> int:
    async with db() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


async def wait_for_lock_waiters(
    db: async_sessionmaker[AsyncSession],
    expected: int,
    tasks: list[asyncio.Task[object]],
) -> None:
    for _ in range(100):
        async with db() as session:
            waiters = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            )
        if waiters is not None and waiters >= expected:
            return
        if any(task.done() for task in tasks):
            break
    raise AssertionError(f"expected {expected} PostgreSQL lock waiter(s)")


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
async def test_note_create_replays_original_result_and_changed_payload_conflicts_atomically(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    headers = support_headers | {"Idempotency-Key": "note-idem-1", "If-Match": '"1"'}
    first = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Customer prefers email", "association": "account"},
    )
    replay = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Customer prefers email", "association": "account"},
    )
    changed = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Changed body", "association": "account"},
    )
    notes = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        headers=support_headers,
    )

    assert first.status_code == replay.status_code == 201
    assert first.json() == replay.json()
    assert first.headers["Idempotency-Replayed"] == "false"
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert first.headers["X-Customer-Version"] == "2"
    assert replay.headers["X-Customer-Version"] == "2"
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "conflict"
    assert [note["body"] for note in notes.json()["items"]] == ["Customer prefers email"]

    async with db() as session:
        customer = await session.scalar(
            select(Customer).where(
                Customer.scenario_epoch == "epoch_1",
                Customer.customer_id == "cus_unique",
            )
        )
        created_audits = list(
            await session.scalars(
                select(AuditRecord).where(AuditRecord.action == "crm.note.created")
            )
        )
        outbox = list(await session.scalars(select(OutboxRecord)))
        idempotency = list(await session.scalars(select(IdempotencyRecord)))
    assert customer is not None
    assert customer.version == 2
    assert len(created_audits) == len(outbox) == len(idempotency) == 1
    assert outbox[0].event_type == "crm.note.created"
    assert outbox[0].envelope["occurredAt"] == "2026-08-19T10:00:00Z"
    assert outbox[0].envelope["recordedAt"] == "2026-08-19T10:00:00Z"
    assert outbox[0].envelope["data"]["noteId"] == first.json()["noteId"]
    assert created_audits[0].resource_id == first.json()["noteId"]


@pytest.mark.asyncio
async def test_concurrent_same_key_note_calls_converge_on_one_result_and_one_commit(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_crm_idempotency_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7201); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_crm_idempotency_insert "
                "BEFORE INSERT ON idempotency_records FOR EACH ROW "
                "EXECUTE FUNCTION gate_crm_idempotency_insert()"
            )
        )
    headers = support_headers | {"Idempotency-Key": "note-race", "If-Match": '"1"'}
    payload = {"body": "Concurrent fact", "association": "account"}

    async with advisory_gate(db, 7201):
        first = asyncio.create_task(
            crm_client.post("/v1/customers/cus_unique/notes", headers=headers, json=payload)
        )
        second = asyncio.create_task(
            crm_client.post("/v1/customers/cus_unique/notes", headers=headers, json=payload)
        )
        await wait_for_lock_waiters(db, 2, [first, second])
    responses = await asyncio.gather(first, second)

    assert [response.status_code for response in responses] == [201, 201]
    assert responses[0].json() == responses[1].json()
    assert sorted(response.headers["Idempotency-Replayed"] for response in responses) == [
        "false",
        "true",
    ]
    assert await count(db, CustomerNote) == 2
    assert await count(db, IdempotencyRecord) == 1
    assert await count(db, OutboxRecord) == 1
    async with db() as session:
        created_audits = await session.scalar(
            select(func.count())
            .select_from(AuditRecord)
            .where(AuditRecord.action == "crm.note.created")
        )
        customer = await session.scalar(
            select(Customer).where(Customer.customer_id == "cus_unique")
        )
    assert created_audits == 1
    assert customer is not None
    assert customer.version == 2


@pytest.mark.asyncio
async def test_stale_customer_version_and_missing_scope_do_not_create_notes(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    read_only_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    stale = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=support_headers | {"Idempotency-Key": "note-idem-2", "If-Match": '"0"'},
        json={"body": "stale", "association": "account"},
    )
    forbidden = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=read_only_headers | {"Idempotency-Key": "note-idem-3", "If-Match": '"1"'},
        json={"body": "forbidden", "association": "account"},
    )

    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "conflict"
    assert forbidden.status_code == 403
    assert forbidden.json()["error"]["details"] == {"requiredScopes": ["crm:notes:write"]}
    assert await count(db, IdempotencyRecord) == 0
    assert await count(db, OutboxRecord) == 0
    assert await count(db, CustomerNote) == 1


@pytest.mark.asyncio
async def test_note_write_requires_local_active_mode_and_matching_control_epoch(
    crm_harness: CrmHarness,
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        state = await session.get(ScenarioState, 1, with_for_update=True)
        assert state is not None
        state.mode = "preparing"
    inactive = await crm_harness.client.post(
        "/v1/customers/cus_unique/notes",
        headers=crm_harness.support_headers
        | {"Idempotency-Key": "note-inactive", "If-Match": '"1"'},
        json={"body": "must not commit", "association": "account"},
    )
    async with db.begin() as session:
        state = await session.get(ScenarioState, 1, with_for_update=True)
        assert state is not None
        state.mode = "active"
    crm_harness.control.epoch = "epoch_2"
    mismatched = await crm_harness.client.post(
        "/v1/customers/cus_unique/notes",
        headers=crm_harness.future_epoch_headers
        | {"Idempotency-Key": "note-wrong-epoch", "If-Match": '"1"'},
        json={"body": "must not cross epochs", "association": "account"},
    )

    assert inactive.status_code == mismatched.status_code == 503
    assert inactive.json()["error"]["code"] == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert mismatched.json()["error"]["code"] == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert await count(db, IdempotencyRecord) == 0
    assert await count(db, OutboxRecord) == 0
    assert await count(db, CustomerNote) == 1


@pytest.mark.asyncio
async def test_reset_prepare_waits_for_the_complete_note_transaction(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_crm_note_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7202); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_crm_note_insert "
                "BEFORE INSERT ON crm_customer_notes FOR EACH ROW "
                "EXECUTE FUNCTION gate_crm_note_insert()"
            )
        )
    participant = ResetParticipant(db, CrmScenarioLoader(), service="crm")

    async with advisory_gate(db, 7202):
        note_task = asyncio.create_task(
            crm_client.post(
                "/v1/customers/cus_unique/notes",
                headers=support_headers
                | {"Idempotency-Key": "note-reset-fence", "If-Match": '"1"'},
                json={"body": "commit before reset", "association": "account"},
            )
        )
        await wait_for_lock_waiters(db, 1, [note_task])
        prepare_task = asyncio.create_task(participant.prepare("epoch_2"))
        await wait_for_lock_waiters(db, 2, [note_task, prepare_task])
        assert prepare_task.done() is False
    response, _ = await asyncio.gather(note_task, prepare_task)

    assert response.status_code == 201
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        customer = await session.scalar(
            select(Customer).where(Customer.customer_id == "cus_unique")
        )
        note = await session.scalar(
            select(CustomerNote).where(CustomerNote.body == "commit before reset")
        )
    assert state is not None
    assert state.mode == "preparing"
    assert state.pending_epoch == "epoch_2"
    assert customer is not None
    assert customer.version == 2
    assert note is not None


@pytest.mark.asyncio
async def test_after_commit_malformed_response_replays_the_committed_result(
    crm_harness: CrmHarness,
    db: async_sessionmaker[AsyncSession],
) -> None:
    crm_harness.control.decision = FaultDecision(effect=FaultEffect.MALFORMED_RESPONSE)
    headers = crm_harness.support_headers | {
        "Idempotency-Key": "note-malformed",
        "If-Match": '"1"',
    }
    first = await crm_harness.client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "committed despite response", "association": "account"},
    )
    crm_harness.control.decision = FaultDecision()
    replay = await crm_harness.client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "committed despite response", "association": "account"},
    )

    assert first.status_code == 200
    assert first.content == b"{"
    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["body"] == "committed despite response"
    assert await count(db, CustomerNote) == 2
    assert await count(db, IdempotencyRecord) == 1
    assert await count(db, OutboxRecord) == 1


@pytest.mark.asyncio
async def test_notes_have_no_update_or_delete_business_route(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    headers = support_headers | {"Idempotency-Key": "note-append-only", "If-Match": '"1"'}
    created = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "append only", "association": "account"},
    )
    note_id = created.json()["noteId"]
    patch = await crm_client.patch(
        f"/v1/customers/cus_unique/notes/{note_id}",
        headers=headers,
        json={"body": "rewritten"},
    )
    delete = await crm_client.delete(
        f"/v1/customers/cus_unique/notes/{note_id}",
        headers=headers,
    )

    assert created.status_code == 201
    assert patch.status_code == delete.status_code == 404
