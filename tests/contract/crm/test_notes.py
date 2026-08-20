# ruff: noqa: S106

import asyncio
import base64
import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import ClientDisconnect
from starlette.types import ASGIApp, Message, Scope

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.canonical import canonical_json
from enterprise_twins.common.control.contracts import ClockValue, FaultDecision, FaultEffect
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import (
    AuditRecord,
    IdempotencyRecord,
    OutboxRecord,
    ScenarioState,
)
from enterprise_twins.common.http.context import RequestContext, current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.scenario import CrmScenarioLoader
from enterprise_twins.services.crm.schemas import NoteCreate
from enterprise_twins.services.crm.service import CrmService


class ControlState(Protocol):
    epoch: str
    decision: FaultDecision


class CrmHarness(Protocol):
    app: ASGIApp
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


def encode_segment(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def signed_note_cursor(payload: dict[str, object]) -> str:
    body = canonical_json(payload)
    signature = hmac.new(b"crm-test-cursor", body, hashlib.sha256).digest()
    return f"{encode_segment(body)}.{encode_segment(signature)}"


async def invoke_note_create(
    app: ASGIApp,
    headers: dict[str, str],
    payload: dict[str, str],
) -> tuple[list[Message], BaseException | None]:
    request_sent = False

    async def receive() -> Message:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {
                "type": "http.request",
                "body": json.dumps(payload).encode(),
                "more_body": False,
            }
        return {"type": "http.disconnect"}

    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/customers/cus_unique/notes",
        "raw_path": b"/v1/customers/cus_unique/notes",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (name.lower().encode(), value.encode())
            for name, value in (headers | {"Content-Type": "application/json"}).items()
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("crm", 8000),
    }
    failure: BaseException | None = None
    try:
        await app(scope, receive, send)
    except (ConnectionResetError, ClientDisconnect, ExceptionGroup) as error:
        failure = error
    return messages, failure


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
async def test_note_create_uses_one_atomic_control_snapshot(
    db: async_sessionmaker[AsyncSession],
) -> None:
    snapshot_time = datetime(2026, 8, 19, 10, 5, tzinfo=UTC)

    class SplitControl:
        async def snapshot(self) -> ClockValue:
            return ClockValue(now=snapshot_time, scenarioEpoch="epoch_1")

        async def now(self) -> datetime:
            return datetime(2026, 8, 18, 9, tzinfo=UTC)

        async def current_epoch(self) -> str:
            return "epoch_1"

        async def ready_epoch(self) -> str:
            return "epoch_1"

        async def evaluate_fault(self, _probe: object) -> FaultDecision:
            return FaultDecision()

    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            Customer(
                row_id="crow_atomic_snapshot",
                scenario_epoch="epoch_1",
                customer_id="cus_atomic_snapshot",
                display_name="Atomic Snapshot",
                primary_email="atomic@example.test",
                external_reference="ext-atomic",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
    principal = Principal(
        subject="person-support-1",
        actor_type="human",
        role="support_agent",
        scopes=frozenset({"crm:notes:write"}),
        tenant_id="tenant_synthetic",
        token_id="tok_atomic",
        scenario_epoch="epoch_1",
    )
    context_token = current_request.set(RequestContext("req_atomic", "corr_atomic", None))
    try:
        await CrmService(db, SplitControl()).create_note(
            "cus_atomic_snapshot",
            NoteCreate(body="atomic time", association="account"),
            1,
            "atomic-idempotency",
            principal,
        )
    finally:
        current_request.reset(context_token)

    async with db() as session:
        note = await session.scalar(select(CustomerNote).where(CustomerNote.body == "atomic time"))
    assert note is not None
    assert note.created_at == snapshot_time


@pytest.mark.asyncio
async def test_old_epoch_principal_cannot_create_a_note_in_the_new_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    class NewEpochControl:
        async def snapshot(self) -> ClockValue:
            return ClockValue(now=datetime(2026, 8, 19, 10, tzinfo=UTC), scenarioEpoch="epoch_new")

        async def now(self) -> datetime:
            return datetime(2026, 8, 19, 10, tzinfo=UTC)

        async def current_epoch(self) -> str:
            return "epoch_new"

        async def ready_epoch(self) -> str:
            return "epoch_new"

        async def evaluate_fault(self, _probe: object) -> FaultDecision:
            return FaultDecision()

    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_new"))
        session.add(
            Customer(
                row_id="crow_new_epoch",
                scenario_epoch="epoch_new",
                customer_id="cus_new_epoch",
                display_name="New Epoch",
                primary_email="new-epoch@example.test",
                external_reference="ext-new-epoch",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
    old_principal = Principal(
        subject="person-support-1",
        actor_type="human",
        role="support_agent",
        scopes=frozenset({"crm:notes:write"}),
        tenant_id="tenant_synthetic",
        token_id="tok_old_epoch",
        scenario_epoch="epoch_old",
    )
    context_token = current_request.set(RequestContext("req_old", "corr_old", None))
    try:
        with pytest.raises(ApiError) as raised:
            await CrmService(db, NewEpochControl()).create_note(
                "cus_new_epoch",
                NoteCreate(body="must not cross epochs", association="account"),
                1,
                "old-principal-idempotency",
                old_principal,
            )
    finally:
        current_request.reset(context_token)

    assert raised.value.status_code == 503
    assert await count(db, CustomerNote) == 0
    assert await count(db, IdempotencyRecord) == 0


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
        await wait_for_lock_waiters(db, 1, [first])
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
async def test_note_create_rejects_noncanonical_if_match_before_business_work(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    invalid_values = ["1", '""1""', "+1", "-1", ' "1" ', '"01"']
    for index, value in enumerate(invalid_values):
        response = await crm_client.post(
            "/v1/customers/cus_unique/notes",
            headers=support_headers
            | {"Idempotency-Key": f"invalid-etag-{index}", "If-Match": value},
            json={"body": "must not commit", "association": "account"},
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_request"

    assert await count(db, CustomerNote) == 1
    assert await count(db, IdempotencyRecord) == 0
    assert await count(db, OutboxRecord) == 0
    async with db() as session:
        customer = await session.scalar(
            select(Customer).where(Customer.customer_id == "cus_unique")
        )
    assert customer is not None
    assert customer.version == 1


@pytest.mark.asyncio
async def test_webhook_delete_validates_if_match_before_relay_availability(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    response = await crm_client.delete(
        "/v1/webhook-subscriptions/sub_absent",
        headers=support_headers | {"Idempotency-Key": "invalid-proxy-etag", "If-Match": "1"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


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
async def test_after_commit_connection_loss_starts_response_and_retry_replays_commit(
    crm_harness: CrmHarness,
    db: async_sessionmaker[AsyncSession],
) -> None:
    crm_harness.control.decision = FaultDecision(effect=FaultEffect.CONNECTION_LOSS)
    headers = crm_harness.support_headers | {
        "Idempotency-Key": "note-connection-loss",
        "If-Match": '"1"',
    }
    messages, failure = await invoke_note_create(
        crm_harness.app,
        headers,
        {"body": "committed before disconnect", "association": "account"},
    )

    assert failure is not None
    starts = [message for message in messages if message["type"] == "http.response.start"]
    assert len(starts) == 1
    assert starts[0]["status"] == 201

    crm_harness.control.decision = FaultDecision()
    replay = await crm_harness.client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "committed before disconnect", "association": "account"},
    )

    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["body"] == "committed before disconnect"
    assert await count(db, CustomerNote) == 2
    assert await count(db, IdempotencyRecord) == 1
    assert await count(db, OutboxRecord) == 1
    async with db() as session:
        customer = await session.scalar(
            select(Customer).where(Customer.customer_id == "cus_unique")
        )
    assert customer is not None
    assert customer.version == 2


@pytest.mark.asyncio
async def test_note_pagination_uses_stable_timestamp_and_note_id_boundary(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    db: async_sessionmaker[AsyncSession],
) -> None:
    shared_time = datetime(2026, 8, 19, 9, tzinfo=UTC)
    async with db.begin() as session:
        session.add_all(
            [
                CustomerNote(
                    row_id=f"nrow_page_{note_id}",
                    note_id=note_id,
                    scenario_epoch=epoch,
                    customer_id="cus_unique",
                    body=note_id,
                    association="account",
                    created_by="person-support-1",
                    created_at=created_at,
                    archived=False,
                    version=1,
                )
                for note_id, created_at, epoch in [
                    ("note_a", shared_time, "epoch_1"),
                    ("note_b", shared_time, "epoch_1"),
                    ("note_c", shared_time.replace(hour=10), "epoch_1"),
                    ("note_old_same_customer", shared_time, "epoch_old"),
                ]
            ]
        )

    first = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        params={"limit": 1},
        headers=support_headers,
    )
    second = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        params={"limit": 1, "after": first.json()["nextCursor"]},
        headers=support_headers,
    )
    third = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        params={"limit": 1, "after": second.json()["nextCursor"]},
        headers=support_headers,
    )

    assert [first.status_code, second.status_code, third.status_code] == [200, 200, 200]
    assert [
        first.json()["items"][0]["noteId"],
        second.json()["items"][0]["noteId"],
        third.json()["items"][0]["noteId"],
    ] == ["note_a", "note_b", "note_c"]
    assert first.json()["nextCursor"] is not None
    assert second.json()["nextCursor"] is not None
    assert third.json()["nextCursor"] is None


@pytest.mark.asyncio
async def test_note_cursor_rejects_tampering_noncanonical_data_and_cross_list_reuse(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    valid_payload: dict[str, object] = {
        "kind": "crm-note-list",
        "customerId": "cus_unique",
        "includeArchived": False,
        "scenarioEpoch": "epoch_1",
        "createdAt": "2026-08-19T09:00:00Z",
        "noteId": "note_a",
    }
    valid = signed_note_cursor(valid_payload)
    payload_part, signature_part = valid.split(".")
    changed_signature = signature_part[:-1] + ("A" if signature_part[-1] != "A" else "B")
    invalid = [
        f"{payload_part}*.{signature_part}",
        f"{valid}.extra",
        f"{payload_part}.{signature_part}=",
        f"{payload_part}.{changed_signature}",
        signed_note_cursor({"customerId": "cus_unique"}),
        signed_note_cursor(valid_payload | {"includeArchived": "false"}),
    ]
    for cursor in invalid:
        response = await crm_client.get(
            "/v1/customers/cus_unique/notes",
            params={"after": cursor},
            headers=support_headers,
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_request"

    other_customer = await crm_client.get(
        "/v1/customers/cus_ambiguous_a/notes",
        params={"after": valid},
        headers=support_headers,
    )
    changed_filter = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        params={"after": valid, "includeArchived": True},
        headers=support_headers,
    )
    assert other_customer.status_code == changed_filter.status_code == 422
    assert other_customer.json()["error"]["code"] == "invalid_request"
    assert changed_filter.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_note_cursor_cannot_cross_a_scenario_epoch(
    crm_harness: CrmHarness,
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add_all(
            [
                CustomerNote(
                    row_id=f"nrow_cursor_{note_id}",
                    note_id=note_id,
                    scenario_epoch="epoch_1",
                    customer_id="cus_unique",
                    body=note_id,
                    association="account",
                    created_by="person-support-1",
                    created_at=created_at,
                    archived=False,
                    version=1,
                )
                for note_id, created_at in [
                    ("note_cursor_a", datetime(2026, 8, 19, 9, tzinfo=UTC)),
                    ("note_cursor_b", datetime(2026, 8, 19, 10, tzinfo=UTC)),
                ]
            ]
        )
    first = await crm_harness.client.get(
        "/v1/customers/cus_unique/notes",
        params={"limit": 1},
        headers=crm_harness.support_headers,
    )
    cursor = first.json()["nextCursor"]
    assert cursor is not None

    async with db.begin() as session:
        state = await session.get(ScenarioState, 1, with_for_update=True)
        assert state is not None
        state.active_epoch = "epoch_2"
        session.add(
            Customer(
                row_id="crow_epoch_2",
                scenario_epoch="epoch_2",
                customer_id="cus_unique",
                display_name="Epoch two customer",
                primary_email="epoch2@example.test",
                external_reference="ext-epoch-2",
                account_status="active",
                contact_methods=[],
                external_identifiers={},
                version=1,
            )
        )
        session.add(
            CustomerNote(
                row_id="nrow_epoch_2",
                note_id="note_epoch_2",
                scenario_epoch="epoch_2",
                customer_id="cus_unique",
                body="new epoch",
                association="account",
                created_by="person-support-1",
                created_at=datetime(2026, 8, 19, 11, tzinfo=UTC),
                archived=False,
                version=1,
            )
        )

    crm_harness.control.epoch = "epoch_2"
    reused = await crm_harness.client.get(
        "/v1/customers/cus_unique/notes",
        params={"after": cursor},
        headers=crm_harness.future_epoch_headers,
    )

    assert reused.status_code == 422
    assert reused.json()["error"]["code"] == "invalid_request"


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
