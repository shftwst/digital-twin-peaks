import asyncio
import hashlib
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ClockValue,
    FaultDecision,
    FaultEffect,
    ParticipantLoadRequest,
)
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import IdempotencyRecord, ScenarioState
from enterprise_twins.common.events.contracts import EventEnvelope, WebhookSubscriptionCreate
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.relay.delivery import WebhookWorker
from enterprise_twins.services.relay.models import (
    Delivery,
    DeliveryAttempt,
    SourceEvent,
    Subscription,
)
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.scenario import RelayScenarioLoader

NOW = datetime(2026, 8, 19, 10, tzinfo=UTC)


class Clock:
    def __init__(
        self,
        *,
        now: datetime = NOW,
        epoch: str = "epoch_1",
        decision: FaultDecision | None = None,
        evaluation_entered: asyncio.Event | None = None,
        release_evaluation: asyncio.Event | None = None,
    ) -> None:
        self.value = now
        self.epoch = epoch
        self.decision = decision or FaultDecision()
        self.evaluation_entered = evaluation_entered
        self.release_evaluation = release_evaluation

    async def now(self) -> datetime:
        return self.value

    async def snapshot(self) -> ClockValue:
        return ClockValue(now=self.value, scenarioEpoch=self.epoch)

    async def current_epoch(self) -> str:
        return self.epoch

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        if self.evaluation_entered is not None:
            self.evaluation_entered.set()
        if self.release_evaluation is not None:
            await self.release_evaluation.wait()
        return self.decision


async def initialise_relay(
    db: async_sessionmaker[AsyncSession], *, epoch: str = "epoch_1"
) -> RelayRepository:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch=epoch))
    return RelayRepository(db, allowed_targets={"webhook-receiver"})


def subscription_request(*, event_type: str = "crm.note.created") -> WebhookSubscriptionCreate:
    return WebhookSubscriptionCreate(
        eventTypes=[event_type],
        targetUrl="http://webhook-receiver:8080/events",
    )


def source_event(**overrides: object) -> EventEnvelope:
    values: dict[str, object] = {
        "eventId": "evt_1",
        "eventType": "crm.note.created",
        "source": "crm",
        "subject": "note/note_1",
        "resourceVersion": 1,
        "correlationId": "case-1",
        "causationId": "req-1",
        "occurredAt": "2026-08-19T10:00:00Z",
        "recordedAt": "2026-08-19T10:00:00Z",
        "data": {"noteId": "note_1"},
    }
    values.update(overrides)
    return EventEnvelope.model_validate(values)


async def create_subscription_and_event(
    repository: RelayRepository,
    *,
    event: EventEnvelope | None = None,
) -> tuple[str, EventEnvelope]:
    created = await repository.create_subscription(
        "crm",
        "person-support-1",
        "subscription-idem-1",
        subscription_request(),
        NOW,
    )
    item = event or source_event()
    assert await repository.ingest(item) is True
    return created.secret, item


async def finish_empty_reset(participant: ResetParticipant, epoch: str) -> None:
    payload = {
        "schemaVersion": "1",
        "subscriptions": [],
        "expectedCounts": {"subscriptions": 0, "events": 0, "deliveries": 0, "attempts": 0},
    }
    await participant.load(
        ParticipantLoadRequest(
            scenarioEpoch=epoch,
            scenarioId="platform-contracts",
            scenarioVersion=1,
            randomSeed=7,
            payload=payload,
            checksum=sha256_hex(payload),
            manifestChecksum="b" * 64,
        )
    )
    await participant.commit(epoch)
    await participant.finalize(epoch)


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
async def test_event_is_stored_once_and_delivered_with_valid_signature(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    created = await repository.create_subscription(
        "crm",
        "person-support-1",
        "subscription-idem-1",
        subscription_request(),
        NOW,
    )
    event = source_event()
    assert await repository.ingest(event) is True
    assert await repository.ingest(event) is False

    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(repository, Clock(), client)
        assert await worker.run_once() == 1

    assert len(requests) == 1
    timestamp = requests[0].headers["X-Twin-Timestamp"]
    expected = hmac.new(
        created.secret.encode(), timestamp.encode() + b"." + requests[0].content, hashlib.sha256
    ).hexdigest()
    assert requests[0].headers["X-Twin-Signature"] == f"v1={expected}"
    assert requests[0].headers["X-Twin-Event-Id"] == "evt_1"
    listed = await repository.list_subscriptions("crm")
    assert listed[0].model_dump().get("secret") is None
    async with db() as session:
        attempts = list(await session.scalars(select(DeliveryAttempt)))
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].outcome == "acknowledged"


@pytest.mark.asyncio
async def test_delivery_uses_one_atomic_control_snapshot(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    requests: list[httpx.Request] = []

    class SplitClock(Clock):
        async def snapshot(self) -> ClockValue:
            return ClockValue(now=NOW, scenarioEpoch="epoch_1")

        async def now(self) -> datetime:
            return NOW + timedelta(hours=1)

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await WebhookWorker(repository, SplitClock(), client).run_once() == 1

    assert requests[0].headers["X-Twin-Timestamp"] == "2026-08-19T10:00:00Z"


@pytest.mark.asyncio
async def test_target_outside_allowlist_is_rejected(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    with pytest.raises(ValueError, match="target host is not allowed"):
        await repository.create_subscription(
            "crm",
            "person-support-1",
            "subscription-idem-2",
            WebhookSubscriptionCreate(
                eventTypes=["crm.note.created"], targetUrl="http://127.0.0.1/x"
            ),
            NOW,
        )


@pytest.mark.asyncio
async def test_concurrent_identical_ingest_is_atomic_and_changed_body_conflicts(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await repository.create_subscription(
        "crm", "person-support-1", "subscription-idem-1", subscription_request(), NOW
    )
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_relay_source_event_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7101); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_relay_source_event_insert "
                "BEFORE INSERT ON relay_source_events FOR EACH ROW "
                "EXECUTE FUNCTION gate_relay_source_event_insert()"
            )
        )

    async with advisory_gate(db, 7101):
        first = asyncio.create_task(repository.ingest(source_event()))
        second = asyncio.create_task(repository.ingest(source_event()))
        await wait_for_lock_waiters(db, 2, [first, second])
    results = await asyncio.gather(first, second)

    assert sorted(results) == [False, True]
    async with db() as session:
        source_events = await session.scalar(select(func.count()).select_from(SourceEvent))
        deliveries = await session.scalar(select(func.count()).select_from(Delivery))
    assert source_events == 1
    assert deliveries == 1
    with pytest.raises(ApiError) as conflict:
        await repository.ingest(source_event(data={"noteId": "changed"}))
    assert conflict.value.code == ErrorCode.CONFLICT
    assert conflict.value.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_subscription_replays_one_original_secret_and_changed_data_conflicts(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_relay_idempotency_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7102); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_relay_idempotency_insert "
                "BEFORE INSERT ON idempotency_records FOR EACH ROW "
                "EXECUTE FUNCTION gate_relay_idempotency_insert()"
            )
        )

    async with advisory_gate(db, 7102):
        first = asyncio.create_task(
            repository.create_subscription(
                "crm", "caller-1", "same-key", subscription_request(), NOW
            )
        )
        second = asyncio.create_task(
            repository.create_subscription(
                "crm", "caller-1", "same-key", subscription_request(), NOW
            )
        )
        await wait_for_lock_waiters(db, 2, [first, second])
    original, replay = await asyncio.gather(first, second)

    assert replay.subscription_id == original.subscription_id
    assert replay.secret == original.secret
    async with db() as session:
        subscriptions = await session.scalar(select(func.count()).select_from(Subscription))
    assert subscriptions == 1
    with pytest.raises(ApiError) as conflict:
        await repository.create_subscription(
            "crm",
            "caller-1",
            "same-key",
            subscription_request(event_type="crm.person.updated"),
            NOW,
        )
    assert conflict.value.status_code == 409


@pytest.mark.asyncio
async def test_reset_prepare_cannot_overtake_subscription_create_or_leave_replay_ghost(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_reset_create_subscription() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7201); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_reset_create_subscription "
                "BEFORE INSERT ON idempotency_records FOR EACH ROW "
                "EXECUTE FUNCTION gate_reset_create_subscription()"
            )
        )
    participant = ResetParticipant(db, RelayScenarioLoader(), "relay")

    async with advisory_gate(db, 7201):
        creating = asyncio.create_task(
            repository.create_subscription(
                "crm", "caller-1", "reset-race-key", subscription_request(), NOW
            )
        )
        await wait_for_lock_waiters(db, 1, [creating])
        preparing = asyncio.create_task(participant.prepare("epoch_new"))
        await wait_for_lock_waiters(db, 2, [creating, preparing])
        assert preparing.done() is False
    old_result = await creating
    await preparing
    await finish_empty_reset(participant, "epoch_new")

    new_result = await repository.create_subscription(
        "crm", "caller-1", "reset-race-key", subscription_request(), NOW
    )

    assert new_result.subscription_id != old_result.subscription_id
    async with db() as session:
        epochs = list(await session.scalars(select(Subscription.scenario_epoch)))
        idempotency_epochs = list(await session.scalars(select(IdempotencyRecord.scenario_epoch)))
    assert epochs == ["epoch_new"]
    assert idempotency_epochs == ["epoch_new"]


@pytest.mark.asyncio
async def test_reset_prepare_cannot_overtake_event_ingest_or_leave_old_delivery_rows(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    await repository.create_subscription(
        "crm", "caller-1", "setup-key", subscription_request(), NOW
    )
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_reset_ingest() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7202); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_reset_ingest BEFORE INSERT ON relay_source_events "
                "FOR EACH ROW EXECUTE FUNCTION gate_reset_ingest()"
            )
        )
    participant = ResetParticipant(db, RelayScenarioLoader(), "relay")

    async with advisory_gate(db, 7202):
        ingesting = asyncio.create_task(repository.ingest(source_event()))
        await wait_for_lock_waiters(db, 1, [ingesting])
        preparing = asyncio.create_task(participant.prepare("epoch_new"))
        await wait_for_lock_waiters(db, 2, [ingesting, preparing])
        assert preparing.done() is False
    assert await ingesting is True
    await preparing
    await finish_empty_reset(participant, "epoch_new")

    async with db() as session:
        event_count = await session.scalar(select(func.count()).select_from(SourceEvent))
        delivery_count = await session.scalar(select(func.count()).select_from(Delivery))
        idempotency_count = await session.scalar(
            select(func.count()).select_from(IdempotencyRecord)
        )
    assert event_count == 0
    assert delivery_count == 0
    assert idempotency_count == 0


@pytest.mark.asyncio
async def test_reset_prepare_cannot_overtake_subscription_delete_or_replay_old_success(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    created = await repository.create_subscription(
        "crm", "caller-1", "setup-key", subscription_request(), NOW
    )
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION gate_reset_delete_subscription() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(7203); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER gate_reset_delete_subscription "
                "BEFORE INSERT ON idempotency_records FOR EACH ROW "
                "EXECUTE FUNCTION gate_reset_delete_subscription()"
            )
        )
    participant = ResetParticipant(db, RelayScenarioLoader(), "relay")

    async with advisory_gate(db, 7203):
        deleting = asyncio.create_task(
            repository.delete_subscription(
                "crm", "caller-1", "delete-race-key", created.subscription_id, 1
            )
        )
        await wait_for_lock_waiters(db, 1, [deleting])
        preparing = asyncio.create_task(participant.prepare("epoch_new"))
        await wait_for_lock_waiters(db, 2, [deleting, preparing])
        assert preparing.done() is False
    await deleting
    await preparing
    await finish_empty_reset(participant, "epoch_new")

    with pytest.raises(ApiError) as replay:
        await repository.delete_subscription(
            "crm", "caller-1", "delete-race-key", created.subscription_id, 1
        )

    assert replay.value.code == ErrorCode.NOT_FOUND
    async with db() as session:
        subscription_count = await session.scalar(select(func.count()).select_from(Subscription))
        idempotency_count = await session.scalar(
            select(func.count()).select_from(IdempotencyRecord)
        )
    assert subscription_count == 0
    assert idempotency_count == 0


@pytest.mark.asyncio
async def test_expired_lease_records_uncertainty_and_stale_completion_cannot_overwrite_new_lease(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)

    first = await repository.next_delivery(NOW)
    assert first is not None
    first_delivery, _, _ = first
    first_token = first_delivery.lease_token
    assert first_token is not None
    reclaim_time = NOW + timedelta(seconds=31)
    second = await repository.next_delivery(reclaim_time)
    assert second is not None
    second_delivery, _, _ = second
    second_token = second_delivery.lease_token
    assert second_token is not None
    assert second_token != first_token

    stale = await repository.finish_attempt(
        first_delivery.delivery_id,
        first_token,
        reclaim_time,
        204,
        None,
    )
    assert stale is False
    completed = await repository.finish_attempt(
        second_delivery.delivery_id,
        second_token,
        reclaim_time,
        500,
        None,
    )
    assert completed is True

    async with db() as session:
        delivery = await session.get(Delivery, first_delivery.delivery_id)
        attempts = list(
            await session.scalars(select(DeliveryAttempt).order_by(DeliveryAttempt.attempt_number))
        )
    assert delivery is not None
    assert delivery.state == "pending"
    assert delivery.next_attempt_at == reclaim_time + timedelta(seconds=4)
    assert [(item.attempt_number, item.outcome) for item in attempts] == [
        (1, "uncertain"),
        (2, "http_failure"),
    ]
    assert attempts[0].resulting_next_attempt_at == reclaim_time
    assert attempts[1].resulting_next_attempt_at == reclaim_time + timedelta(seconds=4)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("effect", "delay_ms", "expected_state", "expected_delay", "expected_outcome"),
    [
        (FaultEffect.DELAY, 250, "pending", timedelta(milliseconds=250), "injected_delay"),
        (FaultEffect.RETRY, None, "pending", timedelta(seconds=1), "injected_retry"),
        (FaultEffect.REORDER, None, "pending", timedelta(seconds=2), "injected_reorder"),
        (FaultEffect.SUPPRESS, None, "suppressed", None, "injected_suppress"),
    ],
)
async def test_delivery_fault_effects_update_the_persisted_attempt(
    db: async_sessionmaker[AsyncSession],
    effect: FaultEffect,
    delay_ms: int | None,
    expected_state: str,
    expected_delay: timedelta | None,
    expected_outcome: str,
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    candidate = await repository.next_delivery(NOW)
    assert candidate is not None
    delivery, _, _ = candidate

    handled = await repository.apply_delivery_fault(
        delivery,
        FaultDecision(effect=effect, delayMs=delay_ms),
        NOW,
    )

    assert handled is True
    async with db() as session:
        stored_delivery = await session.get(Delivery, delivery.delivery_id)
        attempts = list(await session.scalars(select(DeliveryAttempt)))
    assert stored_delivery is not None
    assert stored_delivery.state == expected_state
    assert len(attempts) == 1
    assert attempts[0].outcome == expected_outcome
    expected_next = NOW + expected_delay if expected_delay is not None else None
    assert attempts[0].resulting_next_attempt_at == expected_next
    if expected_next is not None:
        assert stored_delivery.next_attempt_at == expected_next


@pytest.mark.asyncio
async def test_duplicate_fault_emits_two_copies_but_keeps_one_lease_attempt(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(
            repository,
            Clock(decision=FaultDecision(effect=FaultEffect.DUPLICATE)),
            client,
        )
        assert await worker.run_once() == 1

    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    async with db() as session:
        attempts = list(await session.scalars(select(DeliveryAttempt)))
    assert len(attempts) == 1
    assert attempts[0].outcome == "acknowledged"


@pytest.mark.asyncio
async def test_delivery_worker_rejects_an_unsupported_effect_without_sending_or_delivering(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(
            repository,
            Clock(decision=FaultDecision(effect=FaultEffect.FAILED_REFUND)),
            client,
        )
        with pytest.raises(RuntimeError, match="unsupported webhook delivery fault"):
            await worker.run_once()

    assert requests == []
    async with db() as session:
        delivery = await session.scalar(select(Delivery))
    assert delivery is not None
    assert delivery.state != "delivered"


@pytest.mark.asyncio
async def test_duplicate_fault_records_acknowledgement_when_either_copy_succeeds(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    statuses = iter((500, 204))

    async def receive(request: httpx.Request) -> httpx.Response:
        return httpx.Response(next(statuses))

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(
            repository,
            Clock(decision=FaultDecision(effect=FaultEffect.DUPLICATE)),
            client,
        )
        assert await worker.run_once() == 1

    async with db() as session:
        delivery = await session.scalar(select(Delivery))
        attempt = await session.scalar(select(DeliveryAttempt))
    assert delivery is not None
    assert attempt is not None
    assert delivery.state == "delivered"
    assert attempt.outcome == "acknowledged"
    assert attempt.response_status == 204


class EndlessBody(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.iterated = False
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.iterated = True
        while True:
            await asyncio.sleep(0.01)
            yield b"endless"

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_delivery_acknowledges_headers_without_entering_an_endless_body_stream(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    body = EndlessBody()

    async def receive(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, stream=body)

    completed = False
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        try:
            async with asyncio.timeout(0.4):
                assert await WebhookWorker(repository, Clock(), client).run_once() == 1
                completed = True
        except TimeoutError:
            pass

    assert completed is True
    assert body.iterated is False
    assert body.closed is True
    async with db() as session:
        attempt = await session.scalar(select(DeliveryAttempt))
    assert attempt is not None
    assert attempt.outcome == "acknowledged"


@pytest.mark.asyncio
async def test_duplicate_loop_total_timeout_without_success_is_failed_and_releases_reset_fence(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    await create_subscription_and_event(repository)
    second_entered = asyncio.Event()
    release_second = asyncio.Event()
    calls = 0

    async def receive(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(500)
        second_entered.set()
        await release_second.wait()
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(
            repository,
            Clock(epoch="epoch_old", decision=FaultDecision(effect=FaultEffect.DUPLICATE)),
            client,
        )
        delivery_task = asyncio.create_task(worker.run_once())
        await second_entered.wait()
        prepare_task = asyncio.create_task(
            ResetParticipant(db, RelayScenarioLoader(), "relay").prepare("epoch_new")
        )
        completed_without_receiver_release = True
        try:
            async with asyncio.timeout(2.5):
                await asyncio.shield(asyncio.gather(delivery_task, prepare_task))
        except TimeoutError:
            completed_without_receiver_release = False
            release_second.set()
            await asyncio.gather(delivery_task, prepare_task)

    assert completed_without_receiver_release is True
    async with db() as session:
        delivery = await session.scalar(select(Delivery))
        attempt = await session.scalar(select(DeliveryAttempt))
        state = await session.get(ScenarioState, 1)
    assert delivery is not None
    assert attempt is not None
    assert state is not None
    assert delivery.state == "pending"
    assert attempt.response_status is None
    assert attempt.outcome == "total_timeout"
    assert state.mode == "preparing"


@pytest.mark.asyncio
async def test_duplicate_loop_keeps_first_acknowledgement_when_a_later_copy_times_out(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    second_entered = asyncio.Event()
    release_second = asyncio.Event()
    calls = 0

    async def receive(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(204)
        second_entered.set()
        await release_second.wait()
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        delivery_task = asyncio.create_task(
            WebhookWorker(
                repository,
                Clock(decision=FaultDecision(effect=FaultEffect.DUPLICATE)),
                client,
            ).run_once()
        )
        await second_entered.wait()
        completed_without_receiver_release = True
        try:
            async with asyncio.timeout(2.5):
                await asyncio.shield(delivery_task)
        except TimeoutError:
            completed_without_receiver_release = False
            release_second.set()
            await delivery_task

    assert completed_without_receiver_release is True
    async with db() as session:
        delivery = await session.scalar(select(Delivery))
        attempt = await session.scalar(select(DeliveryAttempt))
    assert delivery is not None
    assert attempt is not None
    assert delivery.state == "delivered"
    assert attempt.response_status == 204
    assert attempt.outcome == "acknowledged"


@pytest.mark.asyncio
async def test_external_worker_cancellation_is_not_recorded_as_a_delivery_timeout(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    await create_subscription_and_event(repository)
    request_entered = asyncio.Event()

    async def receive(_request: httpx.Request) -> httpx.Response:
        request_entered.set()
        await asyncio.Event().wait()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        delivery_task = asyncio.create_task(WebhookWorker(repository, Clock(), client).run_once())
        await request_entered.wait()
        delivery_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await delivery_task

    async with db() as session:
        attempt = await session.scalar(select(DeliveryAttempt))
    assert attempt is not None
    assert attempt.outcome == "in_flight"


@pytest.mark.asyncio
async def test_reset_prepare_waits_for_an_outbound_request_holding_the_epoch_fence(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    await create_subscription_and_event(repository)
    request_entered = asyncio.Event()
    release_request = asyncio.Event()

    async def receive(request: httpx.Request) -> httpx.Response:
        request_entered.set()
        await release_request.wait()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        worker = WebhookWorker(repository, Clock(epoch="epoch_old"), client)
        delivery_task = asyncio.create_task(worker.run_once())
        await request_entered.wait()
        participant = ResetParticipant(db, RelayScenarioLoader(), "relay")
        prepare_task = asyncio.create_task(participant.prepare("epoch_new"))
        await wait_for_lock_waiters(db, 1, [prepare_task])
        assert prepare_task.done() is False
        release_request.set()
        assert await delivery_task == 1
        await prepare_task

    async with db() as session:
        state = await session.get(ScenarioState, 1)
    assert state is not None
    assert state.mode == "preparing"


@pytest.mark.asyncio
async def test_reset_prepare_before_send_prevents_the_old_epoch_webhook(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    await create_subscription_and_event(repository)
    evaluation_entered = asyncio.Event()
    release_evaluation = asyncio.Event()
    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    clock = Clock(
        epoch="epoch_old",
        evaluation_entered=evaluation_entered,
        release_evaluation=release_evaluation,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        delivery_task = asyncio.create_task(WebhookWorker(repository, clock, client).run_once())
        await evaluation_entered.wait()
        await ResetParticipant(db, RelayScenarioLoader(), "relay").prepare("epoch_new")
        release_evaluation.set()
        assert await delivery_task == 1

    assert requests == []


@pytest.mark.asyncio
async def test_control_epoch_mismatch_prevents_webhook_delivery(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db, epoch="epoch_old")
    await create_subscription_and_event(repository)
    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await WebhookWorker(repository, Clock(epoch="epoch_new"), client).run_once() == 1
    assert requests == []


@pytest.mark.asyncio
async def test_reset_loader_deletes_only_the_discarded_epoch(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        for epoch in ("epoch_old", "epoch_new"):
            suffix = "old" if epoch == "epoch_old" else "new"
            session.add(
                Subscription(
                    row_id=f"subrow_{suffix}",
                    subscription_id=f"sub_{suffix}",
                    scenario_epoch=epoch,
                    source="crm",
                    event_types=["crm.note.created"],
                    target_url="http://webhook-receiver/events",
                    signing_secret=f"secret-{suffix}",
                    version=1,
                    active=True,
                    created_at=NOW,
                )
            )
            session.add(
                SourceEvent(
                    event_id=f"evt_{suffix}",
                    scenario_epoch=epoch,
                    source="crm",
                    event_type="crm.note.created",
                    body_hash="a" * 64,
                    envelope=source_event(eventId=f"evt_{suffix}").model_dump(
                        mode="json", by_alias=True
                    ),
                )
            )
            session.add(
                Delivery(
                    delivery_id=f"dlv_{suffix}",
                    scenario_epoch=epoch,
                    event_id=f"evt_{suffix}",
                    subscription_id=f"sub_{suffix}",
                    state="delivered",
                    attempt_count=1,
                    next_attempt_at=NOW,
                )
            )
            session.add(
                DeliveryAttempt(
                    attempt_id=f"att_{suffix}",
                    scenario_epoch=epoch,
                    delivery_id=f"dlv_{suffix}",
                    attempt_number=1,
                    lease_token=f"lease-{suffix}",
                    attempted_at=NOW,
                    response_status=204,
                    outcome="acknowledged",
                )
            )

    async with db.begin() as session:
        await RelayScenarioLoader().discard(session, "epoch_old")

    async with db() as session:
        for model in (Subscription, SourceEvent, Delivery, DeliveryAttempt):
            epochs = list(await session.scalars(select(model.scenario_epoch)))
            assert epochs == ["epoch_new"]


@pytest.mark.asyncio
async def test_relay_loader_rejects_an_unsupported_schema_before_staging(
    db: async_sessionmaker[AsyncSession],
) -> None:
    payload = {
        "schemaVersion": "2",
        "subscriptions": [],
    }

    async with db() as session:
        with pytest.raises(ValueError, match="schemaVersion"):
            await RelayScenarioLoader().load(session, "epoch_new", payload)
        assert not session.new
