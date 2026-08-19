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

from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import ScenarioState
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
