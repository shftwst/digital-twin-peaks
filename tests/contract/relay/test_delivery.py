import hashlib
import hmac
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.contracts import EventEnvelope, WebhookSubscriptionCreate
from enterprise_twins.services.relay.delivery import WebhookWorker
from enterprise_twins.services.relay.models import DeliveryAttempt
from enterprise_twins.services.relay.repository import RelayRepository


class Clock:
    async def now(self) -> datetime:
        return datetime(2026, 8, 19, 10, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()


@pytest.mark.asyncio
async def test_event_is_stored_once_and_delivered_with_valid_signature(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
    repository = RelayRepository(db, allowed_targets={"webhook-receiver"})
    created = await repository.create_subscription(
        "crm",
        "person-support-1",
        "subscription-idem-1",
        WebhookSubscriptionCreate(
            eventTypes=["crm.note.created"],
            targetUrl="http://webhook-receiver:8080/events",
        ),
        datetime(2026, 8, 19, 10, tzinfo=UTC),
    )
    event = EventEnvelope(
        eventId="evt_1",
        eventType="crm.note.created",
        source="crm",
        subject="note/note_1",
        resourceVersion=1,
        correlationId="case-1",
        causationId="req-1",
        occurredAt="2026-08-19T10:00:00Z",
        recordedAt="2026-08-19T10:00:00Z",
        data={"noteId": "note_1"},
    )
    assert await repository.ingest(event) is True
    assert await repository.ingest(event) is False

    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    worker = WebhookWorker(
        repository,
        Clock(),
        httpx.AsyncClient(transport=httpx.MockTransport(receive)),
    )
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
        attempts = await session.scalar(select(func.count()).select_from(DeliveryAttempt))
    assert attempts == 1


@pytest.mark.asyncio
async def test_target_outside_allowlist_is_rejected(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = RelayRepository(db, allowed_targets={"webhook-receiver"})
    with pytest.raises(ValueError, match="target host is not allowed"):
        await repository.create_subscription(
            "crm",
            "person-support-1",
            "subscription-idem-2",
            WebhookSubscriptionCreate(
                eventTypes=["crm.note.created"], targetUrl="http://127.0.0.1/x"
            ),
            datetime(2026, 8, 19, 10, tzinfo=UTC),
        )
