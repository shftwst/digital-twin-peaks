from datetime import UTC, datetime

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import ClockValue, FaultDecision
from enterprise_twins.common.db.records import IdempotencyRecord, OutboxRecord, ScenarioState
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionView,
)
from enterprise_twins.common.events.publisher import OutboxDispatcher
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.relay.api import relay_router
from enterprise_twins.services.relay.app import RelayStatus
from enterprise_twins.services.relay.models import SourceEvent, Subscription
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings

NOW = datetime(2026, 8, 19, 10, tzinfo=UTC)


class Clock:
    async def snapshot(self) -> ClockValue:
        return ClockValue(now=NOW, scenarioEpoch="epoch_1")

    async def now(self) -> datetime:
        return NOW

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def ready_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()


class SnapshotClock(Clock):
    def __init__(self, epoch: str = "epoch_1", *, unavailable: bool = False) -> None:
        self.epoch = epoch
        self.unavailable = unavailable
        self.snapshot_calls = 0

    async def snapshot(self) -> ClockValue:
        self.snapshot_calls += 1
        if self.unavailable:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "Control is temporarily unavailable",
                status_code=503,
                retryable=True,
            )
        return ClockValue(now=NOW, scenarioEpoch=self.epoch)

    async def now(self) -> datetime:
        raise AssertionError("Relay business routes must use one Control snapshot")


def event_values(**overrides: object) -> dict[str, object]:
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
    return values


def source_event(**overrides: object) -> EventEnvelope:
    return EventEnvelope.model_validate(event_values(**overrides))


async def initialise_relay(
    db: async_sessionmaker[AsyncSession],
) -> RelayRepository:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
    return RelayRepository(db, allowed_targets={"webhook-receiver"})


def settings() -> RelaySettings:
    return RelaySettings(
        database_url="postgresql+asyncpg://unused",
        source_tokens={"crm": "crm-source-secret", "erp": "erp-source-secret"},
        allowed_targets={"webhook-receiver"},
    )


def relay_app(
    db: async_sessionmaker[AsyncSession],
    repository: RelayRepository,
    control: Clock | None = None,
) -> object:
    selected_control = control or Clock()
    return create_app(
        "Relay probe",
        (),
        RelayStatus(db, selected_control),  # type: ignore[arg-type]
        (relay_router(repository, selected_control, settings()),),  # type: ignore[arg-type]
    )


async def seed_subscription(db: async_sessionmaker[AsyncSession]) -> None:
    async with db.begin() as session:
        session.add(
            Subscription(
                row_id="subrow_existing",
                subscription_id="sub_existing",
                scenario_epoch="epoch_1",
                source="crm",
                event_types=["crm.note.created"],
                target_url="http://webhook-receiver:8080/events",
                signing_secret="existing-signing-secret",  # noqa: S106
                version=1,
                active=True,
                created_at=NOW,
            )
        )


async def invoke_business_route(client: httpx.AsyncClient, operation: str) -> httpx.Response:
    if operation == "create":
        return await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers={
                "Authorization": "Bearer crm-source-secret",
                "Idempotency-Key": "epoch-fence-create",
                "X-Caller-Id": "person-support-1",
            },
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
    if operation == "list":
        return await client.get(
            "/internal/v1/sources/crm/subscriptions",
            headers={"Authorization": "Bearer crm-source-secret"},
        )
    if operation == "delete":
        return await client.delete(
            "/internal/v1/sources/crm/subscriptions/sub_existing",
            headers={
                "Authorization": "Bearer crm-source-secret",
                "Idempotency-Key": "epoch-fence-delete",
                "X-Caller-Id": "person-support-1",
                "If-Match": '"1"',
            },
        )
    return await client.post(
        "/internal/v1/events",
        headers={"Authorization": "Bearer crm-source-secret"},
        json=event_values(),
    )


@pytest.mark.asyncio
async def test_source_routes_enforce_roles_redact_tokens_and_replay_original_secret(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository)),
        base_url="http://relay",
    ) as client:
        denied = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers={
                "Authorization": "Bearer wrong-source-secret",
                "Idempotency-Key": "idem-1",
                "X-Caller-Id": "person-support-1",
            },
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
        assert denied.status_code == 401
        assert denied.json()["error"]["code"] == "unauthenticated"
        assert "wrong-source-secret" not in denied.text

        headers = {
            "Authorization": "Bearer crm-source-secret",
            "Idempotency-Key": "idem-1",
            "X-Caller-Id": "person-support-1",
        }
        created = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers=headers,
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
        replay = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers=headers,
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
        assert created.status_code == 201
        assert replay.status_code == 201
        assert replay.json()["subscriptionId"] == created.json()["subscriptionId"]
        assert replay.json()["secret"] == created.json()["secret"]

        listed = await client.get(
            "/internal/v1/sources/crm/subscriptions",
            headers={"Authorization": "Bearer crm-source-secret"},
        )
        assert listed.status_code == 200
        assert "secret" not in listed.json()[0]

        changed = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers=headers,
            json={
                "eventTypes": ["crm.person.updated"],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
        assert changed.status_code == 409
        assert changed.json()["error"]["code"] == "conflict"

        cross_source = await client.get(
            "/internal/v1/sources/erp/subscriptions",
            headers={"Authorization": "Bearer crm-source-secret"},
        )
        assert cross_source.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization",
    [
        "crm-source-secret",
        "bearer crm-source-secret",
        "Bearer\tcrm-source-secret",
        "Bearer  crm-source-secret",
        " Bearer crm-source-secret",
        "Bearer crm-source-secret ",
        "Bearer crm source-secret",
    ],
)
async def test_source_routes_reject_noncanonical_bearer_headers_before_repository_work(
    db: async_sessionmaker[AsyncSession],
    authorization: str,
) -> None:
    repository = await initialise_relay(db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository)),
        base_url="http://relay",
    ) as client:
        response = await client.get(
            "/internal/v1/sources/crm/subscriptions",
            headers={"Authorization": authorization},
        )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthenticated"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "list", "delete", "ingest"])
@pytest.mark.parametrize(
    "failure",
    ["control_unavailable", "epoch_mismatch", "local_inactive"],
)
async def test_every_business_route_requires_one_matching_control_snapshot_before_repository_work(
    db: async_sessionmaker[AsyncSession],
    operation: str,
    failure: str,
) -> None:
    repository = await initialise_relay(db)
    await seed_subscription(db)
    if failure == "local_inactive":
        async with db.begin() as session:
            state = await session.get(ScenarioState, 1)
            assert state is not None
            state.mode = "preparing"
    control = SnapshotClock(
        epoch="epoch_new" if failure == "epoch_mismatch" else "epoch_1",
        unavailable=failure == "control_unavailable",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository, control)),
        base_url="http://relay",
    ) as client:
        response = await invoke_business_route(client, operation)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.json()["error"]["retryable"] is True
    assert response.headers["X-Scenario-Epoch"] == "epoch_1"
    assert control.snapshot_calls == 1
    async with db() as session:
        subscription_count = await session.scalar(select(func.count()).select_from(Subscription))
        source_event_count = await session.scalar(select(func.count()).select_from(SourceEvent))
        idempotency_count = await session.scalar(
            select(func.count()).select_from(IdempotencyRecord)
        )
        subscription = await session.get(Subscription, "subrow_existing")
    assert subscription_count == 1
    assert source_event_count == 0
    assert idempotency_count == 0
    assert subscription is not None
    assert subscription.active is True
    assert subscription.version == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "list", "delete", "ingest"])
async def test_every_business_route_uses_exactly_one_control_snapshot_on_success(
    db: async_sessionmaker[AsyncSession],
    operation: str,
) -> None:
    repository = await initialise_relay(db)
    await seed_subscription(db)
    control = SnapshotClock()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository, control)),
        base_url="http://relay",
    ) as client:
        response = await invoke_business_route(client, operation)

    assert (
        response.status_code
        == {
            "create": 201,
            "list": 200,
            "delete": 204,
            "ingest": 202,
        }[operation]
    )
    assert control.snapshot_calls == 1
    if operation == "create":
        async with db() as session:
            created = await session.scalar(
                select(Subscription).where(Subscription.row_id != "subrow_existing")
            )
        assert created is not None
        assert created.created_at == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    [
        "unauthorised_create",
        "unauthorised_list",
        "unauthorised_delete",
        "unauthorised_ingest",
        "invalid_create",
        "invalid_delete",
        "invalid_ingest",
    ],
)
async def test_invalid_auth_and_inputs_stop_before_control_or_repository_work(
    db: async_sessionmaker[AsyncSession],
    boundary: str,
) -> None:
    repository = await initialise_relay(db)
    await seed_subscription(db)
    control = SnapshotClock(unavailable=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository, control)),
        base_url="http://relay",
    ) as client:
        if boundary == "unauthorised_create":
            response = await client.post(
                "/internal/v1/sources/crm/subscriptions",
                headers={
                    "Authorization": "Bearer wrong",
                    "Idempotency-Key": "invalid-auth-create",
                    "X-Caller-Id": "person-support-1",
                },
                json={
                    "eventTypes": ["crm.note.created"],
                    "targetUrl": "http://webhook-receiver:8080/events",
                },
            )
        elif boundary == "unauthorised_list":
            response = await client.get(
                "/internal/v1/sources/crm/subscriptions",
                headers={"Authorization": "Bearer wrong"},
            )
        elif boundary == "unauthorised_delete":
            response = await client.delete(
                "/internal/v1/sources/crm/subscriptions/sub_existing",
                headers={
                    "Authorization": "Bearer wrong",
                    "Idempotency-Key": "invalid-auth-delete",
                    "X-Caller-Id": "person-support-1",
                    "If-Match": '"1"',
                },
            )
        elif boundary == "unauthorised_ingest":
            response = await client.post(
                "/internal/v1/events",
                headers={"Authorization": "Bearer wrong"},
                json=event_values(),
            )
        elif boundary == "invalid_create":
            response = await client.post(
                "/internal/v1/sources/crm/subscriptions",
                headers={
                    "Authorization": "Bearer crm-source-secret",
                    "Idempotency-Key": "invalid-input-create",
                    "X-Caller-Id": "person-support-1",
                },
                json={
                    "eventTypes": [],
                    "targetUrl": "http://webhook-receiver:8080/events",
                },
            )
        elif boundary == "invalid_delete":
            response = await client.delete(
                "/internal/v1/sources/crm/subscriptions/sub_existing",
                headers={
                    "Authorization": "Bearer crm-source-secret",
                    "Idempotency-Key": "invalid-input-delete",
                    "X-Caller-Id": "person-support-1",
                    "If-Match": "1",
                },
            )
        else:
            response = await client.post(
                "/internal/v1/events",
                headers={"Authorization": "Bearer crm-source-secret"},
                json=event_values(eventId="e" * 65),
            )

    assert response.status_code == (401 if boundary.startswith("unauthorised") else 422)
    assert control.snapshot_calls == 0
    async with db() as session:
        subscription_count = await session.scalar(select(func.count()).select_from(Subscription))
        source_event_count = await session.scalar(select(func.count()).select_from(SourceEvent))
        idempotency_count = await session.scalar(
            select(func.count()).select_from(IdempotencyRecord)
        )
        subscription = await session.get(Subscription, "subrow_existing")
    assert subscription_count == 1
    assert source_event_count == 0
    assert idempotency_count == 0
    assert subscription is not None
    assert subscription.active is True


@pytest.mark.asyncio
async def test_relay_delete_rejects_noncanonical_if_match_before_repository_work(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository)),
        base_url="http://relay",
    ) as client:
        for index, value in enumerate(["1", '""1""', "+1", "-1", ' "1" ', '"01"']):
            response = await client.delete(
                "/internal/v1/sources/crm/subscriptions/sub_absent",
                headers={
                    "Authorization": "Bearer crm-source-secret",
                    "Idempotency-Key": f"relay-etag-{index}",
                    "X-Caller-Id": "person-support-1",
                    "If-Match": value,
                },
            )
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_changed_event_body_returns_common_conflict_envelope(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository)),
        base_url="http://relay",
    ) as client:
        headers = {"Authorization": "Bearer crm-source-secret"}
        accepted = await client.post("/internal/v1/events", headers=headers, json=event_values())
        conflict = await client.post(
            "/internal/v1/events",
            headers=headers,
            json=event_values(data={"noteId": "changed"}),
        )

    assert accepted.status_code == 202
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "conflict"


@pytest.mark.asyncio
async def test_relay_client_redirect_is_retryable_and_outbox_remains_unpublished(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_relay(db)
    event = source_event()
    async with db.begin() as session:
        session.add(
            OutboxRecord(
                event_id=event.event_id,
                scenario_epoch="epoch_1",
                event_type=event.event_type,
                envelope=event.model_dump(mode="json", by_alias=True),
                published=False,
                publish_attempts=0,
            )
        )

    async def redirect(request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, headers={"Location": "http://other-relay/internal/v1/events"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect)) as client:
        relay = RelayClient("http://relay", "crm", "source-secret", client)
        with pytest.raises(ApiError) as error:
            await relay.ingest(event)
        assert error.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
        assert error.value.retryable is True
        assert await OutboxDispatcher(db, relay).run_once() == 0

    async with db() as session:
        record = await session.get(OutboxRecord, event.event_id)
    assert record is not None
    assert record.published is False
    assert record.publish_attempts == 1


@pytest.mark.asyncio
async def test_outbox_is_published_only_after_exact_202_acceptance(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_relay(db)
    event = source_event()
    async with db.begin() as session:
        session.add(
            OutboxRecord(
                event_id=event.event_id,
                scenario_epoch="epoch_1",
                event_type=event.event_type,
                envelope=event.model_dump(mode="json", by_alias=True),
                published=False,
                publish_attempts=0,
            )
        )

    async def accepted(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"accepted": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        relay = RelayClient("http://relay", "crm", "source-secret", client)
        assert await OutboxDispatcher(db, relay).run_once() == 1

    async with db() as session:
        record = await session.get(OutboxRecord, event.event_id)
    assert record is not None
    assert record.published is True
    assert record.publish_attempts == 1


@pytest.mark.asyncio
async def test_relay_client_transport_failures_are_generic_and_retryable() -> None:
    async def unavailable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("transport leaked source-secret", request=request)

    request = WebhookSubscriptionCreate(
        eventTypes=["crm.note.created"],
        targetUrl="http://webhook-receiver/events",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as client:
        relay = RelayClient("http://relay", "crm", "source-secret", client)
        operations = (
            lambda: relay.ingest(source_event()),
            lambda: relay.create_subscription("person-1", "idem-1", request),
            relay.list_subscriptions,
            lambda: relay.delete_subscription("person-1", "idem-2", "sub_1", 1),
        )
        for operation in operations:
            with pytest.raises(ApiError) as raised:
                await operation()
            assert raised.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
            assert raised.value.status_code == 503
            assert raised.value.retryable is True
            assert raised.value.message == "event relay is temporarily unavailable"
            assert raised.value.details == {}
            assert "source-secret" not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "failure"),
    [
        ("create", "json"),
        ("create", "model"),
        ("list", "json"),
        ("list", "model"),
    ],
)
async def test_relay_client_malformed_successes_are_generic_and_retryable(
    operation: str,
    failure: str,
) -> None:
    async def malformed(_request: httpx.Request) -> httpx.Response:
        status = 201 if operation == "create" else 200
        if failure == "json":
            return httpx.Response(status, content=b'{"secret":"upstream-secret"')
        body: object
        if operation == "create":
            body = {"secret": "upstream-secret"}
        else:
            body = [{"source": "crm", "secret": "upstream-secret"}]
        return httpx.Response(status, json=body)

    request = WebhookSubscriptionCreate(
        eventTypes=["crm.note.created"],
        targetUrl="http://webhook-receiver/events",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(malformed)) as client:
        relay = RelayClient("http://relay", "crm", "source-secret", client)
        with pytest.raises(ApiError) as raised:
            if operation == "create":
                await relay.create_subscription("person-1", "idem-1", request)
            else:
                await relay.list_subscriptions()

    assert raised.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert raised.value.status_code == 503
    assert raised.value.retryable is True
    assert raised.value.message == "event relay is temporarily unavailable"
    assert raised.value.details == {}
    assert "upstream-secret" not in str(raised.value)


@pytest.mark.asyncio
async def test_relay_client_preserves_explicit_relay_api_errors() -> None:
    async def conflict(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "error": {
                    "code": "conflict",
                    "message": "subscription request conflicts",
                    "retryable": False,
                    "details": {"operation": "identity.subscription.create"},
                }
            },
        )

    request = WebhookSubscriptionCreate(
        eventTypes=["crm.note.created"],
        targetUrl="http://webhook-receiver/events",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(conflict)) as client:
        relay = RelayClient("http://relay", "crm", "source-secret", client)
        with pytest.raises(ApiError) as raised:
            await relay.create_subscription("person-1", "idem-1", request)

    assert raised.value.code == ErrorCode.CONFLICT
    assert raised.value.status_code == 409
    assert raised.value.message == "subscription request conflicts"
    assert raised.value.retryable is False
    assert raised.value.details == {"operation": "identity.subscription.create"}


@pytest.mark.parametrize(
    "overrides",
    [
        {"eventId": "e" * 65},
        {"eventType": "e" * 161},
        {"eventType": "   "},
        {"source": "s" * 81},
        {"source": "   "},
        {"resourceVersion": 0},
        {"correlationId": "c" * 129},
        {"causationId": "c" * 129},
        {"occurredAt": "2026-08-19T10:00:00", "recordedAt": "2026-08-19T10:00:00Z"},
        {"occurredAt": "2026-08-19T10:00:01Z", "recordedAt": "2026-08-19T10:00:00Z"},
    ],
)
def test_event_contract_rejects_values_outside_persistence_invariants(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(event_values(**overrides))


@pytest.mark.parametrize(
    "values",
    [
        {"eventTypes": [], "targetUrl": "http://webhook-receiver/events"},
        {"eventTypes": [""], "targetUrl": "http://webhook-receiver/events"},
        {"eventTypes": ["e" * 161], "targetUrl": "http://webhook-receiver/events"},
        {
            "eventTypes": ["crm.note.created"],
            "targetUrl": "http://webhook-receiver/" + "x" * 1000,
        },
    ],
)
def test_subscription_contract_rejects_values_outside_persistence_limits(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        WebhookSubscriptionCreate.model_validate(values)


def test_subscription_view_requires_positive_version() -> None:
    with pytest.raises(ValidationError):
        WebhookSubscriptionView(
            subscriptionId="sub_1",
            source="crm",
            eventTypes=["crm.note.created"],
            targetUrl="http://webhook-receiver/events",
            version=0,
        )


@pytest.mark.asyncio
async def test_api_validation_failures_use_the_common_error_envelope(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=relay_app(db, repository)),
        base_url="http://relay",
    ) as client:
        event_response = await client.post(
            "/internal/v1/events",
            headers={"Authorization": "Bearer crm-source-secret"},
            json=event_values(eventId="e" * 65),
        )
        header_response = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers={
                "Authorization": "Bearer crm-source-secret",
                "Idempotency-Key": "i" * 201,
                "X-Caller-Id": "person-support-1",
            },
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver/events",
            },
        )

    assert event_response.status_code == 422
    assert event_response.json()["error"]["code"] == "invalid_request"
    assert header_response.status_code == 422
    assert header_response.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_readiness_returns_503_when_the_database_query_fails(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_relay(db)
    async with db.begin() as session:
        await session.execute(text("DROP TABLE scenario_state"))
    app = create_app("Relay probe", (), RelayStatus(db, Clock()))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://relay"
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {"database": "not_ready", "scenario": "unavailable"},
    }
    assert response.headers["X-Scenario-Epoch"] == "none"


@pytest.mark.asyncio
async def test_relay_business_call_reports_control_outage_as_retryable_503(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = await initialise_relay(db)

    class UnavailableClock(Clock):
        async def snapshot(self) -> ClockValue:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "Control is temporarily unavailable",
                status_code=503,
                retryable=True,
            )

    control = UnavailableClock()
    app = create_app(
        "Relay probe",
        (),
        RelayStatus(db, control),
        (relay_router(repository, control, settings()),),  # type: ignore[arg-type]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://relay"
    ) as client:
        response = await client.post(
            "/internal/v1/sources/crm/subscriptions",
            headers={
                "Authorization": "Bearer crm-source-secret",
                "Idempotency-Key": "outage-key",
                "X-Caller-Id": "person-support-1",
            },
            json={
                "eventTypes": ["crm.note.created"],
                "targetUrl": "http://webhook-receiver/events",
            },
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.json()["error"]["retryable"] is True
