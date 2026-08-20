import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from pydantic import AnyHttpUrl
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect
from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.relay.models import (
    Delivery,
    DeliveryAttempt,
    SourceEvent,
    Subscription,
)


class RelayRepository:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        allowed_targets: set[str],
    ) -> None:
        self.factory = factory
        self.allowed_targets = allowed_targets

    async def active_epoch(self, session: AsyncSession) -> str:
        state = await session.scalar(
            select(ScenarioState).where(ScenarioState.singleton_id == 1).with_for_update(read=True)
        )
        if state is None or state.mode != "active":
            raise RuntimeError("relay scenario is not active")
        return state.active_epoch

    async def create_subscription(
        self,
        source: str,
        caller_id: str,
        idempotency_key: str,
        request: WebhookSubscriptionCreate,
        now: datetime,
    ) -> WebhookSubscriptionCreated:
        host = request.target_url.host
        if host not in self.allowed_targets:
            raise ValueError("target host is not allowed")
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            namespace = IdempotencyNamespace(
                "tenant_synthetic",
                caller_id,
                f"{source}.subscription.create",
                idempotency_key,
            )

            async def work() -> StoredResponse:
                item = Subscription(
                    row_id=new_id("subrow"),
                    subscription_id=new_id("sub"),
                    scenario_epoch=epoch,
                    source=source,
                    event_types=sorted(set(request.event_types)),
                    target_url=str(request.target_url),
                    signing_secret=secrets.token_urlsafe(32),
                    version=1,
                    active=True,
                    created_at=now,
                )
                session.add(item)
                return StoredResponse(
                    201,
                    WebhookSubscriptionCreated(
                        subscriptionId=item.subscription_id,
                        source=item.source,
                        eventTypes=item.event_types,
                        targetUrl=request.target_url,
                        version=item.version,
                        secret=item.signing_secret,
                    ).model_dump(mode="json", by_alias=True),
                    {},
                )

            result, _replayed = await run_idempotent(
                session,
                epoch,
                namespace,
                request.model_dump(mode="json", by_alias=True),
                work,
            )
            return WebhookSubscriptionCreated.model_validate(result.body)

    async def ingest(self, event: EventEnvelope) -> bool:
        body = event.model_dump(mode="json", by_alias=True)
        digest = sha256_hex(body)
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            inserted = await session.scalar(
                insert(SourceEvent)
                .values(
                    event_id=event.event_id,
                    scenario_epoch=epoch,
                    source=event.source,
                    event_type=event.event_type,
                    body_hash=digest,
                    envelope=body,
                )
                .on_conflict_do_nothing(index_elements=[SourceEvent.event_id])
                .returning(SourceEvent.event_id)
            )
            if inserted is None:
                existing = await session.get(SourceEvent, event.event_id)
                if existing is None:
                    raise RuntimeError("conflicting source event is missing")
                if existing.body_hash != digest:
                    raise ApiError(
                        ErrorCode.CONFLICT,
                        "event ID was reused with changed data",
                        status_code=409,
                    )
                return False
            subscriptions = await session.scalars(
                select(Subscription).where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.source == event.source,
                    Subscription.active.is_(True),
                    Subscription.event_types.any(event.event_type),  # type: ignore[arg-type]
                )
            )
            for subscription in subscriptions:
                session.add(
                    Delivery(
                        delivery_id=new_id("dlv"),
                        scenario_epoch=epoch,
                        event_id=event.event_id,
                        subscription_id=subscription.subscription_id,
                        state="pending",
                        attempt_count=0,
                        next_attempt_at=event.occurred_at,
                    )
                )
            return True

    async def list_subscriptions(self, source: str) -> list[WebhookSubscriptionView]:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            rows = await session.scalars(
                select(Subscription)
                .where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.source == source,
                    Subscription.active.is_(True),
                )
                .order_by(Subscription.subscription_id)
            )
            return [
                WebhookSubscriptionView(
                    subscriptionId=row.subscription_id,
                    source=row.source,
                    eventTypes=row.event_types,
                    targetUrl=AnyHttpUrl(row.target_url),
                    version=row.version,
                )
                for row in rows
            ]

    async def delete_subscription(
        self,
        source: str,
        caller_id: str,
        idempotency_key: str,
        subscription_id: str,
        expected_version: int,
    ) -> None:
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            namespace = IdempotencyNamespace(
                "tenant_synthetic",
                caller_id,
                f"{source}.subscription.delete",
                idempotency_key,
            )

            async def work() -> StoredResponse:
                row = await session.scalar(
                    select(Subscription)
                    .where(
                        Subscription.scenario_epoch == epoch,
                        Subscription.source == source,
                        Subscription.subscription_id == subscription_id,
                        Subscription.active.is_(True),
                    )
                    .with_for_update()
                )
                if row is None:
                    raise ApiError(
                        ErrorCode.NOT_FOUND,
                        "subscription was not found",
                        status_code=404,
                    )
                if row.version != expected_version:
                    raise ApiError(
                        ErrorCode.CONFLICT,
                        "subscription version differs",
                        status_code=409,
                    )
                row.active = False
                row.version += 1
                return StoredResponse(204, {}, {})

            await run_idempotent(
                session,
                epoch,
                namespace,
                {"subscriptionId": subscription_id, "expectedVersion": expected_version},
                work,
            )

    async def next_delivery(
        self,
        now: datetime,
    ) -> tuple[Delivery, Subscription, SourceEvent] | None:
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            if state is None or state.mode != "active":
                raise RuntimeError("relay scenario is not active")
            epoch = state.active_epoch
            delivery = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.scenario_epoch == epoch,
                    or_(
                        Delivery.state == "pending",
                        (Delivery.state == "in_flight") & (Delivery.lease_until <= now),
                    ),
                    Delivery.next_attempt_at <= now,
                )
                .order_by(Delivery.next_attempt_at, Delivery.delivery_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if delivery is None:
                return None
            subscription = await session.scalar(
                select(Subscription).where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.subscription_id == delivery.subscription_id,
                )
            )
            event = await session.get(SourceEvent, delivery.event_id)
            if subscription is None or event is None:
                raise RuntimeError("delivery source records are missing")
            if delivery.state == "in_flight" and delivery.current_attempt_id is not None:
                uncertain = await session.get(DeliveryAttempt, delivery.current_attempt_id)
                if uncertain is not None and uncertain.outcome == "in_flight":
                    uncertain.outcome = "uncertain"
                    uncertain.resulting_next_attempt_at = now
            lease_token = new_id("lease")
            attempt_id = new_id("att")
            delivery.state = "in_flight"
            delivery.attempt_count += 1
            delivery.lease_until = now + timedelta(seconds=30)
            delivery.lease_token = lease_token
            delivery.current_attempt_id = attempt_id
            session.add(
                DeliveryAttempt(
                    attempt_id=attempt_id,
                    scenario_epoch=delivery.scenario_epoch,
                    delivery_id=delivery.delivery_id,
                    attempt_number=delivery.attempt_count,
                    lease_token=lease_token,
                    attempted_at=now,
                    response_status=None,
                    outcome="in_flight",
                    resulting_next_attempt_at=None,
                )
            )
            return delivery, subscription, event

    async def finish_attempt(
        self,
        delivery_id: str,
        lease_token: str,
        attempted_at: datetime,
        response_status: int | None,
        transport_error: str | None,
    ) -> bool:
        async with self.factory.begin() as session:
            delivery = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.delivery_id == delivery_id,
                    Delivery.state == "in_flight",
                    Delivery.lease_token == lease_token,
                )
                .with_for_update()
            )
            if delivery is None:
                return False
            if delivery.current_attempt_id is None:
                raise RuntimeError("delivery attempt is missing")
            attempt = await session.get(DeliveryAttempt, delivery.current_attempt_id)
            if attempt is None or attempt.lease_token != lease_token:
                raise RuntimeError("delivery attempt is missing")
            success = response_status is not None and 200 <= response_status < 300
            attempt.response_status = response_status
            attempt.outcome = "acknowledged" if success else transport_error or "http_failure"
            delivery.last_status = response_status
            delivery.lease_until = None
            delivery.lease_token = None
            delivery.current_attempt_id = None
            if success:
                delivery.state = "delivered"
                attempt.resulting_next_attempt_at = None
            else:
                delivery.state = "pending"
                delay = min(2**delivery.attempt_count, 300)
                delivery.next_attempt_at = attempted_at + timedelta(seconds=delay)
                attempt.resulting_next_attempt_at = delivery.next_attempt_at
            return True

    async def apply_delivery_fault(
        self,
        delivery: Delivery,
        decision: FaultDecision,
        now: datetime,
    ) -> bool:
        if decision.effect == FaultEffect.DUPLICATE:
            return False
        async with self.factory.begin() as session:
            current = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.delivery_id == delivery.delivery_id,
                    Delivery.state == "in_flight",
                    Delivery.lease_token == delivery.lease_token,
                )
                .with_for_update()
            )
            if current is None:
                return True
            if current.current_attempt_id is None:
                raise RuntimeError("delivery attempt is missing")
            attempt = await session.get(DeliveryAttempt, current.current_attempt_id)
            if attempt is None:
                raise RuntimeError("delivery attempt is missing")
            if decision.effect == FaultEffect.DELAY:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(milliseconds=decision.delay_ms or 1)
                resulting_next_attempt_at = current.next_attempt_at
                outcome = "injected_delay"
            elif decision.effect == FaultEffect.SUPPRESS:
                current.state = "suppressed"
                resulting_next_attempt_at = None
                outcome = "injected_suppress"
            elif decision.effect == FaultEffect.RETRY:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(seconds=1)
                resulting_next_attempt_at = current.next_attempt_at
                outcome = "injected_retry"
            elif decision.effect == FaultEffect.REORDER:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(seconds=2)
                resulting_next_attempt_at = current.next_attempt_at
                outcome = "injected_reorder"
            else:
                return False
            attempt.outcome = outcome
            attempt.resulting_next_attempt_at = resulting_next_attempt_at
            current.lease_until = None
            current.lease_token = None
            current.current_attempt_id = None
        return True

    @asynccontextmanager
    async def delivery_fence(
        self,
        delivery: Delivery,
        control_epoch: str,
    ) -> AsyncIterator[bool]:
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            current = await session.get(Delivery, delivery.delivery_id)
            allowed = (
                state is not None
                and state.mode == "active"
                and state.active_epoch == delivery.scenario_epoch
                and control_epoch == delivery.scenario_epoch
                and current is not None
                and current.state == "in_flight"
                and current.lease_token == delivery.lease_token
            )
            yield allowed
