import asyncio
import hashlib
import hmac
from datetime import datetime
from typing import Protocol

import httpx
from sqlalchemy.exc import SQLAlchemyError

from enterprise_twins.common.canonical import canonical_json
from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
)
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings


def signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"v1={digest}"


class RelayControl(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


class WebhookWorker:
    def __init__(
        self,
        repository: RelayRepository,
        control: RelayControl,
        client: httpx.AsyncClient,
    ) -> None:
        self.repository = repository
        self.control = control
        self.client = client

    async def run_once(self) -> int:
        now = await self.control.now()
        candidate = await self.repository.next_delivery(now)
        if candidate is None:
            return 0
        delivery, subscription, event = candidate
        body = canonical_json(event.envelope)
        timestamp = now.isoformat().replace("+00:00", "Z")
        decision = await self.control.evaluate_fault(
            FaultProbe(
                targetService="event-relay",
                operation="webhook.deliver",
                phase=FaultPhase.EVENT_DELIVERY,
                resourceId=delivery.delivery_id,
                correlationId=event.envelope["correlationId"],
            )
        )
        if decision.effect is not None:
            handled = await self.repository.apply_delivery_fault(delivery, decision, now)
            if handled:
                return 1
        if delivery.lease_token is None:
            raise RuntimeError("delivery lease token is missing")
        control_epoch = await self.control.current_epoch()
        copies = 2 if decision.effect == FaultEffect.DUPLICATE else 1
        async with self.repository.delivery_fence(delivery, control_epoch) as allowed:
            if not allowed:
                return 1
            acknowledged_status: int | None = None
            last_status: int | None = None
            last_transport_error: str | None = None
            for _copy in range(copies):
                try:
                    response = await self.client.post(
                        subscription.target_url,
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "X-Twin-Event-Id": event.event_id,
                            "X-Twin-Timestamp": timestamp,
                            "X-Twin-Signature": signature(
                                subscription.signing_secret,
                                timestamp,
                                body,
                            ),
                        },
                        timeout=2.0,
                    )
                    last_status = response.status_code
                    if 200 <= response.status_code < 300:
                        acknowledged_status = response.status_code
                except httpx.HTTPError as error:
                    last_transport_error = type(error).__name__
            await self.repository.finish_attempt(
                delivery.delivery_id,
                delivery.lease_token,
                now,
                acknowledged_status if acknowledged_status is not None else last_status,
                None if last_status is not None else last_transport_error,
            )
        return 1


async def main() -> None:
    settings = RelaySettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    async with httpx.AsyncClient() as client:
        control = ControlClient(settings.control_url, settings.control_token, client)
        worker = WebhookWorker(
            RelayRepository(factory, settings.allowed_targets),
            control,
            client,
        )
        try:
            while True:
                try:
                    processed = await worker.run_once()
                except RuntimeError, httpx.HTTPError, SQLAlchemyError:
                    processed = 0
                if processed == 0:
                    await asyncio.sleep(0.05)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
