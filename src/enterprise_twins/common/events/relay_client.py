from collections.abc import Awaitable
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, StrictBool

from enterprise_twins.common.auth.credentials import validate_private_credential
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class PrivateRelayErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str
    message: str = Field(min_length=1)
    retryable: StrictBool
    requestId: str = Field(min_length=1)
    details: dict[str, Any]


class PrivateRelayErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    error: PrivateRelayErrorBody


SAFE_RELAY_ERRORS = {
    (404, ErrorCode.NOT_FOUND): "event relay resource was not found",
    (409, ErrorCode.CONFLICT): "event relay request conflicts",
    (422, ErrorCode.INVALID_REQUEST): "event relay request is invalid",
}


def relay_unavailable() -> ApiError:
    return ApiError(
        ErrorCode.TEMPORARILY_UNAVAILABLE,
        "event relay is temporarily unavailable",
        status_code=503,
        retryable=True,
    )


def require_relay_success(
    response: httpx.Response,
    expected_statuses: set[int] | None = None,
) -> None:
    accepted = expected_statuses or set(range(200, 300))
    if response.status_code in accepted:
        return
    if response.status_code < 400:
        raise relay_unavailable()
    content_type = response.headers.get("Content-Type", "").partition(";")[0].strip().lower()
    if content_type != "application/json":
        raise relay_unavailable()
    try:
        envelope = PrivateRelayErrorEnvelope.model_validate(response.json())
        code = ErrorCode(envelope.error.code)
    except TypeError, ValueError:
        raise relay_unavailable() from None
    message = SAFE_RELAY_ERRORS.get((response.status_code, code))
    if message is None:
        raise relay_unavailable()
    raise ApiError(
        code,
        message,
        status_code=response.status_code,
    )


class RelayClient:
    def __init__(self, base_url: str, source: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.headers = {"Authorization": f"Bearer {validate_private_credential(token)}"}
        self.client = client

    @staticmethod
    async def receive(request: Awaitable[httpx.Response]) -> httpx.Response:
        try:
            return await request
        except httpx.HTTPError:
            raise relay_unavailable() from None

    async def ingest(self, event: EventEnvelope) -> None:
        response = await self.receive(
            self.client.post(
                f"{self.base_url}/internal/v1/events",
                headers=self.headers,
                json=event.model_dump(mode="json", by_alias=True),
                timeout=2.0,
            )
        )
        require_relay_success(response, {202})

    async def ready_epoch(self) -> str:
        response = await self.receive(
            self.client.get(
                f"{self.base_url}/health/ready",
                timeout=2.0,
            )
        )
        try:
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("status") != "ready":
                raise ValueError("Relay is not ready")
            epoch = response.headers.get("X-Scenario-Epoch")
            if not isinstance(epoch, str) or not epoch:
                raise ValueError("Relay readiness epoch is missing")
            return epoch
        except httpx.HTTPError, TypeError, ValueError:
            raise relay_unavailable() from None

    async def create_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        request: WebhookSubscriptionCreate,
    ) -> WebhookSubscriptionCreated:
        response = await self.receive(
            self.client.post(
                f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
                headers=self.headers
                | {"X-Caller-Id": caller_id, "Idempotency-Key": idempotency_key},
                json=request.model_dump(mode="json", by_alias=True),
                timeout=2.0,
            )
        )
        require_relay_success(response, {201})
        try:
            return WebhookSubscriptionCreated.model_validate(response.json())
        except (TypeError, ValueError):  # fmt: skip
            raise relay_unavailable() from None

    async def list_subscriptions(self) -> list[WebhookSubscriptionView]:
        response = await self.receive(
            self.client.get(
                f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
                headers=self.headers,
                timeout=2.0,
            )
        )
        require_relay_success(response, {200})
        try:
            body = response.json()
            if not isinstance(body, list):
                raise TypeError
            return [WebhookSubscriptionView.model_validate(item) for item in body]
        except (TypeError, ValueError):  # fmt: skip
            raise relay_unavailable() from None

    async def delete_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        subscription_id: str,
        version: int,
    ) -> None:
        response = await self.receive(
            self.client.delete(
                f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions/{subscription_id}",
                headers=self.headers
                | {
                    "X-Caller-Id": caller_id,
                    "Idempotency-Key": idempotency_key,
                    "If-Match": f'"{version}"',
                },
                timeout=2.0,
            )
        )
        require_relay_success(response, {204})
