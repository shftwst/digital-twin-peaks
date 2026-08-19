from collections.abc import Awaitable

import httpx

from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode


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
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "event relay returned an unexpected success status",
            status_code=503,
            retryable=True,
            details={"relayStatus": response.status_code},
        )
    try:
        error = response.json()["error"]
        code = ErrorCode(error["code"])
        message = str(error["message"])
        retryable = bool(error.get("retryable", False))
        details = dict(error.get("details", {}))
    except KeyError, TypeError, ValueError:
        code = ErrorCode.TEMPORARILY_UNAVAILABLE
        message = "event relay returned an invalid error response"
        retryable = response.status_code >= 500
        details = {}
    raise ApiError(
        code,
        message,
        status_code=response.status_code,
        retryable=retryable,
        details=details,
    )


class RelayClient:
    def __init__(self, base_url: str, source: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.headers = {"Authorization": f"Bearer {token}"}
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
