import httpx

from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode


def require_relay_success(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
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

    async def ingest(self, event: EventEnvelope) -> None:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/events",
            headers=self.headers,
            json=event.model_dump(mode="json", by_alias=True),
            timeout=2.0,
        )
        require_relay_success(response)

    async def create_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        request: WebhookSubscriptionCreate,
    ) -> WebhookSubscriptionCreated:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
            headers=self.headers | {"X-Caller-Id": caller_id, "Idempotency-Key": idempotency_key},
            json=request.model_dump(mode="json", by_alias=True),
            timeout=2.0,
        )
        require_relay_success(response)
        return WebhookSubscriptionCreated.model_validate(response.json())

    async def list_subscriptions(self) -> list[WebhookSubscriptionView]:
        response = await self.client.get(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
            headers=self.headers,
            timeout=2.0,
        )
        require_relay_success(response)
        return [WebhookSubscriptionView.model_validate(item) for item in response.json()]

    async def delete_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        subscription_id: str,
        version: int,
    ) -> None:
        response = await self.client.delete(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions/{subscription_id}",
            headers=self.headers
            | {
                "X-Caller-Id": caller_id,
                "Idempotency-Key": idempotency_key,
                "If-Match": f'"{version}"',
            },
            timeout=2.0,
        )
        require_relay_success(response)
