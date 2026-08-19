import hmac
from typing import Annotated

from fastapi import APIRouter, Header, Path

from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.http.etag import parse_quoted_version
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings


def relay_router(
    repository: RelayRepository,
    control: ControlClient,
    settings: RelaySettings,
) -> APIRouter:
    router = APIRouter(prefix="/internal/v1")

    def authorise(source: str, authorization: str | None) -> None:
        expected = settings.source_tokens.get(source, "")
        supplied = authorization.removeprefix("Bearer ") if authorization else ""
        if not expected or not hmac.compare_digest(expected, supplied):
            raise ApiError(
                ErrorCode.UNAUTHENTICATED,
                "invalid source credential",
                status_code=401,
            )

    @router.post("/sources/{source}/subscriptions", status_code=201)
    async def create_subscription(
        source: Annotated[str, Path(min_length=1, max_length=80)],
        body: WebhookSubscriptionCreate,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=200)
        ],
        caller_id: Annotated[str, Header(alias="X-Caller-Id", min_length=1, max_length=128)],
        authorization: Annotated[str | None, Header()] = None,
    ) -> WebhookSubscriptionCreated:
        authorise(source, authorization)
        try:
            return await repository.create_subscription(
                source,
                caller_id,
                idempotency_key,
                body,
                await control.now(),
            )
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, str(error), status_code=422) from error

    @router.get("/sources/{source}/subscriptions")
    async def list_subscriptions(
        source: Annotated[str, Path(min_length=1, max_length=80)],
        authorization: Annotated[str | None, Header()] = None,
    ) -> list[WebhookSubscriptionView]:
        authorise(source, authorization)
        return await repository.list_subscriptions(source)

    @router.delete("/sources/{source}/subscriptions/{subscription_id}", status_code=204)
    async def delete_subscription(
        source: Annotated[str, Path(min_length=1, max_length=80)],
        subscription_id: Annotated[str, Path(min_length=1, max_length=64)],
        if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=20)],
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=200)
        ],
        caller_id: Annotated[str, Header(alias="X-Caller-Id", min_length=1, max_length=128)],
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        authorise(source, authorization)
        expected_version = parse_quoted_version(if_match, minimum=1)
        await repository.delete_subscription(
            source,
            caller_id,
            idempotency_key,
            subscription_id,
            expected_version,
        )

    @router.post("/events", status_code=202)
    async def ingest(
        event: EventEnvelope,
        authorization: Annotated[str | None, Header()] = None,
    ) -> dict[str, bool]:
        authorise(event.source, authorization)
        return {"accepted": await repository.ingest(event)}

    return router
