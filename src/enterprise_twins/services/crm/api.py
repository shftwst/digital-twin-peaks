from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import JSONResponse

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import BearerAuthenticator, require_scopes
from enterprise_twins.common.control.contracts import FaultEffect
from enterprise_twins.common.events.contracts import (
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.crm.repository import CustomerRepository
from enterprise_twins.services.crm.schemas import (
    CustomerPage,
    CustomerView,
    NoteCreate,
    NotePage,
)
from enterprise_twins.services.crm.service import CrmService, apply_post_commit_fault


def crm_router(
    repository: CustomerRepository,
    service: CrmService,
    authenticator: BearerAuthenticator,
    relay: RelayClient | None,
) -> APIRouter:
    router = APIRouter()
    AnyPrincipal = Annotated[Principal, Depends(authenticator.authenticate)]
    ReadPrincipal = Annotated[
        Principal,
        Depends(require_scopes(authenticator, "crm:read")),
    ]
    WritePrincipal = Annotated[
        Principal,
        Depends(require_scopes(authenticator, "crm:notes:write")),
    ]
    WebhookPrincipal = Annotated[
        Principal,
        Depends(require_scopes(authenticator, "webhooks:manage")),
    ]

    @router.get("/v1/customers")
    async def search_customers(
        _principal: ReadPrincipal,
        email: str | None = None,
        external_reference: str | None = Query(default=None, alias="externalReference"),
        identifier: str | None = None,
        limit: int = Query(default=50, ge=1, le=100),
        after: str | None = None,
    ) -> CustomerPage:
        return await repository.search(
            email=email,
            external_reference=external_reference,
            identifier=identifier,
            limit=limit,
            after=after,
        )

    @router.get("/v1/customers/{customer_id}")
    async def get_customer(
        customer_id: str,
        _principal: ReadPrincipal,
        response: Response,
    ) -> CustomerView:
        customer = await repository.get(customer_id)
        response.headers["ETag"] = f'"{customer.version}"'
        response.headers["X-Resource-Version"] = str(customer.version)
        return customer

    @router.get("/v1/customers/{customer_id}/notes")
    async def list_notes(
        customer_id: str,
        _principal: ReadPrincipal,
        include_archived: bool = Query(default=False, alias="includeArchived"),
    ) -> NotePage:
        return await repository.list_notes(customer_id, include_archived)

    @router.post("/v1/customers/{customer_id}/notes")
    async def create_note(
        customer_id: str,
        body: NoteCreate,
        principal: WritePrincipal,
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=200),
        ],
        if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=20)],
    ) -> Response:
        try:
            expected_version = int(if_match.strip('"'))
        except ValueError as error:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "If-Match is invalid",
                status_code=422,
            ) from error
        result = await service.create_note(
            customer_id,
            body,
            expected_version,
            idempotency_key,
            principal,
        )
        await apply_post_commit_fault(result)
        if result.fault.effect == FaultEffect.MALFORMED_RESPONSE:
            return Response(content=b"{", status_code=200, media_type="application/json")
        headers = result.response.headers | {
            "Idempotency-Replayed": str(result.replayed).lower(),
        }
        return JSONResponse(
            result.response.body,
            status_code=result.response.status_code,
            headers=headers,
        )

    @router.get("/v1/capabilities")
    async def capabilities(_principal: AnyPrincipal) -> dict[str, object]:
        return {
            "service": "crm",
            "capabilities": ["crm:read", "crm:notes:write", "webhooks:manage"],
        }

    @router.post("/v1/webhook-subscriptions", status_code=201)
    async def create_subscription(
        body: WebhookSubscriptionCreate,
        principal: WebhookPrincipal,
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=200),
        ],
    ) -> WebhookSubscriptionCreated:
        if relay is None:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "event relay is unavailable",
                status_code=503,
                retryable=True,
            )
        return await relay.create_subscription(
            principal.subject,
            idempotency_key,
            body,
        )

    @router.get("/v1/webhook-subscriptions")
    async def list_subscriptions(
        _principal: WebhookPrincipal,
    ) -> list[WebhookSubscriptionView]:
        if relay is None:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "event relay is unavailable",
                status_code=503,
                retryable=True,
            )
        return await relay.list_subscriptions()

    @router.delete("/v1/webhook-subscriptions/{subscription_id}", status_code=204)
    async def delete_subscription(
        subscription_id: str,
        principal: WebhookPrincipal,
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=200),
        ],
        if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=20)],
    ) -> None:
        if relay is None:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "event relay is unavailable",
                status_code=503,
                retryable=True,
            )
        try:
            expected_version = int(if_match.strip('"'))
            if expected_version < 1:
                raise ValueError
        except ValueError as error:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "If-Match is invalid",
                status_code=422,
            ) from error
        await relay.delete_subscription(
            principal.subject,
            idempotency_key,
            subscription_id,
            expected_version,
        )

    return router
