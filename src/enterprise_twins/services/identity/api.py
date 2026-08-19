from typing import Annotated

from fastapi import APIRouter, Depends, Form, Header

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import BearerAuthenticator, require_scopes
from enterprise_twins.common.events.contracts import (
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.repository import IdentityRepository
from enterprise_twins.services.identity.settings import IdentitySettings


def identity_router(
    repository: IdentityRepository,
    issuer: TokenIssuer,
    settings: IdentitySettings,
    authenticator: BearerAuthenticator,
    relay: RelayClient | None,
) -> APIRouter:
    router = APIRouter()
    AnyPrincipal = Annotated[Principal, Depends(authenticator.authenticate)]
    WebhookPrincipal = Annotated[
        Principal,
        Depends(require_scopes(authenticator, "webhooks:manage")),
    ]

    @router.get("/.well-known/openid-configuration")
    async def metadata() -> dict[str, object]:
        return {
            "issuer": settings.issuer,
            "token_endpoint": f"{settings.issuer}/oauth/token",
            "jwks_uri": f"{settings.issuer}/.well-known/jwks.json",
            "grant_types_supported": ["client_credentials"],
            "token_endpoint_auth_methods_supported": ["client_secret_post"],
            "scopes_supported": [],
        }

    @router.get("/.well-known/jwks.json")
    async def jwks() -> dict[str, object]:
        return {"keys": [issuer.public_jwk]}

    @router.post("/oauth/token")
    async def token(
        grant_type: Annotated[str, Form(min_length=1, max_length=80)],
        client_id: Annotated[str, Form(min_length=1, max_length=120)],
        client_secret: Annotated[str, Form(min_length=1, max_length=1000)],
        scope: Annotated[str, Form(max_length=4000)] = "",
    ) -> dict[str, object]:
        if grant_type != "client_credentials":
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "grant_type is not supported",
                status_code=422,
            )
        result = await repository.authenticate(client_id, client_secret, scope.split())
        return {
            "access_token": result.access_token,
            "token_type": "bearer",
            "expires_in": result.expires_in,
            "scope": " ".join(result.scopes),
        }

    @router.get("/v1/me")
    async def me(principal: AnyPrincipal) -> dict[str, object]:
        return {
            "subject": principal.subject,
            "actorType": principal.actor_type,
            "role": principal.role,
            "scopes": sorted(principal.scopes),
            "tenant": principal.tenant_id,
            "tokenId": principal.token_id,
        }

    @router.get("/v1/capabilities")
    async def capabilities(_principal: AnyPrincipal) -> dict[str, object]:
        return {
            "service": "identity",
            "capabilities": ["tokens:issue", "webhooks:manage"],
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
        if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=20)],
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=200),
        ],
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
