from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from math import isfinite
from typing import Annotated, Any, Protocol, cast

import httpx
import jwt
from fastapi import Depends, Header

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.credentials import parse_bearer
from enterprise_twins.common.control.contracts import ClockValue
from enterprise_twins.common.http.errors import ApiError, ErrorCode

MIN_NUMERIC_DATE = -62_135_596_800
MAX_NUMERIC_DATE_EXCLUSIVE = 253_402_300_800


class TokenClock(Protocol):
    async def snapshot(self) -> ClockValue:
        raise NotImplementedError

    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError


class JwtVerifier:
    def __init__(
        self,
        issuer: str,
        audience: str,
        jwks_url: str,
        clock: TokenClock,
        client: httpx.AsyncClient,
    ) -> None:
        self.issuer = issuer
        self.audience = audience
        self.jwks_url = jwks_url
        self.clock = clock
        self.client = client
        self.keys: dict[str, jwt.PyJWK] = {}

    async def refresh(self) -> None:
        response = await self.client.get(self.jwks_url, timeout=2.0)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get("keys"), list):
            raise ValueError("JWKS response is invalid")
        if not body["keys"]:
            raise ValueError("JWKS response is empty")
        keys: dict[str, jwt.PyJWK] = {}
        for item in body["keys"]:
            if not isinstance(item, dict) or not self.is_trusted_jwk(item):
                raise ValueError("JWKS contains an untrusted key")
            kid = item["kid"]
            if kid in keys:
                raise ValueError("JWKS contains duplicate key IDs")
            keys[kid] = jwt.PyJWK.from_dict(item, algorithm="EdDSA")
        self.keys = keys

    @staticmethod
    def is_trusted_jwk(item: dict[str, Any]) -> bool:
        return (
            item.get("kty") == "OKP"
            and item.get("crv") == "Ed25519"
            and item.get("use") == "sig"
            and item.get("alg") == "EdDSA"
            and isinstance(item.get("kid"), str)
            and bool(item["kid"])
            and isinstance(item.get("x"), str)
            and bool(item["x"])
            and "d" not in item
        )

    @staticmethod
    def numeric_date(claims: dict[str, Any], name: str) -> int | float:
        value = claims[name]
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise jwt.InvalidTokenError(f"{name} must be a numeric date")
        if isinstance(value, float) and not isfinite(value):
            raise jwt.InvalidTokenError(f"{name} must be finite")
        if not MIN_NUMERIC_DATE <= value < MAX_NUMERIC_DATE_EXCLUSIVE:
            raise jwt.InvalidTokenError(f"{name} is outside the supported date range")
        return cast(int | float, value)

    @staticmethod
    def required_text(claims: dict[str, Any], name: str) -> str:
        value = claims[name]
        if not isinstance(value, str) or not value:
            raise jwt.InvalidTokenError(f"{name} must be a non-empty string")
        return value

    async def verify(self, token: str) -> Principal:
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "EdDSA":
                raise jwt.InvalidAlgorithmError
            kid = header.get("kid")
            if not isinstance(kid, str) or not kid:
                raise jwt.InvalidTokenError("kid is missing")
            if kid not in self.keys:
                await self.refresh()
            key = self.keys[kid]
            claims: dict[str, Any] = jwt.decode(
                token,
                key.key,
                algorithms=["EdDSA"],
                audience=self.audience,
                issuer=self.issuer,
                options={
                    "verify_exp": False,
                    "verify_nbf": False,
                    "verify_iat": False,
                    "require": [
                        "exp",
                        "iat",
                        "nbf",
                        "jti",
                        "sub",
                        "actor_type",
                        "role",
                        "scope",
                        "tenant",
                        "scenario_epoch",
                    ],
                },
            )
            snapshot = await self.clock.snapshot()
            now = snapshot.now.timestamp()
            if self.numeric_date(claims, "exp") <= now:
                raise jwt.ExpiredSignatureError
            if self.numeric_date(claims, "nbf") > now:
                raise jwt.ImmatureSignatureError
            if self.numeric_date(claims, "iat") > now:
                raise jwt.ImmatureSignatureError
            scenario_epoch = self.required_text(claims, "scenario_epoch")
            if scenario_epoch != snapshot.scenario_epoch:
                raise jwt.InvalidTokenError("token belongs to another scenario epoch")
            subject = self.required_text(claims, "sub")
            actor_type = self.required_text(claims, "actor_type")
            role = self.required_text(claims, "role")
            scope = claims["scope"]
            if not isinstance(scope, str):
                raise jwt.InvalidTokenError("scope must be a string")
            tenant = self.required_text(claims, "tenant")
            token_id = self.required_text(claims, "jti")
        except (
            KeyError,
            OverflowError,
            TypeError,
            ValueError,
            httpx.HTTPError,
            jwt.PyJWTError,
        ):
            raise ApiError(
                ErrorCode.UNAUTHENTICATED,
                "bearer token is invalid",
                status_code=401,
            ) from None
        return Principal(
            subject=subject,
            actor_type=actor_type,
            role=role,
            scopes=frozenset(scope.split()),
            tenant_id=tenant,
            token_id=token_id,
            scenario_epoch=scenario_epoch,
        )


class AuthDecisionRecorder(Protocol):
    async def record(
        self,
        principal: Principal | None,
        required_scopes: Sequence[str],
        allowed: bool,
    ) -> None:
        raise NotImplementedError


class BearerAuthenticator:
    def __init__(
        self,
        verifier: JwtVerifier,
        recorder: AuthDecisionRecorder | None = None,
    ) -> None:
        self.verifier = verifier
        self.recorder = recorder

    async def authenticate(
        self,
        authorization: Annotated[str | None, Header()] = None,
    ) -> Principal:
        token = parse_bearer(authorization)
        if token is None:
            if self.recorder is not None:
                await self.recorder.record(None, (), False)
            raise ApiError(
                ErrorCode.UNAUTHENTICATED,
                "bearer token is required",
                status_code=401,
            )
        try:
            principal = await self.verifier.verify(token)
        except ApiError as error:
            if self.recorder is not None and error.code == ErrorCode.UNAUTHENTICATED:
                await self.recorder.record(None, (), False)
            raise
        if self.recorder is not None:
            await self.recorder.record(principal, (), True)
        return principal


def require_scopes(
    authenticator: BearerAuthenticator,
    *required: str,
) -> Callable[[Principal], Awaitable[Principal]]:
    async def dependency(
        principal: Annotated[Principal, Depends(authenticator.authenticate)],
    ) -> Principal:
        try:
            principal.require(*required)
        except ApiError:
            if authenticator.recorder is not None:
                await authenticator.recorder.record(principal, required, False)
            raise
        if authenticator.recorder is not None:
            await authenticator.recorder.record(principal, required, True)
        return principal

    return dependency
