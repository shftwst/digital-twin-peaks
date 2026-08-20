# ruff: noqa: S106

import base64
from datetime import UTC, datetime

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import BearerAuthenticator, JwtVerifier
from enterprise_twins.common.http.errors import ApiError, ErrorCode

NOW = datetime(2026, 8, 19, 10, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.value = NOW
        self.epoch = "epoch_1"

    async def now(self) -> datetime:
        return self.value

    async def current_epoch(self) -> str:
        return self.epoch


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def signing_material(kid: str = "key-1") -> tuple[Ed25519PrivateKey, dict[str, str]]:
    private_key = Ed25519PrivateKey.from_private_bytes(b"1" * 32)
    public = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return private_key, {
        "kty": "OKP",
        "crv": "Ed25519",
        "use": "sig",
        "alg": "EdDSA",
        "kid": kid,
        "x": b64url(public),
    }


def claims(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "iss": "http://identity:8000",
        "aud": "enterprise-twins",
        "sub": "person-1",
        "actor_type": "human",
        "role": "support_agent",
        "scope": "crm:read crm:notes:write",
        "tenant": "tenant_synthetic",
        "scenario_epoch": "epoch_1",
        "jti": "tok_1",
        "iat": int(NOW.timestamp()),
        "nbf": int(NOW.timestamp()),
        "exp": int(NOW.timestamp()) + 600,
    }
    values.update(overrides)
    return values


def token(
    private_key: Ed25519PrivateKey,
    *,
    kid: str = "key-1",
    values: dict[str, object] | None = None,
) -> str:
    return jwt.encode(
        values or claims(),
        private_key,
        algorithm="EdDSA",
        headers={"kid": kid},
    )


class UnavailableVerifier:
    async def verify(self, _token: str) -> Principal:
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "Control is temporarily unavailable",
            status_code=503,
            retryable=True,
        )


class Recorder:
    def __init__(self) -> None:
        self.calls = 0

    async def record(
        self,
        _principal: Principal | None,
        _required_scopes: object,
        _allowed: bool,
    ) -> None:
        self.calls += 1


@pytest.mark.asyncio
async def test_authenticator_does_not_record_dependency_outage_as_denial() -> None:
    recorder = Recorder()
    authenticator = BearerAuthenticator(UnavailableVerifier(), recorder)  # type: ignore[arg-type]

    with pytest.raises(ApiError) as raised:
        await authenticator.authenticate("Bearer token")

    assert raised.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert recorder.calls == 0


def test_principal_reports_exact_missing_scopes() -> None:
    principal = Principal(
        subject="person-1",
        actor_type="human",
        role="support_agent",
        scopes=frozenset({"crm:read"}),
        tenant_id="tenant_synthetic",
        token_id="tok_1",
        scenario_epoch="epoch_1",
    )
    with pytest.raises(ApiError) as raised:
        principal.require("crm:read", "crm:notes:write")
    assert raised.value.status_code == 403
    assert raised.value.details == {"requiredScopes": ["crm:notes:write"]}


@pytest.mark.asyncio
async def test_unknown_key_refreshes_once_then_uses_the_cached_ed25519_key() -> None:
    private_key, public_jwk = signing_material()
    requests: list[httpx.Request] = []

    async def jwks(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"keys": [public_jwk]})

    clock = Clock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            clock,
            client,
        )
        encoded = token(private_key)
        first = await verifier.verify(encoded)
        second = await verifier.verify(encoded)

    assert first == second
    assert first.subject == "person-1"
    assert first.scopes == frozenset({"crm:read", "crm:notes:write"})
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_claims",
    [
        {"iss": "http://untrusted-issuer"},
        {"aud": "wrong-audience"},
        {"exp": int(NOW.timestamp())},
        {"exp": float("nan")},
        {"exp": 10**1000},
        {"nbf": int(NOW.timestamp()) + 1},
        {"nbf": NOW.timestamp() + 0.5},
        {"iat": int(NOW.timestamp()) + 1},
        {"scenario_epoch": "epoch_old"},
        {"jti": None},
        {"tenant": None},
    ],
)
async def test_verifier_rejects_invalid_standard_and_platform_claims(
    changed_claims: dict[str, object],
) -> None:
    private_key, public_jwk = signing_material()
    altered = claims(**changed_claims)
    if changed_claims.get("jti", object()) is None:
        altered.pop("jti")
    if changed_claims.get("tenant", object()) is None:
        altered.pop("tenant")
    encoded = token(private_key, values=altered)

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [public_jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            Clock(),
            client,
        )
        with pytest.raises(ApiError) as raised:
            await verifier.verify(encoded)

    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert raised.value.message == "bearer token is invalid"
    assert encoded not in str(raised.value)


@pytest.mark.asyncio
async def test_old_epoch_token_fails_after_clock_epoch_changes() -> None:
    private_key, public_jwk = signing_material()

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [public_jwk]})

    clock = Clock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            clock,
            client,
        )
        encoded = token(private_key)
        assert (await verifier.verify(encoded)).scenario_epoch == "epoch_1"
        clock.epoch = "epoch_2"
        with pytest.raises(ApiError) as raised:
            await verifier.verify(encoded)

    assert raised.value.code == ErrorCode.UNAUTHENTICATED


@pytest.mark.asyncio
async def test_verifier_rejects_algorithm_confusion_and_untrusted_jwk_types() -> None:
    private_key, _public_jwk = signing_material(kid="bad-key")
    encoded = token(private_key, kid="bad-key")
    untrusted_jwk = {
        "kty": "oct",
        "use": "sig",
        "alg": "HS256",
        "kid": "bad-key",
        "k": b64url(b"untrusted-secret"),
    }

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [untrusted_jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            Clock(),
            client,
        )
        with pytest.raises(ApiError) as raised:
            await verifier.verify(encoded)

    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert "untrusted-secret" not in str(raised.value)


@pytest.mark.asyncio
async def test_unknown_key_id_is_denied_after_refresh_without_leaking_it() -> None:
    private_key, known_jwk = signing_material(kid="known-key")
    encoded = token(private_key, kid="unknown-sensitive-key")

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [known_jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            Clock(),
            client,
        )
        with pytest.raises(ApiError) as raised:
            await verifier.verify(encoded)

    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert "unknown-sensitive-key" not in str(raised.value)


@pytest.mark.asyncio
async def test_jwks_refresh_rejects_mixed_sets_without_replacing_cached_keys() -> None:
    cached_private, cached_jwk = signing_material(kid="cached-key")
    new_private, new_jwk = signing_material(kid="new-key")
    malformed_jwk = {"kty": "OKP", "kid": "malformed-sensitive-key"}

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [new_jwk, malformed_jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            Clock(),
            client,
        )
        cached_key = jwt.PyJWK.from_dict(cached_jwk, algorithm="EdDSA")
        verifier.keys["cached-key"] = cached_key
        with pytest.raises(ApiError) as raised:
            await verifier.verify(token(new_private, kid="new-key"))
        cached = await verifier.verify(token(cached_private, kid="cached-key"))

    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert raised.value.message == "bearer token is invalid"
    assert verifier.keys == {"cached-key": cached_key}
    assert cached.subject == "person-1"
    assert "malformed-sensitive-key" not in str(raised.value)


@pytest.mark.asyncio
async def test_jwks_refresh_rejects_private_keys_without_replacing_cached_keys() -> None:
    cached_private, cached_jwk = signing_material(kid="cached-key")
    new_private, new_jwk = signing_material(kid="private-key")
    private_value = new_private.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    private_jwk = new_jwk | {"d": b64url(private_value)}

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [private_jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as client:
        verifier = JwtVerifier(
            "http://identity:8000",
            "enterprise-twins",
            "http://identity:8000/.well-known/jwks.json",
            Clock(),
            client,
        )
        cached_key = jwt.PyJWK.from_dict(cached_jwk, algorithm="EdDSA")
        verifier.keys["cached-key"] = cached_key
        with pytest.raises(ApiError) as raised:
            await verifier.verify(token(new_private, kid="private-key"))
        cached = await verifier.verify(token(cached_private, kid="cached-key"))

    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert raised.value.message == "bearer token is invalid"
    assert verifier.keys == {"cached-key": cached_key}
    assert cached.subject == "person-1"
    assert b64url(private_value) not in str(raised.value)
