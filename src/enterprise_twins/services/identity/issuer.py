import base64
import hashlib
from datetime import datetime, timedelta

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from enterprise_twins.common.ids import new_id
from enterprise_twins.services.identity.models import IdentityClient


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class TokenIssuer:
    def __init__(
        self,
        issuer: str,
        audience: str,
        signing_seed: str,
        ttl_seconds: int,
    ) -> None:
        seed = hashlib.sha256(signing_seed.encode()).digest()
        self.private_key = Ed25519PrivateKey.from_private_bytes(seed)
        public = self.private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        self.kid = hashlib.sha256(public).hexdigest()[:16]
        self.public_jwk = {
            "kty": "OKP",
            "crv": "Ed25519",
            "use": "sig",
            "alg": "EdDSA",
            "kid": self.kid,
            "x": b64url(public),
        }
        self.issuer = issuer
        self.audience = audience
        self.ttl = timedelta(seconds=ttl_seconds)

    def issue(
        self,
        client: IdentityClient,
        scopes: list[str],
        now: datetime,
        scenario_epoch: str,
    ) -> tuple[str, str]:
        token_id = new_id("tok")
        issued_at = int(now.timestamp())
        claims = {
            "iss": self.issuer,
            "aud": self.audience,
            "sub": client.subject,
            "actor_type": client.actor_type,
            "role": client.role,
            "scope": " ".join(sorted(scopes)),
            "tenant": client.tenant_id,
            "scenario_epoch": scenario_epoch,
            "jti": token_id,
            "iat": issued_at,
            "nbf": issued_at,
            "exp": int((now + self.ttl).timestamp()),
        }
        token = jwt.encode(
            claims,
            self.private_key,
            algorithm="EdDSA",
            headers={"kid": self.kid},
        )
        return token, token_id
