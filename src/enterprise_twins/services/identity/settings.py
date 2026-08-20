from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential
from enterprise_twins.common.auth.origins import IssuerOrigins, canonical_http_origin


class IdentitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_IDENTITY_", extra="ignore")

    database_url: str
    issuer: str = "http://identity:8000"
    issuer_aliases: IssuerOrigins = (
        "http://identity:8000",
        "http://127.0.0.1:8101",
    )
    audience: str = "enterprise-twins"
    signing_seed: PrivateCredential = "identity-test-signing-seed"
    secret_pepper: PrivateCredential
    token_ttl_seconds: int = 600
    control_url: str = "http://control:8000"
    control_token: PrivateCredential = "twin-local-token"  # noqa: S105
    relay_url: str = "http://event-relay-api:8000"
    relay_token: PrivateCredential = "identity-relay-local-token"  # noqa: S105
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105

    @model_validator(mode="after")
    def validate_issuer_configuration(self) -> IdentitySettings:
        self.issuer = canonical_http_origin(self.issuer)
        if self.issuer not in self.issuer_aliases:
            raise ValueError("primary issuer must be one of the configured aliases")
        return self
