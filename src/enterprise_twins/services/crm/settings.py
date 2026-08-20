from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential
from enterprise_twins.common.auth.origins import IssuerOrigins, canonical_http_origin


class CrmSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CRM_", extra="ignore")

    database_url: str
    cursor_secret: PrivateCredential = "crm-local-cursor-secret"  # noqa: S105
    identity_issuer: str = "http://identity:8000"
    identity_issuer_aliases: IssuerOrigins = (
        "http://identity:8000",
        "http://127.0.0.1:8101",
    )
    identity_jwks_url: str = "http://identity:8000/.well-known/jwks.json"
    identity_audience: str = "enterprise-twins"
    control_url: str = "http://control:8000"
    control_token: PrivateCredential = "twin-local-token"  # noqa: S105
    relay_url: str = "http://event-relay-api:8000"
    relay_token: PrivateCredential = "crm-relay-local-token"  # noqa: S105
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105

    @model_validator(mode="after")
    def validate_identity_configuration(self) -> CrmSettings:
        self.identity_issuer = canonical_http_origin(self.identity_issuer)
        if self.identity_issuer not in self.identity_issuer_aliases:
            raise ValueError("primary Identity issuer must be one of the configured aliases")
        expected_jwks = f"{self.identity_issuer}/.well-known/jwks.json"
        if self.identity_jwks_url != expected_jwks:
            raise ValueError("Identity JWKS URL must use the primary configured issuer")
        return self
