from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential


class CrmSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CRM_", extra="ignore")

    database_url: str
    cursor_secret: PrivateCredential = "crm-local-cursor-secret"  # noqa: S105
    identity_issuer: str = "http://identity:8000"
    identity_jwks_url: str = "http://identity:8000/.well-known/jwks.json"
    identity_audience: str = "enterprise-twins"
    control_url: str = "http://control:8000"
    control_token: PrivateCredential = "twin-local-token"  # noqa: S105
    relay_url: str = "http://event-relay-api:8000"
    relay_token: PrivateCredential = "crm-relay-local-token"  # noqa: S105
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105
