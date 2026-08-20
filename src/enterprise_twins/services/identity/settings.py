from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential


class IdentitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_IDENTITY_", extra="ignore")

    database_url: str
    issuer: str = "http://identity:8000"
    audience: str = "enterprise-twins"
    signing_seed: PrivateCredential = "identity-test-signing-seed"
    secret_pepper: PrivateCredential
    token_ttl_seconds: int = 600
    control_url: str = "http://control:8000"
    control_token: PrivateCredential = "twin-local-token"  # noqa: S105
    relay_url: str = "http://event-relay-api:8000"
    relay_token: PrivateCredential = "identity-relay-local-token"  # noqa: S105
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105
