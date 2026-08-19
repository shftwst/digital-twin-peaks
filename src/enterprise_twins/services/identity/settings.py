from pydantic_settings import BaseSettings, SettingsConfigDict


class IdentitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_IDENTITY_", extra="ignore")

    database_url: str
    issuer: str = "http://identity:8000"
    audience: str = "enterprise-twins"
    signing_seed: str = "identity-test-signing-seed"
    secret_pepper: str
    token_ttl_seconds: int = 600
    control_url: str = "http://control:8000"
    control_token: str = "twin-local-token"  # noqa: S105
    relay_url: str = "http://event-relay-api:8000"
    relay_token: str = "identity-relay-local-token"  # noqa: S105
    participant_token: str = "participant-local-token"  # noqa: S105
