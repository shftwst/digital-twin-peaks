from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RelaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_RELAY_", extra="ignore")

    database_url: str
    control_url: str = "http://control:8000"
    control_token: str = "twin-local-token"  # noqa: S105
    source_tokens: dict[str, str] = Field(default_factory=dict)
    allowed_targets: set[str] = Field(default_factory=set)
    participant_token: str = "participant-local-token"  # noqa: S105
