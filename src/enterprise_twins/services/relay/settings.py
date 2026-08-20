from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential


class RelaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_RELAY_", extra="ignore")

    database_url: str
    control_url: str = "http://control:8000"
    control_token: PrivateCredential = "twin-local-token"  # noqa: S105
    source_tokens: dict[PrivateCredential, PrivateCredential] = Field(default_factory=dict)
    allowed_targets: set[str] = Field(default_factory=set)
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105
