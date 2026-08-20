from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from enterprise_twins.common.auth.credentials import PrivateCredential


class ControlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CONTROL_", extra="ignore")

    database_url: str
    controller_token: PrivateCredential
    twin_token: PrivateCredential
    participant_token: PrivateCredential = "participant-local-token"  # noqa: S105
    participants: dict[str, str] = Field(default_factory=dict)
    scenario_root: Path = Path("scenarios/base")
    bootstrap_scenario: str = "platform-contracts"
    bootstrap_version: int = 1
