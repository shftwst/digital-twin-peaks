from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ControlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CONTROL_", extra="ignore")

    database_url: str
    controller_token: str
    twin_token: str
    participant_token: str = "participant-local-token"  # noqa: S105
    participants: dict[str, str] = Field(default_factory=dict)
    scenario_root: Path = Path("scenarios/base")
    bootstrap_scenario: str = "platform-contracts"
    bootstrap_version: int = 1
