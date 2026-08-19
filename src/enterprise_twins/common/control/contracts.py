from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ClockValue(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    now: datetime
    scenario_epoch: str = Field(alias="scenarioEpoch")


class SetClockRequest(BaseModel):
    now: datetime

    @field_validator("now")
    @classmethod
    def normalise_now(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("virtual time must include a UTC offset")
        return value.astimezone(UTC)


class AdvanceClockRequest(BaseModel):
    duration: str
