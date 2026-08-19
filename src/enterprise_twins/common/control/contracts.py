from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ClockValue(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    now: datetime
    scenario_epoch: str = Field(alias="scenarioEpoch")


class SetClockRequest(BaseModel):
    now: datetime


class AdvanceClockRequest(BaseModel):
    duration: str
