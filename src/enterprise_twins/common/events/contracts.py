from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventId")
    event_type: str = Field(alias="eventType")
    schema_version: str = Field(default="1.0", alias="schemaVersion")
    source: str
    subject: str
    resource_version: int = Field(alias="resourceVersion")
    correlation_id: str = Field(alias="correlationId")
    causation_id: str = Field(alias="causationId")
    occurred_at: datetime = Field(alias="occurredAt")
    recorded_at: datetime = Field(alias="recordedAt")
    data: dict[str, Any]
