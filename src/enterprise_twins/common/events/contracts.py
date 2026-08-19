from datetime import UTC, datetime
from typing import Annotated, Any, Self

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

EventId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]
EventType = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=160)]
SourceName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]
RequestId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: EventId = Field(alias="eventId")
    event_type: EventType = Field(alias="eventType")
    schema_version: str = Field(default="1.0", alias="schemaVersion")
    source: SourceName
    subject: str
    resource_version: int = Field(ge=1, alias="resourceVersion")
    correlation_id: RequestId = Field(alias="correlationId")
    causation_id: RequestId = Field(alias="causationId")
    occurred_at: datetime = Field(alias="occurredAt")
    recorded_at: datetime = Field(alias="recordedAt")
    data: dict[str, Any]

    @field_validator("occurred_at", "recorded_at")
    @classmethod
    def normalise_timestamp(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("event timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_recording_order(self) -> Self:
        if self.recorded_at < self.occurred_at:
            raise ValueError("recordedAt must not be before occurredAt")
        return self


class WebhookSubscriptionCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_types: list[EventType] = Field(min_length=1, alias="eventTypes")
    target_url: AnyHttpUrl = Field(max_length=1000, alias="targetUrl")


class WebhookSubscriptionView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    subscription_id: EventId = Field(alias="subscriptionId")
    source: SourceName
    event_types: list[EventType] = Field(min_length=1, alias="eventTypes")
    target_url: AnyHttpUrl = Field(max_length=1000, alias="targetUrl")
    version: int = Field(ge=1)


class WebhookSubscriptionCreated(WebhookSubscriptionView):
    secret: Annotated[str, StringConstraints(min_length=1, max_length=200)]
