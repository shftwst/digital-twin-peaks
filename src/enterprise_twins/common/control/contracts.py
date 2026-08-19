from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

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


class FaultPhase(StrEnum):
    BEFORE_VALIDATION = "before_validation"
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    READ = "read"
    EVENT_DELIVERY = "event_delivery"
    DOMAIN_COMPLETION = "domain_completion"


class FaultEffect(StrEnum):
    MALFORMED_TRANSPORT = "malformed_transport"
    UNAUTHENTICATED = "unauthenticated"
    RATE_LIMITED = "rate_limited"
    TEMPORARY_FAILURE = "temporary_failure"
    DELAY = "delay"
    TIMEOUT = "timeout"
    CONNECTION_LOSS = "connection_loss"
    MALFORMED_RESPONSE = "malformed_response"
    STALE_VERSION = "stale_version"
    TEMPORARY_ABSENCE = "temporary_absence"
    PAGINATION_CHANGE = "pagination_change"
    DUPLICATE = "duplicate"
    REORDER = "reorder"
    SUPPRESS = "suppress"
    RETRY = "retry"
    FAILED_REFUND = "failed_refund"
    DELAYED_SETTLEMENT = "delayed_settlement"
    BOUNCE = "bounce"
    DEFER = "defer"
    DROP = "drop"


class FaultRuleCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    rule_id: str = Field(alias="ruleId", max_length=120)
    target_service: str = Field(alias="targetService", max_length=80)
    operation: str = Field(max_length=160)
    phase: FaultPhase
    effect: FaultEffect
    actor_id: str | None = Field(default=None, alias="actorId", max_length=128)
    resource_id: str | None = Field(default=None, alias="resourceId", max_length=128)
    correlation_id: str | None = Field(default=None, alias="correlationId", max_length=128)
    request_hash: str | None = Field(default=None, alias="requestHash", max_length=64)
    occurrence: int = Field(default=1, ge=1)
    activation_count: int = Field(default=1, ge=1, alias="activationCount")
    delay_ms: int | None = Field(default=None, ge=0, alias="delayMs")
    response_data: dict[str, Any] = Field(default_factory=dict, alias="responseData")


class FaultProbe(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    target_service: str = Field(alias="targetService", max_length=80)
    operation: str = Field(max_length=160)
    phase: FaultPhase
    actor_id: str | None = Field(default=None, alias="actorId", max_length=128)
    resource_id: str | None = Field(default=None, alias="resourceId", max_length=128)
    correlation_id: str | None = Field(default=None, alias="correlationId", max_length=128)
    request_hash: str | None = Field(default=None, alias="requestHash", max_length=64)


class FaultDecision(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    rule_id: str | None = Field(default=None, alias="ruleId")
    effect: FaultEffect | None = None
    delay_ms: int | None = Field(default=None, alias="delayMs")
    response_data: dict[str, Any] = Field(default_factory=dict, alias="responseData")
