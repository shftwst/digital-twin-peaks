from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class Subscription(ScenarioOwned, Base):
    __tablename__ = "relay_subscriptions"
    __table_args__ = (UniqueConstraint("scenario_epoch", "subscription_id"),)

    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    subscription_id: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(80), index=True)
    event_types: Mapped[list[str]] = mapped_column(ARRAY(String(160)))
    target_url: Mapped[str] = mapped_column(String(1000))
    signing_secret: Mapped[str] = mapped_column(String(200))
    version: Mapped[int] = mapped_column(Integer, default=1)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(Timestamp)


class SourceEvent(ScenarioOwned, Base):
    __tablename__ = "relay_source_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(80), index=True)
    event_type: Mapped[str] = mapped_column(String(160), index=True)
    body_hash: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB)


class Delivery(ScenarioOwned, Base):
    __tablename__ = "relay_deliveries"
    __table_args__ = (UniqueConstraint("event_id", "subscription_id"),)

    delivery_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), index=True)
    subscription_id: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(Timestamp, index=True)
    lease_until: Mapped[datetime | None] = mapped_column(Timestamp, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), index=True)
    current_attempt_id: Mapped[str | None] = mapped_column(String(64), index=True)
    last_status: Mapped[int | None] = mapped_column(Integer)


class DeliveryAttempt(ScenarioOwned, Base):
    __tablename__ = "relay_delivery_attempts"

    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    delivery_id: Mapped[str] = mapped_column(String(64), index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    lease_token: Mapped[str] = mapped_column(String(64), index=True)
    attempted_at: Mapped[datetime] = mapped_column(Timestamp)
    response_status: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String(40))
    resulting_next_attempt_at: Mapped[datetime | None] = mapped_column(Timestamp)


class WorkerHeartbeat(Base):
    __tablename__ = "relay_worker_heartbeat"

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(Timestamp)
    ready: Mapped[bool] = mapped_column(Boolean, nullable=False)
