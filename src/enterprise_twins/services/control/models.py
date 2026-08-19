from datetime import datetime
from typing import Any

from sqlalchemy import Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class VirtualClock(Base):
    __tablename__ = "virtual_clock"

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    now: Mapped[datetime] = mapped_column(Timestamp, nullable=False)


class FaultRule(ScenarioOwned, Base):
    __tablename__ = "fault_rules"
    rule_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    target_service: Mapped[str] = mapped_column(String(80), index=True)
    operation: Mapped[str] = mapped_column(String(160), index=True)
    phase: Mapped[str] = mapped_column(String(40))
    effect: Mapped[str] = mapped_column(String(40))
    actor_id: Mapped[str | None] = mapped_column(String(128))
    resource_id: Mapped[str | None] = mapped_column(String(128))
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    request_hash: Mapped[str | None] = mapped_column(String(64))
    occurrence: Mapped[int] = mapped_column(Integer, nullable=False)
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    remaining_count: Mapped[int] = mapped_column(Integer, nullable=False)
    delay_ms: Mapped[int | None] = mapped_column(Integer)
    response_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class FaultActivation(ScenarioOwned, Base):
    __tablename__ = "fault_activations"
    activation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    rule_id: Mapped[str] = mapped_column(String(120), index=True)
    operation: Mapped[str] = mapped_column(String(160))
    correlation_id: Mapped[str | None] = mapped_column(String(128), index=True)
    phase: Mapped[str] = mapped_column(String(40))
    effect: Mapped[str] = mapped_column(String(40))
    activated_at: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
