from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class ScenarioState(Base):
    __tablename__ = "scenario_state"

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    mode: Mapped[str] = mapped_column(String(24), nullable=False, default="uninitialised")
    active_epoch: Mapped[str] = mapped_column(String(64), nullable=False, default="none")
    pending_epoch: Mapped[str | None] = mapped_column(String(64))
    scenario_id: Mapped[str | None] = mapped_column(String(80))
    scenario_version: Mapped[int | None] = mapped_column(Integer)
    random_seed: Mapped[int | None] = mapped_column(BigInteger)
    manifest_checksum: Mapped[str | None] = mapped_column(String(64))
    pending_scenario_id: Mapped[str | None] = mapped_column(String(80))
    pending_scenario_version: Mapped[int | None] = mapped_column(Integer)
    pending_random_seed: Mapped[int | None] = mapped_column(BigInteger)
    pending_manifest_checksum: Mapped[str | None] = mapped_column(String(64))
    rollback_epoch: Mapped[str | None] = mapped_column(String(64))
    rollback_scenario_id: Mapped[str | None] = mapped_column(String(80))
    rollback_scenario_version: Mapped[int | None] = mapped_column(Integer)
    rollback_random_seed: Mapped[int | None] = mapped_column(BigInteger)
    rollback_manifest_checksum: Mapped[str | None] = mapped_column(String(64))


class AuditRecord(ScenarioOwned, Base):
    __tablename__ = "audit_records"

    audit_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    action: Mapped[str] = mapped_column(String(120), index=True)
    resource_type: Mapped[str] = mapped_column(String(80))
    resource_id: Mapped[str] = mapped_column(String(128), index=True)
    actor_id: Mapped[str] = mapped_column(String(128))
    correlation_id: Mapped[str] = mapped_column(String(128), index=True)
    occurred_at: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class IdempotencyRecord(ScenarioOwned, Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "actor_id", "operation", "key", name="uq_idempotency_namespace"
        ),
    )

    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(80))
    actor_id: Mapped[str] = mapped_column(String(128))
    operation: Mapped[str] = mapped_column(String(120))
    key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    response_headers: Mapped[dict[str, str] | None] = mapped_column(JSONB)


class OutboxRecord(ScenarioOwned, Base):
    __tablename__ = "outbox_records"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(160), index=True)
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published_at: Mapped[datetime | None] = mapped_column(Timestamp)


Index("ix_outbox_pending", OutboxRecord.published, OutboxRecord.event_id)
