from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class Customer(ScenarioOwned, Base):
    __tablename__ = "crm_customers"
    __table_args__ = (UniqueConstraint("scenario_epoch", "customer_id"),)

    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(80), index=True)
    display_name: Mapped[str] = mapped_column(String(200))
    primary_email: Mapped[str] = mapped_column(String(320), index=True)
    external_reference: Mapped[str] = mapped_column(String(120), index=True)
    account_status: Mapped[str] = mapped_column(String(40))
    contact_methods: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    external_identifiers: Mapped[dict[str, str]] = mapped_column(JSONB)
    version: Mapped[int] = mapped_column(Integer, default=1)


class CustomerNote(ScenarioOwned, Base):
    __tablename__ = "crm_customer_notes"
    __table_args__ = (UniqueConstraint("scenario_epoch", "note_id"),)

    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    note_id: Mapped[str] = mapped_column(String(80), index=True)
    customer_id: Mapped[str] = mapped_column(String(80), index=True)
    body: Mapped[str] = mapped_column(Text)
    association: Mapped[str] = mapped_column(String(80))
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(Timestamp)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    version: Mapped[int] = mapped_column(Integer, default=1)
