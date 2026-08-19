from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned


class IdentityClient(ScenarioOwned, Base):
    __tablename__ = "identity_clients"
    __table_args__ = (UniqueConstraint("scenario_epoch", "client_id"),)

    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_id: Mapped[str] = mapped_column(String(120), index=True)
    secret_digest: Mapped[str] = mapped_column(String(128))
    subject: Mapped[str] = mapped_column(String(128))
    actor_type: Mapped[str] = mapped_column(String(24))
    role: Mapped[str] = mapped_column(String(80))
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String(160)))
    tenant_id: Mapped[str] = mapped_column(String(80))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
