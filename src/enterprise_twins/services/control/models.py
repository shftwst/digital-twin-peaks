from datetime import datetime

from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, Timestamp


class VirtualClock(Base):
    __tablename__ = "virtual_clock"

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    now: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
