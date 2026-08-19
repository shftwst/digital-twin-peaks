from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ScenarioOwned:
    scenario_epoch: Mapped[str] = mapped_column(String(64), index=True)


class Versioned:
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


Timestamp = DateTime(timezone=True)
