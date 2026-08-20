import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.relay.models import WorkerHeartbeat
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings

WORKER_HEARTBEAT_MAX_AGE = timedelta(seconds=5)


class HeartbeatReader(Protocol):
    async def worker_heartbeat(self) -> WorkerHeartbeat | None:
        raise NotImplementedError


async def worker_heartbeat_state(
    reader: HeartbeatReader,
    *,
    now: datetime,
) -> str:
    heartbeat = await reader.worker_heartbeat()
    if heartbeat is None:
        return "missing"
    if not heartbeat.ready:
        return "degraded"
    if heartbeat.observed_at > now or now - heartbeat.observed_at > WORKER_HEARTBEAT_MAX_AGE:
        return "stale"
    return "ready"


async def check_from_env(
    *,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> bool:
    settings = RelaySettings()  # type: ignore[call-arg]
    engine = make_engine(settings.database_url)
    try:
        repository = RelayRepository(make_session_factory(engine), set())
        return await worker_heartbeat_state(repository, now=wall_clock()) == "ready"
    finally:
        await engine.dispose()


def main(run: Callable[[Coroutine[Any, Any, bool]], bool] = asyncio.run) -> None:
    if not run(check_from_env()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
