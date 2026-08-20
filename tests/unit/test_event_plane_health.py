import asyncio

import httpx
import pytest

from enterprise_twins.common.events.publisher import DispatcherSupervisor
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.relay.delivery import run_worker_iteration


class CrashThenRecoverDispatcher:
    def __init__(self) -> None:
        self.calls = 0
        self.crashed = asyncio.Event()
        self.recovery_entered = asyncio.Event()
        self.release_recovery = asyncio.Event()
        self.park_entered = asyncio.Event()
        self.park = asyncio.Event()

    async def run_once(self) -> int:
        self.calls += 1
        if self.calls == 1:
            self.crashed.set()
            raise RuntimeError("dispatcher crashed")
        if self.calls == 2:
            self.recovery_entered.set()
            await self.release_recovery.wait()
            return 0
        self.park_entered.set()
        await self.park.wait()
        return 0


@pytest.mark.asyncio
async def test_dispatcher_supervisor_observes_a_crash_and_recovers() -> None:
    clock = [1.0]
    dispatcher = CrashThenRecoverDispatcher()
    supervisor = DispatcherSupervisor(
        dispatcher,
        interval_seconds=0,
        freshness_seconds=1,
        monotonic=lambda: clock[0],
    )

    task = supervisor.start()
    await dispatcher.crashed.wait()
    await dispatcher.recovery_entered.wait()

    assert supervisor.is_ready() is False

    dispatcher.release_recovery.set()
    await dispatcher.park_entered.wait()
    assert supervisor.is_ready() is True
    assert task.done() is False

    clock[0] = 3.0
    assert supervisor.is_ready() is False

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert supervisor.is_ready() is False


@pytest.mark.asyncio
async def test_relay_client_ready_epoch_uses_public_readiness_without_credentials() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"X-Scenario-Epoch": "epoch_7"},
            json={"status": "ready", "checks": {"worker": "ready"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        relay = RelayClient("http://relay/", "crm", "source-token", http_client)
        assert await relay.ready_epoch() == "epoch_7"

    assert requests[0].url.path == "/health/ready"
    assert "Authorization" not in requests[0].headers
    assert requests[0].extensions["timeout"] == {
        "connect": 2.0,
        "read": 2.0,
        "write": 2.0,
        "pool": 2.0,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(503, json={"status": "not_ready"}),
        httpx.Response(200, content=b'{"status":'),
        httpx.Response(200, json={"status": "not_ready"}),
        httpx.Response(200, json={"status": "ready"}),
    ],
)
async def test_relay_client_ready_epoch_redacts_dependency_failures(
    response: httpx.Response,
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: response)
    ) as http_client:
        relay = RelayClient("http://relay", "crm", "source-token", http_client)
        with pytest.raises(ApiError) as raised:
            await relay.ready_epoch()

    assert raised.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert raised.value.status_code == 503
    assert raised.value.retryable is True
    assert raised.value.details == {}


@pytest.mark.asyncio
async def test_relay_worker_persists_degraded_then_ready_health_on_recovery() -> None:
    heartbeats: list[bool] = []

    class Repository:
        async def record_worker_heartbeat(self, _observed_at: object, *, ready: bool) -> None:
            heartbeats.append(ready)

    class Worker:
        def __init__(self) -> None:
            self.repository = Repository()
            self.fail = True

        async def run_once(self) -> int:
            if self.fail:
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    "dependency unavailable",
                    status_code=503,
                    retryable=True,
                )
            return 0

    worker = Worker()
    assert await run_worker_iteration(worker) == 0  # type: ignore[arg-type]
    assert heartbeats == [False]

    worker.fail = False
    assert await run_worker_iteration(worker) == 0  # type: ignore[arg-type]
    assert heartbeats == [False, True]


@pytest.mark.asyncio
async def test_relay_worker_persists_degraded_health_before_reraising_a_fatal_failure() -> None:
    heartbeats: list[bool] = []

    class Repository:
        async def record_worker_heartbeat(self, _observed_at: object, *, ready: bool) -> None:
            heartbeats.append(ready)

    class Worker:
        repository = Repository()

        async def run_once(self) -> int:
            raise RuntimeError("unsupported webhook delivery fault effect")

    with pytest.raises(RuntimeError, match="unsupported webhook delivery fault effect"):
        await run_worker_iteration(Worker())  # type: ignore[arg-type]

    assert heartbeats == [False]


@pytest.mark.asyncio
async def test_relay_worker_preserves_a_fatal_failure_when_degraded_health_cannot_persist() -> None:
    heartbeat_attempts = 0

    class Repository:
        async def record_worker_heartbeat(self, _observed_at: object, *, ready: bool) -> None:
            nonlocal heartbeat_attempts
            heartbeat_attempts += 1
            assert ready is False
            raise OSError("heartbeat database detail")

    class Worker:
        repository = Repository()

        async def run_once(self) -> int:
            raise RuntimeError("fatal worker detail")

    with pytest.raises(RuntimeError, match="fatal worker detail"):
        await run_worker_iteration(Worker())  # type: ignore[arg-type]

    assert heartbeat_attempts == 1


@pytest.mark.asyncio
async def test_relay_worker_cancellation_propagates_without_recording_health() -> None:
    heartbeats: list[bool] = []

    class Repository:
        async def record_worker_heartbeat(self, _observed_at: object, *, ready: bool) -> None:
            heartbeats.append(ready)

    class Worker:
        repository = Repository()

        async def run_once(self) -> int:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_worker_iteration(Worker())  # type: ignore[arg-type]

    assert heartbeats == []
