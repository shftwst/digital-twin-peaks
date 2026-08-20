import httpx
import pytest

from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.control.contracts import FaultPhase, FaultProbe
from enterprise_twins.common.http.errors import ApiError, ErrorCode


def assert_unavailable(error: ApiError) -> None:
    assert error.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert error.status_code == 503
    assert error.retryable is True
    assert error.message == "Control is temporarily unavailable"
    assert error.details == {}
    assert "sensitive" not in str(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "status", "json", "model"])
async def test_snapshot_normalises_every_dependency_failure(failure: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if failure == "transport":
            raise httpx.ConnectError("sensitive transport detail", request=request)
        if failure == "status":
            return httpx.Response(401, text="sensitive upstream token")
        if failure == "json":
            return httpx.Response(200, content=b'{"sensitive":')
        return httpx.Response(200, json={"now": "sensitive", "scenarioEpoch": 7})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        client = ControlClient("http://control", "sensitive-token", http_client)
        with pytest.raises(ApiError) as raised:
            await client.snapshot()

    assert_unavailable(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["status", "json", "model"])
async def test_fault_evaluation_normalises_response_failures(failure: str) -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        if failure == "status":
            return httpx.Response(500, text="sensitive upstream error")
        if failure == "json":
            return httpx.Response(200, content=b'{"sensitive":')
        return httpx.Response(200, json={"effect": "not-an-effect", "sensitive": True})

    probe = FaultProbe(
        targetService="crm",
        operation="crm.note.create",
        phase=FaultPhase.BEFORE_COMMIT,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        client = ControlClient("http://control", "sensitive-token", http_client)
        with pytest.raises(ApiError) as raised:
            await client.evaluate_fault(probe)

    assert_unavailable(raised.value)


@pytest.mark.asyncio
async def test_ready_epoch_requires_ready_body_epoch_header_and_two_second_timeout() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"X-Scenario-Epoch": "epoch_7"},
            json={"status": "ready", "checks": {"database": "ready"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        client = ControlClient("http://control/", "sensitive-token", http_client)
        assert await client.ready_epoch() == "epoch_7"

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
        httpx.Response(
            200,
            headers={"X-Scenario-Epoch": ""},
            json={"status": "ready"},
        ),
    ],
)
async def test_ready_epoch_normalises_non_ready_or_malformed_responses(
    response: httpx.Response,
) -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        client = ControlClient("http://control", "sensitive-token", http_client)
        with pytest.raises(ApiError) as raised:
            await client.ready_epoch()

    assert_unavailable(raised.value)
