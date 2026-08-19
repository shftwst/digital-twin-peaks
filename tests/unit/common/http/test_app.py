from fastapi import APIRouter, Query
from fastapi.testclient import TestClient

from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ReadyStatus:
    async def current_epoch(self) -> str:
        return "epoch_test"

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        return True, {"database": "ready"}


router = APIRouter()


@router.get("/v1/failure")
async def failure() -> None:
    raise ApiError(ErrorCode.CONFLICT, "version changed", status_code=409)


@router.get("/v1/validated")
async def validated(secret: str = Query(min_length=30)) -> dict[str, str]:
    return {"secret": secret}


client = TestClient(create_app("probe", ("probe:read",), ReadyStatus(), (router,)))


def test_health_and_openapi_are_public() -> None:
    assert client.get("/health/live").json() == {"status": "live"}
    assert client.get("/health/ready").status_code == 200
    assert client.get("/openapi.json").json()["openapi"].startswith("3.1")


def test_business_request_requires_correlation_id() -> None:
    response = client.get("/v1/failure")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"


def test_error_envelope_and_response_metadata() -> None:
    response = client.get("/v1/failure", headers={"X-Correlation-Id": "case-123"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"
    assert response.json()["error"]["requestId"].startswith("req_")
    assert response.headers["X-Scenario-Epoch"] == "epoch_test"
    assert response.headers["X-Request-Id"].startswith("req_")


def test_validation_errors_do_not_disclose_input_values() -> None:
    response = client.get(
        "/v1/validated",
        params={"secret": "client-secret"},
        headers={"X-Correlation-Id": "case-123"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert "client-secret" not in response.text
    assert '"input"' not in response.text


def test_unknown_business_route_uses_error_envelope() -> None:
    response = client.get("/v1/missing", headers={"X-Correlation-Id": "case-123"})
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert response.json()["error"]["requestId"].startswith("req_")
