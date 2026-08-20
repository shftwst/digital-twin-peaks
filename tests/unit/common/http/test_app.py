from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.context import bind_response_epoch
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


@router.get("/oauth/probe")
async def oauth_probe() -> dict[str, str]:
    return {"status": "reachable"}


@router.get("/v1/internal-failure")
async def internal_failure() -> None:
    raise RuntimeError("database password is sensitive-value")


@router.get("/epoch-bound-response")
async def epoch_bound_response(response: Response) -> dict[str, str]:
    response.headers["X-Scenario-Epoch"] = "epoch_bound_to_body"
    return {"scenarioEpoch": "epoch_bound_to_body"}


@router.get("/v1/epoch-bound-api-error")
async def epoch_bound_api_error() -> None:
    bind_response_epoch("epoch_bound_to_error")
    raise ApiError(ErrorCode.CONFLICT, "version changed", status_code=409)


@router.get("/v1/epoch-bound-validation-error")
async def epoch_bound_validation_error() -> None:
    bind_response_epoch("epoch_bound_to_error")
    raise RequestValidationError(
        [
            {
                "type": "missing",
                "loc": ("query", "needed"),
                "msg": "Field required",
                "input": None,
            }
        ]
    )


@router.get("/v1/epoch-bound-http-error")
async def epoch_bound_http_error() -> None:
    bind_response_epoch("epoch_bound_to_error")
    raise HTTPException(status_code=404, detail="missing")


@router.get("/v1/epoch-bound-internal-error")
async def epoch_bound_internal_error() -> None:
    bind_response_epoch("epoch_bound_to_error")
    raise RuntimeError("sensitive old-epoch failure")


client = TestClient(create_app("probe", ("probe:read",), ReadyStatus(), (router,)))


def test_health_and_openapi_are_public() -> None:
    assert client.get("/health/live").json() == {"status": "live"}
    assert client.get("/health/ready").status_code == 200
    assert client.get("/openapi.json").json()["openapi"].startswith("3.1")


def test_business_request_requires_correlation_id() -> None:
    response = client.get("/v1/failure")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"


def test_supplied_correlation_id_is_bounded_on_v1_and_other_application_paths() -> None:
    for path in ("/v1/failure", "/oauth/probe"):
        response = client.get(path, headers={"X-Correlation-Id": "c" * 129})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_request"
        assert response.json()["error"]["requestId"].startswith("req_")
        assert response.headers["X-Scenario-Epoch"] == "epoch_test"


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


def test_unexpected_exception_uses_redacted_internal_error_envelope() -> None:
    response = TestClient(client.app, raise_server_exceptions=False).get(
        "/v1/internal-failure",
        headers={"X-Correlation-Id": "case-internal"},
    )

    assert response.status_code == 500
    assert response.json()["error"] == {
        "code": "internal_error",
        "message": "internal server error",
        "retryable": False,
        "requestId": response.headers["X-Request-Id"],
        "details": {},
    }
    assert response.headers["X-Scenario-Epoch"] == "epoch_test"
    assert "sensitive-value" not in response.text


def test_request_middleware_preserves_an_epoch_bound_by_business_work() -> None:
    response = client.get("/epoch-bound-response")

    assert response.json() == {"scenarioEpoch": "epoch_bound_to_body"}
    assert response.headers["X-Scenario-Epoch"] == "epoch_bound_to_body"


def test_error_handlers_prefer_the_epoch_bound_by_business_work() -> None:
    error_client = TestClient(client.app, raise_server_exceptions=False)

    for path, expected_status in (
        ("/v1/epoch-bound-api-error", 409),
        ("/v1/epoch-bound-validation-error", 422),
        ("/v1/epoch-bound-http-error", 404),
        ("/v1/epoch-bound-internal-error", 500),
    ):
        response = error_client.get(path, headers={"X-Correlation-Id": "case-bound-error"})

        assert response.status_code == expected_status
        assert response.headers["X-Scenario-Epoch"] == "epoch_bound_to_error"
