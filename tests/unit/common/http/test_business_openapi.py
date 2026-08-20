# ruff: noqa: S106

from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.verifier import JwtVerifier
from enterprise_twins.services.crm.app import create_crm_app
from enterprise_twins.services.crm.settings import CrmSettings
from enterprise_twins.services.identity.app import create_identity_app
from enterprise_twins.services.identity.repository import IdentityControl
from enterprise_twins.services.identity.settings import IdentitySettings

HTTP_METHODS = {"get", "post", "put", "patch", "delete"}
COMMON_RESPONSE_HEADERS = {"X-Request-Id", "X-Scenario-Epoch", "traceparent"}


def service_openapi_documents() -> dict[str, dict[str, Any]]:
    factory = cast(async_sessionmaker[AsyncSession], object())
    control = cast(IdentityControl, object())
    identity = create_identity_app(
        factory,
        IdentitySettings(
            database_url="postgresql+asyncpg://unused",
            secret_pepper="identity-pepper",
        ),
        control,
    )
    crm = create_crm_app(
        factory,
        CrmSettings(database_url="postgresql+asyncpg://unused"),
        control,  # type: ignore[arg-type]
        cast(JwtVerifier, object()),
    )
    return {"identity": identity.openapi(), "crm": crm.openapi()}


def business_operations(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        operation
        for path, item in document["paths"].items()
        if path.startswith("/v1/")
        for method, operation in item.items()
        if method in HTTP_METHODS
    ]


def parameter(operation: dict[str, Any], name: str) -> dict[str, Any]:
    return next(
        item
        for item in operation.get("parameters", [])
        if item["in"] == "header" and item["name"] == name
    )


def test_every_business_operation_declares_strict_auth_and_request_metadata() -> None:
    for document in service_openapi_documents().values():
        assert document["components"]["securitySchemes"]["BearerAuth"] == {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
        for operation in business_operations(document):
            assert operation["security"] == [{"BearerAuth": []}]
            correlation = parameter(operation, "X-Correlation-Id")
            assert correlation["required"] is True
            assert correlation["schema"]["minLength"] == 1
            assert correlation["schema"]["maxLength"] == 128
            assert parameter(operation, "traceparent")["required"] is False
            assert all(
                item["name"].lower() != "authorization" for item in operation.get("parameters", [])
            )


def test_every_business_operation_uses_common_error_and_response_metadata_contracts() -> None:
    for document in service_openapi_documents().values():
        serialised = str(business_operations(document))
        assert "HTTPValidationError" not in serialised
        for operation in business_operations(document):
            for status in ("400", "401", "422"):
                assert operation["responses"][status]["content"]["application/json"]["schema"] == {
                    "$ref": "#/components/schemas/ErrorEnvelope"
                }
            for status, response in operation["responses"].items():
                assert COMMON_RESPONSE_HEADERS <= set(response["headers"])
                if int(status) >= 400:
                    assert response["content"]["application/json"]["schema"] == {
                        "$ref": "#/components/schemas/ErrorEnvelope"
                    }


def test_crm_note_and_customer_success_contracts_are_truthful() -> None:
    document = service_openapi_documents()["crm"]
    note = document["paths"]["/v1/customers/{customer_id}/notes"]["post"]
    assert "200" not in note["responses"]
    assert note["responses"]["201"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/NoteView"
    }
    assert {
        "ETag",
        "X-Customer-Version",
        "Idempotency-Replayed",
    } <= set(note["responses"]["201"]["headers"])

    customer = document["paths"]["/v1/customers/{customer_id}"]["get"]
    assert {"ETag", "X-Resource-Version"} <= set(customer["responses"]["200"]["headers"])


def test_unprotected_identity_operations_do_not_publish_bearer_security() -> None:
    document = service_openapi_documents()["identity"]
    for path, method in (
        ("/.well-known/openid-configuration", "get"),
        ("/.well-known/jwks.json", "get"),
        ("/oauth/token", "post"),
        ("/health/live", "get"),
        ("/health/ready", "get"),
    ):
        assert not document["paths"][path][method].get("security")
