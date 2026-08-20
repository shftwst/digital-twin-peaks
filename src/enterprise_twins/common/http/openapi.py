from collections.abc import Callable
from typing import Any, cast

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from enterprise_twins.common.http.errors import ErrorEnvelope

HTTP_METHODS = {"get", "post", "put", "patch", "delete"}
COMMON_ERROR_RESPONSES = {
    "400": "Invalid request",
    "401": "Unauthenticated",
    "403": "Forbidden",
    "404": "Not found",
    "409": "Conflict",
    "412": "Precondition failed",
    "422": "Invalid request",
    "429": "Rate limited",
    "500": "Internal error",
    "503": "Temporarily unavailable",
}


def request_header(name: str, *, required: bool, max_length: int | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "string"}
    if required:
        schema["minLength"] = 1
    if max_length is not None:
        schema["maxLength"] = max_length
    return {
        "name": name,
        "in": "header",
        "required": required,
        "schema": schema,
    }


def response_headers() -> dict[str, Any]:
    return {
        "X-Request-Id": {
            "description": "Unique request identifier",
            "schema": {"type": "string"},
        },
        "X-Scenario-Epoch": {
            "description": "Scenario epoch bound to the response",
            "schema": {"type": "string"},
        },
        "traceparent": {
            "description": "Echoed trace context when supplied",
            "schema": {"type": "string"},
        },
    }


def add_error_schema(document: dict[str, Any]) -> None:
    components = document.setdefault("components", {}).setdefault("schemas", {})
    envelope = ErrorEnvelope.model_json_schema(ref_template="#/components/schemas/{model}")
    definitions = envelope.pop("$defs", {})
    components.update(definitions)
    components["ErrorEnvelope"] = envelope


def business_openapi(app: FastAPI) -> Callable[[], dict[str, Any]]:
    def build() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        document = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            routes=app.routes,
        )
        add_error_schema(document)
        paths = cast(dict[str, dict[str, Any]], document["paths"])
        for path, item in paths.items():
            if not path.startswith("/v1/"):
                continue
            for method, operation in item.items():
                if method not in HTTP_METHODS:
                    continue
                parameters = operation.setdefault("parameters", [])
                parameters.extend(
                    [
                        request_header("X-Correlation-Id", required=True, max_length=128),
                        request_header("traceparent", required=False),
                    ]
                )
                responses = operation.setdefault("responses", {})
                for status, description in COMMON_ERROR_RESPONSES.items():
                    response = responses.setdefault(status, {"description": description})
                    response["content"] = {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
                        }
                    }
                for response in responses.values():
                    response.setdefault("headers", {}).update(response_headers())
        schemas = document.get("components", {}).get("schemas", {})
        schemas.pop("HTTPValidationError", None)
        schemas.pop("ValidationError", None)
        app.openapi_schema = document
        return document

    return build
