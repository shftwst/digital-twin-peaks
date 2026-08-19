import argparse
import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import shutil
import subprocess
from collections import Counter
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import httpx

from enterprise_twins.common.canonical import canonical_json

JsonObject = dict[str, Any]
Transcript = list[dict[str, object]]

INITIAL_TIME = "2026-08-19T10:00:00Z"
SCENARIO_ID = "platform-contracts"
SCENARIO_VERSION = 1
RANDOM_SEED = 7
RECEIVER_TARGET = "http://webhook-receiver:8080/events"
FORBIDDEN_ARTEFACT_KEYS = {
    "authorization",
    "access_token",
    "client_secret",
    "secret",
    "x-twin-signature",
}
FORBIDDEN_NORMALISED_ARTEFACT_KEYS = {
    "authorization",
    "token",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "bearertoken",
    "apitoken",
    "authtoken",
    "sessiontoken",
    "jwt",
    "clientsecret",
    "secret",
    "xtwinsignature",
}
FORBIDDEN_ARTEFACT_VALUES = {
    "support-secret",
    "evaluator-secret",
    "webhook-secret",
    "controller-local-token",
    "receiver-conformance-local-token",
}
REQUIRED_SUCCESS_OPERATIONS = {
    "control.reset",
    "identity.token.issue",
    "identity.subscription.create",
    "control.time.advance.identity-retry",
    "relay.identity.retry",
    "crm.subscription.create",
    "receiver.signature.cross-subscription-reject",
    "receiver.signature.zero-reject",
    "identity.me",
    "crm.customer.search",
    "crm.customer.get",
    "crm.note.create",
    "crm.note.replay",
}
REQUIRED_FAILURE_OPERATIONS = {
    "control.reset",
    "control.status.initial",
    "identity.token.issue",
    "auth.missing",
    "auth.read-only-read",
    "auth.read-only-write",
    "crm.notes.after-read-only-denial",
    "crm.search.ambiguous",
    "crm.note.precondition.stale",
    "crm.note.create.idempotency-baseline",
    "crm.note.idempotency-mismatch",
    "crm.notes.after-idempotency-denial",
    "webhook.target.denied",
    "control.fault.create",
    "crm.customer.before-timeout",
    "crm.note.after-commit-timeout",
    "crm.note.timeout.read",
    "crm.note.timeout.replay",
    "control.fault-activations.before-reset",
    "identity.subscription.create",
    "crm.subscription.create",
    "identity.subscriptions.before-reset",
    "crm.subscriptions.before-reset",
    "control.time.advance.pre-reset",
    "control.status.after-reset",
    "identity.old-token.after-reset",
    "identity.subscriptions.after-reset",
    "crm.subscriptions.after-reset",
    "crm.notes.after-reset",
    "control.fault-activations.after-reset",
    "crm.note.idempotency-reuse.after-reset",
}
JWT_SHAPED_VALUE = re.compile(
    r"(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{8,})\.([A-Za-z0-9_-]{8,})\."
    r"([A-Za-z0-9_-]{8,})(?![A-Za-z0-9_-])"
)
INFER_RESPONSE_STATUS = object()


@dataclass(frozen=True)
class OperationContract:
    count: int
    method: str
    path: str
    request_fields: JsonObject
    expected: JsonObject
    actual: JsonObject
    response_status: int | None
    transport_error: str | None
    required_request_keys: tuple[str, ...] = ()


def operation_contract(
    count: int,
    method: str,
    path: str,
    *,
    request_fields: JsonObject | None = None,
    expected: JsonObject | None = None,
    actual: JsonObject | None = None,
    response_status: int | object | None = INFER_RESPONSE_STATUS,
    transport_error: str | None = None,
    required_request_keys: tuple[str, ...] = (),
) -> OperationContract:
    expected_fragment = expected or {}
    actual_fragment = actual or {}
    resolved_response_status: int | None
    if response_status is INFER_RESPONSE_STATUS:
        resolved_response_status = cast(
            int,
            actual_fragment.get("status", expected_fragment.get("status", 200)),
        )
    elif response_status is None or isinstance(response_status, int):
        resolved_response_status = response_status
    else:
        raise TypeError("operation contract response status is invalid")
    return OperationContract(
        count=count,
        method=method,
        path=path,
        request_fields=request_fields or {},
        expected=expected_fragment,
        actual=actual_fragment,
        response_status=resolved_response_status,
        transport_error=transport_error,
        required_request_keys=required_request_keys,
    )


SUCCESS_OPERATION_CONTRACTS = {
    "control.reset": operation_contract(
        1,
        "POST",
        "/control/v1/reset",
        request_fields={
            "randomSeed": RANDOM_SEED,
            "scenarioId": SCENARIO_ID,
            "version": SCENARIO_VERSION,
        },
        expected={"status": 200, "randomSeed": RANDOM_SEED},
        actual={"status": 200, "randomSeed": RANDOM_SEED},
    ),
    "identity.token.issue": operation_contract(
        2,
        "POST",
        "/oauth/token",
        expected={"status": 200, "tokenIssued": True},
        actual={"status": 200, "tokenIssued": True},
    ),
    "identity.subscription.create": operation_contract(
        1,
        "POST",
        "/v1/webhook-subscriptions",
        request_fields={
            "eventTypes": ["identity.token.issued"],
            "targetHost": "webhook-receiver",
        },
        expected={"status": 201, "source": "identity"},
        actual={"status": 201, "source": "identity"},
    ),
    "control.time.advance.identity-retry": operation_contract(
        1,
        "POST",
        "/control/v1/time/advance",
        request_fields={"duration": "PT2S"},
        expected={"now": "2026-08-19T10:00:02Z"},
        actual={"now": "2026-08-19T10:00:02Z"},
    ),
    "relay.identity.retry": operation_contract(
        1,
        "POST",
        "/events",
        request_fields={"virtualAdvance": "PT2S"},
        expected={"sameEventId": True, "sameBodyHash": True},
        actual={"sameEventId": True, "sameBodyHash": True},
        response_status=204,
        required_request_keys=("eventId", "bodyHash"),
    ),
    "crm.subscription.create": operation_contract(
        1,
        "POST",
        "/v1/webhook-subscriptions",
        request_fields={
            "eventTypes": ["crm.note.created"],
            "targetHost": "webhook-receiver",
        },
        expected={"status": 201, "source": "crm"},
        actual={"status": 201, "source": "crm"},
    ),
    "receiver.signature.cross-subscription-reject": operation_contract(
        1,
        "POST",
        "/events",
        request_fields={
            "eventId": "evt_cross_subscription_signature",
            "envelopeSource": "crm",
            "eventType": "crm.note.created",
            "signingSecretSource": "identity",
        },
        expected={
            "status": 401,
            "error": {"code": "unauthenticated"},
            "accepted": False,
        },
        actual={
            "status": 401,
            "error": {"code": "unauthenticated"},
            "accepted": False,
        },
        required_request_keys=("bodyHash",),
    ),
    "receiver.signature.zero-reject": operation_contract(
        1,
        "POST",
        "/events",
        request_fields={"source": "crm", "eventType": "crm.note.created"},
        expected={"status": 401, "accepted": False},
        actual={"status": 401, "accepted": False},
        required_request_keys=("bodyHash",),
    ),
    "identity.me": operation_contract(
        1,
        "GET",
        "/v1/me",
        expected={"subject": "person-support-1"},
        actual={"subject": "person-support-1"},
    ),
    "crm.customer.search": operation_contract(
        1,
        "GET",
        "/v1/customers",
        expected={"customerIds": ["cus_unique"]},
        actual={"customerIds": ["cus_unique"]},
        required_request_keys=("emailHash",),
    ),
    "crm.customer.get": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique",
        expected={"customerId": "cus_unique", "version": 1},
        actual={"customerId": "cus_unique", "version": 1},
    ),
    "crm.note.create": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"association": "account", "expectedVersion": 1},
        expected={"status": 201, "replayed": False},
        actual={"status": 201, "replayed": False},
        required_request_keys=("bodyHash",),
    ),
    "crm.note.replay": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"sameIdempotencyKey": True, "sameBodyHash": True},
        expected={"sameNoteId": True, "replayed": True},
        actual={"sameNoteId": True, "replayed": True},
        response_status=201,
    ),
}


FAILURE_OPERATION_CONTRACTS = {
    "control.reset": operation_contract(
        2,
        "POST",
        "/control/v1/reset",
        request_fields={
            "randomSeed": RANDOM_SEED,
            "scenarioId": SCENARIO_ID,
            "version": SCENARIO_VERSION,
        },
        expected={"status": 200, "randomSeed": RANDOM_SEED},
        actual={"status": 200, "randomSeed": RANDOM_SEED},
    ),
    "control.status.initial": operation_contract(
        1,
        "GET",
        "/control/v1/status",
        expected={"status": 200, "now": INITIAL_TIME, "randomSeed": RANDOM_SEED},
        actual={"status": 200, "now": INITIAL_TIME, "randomSeed": RANDOM_SEED},
    ),
    "identity.token.issue": operation_contract(
        5,
        "POST",
        "/oauth/token",
        expected={"status": 200, "tokenIssued": True},
        actual={"status": 200, "tokenIssued": True},
    ),
    "auth.missing": operation_contract(
        1,
        "GET",
        "/v1/customers",
        expected={"status": 401, "error": {"code": "unauthenticated"}},
        actual={"status": 401, "error": {"code": "unauthenticated"}},
    ),
    "auth.read-only-read": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique",
        request_fields={"actorClass": "read-only"},
        expected={"status": 200, "customerId": "cus_unique"},
        actual={"status": 200, "customerId": "cus_unique"},
    ),
    "auth.read-only-write": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"actorClass": "read-only", "expectedVersion": 1},
        expected={"status": 403, "error": {"code": "forbidden"}},
        actual={"status": 403, "error": {"code": "forbidden"}},
    ),
    "crm.notes.after-read-only-denial": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique/notes",
        expected={"noteCount": 0, "rejectedBodyPresent": False},
        actual={"noteCount": 0, "rejectedBodyPresent": False},
        required_request_keys=("rejectedBodyHash",),
    ),
    "crm.search.ambiguous": operation_contract(
        1,
        "GET",
        "/v1/customers",
        expected={"customerIds": ["cus_ambiguous_a", "cus_ambiguous_b"]},
        actual={"customerIds": ["cus_ambiguous_a", "cus_ambiguous_b"]},
        required_request_keys=("emailHash",),
    ),
    "crm.note.precondition.stale": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 0},
        expected={"status": 409},
        actual={"status": 409, "error": {"code": "conflict"}},
    ),
    "crm.note.create.idempotency-baseline": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 1},
        expected={"status": 201, "replayed": False},
        actual={"status": 201, "replayed": False},
        required_request_keys=("bodyHash",),
    ),
    "crm.note.idempotency-mismatch": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"keyLabel": "reused-after-reset"},
        expected={"status": 409},
        actual={"status": 409, "error": {"code": "conflict"}},
        required_request_keys=("bodyHash",),
    ),
    "crm.notes.after-idempotency-denial": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique/notes",
        expected={
            "noteCount": 1,
            "validNoteCount": 1,
            "validNoteIdMatches": True,
            "forbiddenBodyPresent": False,
            "changedBodyPresent": False,
        },
        actual={
            "noteCount": 1,
            "validNoteCount": 1,
            "validNoteIdMatches": True,
            "forbiddenBodyPresent": False,
            "changedBodyPresent": False,
        },
        required_request_keys=("validNoteId", "forbiddenBodyHash", "changedBodyHash"),
    ),
    "webhook.target.denied": operation_contract(
        1,
        "POST",
        "/v1/webhook-subscriptions",
        request_fields={"targetHost": "127.0.0.1"},
        expected={"status": 422},
        actual={"status": 422, "error": {"code": "invalid_request"}},
    ),
    "control.fault.create": operation_contract(
        1,
        "POST",
        "/control/v1/faults",
        request_fields={
            "ruleId": "crm-note-timeout-once",
            "targetService": "crm",
            "phase": "after_commit",
            "delayMs": 500,
        },
        expected={"status": 201, "ruleId": "crm-note-timeout-once"},
        actual={"status": 201, "ruleId": "crm-note-timeout-once"},
    ),
    "crm.customer.before-timeout": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique",
        expected={"status": 200, "customerId": "cus_unique"},
        actual={"status": 200, "customerId": "cus_unique"},
    ),
    "crm.note.after-commit-timeout": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"clientReadTimeoutMs": 100, "serverDelayMs": 500},
        expected={"error": "ReadTimeout"},
        actual={"error": "ReadTimeout"},
        response_status=None,
        transport_error="ReadTimeout",
        required_request_keys=("bodyHash",),
    ),
    "crm.note.timeout.read": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique/notes",
        expected={"status": 200, "noteCount": 2, "timeoutNoteCount": 1},
        actual={"status": 200, "noteCount": 2, "timeoutNoteCount": 1},
        required_request_keys=("bodyHash",),
    ),
    "crm.note.timeout.replay": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"sameIdempotencyKey": True},
        expected={"status": 201, "sameNoteId": True, "replayed": True},
        actual={"status": 201, "sameNoteId": True, "replayed": True},
    ),
    "control.fault-activations.before-reset": operation_contract(
        1,
        "GET",
        "/control/v1/fault-activations",
        expected={"status": 200, "activationCount": 1},
        actual={"status": 200, "activationCount": 1},
    ),
    "identity.subscription.create": operation_contract(
        1,
        "POST",
        "/v1/webhook-subscriptions",
        request_fields={
            "eventTypes": ["identity.token.issued"],
            "targetHost": "webhook-receiver",
        },
        expected={"status": 201, "source": "identity"},
        actual={"status": 201, "source": "identity"},
    ),
    "crm.subscription.create": operation_contract(
        1,
        "POST",
        "/v1/webhook-subscriptions",
        request_fields={
            "eventTypes": ["crm.note.created"],
            "targetHost": "webhook-receiver",
        },
        expected={"status": 201, "source": "crm"},
        actual={"status": 201, "source": "crm"},
    ),
    "identity.subscriptions.before-reset": operation_contract(
        1,
        "GET",
        "/v1/webhook-subscriptions",
        request_fields={"source": "identity"},
        expected={"status": 200, "subscriptionCount": 1},
        actual={"status": 200, "subscriptionCount": 1},
    ),
    "crm.subscriptions.before-reset": operation_contract(
        1,
        "GET",
        "/v1/webhook-subscriptions",
        request_fields={"source": "crm"},
        expected={"status": 200, "subscriptionCount": 1},
        actual={"status": 200, "subscriptionCount": 1},
    ),
    "control.time.advance.pre-reset": operation_contract(
        1,
        "POST",
        "/control/v1/time/advance",
        request_fields={"duration": "PT5M"},
        expected={"status": 200, "now": "2026-08-19T10:05:00Z"},
        actual={"status": 200, "now": "2026-08-19T10:05:00Z"},
    ),
    "control.status.after-reset": operation_contract(
        1,
        "GET",
        "/control/v1/status",
        expected={"status": 200, "now": INITIAL_TIME, "randomSeed": RANDOM_SEED},
        actual={"status": 200, "now": INITIAL_TIME, "randomSeed": RANDOM_SEED},
    ),
    "identity.old-token.after-reset": operation_contract(
        1,
        "GET",
        "/v1/me",
        request_fields={"tokenEpoch": "before-reset"},
        expected={"status": 401, "error": {"code": "unauthenticated"}},
        actual={"status": 401, "error": {"code": "unauthenticated"}},
    ),
    "identity.subscriptions.after-reset": operation_contract(
        1,
        "GET",
        "/v1/webhook-subscriptions",
        request_fields={"source": "identity"},
        expected={"status": 200, "subscriptionCount": 0},
        actual={"status": 200, "subscriptionCount": 0},
    ),
    "crm.subscriptions.after-reset": operation_contract(
        1,
        "GET",
        "/v1/webhook-subscriptions",
        request_fields={"source": "crm"},
        expected={"status": 200, "subscriptionCount": 0},
        actual={"status": 200, "subscriptionCount": 0},
    ),
    "crm.notes.after-reset": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique/notes",
        expected={"status": 200, "noteCount": 0},
        actual={"status": 200, "noteCount": 0},
    ),
    "control.fault-activations.after-reset": operation_contract(
        1,
        "GET",
        "/control/v1/fault-activations",
        expected={"status": 200, "activationCount": 0},
        actual={"status": 200, "activationCount": 0},
    ),
    "crm.note.idempotency-reuse.after-reset": operation_contract(
        1,
        "POST",
        "/v1/customers/cus_unique/notes",
        request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 1},
        expected={"status": 201, "replayed": False},
        actual={"status": 201, "replayed": False},
    ),
}


RESTART_OPERATION_CONTRACTS = {
    "identity.token.issue": operation_contract(
        1,
        "POST",
        "/oauth/token",
        expected={"status": 200, "tokenIssued": True},
        actual={"status": 200, "tokenIssued": True},
    ),
    "crm.notes.after-restart": operation_contract(
        1,
        "GET",
        "/v1/customers/cus_unique/notes",
        expected={"status": 200, "savedNotePresent": True},
        actual={"status": 200, "savedNotePresent": True},
        required_request_keys=("expectedNoteId",),
    ),
}

SUCCESS_OPERATION_SEQUENCE = (
    "control.reset",
    "identity.token.issue",
    "identity.subscription.create",
    "identity.token.issue",
    "control.time.advance.identity-retry",
    "relay.identity.retry",
    "crm.subscription.create",
    "receiver.signature.cross-subscription-reject",
    "receiver.signature.zero-reject",
    "identity.me",
    "crm.customer.search",
    "crm.customer.get",
    "crm.note.create",
    "crm.note.replay",
)

FAILURE_OPERATION_SEQUENCE = (
    "control.reset",
    "control.status.initial",
    "identity.token.issue",
    "identity.token.issue",
    "auth.missing",
    "identity.token.issue",
    "auth.read-only-read",
    "auth.read-only-write",
    "crm.notes.after-read-only-denial",
    "crm.search.ambiguous",
    "crm.note.precondition.stale",
    "crm.note.create.idempotency-baseline",
    "crm.note.idempotency-mismatch",
    "crm.notes.after-idempotency-denial",
    "webhook.target.denied",
    "control.fault.create",
    "crm.customer.before-timeout",
    "crm.note.after-commit-timeout",
    "crm.note.timeout.read",
    "crm.note.timeout.replay",
    "control.fault-activations.before-reset",
    "identity.subscription.create",
    "crm.subscription.create",
    "identity.subscriptions.before-reset",
    "crm.subscriptions.before-reset",
    "control.time.advance.pre-reset",
    "control.reset",
    "control.status.after-reset",
    "identity.old-token.after-reset",
    "identity.token.issue",
    "identity.subscriptions.after-reset",
    "crm.subscriptions.after-reset",
    "identity.token.issue",
    "crm.notes.after-reset",
    "control.fault-activations.after-reset",
    "crm.note.idempotency-reuse.after-reset",
)

RESTART_OPERATION_SEQUENCE = (
    "identity.token.issue",
    "crm.notes.after-restart",
)


def object_body(response: httpx.Response) -> JsonObject:
    value = response.json()
    if not isinstance(value, dict):
        raise AssertionError("expected a JSON object response")
    return cast(JsonObject, value)


def contains_jwt_shaped_value(value: str) -> bool:
    for candidate in JWT_SHAPED_VALUE.finditer(value):
        encoded_header = candidate.group(1)
        padding = "=" * (-len(encoded_header) % 4)
        try:
            header = json.loads(base64.urlsafe_b64decode(encoded_header + padding))
        except binascii.Error, json.JSONDecodeError, UnicodeDecodeError, ValueError:
            continue
        if isinstance(header, dict) and ("alg" in header or "typ" in header):
            return True
    return False


def list_body(response: httpx.Response) -> list[JsonObject]:
    value = response.json()
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise AssertionError("expected a JSON object list response")
    return cast(list[JsonObject], value)


def require_status(response: httpx.Response, expected: int) -> None:
    if response.status_code != expected:
        raise AssertionError(
            f"{response.request.method} {response.request.url.path} returned "
            f"{response.status_code}, expected {expected}"
        )


def safe_error(response: httpx.Response) -> JsonObject:
    body = object_body(response)
    error = body.get("error")
    if not isinstance(error, dict):
        return {"error": "structured error missing"}
    return {
        "code": error.get("code"),
        "retryable": error.get("retryable"),
        "details": error.get("details", {}),
    }


def check_no_sensitive_values(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalised_key = "".join(
                character for character in key.casefold() if character.isalnum()
            )
            if (
                key.casefold() in FORBIDDEN_ARTEFACT_KEYS
                or normalised_key in FORBIDDEN_NORMALISED_ARTEFACT_KEYS
            ):
                raise AssertionError(f"artefact contains forbidden key: {key}")
            check_no_sensitive_values(item)
    elif isinstance(value, list):
        for item in value:
            check_no_sensitive_values(item)
    elif isinstance(value, str):
        if (
            "bearer " in value.casefold()
            or contains_jwt_shaped_value(value)
            or any(secret in value for secret in FORBIDDEN_ARTEFACT_VALUES)
        ):
            raise AssertionError("artefact contains a credential value")


def evidence_object(value: object, name: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{name} evidence is not an object")
    return cast(JsonObject, value)


def evidence_list(value: object, name: str) -> list[JsonObject]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise AssertionError(f"{name} evidence is not an object list")
    return cast(list[JsonObject], value)


def expected_matches(expected: object, actual: object) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and expected_matches(value, actual[key])
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(expected) == len(actual)
            and all(
                expected_matches(expected_item, actual_item)
                for expected_item, actual_item in zip(expected, actual, strict=True)
            )
        )
    return type(expected) is type(actual) and expected == actual


def validate_transcript_record(
    record: JsonObject,
    *,
    expected_sequence: int,
    name: str,
) -> None:
    if set(record) != {"sequence", "operation", "request", "response", "assertion"}:
        raise AssertionError(f"{name} transcript record has incomplete fields")
    if record["sequence"] != expected_sequence:
        raise AssertionError(f"{name} transcript sequence is not contiguous")
    if not isinstance(record["operation"], str) or not record["operation"]:
        raise AssertionError(f"{name} transcript operation is invalid")
    request = evidence_object(record["request"], f"{name} transcript request")
    if set(request) != {"method", "path", "fields"}:
        raise AssertionError(f"{name} transcript request has incomplete fields")
    if request["method"] not in {"DELETE", "GET", "PATCH", "POST", "PUT"}:
        raise AssertionError(f"{name} transcript HTTP method is invalid")
    if not isinstance(request["path"], str) or not request["path"].startswith("/"):
        raise AssertionError(f"{name} transcript path is invalid")
    if not isinstance(request["fields"], dict):
        raise AssertionError(f"{name} transcript request fields are invalid")
    response = evidence_object(record["response"], f"{name} transcript response")
    if set(response) != {"status", "body", "error"}:
        raise AssertionError(f"{name} transcript response has incomplete fields")
    assertion = evidence_object(record["assertion"], f"{name} transcript assertion")
    if set(assertion) != {"expected", "actual", "outcome"}:
        raise AssertionError(f"{name} transcript assertion has incomplete fields")
    if assertion["outcome"] != "passed":
        raise AssertionError(f"{name} transcript assertion did not pass")
    if not expected_matches(assertion["expected"], assertion["actual"]):
        raise AssertionError(f"{name} transcript expected result differs from actual")
    if response["error"] is None:
        if not isinstance(response["status"], int) or response["body"] != assertion["actual"]:
            raise AssertionError(f"{name} transcript HTTP response is inconsistent")
        if (
            isinstance(assertion["actual"], dict)
            and "status" in assertion["actual"]
            and assertion["actual"]["status"] != response["status"]
        ):
            raise AssertionError(f"{name} transcript HTTP status differs from actual")
    elif (
        response["status"] is not None
        or response["body"] is not None
        or not isinstance(response["error"], str)
        or not response["error"]
    ):
        raise AssertionError(f"{name} transcript transport error is inconsistent")
    elif (
        not isinstance(assertion["actual"], dict)
        or assertion["actual"].get("error") != response["error"]
    ):
        raise AssertionError(f"{name} transcript transport error differs from actual")


def validate_transcript(value: object, name: str) -> list[JsonObject]:
    calls = evidence_list(value, f"{name} transcript")
    if not calls:
        raise AssertionError(f"{name} transcript is empty")
    for sequence, record in enumerate(calls, start=1):
        validate_transcript_record(record, expected_sequence=sequence, name=name)
    return calls


def validate_operation_contracts(
    calls: list[JsonObject],
    contracts: dict[str, OperationContract],
    required_sequence: tuple[str, ...],
    name: str,
) -> None:
    counts = Counter(cast(str, call["operation"]) for call in calls)
    expected_counts = Counter(
        {operation: contract.count for operation, contract in contracts.items()}
    )
    if counts != expected_counts:
        differences = [
            f"{operation} expected {expected_counts[operation]}, observed {counts[operation]}"
            for operation in sorted(set(counts) | set(expected_counts))
            if counts[operation] != expected_counts[operation]
        ]
        raise AssertionError(
            f"{name} operation contract multiplicity differs: {', '.join(differences)}"
        )
    observed_sequence = tuple(cast(str, call["operation"]) for call in calls)
    if observed_sequence != required_sequence:
        raise AssertionError(f"{name} workflow order differs from the required sequence")
    for call in calls:
        operation = cast(str, call["operation"])
        contract = contracts[operation]
        request = evidence_object(call["request"], f"{operation} request")
        fields = evidence_object(request["fields"], f"{operation} request fields")
        response = evidence_object(call["response"], f"{operation} response")
        assertion = evidence_object(call["assertion"], f"{operation} assertion")
        missing_request_keys = set(contract.required_request_keys) - set(fields)
        hash_values_valid = all(
            not key.casefold().endswith("hash")
            or (
                isinstance(fields[key], str)
                and re.fullmatch(r"[0-9a-f]{64}", fields[key]) is not None
            )
            for key in contract.required_request_keys
            if key in fields
        )
        if (
            request["method"] != contract.method
            or request["path"] != contract.path
            or response["status"] != contract.response_status
            or response["error"] != contract.transport_error
            or missing_request_keys
            or not hash_values_valid
            or not expected_matches(contract.request_fields, fields)
            or not expected_matches(contract.expected, assertion["expected"])
            or not expected_matches(contract.actual, assertion["actual"])
        ):
            raise AssertionError(f"{operation} operation contract differs from evidence")


def operation_actuals(calls: list[JsonObject], operation: str) -> list[JsonObject]:
    return [
        evidence_object(call["assertion"], f"{operation} assertion")["actual"]
        for call in calls
        if call["operation"] == operation
    ]


def validate_reset_transcript_binding(
    calls: list[JsonObject],
    before: JsonObject,
    after: JsonObject,
    virtual_time: JsonObject,
) -> None:
    resets = [
        evidence_object(value, "control.reset actual")
        for value in operation_actuals(calls, "control.reset")
    ]
    initial_statuses = [
        evidence_object(value, "initial Control status actual")
        for value in operation_actuals(calls, "control.status.initial")
    ]
    after_statuses = [
        evidence_object(value, "post-reset Control status actual")
        for value in operation_actuals(calls, "control.status.after-reset")
    ]
    if len(resets) != 2 or len(initial_statuses) != 1 or len(after_statuses) != 1:
        raise AssertionError("reset transcript does not contain the required Control records")
    first_reset, second_reset = resets
    initial_status = initial_statuses[0]
    after_status = after_statuses[0]
    safe_fields = ("scenarioEpoch", "manifestChecksum", "randomSeed")
    if (
        any(first_reset.get(field) != before.get(field) for field in safe_fields)
        or any(second_reset.get(field) != after.get(field) for field in safe_fields)
        or any(initial_status.get(field) != before.get(field) for field in safe_fields)
        or any(after_status.get(field) != after.get(field) for field in safe_fields)
        or initial_status.get("now") != virtual_time.get("initial")
        or after_status.get("now") != virtual_time.get("afterReset")
        or first_reset.get("scenarioEpoch") == second_reset.get("scenarioEpoch")
        or first_reset.get("manifestChecksum") != second_reset.get("manifestChecksum")
        or first_reset.get("randomSeed") != second_reset.get("randomSeed")
    ):
        raise AssertionError("reset transcript and reset evidence disagree")


def run_command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        arguments,
        check=True,
        capture_output=True,
        text=True,
    )


class Driver:
    def __init__(self) -> None:
        self.identity = os.environ["IDENTITY_URL"].rstrip("/")
        self.crm = os.environ["CRM_URL"].rstrip("/")
        self.control = os.environ["CONTROL_URL"].rstrip("/")
        self.receiver = os.environ["RECEIVER_URL"].rstrip("/")
        self.run_id = os.environ["CONFORMANCE_RUN_ID"]
        self.artifacts = Path(os.environ["ARTIFACT_ROOT"])
        if self.artifacts.exists():
            if not self.artifacts.is_dir() or next(self.artifacts.iterdir(), None) is not None:
                raise AssertionError("new conformance run directory is not empty")
        else:
            self.artifacts.mkdir(parents=True)
        self.control_headers = {
            "Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}",
        }
        self.receiver_headers = {
            "Authorization": f"Bearer {os.environ['RECEIVER_TOKEN']}",
        }
        self.client = httpx.AsyncClient(timeout=10.0, trust_env=False)
        self.success_calls: Transcript = []
        self.failure_calls: Transcript = []

    @classmethod
    def resume(cls) -> Driver:
        self = cls.__new__(cls)
        self.identity = os.environ["IDENTITY_URL"].rstrip("/")
        self.crm = os.environ["CRM_URL"].rstrip("/")
        self.control = os.environ["CONTROL_URL"].rstrip("/")
        self.receiver = os.environ["RECEIVER_URL"].rstrip("/")
        self.run_id = os.environ["CONFORMANCE_RUN_ID"]
        self.artifacts = Path(os.environ["ARTIFACT_ROOT"])
        if not self.artifacts.is_dir():
            raise AssertionError("conformance run directory does not exist")
        self.control_headers = {
            "Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}",
        }
        self.receiver_headers = {
            "Authorization": f"Bearer {os.environ['RECEIVER_TOKEN']}",
        }
        self.client = httpx.AsyncClient(timeout=10.0, trust_env=False)
        self.success_calls = []
        self.failure_calls = []
        return self

    async def close(self) -> None:
        await self.client.aclose()

    def write(self, name: str, value: JsonObject) -> None:
        payload = {"runId": self.run_id} | value
        check_no_sensitive_values(payload)
        path = self.artifacts / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def read(self, name: str) -> JsonObject:
        value = json.loads((self.artifacts / name).read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("runId") != self.run_id:
            raise AssertionError(f"{name} is not bound to this run")
        return cast(JsonObject, value)

    def record(
        self,
        target: Transcript,
        *,
        operation: str,
        method: str,
        path: str,
        request_fields: JsonObject,
        expected: object,
        actual: object,
        response_status: int | None = None,
        error: str | None = None,
    ) -> None:
        if method not in {"DELETE", "GET", "PATCH", "POST", "PUT"}:
            raise AssertionError(f"unsupported transcript HTTP method: {method}")
        if not expected_matches(expected, actual):
            raise AssertionError(f"{operation} expected result differs from actual")
        record = {
            "sequence": len(target) + 1,
            "operation": operation,
            "request": {
                "method": method,
                "path": path,
                "fields": request_fields,
            },
            "response": {
                "status": response_status,
                "body": actual if error is None else None,
                "error": error,
            },
            "assertion": {
                "expected": expected,
                "actual": actual,
                "outcome": "passed",
            },
        }
        validate_transcript_record(
            record,
            expected_sequence=len(target) + 1,
            name=operation,
        )
        target.append(record)

    async def reset(self, target: Transcript) -> JsonObject:
        response = await self.client.post(
            f"{self.control}/control/v1/reset",
            headers=self.control_headers,
            json={
                "scenarioId": SCENARIO_ID,
                "version": SCENARIO_VERSION,
                "randomSeed": RANDOM_SEED,
            },
            timeout=httpx.Timeout(65.0),
        )
        require_status(response, 200)
        body = object_body(response)
        self.record(
            target,
            operation="control.reset",
            method="POST",
            path="/control/v1/reset",
            request_fields={
                "scenarioId": SCENARIO_ID,
                "version": SCENARIO_VERSION,
                "randomSeed": RANDOM_SEED,
            },
            expected={"status": 200, "randomSeed": RANDOM_SEED},
            actual={
                "status": response.status_code,
                "randomSeed": body["randomSeed"],
                "scenarioEpoch": body["scenarioEpoch"],
                "manifestChecksum": body["manifestChecksum"],
            },
            response_status=response.status_code,
        )
        return body

    async def token(
        self,
        client_id: str,
        secret: str,
        scope: str,
        correlation_id: str,
        target: Transcript,
    ) -> str:
        response = await self.client.post(
            f"{self.identity}/oauth/token",
            headers={"X-Correlation-Id": correlation_id},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": secret,
                "scope": scope,
            },
        )
        require_status(response, 200)
        body = object_body(response)
        access_token = body.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise AssertionError("Identity response omitted its access token")
        self.record(
            target,
            operation="identity.token.issue",
            method="POST",
            path="/oauth/token",
            request_fields={"scope": scope},
            expected={"status": 200, "tokenIssued": True},
            actual={
                "status": response.status_code,
                "tokenIssued": True,
                "scope": body.get("scope"),
            },
            response_status=response.status_code,
        )
        return access_token

    @staticmethod
    def business_headers(token: str, correlation_id: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "X-Correlation-Id": correlation_id,
        }

    async def receiver_reset(self) -> None:
        response = await self.client.post(
            f"{self.receiver}/internal/v1/reset",
            headers=self.receiver_headers,
        )
        require_status(response, 204)

    async def receiver_list(self, resource: str) -> list[JsonObject]:
        response = await self.client.get(
            f"{self.receiver}/internal/v1/{resource}",
            headers=self.receiver_headers,
        )
        require_status(response, 200)
        return list_body(response)

    async def arm_receiver(self, source: str, event_type: str, secret: str) -> None:
        response = await self.client.post(
            f"{self.receiver}/internal/v1/secrets",
            headers=self.receiver_headers,
            json={"source": source, "eventType": event_type, "secret": secret},
        )
        require_status(response, 204)

    async def wait_for(
        self,
        resource: str,
        predicate: Callable[[list[JsonObject]], bool],
        description: str,
    ) -> list[JsonObject]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10.0
        while True:
            values = await self.receiver_list(resource)
            if predicate(values):
                return values
            if loop.time() >= deadline:
                raise AssertionError(f"timed out waiting for {description}")
            await asyncio.sleep(0.05)

    async def create_subscription(
        self,
        base_url: str,
        token: str,
        source: str,
        event_type: str,
        key: str,
        target: Transcript,
    ) -> str:
        response = await self.client.post(
            f"{base_url}/v1/webhook-subscriptions",
            headers=self.business_headers(token, f"case-{source}-subscription")
            | {"Idempotency-Key": key},
            json={"eventTypes": [event_type], "targetUrl": RECEIVER_TARGET},
        )
        require_status(response, 201)
        body = object_body(response)
        secret = body.get("secret")
        if not isinstance(secret, str) or not secret:
            raise AssertionError("subscription response omitted its one-time secret")
        self.record(
            target,
            operation=f"{source}.subscription.create",
            method="POST",
            path="/v1/webhook-subscriptions",
            request_fields={"eventTypes": [event_type], "targetHost": "webhook-receiver"},
            expected={"status": 201, "source": source},
            actual={
                "status": response.status_code,
                "source": body.get("source"),
                "eventTypes": body.get("eventTypes"),
                "subscriptionId": body.get("subscriptionId"),
            },
            response_status=response.status_code,
        )
        return secret

    async def advance_two_seconds(self) -> None:
        response = await self.client.post(
            f"{self.control}/control/v1/time/advance",
            headers=self.control_headers,
            json={"duration": "PT2S"},
        )
        require_status(response, 200)
        body = object_body(response)
        if not str(body["now"]).endswith("10:00:02Z"):
            raise AssertionError("virtual clock did not advance by exactly PT2S")
        self.record(
            self.success_calls,
            operation="control.time.advance.identity-retry",
            method="POST",
            path="/control/v1/time/advance",
            request_fields={"duration": "PT2S"},
            expected={"now": "2026-08-19T10:00:02Z"},
            actual={"now": body["now"]},
            response_status=response.status_code,
        )

    async def deliberate_bad_signature(self) -> None:
        body = canonical_json(
            {
                "eventId": "evt_deliberate_bad_signature",
                "eventType": "crm.note.created",
                "schemaVersion": "1.0",
                "source": "crm",
                "subject": "note/deliberate-bad-signature",
                "resourceVersion": 1,
                "correlationId": "case-bad-signature",
                "causationId": "req_bad_signature",
                "occurredAt": "2026-08-19T10:00:02Z",
                "recordedAt": "2026-08-19T10:00:02Z",
                "data": {"proof": "deliberate-negative"},
            }
        )
        response = await self.client.post(
            f"{self.receiver}/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Twin-Event-Id": "evt_deliberate_bad_signature",
                "X-Twin-Timestamp": "2026-08-19T10:00:02Z",
                "X-Twin-Signature": f"v1={'0' * 64}",
            },
        )
        require_status(response, 401)
        error = safe_error(response)
        if error["code"] != "unauthenticated":
            raise AssertionError("wrong webhook signature did not return unauthenticated")
        accepted = await self.receiver_list("events")
        if any(item["eventId"] == "evt_deliberate_bad_signature" for item in accepted):
            raise AssertionError("wrong-signature webhook entered the accepted-event list")
        self.record(
            self.success_calls,
            operation="receiver.signature.zero-reject",
            method="POST",
            path="/events",
            request_fields={
                "source": "crm",
                "eventType": "crm.note.created",
                "bodyHash": hashlib.sha256(body).hexdigest(),
            },
            expected={"status": 401, "accepted": False},
            actual={"status": response.status_code, "error": error, "accepted": False},
            response_status=response.status_code,
        )

    async def deliberate_cross_subscription_signature(self, identity_secret: str) -> None:
        event_id = "evt_cross_subscription_signature"
        timestamp = "2026-08-19T10:00:02Z"
        body = canonical_json(
            {
                "eventId": event_id,
                "eventType": "crm.note.created",
                "schemaVersion": "1.0",
                "source": "crm",
                "subject": "note/cross-subscription-signature",
                "resourceVersion": 1,
                "correlationId": "case-cross-subscription-signature",
                "causationId": "req_cross_subscription_signature",
                "occurredAt": timestamp,
                "recordedAt": timestamp,
                "data": {"proof": "identity-secret-cannot-sign-crm"},
            }
        )
        signature = hmac.new(
            identity_secret.encode(),
            timestamp.encode() + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        response = await self.client.post(
            f"{self.receiver}/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Twin-Event-Id": event_id,
                "X-Twin-Timestamp": timestamp,
                "X-Twin-Signature": f"v1={signature}",
            },
        )
        require_status(response, 401)
        error = safe_error(response)
        accepted = await self.receiver_list("events")
        accepted_event = any(item["eventId"] == event_id for item in accepted)
        self.record(
            self.success_calls,
            operation="receiver.signature.cross-subscription-reject",
            method="POST",
            path="/events",
            request_fields={
                "eventId": event_id,
                "envelopeSource": "crm",
                "eventType": "crm.note.created",
                "signingSecretSource": "identity",
                "bodyHash": hashlib.sha256(body).hexdigest(),
            },
            expected={
                "status": 401,
                "error": {"code": "unauthenticated"},
                "accepted": False,
            },
            actual={
                "status": response.status_code,
                "error": error,
                "accepted": accepted_event,
            },
            response_status=response.status_code,
        )

    async def success(self) -> None:
        reset = await self.reset(self.success_calls)
        await self.receiver_reset()
        manager = await self.token(
            "webhook-manager",
            "webhook-secret",
            "webhooks:manage",
            "case-manager-before-subscription",
            self.success_calls,
        )
        identity_secret = await self.create_subscription(
            self.identity,
            manager,
            "identity",
            "identity.token.issued",
            "identity-subscription-conformance",
            self.success_calls,
        )
        support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
            "case-identity-unarmed-retry",
            self.success_calls,
        )
        unarmed = await self.wait_for(
            "attempts",
            lambda values: any(
                item.get("source") == "identity"
                and item.get("eventType") == "identity.token.issued"
                and item.get("correlationId") == "case-identity-unarmed-retry"
                and item.get("outcome") == "unarmed"
                for item in values
            ),
            "the unarmed Identity delivery",
        )
        original = next(
            item for item in unarmed if item.get("correlationId") == "case-identity-unarmed-retry"
        )
        await self.arm_receiver("identity", "identity.token.issued", identity_secret)
        await self.advance_two_seconds()
        attempts = await self.wait_for(
            "attempts",
            lambda values: any(
                item.get("eventId") == original["eventId"]
                and item.get("bodyHash") == original["bodyHash"]
                and item.get("outcome") == "accepted"
                for item in values
            ),
            "the same Identity delivery after its exact PT2S retry",
        )
        retried = next(
            item
            for item in attempts
            if item.get("eventId") == original["eventId"] and item.get("outcome") == "accepted"
        )
        if retried["bodyHash"] != original["bodyHash"]:
            raise AssertionError("Identity retry changed the event body")
        self.record(
            self.success_calls,
            operation="relay.identity.retry",
            method="POST",
            path="/events",
            request_fields={
                "virtualAdvance": "PT2S",
                "eventId": original["eventId"],
                "bodyHash": original["bodyHash"],
            },
            expected={"sameEventId": True, "sameBodyHash": True},
            actual={
                "sameEventId": retried["eventId"] == original["eventId"],
                "sameBodyHash": retried["bodyHash"] == original["bodyHash"],
            },
            response_status=204,
        )

        crm_secret = await self.create_subscription(
            self.crm,
            manager,
            "crm",
            "crm.note.created",
            "crm-subscription-conformance",
            self.success_calls,
        )
        await self.arm_receiver("crm", "crm.note.created", crm_secret)
        await self.deliberate_cross_subscription_signature(identity_secret)
        await self.deliberate_bad_signature()

        headers = self.business_headers(support, "case-platform-success")
        me = await self.client.get(f"{self.identity}/v1/me", headers=headers)
        require_status(me, 200)
        me_body = object_body(me)
        self.record(
            self.success_calls,
            operation="identity.me",
            method="GET",
            path="/v1/me",
            request_fields={},
            expected={"subject": "person-support-1"},
            actual={"subject": me_body["subject"], "role": me_body["role"]},
            response_status=me.status_code,
        )
        search = await self.client.get(
            f"{self.crm}/v1/customers",
            params={"email": "alex.unique@example.test"},
            headers=headers,
        )
        require_status(search, 200)
        search_body = object_body(search)
        items = cast(list[JsonObject], search_body["items"])
        ids = [item["customerId"] for item in items]
        if ids != ["cus_unique"]:
            raise AssertionError("unique customer search did not return exactly cus_unique")
        self.record(
            self.success_calls,
            operation="crm.customer.search",
            method="GET",
            path="/v1/customers",
            request_fields={"emailHash": hashlib.sha256(b"alex.unique@example.test").hexdigest()},
            expected={"customerIds": ["cus_unique"]},
            actual={"customerIds": ids},
            response_status=search.status_code,
        )
        customer = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=headers,
        )
        require_status(customer, 200)
        customer_body = object_body(customer)
        self.record(
            self.success_calls,
            operation="crm.customer.get",
            method="GET",
            path="/v1/customers/cus_unique",
            request_fields={},
            expected={"customerId": "cus_unique", "version": 1},
            actual={
                "customerId": customer_body["customerId"],
                "version": customer_body["version"],
            },
            response_status=customer.status_code,
        )
        note_payload = {"body": "Customer prefers email", "association": "account"}
        note_headers = headers | {
            "Idempotency-Key": "platform-success-note",
            "If-Match": customer.headers["ETag"],
        }
        created = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=note_headers,
            json=note_payload,
        )
        require_status(created, 201)
        created_body = object_body(created)
        self.record(
            self.success_calls,
            operation="crm.note.create",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "bodyHash": hashlib.sha256(note_payload["body"].encode()).hexdigest(),
                "association": "account",
                "expectedVersion": 1,
            },
            expected={"status": 201, "replayed": False},
            actual={
                "status": created.status_code,
                "noteId": created_body["noteId"],
                "replayed": created.headers["Idempotency-Replayed"] == "true",
            },
            response_status=created.status_code,
        )
        replay = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=note_headers,
            json=note_payload,
        )
        require_status(replay, 201)
        replay_body = object_body(replay)
        if replay_body != created_body or replay.headers.get("Idempotency-Replayed") != "true":
            raise AssertionError("CRM idempotent replay differed from the original note")
        self.record(
            self.success_calls,
            operation="crm.note.replay",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"sameIdempotencyKey": True, "sameBodyHash": True},
            expected={"sameNoteId": True, "replayed": True},
            actual={
                "sameNoteId": replay_body["noteId"] == created_body["noteId"],
                "replayed": replay.headers["Idempotency-Replayed"] == "true",
            },
            response_status=replay.status_code,
        )
        events = await self.wait_for(
            "events",
            lambda values: any(
                item.get("eventType") == "crm.note.created"
                and item.get("correlationId") == "case-platform-success"
                for item in values
            ),
            "the due CRM note event without a virtual-time advance",
        )
        webhook_attempts = await self.receiver_list("attempts")
        if not any(item.get("outcome") == "signature_rejected" for item in webhook_attempts):
            raise AssertionError("webhook transcript omitted the deliberate signature rejection")
        self.write("successful-calls.json", {"calls": self.success_calls})
        self.write(
            "webhook-transcript.json",
            {
                "attempts": webhook_attempts,
                "acceptedEvents": events,
                "identityRetry": {
                    "virtualAdvance": "PT2S",
                    "sameEventId": retried["eventId"] == original["eventId"],
                    "sameBodyHash": retried["bodyHash"] == original["bodyHash"],
                },
                "crossSubscriptionRejected": True,
                "zeroSignatureRejected": True,
                "wrongSignatureRejected": True,
            },
        )
        self.write(
            "prepare-state.json",
            {
                "scenarioEpoch": reset["scenarioEpoch"],
                "manifestChecksum": reset["manifestChecksum"],
                "noteId": created_body["noteId"],
            },
        )

    async def after_restart(
        self,
        before_id: str,
        before_started_at: str,
        after_id: str,
        after_started_at: str,
    ) -> None:
        if not all((before_id, before_started_at, after_id, after_started_at)):
            raise AssertionError("restart metadata is incomplete")
        if before_started_at == after_started_at:
            raise AssertionError("CRM StartedAt did not change across restart")
        saved = self.read("prepare-state.json")
        calls: Transcript = []
        support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
            "case-platform-restart",
            calls,
        )
        notes = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=self.business_headers(support, "case-platform-restart"),
        )
        require_status(notes, 200)
        body = object_body(notes)
        note_ids = [item["noteId"] for item in cast(list[JsonObject], body["items"])]
        if saved["noteId"] not in note_ids:
            raise AssertionError("CRM note did not survive the application restart")
        self.record(
            calls,
            operation="crm.notes.after-restart",
            method="GET",
            path="/v1/customers/cus_unique/notes",
            request_fields={"expectedNoteId": saved["noteId"]},
            expected={"status": 200, "savedNotePresent": True},
            actual={
                "status": notes.status_code,
                "noteCount": len(note_ids),
                "savedNotePresent": saved["noteId"] in note_ids,
            },
            response_status=notes.status_code,
        )
        self.write("restart-calls.json", {"calls": calls})
        self.write(
            "restart.json",
            {
                "before": {"containerId": before_id, "startedAt": before_started_at},
                "after": {"containerId": after_id, "startedAt": after_started_at},
                "publicState": {"noteId": saved["noteId"], "survived": True},
            },
        )

    async def failure(self) -> None:
        initial_reset = await self.reset(self.failure_calls)
        initial_status_response = await self.client.get(
            f"{self.control}/control/v1/status",
            headers=self.control_headers,
        )
        require_status(initial_status_response, 200)
        initial_status = object_body(initial_status_response)
        self.record(
            self.failure_calls,
            operation="control.status.initial",
            method="GET",
            path="/control/v1/status",
            request_fields={},
            expected={
                "status": 200,
                "now": INITIAL_TIME,
                "randomSeed": RANDOM_SEED,
            },
            actual={
                "status": initial_status_response.status_code,
                "now": initial_status["now"],
                "randomSeed": initial_status["randomSeed"],
                "scenarioEpoch": initial_status["scenarioEpoch"],
                "manifestChecksum": initial_status["manifestChecksum"],
            },
            response_status=initial_status_response.status_code,
        )
        old_support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
            "case-failure-old-token",
            self.failure_calls,
        )
        manager = await self.token(
            "webhook-manager",
            "webhook-secret",
            "webhooks:manage",
            "case-failure-manager",
            self.failure_calls,
        )
        missing = await self.client.get(
            f"{self.crm}/v1/customers",
            headers={"X-Correlation-Id": "case-failure-missing-token"},
        )
        require_status(missing, 401)
        self.record(
            self.failure_calls,
            operation="auth.missing",
            method="GET",
            path="/v1/customers",
            request_fields={},
            expected={"status": 401, "error": {"code": "unauthenticated"}},
            actual={"status": missing.status_code, "error": safe_error(missing)},
            response_status=missing.status_code,
        )
        read_only = await self.token(
            "read-only-evaluator",
            "evaluator-secret",
            "crm:read",
            "case-failure-read-only",
            self.failure_calls,
        )
        read_headers = self.business_headers(read_only, "case-failure-read-only")
        allowed = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=read_headers,
        )
        require_status(allowed, 200)
        allowed_body = object_body(allowed)
        self.record(
            self.failure_calls,
            operation="auth.read-only-read",
            method="GET",
            path="/v1/customers/cus_unique",
            request_fields={"actorClass": "read-only"},
            expected={"status": 200, "customerId": "cus_unique"},
            actual={
                "status": allowed.status_code,
                "customerId": allowed_body["customerId"],
                "version": allowed_body["version"],
            },
            response_status=allowed.status_code,
        )
        forbidden_body_text = "must not be written"
        forbidden = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=read_headers | {"Idempotency-Key": "forbidden-note", "If-Match": '"1"'},
            json={"body": forbidden_body_text, "association": "account"},
        )
        require_status(forbidden, 403)
        self.record(
            self.failure_calls,
            operation="auth.read-only-write",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"actorClass": "read-only", "expectedVersion": 1},
            expected={"status": 403, "error": {"code": "forbidden"}},
            actual={
                "status": forbidden.status_code,
                "error": safe_error(forbidden),
            },
            response_status=forbidden.status_code,
        )
        headers = self.business_headers(old_support, "case-platform-failures")
        after_forbidden = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=headers,
        )
        require_status(after_forbidden, 200)
        after_forbidden_items = cast(list[JsonObject], object_body(after_forbidden)["items"])
        rejected_body_present = any(
            item["body"] == forbidden_body_text for item in after_forbidden_items
        )
        self.record(
            self.failure_calls,
            operation="crm.notes.after-read-only-denial",
            method="GET",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "rejectedBodyHash": hashlib.sha256(forbidden_body_text.encode()).hexdigest()
            },
            expected={"noteCount": 0, "rejectedBodyPresent": False},
            actual={
                "noteCount": len(after_forbidden_items),
                "rejectedBodyPresent": rejected_body_present,
            },
            response_status=after_forbidden.status_code,
        )
        ambiguous = await self.client.get(
            f"{self.crm}/v1/customers",
            params={"email": "shared@example.test"},
            headers=headers,
        )
        require_status(ambiguous, 200)
        ambiguous_body = object_body(ambiguous)
        ambiguous_ids = [
            item["customerId"] for item in cast(list[JsonObject], ambiguous_body["items"])
        ]
        if ambiguous_ids != ["cus_ambiguous_a", "cus_ambiguous_b"]:
            raise AssertionError("ambiguous search did not return both ordered customers")
        self.record(
            self.failure_calls,
            operation="crm.search.ambiguous",
            method="GET",
            path="/v1/customers",
            request_fields={"emailHash": hashlib.sha256(b"shared@example.test").hexdigest()},
            expected={"customerIds": ["cus_ambiguous_a", "cus_ambiguous_b"]},
            actual={"customerIds": ambiguous_ids},
            response_status=ambiguous.status_code,
        )
        baseline_payload = {"body": "idempotency baseline", "association": "account"}
        stale_headers = headers | {
            "Idempotency-Key": "reused-after-reset",
            "If-Match": '"0"',
        }
        stale = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=stale_headers,
            json=baseline_payload,
        )
        require_status(stale, 409)
        self.record(
            self.failure_calls,
            operation="crm.note.precondition.stale",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 0},
            expected={"status": 409},
            actual={"status": stale.status_code, "error": safe_error(stale)},
            response_status=stale.status_code,
        )
        valid_headers = stale_headers | {"If-Match": '"1"'}
        valid = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json=baseline_payload,
        )
        require_status(valid, 201)
        valid_body = object_body(valid)
        self.record(
            self.failure_calls,
            operation="crm.note.create.idempotency-baseline",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "keyLabel": "reused-after-reset",
                "expectedVersion": 1,
                "bodyHash": hashlib.sha256(baseline_payload["body"].encode()).hexdigest(),
            },
            expected={"status": 201, "replayed": False},
            actual={
                "status": valid.status_code,
                "noteId": valid_body["noteId"],
                "replayed": valid.headers.get("Idempotency-Replayed") == "true",
            },
            response_status=valid.status_code,
        )
        changed_body_text = "changed under the same key"
        changed = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json={"body": changed_body_text, "association": "account"},
        )
        require_status(changed, 409)
        self.record(
            self.failure_calls,
            operation="crm.note.idempotency-mismatch",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "keyLabel": "reused-after-reset",
                "bodyHash": hashlib.sha256(changed_body_text.encode()).hexdigest(),
            },
            expected={"status": 409},
            actual={
                "status": changed.status_code,
                "error": safe_error(changed),
            },
            response_status=changed.status_code,
        )
        after_changed = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=headers,
        )
        require_status(after_changed, 200)
        after_changed_items = cast(list[JsonObject], object_body(after_changed)["items"])
        valid_note_count = sum(
            item["noteId"] == valid_body["noteId"] and item["body"] == baseline_payload["body"]
            for item in after_changed_items
        )
        self.record(
            self.failure_calls,
            operation="crm.notes.after-idempotency-denial",
            method="GET",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "validNoteId": valid_body["noteId"],
                "forbiddenBodyHash": hashlib.sha256(forbidden_body_text.encode()).hexdigest(),
                "changedBodyHash": hashlib.sha256(changed_body_text.encode()).hexdigest(),
            },
            expected={
                "noteCount": 1,
                "validNoteCount": 1,
                "validNoteIdMatches": True,
                "forbiddenBodyPresent": False,
                "changedBodyPresent": False,
            },
            actual={
                "noteCount": len(after_changed_items),
                "validNoteCount": valid_note_count,
                "validNoteIdMatches": any(
                    item["noteId"] == valid_body["noteId"] for item in after_changed_items
                ),
                "forbiddenBodyPresent": any(
                    item["body"] == forbidden_body_text for item in after_changed_items
                ),
                "changedBodyPresent": any(
                    item["body"] == changed_body_text for item in after_changed_items
                ),
            },
            response_status=after_changed.status_code,
        )
        invalid_target = await self.client.post(
            f"{self.crm}/v1/webhook-subscriptions",
            headers=self.business_headers(manager, "case-invalid-target")
            | {"Idempotency-Key": "invalid-target"},
            json={"eventTypes": ["crm.note.created"], "targetUrl": "http://127.0.0.1/events"},
        )
        require_status(invalid_target, 422)
        self.record(
            self.failure_calls,
            operation="webhook.target.denied",
            method="POST",
            path="/v1/webhook-subscriptions",
            request_fields={"targetHost": "127.0.0.1"},
            expected={"status": 422},
            actual={"status": invalid_target.status_code, "error": safe_error(invalid_target)},
            response_status=invalid_target.status_code,
        )
        fault = await self.client.post(
            f"{self.control}/control/v1/faults",
            headers=self.control_headers,
            json={
                "ruleId": "crm-note-timeout-once",
                "targetService": "crm",
                "operation": "crm.note.create",
                "phase": "after_commit",
                "effect": "timeout",
                "actorId": "person-support-1",
                "occurrence": 1,
                "activationCount": 1,
                "delayMs": 500,
            },
        )
        require_status(fault, 201)
        fault_body = object_body(fault)
        self.record(
            self.failure_calls,
            operation="control.fault.create",
            method="POST",
            path="/control/v1/faults",
            request_fields={
                "ruleId": "crm-note-timeout-once",
                "targetService": "crm",
                "phase": "after_commit",
                "delayMs": 500,
            },
            expected={"status": 201, "ruleId": "crm-note-timeout-once"},
            actual={
                "status": fault.status_code,
                "ruleId": fault_body["ruleId"],
            },
            response_status=fault.status_code,
        )
        current = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=headers,
        )
        require_status(current, 200)
        current_body = object_body(current)
        self.record(
            self.failure_calls,
            operation="crm.customer.before-timeout",
            method="GET",
            path="/v1/customers/cus_unique",
            request_fields={},
            expected={"status": 200, "customerId": "cus_unique"},
            actual={
                "status": current.status_code,
                "customerId": current_body["customerId"],
                "version": current_body["version"],
            },
            response_status=current.status_code,
        )
        timeout_headers = headers | {
            "Idempotency-Key": "timeout-note",
            "If-Match": current.headers["ETag"],
        }
        timeout_payload = {"body": "committed before timeout", "association": "account"}
        try:
            await self.client.post(
                f"{self.crm}/v1/customers/cus_unique/notes",
                headers=timeout_headers,
                json=timeout_payload,
                timeout=httpx.Timeout(connect=5.0, read=0.1, write=5.0, pool=5.0),
            )
        except httpx.ReadTimeout:
            self.record(
                self.failure_calls,
                operation="crm.note.after-commit-timeout",
                method="POST",
                path="/v1/customers/cus_unique/notes",
                request_fields={
                    "bodyHash": hashlib.sha256(timeout_payload["body"].encode()).hexdigest(),
                    "serverDelayMs": 500,
                    "clientReadTimeoutMs": 100,
                },
                expected={"error": "ReadTimeout"},
                actual={"error": "ReadTimeout"},
                error="ReadTimeout",
            )
        else:
            raise AssertionError("after-commit fault did not cause the expected ReadTimeout")
        notes = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=headers,
        )
        require_status(notes, 200)
        note_items = cast(list[JsonObject], object_body(notes)["items"])
        timeout_notes = [item for item in note_items if item["body"] == timeout_payload["body"]]
        if len(timeout_notes) != 1:
            raise AssertionError("timeout reconciliation did not find exactly one committed note")
        self.record(
            self.failure_calls,
            operation="crm.note.timeout.read",
            method="GET",
            path="/v1/customers/cus_unique/notes",
            request_fields={
                "bodyHash": hashlib.sha256(timeout_payload["body"].encode()).hexdigest()
            },
            expected={"status": 200, "noteCount": 2, "timeoutNoteCount": 1},
            actual={
                "status": notes.status_code,
                "noteCount": len(note_items),
                "timeoutNoteCount": len(timeout_notes),
                "timeoutNoteId": timeout_notes[0]["noteId"],
            },
            response_status=notes.status_code,
        )
        replay = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=timeout_headers,
            json=timeout_payload,
        )
        require_status(replay, 201)
        replay_body = object_body(replay)
        if (
            replay_body["noteId"] != timeout_notes[0]["noteId"]
            or replay.headers.get("Idempotency-Replayed") != "true"
        ):
            raise AssertionError("timeout replay did not return the committed note")
        self.record(
            self.failure_calls,
            operation="crm.note.timeout.replay",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"sameIdempotencyKey": True},
            expected={"status": 201, "replayed": True, "sameNoteId": True},
            actual={
                "status": replay.status_code,
                "sameNoteId": replay_body["noteId"] == timeout_notes[0]["noteId"],
                "replayed": replay.headers.get("Idempotency-Replayed") == "true",
            },
            response_status=replay.status_code,
        )
        activations_response = await self.client.get(
            f"{self.control}/control/v1/fault-activations",
            headers=self.control_headers,
        )
        require_status(activations_response, 200)
        activations = list_body(activations_response)
        if len(activations) != 1 or activations[0]["ruleId"] != "crm-note-timeout-once":
            raise AssertionError("expected exactly one recorded CRM timeout activation")
        self.record(
            self.failure_calls,
            operation="control.fault-activations.before-reset",
            method="GET",
            path="/control/v1/fault-activations",
            request_fields={},
            expected={"status": 200, "activationCount": 1},
            actual={
                "status": activations_response.status_code,
                "activationCount": len(activations),
                "ruleIds": [item["ruleId"] for item in activations],
            },
            response_status=activations_response.status_code,
        )
        await self.create_subscription(
            self.identity,
            manager,
            "identity",
            "identity.token.issued",
            "identity-subscription-cleared-by-final-reset",
            self.failure_calls,
        )
        await self.create_subscription(
            self.crm,
            manager,
            "crm",
            "crm.note.created",
            "subscription-cleared-by-final-reset",
            self.failure_calls,
        )
        identity_subscriptions_before_response = await self.client.get(
            f"{self.identity}/v1/webhook-subscriptions",
            headers=self.business_headers(manager, "case-identity-subscriptions-before-reset"),
        )
        crm_subscriptions_before_response = await self.client.get(
            f"{self.crm}/v1/webhook-subscriptions",
            headers=self.business_headers(manager, "case-crm-subscriptions-before-reset"),
        )
        require_status(identity_subscriptions_before_response, 200)
        require_status(crm_subscriptions_before_response, 200)
        identity_subscriptions_before = list_body(identity_subscriptions_before_response)
        crm_subscriptions_before = list_body(crm_subscriptions_before_response)
        if len(identity_subscriptions_before) != 1 or len(crm_subscriptions_before) != 1:
            raise AssertionError("failure proof did not observe both subscriptions before reset")
        self.record(
            self.failure_calls,
            operation="identity.subscriptions.before-reset",
            method="GET",
            path="/v1/webhook-subscriptions",
            request_fields={"source": "identity"},
            expected={"status": 200, "subscriptionCount": 1},
            actual={
                "status": identity_subscriptions_before_response.status_code,
                "subscriptionCount": len(identity_subscriptions_before),
                "subscriptionIds": [
                    item["subscriptionId"] for item in identity_subscriptions_before
                ],
            },
            response_status=identity_subscriptions_before_response.status_code,
        )
        self.record(
            self.failure_calls,
            operation="crm.subscriptions.before-reset",
            method="GET",
            path="/v1/webhook-subscriptions",
            request_fields={"source": "crm"},
            expected={"status": 200, "subscriptionCount": 1},
            actual={
                "status": crm_subscriptions_before_response.status_code,
                "subscriptionCount": len(crm_subscriptions_before),
                "subscriptionIds": [item["subscriptionId"] for item in crm_subscriptions_before],
            },
            response_status=crm_subscriptions_before_response.status_code,
        )
        advance_response = await self.client.post(
            f"{self.control}/control/v1/time/advance",
            headers=self.control_headers,
            json={"duration": "PT5M"},
        )
        require_status(advance_response, 200)
        advanced_time = object_body(advance_response)["now"]
        self.record(
            self.failure_calls,
            operation="control.time.advance.pre-reset",
            method="POST",
            path="/control/v1/time/advance",
            request_fields={"duration": "PT5M"},
            expected={"status": 200, "now": "2026-08-19T10:05:00Z"},
            actual={"status": advance_response.status_code, "now": advanced_time},
            response_status=advance_response.status_code,
        )

        final_reset = await self.reset(self.failure_calls)
        if final_reset["scenarioEpoch"] == initial_reset["scenarioEpoch"]:
            raise AssertionError("reset did not assign a new scenario epoch")
        if final_reset["manifestChecksum"] != initial_reset["manifestChecksum"]:
            raise AssertionError("same-scenario same-seed reset checksum changed")
        if final_reset["randomSeed"] != RANDOM_SEED:
            raise AssertionError("reset result did not retain random seed 7")
        status_response = await self.client.get(
            f"{self.control}/control/v1/status",
            headers=self.control_headers,
        )
        require_status(status_response, 200)
        status = object_body(status_response)
        if (
            status["randomSeed"] != RANDOM_SEED
            or status["manifestChecksum"] != final_reset["manifestChecksum"]
            or status["scenarioEpoch"] != final_reset["scenarioEpoch"]
            or status["now"] != INITIAL_TIME
        ):
            raise AssertionError("stored reset status differs from the reset result")
        self.record(
            self.failure_calls,
            operation="control.status.after-reset",
            method="GET",
            path="/control/v1/status",
            request_fields={},
            expected={
                "status": 200,
                "now": initial_status["now"],
                "randomSeed": RANDOM_SEED,
                "scenarioEpoch": final_reset["scenarioEpoch"],
                "manifestChecksum": final_reset["manifestChecksum"],
            },
            actual={
                "status": status_response.status_code,
                "now": status["now"],
                "randomSeed": status["randomSeed"],
                "scenarioEpoch": status["scenarioEpoch"],
                "manifestChecksum": status["manifestChecksum"],
            },
            response_status=status_response.status_code,
        )
        old_token = await self.client.get(
            f"{self.identity}/v1/me",
            headers=self.business_headers(old_support, "case-old-token-after-reset"),
        )
        require_status(old_token, 401)
        self.record(
            self.failure_calls,
            operation="identity.old-token.after-reset",
            method="GET",
            path="/v1/me",
            request_fields={"tokenEpoch": "before-reset"},
            expected={"status": 401, "error": {"code": "unauthenticated"}},
            actual={"status": old_token.status_code, "error": safe_error(old_token)},
            response_status=old_token.status_code,
        )
        new_manager = await self.token(
            "webhook-manager",
            "webhook-secret",
            "webhooks:manage",
            "case-manager-after-reset",
            self.failure_calls,
        )
        identity_subscriptions = await self.client.get(
            f"{self.identity}/v1/webhook-subscriptions",
            headers=self.business_headers(new_manager, "case-subscriptions-after-reset"),
        )
        crm_subscriptions = await self.client.get(
            f"{self.crm}/v1/webhook-subscriptions",
            headers=self.business_headers(new_manager, "case-subscriptions-after-reset"),
        )
        require_status(identity_subscriptions, 200)
        require_status(crm_subscriptions, 200)
        identity_subscriptions_after = list_body(identity_subscriptions)
        crm_subscriptions_after = list_body(crm_subscriptions)
        if identity_subscriptions_after or crm_subscriptions_after:
            raise AssertionError("reset retained webhook subscriptions")
        self.record(
            self.failure_calls,
            operation="identity.subscriptions.after-reset",
            method="GET",
            path="/v1/webhook-subscriptions",
            request_fields={"source": "identity"},
            expected={"status": 200, "subscriptionCount": 0},
            actual={
                "status": identity_subscriptions.status_code,
                "subscriptionCount": len(identity_subscriptions_after),
                "subscriptionIds": [
                    item["subscriptionId"] for item in identity_subscriptions_after
                ],
            },
            response_status=identity_subscriptions.status_code,
        )
        self.record(
            self.failure_calls,
            operation="crm.subscriptions.after-reset",
            method="GET",
            path="/v1/webhook-subscriptions",
            request_fields={"source": "crm"},
            expected={"status": 200, "subscriptionCount": 0},
            actual={
                "status": crm_subscriptions.status_code,
                "subscriptionCount": len(crm_subscriptions_after),
                "subscriptionIds": [item["subscriptionId"] for item in crm_subscriptions_after],
            },
            response_status=crm_subscriptions.status_code,
        )
        new_support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
            "case-support-after-reset",
            self.failure_calls,
        )
        new_headers = self.business_headers(new_support, "case-state-after-reset")
        empty_notes = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=new_headers,
        )
        require_status(empty_notes, 200)
        empty_note_items = cast(list[JsonObject], object_body(empty_notes)["items"])
        if empty_note_items:
            raise AssertionError("reset retained CRM notes")
        self.record(
            self.failure_calls,
            operation="crm.notes.after-reset",
            method="GET",
            path="/v1/customers/cus_unique/notes",
            request_fields={},
            expected={"status": 200, "noteCount": 0},
            actual={"status": empty_notes.status_code, "noteCount": len(empty_note_items)},
            response_status=empty_notes.status_code,
        )
        cleared_faults = await self.client.get(
            f"{self.control}/control/v1/fault-activations",
            headers=self.control_headers,
        )
        require_status(cleared_faults, 200)
        cleared_fault_items = list_body(cleared_faults)
        if cleared_fault_items:
            raise AssertionError("reset retained fault activations")
        self.record(
            self.failure_calls,
            operation="control.fault-activations.after-reset",
            method="GET",
            path="/control/v1/fault-activations",
            request_fields={},
            expected={"status": 200, "activationCount": 0},
            actual={
                "status": cleared_faults.status_code,
                "activationCount": len(cleared_fault_items),
                "ruleIds": [item["ruleId"] for item in cleared_fault_items],
            },
            response_status=cleared_faults.status_code,
        )
        reused = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=new_headers | {"Idempotency-Key": "reused-after-reset", "If-Match": '"1"'},
            json=baseline_payload,
        )
        require_status(reused, 201)
        if reused.headers.get("Idempotency-Replayed") != "false":
            raise AssertionError("pre-reset idempotency key replayed after reset")
        self.record(
            self.failure_calls,
            operation="crm.note.idempotency-reuse.after-reset",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 1},
            expected={"status": 201, "replayed": False},
            actual={
                "status": reused.status_code,
                "noteId": object_body(reused)["noteId"],
                "replayed": reused.headers.get("Idempotency-Replayed") == "true",
            },
            response_status=reused.status_code,
        )
        self.write("failure-calls.json", {"calls": self.failure_calls})
        self.write("fault-activations.json", {"activations": activations})
        self.write(
            "reset-checksums.json",
            {
                "before": {
                    "scenarioEpoch": initial_reset["scenarioEpoch"],
                    "randomSeed": initial_reset["randomSeed"],
                    "manifestChecksum": initial_reset["manifestChecksum"],
                },
                "after": {
                    "scenarioEpoch": final_reset["scenarioEpoch"],
                    "randomSeed": final_reset["randomSeed"],
                    "manifestChecksum": final_reset["manifestChecksum"],
                    "storedRandomSeed": status["randomSeed"],
                    "storedManifestChecksum": status["manifestChecksum"],
                },
                "assertions": {
                    "newEpoch": final_reset["scenarioEpoch"] != initial_reset["scenarioEpoch"],
                    "sameChecksum": final_reset["manifestChecksum"]
                    == initial_reset["manifestChecksum"],
                    "oldTokenRejected": old_token.status_code == 401,
                    "faultsCleared": not cleared_fault_items,
                    "subscriptionsCleared": not identity_subscriptions_after
                    and not crm_subscriptions_after,
                    "notesClearedBeforeReuse": not empty_note_items,
                    "virtualTimeRestored": status["now"] == initial_status["now"],
                    "idempotencyKeyDidNotReplay": reused.headers.get("Idempotency-Replayed")
                    == "false",
                    "identitySubscriptionObservedBeforeReset": len(identity_subscriptions_before)
                    == 1,
                    "crmSubscriptionObservedBeforeReset": len(crm_subscriptions_before) == 1,
                },
                "subscriptionCounts": {
                    "identityBeforeReset": len(identity_subscriptions_before),
                    "crmBeforeReset": len(crm_subscriptions_before),
                    "identityAfterReset": len(identity_subscriptions_after),
                    "crmAfterReset": len(crm_subscriptions_after),
                },
                "virtualTime": {
                    "initial": initial_status["now"],
                    "beforeReset": advanced_time,
                    "afterReset": status["now"],
                },
            },
        )

    def summarise(self, mode: str) -> None:
        required_by_mode = {
            "platform-success": {
                "successful-calls.json",
                "webhook-transcript.json",
                "prepare-state.json",
            },
            "platform-failure": {
                "failure-calls.json",
                "fault-activations.json",
                "reset-checksums.json",
            },
            "platform-contracts": {
                "successful-calls.json",
                "failure-calls.json",
                "webhook-transcript.json",
                "reset-checksums.json",
                "fault-activations.json",
                "restart-calls.json",
                "restart.json",
                "network.json",
                "prepare-state.json",
            },
        }
        required = required_by_mode[mode]
        evidence = {name: self.read(name) for name in required}
        if mode in {"platform-success", "platform-contracts"}:
            successful = validate_transcript(
                evidence["successful-calls.json"].get("calls"), "successful-call"
            )
            webhook = evidence["webhook-transcript.json"]
            attempts = evidence_list(webhook.get("attempts"), "webhook attempt")
            accepted_events = evidence_list(webhook.get("acceptedEvents"), "accepted webhook event")
            outcomes = {item.get("outcome") for item in attempts}
            retry = evidence_object(webhook.get("identityRetry"), "identity retry")
            cross_event_id = "evt_cross_subscription_signature"
            cross_attempts = [
                item
                for item in attempts
                if item.get("eventId") == cross_event_id
                and item.get("source") == "crm"
                and item.get("eventType") == "crm.note.created"
                and item.get("outcome") == "signature_rejected"
                and item.get("responseStatus") == 401
            ]
            if (
                not {"unarmed", "accepted", "signature_rejected"}.issubset(outcomes)
                or retry.get("virtualAdvance") != "PT2S"
                or retry.get("sameEventId") is not True
                or retry.get("sameBodyHash") is not True
                or webhook.get("crossSubscriptionRejected") is not True
                or webhook.get("wrongSignatureRejected") is not True
                or len(cross_attempts) != 1
                or any(item.get("eventId") == cross_event_id for item in accepted_events)
            ):
                raise AssertionError(
                    "webhook evidence does not prove retry and cross-subscription signature checks"
                )
            successful_operations = {cast(str, call["operation"]) for call in successful}
            missing_success = REQUIRED_SUCCESS_OPERATIONS - successful_operations
            if missing_success:
                raise AssertionError(
                    f"successful transcript omits required operations: {sorted(missing_success)}"
                )
            validate_operation_contracts(
                successful,
                SUCCESS_OPERATION_CONTRACTS,
                SUCCESS_OPERATION_SEQUENCE,
                "successful transcript",
            )
            cross_calls = [
                call
                for call in successful
                if call["operation"] == "receiver.signature.cross-subscription-reject"
            ]
            cross_request = evidence_object(cross_calls[0]["request"], "cross-subscription request")
            cross_fields = evidence_object(
                cross_request["fields"], "cross-subscription request fields"
            )
            cross_attempt = cross_attempts[0]
            if cross_fields.get("eventId") != cross_attempt.get("eventId") or cross_fields.get(
                "bodyHash"
            ) != cross_attempt.get("bodyHash"):
                raise AssertionError(
                    "cross-subscription receiver attempt does not match the transcript"
                )
            retry_call = next(
                call for call in successful if call["operation"] == "relay.identity.retry"
            )
            retry_request = evidence_object(retry_call["request"], "Identity retry request")
            retry_fields = evidence_object(retry_request["fields"], "Identity retry request fields")
            matching_retry_attempts = [
                attempt
                for attempt in attempts
                if attempt.get("eventId") == retry_fields.get("eventId")
                and attempt.get("bodyHash") == retry_fields.get("bodyHash")
                and attempt.get("source") == "identity"
                and attempt.get("eventType") == "identity.token.issued"
            ]
            if not any(
                attempt.get("outcome") == "unarmed" and attempt.get("responseStatus") == 503
                for attempt in matching_retry_attempts
            ) or not any(
                attempt.get("outcome") == "accepted" and attempt.get("responseStatus") == 204
                for attempt in matching_retry_attempts
            ):
                raise AssertionError("Identity retry attempts do not match the transcript")
            prepare = evidence["prepare-state.json"]
            success_reset = evidence_object(
                operation_actuals(successful, "control.reset")[0],
                "successful reset actual",
            )
            created_note = evidence_object(
                operation_actuals(successful, "crm.note.create")[0],
                "created note actual",
            )
            if (
                prepare.get("scenarioEpoch") != success_reset.get("scenarioEpoch")
                or prepare.get("manifestChecksum") != success_reset.get("manifestChecksum")
                or prepare.get("noteId") != created_note.get("noteId")
            ):
                raise AssertionError("prepare state does not match the success transcript")
        if mode in {"platform-failure", "platform-contracts"}:
            failures = validate_transcript(
                evidence["failure-calls.json"].get("calls"), "failure-call"
            )
            failure_operations = {cast(str, call["operation"]) for call in failures}
            unchanged_state_operations = {
                "crm.notes.after-read-only-denial",
                "crm.notes.after-idempotency-denial",
            }
            missing_unchanged_state = unchanged_state_operations - failure_operations
            if missing_unchanged_state:
                raise AssertionError(
                    "failure transcript omits required unchanged-state reads: "
                    f"{sorted(missing_unchanged_state)}"
                )
            missing_failure = REQUIRED_FAILURE_OPERATIONS - failure_operations
            if missing_failure:
                raise AssertionError(
                    f"failure transcript omits required operations: {sorted(missing_failure)}"
                )
            validate_operation_contracts(
                failures,
                FAILURE_OPERATION_CONTRACTS,
                FAILURE_OPERATION_SEQUENCE,
                "failure transcript",
            )
            activations = evidence_list(
                evidence["fault-activations.json"].get("activations"), "fault activation"
            )
            if len(activations) != 1 or activations[0].get("ruleId") != "crm-note-timeout-once":
                raise AssertionError("failure evidence does not prove one timeout activation")
            activation_actual = evidence_object(
                operation_actuals(
                    failures,
                    "control.fault-activations.before-reset",
                )[0],
                "fault activation transcript actual",
            )
            if activation_actual.get("ruleIds") != [
                activation.get("ruleId") for activation in activations
            ]:
                raise AssertionError("fault activation file does not match the transcript")
            reset = evidence["reset-checksums.json"]
            before = evidence_object(reset.get("before"), "reset before")
            after = evidence_object(reset.get("after"), "reset after")
            reset_assertions = evidence_object(reset.get("assertions"), "reset assertion")
            subscription_counts = evidence_object(
                reset.get("subscriptionCounts"), "subscription count"
            )
            virtual_time = evidence_object(reset.get("virtualTime"), "observed reset virtual time")
            expected_reset_assertions = {
                "newEpoch",
                "sameChecksum",
                "oldTokenRejected",
                "faultsCleared",
                "subscriptionsCleared",
                "notesClearedBeforeReuse",
                "virtualTimeRestored",
                "idempotencyKeyDidNotReplay",
                "identitySubscriptionObservedBeforeReset",
                "crmSubscriptionObservedBeforeReset",
            }
            if (
                before.get("scenarioEpoch") == after.get("scenarioEpoch")
                or before.get("randomSeed") != RANDOM_SEED
                or after.get("randomSeed") != RANDOM_SEED
                or after.get("storedRandomSeed") != RANDOM_SEED
                or before.get("manifestChecksum") != after.get("manifestChecksum")
                or after.get("storedManifestChecksum") != before.get("manifestChecksum")
                or not reset_assertions
                or subscription_counts
                != {
                    "identityBeforeReset": 1,
                    "crmBeforeReset": 1,
                    "identityAfterReset": 0,
                    "crmAfterReset": 0,
                }
                or virtual_time.get("initial") != INITIAL_TIME
                or virtual_time.get("beforeReset") == virtual_time.get("initial")
                or virtual_time.get("afterReset") != virtual_time.get("initial")
                or set(reset_assertions) != expected_reset_assertions
                or not all(value is True for value in reset_assertions.values())
            ):
                raise AssertionError("observed reset evidence does not prove the contract")
            validate_reset_transcript_binding(
                failures,
                before,
                after,
                virtual_time,
            )
        if mode == "platform-contracts":
            restart_calls = validate_transcript(
                evidence["restart-calls.json"].get("calls"), "restart-call"
            )
            validate_operation_contracts(
                restart_calls,
                RESTART_OPERATION_CONTRACTS,
                RESTART_OPERATION_SEQUENCE,
                "restart transcript",
            )
            restart_operations = {cast(str, call["operation"]) for call in restart_calls}
            if not {"identity.token.issue", "crm.notes.after-restart"}.issubset(restart_operations):
                raise AssertionError("restart transcript omits the public persistence read")
            restart = evidence["restart.json"]
            before_restart = evidence_object(restart.get("before"), "restart before")
            after_restart = evidence_object(restart.get("after"), "restart after")
            public_state = evidence_object(restart.get("publicState"), "restart public state")
            network_assertions = evidence_object(
                evidence["network.json"].get("assertions"), "network assertion"
            )
            if (
                not before_restart.get("containerId")
                or not after_restart.get("containerId")
                or before_restart.get("startedAt") == after_restart.get("startedAt")
                or public_state.get("survived") is not True
            ):
                raise AssertionError("restart evidence does not prove restart persistence")
            restart_read = next(
                call for call in restart_calls if call["operation"] == "crm.notes.after-restart"
            )
            restart_request = evidence_object(restart_read["request"], "restart-state request")
            restart_fields = evidence_object(
                restart_request["fields"], "restart-state request fields"
            )
            prepare = evidence["prepare-state.json"]
            if public_state.get("noteId") != restart_fields.get(
                "expectedNoteId"
            ) or public_state.get("noteId") != prepare.get("noteId"):
                raise AssertionError("restart state does not match the transcript")
            if not network_assertions or not all(
                value is True for value in network_assertions.values()
            ):
                raise AssertionError("network evidence does not prove the isolation assertions")
        if mode == "platform-contracts":
            summary = {
                "status": "passed",
                "successfulSequence": "passed",
                "failureSequence": "passed",
                "restartPersistence": "passed",
                "webhookSignatures": "passed",
                "resetContract": "passed",
                "networkIsolation": "passed",
            }
        else:
            summary = {"status": "passed", "mode": mode}
        self.write("summary.json", summary)


def inspect_json(docker: str, container: str) -> JsonObject:
    value = json.loads(run_command([docker, "inspect", container]).stdout)
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise AssertionError("Docker inspect returned an unexpected document")
    return cast(JsonObject, value[0])


def record_network_proof(artifact_root: Path, run_id: str) -> None:
    docker = shutil.which("docker")
    if docker is None:
        raise AssertionError("docker is required for the network proof")
    configuration = json.loads(
        run_command([docker, "compose", "--profile", "test", "config", "--format", "json"]).stdout
    )
    services = cast(dict[str, JsonObject], configuration["services"])
    expected = {
        "webhook-receiver": {"conformance-admin", "twin-webhook-egress"},
        "conformance": {"conformance-admin", "twin-control", "twin-public"},
        "public-probe": {"twin-public"},
    }
    static_networks = {name: set(services[name]["networks"]) for name in expected}
    if static_networks != expected:
        raise AssertionError("profile service Compose network memberships differ")
    if services["control"].get("ports"):
        raise AssertionError("Control has a host port binding")
    environment = services["conformance"].get("environment", {})
    if "DATABASE_URL" in json.dumps(environment).upper():
        raise AssertionError("conformance service receives a database URL")
    if any(
        "/var/run/docker.sock" in json.dumps(service.get("volumes", []))
        for service in services.values()
    ):
        raise AssertionError("a Compose service mounts the Docker socket")

    runtime_networks: dict[str, list[str]] = {}
    host_bindings: dict[str, object] = {}
    for name, networks in expected.items():
        container = run_command([docker, "compose", "ps", "-q", name]).stdout.strip()
        if not container:
            raise AssertionError(f"{name} is not running")
        inspected = inspect_json(docker, container)
        actual_networks = {
            value["Labels"]["com.docker.compose.network"]
            for network_name in cast(JsonObject, inspected["NetworkSettings"])["Networks"]
            for value in json.loads(
                run_command([docker, "network", "inspect", str(network_name)]).stdout
            )
        }
        if actual_networks != networks:
            raise AssertionError(f"{name} runtime networks differ")
        runtime_networks[name] = sorted(actual_networks)
        bindings = cast(JsonObject, inspected["HostConfig"])["PortBindings"]
        if bindings:
            raise AssertionError(f"{name} has a host port binding")
        host_bindings[name] = bindings
    probe = run_command(
        [docker, "compose", "run", "--rm", "public-probe", "sh", "-c", "! getent hosts control"]
    )
    if probe.returncode != 0:
        raise AssertionError("public-only probe resolved Control")
    payload = {
        "runId": run_id,
        "composeNetworks": {name: sorted(value) for name, value in static_networks.items()},
        "runtimeNetworks": runtime_networks,
        "hostBindings": host_bindings,
        "assertions": {
            "controlNotPublished": True,
            "conformanceHasNoDatabaseUrl": True,
            "dockerSocketAbsent": True,
            "driverOffWebhookEgress": True,
            "receiverOffControl": True,
            "publicProbeCannotResolveControl": True,
        },
    }
    check_no_sensitive_values(payload)
    path = artifact_root / "network.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def publish_latest(root: Path, artifact_root: Path, run_id: str) -> None:
    summary = json.loads((artifact_root / "summary.json").read_text(encoding="utf-8"))
    if summary.get("runId") != run_id or summary.get("status") != "passed":
        raise AssertionError("only a passed run-bound summary can be published")
    relative = artifact_root.relative_to(root)
    pointer = {"runId": run_id, "artifactRoot": str(relative)}
    temporary = root / f".latest-run-{run_id}.tmp"
    temporary.write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(root / "latest-run.json")


async def with_driver(
    operation: Callable[[Driver], Coroutine[Any, Any, None]],
    *,
    resume: bool,
) -> None:
    driver = Driver.resume() if resume else Driver()
    try:
        await operation(driver)
    finally:
        await driver.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="phase", required=True)
    subparsers.add_parser("success")
    subparsers.add_parser("failure")
    restart = subparsers.add_parser("after-restart")
    restart.add_argument("--before-id", required=True)
    restart.add_argument("--before-started-at", required=True)
    restart.add_argument("--after-id", required=True)
    restart.add_argument("--after-started-at", required=True)
    network = subparsers.add_parser("network")
    network.add_argument("--artifact-root", type=Path, required=True)
    network.add_argument("--run-id", required=True)
    summarise = subparsers.add_parser("summarise")
    summarise.add_argument(
        "mode",
        choices=["platform-success", "platform-failure", "platform-contracts"],
    )
    publish = subparsers.add_parser("publish")
    publish.add_argument("--root", type=Path, required=True)
    publish.add_argument("--artifact-root", type=Path, required=True)
    publish.add_argument("--run-id", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.phase == "success":
        asyncio.run(with_driver(lambda driver: driver.success(), resume=False))
    elif args.phase == "failure":
        artifact_root = Path(os.environ["ARTIFACT_ROOT"])
        asyncio.run(with_driver(lambda driver: driver.failure(), resume=artifact_root.is_dir()))
    elif args.phase == "after-restart":
        asyncio.run(
            with_driver(
                lambda driver: driver.after_restart(
                    args.before_id,
                    args.before_started_at,
                    args.after_id,
                    args.after_started_at,
                ),
                resume=True,
            )
        )
    elif args.phase == "network":
        record_network_proof(args.artifact_root, args.run_id)
    elif args.phase == "summarise":
        asyncio.run(with_driver(lambda driver: _summarise(driver, args.mode), resume=True))
    elif args.phase == "publish":
        publish_latest(args.root, args.artifact_root, args.run_id)
    else:  # pragma: no cover
        raise AssertionError("unknown phase")


async def _summarise(driver: Driver, mode: str) -> None:
    driver.summarise(mode)


if __name__ == "__main__":
    main()
