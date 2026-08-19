import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
from collections.abc import Callable, Coroutine
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
FORBIDDEN_ARTEFACT_VALUES = {
    "support-secret",
    "evaluator-secret",
    "webhook-secret",
    "controller-local-token",
    "receiver-conformance-local-token",
}


def object_body(response: httpx.Response) -> JsonObject:
    value = response.json()
    if not isinstance(value, dict):
        raise AssertionError("expected a JSON object response")
    return cast(JsonObject, value)


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
            if key.casefold() in FORBIDDEN_ARTEFACT_KEYS:
                raise AssertionError(f"artefact contains forbidden key: {key}")
            check_no_sensitive_values(item)
    elif isinstance(value, list):
        for item in value:
            check_no_sensitive_values(item)
    elif isinstance(value, str):
        if value.startswith("Bearer ") or value in FORBIDDEN_ARTEFACT_VALUES:
            raise AssertionError("artefact contains a credential value")


def evidence_object(value: object, name: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{name} evidence is not an object")
    return cast(JsonObject, value)


def evidence_list(value: object, name: str) -> list[JsonObject]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise AssertionError(f"{name} evidence is not an object list")
    return cast(list[JsonObject], value)


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
        target.append(
            {
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
        )

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
            operation="receiver.signature.reject",
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
            request_fields={"virtualAdvance": "PT2S"},
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
            expected={"status": 401, "code": "unauthenticated"},
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
        forbidden = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=read_headers | {"Idempotency-Key": "forbidden-note", "If-Match": '"1"'},
            json={"body": "must not be written", "association": "account"},
        )
        require_status(forbidden, 403)
        self.record(
            self.failure_calls,
            operation="auth.read-only-write",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"actorClass": "read-only", "expectedVersion": 1},
            expected={"readStatus": 200, "writeStatus": 403},
            actual={
                "readStatus": allowed.status_code,
                "writeStatus": forbidden.status_code,
                "error": safe_error(forbidden),
            },
            response_status=forbidden.status_code,
        )
        headers = self.business_headers(old_support, "case-platform-failures")
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
        valid_headers = stale_headers | {"If-Match": '"1"'}
        valid = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json=baseline_payload,
        )
        require_status(valid, 201)
        valid_body = object_body(valid)
        changed = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json={"body": "changed under the same key", "association": "account"},
        )
        require_status(changed, 409)
        self.record(
            self.failure_calls,
            operation="crm.note.precondition-and-idempotency",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "expectedVersions": [0, 1]},
            expected={"stale": 409, "valid": 201, "changedReplay": 409},
            actual={
                "stale": stale.status_code,
                "valid": valid.status_code,
                "changedReplay": changed.status_code,
                "noteId": valid_body["noteId"],
            },
            response_status=changed.status_code,
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
        current = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=headers,
        )
        require_status(current, 200)
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
            operation="crm.note.timeout.reconcile",
            method="POST",
            path="/v1/customers/cus_unique/notes",
            request_fields={"sameIdempotencyKey": True},
            expected={"committedCount": 1, "replayed": True},
            actual={
                "committedCount": len(timeout_notes),
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
        await self.create_subscription(
            self.crm,
            manager,
            "crm",
            "crm.note.created",
            "subscription-cleared-by-final-reset",
            self.failure_calls,
        )
        subscriptions_before_response = await self.client.get(
            f"{self.crm}/v1/webhook-subscriptions",
            headers=self.business_headers(manager, "case-subscriptions-before-reset"),
        )
        require_status(subscriptions_before_response, 200)
        subscriptions_before = list_body(subscriptions_before_response)
        if len(subscriptions_before) != 1:
            raise AssertionError("failure proof did not create one subscription before reset")

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
        old_token = await self.client.get(
            f"{self.identity}/v1/me",
            headers=self.business_headers(old_support, "case-old-token-after-reset"),
        )
        require_status(old_token, 401)
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
        if cast(list[JsonObject], object_body(empty_notes)["items"]):
            raise AssertionError("reset retained CRM notes")
        cleared_faults = await self.client.get(
            f"{self.control}/control/v1/fault-activations",
            headers=self.control_headers,
        )
        require_status(cleared_faults, 200)
        if list_body(cleared_faults):
            raise AssertionError("reset retained fault activations")
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
            operation="control.reset.contract",
            method="POST",
            path="/control/v1/reset",
            request_fields={"randomSeed": RANDOM_SEED},
            expected={
                "newEpoch": True,
                "sameChecksum": True,
                "oldTokenStatus": 401,
                "subscriptions": 0,
                "subscriptionsBeforeReset": 1,
                "notesBeforeReuse": 0,
                "faultActivations": 0,
                "restoredTime": INITIAL_TIME,
                "idempotencyReplayed": False,
                "subscriptionCreatedBeforeReset": True,
            },
            actual={
                "newEpoch": final_reset["scenarioEpoch"] != initial_reset["scenarioEpoch"],
                "sameChecksum": (
                    final_reset["manifestChecksum"] == initial_reset["manifestChecksum"]
                ),
                "oldTokenStatus": old_token.status_code,
                "subscriptions": 0,
                "subscriptionsBeforeReset": len(subscriptions_before),
                "notesBeforeReuse": 0,
                "faultActivations": 0,
                "restoredTime": status["now"],
                "idempotencyReplayed": reused.headers.get("Idempotency-Replayed") == "true",
                "subscriptionCreatedBeforeReset": True,
            },
            response_status=200,
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
                    "newEpoch": True,
                    "sameChecksum": True,
                    "oldTokenRejected": True,
                    "faultsCleared": True,
                    "subscriptionsCleared": True,
                    "notesClearedBeforeReuse": True,
                    "virtualTimeRestored": True,
                    "idempotencyKeyDidNotReplay": True,
                    "subscriptionCreatedBeforeReset": True,
                },
                "subscriptionCounts": {
                    "beforeReset": len(subscriptions_before),
                    "identityAfterReset": len(identity_subscriptions_after),
                    "crmAfterReset": len(crm_subscriptions_after),
                },
            },
        )

    def summarise(self, mode: str) -> None:
        required_by_mode = {
            "platform-success": {"successful-calls.json", "webhook-transcript.json"},
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
                "restart.json",
                "network.json",
            },
        }
        required = required_by_mode[mode]
        evidence = {name: self.read(name) for name in required}
        if mode in {"platform-success", "platform-contracts"}:
            successful = evidence_list(
                evidence["successful-calls.json"].get("calls"), "successful-call"
            )
            if not successful or not all(
                isinstance(item.get("assertion"), dict)
                and item["assertion"].get("outcome") == "passed"
                for item in successful
            ):
                raise AssertionError("successful-call evidence has an unproved assertion")
            webhook = evidence["webhook-transcript.json"]
            attempts = evidence_list(webhook.get("attempts"), "webhook attempt")
            outcomes = {item.get("outcome") for item in attempts}
            retry = evidence_object(webhook.get("identityRetry"), "identity retry")
            if (
                not {"unarmed", "accepted", "signature_rejected"}.issubset(outcomes)
                or retry.get("virtualAdvance") != "PT2S"
                or retry.get("sameEventId") is not True
                or retry.get("sameBodyHash") is not True
                or webhook.get("wrongSignatureRejected") is not True
            ):
                raise AssertionError("webhook evidence does not prove retry and signature checks")
        if mode in {"platform-failure", "platform-contracts"}:
            failures = evidence_list(evidence["failure-calls.json"].get("calls"), "failure-call")
            if not failures or not all(
                isinstance(item.get("assertion"), dict)
                and item["assertion"].get("outcome") == "passed"
                for item in failures
            ):
                raise AssertionError("failure-call evidence has an unproved assertion")
            activations = evidence_list(
                evidence["fault-activations.json"].get("activations"), "fault activation"
            )
            reset = evidence["reset-checksums.json"]
            before = evidence_object(reset.get("before"), "reset before")
            after = evidence_object(reset.get("after"), "reset after")
            reset_assertions = evidence_object(reset.get("assertions"), "reset assertion")
            subscription_counts = evidence_object(
                reset.get("subscriptionCounts"), "subscription count"
            )
            expected_reset_assertions = {
                "newEpoch",
                "sameChecksum",
                "oldTokenRejected",
                "faultsCleared",
                "subscriptionsCleared",
                "notesClearedBeforeReuse",
                "virtualTimeRestored",
                "idempotencyKeyDidNotReplay",
                "subscriptionCreatedBeforeReset",
            }
            if (
                len(activations) != 1
                or activations[0].get("ruleId") != "crm-note-timeout-once"
                or before.get("scenarioEpoch") == after.get("scenarioEpoch")
                or before.get("randomSeed") != RANDOM_SEED
                or after.get("randomSeed") != RANDOM_SEED
                or before.get("manifestChecksum") != after.get("manifestChecksum")
                or not reset_assertions
                or subscription_counts
                != {"beforeReset": 1, "identityAfterReset": 0, "crmAfterReset": 0}
                or not expected_reset_assertions.issubset(reset_assertions)
                or not all(value is True for value in reset_assertions.values())
            ):
                raise AssertionError("failure and reset evidence does not prove the contract")
        if mode == "platform-contracts":
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
