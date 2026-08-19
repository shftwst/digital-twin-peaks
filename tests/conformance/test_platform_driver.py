import json
from copy import deepcopy
from pathlib import Path

import pytest

from enterprise_twins.conformance.platform_contracts import (
    Driver,
    check_no_sensitive_values,
    publish_latest,
)

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


def write_run_file(root: Path, run_id: str, name: str, body: dict[str, object]) -> None:
    (root / name).write_text(
        json.dumps({"runId": run_id} | body),
        encoding="utf-8",
    )


def read_run_body(root: Path, name: str) -> dict[str, object]:
    payload = json.loads((root / name).read_text(encoding="utf-8"))
    del payload["runId"]
    return payload


def transcript_call(operation: str) -> dict[str, object]:
    result = {"status": 200}
    return {
        "sequence": 1,
        "operation": operation,
        "request": {"method": "GET", "path": "/proof", "fields": {}},
        "response": {"status": 200, "body": result, "error": None},
        "assertion": {"expected": result, "actual": result, "outcome": "passed"},
    }


def proof_call(
    operation: str,
    method: str,
    path: str,
    *,
    request_fields: dict[str, object] | None = None,
    result: dict[str, object] | None = None,
    error: str | None = None,
    response_status: int | None = None,
) -> dict[str, object]:
    actual = result or {"status": 200}
    return {
        "sequence": 0,
        "operation": operation,
        "request": {"method": method, "path": path, "fields": request_fields or {}},
        "response": {
            "status": (
                None
                if error
                else response_status
                if response_status is not None
                else actual.get("status", 200)
            ),
            "body": None if error else actual,
            "error": error,
        },
        "assertion": {"expected": actual, "actual": actual, "outcome": "passed"},
    }


def numbered(calls: list[dict[str, object]]) -> list[dict[str, object]]:
    for sequence, call in enumerate(calls, start=1):
        call["sequence"] = sequence
    return calls


def valid_success_calls(*, cross_body_hash: str = "a" * 64) -> list[dict[str, object]]:
    calls = [
        proof_call(
            "control.reset",
            "POST",
            "/control/v1/reset",
            request_fields={
                "randomSeed": 7,
                "scenarioId": "platform-contracts",
                "version": 1,
            },
            result={
                "status": 200,
                "randomSeed": 7,
                "scenarioEpoch": "epoch-success",
                "manifestChecksum": "success-checksum",
            },
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "identity.subscription.create",
            "POST",
            "/v1/webhook-subscriptions",
            request_fields={
                "eventTypes": ["identity.token.issued"],
                "targetHost": "webhook-receiver",
            },
            result={"status": 201, "source": "identity"},
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "control.time.advance.identity-retry",
            "POST",
            "/control/v1/time/advance",
            request_fields={"duration": "PT2S"},
            result={"now": "2026-08-19T10:00:02Z"},
        ),
        proof_call(
            "relay.identity.retry",
            "POST",
            "/events",
            request_fields={
                "virtualAdvance": "PT2S",
                "eventId": "evt-identity-retry",
                "bodyHash": "e" * 64,
            },
            result={"sameEventId": True, "sameBodyHash": True},
            response_status=204,
        ),
        proof_call(
            "crm.subscription.create",
            "POST",
            "/v1/webhook-subscriptions",
            request_fields={
                "eventTypes": ["crm.note.created"],
                "targetHost": "webhook-receiver",
            },
            result={"status": 201, "source": "crm"},
        ),
        proof_call(
            "receiver.signature.cross-subscription-reject",
            "POST",
            "/events",
            request_fields={
                "eventId": "evt_cross_subscription_signature",
                "bodyHash": cross_body_hash,
                "envelopeSource": "crm",
                "eventType": "crm.note.created",
                "signingSecretSource": "identity",
            },
            result={
                "status": 401,
                "error": {"code": "unauthenticated"},
                "accepted": False,
            },
        ),
        proof_call(
            "receiver.signature.zero-reject",
            "POST",
            "/events",
            request_fields={
                "eventId": "evt_deliberate_bad_signature",
                "source": "crm",
                "eventType": "crm.note.created",
                "bodyHash": "f" * 64,
            },
            result={"status": 401, "accepted": False},
        ),
        proof_call(
            "identity.me",
            "GET",
            "/v1/me",
            result={"subject": "person-support-1"},
        ),
        proof_call(
            "crm.customer.search",
            "GET",
            "/v1/customers",
            request_fields={"emailHash": "0" * 64},
            result={"customerIds": ["cus_unique"]},
        ),
        proof_call(
            "crm.customer.get",
            "GET",
            "/v1/customers/cus_unique",
            result={"customerId": "cus_unique", "version": 1},
        ),
        proof_call(
            "crm.note.create",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={
                "association": "account",
                "expectedVersion": 1,
                "bodyHash": "1" * 64,
            },
            result={"status": 201, "replayed": False, "noteId": "note-saved"},
        ),
        proof_call(
            "crm.note.replay",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"sameIdempotencyKey": True, "sameBodyHash": True},
            result={"sameNoteId": True, "replayed": True},
            response_status=201,
        ),
    ]
    return numbered(calls)


def valid_failure_calls() -> list[dict[str, object]]:
    calls = [
        proof_call(
            "control.reset",
            "POST",
            "/control/v1/reset",
            request_fields={
                "randomSeed": 7,
                "scenarioId": "platform-contracts",
                "version": 1,
            },
            result={
                "status": 200,
                "randomSeed": 7,
                "scenarioEpoch": "epoch-before",
                "manifestChecksum": "checksum",
            },
        ),
        proof_call(
            "control.status.initial",
            "GET",
            "/control/v1/status",
            result={
                "status": 200,
                "now": "2026-08-19T10:00:00Z",
                "randomSeed": 7,
                "scenarioEpoch": "epoch-before",
                "manifestChecksum": "checksum",
            },
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "auth.missing",
            "GET",
            "/v1/customers",
            result={"status": 401, "error": {"code": "unauthenticated"}},
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "auth.read-only-read",
            "GET",
            "/v1/customers/cus_unique",
            request_fields={"actorClass": "read-only"},
            result={"status": 200, "customerId": "cus_unique"},
        ),
        proof_call(
            "auth.read-only-write",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"actorClass": "read-only", "expectedVersion": 1},
            result={"status": 403, "error": {"code": "forbidden"}},
        ),
        proof_call(
            "crm.notes.after-read-only-denial",
            "GET",
            "/v1/customers/cus_unique/notes",
            request_fields={"rejectedBodyHash": "b" * 64},
            result={"noteCount": 0, "rejectedBodyPresent": False},
        ),
        proof_call(
            "crm.search.ambiguous",
            "GET",
            "/v1/customers",
            request_fields={"emailHash": "2" * 64},
            result={"customerIds": ["cus_ambiguous_a", "cus_ambiguous_b"]},
        ),
        proof_call(
            "crm.note.precondition.stale",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 0},
            result={"status": 409, "error": {"code": "conflict"}},
        ),
        proof_call(
            "crm.note.create.idempotency-baseline",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={
                "keyLabel": "reused-after-reset",
                "expectedVersion": 1,
                "bodyHash": "c" * 64,
            },
            result={"status": 201, "replayed": False},
        ),
        proof_call(
            "crm.note.idempotency-mismatch",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "bodyHash": "d" * 64},
            result={"status": 409, "error": {"code": "conflict"}},
        ),
        proof_call(
            "crm.notes.after-idempotency-denial",
            "GET",
            "/v1/customers/cus_unique/notes",
            request_fields={
                "validNoteId": "note_valid",
                "forbiddenBodyHash": "b" * 64,
                "changedBodyHash": "d" * 64,
            },
            result={
                "noteCount": 1,
                "validNoteCount": 1,
                "validNoteIdMatches": True,
                "forbiddenBodyPresent": False,
                "changedBodyPresent": False,
            },
        ),
        proof_call(
            "webhook.target.denied",
            "POST",
            "/v1/webhook-subscriptions",
            request_fields={"targetHost": "127.0.0.1"},
            result={"status": 422, "error": {"code": "invalid_request"}},
        ),
        proof_call(
            "control.fault.create",
            "POST",
            "/control/v1/faults",
            request_fields={
                "ruleId": "crm-note-timeout-once",
                "targetService": "crm",
                "phase": "after_commit",
                "delayMs": 500,
            },
            result={"status": 201, "ruleId": "crm-note-timeout-once"},
        ),
        proof_call(
            "crm.customer.before-timeout",
            "GET",
            "/v1/customers/cus_unique",
            result={"status": 200, "customerId": "cus_unique"},
        ),
        proof_call(
            "crm.note.after-commit-timeout",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={
                "bodyHash": "e" * 64,
                "clientReadTimeoutMs": 100,
                "serverDelayMs": 500,
            },
            result={"error": "ReadTimeout"},
            error="ReadTimeout",
        ),
        proof_call(
            "crm.note.timeout.read",
            "GET",
            "/v1/customers/cus_unique/notes",
            request_fields={"bodyHash": "e" * 64},
            result={"status": 200, "noteCount": 2, "timeoutNoteCount": 1},
        ),
        proof_call(
            "crm.note.timeout.replay",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"sameIdempotencyKey": True},
            result={"status": 201, "sameNoteId": True, "replayed": True},
        ),
        proof_call(
            "control.fault-activations.before-reset",
            "GET",
            "/control/v1/fault-activations",
            result={
                "status": 200,
                "activationCount": 1,
                "ruleIds": ["crm-note-timeout-once"],
            },
        ),
        proof_call(
            "identity.subscription.create",
            "POST",
            "/v1/webhook-subscriptions",
            request_fields={
                "eventTypes": ["identity.token.issued"],
                "targetHost": "webhook-receiver",
            },
            result={"status": 201, "source": "identity"},
        ),
        proof_call(
            "crm.subscription.create",
            "POST",
            "/v1/webhook-subscriptions",
            request_fields={
                "eventTypes": ["crm.note.created"],
                "targetHost": "webhook-receiver",
            },
            result={"status": 201, "source": "crm"},
        ),
        proof_call(
            "identity.subscriptions.before-reset",
            "GET",
            "/v1/webhook-subscriptions",
            request_fields={"source": "identity"},
            result={"status": 200, "subscriptionCount": 1},
        ),
        proof_call(
            "crm.subscriptions.before-reset",
            "GET",
            "/v1/webhook-subscriptions",
            request_fields={"source": "crm"},
            result={"status": 200, "subscriptionCount": 1},
        ),
        proof_call(
            "control.time.advance.pre-reset",
            "POST",
            "/control/v1/time/advance",
            request_fields={"duration": "PT5M"},
            result={"status": 200, "now": "2026-08-19T10:05:00Z"},
        ),
        proof_call(
            "control.reset",
            "POST",
            "/control/v1/reset",
            request_fields={
                "randomSeed": 7,
                "scenarioId": "platform-contracts",
                "version": 1,
            },
            result={
                "status": 200,
                "randomSeed": 7,
                "scenarioEpoch": "epoch-after",
                "manifestChecksum": "checksum",
            },
        ),
        proof_call(
            "control.status.after-reset",
            "GET",
            "/control/v1/status",
            result={
                "status": 200,
                "now": "2026-08-19T10:00:00Z",
                "randomSeed": 7,
                "scenarioEpoch": "epoch-after",
                "manifestChecksum": "checksum",
            },
        ),
        proof_call(
            "identity.old-token.after-reset",
            "GET",
            "/v1/me",
            request_fields={"tokenEpoch": "before-reset"},
            result={"status": 401, "error": {"code": "unauthenticated"}},
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "identity.subscriptions.after-reset",
            "GET",
            "/v1/webhook-subscriptions",
            request_fields={"source": "identity"},
            result={"status": 200, "subscriptionCount": 0},
        ),
        proof_call(
            "crm.subscriptions.after-reset",
            "GET",
            "/v1/webhook-subscriptions",
            request_fields={"source": "crm"},
            result={"status": 200, "subscriptionCount": 0},
        ),
        proof_call(
            "identity.token.issue",
            "POST",
            "/oauth/token",
            result={"status": 200, "tokenIssued": True},
        ),
        proof_call(
            "crm.notes.after-reset",
            "GET",
            "/v1/customers/cus_unique/notes",
            result={"status": 200, "noteCount": 0},
        ),
        proof_call(
            "control.fault-activations.after-reset",
            "GET",
            "/control/v1/fault-activations",
            result={"status": 200, "activationCount": 0},
        ),
        proof_call(
            "crm.note.idempotency-reuse.after-reset",
            "POST",
            "/v1/customers/cus_unique/notes",
            request_fields={"keyLabel": "reused-after-reset", "expectedVersion": 1},
            result={"status": 201, "replayed": False},
        ),
    ]
    return numbered(calls)


def old_reset_evidence() -> dict[str, object]:
    return {
        "before": {"scenarioEpoch": "epoch-before", "randomSeed": 7, "manifestChecksum": "x"},
        "after": {"scenarioEpoch": "epoch-after", "randomSeed": 7, "manifestChecksum": "x"},
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
            "beforeReset": 1,
            "identityAfterReset": 0,
            "crmAfterReset": 0,
        },
    }


def valid_reset_evidence() -> dict[str, object]:
    return {
        "before": {
            "scenarioEpoch": "epoch-before",
            "randomSeed": 7,
            "manifestChecksum": "checksum",
        },
        "after": {
            "scenarioEpoch": "epoch-after",
            "randomSeed": 7,
            "manifestChecksum": "checksum",
            "storedRandomSeed": 7,
            "storedManifestChecksum": "checksum",
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
            "identitySubscriptionObservedBeforeReset": True,
            "crmSubscriptionObservedBeforeReset": True,
        },
        "subscriptionCounts": {
            "identityBeforeReset": 1,
            "crmBeforeReset": 1,
            "identityAfterReset": 0,
            "crmAfterReset": 0,
        },
        "virtualTime": {
            "initial": "2026-08-19T10:00:00Z",
            "beforeReset": "2026-08-19T10:05:00Z",
            "afterReset": "2026-08-19T10:00:00Z",
        },
    }


def write_failure_evidence(
    root: Path,
    run_id: str,
    operations: set[str],
    reset: dict[str, object] | None = None,
) -> None:
    calls = numbered([call for call in valid_failure_calls() if call["operation"] in operations])
    write_run_file(root, run_id, "failure-calls.json", {"calls": calls})
    write_run_file(
        root,
        run_id,
        "fault-activations.json",
        {"activations": [{"ruleId": "crm-note-timeout-once"}]},
    )
    write_run_file(root, run_id, "reset-checksums.json", reset or valid_reset_evidence())


def write_success_evidence(
    root: Path,
    run_id: str,
    *,
    transcript_body_hash: str = "a" * 64,
    attempt_body_hash: str = "a" * 64,
    retry_accepted_body_hash: str = "e" * 64,
    prepare_note_id: str = "note-saved",
) -> None:
    write_run_file(
        root,
        run_id,
        "successful-calls.json",
        {"calls": valid_success_calls(cross_body_hash=transcript_body_hash)},
    )
    write_run_file(
        root,
        run_id,
        "webhook-transcript.json",
        {
            "attempts": [
                {
                    "eventId": "evt-identity-retry",
                    "source": "identity",
                    "eventType": "identity.token.issued",
                    "correlationId": "case-identity-unarmed-retry",
                    "bodyHash": "e" * 64,
                    "outcome": "unarmed",
                    "responseStatus": 503,
                },
                {
                    "eventId": "evt-identity-retry",
                    "source": "identity",
                    "eventType": "identity.token.issued",
                    "correlationId": "case-identity-unarmed-retry",
                    "bodyHash": retry_accepted_body_hash,
                    "outcome": "accepted",
                    "responseStatus": 204,
                },
                {
                    "eventId": "evt_cross_subscription_signature",
                    "source": "crm",
                    "eventType": "crm.note.created",
                    "bodyHash": attempt_body_hash,
                    "outcome": "signature_rejected",
                    "responseStatus": 401,
                },
                {
                    "eventId": "evt_deliberate_bad_signature",
                    "source": "crm",
                    "eventType": "crm.note.created",
                    "correlationId": "case-bad-signature",
                    "bodyHash": "f" * 64,
                    "outcome": "signature_rejected",
                    "responseStatus": 401,
                },
                {
                    "eventId": "evt-crm-note",
                    "source": "crm",
                    "eventType": "crm.note.created",
                    "correlationId": "case-platform-success",
                    "bodyHash": "9" * 64,
                    "outcome": "accepted",
                    "responseStatus": 204,
                },
            ],
            "acceptedEvents": [
                {
                    "eventId": "evt-identity-retry",
                    "source": "identity",
                    "eventType": "identity.token.issued",
                    "correlationId": "case-identity-unarmed-retry",
                    "bodyHash": "e" * 64,
                    "outcome": "accepted",
                    "responseStatus": 204,
                    "signatureValid": True,
                },
                {
                    "eventId": "evt-crm-note",
                    "source": "crm",
                    "eventType": "crm.note.created",
                    "correlationId": "case-platform-success",
                    "bodyHash": "9" * 64,
                    "outcome": "accepted",
                    "responseStatus": 204,
                    "signatureValid": True,
                },
            ],
            "identityRetry": {
                "virtualAdvance": "PT2S",
                "sameEventId": True,
                "sameBodyHash": True,
            },
            "crossSubscriptionRejected": True,
            "zeroSignatureRejected": True,
            "wrongSignatureRejected": True,
        },
    )
    write_run_file(
        root,
        run_id,
        "prepare-state.json",
        {
            "scenarioEpoch": "epoch-success",
            "manifestChecksum": "success-checksum",
            "noteId": prepare_note_id,
        },
    )


def valid_restart_calls(*, expected_note_id: str = "note-saved") -> list[dict[str, object]]:
    return numbered(
        [
            proof_call(
                "identity.token.issue",
                "POST",
                "/oauth/token",
                result={"status": 200, "tokenIssued": True},
            ),
            proof_call(
                "crm.notes.after-restart",
                "GET",
                "/v1/customers/cus_unique/notes",
                request_fields={"expectedNoteId": expected_note_id},
                result={"status": 200, "noteCount": 1, "savedNotePresent": True},
            ),
        ]
    )


def write_full_evidence(
    root: Path,
    run_id: str,
    *,
    restart_note_id: str = "note-saved",
) -> None:
    write_success_evidence(root, run_id)
    write_failure_evidence(root, run_id, REQUIRED_FAILURE_OPERATIONS)
    write_run_file(
        root,
        run_id,
        "restart-calls.json",
        {"calls": valid_restart_calls()},
    )
    write_run_file(
        root,
        run_id,
        "restart.json",
        {
            "before": {"containerId": "container", "startedAt": "before"},
            "after": {"containerId": "container", "startedAt": "after"},
            "publicState": {"noteId": restart_note_id, "survived": True},
        },
    )
    write_run_file(
        root,
        run_id,
        "network.json",
        {"assertions": {"conformanceIsolated": True}},
    )


def test_summary_rejects_run_bound_files_with_unproved_assertions(tmp_path: Path) -> None:
    run_id = "run-invalid"
    for name in (
        "successful-calls.json",
        "failure-calls.json",
        "webhook-transcript.json",
        "reset-checksums.json",
        "fault-activations.json",
        "restart-calls.json",
        "restart.json",
        "network.json",
        "prepare-state.json",
    ):
        write_run_file(tmp_path, run_id, name, {})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="evidence"):
        driver.summarise("platform-contracts")

    assert not (tmp_path / "summary.json").exists()


def test_latest_pointer_is_published_only_for_a_passed_matching_run(tmp_path: Path) -> None:
    run_id = "run-publish"
    run_root = tmp_path / "runs" / run_id
    run_root.mkdir(parents=True)
    write_run_file(run_root, "different-run", "summary.json", {"status": "passed"})

    with pytest.raises(AssertionError, match="passed run-bound summary"):
        publish_latest(tmp_path, run_root, run_id)

    assert not (tmp_path / "latest-run.json").exists()


def test_transcript_rejects_aggregate_pseudo_methods() -> None:
    driver = Driver.__new__(Driver)
    calls: list[dict[str, object]] = []

    with pytest.raises(AssertionError, match="HTTP method"):
        driver.record(
            calls,
            operation="composite-proof",
            method="GET+POST",
            path="/proof",
            request_fields={},
            expected={},
            actual={},
        )

    assert calls == []


def test_transcript_rejects_expected_actual_mismatch() -> None:
    driver = Driver.__new__(Driver)
    calls: list[dict[str, object]] = []

    with pytest.raises(AssertionError, match="expected result"):
        driver.record(
            calls,
            operation="direct-mismatch-reproduction",
            method="GET",
            path="/proof",
            request_fields={},
            expected={"value": 1},
            actual={"value": 2},
            response_status=200,
        )

    assert calls == []


def test_summary_rejects_structurally_incomplete_transcript(tmp_path: Path) -> None:
    run_id = "run-incomplete-transcript"
    write_run_file(
        tmp_path,
        run_id,
        "successful-calls.json",
        {"calls": [{"assertion": {"outcome": "passed"}}]},
    )
    write_run_file(
        tmp_path,
        run_id,
        "webhook-transcript.json",
        {
            "attempts": [
                {"outcome": "unarmed"},
                {"outcome": "accepted"},
                {"outcome": "signature_rejected"},
            ],
            "identityRetry": {
                "virtualAdvance": "PT2S",
                "sameEventId": True,
                "sameBodyHash": True,
            },
            "wrongSignatureRejected": True,
        },
    )
    write_run_file(tmp_path, run_id, "prepare-state.json", {})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="transcript"):
        driver.summarise("platform-success")


def test_summary_rejects_success_without_cross_subscription_rejection(
    tmp_path: Path,
) -> None:
    run_id = "run-global-secret-could-pass"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    attempts = webhook["attempts"]
    assert isinstance(attempts, list)
    cross_attempt = next(
        attempt for attempt in attempts if attempt["eventId"] == "evt_cross_subscription_signature"
    )
    cross_attempt["eventId"] = "evt_some_other_rejection"
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="cross-subscription"):
        driver.summarise("platform-success")


def test_summary_rejects_vacuous_reset_observations(tmp_path: Path) -> None:
    run_id = "run-vacuous-reset"
    write_failure_evidence(
        tmp_path,
        run_id,
        REQUIRED_FAILURE_OPERATIONS,
        reset=old_reset_evidence(),
    )
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="observed reset"):
        driver.summarise("platform-failure")


def test_summary_requires_unchanged_state_reads_after_rejected_writes(
    tmp_path: Path,
) -> None:
    run_id = "run-missing-unchanged-state"
    operations = REQUIRED_FAILURE_OPERATIONS - {
        "crm.notes.after-read-only-denial",
        "crm.notes.after-idempotency-denial",
    }
    write_failure_evidence(tmp_path, run_id, operations)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="unchanged-state"):
        driver.summarise("platform-failure")


def test_summary_requires_both_failure_resets(tmp_path: Path) -> None:
    run_id = "run-missing-final-reset"
    calls = valid_failure_calls()
    second_reset = [
        index for index, call in enumerate(calls) if call["operation"] == "control.reset"
    ][1]
    del calls[second_reset]
    numbered(calls)
    write_failure_evidence(tmp_path, run_id, REQUIRED_FAILURE_OPERATIONS)
    write_run_file(tmp_path, run_id, "failure-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"control\.reset"):
        driver.summarise("platform-failure")


def test_summary_rejects_duplicated_first_reset_as_final_reset(tmp_path: Path) -> None:
    run_id = "run-duplicated-first-reset"
    calls = valid_failure_calls()
    reset_indexes = [
        index for index, call in enumerate(calls) if call["operation"] == "control.reset"
    ]
    second_sequence = calls[reset_indexes[1]]["sequence"]
    calls[reset_indexes[1]] = deepcopy(calls[reset_indexes[0]])
    calls[reset_indexes[1]]["sequence"] = second_sequence
    write_failure_evidence(tmp_path, run_id, REQUIRED_FAILURE_OPERATIONS)
    write_run_file(tmp_path, run_id, "failure-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"reset transcript.*evidence"):
        driver.summarise("platform-failure")


def test_summary_rejects_success_operations_out_of_workflow_order(tmp_path: Path) -> None:
    run_id = "run-reordered-success"
    calls = valid_success_calls()
    identity_me = next(
        index for index, call in enumerate(calls) if call["operation"] == "identity.me"
    )
    customer_search = next(
        index for index, call in enumerate(calls) if call["operation"] == "crm.customer.search"
    )
    calls[identity_me], calls[customer_search] = calls[customer_search], calls[identity_me]
    numbered(calls)
    write_success_evidence(tmp_path, run_id)
    write_run_file(tmp_path, run_id, "successful-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="workflow order"):
        driver.summarise("platform-success")


def test_summary_rejects_semantically_empty_unchanged_state_records(
    tmp_path: Path,
) -> None:
    run_id = "run-substituted-state-reads"
    calls = valid_failure_calls()
    for index, call in enumerate(calls):
        if call["operation"] in {
            "crm.notes.after-read-only-denial",
            "crm.notes.after-idempotency-denial",
        }:
            replacement = transcript_call(str(call["operation"]))
            replacement["sequence"] = call["sequence"]
            calls[index] = replacement
    write_failure_evidence(tmp_path, run_id, REQUIRED_FAILURE_OPERATIONS)
    write_run_file(tmp_path, run_id, "failure-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="operation contract"):
        driver.summarise("platform-failure")


def test_summary_binds_cross_subscription_attempt_to_transcript_body(
    tmp_path: Path,
) -> None:
    run_id = "run-unbound-cross-attempt"
    write_success_evidence(
        tmp_path,
        run_id,
        transcript_body_hash="1" * 64,
        attempt_body_hash="0" * 64,
    )
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"cross-subscription.*transcript"):
        driver.summarise("platform-success")


def test_summary_binds_identity_retry_attempt_to_transcript_body(tmp_path: Path) -> None:
    run_id = "run-unbound-identity-retry"
    write_success_evidence(
        tmp_path,
        run_id,
        retry_accepted_body_hash="d" * 64,
    )
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(
        AssertionError,
        match=r"Identity retry.*transcript|accepted webhook event.*attempt",
    ):
        driver.summarise("platform-success")


def test_summary_rejects_accepted_event_with_different_body_hash(tmp_path: Path) -> None:
    run_id = "run-accepted-event-wrong-body"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    accepted_events = webhook["acceptedEvents"]
    assert isinstance(accepted_events, list)
    identity_event = next(
        event for event in accepted_events if event["eventId"] == "evt-identity-retry"
    )
    identity_event["bodyHash"] = "0" * 64
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"accepted (webhook event|attempt).*"):
        driver.summarise("platform-success")


def test_summary_rejects_accepted_event_with_invalid_signature(tmp_path: Path) -> None:
    run_id = "run-accepted-event-invalid-signature"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    accepted_events = webhook["acceptedEvents"]
    assert isinstance(accepted_events, list)
    identity_event = next(
        event for event in accepted_events if event["eventId"] == "evt-identity-retry"
    )
    identity_event["signatureValid"] = False
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"accepted webhook event.*signature"):
        driver.summarise("platform-success")


def test_summary_requires_accepted_crm_note_event(tmp_path: Path) -> None:
    run_id = "run-missing-crm-accepted-event"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    accepted_events = webhook["acceptedEvents"]
    assert isinstance(accepted_events, list)
    webhook["acceptedEvents"] = [
        event for event in accepted_events if event.get("correlationId") != "case-platform-success"
    ]
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"accepted attempt.*event|CRM.*accepted event"):
        driver.summarise("platform-success")


def test_summary_rejects_duplicate_accepted_event(tmp_path: Path) -> None:
    run_id = "run-duplicate-accepted-event"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    accepted_events = webhook["acceptedEvents"]
    assert isinstance(accepted_events, list)
    accepted_events.append(deepcopy(accepted_events[0]))
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"accepted (webhook event|attempt).*"):
        driver.summarise("platform-success")


def test_summary_rejects_unrelated_accepted_event(tmp_path: Path) -> None:
    run_id = "run-unrelated-accepted-event"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    accepted_events = webhook["acceptedEvents"]
    assert isinstance(accepted_events, list)
    accepted_events.append(
        {
            "eventId": "evt-unrelated",
            "source": "crm",
            "eventType": "crm.note.created",
            "correlationId": "case-unrelated",
            "bodyHash": "8" * 64,
            "outcome": "accepted",
            "responseStatus": 204,
            "signatureValid": True,
        }
    )
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"accepted webhook event.*attempt"):
        driver.summarise("platform-success")


def test_summary_requires_zero_signature_rejection_flag(tmp_path: Path) -> None:
    run_id = "run-zero-signature-flag-false"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    webhook["zeroSignatureRejected"] = False
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"signature checks"):
        driver.summarise("platform-success")


def test_summary_binds_zero_signature_attempt_to_transcript(tmp_path: Path) -> None:
    run_id = "run-unbound-zero-signature-attempt"
    write_success_evidence(tmp_path, run_id)
    webhook = read_run_body(tmp_path, "webhook-transcript.json")
    attempts = webhook["attempts"]
    assert isinstance(attempts, list)
    bad_signature_attempt = next(
        attempt for attempt in attempts if attempt["eventId"] == "evt_deliberate_bad_signature"
    )
    bad_signature_attempt["bodyHash"] = "7" * 64
    write_run_file(tmp_path, run_id, "webhook-transcript.json", webhook)
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"zero-signature.*transcript"):
        driver.summarise("platform-success")


def test_summary_binds_prepare_state_to_success_transcript(tmp_path: Path) -> None:
    run_id = "run-unbound-prepare-state"
    write_success_evidence(tmp_path, run_id, prepare_note_id="note-other")
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"prepare state.*transcript"):
        driver.summarise("platform-success")


def test_summary_binds_fault_activation_file_to_transcript(tmp_path: Path) -> None:
    run_id = "run-unbound-fault-activation"
    calls = valid_failure_calls()
    activation_call = next(
        call for call in calls if call["operation"] == "control.fault-activations.before-reset"
    )
    contradictory = {
        "status": 200,
        "activationCount": 1,
        "ruleIds": ["different-rule"],
    }
    activation_call["response"] = {
        "status": 200,
        "body": contradictory,
        "error": None,
    }
    activation_call["assertion"] = {
        "expected": contradictory,
        "actual": contradictory,
        "outcome": "passed",
    }
    write_failure_evidence(tmp_path, run_id, REQUIRED_FAILURE_OPERATIONS)
    write_run_file(tmp_path, run_id, "failure-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"fault activation.*transcript"):
        driver.summarise("platform-failure")


def test_summary_binds_restart_public_state_to_transcript(tmp_path: Path) -> None:
    run_id = "run-unbound-restart-state"
    write_full_evidence(tmp_path, run_id, restart_note_id="note-other")
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"restart state.*transcript"):
        driver.summarise("platform-contracts")


def test_summary_binds_pre_reset_virtual_time_to_advance_transcript(tmp_path: Path) -> None:
    run_id = "run-unbound-pre-reset-time"
    reset = valid_reset_evidence()
    virtual_time = reset["virtualTime"]
    assert isinstance(virtual_time, dict)
    virtual_time["beforeReset"] = "2026-08-19T10:06:00Z"
    write_failure_evidence(
        tmp_path,
        run_id,
        REQUIRED_FAILURE_OPERATIONS,
        reset=reset,
    )
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"time advance.*reset evidence"):
        driver.summarise("platform-failure")


def test_summary_binds_operation_contract_to_response_status(tmp_path: Path) -> None:
    run_id = "run-wrong-response-status"
    calls = valid_success_calls()
    identity_me = next(call for call in calls if call["operation"] == "identity.me")
    response = identity_me["response"]
    assert isinstance(response, dict)
    response["status"] = 201
    write_success_evidence(tmp_path, run_id)
    write_run_file(tmp_path, run_id, "successful-calls.json", {"calls": calls})
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match=r"identity\.me operation contract"):
        driver.summarise("platform-success")


def test_redaction_rejects_common_token_key_variants() -> None:
    for key in (
        "token",
        "accessToken",
        "access_token",
        "clientSecret",
        "refresh-token",
        "apiToken",
        "auth_token",
        "sessionToken",
        "jwt",
    ):
        with pytest.raises(AssertionError, match="forbidden key"):
            check_no_sensitive_values({key: "synthetic-non-secret"})


def test_redaction_rejects_embedded_bearer_values() -> None:
    with pytest.raises(AssertionError, match="credential value"):
        check_no_sensitive_values({"message": "prefix Bearer synthetic-token suffix"})


def test_redaction_rejects_jwt_shaped_values() -> None:
    synthetic_jwt = (
        "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJub25lIn0.c3ludGhldGljLXNpZ25hdHVyZQ"
    )

    with pytest.raises(AssertionError, match="credential value"):
        check_no_sensitive_values({"message": f"prefix {synthetic_jwt} suffix"})


def test_redaction_rejects_known_credential_values_when_embedded() -> None:
    with pytest.raises(AssertionError, match="credential value"):
        check_no_sensitive_values({"message": "prefix-controller-local-token-suffix"})


def test_redaction_accepts_dotted_operation_names_that_are_not_jwts() -> None:
    check_no_sensitive_values({"operation": "receiver.signature.cross-subscription-reject"})


@pytest.mark.asyncio
async def test_driver_uses_an_empty_host_created_run_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = tmp_path / "runs" / "run-host-owned"
    run_root.mkdir(parents=True)
    environment = {
        "IDENTITY_URL": "http://identity:8000",
        "CRM_URL": "http://crm:8000",
        "CONTROL_URL": "http://control:8000",
        "RECEIVER_URL": "http://webhook-receiver:8080",
        "CONFORMANCE_RUN_ID": "run-host-owned",
        "ARTIFACT_ROOT": str(run_root),
        "CONTROL_TOKEN": "test-control-token",
        "RECEIVER_TOKEN": "test-receiver-token",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    driver = Driver()
    await driver.close()

    assert driver.artifacts == run_root
