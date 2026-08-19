import json
from pathlib import Path

import pytest

from enterprise_twins.conformance.platform_contracts import (
    REQUIRED_SUCCESS_OPERATIONS,
    Driver,
    publish_latest,
)

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


def transcript_call(operation: str) -> dict[str, object]:
    result = {"status": 200}
    return {
        "sequence": 1,
        "operation": operation,
        "request": {"method": "GET", "path": "/proof", "fields": {}},
        "response": {"status": 200, "body": result, "error": None},
        "assertion": {"expected": result, "actual": result, "outcome": "passed"},
    }


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


def write_failure_evidence(
    root: Path,
    run_id: str,
    operations: set[str],
    reset: dict[str, object] | None = None,
) -> None:
    calls = []
    for sequence, operation in enumerate(sorted(operations), start=1):
        call = transcript_call(operation)
        call["sequence"] = sequence
        calls.append(call)
    write_run_file(root, run_id, "failure-calls.json", {"calls": calls})
    write_run_file(
        root,
        run_id,
        "fault-activations.json",
        {"activations": [{"ruleId": "crm-note-timeout-once"}]},
    )
    write_run_file(root, run_id, "reset-checksums.json", reset or old_reset_evidence())


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
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="transcript"):
        driver.summarise("platform-success")


def test_summary_rejects_success_without_cross_subscription_rejection(
    tmp_path: Path,
) -> None:
    run_id = "run-global-secret-could-pass"
    calls = []
    for sequence, operation in enumerate(sorted(REQUIRED_SUCCESS_OPERATIONS), start=1):
        call = transcript_call(operation)
        call["sequence"] = sequence
        calls.append(call)
    write_run_file(
        tmp_path,
        run_id,
        "successful-calls.json",
        {"calls": calls},
    )
    write_run_file(
        tmp_path,
        run_id,
        "webhook-transcript.json",
        {
            "attempts": [
                {"outcome": "unarmed"},
                {"outcome": "accepted"},
                {
                    "eventId": "evt_some_other_rejection",
                    "source": "crm",
                    "eventType": "crm.note.created",
                    "outcome": "signature_rejected",
                },
            ],
            "acceptedEvents": [],
            "identityRetry": {
                "virtualAdvance": "PT2S",
                "sameEventId": True,
                "sameBodyHash": True,
            },
            "crossSubscriptionRejected": True,
            "wrongSignatureRejected": True,
        },
    )
    driver = Driver.__new__(Driver)
    driver.run_id = run_id
    driver.artifacts = tmp_path

    with pytest.raises(AssertionError, match="cross-subscription"):
        driver.summarise("platform-success")


def test_summary_rejects_vacuous_reset_observations(tmp_path: Path) -> None:
    run_id = "run-vacuous-reset"
    write_failure_evidence(tmp_path, run_id, REQUIRED_FAILURE_OPERATIONS)
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
