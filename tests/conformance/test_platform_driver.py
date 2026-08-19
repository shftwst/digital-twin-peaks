import json
from pathlib import Path

import pytest

from enterprise_twins.conformance.platform_contracts import Driver, publish_latest


def write_run_file(root: Path, run_id: str, name: str, body: dict[str, object]) -> None:
    (root / name).write_text(
        json.dumps({"runId": run_id} | body),
        encoding="utf-8",
    )


def test_summary_rejects_run_bound_files_with_unproved_assertions(tmp_path: Path) -> None:
    run_id = "run-invalid"
    for name in (
        "successful-calls.json",
        "failure-calls.json",
        "webhook-transcript.json",
        "reset-checksums.json",
        "fault-activations.json",
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
