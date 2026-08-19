import json
import os
import subprocess
from pathlib import Path

import pytest


def test_conformance_wrapper_exposes_independent_platform_modes() -> None:
    script = Path("scripts/conformance")
    assert script.is_file()
    contents = script.read_text(encoding="utf-8")
    assert "platform-success" in contents
    assert "platform-failure" in contents
    assert "platform-contracts" in contents


@pytest.mark.integration
def test_platform_contract_script_exports_run_bound_success_and_failure_evidence() -> None:
    if os.environ.get("RUN_PLATFORM_CONFORMANCE") != "1":
        pytest.skip("set RUN_PLATFORM_CONFORMANCE=1 to run the Compose conformance proof")

    result = subprocess.run(
        ["./scripts/conformance", "platform-contracts"],
        check=False,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    root = Path("artifacts/platform-contracts")
    pointer = json.loads((root / "latest-run.json").read_text(encoding="utf-8"))
    run_id = pointer["runId"]
    run_root = root / "runs" / run_id
    assert pointer["artifactRoot"] == f"runs/{run_id}"
    summary = json.loads((run_root / "summary.json").read_text(encoding="utf-8"))
    assert summary == {
        "runId": run_id,
        "status": "passed",
        "successfulSequence": "passed",
        "failureSequence": "passed",
        "restartPersistence": "passed",
        "webhookSignatures": "passed",
        "resetContract": "passed",
        "networkIsolation": "passed",
    }

    evidence_files = {
        "successful-calls.json",
        "failure-calls.json",
        "webhook-transcript.json",
        "reset-checksums.json",
        "fault-activations.json",
        "restart.json",
        "network.json",
    }
    for name in evidence_files:
        artefact = json.loads((run_root / name).read_text(encoding="utf-8"))
        assert artefact["runId"] == run_id
    faults = json.loads((run_root / "fault-activations.json").read_text(encoding="utf-8"))
    assert len(faults["activations"]) == 1
