import json
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from enterprise_twins.services.control import cli


def install_http_boundary(monkeypatch: pytest.MonkeyPatch, requests: list[httpx.Request]) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handle)

    def make_client() -> httpx.Client:
        return httpx.Client(
            base_url="http://control",
            headers={"Authorization": "Bearer controller-token"},
            transport=transport,
        )

    monkeypatch.setattr(cli, "client", make_client)


@pytest.mark.parametrize(
    "invalid_token",
    ["embedded whitespace", "non-ascii-é", "padding=inside"],
)
def test_control_cli_rejects_an_invalid_configured_token_before_http_construction(
    monkeypatch: pytest.MonkeyPatch,
    invalid_token: str,
) -> None:
    monkeypatch.setenv("TWINS_CONTROL_CONTROLLER_TOKEN", invalid_token)

    def unexpected_client(*_args: object, **_kwargs: object) -> httpx.Client:
        raise AssertionError("HTTP client was constructed")

    monkeypatch.setattr(cli.httpx, "Client", unexpected_client)

    with pytest.raises(ValueError, match="credential"):
        cli.client()


def test_reset_and_status_commands_use_controller_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    install_http_boundary(monkeypatch, requests)
    runner = CliRunner()

    reset = runner.invoke(
        cli.app,
        ["reset", "platform-contracts", "--version", "2", "--random-seed", "7"],
    )
    status = runner.invoke(cli.app, ["status"])

    assert reset.exit_code == 0
    assert status.exit_code == 0
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/control/v1/reset"),
        ("GET", "/control/v1/status"),
    ]
    assert json.loads(requests[0].content) == {
        "scenarioId": "platform-contracts",
        "version": 2,
        "randomSeed": 7,
    }
    assert all(
        request.headers["Authorization"] == "Bearer controller-token" for request in requests
    )


def test_time_advance_and_fault_apply_commands_send_expected_bodies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []
    install_http_boundary(monkeypatch, requests)
    rule = {"ruleId": "timeout", "effect": "timeout"}
    rule_file = tmp_path / "fault.json"
    rule_file.write_text(json.dumps(rule), encoding="utf-8")
    runner = CliRunner()

    advanced = runner.invoke(cli.app, ["time", "advance", "PT5M"])
    applied = runner.invoke(cli.app, ["faults", "apply", str(rule_file)])

    assert advanced.exit_code == 0
    assert applied.exit_code == 0
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/control/v1/time/advance"),
        ("POST", "/control/v1/faults"),
    ]
    assert json.loads(requests[0].content) == {"duration": "PT5M"}
    assert json.loads(requests[1].content) == rule
    assert requests[1].headers["Content-Type"] == "application/json"
