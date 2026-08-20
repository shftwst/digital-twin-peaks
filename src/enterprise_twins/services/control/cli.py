import os
from pathlib import Path

import httpx
import typer

from enterprise_twins.common.auth.credentials import validate_private_credential

app = typer.Typer(no_args_is_help=True)
time_app = typer.Typer()
faults_app = typer.Typer()
app.add_typer(time_app, name="time")
app.add_typer(faults_app, name="faults")


def client() -> httpx.Client:
    token = validate_private_credential(os.environ["TWINS_CONTROL_CONTROLLER_TOKEN"])
    return httpx.Client(
        base_url=os.environ.get("TWINS_CONTROL_CLI_URL", "http://127.0.0.1:8000"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )


@app.command()
def reset(scenario_id: str, version: int = 1, random_seed: int | None = None) -> None:
    response = client().post(
        "/control/v1/reset",
        json={"scenarioId": scenario_id, "version": version, "randomSeed": random_seed},
    )
    response.raise_for_status()
    typer.echo(response.text)


@time_app.command("advance")
def advance(duration: str) -> None:
    response = client().post("/control/v1/time/advance", json={"duration": duration})
    response.raise_for_status()
    typer.echo(response.text)


@app.command()
def status() -> None:
    response = client().get("/control/v1/status")
    response.raise_for_status()
    typer.echo(response.text)


@faults_app.command("apply")
def apply_fault(rule_file: str) -> None:
    response = client().post(
        "/control/v1/faults",
        content=Path(rule_file).read_bytes(),
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    typer.echo(response.text)
