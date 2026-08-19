import json
import os
import shutil
import subprocess

import httpx
import pytest

pytestmark = pytest.mark.integration


def require_host_topology() -> None:
    if os.environ.get("RUN_LIVE_TOPOLOGY_TESTS") != "1":
        pytest.skip("set RUN_LIVE_TOPOLOGY_TESTS=1 to run live topology tests")
    if os.environ.get("CONTROL_URL"):
        pytest.skip("the host publication proof runs outside the test-runner container")


def compose_configuration() -> dict[str, object]:
    docker = shutil.which("docker")
    assert docker is not None
    result = subprocess.run(  # noqa: S603
        [docker, "compose", "--profile", "test", "config", "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_only_business_apis_are_published() -> None:
    require_host_topology()
    docker = shutil.which("docker")
    assert docker is not None

    assert httpx.get("http://127.0.0.1:8101/health/live", timeout=5.0).status_code == 200
    assert httpx.get("http://127.0.0.1:8102/health/live", timeout=5.0).status_code == 200
    for service in (
        "postgres",
        "control",
        "event-relay-api",
        "event-relay-admin",
        "identity-admin",
        "crm-admin",
        "webhook-receiver",
        "conformance",
        "public-probe",
    ):
        container = subprocess.run(  # noqa: S603
            [docker, "compose", "ps", "-q", service],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert container
        result = subprocess.run(  # noqa: S603
            [docker, "inspect", "--format", "{{json .HostConfig.PortBindings}}", container],
            check=False,
            capture_output=True,
            text=True,
        )
        assert json.loads(result.stdout) == {}


def test_service_network_membership_matches_the_platform_boundary() -> None:
    require_host_topology()
    configuration = compose_configuration()
    services = configuration["services"]
    expected = {
        "control": {"twin-control"},
        "event-relay-api": {"twin-control", "twin-integration"},
        "event-relay-worker": {
            "twin-control",
            "twin-integration",
            "twin-webhook-egress",
        },
        "event-relay-admin": {"twin-control"},
        "identity": {"twin-control", "twin-integration", "twin-public"},
        "identity-admin": {"twin-control"},
        "crm": {"twin-control", "twin-integration", "twin-public"},
        "crm-admin": {"twin-control"},
        "postgres": {"twin-control", "twin-integration"},
        "test-runner": {"twin-control", "twin-public"},
        "webhook-receiver": {"conformance-admin", "twin-webhook-egress"},
        "conformance": {"conformance-admin", "twin-control", "twin-public"},
        "public-probe": {"twin-public"},
    }

    assert {
        name: set(service["networks"]) for name, service in services.items() if name in expected
    } == expected
    assert all(
        "/var/run/docker.sock" not in json.dumps(service.get("volumes", []))
        for service in services.values()
    )


def test_relay_worker_effective_command_uses_the_module_entrypoint() -> None:
    require_host_topology()
    docker = shutil.which("docker")
    assert docker is not None
    container = subprocess.run(  # noqa: S603
        [docker, "compose", "ps", "-q", "event-relay-worker"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert container
    configuration = json.loads(
        subprocess.run(  # noqa: S603
            [docker, "inspect", container],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )[0]["Config"]

    assert configuration["Entrypoint"] == ["python", "-m"]
    assert configuration["Cmd"] == ["enterprise_twins.services.relay.delivery"]
