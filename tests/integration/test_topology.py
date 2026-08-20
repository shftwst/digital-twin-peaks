import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pytest

from enterprise_twins.conformance.platform_contracts import (
    record_network_proof,
    validate_network_evidence,
)
from enterprise_twins.endpoint_manifest import EndpointManifest
from enterprise_twins.topology import validate_compose_topology

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

    measured = validate_compose_topology(configuration)

    assert {
        name: set(service["networks"]) for name, service in services.items() if name in expected
    } == expected
    assert measured["composeNetworks"] == {
        name: sorted(networks) for name, networks in expected.items()
    }
    assert measured["composeNetworkInternal"] == {
        "conformance-admin": True,
        "twin-control": True,
        "twin-integration": True,
        "twin-public": False,
        "twin-webhook-egress": True,
    }
    assert all(
        "/var/run/docker.sock" not in json.dumps(service.get("volumes", []))
        for service in services.values()
    )


def test_endpoint_manifest_matches_effective_compose_and_contains_only_business_urls() -> None:
    require_host_topology()
    configuration = compose_configuration()
    services = configuration["services"]
    manifest = EndpointManifest.model_validate_json(
        Path("artifacts/endpoints/manifest-v1.json").read_text(encoding="utf-8")
    )

    assert set(manifest.services.model_fields_set) == {"identity", "crm"}
    for service_name, endpoint in (
        ("identity", manifest.services.identity),
        ("crm", manifest.services.crm),
    ):
        container = urlsplit(endpoint.container_url)
        loopback = urlsplit(endpoint.loopback_url)
        assert container.hostname == service_name
        assert container.port == 8000
        assert loopback.hostname == "127.0.0.1"
        assert services[service_name]["ports"] == [
            {
                "mode": "ingress",
                "host_ip": "127.0.0.1",
                "target": 8000,
                "published": str(loopback.port),
                "protocol": "tcp",
            }
        ]

    trusted = [manifest.services.identity.container_url, manifest.services.identity.loopback_url]
    identity_aliases = services["identity"]["environment"]["TWINS_IDENTITY_ISSUER_ALIASES"]
    assert json.loads(identity_aliases) == trusted
    assert (
        json.loads(services["crm"]["environment"]["TWINS_CRM_IDENTITY_ISSUER_ALIASES"]) == trusted
    )
    assert services["crm"]["environment"]["TWINS_CRM_IDENTITY_JWKS_URL"] == (
        f"{manifest.services.identity.container_url}/.well-known/jwks.json"
    )
    writer = services["endpoint-manifest"]
    assert writer.get("network_mode") == "none"
    assert writer.get("read_only") is True
    assert not writer.get("environment")
    assert not any(
        marker in json.dumps(manifest.model_dump(mode="json", by_alias=True)).lower()
        for marker in ("token", "secret", "password", "authorization")
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


def test_canonical_runtime_network_proof_is_exhaustive(tmp_path: Path) -> None:
    require_host_topology()
    record_network_proof(tmp_path, "topology-integration")
    evidence = json.loads((tmp_path / "network.json").read_text(encoding="utf-8"))

    validate_network_evidence(evidence)
    expected_services = {
        "control",
        "event-relay-api",
        "event-relay-worker",
        "event-relay-admin",
        "identity",
        "identity-admin",
        "crm",
        "crm-admin",
        "postgres",
        "test-runner",
        "webhook-receiver",
        "conformance",
        "public-probe",
    }
    assert set(evidence["composeNetworks"]) == expected_services
    assert set(evidence["runtimeNetworks"]) == expected_services
    assert evidence["publicProbeIsolation"] == {
        name: {"resolved": False, "reachable": False}
        for name in (
            "control",
            "postgres",
            "event-relay-api",
            "event-relay-worker",
            "event-relay-admin",
            "identity-admin",
            "crm-admin",
        )
    }
