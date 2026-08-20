from collections.abc import Mapping
from typing import Any, cast

JsonObject = dict[str, Any]

PLAN_ONE_SERVICE_NETWORKS: dict[str, tuple[str, ...]] = {
    "control": ("twin-control",),
    "event-relay-api": ("twin-control", "twin-integration"),
    "event-relay-worker": (
        "twin-control",
        "twin-integration",
        "twin-webhook-egress",
    ),
    "event-relay-admin": ("twin-control",),
    "identity": ("twin-control", "twin-integration", "twin-public"),
    "identity-admin": ("twin-control",),
    "crm": ("twin-control", "twin-integration", "twin-public"),
    "crm-admin": ("twin-control",),
    "postgres": ("twin-control", "twin-integration"),
    "test-runner": ("twin-control", "twin-public"),
    "webhook-receiver": ("conformance-admin", "twin-webhook-egress"),
    "conformance": ("conformance-admin", "twin-control", "twin-public"),
    "public-probe": ("twin-public",),
}

NETWORK_INTERNAL = {
    "conformance-admin": True,
    "twin-control": True,
    "twin-integration": True,
    "twin-public": False,
    "twin-webhook-egress": True,
}

RUNTIME_HOST_BINDINGS: dict[str, JsonObject] = {name: {} for name in PLAN_ONE_SERVICE_NETWORKS}
RUNTIME_HOST_BINDINGS["identity"] = {"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8101"}]}
RUNTIME_HOST_BINDINGS["crm"] = {"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8102"}]}

COMPOSE_HOST_PORTS: dict[str, list[JsonObject]] = {name: [] for name in PLAN_ONE_SERVICE_NETWORKS}
COMPOSE_HOST_PORTS["identity"] = [
    {
        "mode": "ingress",
        "host_ip": "127.0.0.1",
        "target": 8000,
        "published": "8101",
        "protocol": "tcp",
    }
]
COMPOSE_HOST_PORTS["crm"] = [
    {
        "mode": "ingress",
        "host_ip": "127.0.0.1",
        "target": 8000,
        "published": "8102",
        "protocol": "tcp",
    }
]

PRIVATE_PROBE_TARGETS = {
    "control": 8000,
    "postgres": 5432,
    "event-relay-api": 8000,
    "event-relay-worker": 8000,
    "event-relay-admin": 9000,
    "identity-admin": 9000,
    "crm-admin": 9000,
}

WORKER_COMMAND = ["enterprise_twins.services.relay.delivery"]
AUXILIARY_SERVICES = {"endpoint-manifest"}
RESTART_SERVICES = {
    "control",
    "event-relay-api",
    "event-relay-worker",
    "event-relay-admin",
    "identity",
    "identity-admin",
    "crm",
    "crm-admin",
}


def _object(value: object, description: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{description} is not an object")
    return cast(JsonObject, value)


def _database_environment_keys(environment: object) -> list[str]:
    values = _object(environment, "conformance environment")
    return sorted(
        key for key in values if "DATABASE" in key.upper() or key.upper().startswith("POSTGRES")
    )


def validate_compose_topology(configuration: Mapping[str, object]) -> JsonObject:
    services = _object(configuration.get("services"), "Compose services")
    expected_services = set(PLAN_ONE_SERVICE_NETWORKS) | AUXILIARY_SERVICES
    if set(services) != expected_services:
        raise AssertionError("effective Compose service set differs from the Plan 1 topology")

    compose_networks: dict[str, list[str]] = {}
    for name, expected_networks in PLAN_ONE_SERVICE_NETWORKS.items():
        service = _object(services[name], f"{name} Compose service")
        networks = service.get("networks")
        if not isinstance(networks, dict):
            raise AssertionError(f"{name} Compose networks are missing")
        actual_networks = sorted(networks)
        if actual_networks != list(expected_networks):
            raise AssertionError(f"{name} Compose networks differ")
        compose_networks[name] = actual_networks
        actual_ports = service.get("ports") or []
        if actual_ports != COMPOSE_HOST_PORTS[name]:
            raise AssertionError(f"{name} Compose host bindings differ")

    networks = _object(configuration.get("networks"), "Compose networks")
    network_internal = {
        name: _object(networks.get(name), f"{name} Compose network").get("internal", False)
        for name in NETWORK_INTERNAL
    }
    if network_internal != NETWORK_INTERNAL:
        raise AssertionError("Compose internal-network flags differ")

    if any(
        "/var/run/docker.sock" in str(_object(service, "Compose service").get("volumes", []))
        for service in services.values()
    ):
        raise AssertionError("a Compose service mounts the Docker socket")
    conformance = _object(services["conformance"], "conformance Compose service")
    if _database_environment_keys(conformance.get("environment", {})):
        raise AssertionError("conformance receives database credentials")
    worker = _object(services["event-relay-worker"], "Relay worker Compose service")
    if worker.get("command") != WORKER_COMMAND or worker.get("ports"):
        raise AssertionError("Relay worker is not listener-free")
    if any(
        _object(services[name], f"{name} Compose service").get("restart") != "on-failure"
        for name in RESTART_SERVICES
    ):
        raise AssertionError("long-running twin restart policy differs")

    worker_dependencies = set(_object(worker.get("depends_on"), "worker dependencies"))
    if worker_dependencies != {"control", "postgres"}:
        raise AssertionError("Relay worker startup dependencies create an unsafe cycle")
    relay_api = _object(services["event-relay-api"], "Relay API Compose service")
    api_dependencies = set(_object(relay_api.get("depends_on"), "Relay API dependencies"))
    if "event-relay-worker" not in api_dependencies:
        raise AssertionError("Relay API does not wait for worker health")

    return {
        "composeNetworks": compose_networks,
        "composeNetworkInternal": network_internal,
    }


def validate_network_evidence(network: Mapping[str, object]) -> None:
    expected_keys = {
        "runId",
        "composeNetworks",
        "runtimeNetworks",
        "composeNetworkInternal",
        "runtimeNetworkInternal",
        "hostBindings",
        "publicProbeIsolation",
        "security",
        "workerCommand",
    }
    expected_networks = {
        name: list(networks) for name, networks in PLAN_ONE_SERVICE_NETWORKS.items()
    }
    expected_probe = {
        name: {"resolved": False, "reachable": False} for name in PRIVATE_PROBE_TARGETS
    }
    expected_security: dict[str, list[str]] = {
        "dockerSocketMounts": [],
        "conformanceDatabaseEnvironmentKeys": [],
        "workerExposedPorts": [],
    }
    if (
        set(network) != expected_keys
        or network.get("composeNetworks") != expected_networks
        or network.get("runtimeNetworks") != expected_networks
        or network.get("composeNetworkInternal") != NETWORK_INTERNAL
        or network.get("runtimeNetworkInternal") != NETWORK_INTERNAL
        or network.get("hostBindings") != RUNTIME_HOST_BINDINGS
        or network.get("publicProbeIsolation") != expected_probe
        or network.get("security") != expected_security
        or network.get("workerCommand") != WORKER_COMMAND
    ):
        raise AssertionError("network evidence does not match the isolated Plan 1 contract")
