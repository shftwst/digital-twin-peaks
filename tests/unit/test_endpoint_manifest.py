import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from enterprise_twins.endpoint_manifest import EndpointManifest, write_manifest


def manifest_values() -> dict[str, object]:
    return {
        "schemaVersion": "1",
        "services": {
            "identity": {
                "containerUrl": "http://identity:8000",
                "loopbackUrl": "http://127.0.0.1:8101",
            },
            "crm": {
                "containerUrl": "http://crm:8000",
                "loopbackUrl": "http://127.0.0.1:8102",
            },
        },
    }


def test_endpoint_manifest_writer_emits_exact_versioned_credential_free_schema(
    tmp_path: Path,
) -> None:
    output = tmp_path / "manifest-v1.json"

    write_manifest(output, EndpointManifest.model_validate(manifest_values()))

    assert output.read_text(encoding="utf-8").endswith("\n")
    assert json.loads(output.read_text(encoding="utf-8")) == manifest_values()
    assert not any(
        marker in output.read_text(encoding="utf-8").lower()
        for marker in ("token", "secret", "password", "authorization", "control", "relay")
    )


@pytest.mark.parametrize(
    ("service", "field", "value"),
    [
        ("identity", "containerUrl", "https://identity:8000"),
        ("identity", "containerUrl", "http://user@identity:8000"),
        ("identity", "containerUrl", "http://identity:8000/path"),
        ("identity", "containerUrl", "http://identity:8000?query=value"),
        ("identity", "containerUrl", "http://identity:8000#fragment"),
        ("identity", "loopbackUrl", "http://localhost:8101"),
        ("identity", "loopbackUrl", "http://0.0.0.0:8101"),
        ("crm", "loopbackUrl", "http://127.0.0.2:8102"),
    ],
)
def test_endpoint_manifest_rejects_noncanonical_or_nonloopback_origins(
    service: str,
    field: str,
    value: str,
) -> None:
    values = manifest_values()
    services = values["services"]
    assert isinstance(services, dict)
    entry = services[service]
    assert isinstance(entry, dict)
    entry[field] = value

    with pytest.raises(ValidationError):
        EndpointManifest.model_validate(values)


@pytest.mark.parametrize(
    "mutation",
    [
        {"schemaVersion": "2"},
        {"privateToken": "sensitive"},
        {"services": {"control": {"containerUrl": "http://control:8000"}}},
    ],
)
def test_endpoint_manifest_rejects_wrong_versions_extra_fields_and_private_services(
    mutation: dict[str, object],
) -> None:
    values = manifest_values() | mutation
    with pytest.raises(ValidationError):
        EndpointManifest.model_validate(values)
