import json
from pathlib import Path

import pytest

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.services.control.reset import DirectoryBundleLoader


def write_bundle(root: Path) -> tuple[Path, dict[str, object]]:
    directory = root / "platform-contracts"
    directory.mkdir()
    payload: dict[str, object] = {"expectedCounts": {"clients": 2}}
    (directory / "identity.json").write_text(json.dumps(payload), encoding="utf-8")
    manifest = {
        "scenarioId": "platform-contracts",
        "version": 1,
        "initialTime": "2026-08-19T10:00:00Z",
        "services": {"identity": {"file": "identity.json", "checksum": sha256_hex(payload)}},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return directory, manifest


def test_directory_loader_reads_verified_bundle_under_scenario_root(tmp_path: Path) -> None:
    write_bundle(tmp_path)

    bundle = DirectoryBundleLoader(tmp_path)("platform-contracts", 1)

    assert bundle.scenario_id == "platform-contracts"
    assert bundle.version == 1
    assert bundle.payloads == {"identity": {"expectedCounts": {"clients": 2}}}


def test_directory_loader_rejects_checksum_mismatch(tmp_path: Path) -> None:
    directory, manifest = write_bundle(tmp_path)
    manifest["services"]["identity"]["checksum"] = "0" * 64  # type: ignore[index]
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="checksum differs for identity"):
        DirectoryBundleLoader(tmp_path)("platform-contracts", 1)


def test_directory_loader_rejects_service_file_path_escape(tmp_path: Path) -> None:
    directory, manifest = write_bundle(tmp_path)
    (tmp_path / "outside.json").write_text("{}", encoding="utf-8")
    manifest["services"]["identity"]["file"] = "../outside.json"  # type: ignore[index]
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="escapes its directory"):
        DirectoryBundleLoader(tmp_path)("platform-contracts", 1)


@pytest.mark.parametrize("scenario_id", ["../escape", "UPPER", "-leading"])
def test_directory_loader_rejects_invalid_scenario_ids(tmp_path: Path, scenario_id: str) -> None:
    with pytest.raises(ValueError, match="invalid characters"):
        DirectoryBundleLoader(tmp_path)(scenario_id, 1)
