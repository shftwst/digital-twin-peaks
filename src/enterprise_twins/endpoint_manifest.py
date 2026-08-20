import argparse
import json
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from enterprise_twins.common.auth.origins import canonical_http_origin


class ServiceEndpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    container_url: str = Field(alias="containerUrl")
    loopback_url: str = Field(alias="loopbackUrl")

    @field_validator("container_url")
    @classmethod
    def validate_container_url(cls, value: str) -> str:
        return canonical_http_origin(value)

    @field_validator("loopback_url")
    @classmethod
    def validate_loopback_url(cls, value: str) -> str:
        origin = canonical_http_origin(value)
        if not origin.startswith("http://127.0.0.1:"):
            raise ValueError("loopback URL must use 127.0.0.1")
        return origin


class BusinessServices(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identity: ServiceEndpoint
    crm: ServiceEndpoint


class EndpointManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal["1"] = Field(alias="schemaVersion")
    services: BusinessServices


def write_manifest(path: Path, manifest: EndpointManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(
        manifest.model_dump(mode="json", by_alias=True),
        indent=2,
        sort_keys=True,
    )
    try:
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--identity-container-url", required=True)
    parser.add_argument("--identity-loopback-url", required=True)
    parser.add_argument("--crm-container-url", required=True)
    parser.add_argument("--crm-loopback-url", required=True)
    arguments = parser.parse_args()
    manifest = EndpointManifest(
        schemaVersion="1",
        services=BusinessServices(
            identity=ServiceEndpoint(
                containerUrl=arguments.identity_container_url,
                loopbackUrl=arguments.identity_loopback_url,
            ),
            crm=ServiceEndpoint(
                containerUrl=arguments.crm_container_url,
                loopbackUrl=arguments.crm_loopback_url,
            ),
        ),
    )
    write_manifest(arguments.output, manifest)


if __name__ == "__main__":
    main()
