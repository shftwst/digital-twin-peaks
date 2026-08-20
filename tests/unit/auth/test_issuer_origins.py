# ruff: noqa: S106

import pytest
from pydantic import ValidationError

from enterprise_twins.services.crm.settings import CrmSettings
from enterprise_twins.services.identity.settings import IdentitySettings

TRUSTED_ISSUERS = (
    "http://identity:8000",
    "http://127.0.0.1:8101",
)


def test_identity_and_crm_default_to_the_exact_internal_and_loopback_issuer_set() -> None:
    identity = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        secret_pepper="identity-pepper",
    )
    crm = CrmSettings(database_url="postgresql+asyncpg://unused")

    assert identity.issuer_aliases == TRUSTED_ISSUERS
    assert crm.identity_issuer_aliases == TRUSTED_ISSUERS
    assert crm.identity_jwks_url == "http://identity:8000/.well-known/jwks.json"


@pytest.mark.parametrize(
    "origin",
    [
        "https://identity:8000",
        "http://user@identity:8000",
        "http://identity:8000/path",
        "http://identity:8000?query=value",
        "http://identity:8000#fragment",
        "http://identity:8000/",
        "http://identity:08000",
        "HTTP://identity:8000",
    ],
)
@pytest.mark.parametrize("service", ["identity", "crm"])
def test_configured_issuer_aliases_must_be_canonical_http_origins(
    service: str,
    origin: str,
) -> None:
    with pytest.raises(ValidationError):
        if service == "identity":
            IdentitySettings(
                database_url="postgresql+asyncpg://unused",
                secret_pepper="identity-pepper",
                issuer_aliases=("http://identity:8000", origin),
            )
        else:
            CrmSettings(
                database_url="postgresql+asyncpg://unused",
                identity_issuer_aliases=("http://identity:8000", origin),
            )


@pytest.mark.parametrize("service", ["identity", "crm"])
def test_configured_issuer_aliases_must_be_nonempty_and_unique(service: str) -> None:
    for aliases in ((), ("http://identity:8000", "http://identity:8000")):
        with pytest.raises(ValidationError):
            if service == "identity":
                IdentitySettings(
                    database_url="postgresql+asyncpg://unused",
                    secret_pepper="identity-pepper",
                    issuer_aliases=aliases,
                )
            else:
                CrmSettings(
                    database_url="postgresql+asyncpg://unused",
                    identity_issuer_aliases=aliases,
                )
