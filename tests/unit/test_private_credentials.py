# ruff: noqa: S106

from collections.abc import Callable

import pytest
from pydantic import ValidationError

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import BearerAuthenticator
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.conformance.receiver import create_receiver_app
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.crm.settings import CrmSettings
from enterprise_twins.services.identity.settings import IdentitySettings
from enterprise_twins.services.relay.settings import RelaySettings


def control_settings(**overrides: object) -> ControlSettings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://unused",
        "controller_token": "controller-token",
        "twin_token": "twin-token",
        "participant_token": "participant-token",
    }
    return ControlSettings.model_validate(values | overrides)


def identity_settings(**overrides: object) -> IdentitySettings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://unused",
        "signing_seed": "signing-seed",
        "secret_pepper": "secret-pepper",
        "control_token": "control-token",
        "relay_token": "relay-token",
        "participant_token": "participant-token",
    }
    return IdentitySettings.model_validate(values | overrides)


def crm_settings(**overrides: object) -> CrmSettings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://unused",
        "cursor_secret": "cursor-secret",
        "control_token": "control-token",
        "relay_token": "relay-token",
        "participant_token": "participant-token",
    }
    return CrmSettings.model_validate(values | overrides)


def relay_settings(**overrides: object) -> RelaySettings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://unused",
        "control_token": "control-token",
        "participant_token": "participant-token",
        "source_tokens": {"crm": "source-token"},
    }
    return RelaySettings.model_validate(values | overrides)


@pytest.mark.parametrize("invalid", ["", " \t", "embedded whitespace"])
@pytest.mark.parametrize(
    ("factory", "field"),
    [
        (control_settings, "controller_token"),
        (control_settings, "twin_token"),
        (control_settings, "participant_token"),
        (identity_settings, "signing_seed"),
        (identity_settings, "secret_pepper"),
        (identity_settings, "control_token"),
        (identity_settings, "relay_token"),
        (identity_settings, "participant_token"),
        (crm_settings, "cursor_secret"),
        (crm_settings, "control_token"),
        (crm_settings, "relay_token"),
        (crm_settings, "participant_token"),
        (relay_settings, "control_token"),
        (relay_settings, "participant_token"),
    ],
)
def test_all_credential_settings_reject_empty_or_whitespace_values(
    factory: Callable[..., object],
    field: str,
    invalid: str,
) -> None:
    with pytest.raises(ValidationError):
        factory(**{field: invalid})


@pytest.mark.parametrize(
    "source_tokens",
    [
        {"": "source-token"},
        {" \t": "source-token"},
        {"embedded whitespace": "source-token"},
        {"crm": ""},
        {"crm": " \t"},
        {"crm": "embedded whitespace"},
    ],
)
def test_relay_source_token_keys_and_values_reject_whitespace(
    source_tokens: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        relay_settings(source_tokens=source_tokens)


def test_empty_relay_source_token_map_remains_a_fail_closed_valid_configuration() -> None:
    assert relay_settings(source_tokens={}).source_tokens == {}


@pytest.mark.parametrize("token", ["", " \t", "embedded whitespace"])
def test_conformance_receiver_rejects_invalid_control_token_during_construction(
    token: str,
) -> None:
    with pytest.raises(ValueError, match="credential"):
        create_receiver_app(token)


def test_private_token_dependency_accepts_only_an_exact_bearer_header() -> None:
    dependency = require_token("private-token")
    dependency("Bearer private-token")

    for header in (
        None,
        "private-token",
        "bearer private-token",
        "Bearer\tprivate-token",
        "Bearer  private-token",
        " Bearer private-token",
        "Bearer private-token ",
        "Bearer private token",
    ):
        with pytest.raises(ApiError) as raised:
            dependency(header)
        assert raised.value.code == ErrorCode.UNAUTHENTICATED


class AcceptingVerifier:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    async def verify(self, token: str) -> Principal:
        self.tokens.append(token)
        return Principal(
            subject="person-1",
            actor_type="human",
            role="support_agent",
            scopes=frozenset(),
            tenant_id="tenant_synthetic",
            token_id="tok_1",
            scenario_epoch="epoch_1",
        )


@pytest.mark.asyncio
async def test_bearer_authenticator_rejects_noncanonical_headers_before_verification() -> None:
    verifier = AcceptingVerifier()
    authenticator = BearerAuthenticator(verifier)  # type: ignore[arg-type]

    for header in (
        "token",
        "bearer token",
        "Bearer\ttoken",
        "Bearer  token",
        " Bearer token",
        "Bearer token ",
        "Bearer to ken",
    ):
        with pytest.raises(ApiError) as raised:
            await authenticator.authenticate(header)
        assert raised.value.code == ErrorCode.UNAUTHENTICATED

    assert verifier.tokens == []
