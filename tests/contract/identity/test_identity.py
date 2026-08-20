# ruff: noqa: S105, S106

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import FaultDecision, ParticipantLoadRequest
from enterprise_twins.common.control.participant import ResetParticipant
from enterprise_twins.common.db.records import AuditRecord, OutboxRecord, ScenarioState
from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.identity import repository as repository_module
from enterprise_twins.services.identity.app import IdentityStatus, create_identity_app
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.scenario import IdentityScenarioLoader
from enterprise_twins.services.identity.secrets import digest_secret
from enterprise_twins.services.identity.secrets import secret_matches as real_secret_matches
from enterprise_twins.services.identity.settings import IdentitySettings


class Clock:
    async def now(self) -> datetime:
        return datetime(2026, 8, 19, 10, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def ready_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()


class UnavailableControl(Clock):
    async def evaluate_fault(self, probe: object) -> FaultDecision:
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "Control is temporarily unavailable",
            status_code=503,
            retryable=True,
        )


@pytest_asyncio.fixture
async def identity_client(
    db: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
        token_ttl_seconds=600,
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            IdentityClient(
                row_id="irow_fixture",
                scenario_epoch="epoch_1",
                client_id="support-agent",
                secret_digest=digest_secret(
                    "support-agent", "support-secret", settings.secret_pepper
                ),
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=["crm:read", "crm:notes:write"],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    app = create_identity_app(db, settings, Clock())
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://identity:8000",
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_client_credentials_jwks_and_self_view(
    db: async_sessionmaker[AsyncSession],
) -> None:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
        token_ttl_seconds=600,
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            IdentityClient(
                row_id="irow_1",
                scenario_epoch="epoch_1",
                client_id="support-agent",
                secret_digest=digest_secret(
                    "support-agent", "support-secret", settings.secret_pepper
                ),
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=["crm:read", "crm:notes:write"],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    app = create_identity_app(db, settings, Clock())
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://identity:8000"
    ) as client:
        metadata = await client.get("/.well-known/openid-configuration")
        keys = await client.get("/.well-known/jwks.json")
        token_response = await client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "support-agent",
                "client_secret": "support-secret",
                "scope": "crm:read crm:notes:write",
            },
        )
        token = token_response.json()["access_token"]
        me = await client.get(
            "/v1/me",
            headers={"Authorization": f"Bearer {token}", "X-Correlation-Id": "case-1"},
        )

    assert metadata.json()["jwks_uri"] == "http://identity:8000/.well-known/jwks.json"
    assert keys.json()["keys"][0]["kty"] == "OKP"
    assert token_response.status_code == 200
    unverified = jwt.decode(token, options={"verify_signature": False})
    assert unverified["scope"] == "crm:notes:write crm:read"
    assert unverified["tenant"] == "tenant_synthetic"
    assert unverified["scenario_epoch"] == "epoch_1"
    assert me.json()["subject"] == "person-support-1"
    assert me.json()["role"] == "support_agent"
    async with db() as session:
        audits = list(await session.scalars(select(AuditRecord).order_by(AuditRecord.action)))
        outbox = await session.scalar(select(OutboxRecord))
    assert [audit.action for audit in audits] == [
        "identity.authorisation.allowed",
        "identity.token.issued",
    ]
    assert outbox is not None
    assert outbox.published is False
    assert outbox.publish_attempts == 0
    assert outbox.envelope["occurredAt"] == "2026-08-19T10:00:00Z"
    assert outbox.envelope["recordedAt"] == "2026-08-19T10:00:00Z"
    persisted = str([audit.details for audit in audits]) + str(outbox.envelope)
    assert token not in persisted
    assert "support-secret" not in persisted
    assert "identity-test-pepper" not in persisted


@pytest.mark.asyncio
async def test_wrong_secret_and_ungranted_scope_are_denied(
    identity_client: AsyncClient,
) -> None:
    wrong = await identity_client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "wrong",
        },
    )
    excessive = await identity_client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "support-secret",
            "scope": "risk:restricted:read",
        },
    )
    assert wrong.status_code == 401
    assert excessive.status_code == 403
    assert "wrong" not in wrong.text
    assert "support-secret" not in excessive.text


@pytest.mark.asyncio
async def test_token_business_call_reports_control_outage_as_retryable_503(
    db: async_sessionmaker[AsyncSession],
) -> None:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
    app = create_identity_app(db, settings, UnavailableControl())

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://identity:8000"
    ) as client:
        response = await client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "support-agent",
                "client_secret": "sensitive-secret",
            },
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "temporarily_unavailable"
    assert response.json()["error"]["retryable"] is True
    assert "sensitive-secret" not in response.text


@pytest.mark.asyncio
async def test_webhook_delete_rejects_noncanonical_if_match_before_relay_work(
    db: async_sessionmaker[AsyncSession],
) -> None:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
        token_ttl_seconds=600,
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            IdentityClient(
                row_id="irow_webhook",
                scenario_epoch="epoch_1",
                client_id="webhook-manager",
                secret_digest=digest_secret(
                    "webhook-manager", "manager-secret", settings.secret_pepper
                ),
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=["webhooks:manage"],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    relay_requests: list[httpx.Request] = []

    async def relay_response(request: httpx.Request) -> httpx.Response:
        relay_requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(relay_response)) as relay_http:
        relay = RelayClient("http://relay", "identity", "source-secret", relay_http)
        app = create_identity_app(db, settings, Clock(), relay)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://identity:8000"
        ) as client:
            token_response = await client.post(
                "/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": "webhook-manager",
                    "client_secret": "manager-secret",
                    "scope": "webhooks:manage",
                },
            )
            token = token_response.json()["access_token"]
            for index, value in enumerate(["1", '""1""', "+1", "-1", ' "1" ', '"01"']):
                response = await client.delete(
                    "/v1/webhook-subscriptions/sub_1",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Correlation-Id": "case-identity-etag",
                        "Idempotency-Key": f"identity-etag-{index}",
                        "If-Match": value,
                    },
                )
                assert response.status_code == 422
                assert response.json()["error"]["code"] == "invalid_request"

    assert relay_requests == []


@pytest.mark.asyncio
async def test_absent_client_and_wrong_secret_take_same_secret_check_and_are_audited(
    identity_client: AsyncClient,
    db: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked: list[tuple[str, str]] = []

    def recording_secret_matches(
        client_id: str,
        supplied: str,
        pepper: str,
        expected: str,
    ) -> bool:
        checked.append((client_id, expected))
        return real_secret_matches(client_id, supplied, pepper, expected)

    monkeypatch.setattr(repository_module, "secret_matches", recording_secret_matches)
    known = await identity_client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "wrong-secret",
        },
    )
    absent = await identity_client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "absent-agent",
            "client_secret": "wrong-secret",
        },
    )

    assert known.status_code == absent.status_code == 401
    assert [item[0] for item in checked] == ["support-agent", "absent-agent"]
    assert all(len(item[1]) == 64 for item in checked)
    async with db() as session:
        denials = list(
            await session.scalars(
                select(AuditRecord).where(AuditRecord.action == "identity.authentication.denied")
            )
        )
    assert len(denials) == 2
    assert {denial.details["reason"] for denial in denials} == {"invalid_client_credentials"}
    audit_values = str([denial.details for denial in denials])
    assert "wrong-secret" not in audit_values
    assert "support-secret" not in audit_values


@pytest.mark.asyncio
async def test_identity_reset_loads_digested_clients_and_is_ready_only_when_active(
    db: async_sessionmaker[AsyncSession],
) -> None:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        secret_pepper="identity-test-pepper",
    )
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_old",
                scenario_id="old-scenario",
                scenario_version=2,
                random_seed=3,
                manifest_checksum="a" * 64,
            )
        )
        session.add(
            IdentityClient(
                row_id="irow_old",
                scenario_epoch="epoch_old",
                client_id="old-client",
                secret_digest="0" * 64,
                subject="old-subject",
                actor_type="service",
                role="old_role",
                scopes=[],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    participant = ResetParticipant(
        db,
        IdentityScenarioLoader(settings.secret_pepper),
        service="identity",
    )

    class ReadyControl:
        async def ready_epoch(self) -> str:
            return "epoch_new"

    status = IdentityStatus(db, ReadyControl())
    payload = {
        "schemaVersion": "1",
        "clients": [
            {
                "clientId": "new-client",
                "clientSecret": "new-client-secret",
                "subject": "new-subject",
                "actorType": "service",
                "role": "crm_service",
                "scopes": ["crm:read", "crm:read"],
                "tenantId": "tenant_synthetic",
            }
        ],
        "aliases": {"crm": "new-client"},
    }
    request = ParticipantLoadRequest(
        scenarioEpoch="epoch_new",
        scenarioId="platform-contracts",
        scenarioVersion=1,
        randomSeed=7,
        payload=payload,
        checksum=sha256_hex(payload),
        manifestChecksum="b" * 64,
    )

    await participant.prepare("epoch_new")
    assert await status.readiness() == (
        False,
        {"database": "ready", "scenario": "preparing", "control": "not_ready"},
    )
    report = await participant.load(request)
    await participant.commit("epoch_new")
    assert (await status.readiness())[0] is False
    await participant.finalize("epoch_new")

    assert report.service == "identity"
    assert report.counts == {"clients": 1}
    assert report.aliases == {"crm": "new-client"}
    assert await status.readiness() == (
        True,
        {"database": "ready", "scenario": "active", "control": "ready"},
    )
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        clients = list(await session.scalars(select(IdentityClient)))
    assert state is not None
    assert state.active_epoch == "epoch_new"
    assert state.manifest_checksum == "b" * 64
    assert state.pending_epoch is None
    assert state.rollback_epoch is None
    assert [client.client_id for client in clients] == ["new-client"]
    assert clients[0].scopes == ["crm:read"]
    assert clients[0].secret_digest != "new-client-secret"
    assert real_secret_matches(
        "new-client",
        "new-client-secret",
        settings.secret_pepper,
        clients[0].secret_digest,
    )
