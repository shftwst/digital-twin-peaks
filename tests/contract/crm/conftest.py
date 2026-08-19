# ruff: noqa: S106

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import ASGIApp

from enterprise_twins.common.auth.verifier import JwtVerifier
from enterprise_twins.common.control.contracts import FaultDecision, FaultProbe
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.crm.app import create_crm_app
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.settings import CrmSettings
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.models import IdentityClient

NOW = datetime(2026, 8, 19, 10, tzinfo=UTC)


class Control:
    def __init__(self) -> None:
        self.epoch = "epoch_1"
        self.decision = FaultDecision()

    async def now(self) -> datetime:
        return NOW

    async def current_epoch(self) -> str:
        return self.epoch

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        return self.decision


@dataclass
class Harness:
    app: ASGIApp
    client: AsyncClient
    support_headers: dict[str, str]
    read_only_headers: dict[str, str]
    future_epoch_headers: dict[str, str]
    control: Control


def client_record(subject: str, scopes: list[str], role: str) -> IdentityClient:
    return IdentityClient(
        row_id=f"irow_{subject}",
        scenario_epoch="epoch_1",
        client_id=subject,
        secret_digest="not-used",
        subject=subject,
        actor_type="human" if role == "support_agent" else "service",
        role=role,
        scopes=scopes,
        tenant_id="tenant_synthetic",
        active=True,
        version=1,
    )


@pytest_asyncio.fixture
async def crm_harness(
    db: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Harness]:
    customers = [
        ("cus_ambiguous_b", "Sam Shared B", "shared@example.test", "ext-shared-b", "LOY-2002"),
        ("cus_unique", "Alex Unique", "alex.unique@example.test", "ext-unique", "LOY-1001"),
        ("cus_ambiguous_a", "Sam Shared A", "shared@example.test", "ext-shared-a", "LOY-2001"),
    ]
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        for index, (customer_id, name, email, reference, loyalty) in enumerate(customers):
            session.add(
                Customer(
                    row_id=f"crow_{index}",
                    scenario_epoch="epoch_1",
                    customer_id=customer_id,
                    display_name=name,
                    primary_email=email,
                    external_reference=reference,
                    account_status="active",
                    contact_methods=[{"type": "email", "value": email, "primary": True}],
                    external_identifiers={"loyalty": loyalty},
                    version=1,
                )
            )
        session.add(
            Customer(
                row_id="crow_old",
                scenario_epoch="epoch_old",
                customer_id="cus_old_hidden",
                display_name="Old Hidden",
                primary_email="old@example.test",
                external_reference="ext-old",
                account_status="active",
                contact_methods=[{"type": "email", "value": "old@example.test", "primary": True}],
                external_identifiers={"loyalty": "LOY-OLD"},
                version=7,
            )
        )
        session.add(
            CustomerNote(
                row_id="nrow_old",
                note_id="note_old_hidden",
                scenario_epoch="epoch_old",
                customer_id="cus_old_hidden",
                body="old epoch note",
                association="account",
                created_by="old-actor",
                created_at=NOW,
                archived=False,
                version=1,
            )
        )

    issuer = TokenIssuer(
        "http://identity:8000",
        "enterprise-twins",
        "identity-test-signing-seed",
        600,
    )
    support = client_record(
        "person-support-1",
        ["crm:read", "crm:notes:write", "webhooks:manage"],
        "support_agent",
    )
    evaluator = client_record(
        "service-evaluator-1",
        ["crm:read"],
        "evaluator_service",
    )
    support_token, _ = issuer.issue(
        support,
        ["crm:read", "crm:notes:write", "webhooks:manage"],
        NOW,
        "epoch_1",
    )
    read_only_token, _ = issuer.issue(evaluator, ["crm:read"], NOW, "epoch_1")
    future_epoch_token, _ = issuer.issue(
        support,
        ["crm:read", "crm:notes:write", "webhooks:manage"],
        NOW,
        "epoch_2",
    )

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [issuer.public_jwk]})

    verification_client = httpx.AsyncClient(transport=httpx.MockTransport(jwks))
    control = Control()
    verifier = JwtVerifier(
        "http://identity:8000",
        "enterprise-twins",
        "http://identity:8000/.well-known/jwks.json",
        control,
        verification_client,
    )
    settings = CrmSettings(
        database_url="postgresql+asyncpg://unused",
        cursor_secret="crm-test-cursor",
    )
    app = create_crm_app(db, settings, control, verifier, relay=None)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://crm:8000",
    ) as client:
        yield Harness(
            app,
            client,
            {
                "Authorization": f"Bearer {support_token}",
                "X-Correlation-Id": "case-crm-test",
            },
            {
                "Authorization": f"Bearer {read_only_token}",
                "X-Correlation-Id": "case-crm-read-only",
            },
            {
                "Authorization": f"Bearer {future_epoch_token}",
                "X-Correlation-Id": "case-crm-future-epoch",
            },
            control,
        )
    await verification_client.aclose()


@pytest.fixture
def crm_client(crm_harness: Harness) -> AsyncClient:
    return crm_harness.client


@pytest.fixture
def support_headers(crm_harness: Harness) -> dict[str, str]:
    return crm_harness.support_headers


@pytest.fixture
def read_only_headers(crm_harness: Harness) -> dict[str, str]:
    return crm_harness.read_only_headers
