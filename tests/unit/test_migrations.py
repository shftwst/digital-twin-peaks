import sys

import pytest

from enterprise_twins import migration_metadata, runtime


@pytest.mark.parametrize(
    ("service", "expected_tables"),
    [
        (
            "control",
            {
                "fault_activations",
                "fault_rules",
                "reset_runs",
                "scenario_state",
                "virtual_clock",
            },
        ),
        (
            "relay",
            {
                "audit_records",
                "idempotency_records",
                "outbox_records",
                "relay_deliveries",
                "relay_delivery_attempts",
                "relay_source_events",
                "relay_subscriptions",
                "scenario_state",
            },
        ),
        (
            "identity",
            {
                "audit_records",
                "idempotency_records",
                "identity_clients",
                "outbox_records",
                "scenario_state",
            },
        ),
        (
            "crm",
            {
                "audit_records",
                "crm_customer_notes",
                "crm_customers",
                "idempotency_records",
                "outbox_records",
                "scenario_state",
            },
        ),
    ],
)
def test_service_migration_owns_every_table_used_at_runtime(
    service: str, expected_tables: set[str]
) -> None:
    assert set(migration_metadata.selected_metadata(service).tables) == expected_tables


def test_runtime_migrates_before_binding_the_application(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def migrate(service: str, database_url: str) -> None:
        calls.append(("migrate", service, database_url))

    def serve(application: str, **options: object) -> None:
        calls.append(("serve", application, options))

    monkeypatch.setenv("TWINS_DATABASE_URL", "postgresql+asyncpg://runtime-database")
    monkeypatch.setattr(runtime, "upgrade", migrate)
    monkeypatch.setattr(runtime.uvicorn, "run", serve)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "enterprise_twins.runtime",
            "identity",
            "enterprise_twins.services.identity.app:create_from_env",
            "--port",
            "8123",
        ],
    )

    runtime.main()

    assert calls == [
        ("migrate", "identity", "postgresql+asyncpg://runtime-database"),
        (
            "serve",
            "enterprise_twins.services.identity.app:create_from_env",
            {"host": "0.0.0.0", "port": 8123, "factory": True},  # noqa: S104
        ),
    ]


def test_unknown_migration_service_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown migration service: payments"):
        migration_metadata.selected_metadata("payments")
