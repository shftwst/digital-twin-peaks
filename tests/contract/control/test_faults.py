import asyncio
import json
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient, MockTransport, Request, Response
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
    FaultRuleCreate,
)
from enterprise_twins.common.control.fault_capabilities import (
    FAULT_CAPABILITIES,
    PHASE_EFFECTS,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.app import create_control_app
from enterprise_twins.services.control.faults import FaultRepository
from enterprise_twins.services.control.models import FaultActivation, FaultRule, VirtualClock
from enterprise_twins.services.control.settings import ControlSettings


async def initialise_control(
    db: async_sessionmaker[AsyncSession], *, epoch: str = "epoch_1"
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch=epoch))
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))


def fault_settings() -> ControlSettings:
    return ControlSettings(
        database_url="postgresql+asyncpg://unused",
        controller_token="controller-secret-token",  # noqa: S106
        twin_token="twin-secret-token",  # noqa: S106
    )


def fault_rule(**overrides: object) -> FaultRuleCreate:
    values: dict[str, object] = {
        "ruleId": "crm-note-after-commit",
        "targetService": "crm",
        "operation": "crm.note.create",
        "phase": FaultPhase.AFTER_COMMIT,
        "effect": FaultEffect.TIMEOUT,
    }
    values.update(overrides)
    return FaultRuleCreate.model_validate(values)


def fault_probe(**overrides: object) -> FaultProbe:
    values: dict[str, object] = {
        "targetService": "crm",
        "operation": "crm.note.create",
        "phase": FaultPhase.AFTER_COMMIT,
    }
    values.update(overrides)
    return FaultProbe.model_validate(values)


def test_fault_phase_effect_matrix_and_plan_one_capabilities_are_exact() -> None:
    expected_matrix = {
        (FaultPhase.BEFORE_VALIDATION, FaultEffect.MALFORMED_TRANSPORT),
        (FaultPhase.BEFORE_VALIDATION, FaultEffect.UNAUTHENTICATED),
        (FaultPhase.BEFORE_VALIDATION, FaultEffect.RATE_LIMITED),
        (FaultPhase.BEFORE_COMMIT, FaultEffect.TEMPORARY_FAILURE),
        (FaultPhase.BEFORE_COMMIT, FaultEffect.DELAY),
        (FaultPhase.BEFORE_COMMIT, FaultEffect.TIMEOUT),
        (FaultPhase.AFTER_COMMIT, FaultEffect.TIMEOUT),
        (FaultPhase.AFTER_COMMIT, FaultEffect.CONNECTION_LOSS),
        (FaultPhase.AFTER_COMMIT, FaultEffect.MALFORMED_RESPONSE),
        (FaultPhase.READ, FaultEffect.STALE_VERSION),
        (FaultPhase.READ, FaultEffect.TEMPORARY_ABSENCE),
        (FaultPhase.READ, FaultEffect.PAGINATION_CHANGE),
        (FaultPhase.EVENT_DELIVERY, FaultEffect.DELAY),
        (FaultPhase.EVENT_DELIVERY, FaultEffect.DUPLICATE),
        (FaultPhase.EVENT_DELIVERY, FaultEffect.REORDER),
        (FaultPhase.EVENT_DELIVERY, FaultEffect.SUPPRESS),
        (FaultPhase.EVENT_DELIVERY, FaultEffect.RETRY),
        (FaultPhase.DOMAIN_COMPLETION, FaultEffect.FAILED_REFUND),
        (FaultPhase.DOMAIN_COMPLETION, FaultEffect.DELAYED_SETTLEMENT),
        (FaultPhase.DOMAIN_COMPLETION, FaultEffect.BOUNCE),
        (FaultPhase.DOMAIN_COMPLETION, FaultEffect.DEFER),
        (FaultPhase.DOMAIN_COMPLETION, FaultEffect.DROP),
    }
    expected_capabilities = {
        ("identity", "identity.token.issue", FaultPhase.BEFORE_COMMIT): frozenset(
            {FaultEffect.TEMPORARY_FAILURE, FaultEffect.TIMEOUT}
        ),
        ("crm", "crm.note.create", FaultPhase.AFTER_COMMIT): frozenset(
            {
                FaultEffect.TIMEOUT,
                FaultEffect.CONNECTION_LOSS,
                FaultEffect.MALFORMED_RESPONSE,
            }
        ),
        ("event-relay", "webhook.deliver", FaultPhase.EVENT_DELIVERY): frozenset(
            {
                FaultEffect.DELAY,
                FaultEffect.DUPLICATE,
                FaultEffect.REORDER,
                FaultEffect.SUPPRESS,
                FaultEffect.RETRY,
            }
        ),
    }

    assert {
        (phase, effect) for phase, effects in PHASE_EFFECTS.items() for effect in effects
    } == expected_matrix
    assert FAULT_CAPABILITIES == expected_capabilities


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"targetService": "unknown"},
        {"operation": "crm.note.unknown"},
        {"phase": FaultPhase.READ, "effect": FaultEffect.STALE_VERSION},
        {"phase": FaultPhase.AFTER_COMMIT, "effect": FaultEffect.FAILED_REFUND},
        {
            "targetService": "identity",
            "operation": "identity.token.issue",
            "phase": FaultPhase.AFTER_COMMIT,
        },
        {
            "targetService": "identity",
            "operation": "identity.token.issue",
            "phase": FaultPhase.BEFORE_COMMIT,
            "effect": FaultEffect.DELAY,
        },
    ],
)
async def test_unsupported_fault_rule_returns_422_without_storing_or_activating(
    db: async_sessionmaker[AsyncSession],
    overrides: dict[str, object],
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    payload = fault_rule(**overrides).model_dump(mode="json", by_alias=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
            json=payload,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    async with db() as session:
        assert await session.scalar(select(func.count()).select_from(FaultRule)) == 0
        assert await session.scalar(select(func.count()).select_from(FaultActivation)) == 0


@pytest.mark.asyncio
async def test_every_plan_one_fault_capability_can_be_created(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)

    for (target, operation, phase), effects in FAULT_CAPABILITIES.items():
        for effect in effects:
            await repository.create(
                fault_rule(
                    ruleId=f"{target}-{phase.value}-{effect.value}",
                    targetService=target,
                    operation=operation,
                    phase=phase,
                    effect=effect,
                )
            )

    async with db() as session:
        assert await session.scalar(select(func.count()).select_from(FaultRule)) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"targetService": "unknown"},
        {"operation": "crm.note.unknown"},
        {"phase": FaultPhase.READ},
    ],
)
async def test_unsupported_fault_probe_returns_422_without_database_activation(
    db: async_sessionmaker[AsyncSession],
    overrides: dict[str, object],
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    payload = fault_probe(**overrides).model_dump(mode="json", by_alias=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/faults/evaluate",
            headers={"Authorization": "Bearer twin-secret-token"},
            json=payload,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    async with db() as session:
        assert await session.scalar(select(func.count()).select_from(FaultActivation)) == 0


@pytest.mark.asyncio
async def test_evaluation_rejects_an_invalid_persisted_effect_without_consuming_it(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    async with db.begin() as session:
        session.add(
            FaultRule(
                rule_id="invalid-stored-effect",
                scenario_epoch="epoch_1",
                target_service="crm",
                operation="crm.note.create",
                phase=FaultPhase.AFTER_COMMIT.value,
                effect=FaultEffect.FAILED_REFUND.value,
                actor_id=None,
                resource_id=None,
                correlation_id=None,
                request_hash=None,
                occurrence=1,
                seen_count=0,
                remaining_count=1,
                delay_ms=None,
                response_data={},
            )
        )

    with pytest.raises(ApiError) as raised:
        await FaultRepository(db).evaluate(fault_probe())

    assert raised.value.code == ErrorCode.INVALID_REQUEST
    assert raised.value.status_code == 422
    async with db() as session:
        rule = await session.get(FaultRule, "invalid-stored-effect")
        assert rule is not None
        assert rule.seen_count == 0
        assert rule.remaining_count == 1
        assert await session.scalar(select(func.count()).select_from(FaultActivation)) == 0


async def wait_for_lock_waiters(
    db: async_sessionmaker[AsyncSession], expected: int, tasks: list[asyncio.Task[object]]
) -> None:
    for _ in range(100):
        async with db() as session:
            waiters = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            )
        if waiters is not None and waiters >= expected:
            return
        if any(task.done() for task in tasks):
            break
    raise AssertionError(f"expected {expected} PostgreSQL lock waiter(s)")


@pytest.mark.asyncio
async def test_rule_matches_then_fires_at_configured_occurrence_once(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))
    repository = FaultRepository(db)
    await repository.create(
        FaultRuleCreate(
            ruleId="crm-note-after-commit",
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            effect=FaultEffect.TIMEOUT,
            actorId="support-agent",
            occurrence=2,
            activationCount=1,
            delayMs=250,
        )
    )
    wrong_actor = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="auditor",
            correlationId="case-1",
        )
    )
    first = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    second = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    exhausted = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )

    assert wrong_actor.effect is None
    assert first.effect is None
    assert second.effect == FaultEffect.TIMEOUT
    assert second.delay_ms == 250
    assert exhausted.effect is None
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(FaultActivation))
    assert count == 1


@pytest.mark.asyncio
async def test_duplicate_rule_id_returns_conflict_without_changing_original(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    original = fault_rule(delayMs=250).model_dump(mode="json", by_alias=True)
    changed = fault_rule(delayMs=999).model_dump(mode="json", by_alias=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        created = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
            json=original,
        )
        duplicate = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
            json=changed,
        )

    assert created.status_code == 201
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "conflict"
    async with db() as session:
        stored = await session.get(FaultRule, "crm-note-after-commit")
    assert stored is not None
    assert stored.delay_ms == 250


@pytest.mark.asyncio
async def test_concurrent_duplicate_rule_creates_return_one_conflict(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)

    results = await asyncio.gather(
        repository.create(fault_rule(delayMs=250)),
        repository.create(fault_rule(delayMs=999)),
        return_exceptions=True,
    )

    assert sum(isinstance(result, FaultRuleCreate) for result in results) == 1
    conflicts = [result for result in results if isinstance(result, ApiError)]
    assert len(conflicts) == 1
    assert conflicts[0].code == ErrorCode.CONFLICT
    async with db() as session:
        stored = await session.get(FaultRule, "crm-note-after-commit")
    assert stored is not None
    assert stored.delay_ms in {250, 999}


@pytest.mark.asyncio
async def test_evaluation_waits_for_a_concurrent_matching_rule_lock(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    await repository.create(fault_rule())

    async with db() as locking_session:
        await locking_session.begin()
        await locking_session.scalar(
            select(FaultRule).where(FaultRule.rule_id == "crm-note-after-commit").with_for_update()
        )
        evaluation = asyncio.create_task(repository.evaluate(fault_probe()))
        await wait_for_lock_waiters(db, 1, [evaluation])
        await locking_session.commit()

    decision = await evaluation

    assert decision.effect == FaultEffect.TIMEOUT


@pytest.mark.asyncio
async def test_concurrent_matching_probes_preserve_rule_priority_and_counts(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    await repository.create(fault_rule(ruleId="rule-a", occurrence=2))
    await repository.create(fault_rule(ruleId="rule-b"))

    async with db() as locking_session:
        await locking_session.begin()
        await locking_session.scalar(
            select(ScenarioState).where(ScenarioState.singleton_id == 1).with_for_update()
        )
        first_probe = asyncio.create_task(repository.evaluate(fault_probe(correlationId="first")))
        second_probe = asyncio.create_task(repository.evaluate(fault_probe(correlationId="second")))
        await wait_for_lock_waiters(db, 2, [first_probe, second_probe])
        await locking_session.commit()

    first, second = await asyncio.gather(first_probe, second_probe)
    async with db() as session:
        rules = {
            rule.rule_id: rule
            for rule in await session.scalars(select(FaultRule).order_by(FaultRule.rule_id))
        }

    assert sorted(decision.rule_id for decision in (first, second) if decision.rule_id) == [
        "rule-a"
    ]
    assert sorted(decision.effect for decision in (first, second) if decision.effect) == [
        FaultEffect.TIMEOUT
    ]
    assert rules["rule-a"].seen_count == 2
    assert rules["rule-a"].remaining_count == 0
    assert rules["rule-b"].seen_count == 0
    assert rules["rule-b"].remaining_count == 1

    fallback = await repository.evaluate(fault_probe(correlationId="third"))

    assert fallback.rule_id == "rule-b"
    assert fallback.effect == FaultEffect.TIMEOUT


@pytest.mark.asyncio
async def test_clear_serializes_with_an_in_flight_evaluation(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    await repository.create(fault_rule())
    async with db.begin() as session:
        await session.execute(
            text(
                "CREATE OR REPLACE FUNCTION wait_for_fault_activation_insert() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN PERFORM pg_advisory_xact_lock(5095); RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER wait_for_fault_activation_insert "
                "BEFORE INSERT ON fault_activations FOR EACH ROW "
                "EXECUTE FUNCTION wait_for_fault_activation_insert()"
            )
        )

    async with db() as gate_session:
        await gate_session.begin()
        await gate_session.execute(text("SELECT pg_advisory_xact_lock(5095)"))
        evaluation = asyncio.create_task(repository.evaluate(fault_probe()))
        await wait_for_lock_waiters(db, 1, [evaluation])
        clearing = asyncio.create_task(repository.clear())
        await wait_for_lock_waiters(db, 2, [evaluation, clearing])
        await gate_session.commit()

    await asyncio.gather(evaluation, clearing)
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(FaultActivation))
    assert count == 0


@pytest.mark.asyncio
async def test_rule_can_activate_more_than_once_after_its_occurrence(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    await repository.create(fault_rule(occurrence=2, activationCount=2))

    decisions = [await repository.evaluate(fault_probe()) for _ in range(4)]

    assert [decision.effect for decision in decisions] == [
        None,
        FaultEffect.TIMEOUT,
        FaultEffect.TIMEOUT,
        None,
    ]
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(FaultActivation))
    assert count == 2


@pytest.mark.asyncio
async def test_rule_isolated_to_its_scenario_epoch_and_uses_virtual_activation_time(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db, epoch="epoch_1")
    repository = FaultRepository(db)
    await repository.create(fault_rule())
    async with db.begin() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        state.active_epoch = "epoch_2"

    decision = await repository.evaluate(fault_probe())

    assert decision.effect is None
    async with db() as session:
        activations = list(await session.scalars(select(FaultActivation)))
    assert activations == []

    async with db.begin() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        state.active_epoch = "epoch_1"
    decision = await repository.evaluate(fault_probe())

    assert decision.effect == FaultEffect.TIMEOUT
    async with db() as session:
        activation = await session.scalar(select(FaultActivation))
    assert activation is not None
    assert activation.activated_at == datetime(2026, 8, 19, 10, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rule_field", "probe_field"),
    [
        ("actorId", "actorId"),
        ("resourceId", "resourceId"),
        ("correlationId", "correlationId"),
        ("requestHash", "requestHash"),
    ],
)
async def test_optional_matchers_do_not_advance_a_rule_when_they_do_not_match(
    db: async_sessionmaker[AsyncSession], rule_field: str, probe_field: str
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    await repository.create(fault_rule(**{rule_field: "expected", "occurrence": 2}))

    mismatch = await repository.evaluate(fault_probe(**{probe_field: "unexpected"}))
    first_match = await repository.evaluate(fault_probe(**{probe_field: "expected"}))
    second_match = await repository.evaluate(fault_probe(**{probe_field: "expected"}))

    assert mismatch.effect is None
    assert first_match.effect is None
    assert second_match.effect == FaultEffect.TIMEOUT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "limit"),
    [
        ("ruleId", 120),
        ("targetService", 80),
        ("operation", 160),
        ("actorId", 128),
        ("resourceId", 128),
        ("correlationId", 128),
        ("requestHash", 64),
    ],
)
async def test_create_fault_rejects_strings_longer_than_database_columns(
    db: async_sessionmaker[AsyncSession], field: str, limit: int
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    payload = fault_rule().model_dump(mode="json", by_alias=True)
    payload[field] = "x" * (limit + 1)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
            json=payload,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "limit"),
    [
        ("targetService", 80),
        ("operation", 160),
        ("actorId", 128),
        ("resourceId", 128),
        ("correlationId", 128),
        ("requestHash", 64),
    ],
)
async def test_evaluate_fault_rejects_strings_longer_than_database_columns(
    db: async_sessionmaker[AsyncSession], field: str, limit: int
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    payload = fault_probe().model_dump(mode="json", by_alias=True)
    payload[field] = "x" * (limit + 1)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        response = await client.post(
            "/control/v1/faults/evaluate",
            headers={"Authorization": "Bearer twin-secret-token"},
            json=payload,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_fault_routes_enforce_roles_redact_tokens_and_client_serializes_probes(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    app = create_control_app(db, fault_settings())
    rule_payload = fault_rule().model_dump(mode="json", by_alias=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        denied_create = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer twin-secret-token"},
            json=rule_payload,
        )
        created = await client.post(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
            json=rule_payload,
        )
        control_client = ControlClient("http://control", "twin-secret-token", client)
        decision = await control_client.evaluate_fault(fault_probe())
        denied_diagnostics = await client.get(
            "/control/v1/fault-activations",
            headers={"Authorization": "Bearer twin-secret-token"},
        )
        diagnostics = await client.get(
            "/control/v1/fault-activations",
            headers={"Authorization": "Bearer controller-secret-token"},
        )
        denied_clear = await client.delete(
            "/control/v1/faults",
            headers={"Authorization": "Bearer twin-secret-token"},
        )
        cleared = await client.delete(
            "/control/v1/faults",
            headers={"Authorization": "Bearer controller-secret-token"},
        )
        denied_client = ControlClient("http://control", "controller-secret-token", client)
        with pytest.raises(ApiError) as denied_error:
            await denied_client.evaluate_fault(fault_probe())

    assert denied_create.status_code == 401
    assert created.status_code == 201
    assert decision.effect == FaultEffect.TIMEOUT
    assert denied_diagnostics.status_code == 401
    assert diagnostics.json()[0]["effect"] == FaultEffect.TIMEOUT
    assert denied_clear.status_code == 401
    assert cleared.status_code == 204
    assert denied_error.value.code == ErrorCode.TEMPORARILY_UNAVAILABLE
    assert denied_error.value.status_code == 503
    for response in (denied_create, denied_diagnostics, denied_clear):
        assert "controller-secret-token" not in response.text
        assert "twin-secret-token" not in response.text


@pytest.mark.asyncio
async def test_control_client_serializes_fault_probe_with_camel_case_keys() -> None:
    captured: dict[str, object] = {}

    def capture_request(request: Request) -> Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return Response(
            200,
            json={
                "ruleId": "rule-a",
                "effect": "timeout",
                "delayMs": 250,
                "responseData": {"reason": "configured"},
            },
        )

    probe = fault_probe(
        actorId="support-agent",
        resourceId="case-1",
        correlationId="correlation-1",
        requestHash="a" * 64,
    )
    async with AsyncClient(transport=MockTransport(capture_request)) as http_client:
        client = ControlClient("http://control.example", "twin-secret-token", http_client)
        decision = await client.evaluate_fault(probe)

    assert decision == FaultDecision(
        ruleId="rule-a",
        effect=FaultEffect.TIMEOUT,
        delayMs=250,
        responseData={"reason": "configured"},
    )
    assert captured["method"] == "POST"
    assert captured["path"] == "/control/v1/faults/evaluate"
    assert captured["body"] == {
        "targetService": "crm",
        "operation": "crm.note.create",
        "phase": "after_commit",
        "actorId": "support-agent",
        "resourceId": "case-1",
        "correlationId": "correlation-1",
        "requestHash": "a" * 64,
    }
    assert isinstance(captured["body"], dict)
    assert (
        not {
            "target_service",
            "actor_id",
            "resource_id",
            "correlation_id",
            "request_hash",
        }
        & captured["body"].keys()
    )


@pytest.mark.asyncio
async def test_fault_diagnostics_are_ordered_by_activation_id(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await initialise_control(db)
    repository = FaultRepository(db)
    async with db.begin() as session:
        session.add_all(
            [
                FaultActivation(
                    activation_id="flt_z",
                    scenario_epoch="epoch_1",
                    rule_id="rule-z",
                    operation="crm.note.create",
                    correlation_id=None,
                    phase=FaultPhase.AFTER_COMMIT.value,
                    effect=FaultEffect.TIMEOUT.value,
                    activated_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
                ),
                FaultActivation(
                    activation_id="flt_a",
                    scenario_epoch="epoch_1",
                    rule_id="rule-a",
                    operation="crm.note.create",
                    correlation_id=None,
                    phase=FaultPhase.AFTER_COMMIT.value,
                    effect=FaultEffect.TIMEOUT.value,
                    activated_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
                ),
            ]
        )

    activations = await repository.list_activations()

    assert [item.activation_id for item in activations] == ["flt_a", "flt_z"]
