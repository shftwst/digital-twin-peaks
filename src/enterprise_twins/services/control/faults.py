from typing import Annotated, cast

from fastapi import APIRouter, Depends
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultProbe,
    FaultRuleCreate,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.control.models import FaultActivation, FaultRule, VirtualClock
from enterprise_twins.services.control.settings import ControlSettings


class FaultRepository:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def lock_state(self, session: AsyncSession) -> ScenarioState | None:
        return cast(
            ScenarioState | None,
            await session.scalar(
                select(ScenarioState).where(ScenarioState.singleton_id == 1).with_for_update()
            ),
        )

    async def create(self, request: FaultRuleCreate) -> FaultRuleCreate:
        async with self.factory.begin() as session:
            state = await self.lock_state(session)
            if state is None or state.mode != "active":
                raise RuntimeError("scenario is not active")
            if await session.get(FaultRule, request.rule_id) is not None:
                raise ApiError(
                    ErrorCode.CONFLICT,
                    "fault rule ID already exists",
                    status_code=409,
                )
            session.add(
                FaultRule(
                    rule_id=request.rule_id,
                    scenario_epoch=state.active_epoch,
                    target_service=request.target_service,
                    operation=request.operation,
                    phase=request.phase.value,
                    effect=request.effect.value,
                    actor_id=request.actor_id,
                    resource_id=request.resource_id,
                    correlation_id=request.correlation_id,
                    request_hash=request.request_hash,
                    occurrence=request.occurrence,
                    seen_count=0,
                    remaining_count=request.activation_count,
                    delay_ms=request.delay_ms,
                    response_data=request.response_data,
                )
            )
        return request

    async def evaluate(self, probe: FaultProbe) -> FaultDecision:
        async with self.factory.begin() as session:
            state = await self.lock_state(session)
            clock = await session.get(VirtualClock, 1)
            if state is None or clock is None:
                raise RuntimeError("control state is not initialised")
            rule = await session.scalar(
                select(FaultRule)
                .where(
                    FaultRule.scenario_epoch == state.active_epoch,
                    FaultRule.target_service == probe.target_service,
                    FaultRule.operation == probe.operation,
                    FaultRule.phase == probe.phase.value,
                    FaultRule.remaining_count > 0,
                    or_(FaultRule.actor_id.is_(None), FaultRule.actor_id == probe.actor_id),
                    or_(
                        FaultRule.resource_id.is_(None), FaultRule.resource_id == probe.resource_id
                    ),
                    or_(
                        FaultRule.correlation_id.is_(None),
                        FaultRule.correlation_id == probe.correlation_id,
                    ),
                    or_(
                        FaultRule.request_hash.is_(None),
                        FaultRule.request_hash == probe.request_hash,
                    ),
                )
                .order_by(FaultRule.rule_id)
                .with_for_update()
                .limit(1)
            )
            if rule is None:
                return FaultDecision()
            rule.seen_count += 1
            if rule.seen_count < rule.occurrence:
                return FaultDecision()
            rule.remaining_count -= 1
            session.add(
                FaultActivation(
                    activation_id=new_id("flt"),
                    scenario_epoch=state.active_epoch,
                    rule_id=rule.rule_id,
                    operation=rule.operation,
                    correlation_id=probe.correlation_id,
                    phase=rule.phase,
                    effect=rule.effect,
                    activated_at=clock.now,
                )
            )
            return FaultDecision(
                ruleId=rule.rule_id,
                effect=FaultEffect(rule.effect),
                delayMs=rule.delay_ms,
                responseData=rule.response_data,
            )

    async def clear(self) -> None:
        async with self.factory.begin() as session:
            state = await self.lock_state(session)
            if state is None:
                raise RuntimeError("control state is not initialised")
            await session.execute(delete(FaultActivation))
            await session.execute(delete(FaultRule))

    async def list_activations(self) -> list[FaultActivation]:
        async with self.factory() as session:
            rows = await session.scalars(
                select(FaultActivation).order_by(FaultActivation.activation_id)
            )
            return list(rows)


def fault_router(repository: FaultRepository, settings: ControlSettings) -> APIRouter:
    router = APIRouter()
    ControllerAuth = Annotated[None, Depends(require_token(settings.controller_token))]
    TwinAuth = Annotated[None, Depends(require_token(settings.twin_token))]

    @router.post("/control/v1/faults", status_code=201)
    async def create_fault(request: FaultRuleCreate, _auth: ControllerAuth) -> FaultRuleCreate:
        return await repository.create(request)

    @router.delete("/control/v1/faults", status_code=204)
    async def clear_faults(_auth: ControllerAuth) -> None:
        await repository.clear()

    @router.post("/control/v1/faults/evaluate")
    async def evaluate_fault(request: FaultProbe, _auth: TwinAuth) -> FaultDecision:
        return await repository.evaluate(request)

    @router.get("/control/v1/fault-activations")
    async def fault_activations(_auth: ControllerAuth) -> list[dict[str, object]]:
        return [
            {
                "activationId": item.activation_id,
                "ruleId": item.rule_id,
                "operation": item.operation,
                "correlationId": item.correlation_id,
                "phase": item.phase,
                "effect": item.effect,
                "activatedAt": item.activated_at,
            }
            for item in await repository.list_activations()
        ]

    return router
