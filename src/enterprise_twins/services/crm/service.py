import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ClockValue,
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
)
from enterprise_twins.common.control.fault_capabilities import FAULT_CAPABILITIES
from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.context import bind_response_epoch, current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.repository import note_view
from enterprise_twins.services.crm.schemas import NoteCreate


class CrmControl(Protocol):
    async def snapshot(self) -> ClockValue:
        raise NotImplementedError

    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def ready_epoch(self) -> str:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


CRM_NOTE_FAULT_EFFECTS = FAULT_CAPABILITIES[("crm", "crm.note.create", FaultPhase.AFTER_COMMIT)]


@dataclass(frozen=True, slots=True)
class NoteWriteResult:
    response: StoredResponse
    replayed: bool
    fault: FaultDecision


class CrmService:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        control: CrmControl,
    ) -> None:
        self.factory = factory
        self.control = control

    async def create_note(
        self,
        customer_id: str,
        request: NoteCreate,
        expected_version: int,
        idempotency_key: str,
        principal: Principal,
    ) -> NoteWriteResult:
        context = current_request.get()
        if context is None:
            raise RuntimeError("request context is missing")
        snapshot = await self.control.snapshot()
        now = snapshot.now
        epoch = snapshot.scenario_epoch
        namespace = IdempotencyNamespace(
            principal.tenant_id,
            principal.subject,
            "crm.note.create",
            idempotency_key,
        )
        payload = {
            "customerId": customer_id,
            "expectedVersion": expected_version,
            "body": request.model_dump(mode="json"),
        }
        async with self.factory.begin() as session:
            state = await session.scalar(
                select(ScenarioState)
                .where(ScenarioState.singleton_id == 1)
                .with_for_update(read=True)
            )
            if state is not None:
                bind_response_epoch(state.active_epoch)
            if (
                state is None
                or state.mode != "active"
                or state.active_epoch != epoch
                or principal.scenario_epoch != state.active_epoch
            ):
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    "CRM scenario is not active",
                    status_code=503,
                    retryable=True,
                )

            async def work() -> StoredResponse:
                customer = await session.scalar(
                    select(Customer)
                    .where(
                        Customer.scenario_epoch == epoch,
                        Customer.customer_id == customer_id,
                    )
                    .with_for_update()
                )
                if customer is None:
                    raise ApiError(
                        ErrorCode.NOT_FOUND,
                        "customer was not found",
                        status_code=404,
                    )
                if customer.version != expected_version:
                    raise ApiError(
                        ErrorCode.CONFLICT,
                        "customer version differs",
                        status_code=409,
                        details={
                            "expectedVersion": expected_version,
                            "currentVersion": customer.version,
                        },
                    )
                note = CustomerNote(
                    row_id=new_id("nrow"),
                    note_id=new_id("note"),
                    scenario_epoch=epoch,
                    customer_id=customer_id,
                    body=request.body,
                    association=request.association,
                    created_by=principal.subject,
                    created_at=now,
                    archived=False,
                    version=1,
                )
                session.add(note)
                customer.version += 1
                record_audit(
                    session,
                    epoch=epoch,
                    action="crm.note.created",
                    resource_type="customer_note",
                    resource_id=note.note_id,
                    actor_id=principal.subject,
                    correlation_id=context.correlation_id,
                    occurred_at=now,
                    details={"customerId": customer_id},
                )
                record_event(
                    session,
                    epoch=epoch,
                    event_type="crm.note.created",
                    source="crm",
                    subject=f"note/{note.note_id}",
                    resource_version=note.version,
                    correlation_id=context.correlation_id,
                    causation_id=context.request_id,
                    occurred_at=now,
                    recorded_at=now,
                    data={"noteId": note.note_id, "customerId": customer_id},
                )
                return StoredResponse(
                    201,
                    note_view(note).model_dump(mode="json", by_alias=True),
                    {
                        "ETag": '"1"',
                        "X-Customer-Version": str(customer.version),
                    },
                )

            response, replayed = await run_idempotent(
                session,
                epoch,
                namespace,
                payload,
                work,
            )
        fault = await self.control.evaluate_fault(
            FaultProbe(
                targetService="crm",
                operation="crm.note.create",
                phase=FaultPhase.AFTER_COMMIT,
                actorId=principal.subject,
                resourceId=customer_id,
                correlationId=context.correlation_id,
                requestHash=sha256_hex(request.model_dump(mode="json")),
            )
        )
        if fault.effect is not None and fault.effect not in CRM_NOTE_FAULT_EFFECTS:
            raise RuntimeError("unsupported CRM note fault effect")
        return NoteWriteResult(response, replayed, fault)


async def apply_post_commit_fault(result: NoteWriteResult) -> None:
    if result.fault.effect is not None and result.fault.effect not in CRM_NOTE_FAULT_EFFECTS:
        raise RuntimeError("unsupported CRM note fault effect")
    if result.fault.effect == FaultEffect.TIMEOUT:
        await asyncio.sleep((result.fault.delay_ms or 250) / 1000)
