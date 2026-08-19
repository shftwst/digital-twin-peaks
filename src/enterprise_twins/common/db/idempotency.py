from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.db.records import IdempotencyRecord
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id


@dataclass(frozen=True, slots=True)
class IdempotencyNamespace:
    tenant_id: str
    actor_id: str
    operation: str
    key: str


@dataclass(frozen=True, slots=True)
class StoredResponse:
    status_code: int
    body: dict[str, Any]
    headers: dict[str, str]


async def run_idempotent(
    session: AsyncSession,
    epoch: str,
    namespace: IdempotencyNamespace,
    request_payload: object,
    work: Callable[[], Awaitable[StoredResponse]],
) -> tuple[StoredResponse, bool]:
    request_hash = sha256_hex(request_payload)
    record_id = new_id("idem")
    statement = (
        insert(IdempotencyRecord)
        .values(
            record_id=record_id,
            scenario_epoch=epoch,
            tenant_id=namespace.tenant_id,
            actor_id=namespace.actor_id,
            operation=namespace.operation,
            key=namespace.key,
            request_hash=request_hash,
            state="reserved",
        )
        .on_conflict_do_nothing(constraint="uq_idempotency_namespace")
        .returning(IdempotencyRecord.record_id)
    )
    inserted = await session.scalar(statement)
    if inserted:
        record = await session.get(IdempotencyRecord, inserted)
        if record is None:
            raise RuntimeError("inserted idempotency record is missing")
        result = await work()
        record.state = "completed"
        record.response_status = result.status_code
        record.response_body = result.body
        record.response_headers = result.headers
        return result, False

    existing = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.tenant_id == namespace.tenant_id,
            IdempotencyRecord.actor_id == namespace.actor_id,
            IdempotencyRecord.operation == namespace.operation,
            IdempotencyRecord.key == namespace.key,
        )
        .with_for_update()
    )
    if existing is None:
        raise RuntimeError("conflicting idempotency record is missing")
    if existing.request_hash != request_hash:
        raise ApiError(
            ErrorCode.CONFLICT,
            "Idempotency-Key was already used with different request data",
            status_code=409,
            details={"operation": namespace.operation},
        )
    if existing.state != "completed":
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "The original request is still in progress",
            status_code=503,
            retryable=True,
        )
    if existing.response_status is None or existing.response_body is None:
        raise RuntimeError("completed idempotency record has no response")
    return (
        StoredResponse(
            existing.response_status,
            existing.response_body,
            existing.response_headers or {},
        ),
        True,
    )
