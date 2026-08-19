import asyncio
import hashlib
import hmac
import os
from dataclasses import dataclass, field
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.events.contracts import EventEnvelope
from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ReceiverSecret(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source: str = Field(min_length=1, max_length=80)
    event_type: str = Field(alias="eventType", min_length=1, max_length=160)
    secret: str = Field(min_length=1, max_length=200)


@dataclass(slots=True)
class ReceiverState:
    secrets: dict[tuple[str, str], str] = field(default_factory=dict)
    attempts: list[dict[str, object]] = field(default_factory=list)
    events: list[dict[str, object]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class ReceiverStatus:
    async def current_epoch(self) -> str:
        return "conformance"

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        return True, {"receiver": "ready"}


def expected_signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return f"v1={digest}"


def safe_attempt(
    *,
    sequence: int,
    envelope: EventEnvelope,
    body_hash: str,
    outcome: Literal["unarmed", "signature_rejected", "accepted"],
    response_status: int,
) -> dict[str, object]:
    return {
        "sequence": sequence,
        "eventId": envelope.event_id,
        "source": envelope.source,
        "eventType": envelope.event_type,
        "correlationId": envelope.correlation_id,
        "bodyHash": body_hash,
        "outcome": outcome,
        "responseStatus": response_status,
    }


def create_receiver_app(control_token: str) -> FastAPI:
    state = ReceiverState()
    private = APIRouter(
        prefix="/internal/v1",
        dependencies=[Depends(require_token(control_token))],
    )
    incoming = APIRouter()

    @private.post("/reset", status_code=204)
    async def reset() -> Response:
        async with state.lock:
            state.secrets.clear()
            state.attempts.clear()
            state.events.clear()
        return Response(status_code=204)

    @private.post("/secrets", status_code=204)
    async def add_secret(body: ReceiverSecret) -> Response:
        async with state.lock:
            state.secrets[(body.source, body.event_type)] = body.secret
        return Response(status_code=204)

    @private.get("/attempts")
    async def attempts() -> list[dict[str, object]]:
        async with state.lock:
            return [dict(item) for item in state.attempts]

    @private.get("/events")
    async def events() -> list[dict[str, object]]:
        async with state.lock:
            return [dict(item) for item in state.events]

    @incoming.post("/events", status_code=204)
    async def receive(
        request: Request,
        x_twin_event_id: Annotated[str, Header(alias="X-Twin-Event-Id")],
        x_twin_timestamp: Annotated[str, Header(alias="X-Twin-Timestamp")],
        x_twin_signature: Annotated[str, Header(alias="X-Twin-Signature")],
    ) -> Response:
        body = await request.body()
        try:
            envelope = EventEnvelope.model_validate_json(body)
        except ValueError as error:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "webhook event envelope is invalid",
                status_code=422,
            ) from error
        if x_twin_event_id != envelope.event_id:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "webhook event ID header differs from the envelope",
                status_code=422,
            )
        body_hash = hashlib.sha256(body).hexdigest()
        async with state.lock:
            secret = state.secrets.get((envelope.source, envelope.event_type))
            sequence = len(state.attempts) + 1
            if secret is None:
                state.attempts.append(
                    safe_attempt(
                        sequence=sequence,
                        envelope=envelope,
                        body_hash=body_hash,
                        outcome="unarmed",
                        response_status=503,
                    )
                )
                raise ApiError(
                    ErrorCode.TEMPORARILY_UNAVAILABLE,
                    "matching webhook subscription secret is not armed",
                    status_code=503,
                    retryable=True,
                )
            if not hmac.compare_digest(
                x_twin_signature,
                expected_signature(secret, x_twin_timestamp, body),
            ):
                state.attempts.append(
                    safe_attempt(
                        sequence=sequence,
                        envelope=envelope,
                        body_hash=body_hash,
                        outcome="signature_rejected",
                        response_status=401,
                    )
                )
                raise ApiError(
                    ErrorCode.UNAUTHENTICATED,
                    "webhook signature differs",
                    status_code=401,
                )
            attempt = safe_attempt(
                sequence=sequence,
                envelope=envelope,
                body_hash=body_hash,
                outcome="accepted",
                response_status=204,
            )
            state.attempts.append(attempt)
            state.events.append(attempt | {"signatureValid": True})
        return Response(status_code=204)

    app = create_app(
        "Twin conformance webhook receiver",
        ("webhooks:receive",),
        ReceiverStatus(),
        (private, incoming),
    )
    app.state.receiver_state = state
    return app


def create_from_env() -> FastAPI:
    return create_receiver_app(os.environ["TWINS_RECEIVER_CONTROL_TOKEN"])
