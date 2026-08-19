from typing import Any, Protocol

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.control.contracts import ParticipantLoadRequest, ParticipantReport
from enterprise_twins.common.db.records import (
    AuditRecord,
    IdempotencyRecord,
    OutboxRecord,
    ScenarioState,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ScenarioLoader(Protocol):
    async def load(
        self, session: AsyncSession, epoch: str, payload: dict[str, Any]
    ) -> dict[str, object]:
        raise NotImplementedError

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        raise NotImplementedError


class ResetParticipant:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        loader: ScenarioLoader,
        service: str = "test",
    ) -> None:
        self.factory = factory
        self.loader = loader
        self.service = service

    async def database_ready(self) -> bool:
        try:
            async with self.factory() as session:
                state = await session.get(ScenarioState, 1)
        except Exception:
            return False
        return state is not None and state.mode == "active"

    async def prepare(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                state = ScenarioState(singleton_id=1, mode="uninitialised", active_epoch="none")
                session.add(state)
            if state.mode not in {"active", "error", "uninitialised"}:
                raise ApiError(ErrorCode.CONFLICT, "another reset is active", status_code=409)
            state.mode = "preparing"
            state.pending_epoch = epoch
            self.clear_pending_metadata(state)
            self.clear_rollback_metadata(state)
            await session.execute(delete(IdempotencyRecord))
            await session.execute(delete(OutboxRecord))
            await session.execute(delete(AuditRecord))

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        if sha256_hex(request.payload) != request.checksum:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "scenario payload checksum differs",
                status_code=422,
            )
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if (
                state is None
                or state.pending_epoch != request.scenario_epoch
                or state.mode != "preparing"
            ):
                raise ApiError(
                    ErrorCode.CONFLICT,
                    "participant is not prepared for this epoch",
                    status_code=409,
                )
            result = await self.loader.load(session, request.scenario_epoch, request.payload)
            state.mode = "loaded"
            state.pending_scenario_id = request.scenario_id
            state.pending_scenario_version = request.scenario_version
            state.pending_random_seed = request.random_seed
            state.pending_manifest_checksum = request.manifest_checksum
            return ParticipantReport.model_validate(
                {
                    "service": self.service,
                    "schemaVersion": str(result["schemaVersion"]),
                    "counts": result["counts"],
                    "aliases": result.get("aliases", {}),
                    "checksum": request.checksum,
                }
            )

    async def commit(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None or state.pending_epoch != epoch or state.mode != "loaded":
                raise ApiError(
                    ErrorCode.CONFLICT,
                    "participant has not loaded this epoch",
                    status_code=409,
                )
            state.rollback_epoch = state.active_epoch
            state.rollback_scenario_id = state.scenario_id
            state.rollback_scenario_version = state.scenario_version
            state.rollback_random_seed = state.random_seed
            state.rollback_manifest_checksum = state.manifest_checksum
            state.active_epoch = epoch
            state.scenario_id = state.pending_scenario_id
            state.scenario_version = state.pending_scenario_version
            state.random_seed = state.pending_random_seed
            state.manifest_checksum = state.pending_manifest_checksum
            state.mode = "committed"

    async def finalize(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if (
                state is not None
                and state.active_epoch == epoch
                and state.mode == "active"
                and state.rollback_epoch is None
            ):
                return
            if (
                state is None
                or state.active_epoch != epoch
                or state.pending_epoch != epoch
                or state.mode != "committed"
                or state.rollback_epoch is None
            ):
                raise ApiError(
                    ErrorCode.CONFLICT,
                    "participant has not committed this epoch",
                    status_code=409,
                )
            if state.rollback_epoch != "none":
                await self.loader.discard(session, state.rollback_epoch)
            state.pending_epoch = None
            self.clear_pending_metadata(state)
            self.clear_rollback_metadata(state)
            state.mode = "active"

    async def abort(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                return
            if state.active_epoch == epoch and state.rollback_epoch is not None:
                await self.loader.discard(session, epoch)
                state.active_epoch = state.rollback_epoch
                state.scenario_id = state.rollback_scenario_id
                state.scenario_version = state.rollback_scenario_version
                state.random_seed = state.rollback_random_seed
                state.manifest_checksum = state.rollback_manifest_checksum
                state.pending_epoch = None
                self.clear_pending_metadata(state)
                self.clear_rollback_metadata(state)
                state.mode = "error"
                return
            if state.pending_epoch == epoch:
                await self.loader.discard(session, epoch)
                state.pending_epoch = None
                self.clear_pending_metadata(state)
                state.mode = "error"

    @staticmethod
    def clear_pending_metadata(state: ScenarioState) -> None:
        state.pending_scenario_id = None
        state.pending_scenario_version = None
        state.pending_random_seed = None
        state.pending_manifest_checksum = None

    @staticmethod
    def clear_rollback_metadata(state: ScenarioState) -> None:
        state.rollback_epoch = None
        state.rollback_scenario_id = None
        state.rollback_scenario_version = None
        state.rollback_random_seed = None
        state.rollback_manifest_checksum = None


def create_participant_app(name: str, participant: ResetParticipant, token: str) -> FastAPI:
    app = FastAPI(title=f"{name} reset participant", docs_url=None, redoc_url=None)
    router = APIRouter(prefix="/internal/v1/reset", dependencies=[Depends(require_token(token))])

    @router.post("/prepare", status_code=204)
    async def prepare(body: dict[str, str]) -> None:
        await participant.prepare(body["scenarioEpoch"])

    @router.post("/load")
    async def load(body: ParticipantLoadRequest) -> ParticipantReport:
        return await participant.load(body)

    @router.post("/commit", status_code=204)
    async def commit(body: dict[str, str]) -> None:
        await participant.commit(body["scenarioEpoch"])

    @router.post("/finalize", status_code=204)
    async def finalize(body: dict[str, str]) -> None:
        await participant.finalize(body["scenarioEpoch"])

    @router.post("/abort", status_code=204)
    async def abort(body: dict[str, str]) -> None:
        await participant.abort(body["scenarioEpoch"])

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        is_ready = await participant.database_ready()
        return JSONResponse(
            {"status": "ready" if is_ready else "not_ready"},
            status_code=200 if is_ready else 503,
        )

    @app.exception_handler(ApiError)
    async def api_error(_request: Request, error: ApiError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "code": error.code,
                    "message": error.message,
                    "retryable": error.retryable,
                    "details": error.details,
                }
            },
            status_code=error.status_code,
        )

    app.include_router(router)
    return app
