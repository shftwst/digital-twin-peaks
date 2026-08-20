import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol

import httpx
from fastapi import APIRouter, Depends
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
    ResetResult,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.control.models import (
    FaultActivation,
    FaultRule,
    ResetRun,
    VirtualClock,
)
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings


@dataclass(frozen=True, slots=True)
class ScenarioBundle:
    scenario_id: str
    version: int
    initial_time: datetime
    payloads: dict[str, dict[str, Any]]

    def checksum(self, random_seed: int) -> str:
        return sha256_hex(
            {
                "scenarioId": self.scenario_id,
                "version": self.version,
                "initialTime": self.initial_time,
                "payloads": self.payloads,
                "randomSeed": random_seed,
            }
        )


class ScenarioIdInvalidError(RuntimeError):
    pass


class ScenarioNotFoundError(RuntimeError):
    pass


class ScenarioVersionNotFoundError(RuntimeError):
    pass


class ScenarioBundleInvalidError(RuntimeError):
    pass


class ParticipantClient(Protocol):
    async def prepare(self, epoch: str) -> None:
        raise NotImplementedError

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        raise NotImplementedError

    async def commit(self, epoch: str) -> None:
        raise NotImplementedError

    async def finalize(self, epoch: str) -> None:
        raise NotImplementedError

    async def abort(self, epoch: str) -> None:
        raise NotImplementedError


BundleLoader = Callable[[str, int], ScenarioBundle]
BeginControl = Callable[[str, ScenarioBundle, int], Awaitable[None]]
CommitControl = Callable[[str, ScenarioBundle, int], Awaitable[None]]
FinalizeControl = Callable[[str], Awaitable[None]]
PendingCleanup = Callable[[], Awaitable[str | None]]
ResetFailurePhase = Literal["pre_cutover", "cleanup"]
FailControl = Callable[[str, ResetFailurePhase], Awaitable[None]]


class ResetCleanupError(RuntimeError):
    pass


class ResetCoordinator:
    def __init__(
        self,
        participants: dict[str, ParticipantClient],
        load_bundle: BundleLoader,
        begin_control: BeginControl,
        commit_control: CommitControl,
        fail_control: FailControl,
        finalize_control: FinalizeControl,
        pending_cleanup: PendingCleanup,
    ) -> None:
        self.participants = participants
        self.load_bundle = load_bundle
        self.begin_control = begin_control
        self.commit_control = commit_control
        self.fail_control = fail_control
        self.finalize_control = finalize_control
        self.pending_cleanup = pending_cleanup
        self.lock = asyncio.Lock()
        self.test_mode = "active"

    @classmethod
    def for_test(
        cls, participants: dict[str, ParticipantClient], bundle: ScenarioBundle
    ) -> ResetCoordinator:
        async def begin(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
            return None

        async def commit(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
            return None

        async def fail(_epoch: str, _phase: ResetFailurePhase) -> None:
            return None

        async def finalize(_epoch: str) -> None:
            return None

        async def pending() -> str | None:
            return None

        return cls(
            participants,
            lambda _sid, _version: bundle,
            begin,
            commit,
            fail,
            finalize,
            pending,
        )

    @staticmethod
    def cleanup_unavailable() -> ApiError:
        return ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "reset cleanup is temporarily unavailable",
            status_code=503,
            retryable=True,
            details={"phase": "cleanup"},
        )

    async def finalize_epoch(self, epoch: str) -> None:
        cleanup_results = await asyncio.gather(
            *(participant.finalize(epoch) for participant in self.participants.values()),
            return_exceptions=True,
        )
        if any(isinstance(result, BaseException) for result in cleanup_results):
            raise ResetCleanupError("participant reset cleanup failed")
        await self.finalize_control(epoch)

    async def mark_cleanup_failed(self, epoch: str) -> None:
        try:
            await self.fail_control(epoch, "cleanup")
        except Exception:
            return

    async def recover_pending_cleanup_unlocked(self) -> bool:
        epoch = await self.pending_cleanup()
        if epoch is None:
            return False
        try:
            await self.finalize_epoch(epoch)
        except Exception:
            await self.mark_cleanup_failed(epoch)
            self.test_mode = "cleanup_error"
            raise self.cleanup_unavailable() from None
        self.test_mode = "active"
        return True

    async def recover_pending_cleanup(self) -> bool:
        async with self.lock:
            return await self.recover_pending_cleanup_unlocked()

    async def reset(self, request: ResetRequest) -> ResetResult:
        async with self.lock:
            await self.recover_pending_cleanup_unlocked()
            bundle = self.load_bundle(request.scenario_id, request.version)
            if set(self.participants) != set(bundle.payloads):
                raise ValueError("participant services differ from scenario bundle services")
            seed = (
                request.random_seed
                if request.random_seed is not None
                else derive_seed(request.scenario_id, request.version)
            )
            manifest_checksum = bundle.checksum(seed)
            epoch = new_id("epoch")
            reports: list[ParticipantReport] = []
            await self.begin_control(epoch, bundle, seed)
            try:
                for participant in self.participants.values():
                    await participant.prepare(epoch)
                for name, participant in self.participants.items():
                    payload = bundle.payloads[name]
                    checksum = sha256_hex(payload)
                    report = await participant.load(
                        ParticipantLoadRequest(
                            scenarioEpoch=epoch,
                            scenarioId=bundle.scenario_id,
                            scenarioVersion=bundle.version,
                            randomSeed=seed,
                            payload=payload,
                            checksum=checksum,
                            manifestChecksum=manifest_checksum,
                        )
                    )
                    if (
                        report.service != name
                        or report.checksum != checksum
                        or report.counts != payload["expectedCounts"]
                    ):
                        raise RuntimeError(f"{name} reset verification failed")
                    reports.append(report)
                for participant in self.participants.values():
                    await participant.commit(epoch)
                await self.commit_control(epoch, bundle, seed)
            except Exception:
                await asyncio.gather(
                    *(participant.abort(epoch) for participant in self.participants.values()),
                    return_exceptions=True,
                )
                await self.fail_control(epoch, "pre_cutover")
                self.test_mode = "error"
                raise
            try:
                await self.finalize_epoch(epoch)
            except Exception:
                await self.mark_cleanup_failed(epoch)
                self.test_mode = "cleanup_error"
                raise self.cleanup_unavailable() from None
            self.test_mode = "active"
            return ResetResult(
                scenarioId=bundle.scenario_id,
                version=bundle.version,
                randomSeed=seed,
                scenarioEpoch=epoch,
                manifestChecksum=manifest_checksum,
                reports=reports,
            )


def derive_seed(scenario_id: str, version: int) -> int:
    digest = hashlib.sha256(f"{scenario_id}:{version}".encode()).digest()
    return int.from_bytes(digest[:8], signed=False) & ((1 << 63) - 1)


class HttpParticipantClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.client = client

    async def post(self, action: str, body: dict[str, object]) -> httpx.Response:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/reset/{action}",
            headers=self.headers,
            json=body,
            timeout=5.0,
        )
        response.raise_for_status()
        return response

    async def prepare(self, epoch: str) -> None:
        await self.post("prepare", {"scenarioEpoch": epoch})

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        response = await self.post("load", request.model_dump(mode="json", by_alias=True))
        return ParticipantReport.model_validate(response.json())

    async def commit(self, epoch: str) -> None:
        await self.post("commit", {"scenarioEpoch": epoch})

    async def finalize(self, epoch: str) -> None:
        await self.post("finalize", {"scenarioEpoch": epoch})

    async def abort(self, epoch: str) -> None:
        await self.post("abort", {"scenarioEpoch": epoch})


class DirectoryBundleLoader:
    def __init__(self, scenario_root: Path) -> None:
        self.scenario_root = scenario_root.resolve()

    def __call__(self, scenario_id: str, version: int) -> ScenarioBundle:
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", scenario_id) is None:
            raise ScenarioIdInvalidError("scenario ID has invalid characters")
        directory = (self.scenario_root / scenario_id).resolve()
        if not directory.is_relative_to(self.scenario_root):
            raise ScenarioBundleInvalidError("scenario path escapes the configured root")
        if not directory.is_dir():
            raise ScenarioNotFoundError("scenario directory is absent")
        try:
            manifest_path = directory / "manifest.json"
            if not manifest_path.is_file():
                raise ScenarioBundleInvalidError("scenario manifest is absent")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise ScenarioBundleInvalidError("scenario manifest is invalid")
            if manifest.get("scenarioId") != scenario_id:
                raise ScenarioBundleInvalidError("scenario manifest ID differs")
            manifest_version = manifest.get("version")
            if type(manifest_version) is not int:
                raise ScenarioBundleInvalidError("scenario manifest version is invalid")
            if manifest_version != version:
                raise ScenarioVersionNotFoundError("scenario version is absent")
            services = manifest.get("services")
            if not isinstance(services, dict):
                raise ScenarioBundleInvalidError("scenario services are invalid")
            payloads: dict[str, dict[str, Any]] = {}
            for service, item in services.items():
                if not isinstance(service, str) or not isinstance(item, dict):
                    raise ScenarioBundleInvalidError("scenario service entry is invalid")
                filename = item.get("file")
                checksum = item.get("checksum")
                if not isinstance(filename, str) or not isinstance(checksum, str):
                    raise ScenarioBundleInvalidError("scenario service file is invalid")
                path = (directory / filename).resolve()
                if not path.is_relative_to(directory):
                    raise ScenarioBundleInvalidError("scenario service path escapes its directory")
                if not path.is_file():
                    raise ScenarioBundleInvalidError("scenario service payload is absent")
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ScenarioBundleInvalidError("scenario service payload is invalid")
                if sha256_hex(payload) != checksum:
                    raise ScenarioBundleInvalidError("scenario service checksum differs")
                payloads[service] = payload
            initial_value = manifest.get("initialTime")
            if not isinstance(initial_value, str):
                raise ScenarioBundleInvalidError("scenario initial time is invalid")
            initial = datetime.fromisoformat(initial_value.replace("Z", "+00:00"))
            if initial.utcoffset() is None:
                raise ScenarioBundleInvalidError("scenario initial time has no UTC offset")
            return ScenarioBundle(scenario_id, version, initial, payloads)
        except ScenarioBundleInvalidError, ScenarioVersionNotFoundError:
            raise
        except Exception as error:
            raise ScenarioBundleInvalidError("scenario bundle is invalid") from error


class ControlResetStore:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def begin(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                state = ScenarioState(singleton_id=1, mode="uninitialised", active_epoch="none")
                session.add(state)
            if state.mode not in {"active", "uninitialised"} and not (
                state.mode == "error" and state.pending_epoch is None
            ):
                raise ApiError(ErrorCode.CONFLICT, "another reset is active", status_code=409)
            state.mode = "preparing"
            state.pending_epoch = epoch
            state.pending_scenario_id = bundle.scenario_id
            state.pending_scenario_version = bundle.version
            state.pending_random_seed = seed
            state.pending_manifest_checksum = bundle.checksum(seed)
            await session.execute(delete(FaultActivation))
            await session.execute(delete(FaultRule))
            clock = await session.get(VirtualClock, 1)
            if clock is None:
                session.add(VirtualClock(singleton_id=1, now=bundle.initial_time))
            else:
                clock.now = bundle.initial_time
            session.add(
                ResetRun(
                    reset_id=new_id("rst"),
                    scenario_id=bundle.scenario_id,
                    scenario_version=bundle.version,
                    random_seed=seed,
                    scenario_epoch=epoch,
                    state="preparing",
                    manifest_checksum=bundle.checksum(seed),
                )
            )

    async def commit(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None or state.pending_epoch != epoch or state.mode != "preparing":
                raise RuntimeError("control reset epoch differs")
            state.active_epoch = epoch
            state.mode = "finalizing"
            state.scenario_id = state.pending_scenario_id
            state.scenario_version = state.pending_scenario_version
            state.random_seed = state.pending_random_seed
            state.manifest_checksum = state.pending_manifest_checksum
            run = await session.scalar(select(ResetRun).where(ResetRun.scenario_epoch == epoch))
            if run is None:
                raise RuntimeError("control reset run is missing")
            run.state = "finalizing"

    async def finalize(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is not None and state.active_epoch == epoch and state.mode == "active":
                return
            if (
                state is None
                or state.active_epoch != epoch
                or state.pending_epoch != epoch
                or state.mode not in {"finalizing", "error"}
            ):
                raise RuntimeError("control reset is not ready to finalize")
            state.pending_epoch = None
            self.clear_pending_metadata(state)
            state.mode = "active"
            run = await session.scalar(select(ResetRun).where(ResetRun.scenario_epoch == epoch))
            if run is None:
                raise RuntimeError("control reset run is missing")
            run.state = "committed"
            run.error = None

    async def pending_cleanup(self) -> str | None:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
        if (
            state is not None
            and state.mode in {"finalizing", "error"}
            and state.pending_epoch is not None
            and state.active_epoch == state.pending_epoch
        ):
            return state.pending_epoch
        return None

    async def fail(self, epoch: str, phase: ResetFailurePhase) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is not None and state.pending_epoch == epoch:
                state.mode = "error"
                if phase == "pre_cutover":
                    state.pending_epoch = None
                    self.clear_pending_metadata(state)
            run = await session.scalar(select(ResetRun).where(ResetRun.scenario_epoch == epoch))
            if run is not None:
                run.state = "failed"
                run.error = (
                    "participant reset failed before cutover"
                    if phase == "pre_cutover"
                    else "participant reset cleanup failed"
                )

    @staticmethod
    def clear_pending_metadata(state: ScenarioState) -> None:
        state.pending_scenario_id = None
        state.pending_scenario_version = None
        state.pending_random_seed = None
        state.pending_manifest_checksum = None


def reset_router(
    coordinator: ResetCoordinator,
    repository: ControlRepository,
    settings: ControlSettings,
) -> APIRouter:
    router = APIRouter()
    ControllerAuth = Annotated[None, Depends(require_token(settings.controller_token))]

    @router.post("/control/v1/reset")
    async def reset(request: ResetRequest, _auth: ControllerAuth) -> ResetResult:
        try:
            return await coordinator.reset(request)
        except ScenarioIdInvalidError as error:
            raise ApiError(
                ErrorCode.INVALID_REQUEST,
                "scenario ID is invalid",
                status_code=422,
            ) from error
        except ScenarioNotFoundError as error:
            raise ApiError(
                ErrorCode.NOT_FOUND,
                "scenario was not found",
                status_code=404,
            ) from error
        except ScenarioVersionNotFoundError as error:
            raise ApiError(
                ErrorCode.NOT_FOUND,
                "scenario version was not found",
                status_code=404,
            ) from error

    @router.get("/control/v1/status")
    async def status(_auth: ControllerAuth) -> dict[str, object]:
        state = await repository.state()
        return {
            "scenarioId": state.scenario_id,
            "version": state.scenario_version,
            "randomSeed": state.random_seed,
            "scenarioEpoch": state.active_epoch,
            "manifestChecksum": state.manifest_checksum,
            "mode": state.mode,
            "pendingEpoch": state.pending_epoch,
            "recoveryRequired": (
                state.mode in {"finalizing", "error"}
                and state.pending_epoch is not None
                and state.active_epoch == state.pending_epoch
            ),
            "now": await repository.now(),
        }

    return router
