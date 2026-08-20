import asyncio
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
)
from enterprise_twins.common.http.errors import ApiError
from enterprise_twins.services.control.reset import ResetCoordinator, ScenarioBundle, derive_seed


class Participant:
    def __init__(
        self,
        name: str,
        fail_on: str | None = None,
        *,
        finalize_failures: int = 0,
        report_schema_version: str | None = None,
        report_aliases: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.fail_on = fail_on
        self.calls: list[tuple[str, str]] = []
        self.active_epoch = "epoch_old"
        self.finalize_failures = finalize_failures
        self.report_schema_version = report_schema_version
        self.report_aliases = report_aliases

    async def prepare(self, epoch: str) -> None:
        self.calls.append(("prepare", epoch))

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        self.calls.append(("load", request.scenario_epoch))
        if self.fail_on == "load":
            raise RuntimeError(f"{self.name} load failed")
        return ParticipantReport(
            service=self.name,
            schemaVersion=self.report_schema_version or str(request.payload["schemaVersion"]),
            counts=request.payload["expectedCounts"],
            aliases=(
                self.report_aliases
                if self.report_aliases is not None
                else request.payload.get("aliases", {})
            ),
            checksum=request.checksum,
        )

    async def commit(self, epoch: str) -> None:
        self.calls.append(("commit", epoch))
        if self.fail_on == "commit":
            raise RuntimeError(f"{self.name} commit failed")
        self.active_epoch = epoch

    async def abort(self, epoch: str) -> None:
        self.calls.append(("abort", epoch))
        if self.active_epoch == epoch:
            self.active_epoch = "epoch_old"

    async def finalize(self, epoch: str) -> None:
        self.calls.append(("finalize", epoch))
        if self.finalize_failures:
            self.finalize_failures -= 1
            raise RuntimeError(f"{self.name} finalize failed")
        if self.fail_on == "finalize":
            raise RuntimeError(f"{self.name} finalize failed")


class InterruptibleParticipant(Participant):
    def __init__(
        self,
        name: str,
        *,
        cancel_on: str | None = None,
        fail_on: str | None = None,
        abort_failures: int = 0,
        abort_always: bool = False,
    ) -> None:
        super().__init__(name, fail_on=fail_on)
        self.cancel_on = cancel_on
        self.abort_failures = abort_failures
        self.abort_always = abort_always

    async def prepare(self, epoch: str) -> None:
        await super().prepare(epoch)
        if self.cancel_on == "prepare":
            raise asyncio.CancelledError

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        if self.cancel_on == "load":
            self.calls.append(("load", request.scenario_epoch))
            raise asyncio.CancelledError
        return await super().load(request)

    async def commit(self, epoch: str) -> None:
        if self.cancel_on == "commit":
            self.calls.append(("commit", epoch))
            raise asyncio.CancelledError
        await super().commit(epoch)

    async def abort(self, epoch: str) -> None:
        self.calls.append(("abort", epoch))
        if self.abort_always or self.abort_failures:
            if self.abort_failures:
                self.abort_failures -= 1
            raise RuntimeError("sensitive participant abort failure")
        if self.active_epoch == epoch:
            self.active_epoch = "epoch_old"


class DurableCoordinatorStore:
    def __init__(self, *, cancel_begin: bool = False, control_commit: str = "normal") -> None:
        self.mode = "active"
        self.active_epoch = "epoch_old"
        self.pending_epoch: str | None = None
        self.cancel_begin = cancel_begin
        self.control_commit = control_commit
        self.begin_calls = 0
        self.finalize_calls: list[str] = []

    async def begin(self, epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        self.begin_calls += 1
        self.mode = "preparing"
        self.pending_epoch = epoch
        if self.cancel_begin:
            self.cancel_begin = False
            raise asyncio.CancelledError

    async def commit(self, epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        if self.control_commit == "rolled_back":
            self.control_commit = "normal"
            raise asyncio.CancelledError
        self.active_epoch = epoch
        self.mode = "finalizing"
        if self.control_commit == "committed":
            self.control_commit = "normal"
            raise asyncio.CancelledError

    async def fail(self, epoch: str, phase: str) -> None:
        if self.pending_epoch != epoch:
            return
        if phase == "pre_cutover" and self.active_epoch != epoch:
            self.mode = "aborting"
        elif phase == "cleanup" and self.active_epoch == epoch:
            self.mode = "error"

    async def finalize(self, epoch: str) -> None:
        assert self.pending_epoch == epoch
        assert self.active_epoch == epoch
        self.finalize_calls.append(epoch)
        self.pending_epoch = None
        self.mode = "active"

    async def pending_cleanup(self) -> str | None:
        if (
            self.mode in {"finalizing", "error"}
            and self.pending_epoch is not None
            and self.active_epoch == self.pending_epoch
        ):
            return self.pending_epoch
        return None

    async def finalize_abort(self, epoch: str) -> None:
        assert self.mode == "aborting"
        assert self.pending_epoch == epoch
        assert self.active_epoch != epoch
        self.pending_epoch = None
        self.mode = "error"

    async def pending_abort(self) -> str | None:
        if (
            self.mode in {"preparing", "aborting"}
            and self.pending_epoch is not None
            and self.active_epoch != self.pending_epoch
        ):
            return self.pending_epoch
        return None


class CancellationGates:
    def __init__(self, *stages: str) -> None:
        self.entered = {stage: asyncio.Event() for stage in stages}
        self.release = {stage: asyncio.Event() for stage in stages}

    async def at(self, stage: str) -> None:
        if stage not in self.entered:
            return
        self.entered[stage].set()
        await self.release[stage].wait()

    async def wait_until_entered(self, stage: str) -> None:
        await asyncio.wait_for(self.entered[stage].wait(), timeout=1)

    def allow(self, stage: str) -> None:
        self.release[stage].set()


class GatedParticipant(Participant):
    def __init__(
        self,
        name: str,
        gates: CancellationGates,
        *,
        fail_load: bool = False,
        abort_fails: bool = False,
    ) -> None:
        super().__init__(name)
        self.gates = gates
        self.fail_load = fail_load
        self.abort_fails = abort_fails

    async def prepare(self, epoch: str) -> None:
        self.calls.append(("prepare", epoch))
        await self.gates.at("prepare")

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        self.calls.append(("load", request.scenario_epoch))
        await self.gates.at("load")
        if self.fail_load:
            raise RuntimeError("load failed")
        return ParticipantReport(
            service=self.name,
            schemaVersion=str(request.payload["schemaVersion"]),
            counts=request.payload["expectedCounts"],
            aliases=request.payload.get("aliases", {}),
            checksum=request.checksum,
        )

    async def commit(self, epoch: str) -> None:
        self.calls.append(("commit", epoch))
        await self.gates.at("participant_commit")
        self.active_epoch = epoch

    async def abort(self, epoch: str) -> None:
        self.calls.append(("abort", epoch))
        await self.gates.at("abort")
        if self.abort_fails:
            raise RuntimeError("private abort failure")
        if self.active_epoch == epoch:
            self.active_epoch = "epoch_old"

    async def finalize(self, epoch: str) -> None:
        self.calls.append(("finalize", epoch))
        await self.gates.at("participant_finalize_before")
        await self.gates.at("participant_finalize_after")


class GatedStore(DurableCoordinatorStore):
    def __init__(
        self,
        gates: CancellationGates,
        *,
        mode: str = "active",
        active_epoch: str = "epoch_old",
        pending_epoch: str | None = None,
    ) -> None:
        super().__init__()
        self.gates = gates
        self.mode = mode
        self.active_epoch = active_epoch
        self.pending_epoch = pending_epoch

    async def begin(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        await self.gates.at("begin_before")
        await super().begin(epoch, bundle, seed)
        await self.gates.at("begin_after")

    async def commit(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        await self.gates.at("control_commit_before")
        await super().commit(epoch, bundle, seed)
        await self.gates.at("control_commit_after")

    async def finalize(self, epoch: str) -> None:
        await self.gates.at("control_finalize")
        await super().finalize(epoch)


def durable_coordinator(
    participants: dict[str, Participant],
    bundle: ScenarioBundle,
    store: DurableCoordinatorStore,
) -> ResetCoordinator:
    return ResetCoordinator(
        participants,
        lambda _sid, _version: bundle,
        store.begin,
        store.commit,
        store.fail,  # type: ignore[arg-type]
        store.finalize,
        store.pending_cleanup,
        store.finalize_abort,
        store.pending_abort,
    )


def one_participant_bundle() -> ScenarioBundle:
    return ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"schemaVersion": "1", "expectedCounts": {}}},
    )


async def assert_task_waits_for_release(task: asyncio.Task[object]) -> None:
    for _ in range(3):
        await asyncio.sleep(0)
    assert not task.done()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "expected_active_epoch"),
    [
        ("begin_before", "epoch_old"),
        ("begin_after", "epoch_old"),
        ("prepare", "epoch_old"),
        ("load", "epoch_old"),
        ("participant_commit", "epoch_old"),
        ("control_commit_before", "epoch_old"),
        ("control_commit_after", "epoch_new"),
        ("participant_finalize_before", "epoch_new"),
        ("participant_finalize_after", "epoch_new"),
        ("control_finalize", "epoch_new"),
    ],
)
async def test_actual_task_cancellation_waits_for_durable_reset_outcome(
    stage: str,
    expected_active_epoch: str,
) -> None:
    gates = CancellationGates(stage)
    store = GatedStore(gates)
    identity = GatedParticipant("identity", gates)
    coordinator = durable_coordinator({"identity": identity}, one_participant_bundle(), store)
    task = asyncio.create_task(
        coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))
    )

    await gates.wait_until_entered(stage)
    task.cancel()
    if stage in {
        "participant_finalize_before",
        "participant_finalize_after",
        "control_finalize",
    }:
        await assert_task_waits_for_release(task)
    gates.allow(stage)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    assert store.pending_epoch is None
    if expected_active_epoch == "epoch_old":
        assert store.active_epoch == identity.active_epoch == "epoch_old"
        assert store.mode == ("active" if stage == "begin_before" else "error")
    else:
        assert store.active_epoch == identity.active_epoch
        assert store.active_epoch != "epoch_old"
        assert store.mode == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["cleanup", "abort"])
async def test_actual_cancellation_waits_for_pending_preflight_recovery(
    recovery: str,
) -> None:
    stage = "participant_finalize_before" if recovery == "cleanup" else "abort"
    gates = CancellationGates(stage)
    pending_epoch = "epoch_pending"
    store = GatedStore(
        gates,
        mode="finalizing" if recovery == "cleanup" else "aborting",
        active_epoch=pending_epoch if recovery == "cleanup" else "epoch_old",
        pending_epoch=pending_epoch,
    )
    identity = GatedParticipant("identity", gates)
    if recovery == "cleanup":
        identity.active_epoch = pending_epoch
    coordinator = durable_coordinator({"identity": identity}, one_participant_bundle(), store)
    task = asyncio.create_task(
        coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))
    )

    await gates.wait_until_entered(stage)
    task.cancel()
    await assert_task_waits_for_release(task)
    gates.allow(stage)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    assert store.pending_epoch is None
    assert store.begin_calls == 0
    assert store.mode == ("active" if recovery == "cleanup" else "error")


@pytest.mark.asyncio
async def test_actual_cancellation_during_ordinary_failure_compensation_waits_for_abort() -> None:
    gates = CancellationGates("abort")
    store = GatedStore(gates)
    identity = GatedParticipant("identity", gates, fail_load=True)
    coordinator = durable_coordinator({"identity": identity}, one_participant_bundle(), store)
    task = asyncio.create_task(
        coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))
    )

    await gates.wait_until_entered("abort")
    task.cancel()
    await assert_task_waits_for_release(task)
    gates.allow("abort")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.pending_epoch is None
    assert store.mode == "error"


@pytest.mark.asyncio
async def test_repeated_actual_cancellation_never_cancels_the_compensation_child() -> None:
    gates = CancellationGates("load", "abort")
    store = GatedStore(gates)
    identity = GatedParticipant("identity", gates)
    coordinator = durable_coordinator({"identity": identity}, one_participant_bundle(), store)
    task = asyncio.create_task(
        coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))
    )

    await gates.wait_until_entered("load")
    task.cancel()
    gates.allow("load")
    await gates.wait_until_entered("abort")
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await assert_task_waits_for_release(task)
    gates.allow("abort")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.pending_epoch is None
    assert store.mode == "error"


@pytest.mark.asyncio
async def test_cancelled_reset_keeps_persisted_recovery_when_abort_durably_fails() -> None:
    gates = CancellationGates("load", "abort")
    store = GatedStore(gates)
    identity = GatedParticipant("identity", gates, abort_fails=True)
    coordinator = durable_coordinator({"identity": identity}, one_participant_bundle(), store)
    task = asyncio.create_task(
        coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))
    )

    await gates.wait_until_entered("load")
    task.cancel()
    gates.allow("load")
    await gates.wait_until_entered("abort")
    gates.allow("abort")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.mode == "aborting"
    assert store.pending_epoch is not None


@pytest.mark.asyncio
async def test_reset_is_ordered_and_same_inputs_have_same_checksum() -> None:
    identity = Participant("identity")
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"schemaVersion": "1", "expectedCounts": {"clients": 2}},
            "crm": {"schemaVersion": "1", "expectedCounts": {"customers": 3}},
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)
    first = await coordinator.reset(
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7)
    )
    second = await coordinator.reset(
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7)
    )
    different_seed = await coordinator.reset(
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=8)
    )

    assert first.manifest_checksum == second.manifest_checksum
    assert first.manifest_checksum != different_seed.manifest_checksum
    assert first.random_seed == 7
    assert different_seed.random_seed == 8
    assert first.scenario_epoch != second.scenario_epoch
    assert [name for name, _epoch in identity.calls[:4]] == [
        "prepare",
        "load",
        "commit",
        "finalize",
    ]
    assert [name for name, _epoch in crm.calls[:4]] == [
        "prepare",
        "load",
        "commit",
        "finalize",
    ]


@pytest.mark.asyncio
async def test_failed_load_aborts_every_participant_and_marks_estate_unhealthy() -> None:
    identity = Participant("identity")
    crm = Participant("crm", fail_on="load")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"schemaVersion": "1", "expectedCounts": {}},
            "crm": {"schemaVersion": "1", "expectedCounts": {}},
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)

    with pytest.raises(RuntimeError, match="crm load failed"):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert identity.calls[-1][0] == "abort"
    assert crm.calls[-1][0] == "abort"
    assert coordinator.test_mode == "error"


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["begin", "prepare", "load", "participant_commit"])
async def test_cancellation_completes_durable_pre_cutover_abort_before_propagating(
    stage: str,
) -> None:
    store = DurableCoordinatorStore(cancel_begin=stage == "begin")
    identity = InterruptibleParticipant(
        "identity", cancel_on={"prepare": "prepare", "load": "load"}.get(stage)
    )
    participants: dict[str, Participant] = {"identity": identity}
    bundle = one_participant_bundle()
    crm: InterruptibleParticipant | None = None
    if stage == "participant_commit":
        crm = InterruptibleParticipant("crm", cancel_on="commit")
        participants["crm"] = crm
        bundle = ScenarioBundle(
            scenario_id="platform-contracts",
            version=1,
            initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
            payloads={
                "identity": {"schemaVersion": "1", "expectedCounts": {}},
                "crm": {"schemaVersion": "1", "expectedCounts": {}},
            },
        )
    coordinator = durable_coordinator(
        participants,
        bundle,
        store,
    )

    with pytest.raises(asyncio.CancelledError):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert store.mode == "error"
    assert store.pending_epoch is None
    assert identity.active_epoch == "epoch_old"
    assert [action for action, _epoch in identity.calls].count("abort") == 1
    if crm is not None:
        assert crm.active_epoch == "epoch_old"
        assert [action for action, _epoch in crm.calls].count("abort") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("control_commit", "expected_active", "expected_finalizes", "expected_aborts"),
    [
        ("rolled_back", "epoch_old", 0, 1),
        ("committed", "new", 1, 0),
    ],
)
async def test_control_commit_cancellation_recovers_from_the_persisted_transaction_outcome(
    control_commit: str,
    expected_active: str,
    expected_finalizes: int,
    expected_aborts: int,
) -> None:
    store = DurableCoordinatorStore(control_commit=control_commit)
    identity = InterruptibleParticipant("identity")
    coordinator = durable_coordinator(
        {"identity": identity},
        one_participant_bundle(),
        store,
    )

    with pytest.raises(asyncio.CancelledError):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    if expected_active == "new":
        assert store.active_epoch != "epoch_old"
        assert identity.active_epoch == store.active_epoch
    else:
        assert store.active_epoch == identity.active_epoch == "epoch_old"
    assert store.mode in {"active", "error"}
    assert store.pending_epoch is None
    assert len(store.finalize_calls) == expected_finalizes
    assert [action for action, _epoch in identity.calls].count("abort") == expected_aborts


@pytest.mark.asyncio
async def test_abort_failure_is_retryable_and_next_reset_recovers_same_epoch_before_beginning() -> (
    None
):
    store = DurableCoordinatorStore()
    identity = InterruptibleParticipant("identity", fail_on="load", abort_failures=1)
    coordinator = durable_coordinator(
        {"identity": identity},
        one_participant_bundle(),
        store,
    )
    request = ResetRequest(scenarioId="platform-contracts", version=1)

    with pytest.raises(ApiError) as failed:
        await coordinator.reset(request)
    failed_epoch = store.pending_epoch

    assert failed.value.status_code == 503
    assert failed.value.retryable is True
    assert failed.value.details == {"phase": "abort"}
    assert "sensitive" not in str(failed.value)
    assert store.mode == "aborting"
    assert failed_epoch is not None
    identity.fail_on = None

    result = await coordinator.reset(request)

    assert result.scenario_epoch != failed_epoch
    assert store.mode == "active"
    assert store.pending_epoch is None
    abort_calls = [call for call in identity.calls if call == ("abort", failed_epoch)]
    assert len(abort_calls) == 2
    recovered_abort_index = identity.calls.index(("abort", failed_epoch), 1)
    new_prepare_index = identity.calls.index(("prepare", result.scenario_epoch))
    assert recovered_abort_index < new_prepare_index


@pytest.mark.asyncio
async def test_repeated_abort_recovery_failure_retains_epoch_and_never_begins_again() -> None:
    store = DurableCoordinatorStore()
    identity = InterruptibleParticipant("identity", fail_on="load", abort_always=True)
    coordinator = durable_coordinator(
        {"identity": identity},
        one_participant_bundle(),
        store,
    )
    request = ResetRequest(scenarioId="platform-contracts", version=1)

    with pytest.raises(ApiError) as first:
        await coordinator.reset(request)
    failed_epoch = store.pending_epoch
    identity.fail_on = None
    with pytest.raises(ApiError) as second:
        await coordinator.reset(request)

    assert first.value.details == second.value.details == {"phase": "abort"}
    assert store.mode == "aborting"
    assert store.pending_epoch == failed_epoch
    assert failed_epoch is not None
    assert store.begin_calls == 1
    assert identity.calls.count(("abort", failed_epoch)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("participant", "expected_failure"),
    [
        (
            Participant("identity", report_schema_version="2"),
            "schemaVersion",
        ),
        (
            Participant("identity", report_aliases={"primary": "unexpected"}),
            "aliases",
        ),
    ],
)
async def test_report_schema_and_alias_mismatch_aborts_before_any_commit(
    participant: Participant,
    expected_failure: str,
) -> None:
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {
                "schemaVersion": "1",
                "expectedCounts": {},
                "aliases": {"primary": "client-1"},
            }
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": participant}, bundle)

    with pytest.raises(RuntimeError, match=expected_failure):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert all(action != "commit" for action, _epoch in participant.calls)
    assert participant.calls[-1][0] == "abort"


@pytest.mark.asyncio
async def test_finalize_failure_keeps_every_participant_on_the_new_epoch_without_abort() -> None:
    identity = Participant("identity", fail_on="finalize")
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"schemaVersion": "1", "expectedCounts": {}},
            "crm": {"schemaVersion": "1", "expectedCounts": {}},
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)

    with pytest.raises(ApiError) as raised:
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert raised.value.status_code == 503
    assert raised.value.retryable is True
    assert raised.value.details == {"phase": "cleanup"}
    assert identity.active_epoch == crm.active_epoch
    assert identity.active_epoch != "epoch_old"
    assert all(action != "abort" for action, _epoch in identity.calls)
    assert all(action != "abort" for action, _epoch in crm.calls)
    assert coordinator.test_mode == "cleanup_error"


@pytest.mark.asyncio
async def test_participant_services_must_exactly_match_bundle_before_reset_begins() -> None:
    identity = Participant("identity")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"schemaVersion": "1", "expectedCounts": {}},
            "crm": {"schemaVersion": "1", "expectedCounts": {}},
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": identity}, bundle)

    with pytest.raises(ValueError, match="participant services differ"):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert identity.calls == []


def test_default_seed_is_deterministic_and_fits_postgresql_bigint() -> None:
    first = derive_seed("platform-contracts", 1)
    second = derive_seed("platform-contracts", 1)

    assert first == second
    assert 0 <= first <= 9_223_372_036_854_775_807


@pytest.mark.asyncio
async def test_omitted_seed_is_resolved_into_result_and_manifest_checksum() -> None:
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={},
    )
    coordinator = ResetCoordinator.for_test({}, bundle)

    result = await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert result.random_seed == 4_470_957_409_635_312_983
    assert (
        result.manifest_checksum
        == "23730c373fbf37ddb6ba71af98ba49dd809032700c6757ddd0812ed4c85056cf"
    )


def test_explicit_seed_is_limited_to_non_negative_postgresql_bigint() -> None:
    assert ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=0).random_seed == 0
    maximum = 9_223_372_036_854_775_807
    assert (
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=maximum).random_seed
        == maximum
    )

    with pytest.raises(ValidationError):
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=-1)
    with pytest.raises(ValidationError):
        ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=maximum + 1)


@pytest.mark.parametrize(
    "scenario_id",
    ["../escape", "UPPER", "-leading", "x" * 81, ""],
)
def test_reset_request_rejects_scenario_ids_outside_catalogue_key_contract(
    scenario_id: str,
) -> None:
    with pytest.raises(ValidationError):
        ResetRequest(scenarioId=scenario_id, version=1)


@pytest.mark.asyncio
async def test_next_reset_recovers_persisted_cleanup_before_starting_new_epoch() -> None:
    identity = Participant("identity", finalize_failures=1)
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"schemaVersion": "1", "expectedCounts": {}},
            "crm": {"schemaVersion": "1", "expectedCounts": {}},
        },
    )
    pending_epoch: str | None = None

    async def begin(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        return None

    async def commit(epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        nonlocal pending_epoch
        pending_epoch = epoch

    async def fail(_epoch: str, _phase: str) -> None:
        return None

    async def finalize(epoch: str) -> None:
        nonlocal pending_epoch
        assert pending_epoch == epoch
        pending_epoch = None

    async def pending() -> str | None:
        return pending_epoch

    async def finalize_abort(_epoch: str) -> None:
        return None

    async def pending_abort() -> str | None:
        return None

    coordinator = ResetCoordinator(
        {"identity": identity, "crm": crm},
        lambda _sid, _version: bundle,
        begin,
        commit,
        fail,  # type: ignore[arg-type]
        finalize,
        pending,
        finalize_abort,
        pending_abort,
    )
    request = ResetRequest(scenarioId="platform-contracts", version=1)

    with pytest.raises(ApiError):
        await coordinator.reset(request)
    failed_epoch = pending_epoch
    assert failed_epoch is not None

    result = await coordinator.reset(request)

    assert result.scenario_epoch != failed_epoch
    assert pending_epoch is None
    assert identity.calls.count(("finalize", failed_epoch)) == 2
    assert crm.calls.count(("finalize", failed_epoch)) == 2
    second_prepare = identity.calls.index(("prepare", result.scenario_epoch))
    recovered_finalize = identity.calls.index(("finalize", failed_epoch), 4)
    assert recovered_finalize < second_prepare


@pytest.mark.asyncio
async def test_repeated_cleanup_recovery_failure_retains_epoch_and_does_not_begin_new_reset() -> (
    None
):
    identity = Participant("identity", fail_on="finalize")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"schemaVersion": "1", "expectedCounts": {}}},
    )
    pending_epoch: str | None = None
    begin_calls = 0

    async def begin(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        nonlocal begin_calls
        begin_calls += 1

    async def commit(epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        nonlocal pending_epoch
        pending_epoch = epoch

    async def fail(_epoch: str, _phase: str) -> None:
        return None

    async def finalize(_epoch: str) -> None:
        raise AssertionError("Control must not finalize while a participant still fails")

    async def pending() -> str | None:
        return pending_epoch

    async def finalize_abort(_epoch: str) -> None:
        return None

    async def pending_abort() -> str | None:
        return None

    coordinator = ResetCoordinator(
        {"identity": identity},
        lambda _sid, _version: bundle,
        begin,
        commit,
        fail,  # type: ignore[arg-type]
        finalize,
        pending,
        finalize_abort,
        pending_abort,
    )
    request = ResetRequest(scenarioId="platform-contracts", version=1)

    with pytest.raises(ApiError):
        await coordinator.reset(request)
    failed_epoch = pending_epoch
    with pytest.raises(ApiError) as retry:
        await coordinator.reset(request)

    assert retry.value.status_code == 503
    assert pending_epoch == failed_epoch
    assert begin_calls == 1


@pytest.mark.asyncio
async def test_cleanup_response_stays_retryable_when_failure_marker_cannot_be_updated() -> None:
    identity = Participant("identity", fail_on="finalize")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"schemaVersion": "1", "expectedCounts": {}}},
    )

    async def begin(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        return None

    async def commit(_epoch: str, _bundle: ScenarioBundle, _seed: int) -> None:
        return None

    async def fail(_epoch: str, _phase: str) -> None:
        raise RuntimeError("sensitive database failure")

    async def finalize(_epoch: str) -> None:
        return None

    async def pending() -> str | None:
        return None

    async def finalize_abort(_epoch: str) -> None:
        return None

    async def pending_abort() -> str | None:
        return None

    coordinator = ResetCoordinator(
        {"identity": identity},
        lambda _sid, _version: bundle,
        begin,
        commit,
        fail,  # type: ignore[arg-type]
        finalize,
        pending,
        finalize_abort,
        pending_abort,
    )

    with pytest.raises(ApiError) as raised:
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert raised.value.status_code == 503
    assert raised.value.retryable is True
    assert raised.value.details == {"phase": "cleanup"}
    assert "sensitive" not in str(raised.value)
