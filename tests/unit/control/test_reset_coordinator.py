from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
)
from enterprise_twins.services.control.reset import ResetCoordinator, ScenarioBundle, derive_seed


class Participant:
    def __init__(self, name: str, fail_on: str | None = None) -> None:
        self.name = name
        self.fail_on = fail_on
        self.calls: list[tuple[str, str]] = []
        self.active_epoch = "epoch_old"

    async def prepare(self, epoch: str) -> None:
        self.calls.append(("prepare", epoch))

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        self.calls.append(("load", request.scenario_epoch))
        if self.fail_on == "load":
            raise RuntimeError(f"{self.name} load failed")
        return ParticipantReport(
            service=self.name,
            schemaVersion="1",
            counts=request.payload["expectedCounts"],
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
        if self.fail_on == "finalize":
            raise RuntimeError(f"{self.name} finalize failed")


@pytest.mark.asyncio
async def test_reset_is_ordered_and_same_inputs_have_same_checksum() -> None:
    identity = Participant("identity")
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"expectedCounts": {"clients": 2}},
            "crm": {"expectedCounts": {"customers": 3}},
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
        payloads={"identity": {"expectedCounts": {}}, "crm": {"expectedCounts": {}}},
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)

    with pytest.raises(RuntimeError, match="crm load failed"):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert identity.calls[-1][0] == "abort"
    assert crm.calls[-1][0] == "abort"
    assert coordinator.test_mode == "error"


@pytest.mark.asyncio
async def test_finalize_failure_keeps_every_participant_on_the_new_epoch_without_abort() -> None:
    identity = Participant("identity", fail_on="finalize")
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"expectedCounts": {}}, "crm": {"expectedCounts": {}}},
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)

    with pytest.raises(RuntimeError, match="finalize failed"):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

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
        payloads={"identity": {"expectedCounts": {}}, "crm": {"expectedCounts": {}}},
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
