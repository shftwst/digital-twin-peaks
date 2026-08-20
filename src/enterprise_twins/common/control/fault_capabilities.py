from enterprise_twins.common.control.contracts import (
    FaultEffect,
    FaultPhase,
    FaultProbe,
    FaultRuleCreate,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode

PHASE_EFFECTS: dict[FaultPhase, frozenset[FaultEffect]] = {
    FaultPhase.BEFORE_VALIDATION: frozenset(
        {
            FaultEffect.MALFORMED_TRANSPORT,
            FaultEffect.UNAUTHENTICATED,
            FaultEffect.RATE_LIMITED,
        }
    ),
    FaultPhase.BEFORE_COMMIT: frozenset(
        {
            FaultEffect.TEMPORARY_FAILURE,
            FaultEffect.DELAY,
            FaultEffect.TIMEOUT,
        }
    ),
    FaultPhase.AFTER_COMMIT: frozenset(
        {
            FaultEffect.TIMEOUT,
            FaultEffect.CONNECTION_LOSS,
            FaultEffect.MALFORMED_RESPONSE,
        }
    ),
    FaultPhase.READ: frozenset(
        {
            FaultEffect.STALE_VERSION,
            FaultEffect.TEMPORARY_ABSENCE,
            FaultEffect.PAGINATION_CHANGE,
        }
    ),
    FaultPhase.EVENT_DELIVERY: frozenset(
        {
            FaultEffect.DELAY,
            FaultEffect.DUPLICATE,
            FaultEffect.REORDER,
            FaultEffect.SUPPRESS,
            FaultEffect.RETRY,
        }
    ),
    FaultPhase.DOMAIN_COMPLETION: frozenset(
        {
            FaultEffect.FAILED_REFUND,
            FaultEffect.DELAYED_SETTLEMENT,
            FaultEffect.BOUNCE,
            FaultEffect.DEFER,
            FaultEffect.DROP,
        }
    ),
}

type FaultCapabilityKey = tuple[str, str, FaultPhase]

FAULT_CAPABILITIES: dict[FaultCapabilityKey, frozenset[FaultEffect]] = {
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


def unsupported_fault() -> ApiError:
    return ApiError(
        ErrorCode.INVALID_REQUEST,
        "fault rule is not supported by this twin slice",
        status_code=422,
    )


def validate_fault_rule(request: FaultRuleCreate) -> None:
    if request.effect not in PHASE_EFFECTS[request.phase]:
        raise unsupported_fault()
    effects = FAULT_CAPABILITIES.get((request.target_service, request.operation, request.phase))
    if effects is None or request.effect not in effects:
        raise unsupported_fault()


def validate_fault_probe(probe: FaultProbe) -> None:
    if (probe.target_service, probe.operation, probe.phase) not in FAULT_CAPABILITIES:
        raise unsupported_fault()
