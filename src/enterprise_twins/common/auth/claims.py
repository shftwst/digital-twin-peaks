from dataclasses import dataclass

from enterprise_twins.common.http.errors import ApiError, ErrorCode


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    actor_type: str
    role: str
    scopes: frozenset[str]
    tenant_id: str
    token_id: str
    scenario_epoch: str

    def require(self, *required: str) -> None:
        missing = sorted(set(required) - self.scopes)
        if missing:
            raise ApiError(
                ErrorCode.FORBIDDEN,
                "required scope is missing",
                status_code=403,
                details={"requiredScopes": missing},
            )
