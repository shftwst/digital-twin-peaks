import hmac
from collections.abc import Callable

from fastapi import Header

from enterprise_twins.common.auth.credentials import parse_bearer, validate_private_credential
from enterprise_twins.common.http.errors import ApiError, ErrorCode


def require_token(expected: str) -> Callable[[str | None], None]:
    validate_private_credential(expected)

    def check(authorization: str | None = Header(default=None)) -> None:
        supplied = parse_bearer(authorization)
        if supplied is None or not hmac.compare_digest(
            supplied.encode("ascii"), expected.encode("ascii")
        ):
            raise ApiError(ErrorCode.UNAUTHENTICATED, "invalid private credential", status_code=401)

    return check
