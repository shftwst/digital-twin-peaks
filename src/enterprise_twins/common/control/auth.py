import hmac
from collections.abc import Callable

from fastapi import Header

from enterprise_twins.common.http.errors import ApiError, ErrorCode


def require_token(expected: str) -> Callable[[str | None], None]:
    def check(authorization: str | None = Header(default=None)) -> None:
        supplied = authorization.removeprefix("Bearer ") if authorization else ""
        if not hmac.compare_digest(supplied, expected):
            raise ApiError(ErrorCode.UNAUTHENTICATED, "invalid private credential", status_code=401)

    return check
