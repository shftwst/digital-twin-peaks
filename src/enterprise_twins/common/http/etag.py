import re

from enterprise_twins.common.http.errors import ApiError, ErrorCode

QUOTED_VERSION = re.compile(r'"(0|[1-9][0-9]*)"')


def parse_quoted_version(value: str, *, minimum: int = 0) -> int:
    match = QUOTED_VERSION.fullmatch(value)
    if match is None:
        raise ApiError(
            ErrorCode.INVALID_REQUEST,
            "If-Match is invalid",
            status_code=422,
        )
    version = int(match.group(1))
    if version < minimum:
        raise ApiError(
            ErrorCode.INVALID_REQUEST,
            "If-Match is invalid",
            status_code=422,
        )
    return version
