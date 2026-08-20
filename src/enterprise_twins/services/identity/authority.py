from urllib.parse import urlsplit

from fastapi import Request

from enterprise_twins.common.http.errors import ApiError, ErrorCode


def invalid_authority() -> ApiError:
    return ApiError(
        ErrorCode.INVALID_REQUEST,
        "request authority is not configured",
        status_code=400,
    )


def select_issuer_origin(request: Request, allowed_origins: tuple[str, ...]) -> str:
    host_values = [
        value for name, value in request.scope.get("headers", []) if name.lower() == b"host"
    ]
    if len(host_values) != 1:
        raise invalid_authority()
    try:
        authority = host_values[0].decode("ascii")
        parsed = urlsplit(f"{request.scope['scheme']}://{authority}")
        port = parsed.port or (80 if parsed.scheme == "http" else 443)
    except KeyError, UnicodeDecodeError, ValueError:
        raise invalid_authority() from None
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise invalid_authority()
    candidate = f"{parsed.scheme}://{parsed.hostname.lower()}:{port}"
    if candidate not in allowed_origins:
        raise invalid_authority()
    return candidate
