from typing import Annotated
from urllib.parse import urlsplit

from pydantic import AfterValidator


def canonical_http_origin(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ValueError("issuer origin is invalid") from error
    if (
        parsed.scheme != "http"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path
        or parsed.hostname is None
        or port is None
    ):
        raise ValueError("issuer origin must be a canonical HTTP origin")
    try:
        parsed.hostname.encode("ascii")
    except UnicodeEncodeError as error:
        raise ValueError("issuer origin host must be ASCII") from error
    canonical = f"http://{parsed.hostname.lower()}:{port}"
    if canonical != value:
        raise ValueError("issuer origin is not canonical")
    return canonical


def validate_issuer_origins(values: tuple[str, ...]) -> tuple[str, ...]:
    canonical = tuple(canonical_http_origin(value) for value in values)
    if not canonical:
        raise ValueError("at least one issuer origin is required")
    if len(set(canonical)) != len(canonical):
        raise ValueError("issuer origins must be unique")
    return canonical


type IssuerOrigins = Annotated[tuple[str, ...], AfterValidator(validate_issuer_origins)]
