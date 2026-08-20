import re
from typing import Annotated

from pydantic import AfterValidator

TOKEN68 = re.compile(r"[A-Za-z0-9._~+/\-]+={0,}", re.ASCII)


def validate_private_credential(value: str) -> str:
    if TOKEN68.fullmatch(value) is None:
        raise ValueError("private credential must use the HTTP-safe token68 grammar")
    return value


type PrivateCredential = Annotated[str, AfterValidator(validate_private_credential)]


def parse_bearer(authorization: str | None) -> str | None:
    if authorization is None or not authorization.startswith("Bearer "):
        return None
    token = authorization[len("Bearer ") :]
    try:
        return validate_private_credential(token)
    except ValueError:
        return None
