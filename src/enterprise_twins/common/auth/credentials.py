from typing import Annotated

from pydantic import AfterValidator


def validate_private_credential(value: str) -> str:
    if not value or any(character.isspace() for character in value):
        raise ValueError("private credential must be non-empty and contain no whitespace")
    return value


type PrivateCredential = Annotated[str, AfterValidator(validate_private_credential)]


def parse_bearer(authorization: str | None) -> str | None:
    if authorization is None or not authorization.startswith("Bearer "):
        return None
    token = authorization[len("Bearer ") :]
    if not token or any(character.isspace() for character in token):
        return None
    return token
