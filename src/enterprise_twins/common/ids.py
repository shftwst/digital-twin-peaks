from uuid import uuid7


def new_id(prefix: str) -> str:
    if not prefix.isalpha() or not prefix.islower():
        raise ValueError("identifier prefix must contain lowercase letters")
    return f"{prefix}_{uuid7().hex}"
