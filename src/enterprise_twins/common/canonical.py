import hashlib
import json
from datetime import date, datetime
from enum import Enum


def _json_default(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot encode {type(value).__name__} as canonical JSON")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode()


def sha256_hex(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()
