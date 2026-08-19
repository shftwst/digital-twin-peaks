import base64
import hashlib
import hmac
import json
import re
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from enterprise_twins.common.canonical import canonical_json


class CustomerView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    customer_id: str = Field(alias="customerId")
    display_name: str = Field(alias="displayName")
    primary_email: str = Field(alias="primaryEmail")
    external_reference: str = Field(alias="externalReference")
    account_status: str = Field(alias="accountStatus")
    contact_methods: list[dict[str, object]] = Field(alias="contactMethods")
    external_identifiers: dict[str, str] = Field(alias="externalIdentifiers")
    version: int


class NoteCreate(BaseModel):
    body: str = Field(min_length=1, max_length=10_000)
    association: str = Field(min_length=1, max_length=80)


class NoteView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    note_id: str = Field(alias="noteId")
    customer_id: str = Field(alias="customerId")
    body: str
    association: str
    created_by: str = Field(alias="createdBy")
    created_at: datetime = Field(alias="createdAt")
    archived: bool
    version: int


class CustomerPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[CustomerView]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


class NotePage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[NoteView]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


def b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def b64decode(value: str) -> bytes:
    if not value or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None or len(value) % 4 == 1:
        raise ValueError("cursor encoding is invalid")
    decoded = base64.b64decode(
        value + "=" * (-len(value) % 4),
        altchars=b"-_",
        validate=True,
    )
    if b64encode(decoded) != value:
        raise ValueError("cursor encoding is not canonical")
    return decoded


def encode_cursor_payload(payload: dict[str, object], secret: str) -> str:
    body = canonical_json(payload)
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return f"{b64encode(body)}.{b64encode(digest)}"


def decode_cursor_payload(value: str, secret: str) -> dict[str, object]:
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError("cursor structure is invalid")
    payload = b64decode(parts[0])
    supplied = b64decode(parts[1])
    if len(supplied) != hashlib.sha256().digest_size:
        raise ValueError("cursor signature length is invalid")
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("cursor signature differs")
    decoded = json.loads(payload)
    if not isinstance(decoded, dict) or canonical_json(decoded) != payload:
        raise ValueError("cursor payload is invalid")
    return decoded


def encode_cursor(customer_id: str, secret: str) -> str:
    return encode_cursor_payload({"kind": "crm-customer-list", "customerId": customer_id}, secret)


def decode_cursor(value: str, secret: str) -> str:
    payload = decode_cursor_payload(value, secret)
    if set(payload) != {"kind", "customerId"}:
        raise ValueError("cursor payload shape is invalid")
    if payload["kind"] != "crm-customer-list":
        raise ValueError("cursor kind is invalid")
    customer_id = payload["customerId"]
    if not isinstance(customer_id, str) or not customer_id:
        raise ValueError("cursor customer ID is invalid")
    return customer_id


def cursor_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def encode_note_cursor(
    customer_id: str,
    include_archived: bool,
    scenario_epoch: str,
    created_at: datetime,
    note_id: str,
    secret: str,
) -> str:
    return encode_cursor_payload(
        {
            "kind": "crm-note-list",
            "customerId": customer_id,
            "includeArchived": include_archived,
            "scenarioEpoch": scenario_epoch,
            "createdAt": cursor_timestamp(created_at),
            "noteId": note_id,
        },
        secret,
    )


def decode_note_cursor(
    value: str,
    customer_id: str,
    include_archived: bool,
    scenario_epoch: str,
    secret: str,
) -> tuple[datetime, str]:
    payload = decode_cursor_payload(value, secret)
    if set(payload) != {
        "kind",
        "customerId",
        "includeArchived",
        "scenarioEpoch",
        "createdAt",
        "noteId",
    }:
        raise ValueError("cursor payload shape is invalid")
    if payload["kind"] != "crm-note-list":
        raise ValueError("cursor kind is invalid")
    if (
        payload["customerId"] != customer_id
        or payload["includeArchived"] is not include_archived
        or payload["scenarioEpoch"] != scenario_epoch
    ):
        raise ValueError("cursor context differs")
    created_at_value = payload["createdAt"]
    note_id = payload["noteId"]
    if not isinstance(created_at_value, str) or not isinstance(note_id, str) or not note_id:
        raise ValueError("cursor boundary is invalid")
    try:
        created_at = datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("cursor timestamp is invalid") from error
    if created_at.utcoffset() is None:
        raise ValueError("cursor timestamp has no offset")
    created_at = created_at.astimezone(UTC)
    if cursor_timestamp(created_at) != created_at_value:
        raise ValueError("cursor timestamp is not canonical")
    return created_at, note_id
