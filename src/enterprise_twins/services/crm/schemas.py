import base64
import hashlib
import hmac
import json
from datetime import datetime

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
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def encode_cursor(customer_id: str, secret: str) -> str:
    payload = canonical_json({"customerId": customer_id})
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    return f"{b64encode(payload)}.{b64encode(digest)}"


def decode_cursor(value: str, secret: str) -> str:
    payload_part, supplied_part = value.split(".", 1)
    payload = b64decode(payload_part)
    supplied = b64decode(supplied_part)
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("cursor signature differs")
    customer_id = json.loads(payload)["customerId"]
    if not isinstance(customer_id, str):
        raise ValueError("cursor customer ID is invalid")
    return customer_id
