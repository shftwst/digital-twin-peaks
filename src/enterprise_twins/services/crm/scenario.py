import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.ids import new_id
from enterprise_twins.services.crm.models import Customer, CustomerNote


class CrmScenarioLoader:
    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        if payload.get("schemaVersion") != "1":
            raise ValueError('CRM schemaVersion must be "1"')
        customers = payload["customers"]
        notes = payload["notes"]
        customer_ids = [item["customerId"] for item in customers]
        if len(customer_ids) != len(set(customer_ids)):
            raise ValueError("CRM customer IDs must be unique")
        note_ids = [item["noteId"] for item in notes]
        if len(note_ids) != len(set(note_ids)):
            raise ValueError("CRM note IDs must be unique")
        known = set(customer_ids)
        for item in customers:
            self.validate_version(item["version"], "CRM customer version")
        note_timestamps: list[datetime] = []
        for item in notes:
            if item["customerId"] not in known:
                raise ValueError("CRM note refers to an unknown customer")
            self.validate_version(item.get("version", 1), "CRM note version")
            note_timestamps.append(self.parse_created_at(item["createdAt"]))
        for item in customers:
            session.add(
                Customer(
                    row_id=new_id("crow"),
                    scenario_epoch=epoch,
                    customer_id=item["customerId"],
                    display_name=item["displayName"],
                    primary_email=item["primaryEmail"].casefold(),
                    external_reference=item["externalReference"],
                    account_status=item["accountStatus"],
                    contact_methods=item["contactMethods"],
                    external_identifiers=item["externalIdentifiers"],
                    version=item["version"],
                )
            )
        for item, created_at in zip(notes, note_timestamps, strict=True):
            session.add(
                CustomerNote(
                    row_id=new_id("nrow"),
                    note_id=item["noteId"],
                    scenario_epoch=epoch,
                    customer_id=item["customerId"],
                    body=item["body"],
                    association=item["association"],
                    created_by=item["createdBy"],
                    created_at=created_at,
                    archived=item.get("archived", False),
                    version=item.get("version", 1),
                )
            )
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"customers": len(customers), "notes": len(notes)},
            "aliases": payload.get("aliases", {}),
        }

    @staticmethod
    def validate_version(value: object, label: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{label} must be a positive integer")

    @staticmethod
    def parse_created_at(value: object) -> datetime:
        if (
            not isinstance(value, str)
            or re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
                value,
            )
            is None
        ):
            raise ValueError("CRM note createdAt must be an offset-aware RFC 3339 timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(
                "CRM note createdAt must be an offset-aware RFC 3339 timestamp"
            ) from error
        if parsed.utcoffset() is None:
            raise ValueError("CRM note createdAt must include a UTC offset")
        return parsed.astimezone(UTC)

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(CustomerNote).where(CustomerNote.scenario_epoch == epoch))
        await session.execute(delete(Customer).where(Customer.scenario_epoch == epoch))
