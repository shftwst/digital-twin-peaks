from datetime import datetime
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
        customers = payload["customers"]
        notes = payload["notes"]
        customer_ids = [item["customerId"] for item in customers]
        if len(customer_ids) != len(set(customer_ids)):
            raise ValueError("CRM customer IDs must be unique")
        known = set(customer_ids)
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
        for item in notes:
            if item["customerId"] not in known:
                raise ValueError("CRM note refers to an unknown customer")
            session.add(
                CustomerNote(
                    row_id=new_id("nrow"),
                    note_id=item["noteId"],
                    scenario_epoch=epoch,
                    customer_id=item["customerId"],
                    body=item["body"],
                    association=item["association"],
                    created_by=item["createdBy"],
                    created_at=datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00")),
                    archived=item.get("archived", False),
                    version=item.get("version", 1),
                )
            )
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"customers": len(customers), "notes": len(notes)},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(CustomerNote).where(CustomerNote.scenario_epoch == epoch))
        await session.execute(delete(Customer).where(Customer.scenario_epoch == epoch))
