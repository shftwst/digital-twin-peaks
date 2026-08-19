from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.ids import new_id
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.secrets import digest_secret


class IdentityScenarioLoader:
    def __init__(self, pepper: str) -> None:
        self.pepper = pepper

    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        clients = payload["clients"]
        client_ids = [item["clientId"] for item in clients]
        if len(client_ids) != len(set(client_ids)):
            raise ValueError("identity client IDs must be unique")
        for item in clients:
            session.add(
                IdentityClient(
                    row_id=new_id("irow"),
                    scenario_epoch=epoch,
                    client_id=item["clientId"],
                    secret_digest=digest_secret(
                        item["clientId"],
                        item["clientSecret"],
                        self.pepper,
                    ),
                    subject=item["subject"],
                    actor_type=item["actorType"],
                    role=item["role"],
                    scopes=sorted(set(item["scopes"])),
                    tenant_id=item["tenantId"],
                    active=True,
                    version=1,
                )
            )
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"clients": len(clients)},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(IdentityClient).where(IdentityClient.scenario_epoch == epoch))
