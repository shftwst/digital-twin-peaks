from datetime import datetime

import httpx

from enterprise_twins.common.control.contracts import ClockValue, FaultDecision, FaultProbe


class ControlClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = client

    async def snapshot(self) -> ClockValue:
        response = await self.client.get(
            f"{self.base_url}/control/v1/time",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        response.raise_for_status()
        return ClockValue.model_validate(response.json())

    async def now(self) -> datetime:
        return (await self.snapshot()).now

    async def current_epoch(self) -> str:
        return (await self.snapshot()).scenario_epoch

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        response = await self.client.post(
            f"{self.base_url}/control/v1/faults/evaluate",
            headers={"Authorization": f"Bearer {self.token}"},
            json=probe.model_dump(mode="json", by_alias=True),
        )
        response.raise_for_status()
        return FaultDecision.model_validate(response.json())
