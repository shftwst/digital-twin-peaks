from datetime import datetime

import httpx

from enterprise_twins.common.control.contracts import ClockValue, FaultDecision, FaultProbe
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ControlClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = client

    @staticmethod
    def unavailable() -> ApiError:
        return ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "Control is temporarily unavailable",
            status_code=503,
            retryable=True,
        )

    async def snapshot(self) -> ClockValue:
        try:
            response = await self.client.get(
                f"{self.base_url}/control/v1/time",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=2.0,
            )
            response.raise_for_status()
            return ClockValue.model_validate(response.json())
        except httpx.HTTPError, TypeError, ValueError:
            raise self.unavailable() from None

    async def now(self) -> datetime:
        return (await self.snapshot()).now

    async def current_epoch(self) -> str:
        return (await self.snapshot()).scenario_epoch

    async def ready_epoch(self) -> str:
        try:
            response = await self.client.get(
                f"{self.base_url}/health/ready",
                timeout=2.0,
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("status") != "ready":
                raise ValueError("Control is not ready")
            epoch = response.headers.get("X-Scenario-Epoch")
            if not isinstance(epoch, str) or not epoch:
                raise ValueError("Control readiness epoch is missing")
            return epoch
        except httpx.HTTPError, TypeError, ValueError:
            raise self.unavailable() from None

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        try:
            response = await self.client.post(
                f"{self.base_url}/control/v1/faults/evaluate",
                headers={"Authorization": f"Bearer {self.token}"},
                json=probe.model_dump(mode="json", by_alias=True),
                timeout=2.0,
            )
            response.raise_for_status()
            return FaultDecision.model_validate(response.json())
        except httpx.HTTPError, TypeError, ValueError:
            raise self.unavailable() from None
