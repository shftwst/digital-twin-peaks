from typing import Annotated

from fastapi import APIRouter, Depends, Response

from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.control.contracts import (
    AdvanceClockRequest,
    ClockValue,
    SetClockRequest,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.control.time import parse_duration


def control_router(repository: ControlRepository, settings: ControlSettings) -> APIRouter:
    router = APIRouter(prefix="/control/v1")
    twin_auth = require_token(settings.twin_token)
    controller_auth = require_token(settings.controller_token)
    TwinAuth = Annotated[None, Depends(twin_auth)]
    ControllerAuth = Annotated[None, Depends(controller_auth)]

    @router.get("/time")
    async def get_time(_auth: TwinAuth, response: Response) -> ClockValue:
        result = await repository.snapshot()
        response.headers["X-Scenario-Epoch"] = result.scenario_epoch
        return result

    @router.put("/time")
    async def set_time(
        request: SetClockRequest, _auth: ControllerAuth, response: Response
    ) -> ClockValue:
        result = await repository.set_time(request.now)
        response.headers["X-Scenario-Epoch"] = result.scenario_epoch
        return result

    @router.post("/time/advance")
    async def advance_time(
        request: AdvanceClockRequest, _auth: ControllerAuth, response: Response
    ) -> ClockValue:
        try:
            amount = parse_duration(request.duration)
        except (OverflowError, ValueError) as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, str(error), status_code=422) from error
        result = await repository.advance_time(amount)
        response.headers["X-Scenario-Epoch"] = result.scenario_epoch
        return result

    return router
