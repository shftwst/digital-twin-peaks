from typing import Annotated

from fastapi import APIRouter, Depends

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
    async def get_time(_auth: TwinAuth) -> ClockValue:
        state = await repository.state()
        return ClockValue(now=await repository.now(), scenarioEpoch=state.active_epoch)

    @router.put("/time")
    async def set_time(request: SetClockRequest, _auth: ControllerAuth) -> ClockValue:
        state = await repository.state()
        return ClockValue(
            now=await repository.set_time(request.now), scenarioEpoch=state.active_epoch
        )

    @router.post("/time/advance")
    async def advance_time(request: AdvanceClockRequest, _auth: ControllerAuth) -> ClockValue:
        try:
            amount = parse_duration(request.duration)
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, str(error), status_code=422) from error
        state = await repository.state()
        return ClockValue(
            now=await repository.advance_time(amount), scenarioEpoch=state.active_epoch
        )

    return router
