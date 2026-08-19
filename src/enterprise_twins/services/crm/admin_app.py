from fastapi import FastAPI

from enterprise_twins.common.control.participant import ResetParticipant, create_participant_app
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.crm.scenario import CrmScenarioLoader
from enterprise_twins.services.crm.settings import CrmSettings


def create_from_env() -> FastAPI:
    settings = CrmSettings()  # type: ignore[call-arg]
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(factory, CrmScenarioLoader(), "crm")
    return create_participant_app("CRM", participant, settings.participant_token)
