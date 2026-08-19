from fastapi import FastAPI

from enterprise_twins.common.control.participant import ResetParticipant, create_participant_app
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.identity.scenario import IdentityScenarioLoader
from enterprise_twins.services.identity.settings import IdentitySettings


def create_from_env() -> FastAPI:
    settings = IdentitySettings()  # type: ignore[call-arg]
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(
        factory,
        IdentityScenarioLoader(settings.secret_pepper),
        "identity",
    )
    return create_participant_app("Identity", participant, settings.participant_token)
