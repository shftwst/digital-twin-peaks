from fastapi import FastAPI

from enterprise_twins.common.control.participant import ResetParticipant, create_participant_app
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.relay.scenario import RelayScenarioLoader
from enterprise_twins.services.relay.settings import RelaySettings


def create_from_env() -> FastAPI:
    settings = RelaySettings()  # type: ignore[call-arg]
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(factory, RelayScenarioLoader(), "relay")
    return create_participant_app("Event Relay", participant, settings.participant_token)
