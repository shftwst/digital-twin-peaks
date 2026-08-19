from sqlalchemy import MetaData

from enterprise_twins.common.db import records as common_records
from enterprise_twins.common.db.base import Base
from enterprise_twins.services.control import models as control_models
from enterprise_twins.services.crm import models as crm_models
from enterprise_twins.services.identity import models as identity_models
from enterprise_twins.services.relay import models as relay_models

REGISTERED_MODELS = (
    common_records,
    control_models,
    crm_models,
    identity_models,
    relay_models,
)

SERVICE_TABLES = {
    "control": {
        "scenario_state",
        "virtual_clock",
        "fault_rules",
        "fault_activations",
        "reset_runs",
    },
    "relay": {
        "scenario_state",
        "audit_records",
        "idempotency_records",
        "outbox_records",
        "relay_subscriptions",
        "relay_source_events",
        "relay_deliveries",
        "relay_delivery_attempts",
    },
    "identity": {
        "scenario_state",
        "audit_records",
        "idempotency_records",
        "outbox_records",
        "identity_clients",
    },
    "crm": {
        "scenario_state",
        "audit_records",
        "idempotency_records",
        "outbox_records",
        "crm_customers",
        "crm_customer_notes",
    },
}


def selected_metadata(service: str) -> MetaData:
    try:
        table_names = SERVICE_TABLES[service]
    except KeyError as error:
        raise ValueError(f"unknown migration service: {service}") from error
    metadata = MetaData()
    for name in sorted(table_names):
        Base.metadata.tables[name].to_metadata(metadata)
    return metadata
