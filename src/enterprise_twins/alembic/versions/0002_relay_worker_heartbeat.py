from collections.abc import Sequence

from alembic import context, op

from enterprise_twins.migration_metadata import selected_metadata

revision: str = "0002_relay_worker_heartbeat"
down_revision: str | None = "0001_platform_contracts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    if service == "relay":
        selected_metadata(service).tables["relay_worker_heartbeat"].create(
            bind=op.get_bind(), checkfirst=True
        )


def downgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    if service == "relay":
        selected_metadata(service).tables["relay_worker_heartbeat"].drop(
            bind=op.get_bind(), checkfirst=True
        )
