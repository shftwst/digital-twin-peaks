from collections.abc import Sequence

from alembic import context, op

from enterprise_twins.migration_metadata import selected_metadata

revision: str = "0001_platform_contracts"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    selected_metadata(service).create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    selected_metadata(service).drop_all(bind=op.get_bind(), checkfirst=True)
