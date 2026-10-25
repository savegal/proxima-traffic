"""Pin invitation destinations and preserve the exact preflight account selection."""

import sqlalchemy as sa
from alembic import op

revision = "0075_invite_verified_destination"
down_revision = "0074_direct_service_actions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "subscriber_invite_launches",
        sa.Column("destination_telegram_id_snapshot", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "subscriber_invite_launches",
        sa.Column("selected_account_ids_snapshot_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "subscriber_invite_launches",
        sa.Column("preflight_failures_snapshot_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("subscriber_invite_launches", "preflight_failures_snapshot_json")
    op.drop_column("subscriber_invite_launches", "selected_account_ids_snapshot_json")
    op.drop_column("subscriber_invite_launches", "destination_telegram_id_snapshot")
