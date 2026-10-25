"""Record whether each direct or invite launch used the dry-run gateway."""

import sqlalchemy as sa
from alembic import op

revision = "0068_direct_launch_mode_snapshot"
down_revision = "0067_control_panel_latency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Historical launches cannot be classified reliably from their result IDs.
    op.add_column("direct_launches", sa.Column("simulated_snapshot", sa.Boolean(), nullable=True))
    op.add_column(
        "subscriber_invite_launches",
        sa.Column("simulated_snapshot", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("subscriber_invite_launches", "simulated_snapshot")
    op.drop_column("direct_launches", "simulated_snapshot")
