"""Track comment dispatch boundaries, simulation and album scan cursors."""

import sqlalchemy as sa
from alembic import op

revision = "0076_comment_dispatch_safety"
down_revision = "0075_invite_verified_destination"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "channel_comment_rules", sa.Column("last_seen_grouped_id", sa.BigInteger(), nullable=True)
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "channel_comment_runs", sa.Column("dispatch_account_id", sa.Integer(), nullable=True)
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("simulated", sa.Boolean(), nullable=False, server_default="0"),
    )
    # Old workers did not persist the RPC boundary. Their in-flight result
    # cannot be inferred from a missing receipt after restart.
    runs = sa.table(
        "channel_comment_runs",
        sa.column("status", sa.String()),
        sa.column("started_at", sa.DateTime()),
        sa.column("dispatch_started_at", sa.DateTime()),
    )
    op.execute(
        runs.update()
        .where(runs.c.status == "RUNNING")
        .values(
            dispatch_started_at=sa.func.coalesce(runs.c.started_at, sa.func.current_timestamp())
        )
    )


def downgrade() -> None:
    op.drop_column("channel_comment_runs", "simulated")
    op.drop_column("channel_comment_runs", "dispatch_account_id")
    op.drop_column("channel_comment_runs", "dispatch_started_at")
    op.drop_column("channel_comment_rules", "last_seen_grouped_id")
