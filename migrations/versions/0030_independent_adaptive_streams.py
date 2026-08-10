"""Add independent account lanes and learned per-chat timing.

Revision ID: 0030_independent_adaptive_streams
Revises: 0029_adaptive_broadcast_profiles
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0030_independent_adaptive_streams"
down_revision: str | None = "0029_adaptive_broadcast_profiles"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(
            sa.Column(
                "sender_mode",
                sa.String(length=16),
                nullable=False,
                server_default="DISTRIBUTED",
            )
        )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("sender_mode_snapshot", sa.String(length=16), nullable=True))
    with op.batch_alter_table("target_account_states") as batch:
        batch.add_column(sa.Column("adaptive_interval_seconds", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("activity_messages_per_minute", sa.Float(), nullable=True))
        batch.add_column(sa.Column("adaptive_tier", sa.String(length=20), nullable=True))
        batch.add_column(
            sa.Column("activity_sampled_at", sa.DateTime(timezone=True), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.drop_column("activity_sampled_at")
        batch.drop_column("adaptive_tier")
        batch.drop_column("activity_messages_per_minute")
        batch.drop_column("adaptive_interval_seconds")
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("sender_mode_snapshot")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("sender_mode")
