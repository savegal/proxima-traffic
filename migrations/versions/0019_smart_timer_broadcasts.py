"""Add continuous timer-driven broadcast scheduling.

Revision ID: 0019_smart_timer_broadcasts
Revises: 0018_target_account_timing
Create Date: 2026-07-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019_smart_timer_broadcasts"
down_revision: str | None = "0018_target_account_timing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(
            sa.Column(
                "smart_timer_mode",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.add_column(
            sa.Column(
                "smart_duration_seconds",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column(
                "smart_no_timer_interval_seconds",
                sa.Integer(),
                nullable=False,
                server_default="900",
            )
        )
        batch.add_column(
            sa.Column(
                "auto_sender_failover",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("repeat_until", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("no_timer_interval_seconds_snapshot", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("auto_sender_failover_snapshot", sa.Boolean(), nullable=True)
        )
        batch.add_column(sa.Column("sender_pool_snapshot_json", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("sender_pool_snapshot_json")
        batch.drop_column("auto_sender_failover_snapshot")
        batch.drop_column("no_timer_interval_seconds_snapshot")
        batch.drop_column("repeat_until")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("auto_sender_failover")
        batch.drop_column("smart_no_timer_interval_seconds")
        batch.drop_column("smart_duration_seconds")
        batch.drop_column("smart_timer_mode")
