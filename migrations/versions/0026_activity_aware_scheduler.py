"""Add visibility-aware smart broadcast settings and run snapshots.

Revision ID: 0026_activity_aware_scheduler
Revises: 0025_membership_preflight
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0026_activity_aware_scheduler"
down_revision: str | None = "0025_membership_preflight"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(
            sa.Column("activity_aware", sa.Boolean(), nullable=False, server_default=sa.false())
        )
        batch.add_column(
            sa.Column(
                "min_messages_between_posts",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column(
                "activity_check_interval_seconds",
                sa.Integer(),
                nullable=False,
                server_default="300",
            )
        )
        batch.add_column(
            sa.Column(
                "visibility_max_wait_seconds",
                sa.Integer(),
                nullable=False,
                server_default="3600",
            )
        )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("activity_aware_snapshot", sa.Boolean(), nullable=True))
        batch.add_column(
            sa.Column("min_messages_between_posts_snapshot", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("activity_check_interval_seconds_snapshot", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("visibility_max_wait_seconds_snapshot", sa.Integer(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("visibility_max_wait_seconds_snapshot")
        batch.drop_column("activity_check_interval_seconds_snapshot")
        batch.drop_column("min_messages_between_posts_snapshot")
        batch.drop_column("activity_aware_snapshot")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("visibility_max_wait_seconds")
        batch.drop_column("activity_check_interval_seconds")
        batch.drop_column("min_messages_between_posts")
        batch.drop_column("activity_aware")
