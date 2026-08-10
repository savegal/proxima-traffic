"""Fence concurrent terminal-run notifications and persist their retries.

Revision ID: 0045_notification_delivery_leases
Revises: 0044_account_operation_leases
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0045_notification_delivery_leases"
down_revision: str | None = "0044_account_operation_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(
            sa.Column("notification_lease_owner", sa.String(length=100), nullable=True)
        )
        batch.add_column(
            sa.Column("notification_lease_token", sa.String(length=32), nullable=True)
        )
        batch.add_column(
            sa.Column("notification_lease_until", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("notification_next_attempt_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "notification_attempt_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(sa.Column("notification_last_error", sa.String(length=500), nullable=True))
        batch.create_index(
            "ix_campaign_runs_notification_due",
            ["status", "notified_at", "notification_next_attempt_at"],
        )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_index("ix_campaign_runs_notification_due")
        batch.drop_column("notification_last_error")
        batch.drop_column("notification_attempt_count")
        batch.drop_column("notification_next_attempt_at")
        batch.drop_column("notification_lease_until")
        batch.drop_column("notification_lease_token")
        batch.drop_column("notification_lease_owner")
