"""Add terminal-run notification tracking.

Revision ID: 0004_run_notifications
Revises: 0003_target_lists
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_run_notifications"
down_revision: str | None = "0003_target_lists"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_index("ix_campaign_runs_notification", ["status", "notified_at"])


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_index("ix_campaign_runs_notification")
        batch.drop_column("notified_at")
