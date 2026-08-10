"""Add a retry schedule for delayed delivery checks.

Revision ID: 0040_delayed_delivery_retry
Revises: 0039_delayed_delivery_monitor
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0040_delayed_delivery_retry"
down_revision: str | None = "0039_delayed_delivery_monitor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "action_logs",
        sa.Column("delayed_delivery_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_action_logs_delayed_delivery_due",
        "action_logs",
        ["status", "delayed_delivery_checked_at", "delayed_delivery_retry_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_action_logs_delayed_delivery_due", table_name="action_logs")
    op.drop_column("action_logs", "delayed_delivery_retry_at")
