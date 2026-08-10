"""Track one delayed verification for each successful campaign message.

Revision ID: 0039_delayed_delivery_monitor
Revises: 0038_participant_reactions
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0039_delayed_delivery_monitor"
down_revision: str | None = "0038_participant_reactions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "action_logs",
        sa.Column("delayed_delivery_checked_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Historical deliveries predate this feature and must not cause a one-off
    # read storm immediately after deployment.
    op.execute("UPDATE action_logs SET delayed_delivery_checked_at = CURRENT_TIMESTAMP")
    op.create_index(
        "ix_action_logs_delayed_delivery",
        "action_logs",
        ["status", "delayed_delivery_checked_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_action_logs_delayed_delivery", table_name="action_logs")
    op.drop_column("action_logs", "delayed_delivery_checked_at")
