"""Distinguish real campaign receipts from simulation and unknown legacy history."""

import sqlalchemy as sa
from alembic import op

revision = "0073_campaign_delivery_mode"
down_revision = "0072_recipient_report_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("action_logs", sa.Column("simulated", sa.Boolean(), nullable=True))
    op.create_index("ix_action_logs_created", "action_logs", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_action_logs_created", table_name="action_logs")
    op.drop_column("action_logs", "simulated")
