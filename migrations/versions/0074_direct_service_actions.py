"""Persist private-chat service actions without reclassifying old message history."""

import sqlalchemy as sa
from alembic import op

revision = "0074_direct_service_actions"
down_revision = "0073_campaign_delivery_mode"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "direct_campaigns",
        sa.Column("operation_kind", sa.String(24), nullable=False, server_default="message"),
    )
    op.add_column("direct_campaigns", sa.Column("auto_delete_seconds", sa.Integer(), nullable=True))
    op.add_column(
        "direct_campaigns", sa.Column("service_account_ids_json", sa.Text(), nullable=True)
    )
    op.add_column(
        "direct_launches",
        sa.Column(
            "operation_kind_snapshot", sa.String(24), nullable=False, server_default="message"
        ),
    )
    op.add_column(
        "direct_launches", sa.Column("auto_delete_seconds_snapshot", sa.Integer(), nullable=True)
    )
    op.add_column("direct_message_tasks", sa.Column("result_code", sa.String(100), nullable=True))


def downgrade() -> None:
    op.drop_column("direct_message_tasks", "result_code")
    op.drop_column("direct_launches", "auto_delete_seconds_snapshot")
    op.drop_column("direct_launches", "operation_kind_snapshot")
    op.drop_column("direct_campaigns", "auto_delete_seconds")
    op.drop_column("direct_campaigns", "service_account_ids_json")
    op.drop_column("direct_campaigns", "operation_kind")
