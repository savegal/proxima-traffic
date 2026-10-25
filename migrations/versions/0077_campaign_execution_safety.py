"""Fence chat publication requests and preserve delayed observations and album IDs."""

import sqlalchemy as sa
from alembic import op

revision = "0077_campaign_execution_safety"
down_revision = "0076_comment_dispatch_safety"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for column in (
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_account_id", sa.Integer(), nullable=True),
        sa.Column("dispatch_idempotency_key", sa.String(120), nullable=True),
        sa.Column("simulated_snapshot", sa.Boolean(), nullable=True),
    ):
        op.add_column("campaign_runs", column)
    for column in (
        sa.Column("external_message_ids_json", sa.Text(), nullable=True),
        sa.Column("delayed_delivery_status", sa.String(20), nullable=True),
        sa.Column("delayed_delivery_error", sa.Text(), nullable=True),
        sa.Column("delayed_delivery_error_code", sa.String(100), nullable=True),
        sa.Column(
            "delayed_delivery_attempt_count", sa.Integer(), nullable=False, server_default="0"
        ),
    ):
        op.add_column("action_logs", column)
    runs = sa.table(
        "campaign_runs",
        sa.column("status"),
        sa.column("started_at"),
        sa.column("dispatch_started_at"),
    )
    op.execute(
        runs.update()
        .where(runs.c.status == "RUNNING")
        .values(
            dispatch_started_at=sa.func.coalesce(runs.c.started_at, sa.func.current_timestamp())
        )
    )


def downgrade() -> None:
    for name in (
        "delayed_delivery_attempt_count",
        "delayed_delivery_error_code",
        "delayed_delivery_error",
        "delayed_delivery_status",
        "external_message_ids_json",
    ):
        op.drop_column("action_logs", name)
    for name in (
        "simulated_snapshot",
        "dispatch_idempotency_key",
        "dispatch_account_id",
        "dispatch_started_at",
    ):
        op.drop_column("campaign_runs", name)
