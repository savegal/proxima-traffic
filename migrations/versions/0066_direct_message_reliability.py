"""Persist direct launch identity and the boundary before a Telegram send."""

import sqlalchemy as sa
from alembic import op

revision = "0066_direct_message_reliability"
down_revision = "0065_indefinite_smart_campaigns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("direct_launches", sa.Column("request_key", sa.String(100), nullable=True))
    op.create_index("uq_direct_launch_request_key", "direct_launches", ["request_key"], unique=True)
    op.add_column(
        "direct_message_tasks",
        sa.Column("send_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Old workers did not record the dispatch boundary. A running legacy task
    # might already have reached Telegram; never replay it on another account.
    op.execute(
        "UPDATE direct_message_tasks SET send_started_at = COALESCE(started_at, created_at) "
        "WHERE status = 'RUNNING'"
    )
    op.create_index(
        "ix_audit_events_entity", "audit_events", ["entity_type", "entity_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_audit_events_entity", table_name="audit_events")
    op.drop_column("direct_message_tasks", "send_started_at")
    op.drop_index("uq_direct_launch_request_key", table_name="direct_launches")
    op.drop_column("direct_launches", "request_key")
