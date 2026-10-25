"""Index recipient history and paginated launch results."""

from alembic import op

revision = "0072_recipient_report_indexes"
down_revision = "0071_direct_recipient_quarantine"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("direct_message_tasks", "subscriber_invite_tasks"):
        op.create_index(f"ix_{table}_recipient", table, ["subscriber_id", "status", "id"])
        op.create_index(f"ix_{table}_result", table, ["status", "id"])


def downgrade() -> None:
    for table in ("subscriber_invite_tasks", "direct_message_tasks"):
        op.drop_index(f"ix_{table}_result", table_name=table)
        op.drop_index(f"ix_{table}_recipient", table_name=table)
