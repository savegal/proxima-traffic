"""Keep permanent direct-recipient refusals across launches."""

import sqlalchemy as sa
from alembic import op

revision = "0071_direct_recipient_quarantine"
down_revision = "0070_comment_delivery_monitor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name, type_ in (
        ("direct_quarantine_code", sa.String(100)),
        ("direct_quarantine_error", sa.String(500)),
        ("direct_quarantine_at", sa.DateTime(timezone=True)),
        ("direct_quarantine_account_id", sa.Integer()),
        ("direct_quarantine_checked_at", sa.DateTime(timezone=True)),
        ("direct_quarantine_check_note", sa.String(500)),
    ):
        op.add_column("subscribers", sa.Column(name, type_, nullable=True))
    op.create_index(
        "ix_subscribers_direct_quarantine", "subscribers", ["direct_quarantine_code", "id"]
    )
    # Preserve historical refusals even if a later cancellation/import overwrote
    # last_error. Old workers had no read-only verification: never infer that a
    # permanent restriction disappeared from elapsed time or unrelated activity.
    # A migrated recipient can be released through the explicit fresh check.
    op.execute("""
        WITH refusals AS (
            SELECT t.subscriber_id, t.error_code, t.error, t.sender_account_id,
                   COALESCE(t.finished_at, t.updated_at, t.created_at) AS refused_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY t.subscriber_id
                       ORDER BY COALESCE(t.finished_at, t.updated_at, t.created_at) DESC, t.id DESC
                   ) AS ordinal
            FROM direct_message_tasks t
            JOIN direct_launches l ON l.id = t.launch_id
            WHERE t.error_code IN (
                'recipient_unavailable', 'recipient_paid_message_required',
                'recipient_identity_mismatch'
            ) AND t.status IN ('SKIPPED', 'FAILED')
              AND (l.simulated_snapshot IS NULL OR l.simulated_snapshot = 0)
        )
        UPDATE subscribers
        SET (direct_quarantine_code, direct_quarantine_error,
             direct_quarantine_account_id, direct_quarantine_at) = (
            SELECT error_code, error, sender_account_id, refused_at
            FROM refusals WHERE subscriber_id = subscribers.id AND ordinal = 1
        )
        WHERE id IN (SELECT subscriber_id FROM refusals)
    """)


def downgrade() -> None:
    op.drop_index("ix_subscribers_direct_quarantine", table_name="subscribers")
    for name in (
        "direct_quarantine_check_note", "direct_quarantine_checked_at",
        "direct_quarantine_account_id", "direct_quarantine_at",
        "direct_quarantine_error", "direct_quarantine_code",
    ):
        op.drop_column("subscribers", name)
