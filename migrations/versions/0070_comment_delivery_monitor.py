"""Persist every auto-comment message and its delayed observations."""

import sqlalchemy as sa
from alembic import op

revision = "0070_comment_delivery_monitor"
down_revision = "0069_blacklist_usernames"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Historical runs only kept the last message ID, without the original
    # sender/discussion. Reconstructing those from an editable rule is unsafe.
    op.create_table(
        "channel_comment_messages",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("channel_ref", sa.String(255), nullable=False),
        sa.Column("source_post_id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=False),
        sa.Column("discussion_peer_id", sa.BigInteger(), nullable=True),
        sa.Column("discussion_access_hash", sa.BigInteger(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("check_stage", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("first_check_status", sa.String(16), nullable=True),
        sa.Column("first_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(100), nullable=True),
        sa.Column("lease_owner", sa.String(100), nullable=True),
        sa.Column("lease_token", sa.String(32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["channel_comment_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "step_index", name="uq_channel_comment_message_step"),
    )
    op.create_index(
        "ix_channel_comment_messages_due",
        "channel_comment_messages",
        ["next_check_at", "lease_until"],
    )


def downgrade() -> None:
    op.drop_index("ix_channel_comment_messages_due", table_name="channel_comment_messages")
    op.drop_table("channel_comment_messages")
