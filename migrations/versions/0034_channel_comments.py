"""Add durable automatic channel-comment flows.

Revision ID: 0034_channel_comments
Revises: 0033_spambot_account_status
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0034_channel_comments"
down_revision: str | None = "0033_spambot_account_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "channel_comment_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=150), nullable=False, unique=True),
        sa.Column("channel_ref", sa.String(length=255), nullable=False, unique=True),
        sa.Column("channel_title", sa.String(length=255), nullable=True),
        sa.Column("mode", sa.String(length=16), nullable=False, server_default="single"),
        sa.Column("primary_account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("primary_text", sa.Text(), nullable=False),
        sa.Column("dialogue_steps_json", sa.Text(), nullable=True),
        sa.Column("initial_delay_seconds", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("approved_by_operator", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("armed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_post_id", sa.Integer(), nullable=True),
        sa.Column("last_scanned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_channel_comment_rules_scan",
        "channel_comment_rules",
        ["enabled", "last_scanned_at"],
        unique=False,
    )
    op.create_table(
        "channel_comment_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "rule_id",
            sa.Integer(),
            sa.ForeignKey("channel_comment_rules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_post_id", sa.Integer(), nullable=False),
        sa.Column("source_post_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column("step_index", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sent_steps", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("thread_root_message_id", sa.Integer(), nullable=True),
        sa.Column("last_comment_message_id", sa.Integer(), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("lease_token", sa.String(length=32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("rule_id", "source_post_id", name="uq_channel_comment_rule_post"),
    )
    op.create_index(
        "ix_channel_comment_runs_due",
        "channel_comment_runs",
        ["status", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "ix_channel_comment_runs_rule",
        "channel_comment_runs",
        ["rule_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_channel_comment_runs_rule", table_name="channel_comment_runs")
    op.drop_index("ix_channel_comment_runs_due", table_name="channel_comment_runs")
    op.drop_table("channel_comment_runs")
    op.drop_index("ix_channel_comment_rules_scan", table_name="channel_comment_rules")
    op.drop_table("channel_comment_rules")
