"""Create the initial durable Proxima schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("label", sa.String(length=100), nullable=False),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
        sa.Column("ownership_confirmed", sa.Boolean(), nullable=False),
        sa.Column(
            "status",
            sa.Enum("ACTIVE", "PAUSED", "DISABLED", name="accountstatus", native_enum=False),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("label"),
        sa.UniqueConstraint("telegram_user_id"),
    )
    op.create_table(
        "targets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("chat_ref", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=150), nullable=False),
        sa.Column("approved_by_operator", sa.Boolean(), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("chat_ref"),
    )
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("actor", sa.String(length=100), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("entity_type", sa.String(length=100), nullable=False),
        sa.Column("entity_id", sa.String(length=100), nullable=True),
        sa.Column("details_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_audit_events_created", "audit_events", ["created_at"])
    op.create_table(
        "campaigns",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("primary_account_id", sa.Integer(), nullable=False),
        sa.Column("reply_account_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("primary_text", sa.Text(), nullable=False),
        sa.Column("reply_text", sa.Text(), nullable=False),
        sa.Column("reply_delay_seconds", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("primary_account_id != reply_account_id", name="different_accounts"),
        sa.CheckConstraint("reply_delay_seconds >= 0", name="non_negative_reply_delay"),
        sa.ForeignKeyConstraint(["primary_account_id"], ["accounts.id"]),
        sa.ForeignKeyConstraint(["reply_account_id"], ["accounts.id"]),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"]),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "campaign_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("request_key", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "QUEUED",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
                name="runstatus",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "next_step",
            sa.Enum("PRIMARY", "REPLY", name="runstep", native_enum=False),
            nullable=False,
        ),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.Column("primary_message_id", sa.Integer(), nullable=True),
        sa.Column("reply_message_id", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["campaign_id"], ["campaigns.id"]),
        sa.UniqueConstraint("request_key", name="uq_campaign_runs_request_key"),
    )
    op.create_index("ix_campaign_runs_campaign_status", "campaign_runs", ["campaign_id", "status"])
    op.create_table(
        "action_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column(
            "kind",
            sa.Enum(
                "PRIMARY_MESSAGE",
                "REPLY_MESSAGE",
                name="actionkind",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum("SUCCEEDED", "FAILED", name="actionstatus", native_enum=False),
            nullable=False,
        ),
        sa.Column("external_message_id", sa.Integer(), nullable=True),
        sa.Column("reply_to_message_id", sa.Integer(), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["campaign_runs.id"]),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"]),
    )
    op.create_index("ix_action_logs_run_created", "action_logs", ["run_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_action_logs_run_created", table_name="action_logs")
    op.drop_table("action_logs")
    op.drop_index("ix_campaign_runs_campaign_status", table_name="campaign_runs")
    op.drop_table("campaign_runs")
    op.drop_table("campaigns")
    op.drop_index("ix_audit_events_created", table_name="audit_events")
    op.drop_table("audit_events")
    op.drop_table("targets")
    op.drop_table("accounts")
