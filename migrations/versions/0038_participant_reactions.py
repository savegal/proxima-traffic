"""Add durable sampled reactions to new participant messages.

Revision ID: 0038_participant_reactions
Revises: 0037_account_profile_about
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0038_participant_reactions"
down_revision: str | None = "0037_account_profile_about"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "participant_reaction_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("target_list_id", sa.Integer(), nullable=False),
        sa.Column("account_ids_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("emoji", sa.String(length=16), nullable=False, server_default="👍"),
        sa.Column("sample_numerator", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("sample_denominator", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("approved_by_operator", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_scanned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "sample_numerator >= 1 AND sample_denominator >= sample_numerator",
            name="valid_participant_reaction_sample",
        ),
        sa.ForeignKeyConstraint(["target_list_id"], ["target_lists.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        "ix_participant_reaction_rules_scan",
        "participant_reaction_rules",
        ["enabled", "last_scanned_at"],
    )
    op.create_table(
        "participant_reaction_target_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("last_seen_message_id", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("primed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_scanned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["participant_reaction_rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("rule_id", "target_id", name="uq_participant_reaction_rule_target"),
    )
    op.create_index(
        "ix_participant_reaction_target_state",
        "participant_reaction_target_states",
        ["rule_id", "last_seen_message_id"],
    )
    op.create_table(
        "participant_reaction_tasks",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("reactor_account_id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=False),
        sa.Column("source_sender_id", sa.BigInteger(), nullable=True),
        sa.Column("emoji", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("lease_token", sa.String(length=32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["participant_reaction_rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["reactor_account_id"], ["accounts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "rule_id", "target_id", "message_id", name="uq_participant_reaction_message"
        ),
    )
    op.create_index(
        "ix_participant_reaction_tasks_due",
        "participant_reaction_tasks",
        ["status", "scheduled_for"],
    )
    op.create_index(
        "ix_participant_reaction_tasks_rule",
        "participant_reaction_tasks",
        ["rule_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_participant_reaction_tasks_rule", table_name="participant_reaction_tasks")
    op.drop_index("ix_participant_reaction_tasks_due", table_name="participant_reaction_tasks")
    op.drop_table("participant_reaction_tasks")
    op.drop_index(
        "ix_participant_reaction_target_state",
        table_name="participant_reaction_target_states",
    )
    op.drop_table("participant_reaction_target_states")
    op.drop_index("ix_participant_reaction_rules_scan", table_name="participant_reaction_rules")
    op.drop_table("participant_reaction_rules")
