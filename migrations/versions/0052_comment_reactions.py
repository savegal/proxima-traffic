"""Add durable reactions to channel comments.

Revision ID: 0052_comment_reactions
Revises: 0051_global_mention_blacklist
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0052_comment_reactions"
down_revision: str | None = "0051_global_mention_blacklist"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "channel_comment_rules",
        sa.Column("reactions_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("reaction_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("reaction_emoji", sa.String(length=16), nullable=False, server_default="👍"),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("reaction_dialogue_step", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("reaction_account_ids_json", sa.Text(), nullable=False, server_default="[]"),
    )

    with op.batch_alter_table("reaction_tasks") as batch_op:
        batch_op.add_column(sa.Column("channel_comment_run_id", sa.Integer(), nullable=True))
        batch_op.alter_column("campaign_id", existing_type=sa.Integer(), nullable=True)
        batch_op.alter_column("run_id", existing_type=sa.Integer(), nullable=True)
        batch_op.alter_column("target_id", existing_type=sa.Integer(), nullable=True)
        batch_op.create_foreign_key(
            "fk_reaction_tasks_channel_comment_run_id",
            "channel_comment_runs",
            ["channel_comment_run_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint(
            "uq_comment_reaction_task_message_reactor",
            ["channel_comment_run_id", "message_id", "reactor_account_id"],
        )
        batch_op.create_check_constraint(
            "exactly_one_reaction_context",
            "(run_id IS NOT NULL AND channel_comment_run_id IS NULL "
            "AND campaign_id IS NOT NULL AND target_id IS NOT NULL) OR "
            "(run_id IS NULL AND channel_comment_run_id IS NOT NULL "
            "AND campaign_id IS NULL AND target_id IS NULL)",
        )

    op.create_table(
        "channel_comment_reaction_account_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "rule_id",
            sa.Integer(),
            sa.ForeignKey("channel_comment_rules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("send_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("blocked_reason", sa.String(length=100), nullable=True),
        sa.Column("blocked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "rule_id",
            "account_id",
            name="uq_channel_comment_reaction_account",
        ),
    )
    op.create_index(
        "ix_channel_comment_reaction_blocked",
        "channel_comment_reaction_account_states",
        ["rule_id", "send_blocked"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_channel_comment_reaction_blocked",
        table_name="channel_comment_reaction_account_states",
    )
    op.drop_table("channel_comment_reaction_account_states")
    op.execute("DELETE FROM reaction_tasks WHERE channel_comment_run_id IS NOT NULL")
    with op.batch_alter_table("reaction_tasks") as batch_op:
        batch_op.drop_constraint("exactly_one_reaction_context", type_="check")
        batch_op.drop_constraint(
            "uq_comment_reaction_task_message_reactor",
            type_="unique",
        )
        batch_op.drop_constraint(
            "fk_reaction_tasks_channel_comment_run_id",
            type_="foreignkey",
        )
        batch_op.drop_column("channel_comment_run_id")
        batch_op.alter_column("target_id", existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column("run_id", existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column("campaign_id", existing_type=sa.Integer(), nullable=False)

    op.drop_column("channel_comment_rules", "reaction_account_ids_json")
    op.drop_column("channel_comment_rules", "reaction_dialogue_step")
    op.drop_column("channel_comment_rules", "reaction_emoji")
    op.drop_column("channel_comment_rules", "reaction_count")
    op.drop_column("channel_comment_rules", "reactions_enabled")
