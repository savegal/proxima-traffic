"""Add optional durable campaign reactions.

Revision ID: 0035_campaign_reactions
Revises: 0034_channel_comments
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0035_campaign_reactions"
down_revision: str | None = "0034_channel_comments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaigns",
        sa.Column("reactions_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "campaigns", sa.Column("reaction_count", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column(
        "campaigns",
        sa.Column("reaction_emoji", sa.String(length=16), nullable=False, server_default="👍"),
    )
    op.add_column(
        "campaigns",
        sa.Column("reaction_dialogue_step", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_table(
        "reaction_tasks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "run_id",
            sa.Integer(),
            sa.ForeignKey("campaign_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "target_id",
            sa.Integer(),
            sa.ForeignKey("targets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("reactor_account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=False),
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
        sa.UniqueConstraint(
            "run_id",
            "message_id",
            "reactor_account_id",
            name="uq_reaction_task_message_reactor",
        ),
    )
    op.create_index(
        "ix_reaction_tasks_due", "reaction_tasks", ["status", "scheduled_for"], unique=False
    )
    op.create_index(
        "ix_reaction_tasks_campaign", "reaction_tasks", ["campaign_id", "created_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_reaction_tasks_campaign", table_name="reaction_tasks")
    op.drop_index("ix_reaction_tasks_due", table_name="reaction_tasks")
    op.drop_table("reaction_tasks")
    op.drop_column("campaigns", "reaction_dialogue_step")
    op.drop_column("campaigns", "reaction_emoji")
    op.drop_column("campaigns", "reaction_count")
    op.drop_column("campaigns", "reactions_enabled")
