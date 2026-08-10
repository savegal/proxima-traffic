"""Persist safe cancellation, mention outcomes and control-bot FSM drafts.

Revision ID: 0048_completion_quality
Revises: 0047_critical_execution_safety
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0048_completion_quality"
down_revision: str | None = "0047_critical_execution_safety"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaign_runs",
        sa.Column("hidden_mention_skip_reason", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "campaign_runs",
        sa.Column("hidden_mention_resolved_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "reaction_tasks",
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("hidden_mention_skip_reason", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("hidden_mention_resolved_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "participant_reaction_tasks",
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.create_table(
        "bot_fsm_states",
        sa.Column("storage_key", sa.String(length=255), nullable=False),
        sa.Column("state", sa.String(length=255), nullable=True),
        sa.Column("data_json", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("storage_key"),
    )


def downgrade() -> None:
    op.drop_table("bot_fsm_states")
    op.drop_column("participant_reaction_tasks", "cancel_requested")
    op.drop_column("channel_comment_runs", "cancel_requested")
    op.drop_column("channel_comment_runs", "hidden_mention_resolved_count")
    op.drop_column("channel_comment_runs", "hidden_mention_skip_reason")
    op.drop_column("reaction_tasks", "cancel_requested")
    op.drop_column("campaign_runs", "hidden_mention_resolved_count")
    op.drop_column("campaign_runs", "hidden_mention_skip_reason")
