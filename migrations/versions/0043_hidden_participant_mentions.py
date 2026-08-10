"""Add opt-in hidden participant mentions to posts and channel comments.

Revision ID: 0043_hidden_participant_mentions
Revises: 0042_channel_comment_message_edit
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0043_hidden_participant_mentions"
down_revision: str | None = "0042_channel_comment_message_edit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaigns",
        sa.Column(
            "hidden_mentions_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "campaign_runs",
        sa.Column("hidden_mentions_enabled_snapshot", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "campaign_runs",
        sa.Column("hidden_mention_user_ids_snapshot_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column(
            "hidden_mentions_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("hidden_mentions_enabled_snapshot", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("hidden_mention_user_ids_snapshot_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("channel_comment_runs", "hidden_mention_user_ids_snapshot_json")
    op.drop_column("channel_comment_runs", "hidden_mentions_enabled_snapshot")
    op.drop_column("channel_comment_rules", "hidden_mentions_enabled")
    op.drop_column("campaign_runs", "hidden_mention_user_ids_snapshot_json")
    op.drop_column("campaign_runs", "hidden_mentions_enabled_snapshot")
    op.drop_column("campaigns", "hidden_mentions_enabled")
