"""Add durable media and actionable error context to channel comments.

Revision ID: 0058_channel_comment_media_and_context
Revises: 0057_subscriber_invite_reliability
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0058_channel_comment_media_and_context"
down_revision: str | None = "0057_subscriber_invite_reliability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "channel_comment_rules",
        sa.Column("primary_media_path", sa.String(length=500), nullable=True),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("primary_media_kind", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("primary_media_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "channel_comment_rules", sa.Column("primary_media_size", sa.Integer(), nullable=True)
    )
    op.add_column(
        "channel_comment_rules", sa.Column("last_error_account_id", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("channel_comment_rules", "last_error_account_id")
    op.drop_column("channel_comment_rules", "primary_media_size")
    op.drop_column("channel_comment_rules", "primary_media_name")
    op.drop_column("channel_comment_rules", "primary_media_kind")
    op.drop_column("channel_comment_rules", "primary_media_path")
