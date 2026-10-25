"""Preserve Telegram formatting in channel comment rules.

Revision ID: 0064_channel_comment_formatted_text
Revises: 0063_campaign_queue_poll_indexes
Create Date: 2026-08-19
"""

import sqlalchemy as sa
from alembic import op

revision = "0064_channel_comment_formatted_text"
down_revision = "0063_campaign_queue_poll_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "channel_comment_rules",
        sa.Column("primary_html", sa.Text(), nullable=True),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("message_edit_initial_html", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("channel_comment_rules", "message_edit_initial_html")
    op.drop_column("channel_comment_rules", "primary_html")
