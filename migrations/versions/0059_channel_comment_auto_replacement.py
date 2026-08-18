"""Add an opt-in fallback sender for channel-comment rules.

Revision ID: 0059_channel_comment_auto_replacement
Revises: 0058_channel_comment_media_and_context
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0059_channel_comment_auto_replacement"
down_revision: str | None = "0058_channel_comment_media_and_context"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("channel_comment_rules")
    }
    if "auto_replace_primary_account" not in columns:
        op.add_column(
            "channel_comment_rules",
            sa.Column(
                "auto_replace_primary_account",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )


def downgrade() -> None:
    op.drop_column("channel_comment_rules", "auto_replace_primary_account")
