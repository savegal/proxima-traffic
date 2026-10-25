"""Persist global blacklist usernames separately from Telegram user IDs."""

import sqlalchemy as sa
from alembic import op

revision = "0069_blacklist_usernames"
down_revision = "0068_direct_launch_mode_snapshot"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mention_blacklist_usernames",
        sa.Column("username", sa.String(32), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("username"),
    )


def downgrade() -> None:
    op.drop_table("mention_blacklist_usernames")
