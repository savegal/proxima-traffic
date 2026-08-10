"""Add durable per-chat rotation to broadcast mentions.

Revision ID: 0053_broadcast_mention_rotation
Revises: 0052_comment_reactions
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0053_broadcast_mention_rotation"
down_revision: str | None = "0052_comment_reactions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaigns",
        sa.Column(
            "mention_cooldown_days",
            sa.Integer(),
            nullable=False,
            server_default="30",
        ),
    )
    op.add_column(
        "campaign_runs",
        sa.Column("mention_cooldown_days_snapshot", sa.Integer(), nullable=True),
    )
    op.create_table(
        "campaign_mention_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("last_mentioned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("mention_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reserved_run_id", sa.Integer(), nullable=True),
        sa.Column("reserved_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["campaign_id"], ["campaigns.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["reserved_run_id"],
            ["campaign_runs.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "campaign_id",
            "target_id",
            "telegram_user_id",
            name="uq_campaign_mention_state_user",
        ),
    )
    op.create_index(
        "ix_campaign_mention_state_cooldown",
        "campaign_mention_states",
        ["campaign_id", "target_id", "last_mentioned_at"],
        unique=False,
    )
    op.create_index(
        "ix_campaign_mention_state_reservation",
        "campaign_mention_states",
        ["reserved_run_id", "reserved_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_campaign_mention_state_reservation",
        table_name="campaign_mention_states",
    )
    op.drop_index(
        "ix_campaign_mention_state_cooldown",
        table_name="campaign_mention_states",
    )
    op.drop_table("campaign_mention_states")
    op.drop_column("campaign_runs", "mention_cooldown_days_snapshot")
    op.drop_column("campaigns", "mention_cooldown_days")
