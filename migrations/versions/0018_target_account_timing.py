"""Store per-account Telegram chat timing state.

Revision ID: 0018_target_account_timing
Revises: 0017_target_chat_state
Create Date: 2026-07-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_target_account_timing"
down_revision: str | None = "0017_target_chat_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.add_column(
            sa.Column(
                "manual_cooldown_seconds",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(
            sa.Column("repeat_count", sa.Integer(), nullable=False, server_default="1")
        )
        batch.add_column(
            sa.Column(
                "repeat_interval_seconds",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(
            sa.Column("cycle_index", sa.Integer(), nullable=False, server_default="0")
        )
    op.create_table(
        "target_account_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("slowmode_seconds", sa.Integer(), nullable=True),
        sa.Column("next_send_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_message_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("target_id", "account_id", name="uq_target_account_state"),
    )
    op.create_index(
        "ix_target_account_next_send",
        "target_account_states",
        ["account_id", "next_send_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_target_account_next_send", table_name="target_account_states")
    op.drop_table("target_account_states")
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("cycle_index")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("repeat_interval_seconds")
        batch.drop_column("repeat_count")
    with op.batch_alter_table("targets") as batch:
        batch.drop_column("manual_cooldown_seconds")
