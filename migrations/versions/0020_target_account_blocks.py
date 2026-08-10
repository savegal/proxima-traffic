"""Remember permanent account blocks per target chat.

Revision ID: 0020_target_account_blocks
Revises: 0019_smart_timer_broadcasts
Create Date: 2026-07-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0020_target_account_blocks"
down_revision: str | None = "0019_smart_timer_broadcasts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.add_column(
            sa.Column(
                "send_blocked",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.add_column(sa.Column("blocked_reason", sa.String(length=100), nullable=True))
        batch.add_column(sa.Column("blocked_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_error", sa.String(length=500), nullable=True))
        batch.create_index(
            "ix_target_account_send_blocked",
            ["target_id", "send_blocked"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.drop_index("ix_target_account_send_blocked")
        batch.drop_column("last_error")
        batch.drop_column("blocked_at")
        batch.drop_column("blocked_reason")
        batch.drop_column("send_blocked")
