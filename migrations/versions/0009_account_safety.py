"""Add account safety/pacing counters (cooldowns, daily caps, join throttle).

Revision ID: 0009_account_safety
Revises: 0008_account_pacing
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_account_safety"
down_revision: str | None = "0008_account_pacing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("cooldown_reason", sa.String(length=200), nullable=True))
        batch.add_column(
            sa.Column("flood_hits", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("flood_window_start", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("sent_today", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("day_window_start", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("sent_since_break", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("joins_today", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("join_window_start", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_join_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_column("last_join_at")
        batch.drop_column("join_window_start")
        batch.drop_column("joins_today")
        batch.drop_column("sent_since_break")
        batch.drop_column("day_window_start")
        batch.drop_column("sent_today")
        batch.drop_column("flood_window_start")
        batch.drop_column("flood_hits")
        batch.drop_column("cooldown_reason")
        batch.drop_column("cooldown_until")
