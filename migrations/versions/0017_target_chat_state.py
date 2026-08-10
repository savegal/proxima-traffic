"""Store Telegram chat sources and runtime state.

Revision ID: 0017_target_chat_state
Revises: 0016_account_profile
Create Date: 2026-07-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_target_chat_state"
down_revision: str | None = "0016_account_profile"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.add_column(sa.Column("chat_type", sa.String(length=24), nullable=True))
        batch.add_column(sa.Column("folder_slug", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("telegram_peer_id", sa.BigInteger(), nullable=True))
        batch.add_column(sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_error_code", sa.String(length=100), nullable=True))
        batch.add_column(sa.Column("last_error", sa.String(length=500), nullable=True))
        batch.add_column(sa.Column("last_error_account_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.drop_column("last_error_account_id")
        batch.drop_column("last_error")
        batch.drop_column("last_error_code")
        batch.drop_column("last_success_at")
        batch.drop_column("last_checked_at")
        batch.drop_column("telegram_peer_id")
        batch.drop_column("folder_slug")
        batch.drop_column("chat_type")
