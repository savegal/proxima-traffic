"""Persist @SpamBot check metadata for account safety decisions.

Revision ID: 0033_spambot_account_status
Revises: 0032_target_quality_rating
Create Date: 2026-08-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0033_spambot_account_status"
down_revision: str | None = "0032_target_quality_rating"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(sa.Column("spambot_checked_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("spambot_status", sa.String(length=32), nullable=True))
        batch.create_index("ix_accounts_spambot_checked_at", ["spambot_checked_at"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_index("ix_accounts_spambot_checked_at")
        batch.drop_column("spambot_status")
        batch.drop_column("spambot_checked_at")
