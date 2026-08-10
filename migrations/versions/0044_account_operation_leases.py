"""Fence live Telegram account sessions across worker processes.

Revision ID: 0044_account_operation_leases
Revises: 0043_hidden_participant_mentions
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0044_account_operation_leases"
down_revision: str | None = "0043_hidden_participant_mentions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "account_operation_leases",
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(length=100), nullable=False),
        sa.Column("lease_token", sa.String(length=32), nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("account_id"),
    )
    op.create_index(
        "ix_account_operation_leases_until",
        "account_operation_leases",
        ["lease_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_account_operation_leases_until", table_name="account_operation_leases")
    op.drop_table("account_operation_leases")
