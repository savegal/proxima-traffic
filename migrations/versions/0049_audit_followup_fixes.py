"""Persist idempotent accounting for secondary Telegram actions.

Revision ID: 0049_audit_followup_fixes
Revises: 0048_completion_quality
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0049_audit_followup_fixes"
down_revision: str | None = "0048_completion_quality"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "account_send_receipts",
        sa.Column("operation_key_hash", sa.String(length=64), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("operation_key_hash"),
    )
    op.create_index(
        "ix_account_send_receipts_account",
        "account_send_receipts",
        ["account_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_account_send_receipts_account", table_name="account_send_receipts")
    op.drop_table("account_send_receipts")
