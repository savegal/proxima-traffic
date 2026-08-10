"""Persist account membership and write readiness for every target.

Revision ID: 0025_membership_preflight
Revises: 0024_target_list_item_enabled
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025_membership_preflight"
down_revision: str | None = "0024_target_list_item_enabled"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.add_column(
            sa.Column(
                "membership_status",
                sa.String(length=14),
                nullable=False,
                server_default="UNKNOWN",
            )
        )
        batch.add_column(sa.Column("can_send", sa.Boolean(), nullable=True))
        batch.add_column(
            sa.Column("membership_checked_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(sa.Column("membership_error_code", sa.String(length=100), nullable=True))
        batch.create_index(
            "ix_target_account_membership",
            ["target_id", "membership_status", "can_send"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.drop_index("ix_target_account_membership")
        batch.drop_column("membership_error_code")
        batch.drop_column("membership_checked_at")
        batch.drop_column("can_send")
        batch.drop_column("membership_status")
