"""Add reversible launch archive.

Revision ID: 0023_launch_archive
Revises: 0022_target_ownership
Create Date: 2026-07-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023_launch_archive"
down_revision: str | None = "0022_target_ownership"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_index(
            "ix_campaign_runs_archive",
            ["archived_at", "id"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_index("ix_campaign_runs_archive")
        batch.drop_column("archived_at")
