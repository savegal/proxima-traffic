"""Add stable launch keys for target-list idempotency.

Revision ID: 0012_run_launch_keys
Revises: 0011_run_payload_snapshots
Create Date: 2026-07-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_run_launch_keys"
down_revision: str | None = "0011_run_payload_snapshots"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("launch_key", sa.String(length=64), nullable=True))
        batch.create_index("ix_campaign_runs_campaign_launch_key", ["campaign_id", "launch_key"])
    op.execute("UPDATE campaign_runs SET launch_key = request_key WHERE launch_key IS NULL")


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_index("ix_campaign_runs_campaign_launch_key")
        batch.drop_column("launch_key")
