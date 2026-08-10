"""Add execution fencing and attempt diagnostics.

Revision ID: 0006_execution_fencing
Revises: 0005_proxy_management
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_execution_fencing"
down_revision: str | None = "0005_proxy_management"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("lease_token", sa.String(length=32), nullable=True))
        batch.add_column(sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("error_code", sa.String(length=100), nullable=True))
        batch.create_index(
            "ix_campaign_runs_lease_fence",
            ["status", "lease_owner", "lease_token"],
        )
    with op.batch_alter_table("action_logs") as batch:
        batch.add_column(
            sa.Column("attempt_number", sa.Integer(), nullable=False, server_default="1")
        )
        batch.add_column(sa.Column("lease_token", sa.String(length=32), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("action_logs") as batch:
        batch.drop_column("lease_token")
        batch.drop_column("attempt_number")
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_index("ix_campaign_runs_lease_fence")
        batch.drop_column("error_code")
        batch.drop_column("heartbeat_at")
        batch.drop_column("lease_token")
