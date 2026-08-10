"""Fence delayed-delivery verification across worker processes.

Revision ID: 0046_delayed_delivery_leases
Revises: 0045_notification_delivery_leases
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0046_delayed_delivery_leases"
down_revision: str | None = "0045_notification_delivery_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "action_logs",
        sa.Column("delayed_delivery_lease_owner", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "action_logs",
        sa.Column("delayed_delivery_lease_token", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "action_logs",
        sa.Column("delayed_delivery_lease_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("action_logs", "delayed_delivery_lease_until")
    op.drop_column("action_logs", "delayed_delivery_lease_token")
    op.drop_column("action_logs", "delayed_delivery_lease_owner")
