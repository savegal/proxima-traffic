"""Track the one permitted retry after a subscription gate.

Revision ID: 0041_membership_gate_retry
Revises: 0040_delayed_delivery_retry
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0041_membership_gate_retry"
down_revision: str | None = "0040_delayed_delivery_retry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaign_runs",
        sa.Column("membership_gate_retry_count", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("campaign_runs", "membership_gate_retry_count")
