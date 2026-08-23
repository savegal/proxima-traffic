"""Index campaign queue polling and expired-lease recovery paths.

Revision ID: 0063_campaign_queue_poll_indexes
Revises: 0062_system_premium_account_group
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0063_campaign_queue_poll_indexes"
down_revision: str | None = "0062_system_premium_account_group"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    indexes = {index["name"] for index in inspector.get_indexes("campaign_runs")}
    if "ix_campaign_runs_due" not in indexes:
        op.create_index(
            "ix_campaign_runs_due",
            "campaign_runs",
            ["status", "scheduled_for", "id"],
        )
    if "ix_campaign_runs_expired_lease" not in indexes:
        op.create_index(
            "ix_campaign_runs_expired_lease",
            "campaign_runs",
            ["status", "lease_until"],
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    indexes = {index["name"] for index in inspector.get_indexes("campaign_runs")}
    if "ix_campaign_runs_expired_lease" in indexes:
        op.drop_index("ix_campaign_runs_expired_lease", table_name="campaign_runs")
    if "ix_campaign_runs_due" in indexes:
        op.drop_index("ix_campaign_runs_due", table_name="campaign_runs")
