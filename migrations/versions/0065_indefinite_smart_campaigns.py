"""Add explicit no-deadline snapshots for active campaign launches.

Revision ID: 0065_indefinite_smart_campaigns
Revises: 0064_channel_comment_formatted_text
Create Date: 2026-08-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0065_indefinite_smart_campaigns"
down_revision: str | None = "0064_channel_comment_formatted_text"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # SQLite supports ADD COLUMN with a constant default.  Using batch mode
    # would rebuild ``campaigns`` and fail while ``campaign_runs`` references
    # it with foreign keys enabled.
    op.add_column(
        "campaigns",
        sa.Column(
            "repeat_forever",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "campaign_runs",
        sa.Column(
            "repeat_forever_snapshot",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("repeat_forever_snapshot")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("repeat_forever")
