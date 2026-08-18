"""Allow one safe subscription-gate retry for an automated comment.

Revision ID: 0060_channel_comment_membership_gate_retry
Revises: 0059_channel_comment_auto_replacement
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0060_channel_comment_membership_gate_retry"
down_revision: str | None = "0059_channel_comment_auto_replacement"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("channel_comment_runs")
    }
    if "membership_gate_retry_count" not in columns:
        op.add_column(
            "channel_comment_runs",
            sa.Column(
                "membership_gate_retry_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )


def downgrade() -> None:
    op.drop_column("channel_comment_runs", "membership_gate_retry_count")
