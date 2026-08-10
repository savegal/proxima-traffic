"""Add delayed message editing to channel comments.

Revision ID: 0042_channel_comment_message_edit
Revises: 0041_membership_gate_retry
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0042_channel_comment_message_edit"
down_revision: str | None = "0041_membership_gate_retry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "channel_comment_rules",
        sa.Column("message_edit_initial_text", sa.Text(), nullable=True),
    )
    op.add_column(
        "channel_comment_rules",
        sa.Column("message_edit_delay_seconds", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("channel_comment_rules", "message_edit_delay_seconds")
    op.drop_column("channel_comment_rules", "message_edit_initial_text")
