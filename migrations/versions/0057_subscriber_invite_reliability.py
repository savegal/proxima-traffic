"""Make subscriber invitations conservative across restarts and sender changes.

Revision ID: 0057_subscriber_invite_reliability
Revises: 0056_subscriber_invites
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0057_subscriber_invite_reliability"
down_revision: str | None = "0056_subscriber_invites"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "subscriber_invite_launches",
        sa.Column("failure_code", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "subscriber_invite_launches", sa.Column("failure", sa.String(length=500), nullable=True)
    )
    op.add_column(
        "subscriber_invite_tasks",
        sa.Column("delivery_attempted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "subscriber_invite_launch_account_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("launch_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("send_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("blocked_reason", sa.String(length=100), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("blocked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["launch_id"], ["subscriber_invite_launches.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("launch_id", "account_id", name="uq_subscriber_invite_launch_account"),
    )
    op.create_index(
        "ix_subscriber_invite_launch_account_block",
        "subscriber_invite_launch_account_states",
        ["launch_id", "send_blocked"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_subscriber_invite_launch_account_block",
        table_name="subscriber_invite_launch_account_states",
    )
    op.drop_table("subscriber_invite_launch_account_states")
    op.drop_column("subscriber_invite_tasks", "delivery_attempted_at")
    op.drop_column("subscriber_invite_launches", "failure")
    op.drop_column("subscriber_invite_launches", "failure_code")
