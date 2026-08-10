"""Add broadcast mode, spread window, sender pool and per-run sender.

Revision ID: 0010_campaign_broadcast
Revises: 0009_account_safety
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_campaign_broadcast"
down_revision: str | None = "0009_account_safety"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MODE_ENUM = sa.Enum("DIALOGUE", "BROADCAST", name="campaignmode", native_enum=False)


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(
            sa.Column(
                "mode",
                _MODE_ENUM,
                nullable=False,
                server_default="DIALOGUE",
            )
        )
        batch.add_column(
            sa.Column("spread_seconds", sa.Integer(), nullable=False, server_default="0")
        )
        batch.alter_column(
            "reply_account_id",
            existing_type=sa.Integer(),
            nullable=True,
        )

    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("sender_account_id", sa.Integer(), nullable=True))

    op.create_table(
        "campaign_accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("campaign_id", "account_id", name="uq_campaign_account"),
    )


def downgrade() -> None:
    op.drop_table("campaign_accounts")
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("sender_account_id")
    with op.batch_alter_table("campaigns") as batch:
        batch.alter_column(
            "reply_account_id",
            existing_type=sa.Integer(),
            nullable=False,
        )
        batch.drop_column("spread_seconds")
        batch.drop_column("mode")
