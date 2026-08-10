"""Persist delivery receipts, send reservations and proxy connection generations.

Revision ID: 0047_critical_execution_safety
Revises: 0046_delayed_delivery_leases
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0047_critical_execution_safety"
down_revision: str | None = "0046_delayed_delivery_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "accounts",
        sa.Column(
            "connection_generation",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "accounts",
        sa.Column("send_reservation_token", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "accounts",
        sa.Column("send_reservation_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_accounts_send_reservation_until",
        "accounts",
        ["send_reservation_until"],
        unique=False,
    )
    op.create_table(
        "telegram_delivery_receipts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("external_message_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )


def downgrade() -> None:
    op.drop_table("telegram_delivery_receipts")
    op.drop_index("ix_accounts_send_reservation_until", table_name="accounts")
    op.drop_column("accounts", "send_reservation_until")
    op.drop_column("accounts", "send_reservation_token")
    op.drop_column("accounts", "connection_generation")
