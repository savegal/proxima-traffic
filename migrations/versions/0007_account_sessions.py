"""Add encrypted account sessions imported from tdata/.session.

Revision ID: 0007_account_sessions
Revises: 0006_execution_fencing
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_account_sessions"
down_revision: str | None = "0006_execution_fencing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(
            sa.Column(
                "session_source",
                sa.Enum(
                    "NONE",
                    "TDATA",
                    "SESSION_FILE",
                    "SESSION_STRING",
                    name="accountsessionsource",
                    native_enum=False,
                ),
                nullable=False,
                server_default="NONE",
            )
        )
        batch.add_column(sa.Column("session_encrypted", sa.Text(), nullable=True))
        batch.add_column(sa.Column("api_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("api_hash_encrypted", sa.Text(), nullable=True))
        batch.add_column(sa.Column("phone", sa.String(length=32), nullable=True))
        batch.add_column(sa.Column("username", sa.String(length=64), nullable=True))
        batch.add_column(
            sa.Column("authorized", sa.Boolean(), nullable=False, server_default=sa.false())
        )
        batch.add_column(sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("last_error", sa.String(length=500), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_column("last_error")
        batch.drop_column("last_checked_at")
        batch.drop_column("authorized")
        batch.drop_column("username")
        batch.drop_column("phone")
        batch.drop_column("api_hash_encrypted")
        batch.drop_column("api_id")
        batch.drop_column("session_encrypted")
        batch.drop_column("session_source")
