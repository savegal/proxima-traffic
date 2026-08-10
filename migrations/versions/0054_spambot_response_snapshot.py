"""Preserve the latest SpamBot response structure for diagnostics.

Revision ID: 0054_spambot_response_snapshot
Revises: 0053_broadcast_mention_rotation
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0054_spambot_response_snapshot"
down_revision: str | None = "0053_broadcast_mention_rotation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(sa.Column("spambot_parser_signal", sa.String(length=100), nullable=True))
        batch.add_column(sa.Column("spambot_response_json", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_column("spambot_response_json")
        batch.drop_column("spambot_parser_signal")
