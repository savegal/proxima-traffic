"""Store Telegram profile bio configured by the operator.

Revision ID: 0037_account_profile_about
Revises: 0036_reaction_task_message_uniqueness
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0037_account_profile_about"
down_revision: str | None = "0036_reaction_task_message_uniqueness"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(sa.Column("about", sa.String(length=70), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_column("about")
