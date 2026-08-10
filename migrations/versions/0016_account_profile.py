"""Store editable Telegram account profile fields.

Revision ID: 0016_account_profile
Revises: 0015_shared_proxy_assignments
Create Date: 2026-07-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_account_profile"
down_revision: str | None = "0015_shared_proxy_assignments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.add_column(sa.Column("first_name", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("last_name", sa.String(length=64), nullable=True))
        batch.add_column(
            sa.Column(
                "has_profile_photo",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("accounts") as batch:
        batch.drop_column("has_profile_photo")
        batch.drop_column("last_name")
        batch.drop_column("first_name")
