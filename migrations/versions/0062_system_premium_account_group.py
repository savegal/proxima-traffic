"""Add an immutable system-backed Telegram Premium account section.

Revision ID: 0062_system_premium_account_group
Revises: 0061_account_management_groups
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0062_system_premium_account_group"
down_revision: str | None = "0061_account_management_groups"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("account_groups")}
    if "system_key" not in columns:
        op.add_column(
            "account_groups",
            sa.Column("system_key", sa.String(length=50), nullable=True),
        )
    indexes = {index["name"] for index in inspector.get_indexes("account_groups")}
    if "ux_account_groups_system_key" not in indexes:
        op.create_index(
            "ux_account_groups_system_key",
            "account_groups",
            ["system_key"],
            unique=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    indexes = {index["name"] for index in inspector.get_indexes("account_groups")}
    if "ux_account_groups_system_key" in indexes:
        op.drop_index("ux_account_groups_system_key", table_name="account_groups")
    columns = {column["name"] for column in inspector.get_columns("account_groups")}
    if "system_key" in columns:
        op.drop_column("account_groups", "system_key")
