"""Add account Premium metadata and custom operator groups.

Revision ID: 0061_account_management_groups
Revises: 0060_channel_comment_membership_gate_retry
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0061_account_management_groups"
down_revision: str | None = "0060_channel_comment_membership_gate_retry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    account_columns = {column["name"] for column in inspector.get_columns("accounts")}
    if "is_premium" not in account_columns:
        op.add_column("accounts", sa.Column("is_premium", sa.Boolean(), nullable=True))
    if "premium_checked_at" not in account_columns:
        op.add_column(
            "accounts",
            sa.Column("premium_checked_at", sa.DateTime(timezone=True), nullable=True),
        )

    account_indexes = {index["name"] for index in inspector.get_indexes("accounts")}
    if "ix_accounts_management" not in account_indexes:
        op.create_index(
            "ix_accounts_management",
            "accounts",
            ["status", "authorized", "is_premium", "id"],
        )

    tables = set(inspector.get_table_names())
    if "account_groups" not in tables:
        op.create_table(
            "account_groups",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("name", sa.String(length=100), nullable=False),
            sa.Column("description", sa.String(length=500), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("name"),
        )
    if "account_group_members" not in tables:
        op.create_table(
            "account_group_members",
            sa.Column("group_id", sa.Integer(), nullable=False),
            sa.Column("account_id", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["group_id"], ["account_groups.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("group_id", "account_id"),
        )
        op.create_index(
            "ix_account_group_members_account",
            "account_group_members",
            ["account_id"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "account_group_members" in tables:
        indexes = {index["name"] for index in inspector.get_indexes("account_group_members")}
        if "ix_account_group_members_account" in indexes:
            op.drop_index("ix_account_group_members_account", table_name="account_group_members")
        op.drop_table("account_group_members")
    if "account_groups" in tables:
        op.drop_table("account_groups")

    account_indexes = {index["name"] for index in inspector.get_indexes("accounts")}
    if "ix_accounts_management" in account_indexes:
        op.drop_index("ix_accounts_management", table_name="accounts")
    account_columns = {column["name"] for column in inspector.get_columns("accounts")}
    if "premium_checked_at" in account_columns:
        op.drop_column("accounts", "premium_checked_at")
    if "is_premium" in account_columns:
        op.drop_column("accounts", "is_premium")
