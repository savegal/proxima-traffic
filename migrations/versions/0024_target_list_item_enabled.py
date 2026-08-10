"""Make chat enablement independent for every audience list.

Revision ID: 0024_target_list_item_enabled
Revises: 0023_launch_archive
Create Date: 2026-07-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024_target_list_item_enabled"
down_revision: str | None = "0023_launch_archive"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("target_list_items") as batch:
        batch.add_column(
            sa.Column(
                "enabled",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch.create_index(
            "ix_target_list_items_enabled",
            ["target_list_id", "enabled"],
            unique=False,
        )
    op.execute(
        "UPDATE target_list_items SET enabled = 1 WHERE target_id IN "
        "(SELECT id FROM targets WHERE approved_by_operator = 1)"
    )
    op.execute(
        "UPDATE targets SET approved_by_operator = 0, approved_at = NULL "
        "WHERE is_standalone = 0"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE targets SET approved_by_operator = 1, approved_at = CURRENT_TIMESTAMP "
        "WHERE id IN (SELECT target_id FROM target_list_items WHERE enabled = 1)"
    )
    with op.batch_alter_table("target_list_items") as batch:
        batch.drop_index("ix_target_list_items_enabled")
        batch.drop_column("enabled")
