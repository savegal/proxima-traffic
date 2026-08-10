"""Separate standalone chats from chats owned by audience lists.

Revision ID: 0022_target_ownership
Revises: 0021_dialogue_chains
Create Date: 2026-07-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0022_target_ownership"
down_revision: str | None = "0021_dialogue_chains"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.add_column(
            sa.Column(
                "is_standalone",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            )
        )
        batch.create_index(
            "ix_targets_standalone_id",
            ["is_standalone", "id"],
            unique=False,
        )
    op.execute(
        "UPDATE targets SET is_standalone = 0 "
        "WHERE (folder_slug IS NOT NULL OR id IN "
        "(SELECT target_id FROM target_list_items)) "
        "AND id NOT IN (SELECT target_id FROM campaigns WHERE target_id IS NOT NULL) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM audit_events "
        "WHERE audit_events.entity_type = 'target' "
        "AND audit_events.event_type IN ('target.created', 'target.promoted_to_standalone') "
        "AND audit_events.entity_id = CAST(targets.id AS VARCHAR)"
        ")"
    )


def downgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.drop_index("ix_targets_standalone_id")
        batch.drop_column("is_standalone")
