"""Add durable multi-message dialogue chains.

Revision ID: 0021_dialogue_chains
Revises: 0020_target_account_blocks
Create Date: 2026-07-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0021_dialogue_chains"
down_revision: str | None = "0020_target_account_blocks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(sa.Column("dialogue_steps_json", sa.Text(), nullable=True))
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(
            sa.Column("dialogue_steps_snapshot_json", sa.Text(), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "dialogue_step_index",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(
            sa.Column("dialogue_last_message_id", sa.Integer(), nullable=True)
        )
    op.execute(
        "UPDATE campaign_runs "
        "SET dialogue_step_index = CASE "
        "WHEN reply_message_id IS NOT NULL THEN 2 "
        "WHEN primary_message_id IS NOT NULL THEN 1 ELSE 0 END"
    )
    op.execute(
        "UPDATE campaign_runs SET dialogue_last_message_id = "
        "COALESCE(reply_message_id, primary_message_id)"
    )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("dialogue_last_message_id")
        batch.drop_column("dialogue_step_index")
        batch.drop_column("dialogue_steps_snapshot_json")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("dialogue_steps_json")
