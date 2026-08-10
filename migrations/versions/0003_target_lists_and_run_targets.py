"""Add named target lists and run target snapshots.

Revision ID: 0003_target_lists
Revises: 0002_message_templates
Create Date: 2026-07-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_target_lists"
down_revision: str | None = "0002_message_templates"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "target_lists",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "target_list_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("target_list_id", sa.Integer(), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["target_id"], ["targets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_list_id"], ["target_lists.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("target_list_id", "target_id", name="uq_target_list_item"),
    )
    with op.batch_alter_table("campaigns") as batch:
        batch.alter_column("target_id", existing_type=sa.Integer(), nullable=True)
        batch.add_column(sa.Column("target_list_id", sa.Integer(), nullable=True))
        batch.create_foreign_key(
            "fk_campaigns_target_list_id", "target_lists", ["target_list_id"], ["id"]
        )
        batch.create_check_constraint(
            "exactly_one_campaign_target",
            "(target_id IS NOT NULL AND target_list_id IS NULL) OR "
            "(target_id IS NULL AND target_list_id IS NOT NULL)",
        )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("target_id", sa.Integer(), nullable=True))
    op.execute(
        "UPDATE campaign_runs SET target_id = "
        "(SELECT campaigns.target_id FROM campaigns "
        "WHERE campaigns.id = campaign_runs.campaign_id)"
    )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.alter_column("target_id", existing_type=sa.Integer(), nullable=False)
        batch.create_foreign_key("fk_campaign_runs_target_id", "targets", ["target_id"], ["id"])


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_constraint("fk_campaign_runs_target_id", type_="foreignkey")
        batch.drop_column("target_id")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_constraint("exactly_one_campaign_target", type_="check")
        batch.drop_constraint("fk_campaigns_target_list_id", type_="foreignkey")
        batch.drop_column("target_list_id")
        batch.alter_column("target_id", existing_type=sa.Integer(), nullable=False)
    op.drop_table("target_list_items")
    op.drop_table("target_lists")
