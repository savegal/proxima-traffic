"""Add missing campaign run sender account foreign key.

Revision ID: 0013_campaign_run_sender_fk
Revises: 0012_run_launch_keys
Create Date: 2026-07-09
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013_campaign_run_sender_fk"
down_revision: str | None = "0012_run_launch_keys"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.create_foreign_key(
            "fk_campaign_runs_sender_account_id",
            "accounts",
            ["sender_account_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_constraint("fk_campaign_runs_sender_account_id", type_="foreignkey")
