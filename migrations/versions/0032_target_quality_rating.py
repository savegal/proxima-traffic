"""Add automatic target quality rating and failure reason codes.

Revision ID: 0032_target_quality_rating
Revises: 0031_preflight_media_permissions
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0032_target_quality_rating"
down_revision: str | None = "0031_preflight_media_permissions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("targets") as batch:
        batch.add_column(
            sa.Column(
                "quality_status",
                sa.String(length=20),
                nullable=False,
                server_default="learning",
            )
        )
        batch.add_column(
            sa.Column("quality_score", sa.Integer(), nullable=False, server_default="100")
        )
        batch.add_column(sa.Column("quality_reason", sa.String(length=500), nullable=True))
        batch.add_column(
            sa.Column("quality_attempts", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("quality_successes", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("quality_failures", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(
            sa.Column("quality_banned_accounts", sa.Integer(), nullable=False, server_default="0")
        )
        batch.add_column(sa.Column("quality_updated_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("quality_bad_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("quality_reset_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_index("ix_targets_quality_status", ["quality_status"], unique=False)

    with op.batch_alter_table("action_logs") as batch:
        batch.add_column(sa.Column("error_code", sa.String(length=100), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("action_logs") as batch:
        batch.drop_column("error_code")

    with op.batch_alter_table("targets") as batch:
        batch.drop_index("ix_targets_quality_status")
        batch.drop_column("quality_reset_at")
        batch.drop_column("quality_bad_at")
        batch.drop_column("quality_updated_at")
        batch.drop_column("quality_banned_accounts")
        batch.drop_column("quality_failures")
        batch.drop_column("quality_successes")
        batch.drop_column("quality_attempts")
        batch.drop_column("quality_reason")
        batch.drop_column("quality_score")
        batch.drop_column("quality_status")
