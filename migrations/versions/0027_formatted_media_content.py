"""Add formatted text and single-media snapshots for broadcast content.

Revision ID: 0027_formatted_media_content
Revises: 0026_activity_aware_scheduler
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027_formatted_media_content"
down_revision: str | None = "0026_activity_aware_scheduler"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(sa.Column("primary_html", sa.Text(), nullable=True))
        batch.add_column(sa.Column("primary_media_path", sa.String(500), nullable=True))
        batch.add_column(sa.Column("primary_media_kind", sa.String(20), nullable=True))
        batch.add_column(sa.Column("primary_media_name", sa.String(255), nullable=True))
        batch.add_column(sa.Column("primary_media_size", sa.Integer(), nullable=True))
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("primary_html_snapshot", sa.Text(), nullable=True))
        batch.add_column(sa.Column("primary_media_path_snapshot", sa.String(500), nullable=True))
        batch.add_column(sa.Column("primary_media_kind_snapshot", sa.String(20), nullable=True))
        batch.add_column(sa.Column("primary_media_name_snapshot", sa.String(255), nullable=True))
        batch.add_column(sa.Column("primary_media_size_snapshot", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("primary_media_size_snapshot")
        batch.drop_column("primary_media_name_snapshot")
        batch.drop_column("primary_media_kind_snapshot")
        batch.drop_column("primary_media_path_snapshot")
        batch.drop_column("primary_html_snapshot")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("primary_media_size")
        batch.drop_column("primary_media_name")
        batch.drop_column("primary_media_kind")
        batch.drop_column("primary_media_path")
        batch.drop_column("primary_html")
