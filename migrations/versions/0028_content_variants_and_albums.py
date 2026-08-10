"""Add rotating content variants and media album snapshots.

Revision ID: 0028_content_variants_and_albums
Revises: 0027_formatted_media_content
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028_content_variants_and_albums"
down_revision: str | None = "0027_formatted_media_content"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("campaigns") as batch:
        batch.add_column(sa.Column("content_variants_json", sa.Text(), nullable=True))
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("media_items_snapshot_json", sa.Text(), nullable=True))
        batch.add_column(sa.Column("content_variants_snapshot_json", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column(
                "content_variant_index",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch.add_column(sa.Column("content_variant_id_snapshot", sa.String(40), nullable=True))
        batch.add_column(sa.Column("content_variant_name_snapshot", sa.String(80), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("content_variant_name_snapshot")
        batch.drop_column("content_variant_id_snapshot")
        batch.drop_column("content_variant_index")
        batch.drop_column("content_variants_snapshot_json")
        batch.drop_column("media_items_snapshot_json")
    with op.batch_alter_table("campaigns") as batch:
        batch.drop_column("content_variants_json")
