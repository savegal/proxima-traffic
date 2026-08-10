"""Add optional proxy rotation URLs.

Revision ID: 0014_proxy_rotation_url
Revises: 0013_campaign_run_sender_fk
Create Date: 2026-07-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_proxy_rotation_url"
down_revision: str | None = "0013_campaign_run_sender_fk"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("proxy_endpoints") as batch:
        batch.add_column(sa.Column("rotation_url_encrypted", sa.Text(), nullable=True))
        batch.add_column(sa.Column("last_rotated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("proxy_endpoints") as batch:
        batch.drop_column("last_rotated_at")
        batch.drop_column("rotation_url_encrypted")
