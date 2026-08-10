"""Track content permissions checked by Telegram preflight.

Revision ID: 0031_preflight_media_permissions
Revises: 0030_independent_adaptive_streams
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0031_preflight_media_permissions"
down_revision: str | None = "0030_independent_adaptive_streams"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.add_column(sa.Column("preflight_media_kinds", sa.String(length=100), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("target_account_states") as batch:
        batch.drop_column("preflight_media_kinds")
