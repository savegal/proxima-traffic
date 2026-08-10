"""Allow one proxy endpoint to serve multiple assignment slots.

Revision ID: 0015_shared_proxy_assignments
Revises: 0014_proxy_rotation_url
Create Date: 2026-07-10
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015_shared_proxy_assignments"
down_revision: str | None = "0014_proxy_rotation_url"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("proxy_assignments") as batch:
        batch.drop_constraint("uq_proxy_assignments_proxy", type_="unique")


def downgrade() -> None:
    with op.batch_alter_table("proxy_assignments") as batch:
        batch.create_unique_constraint("uq_proxy_assignments_proxy", ["proxy_id"])
