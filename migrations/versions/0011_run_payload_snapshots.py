"""Add per-run payload snapshots.

Revision ID: 0011_run_payload_snapshots
Revises: 0010_campaign_broadcast
Create Date: 2026-07-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011_run_payload_snapshots"
down_revision: str | None = "0010_campaign_broadcast"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "UPDATE accounts "
        "SET session_source = UPPER(session_source) "
        "WHERE session_source IN ('none', 'tdata', 'session_file', 'session_string')"
    )
    op.execute(
        "UPDATE campaigns "
        "SET mode = UPPER(mode) "
        "WHERE mode IN ('dialogue', 'broadcast')"
    )
    with op.batch_alter_table("campaign_runs") as batch:
        batch.add_column(sa.Column("primary_text_snapshot", sa.Text(), nullable=True))
        batch.add_column(sa.Column("reply_text_snapshot", sa.Text(), nullable=True))
        batch.add_column(sa.Column("reply_delay_seconds_snapshot", sa.Integer(), nullable=True))
    op.execute(
        "UPDATE campaign_runs "
        "SET primary_text_snapshot = ("
        "  SELECT campaigns.primary_text FROM campaigns "
        "  WHERE campaigns.id = campaign_runs.campaign_id"
        "), "
        "reply_text_snapshot = ("
        "  SELECT campaigns.reply_text FROM campaigns "
        "  WHERE campaigns.id = campaign_runs.campaign_id"
        "), "
        "reply_delay_seconds_snapshot = ("
        "  SELECT campaigns.reply_delay_seconds FROM campaigns "
        "  WHERE campaigns.id = campaign_runs.campaign_id"
        ")"
    )


def downgrade() -> None:
    with op.batch_alter_table("campaign_runs") as batch:
        batch.drop_column("reply_delay_seconds_snapshot")
        batch.drop_column("reply_text_snapshot")
        batch.drop_column("primary_text_snapshot")
