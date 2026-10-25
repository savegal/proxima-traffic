"""Persist story execution mode, retry budget and sender pacing."""

import sqlalchemy as sa
from alembic import op

revision = "0081_story_execution_reliability"
down_revision = "0080_story_viewer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("story_tagging_campaigns", "story_viewer_campaigns"):
        op.add_column(table, sa.Column("simulated", sa.Boolean(), nullable=True))
        # Old campaigns have no trustworthy mode snapshot. Require an operator
        # review instead of continuing previously queued work in a different mode.
        op.execute(
            sa.text(f"UPDATE {table} SET status='PAUSED' WHERE status IN ('ACTIVE', 'active')")
        )
    for table in ("story_tagging_accounts", "story_viewer_accounts"):
        op.add_column(table, sa.Column("next_action_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "story_viewer_tasks",
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "story_viewer_tasks",
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default="3",
        ),
    )


def downgrade() -> None:
    op.drop_column("story_viewer_tasks", "max_attempts")
    op.drop_column("story_viewer_tasks", "attempt_count")
    for table in ("story_tagging_accounts", "story_viewer_accounts"):
        op.drop_column(table, "next_action_at")
    for table in ("story_tagging_campaigns", "story_viewer_campaigns"):
        op.drop_column(table, "simulated")
