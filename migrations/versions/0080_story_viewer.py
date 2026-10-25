"""Story viewer and mass story reactions campaign tables."""

import sqlalchemy as sa
from alembic import op

revision = "0080_story_viewer"
down_revision = "0079_story_tagging"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "story_viewer_campaigns",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=150), nullable=False, unique=True),
        sa.Column(
            "subscriber_list_id",
            sa.Integer(),
            sa.ForeignKey("subscriber_lists.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("send_reaction", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("reaction_emoji", sa.String(length=50), nullable=False, server_default="🔥"),
        sa.Column("delay_min_seconds", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("delay_max_seconds", sa.Integer(), nullable=False, server_default="15"),
        sa.Column("daily_limit_per_account", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("max_targets", sa.Integer(), nullable=False, server_default="500"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="draft"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_story_viewer_campaigns_status",
        "story_viewer_campaigns",
        ["status", "created_at"],
    )

    op.create_table(
        "story_viewer_accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_viewer_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("views_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reactions_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_view_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("campaign_id", "account_id", name="uq_story_viewer_campaign_account"),
    )
    op.create_index(
        "ix_story_viewer_accounts_campaign",
        "story_viewer_accounts",
        ["campaign_id"],
    )

    op.create_table(
        "story_viewer_tasks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_viewer_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("stories_found", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stories_viewed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reaction_sent", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("reaction_emoji", sa.String(length=20), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column(
            "scheduled_for",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("lease_token", sa.String(length=32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_story_viewer_tasks_poll",
        "story_viewer_tasks",
        ["status", "scheduled_for"],
    )
    op.create_index(
        "ix_story_viewer_tasks_campaign",
        "story_viewer_tasks",
        ["campaign_id", "status"],
    )

    op.create_table(
        "story_viewer_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_viewer_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("story_id", sa.Integer(), nullable=False),
        sa.Column("reacted", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("emoji", sa.String(length=20), nullable=True),
        sa.Column(
            "viewed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "account_id", "subscriber_id", "story_id", name="uq_story_viewer_acc_sub_story"
        ),
    )
    op.create_index(
        "ix_story_viewer_records_subscriber",
        "story_viewer_records",
        ["subscriber_id"],
    )


def downgrade() -> None:
    op.drop_table("story_viewer_records")
    op.drop_table("story_viewer_tasks")
    op.drop_table("story_viewer_accounts")
    op.drop_table("story_viewer_campaigns")
