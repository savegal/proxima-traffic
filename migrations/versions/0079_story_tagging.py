"""Story tagging campaign tables for tagging subscribers in Telegram stories."""

import sqlalchemy as sa
from alembic import op

revision = "0079_story_tagging"
down_revision = "0078_account_operator_pause"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "story_tagging_campaigns",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=150), nullable=False, unique=True),
        sa.Column(
            "subscriber_list_id",
            sa.Integer(),
            sa.ForeignKey("subscriber_lists.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("media_path", sa.String(length=500), nullable=False),
        sa.Column("media_kind", sa.String(length=20), nullable=False, server_default="photo"),
        sa.Column("media_name", sa.String(length=255), nullable=True),
        sa.Column("media_size", sa.Integer(), nullable=True),
        sa.Column("caption", sa.Text(), nullable=False, server_default=""),
        sa.Column("tags_per_story", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("total_target_tags", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("daily_limit_per_account", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="180"),
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
        sa.CheckConstraint(
            "tags_per_story >= 1 AND tags_per_story <= 50",
            name="valid_story_tags_per_story",
        ),
        sa.CheckConstraint(
            "total_target_tags >= 1 AND total_target_tags <= 1000000",
            name="valid_story_total_target_tags",
        ),
    )
    op.create_index(
        "ix_story_tagging_campaigns_status",
        "story_tagging_campaigns",
        ["status", "created_at"],
    )

    op.create_table(
        "story_tagging_accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_tagging_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("stories_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_story_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("campaign_id", "account_id", name="uq_story_tagging_campaign_account"),
    )
    op.create_index(
        "ix_story_tagging_accounts_campaign",
        "story_tagging_accounts",
        ["campaign_id"],
    )

    op.create_table(
        "story_tagging_tasks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_tagging_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("story_id", sa.Integer(), nullable=True),
        sa.Column("story_url", sa.String(length=255), nullable=True),
        sa.Column("tagged_subscriber_ids_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("tagged_usernames_json", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column(
            "scheduled_for",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
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
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_story_tagging_tasks_poll",
        "story_tagging_tasks",
        ["status", "scheduled_for"],
    )
    op.create_index(
        "ix_story_tagging_tasks_campaign",
        "story_tagging_tasks",
        ["campaign_id", "status"],
    )

    op.create_table(
        "story_tagging_mention_states",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("story_tagging_campaigns.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "tagged_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("story_id", sa.Integer(), nullable=True),
        sa.UniqueConstraint(
            "campaign_id", "subscriber_id", name="uq_story_mention_campaign_subscriber"
        ),
    )
    op.create_index(
        "ix_story_mention_subscriber",
        "story_tagging_mention_states",
        ["subscriber_id"],
    )


def downgrade() -> None:
    op.drop_table("story_tagging_mention_states")
    op.drop_table("story_tagging_tasks")
    op.drop_table("story_tagging_accounts")
    op.drop_table("story_tagging_campaigns")
