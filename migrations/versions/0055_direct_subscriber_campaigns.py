"""Add scalable subscriber lists and durable direct-message campaigns.

Revision ID: 0055_direct_subscriber_campaigns
Revises: 0054_spambot_response_snapshot
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0055_direct_subscriber_campaigns"
down_revision: str | None = "0054_spambot_response_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscriber_lists",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="IMPORTING"),
        sa.Column("member_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("premium_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("non_premium_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("username_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_import_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_import_added", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_import_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_import_duplicates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_import_invalid", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("last_imported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        "ix_subscriber_lists_status",
        "subscriber_lists",
        ["status", "updated_at"],
        unique=False,
    )

    op.create_table(
        "direct_campaigns",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("subscriber_list_id", sa.Integer(), nullable=False),
        sa.Column("segment", sa.String(length=16), nullable=False, server_default="ALL"),
        sa.Column("recipient_limit", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("contact_cooldown_days", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("primary_text", sa.Text(), nullable=False),
        sa.Column("primary_html", sa.Text(), nullable=True),
        sa.Column("primary_media_path", sa.String(length=500), nullable=True),
        sa.Column("primary_media_kind", sa.String(length=20), nullable=True),
        sa.Column("primary_media_name", sa.String(length=255), nullable=True),
        sa.Column("primary_media_size", sa.Integer(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "recipient_limit >= 1 AND recipient_limit <= 1000000",
            name="valid_direct_campaign_recipient_limit",
        ),
        sa.CheckConstraint(
            "contact_cooldown_days >= 0 AND contact_cooldown_days <= 3650",
            name="valid_direct_campaign_cooldown",
        ),
        sa.ForeignKeyConstraint(
            ["subscriber_list_id"], ["subscriber_lists.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        "ix_direct_campaigns_enabled",
        "direct_campaigns",
        ["enabled", "created_at"],
        unique=False,
    )

    op.create_table(
        "direct_campaign_accounts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["campaign_id"], ["direct_campaigns.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("campaign_id", "account_id", name="uq_direct_campaign_account"),
    )

    op.create_table(
        "direct_launches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("subscriber_list_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="QUEUED"),
        sa.Column("segment_snapshot", sa.String(length=16), nullable=False),
        sa.Column("recipient_limit_snapshot", sa.Integer(), nullable=False),
        sa.Column("contact_cooldown_days_snapshot", sa.Integer(), nullable=False),
        sa.Column("account_ids_snapshot_json", sa.Text(), nullable=False),
        sa.Column("primary_text_snapshot", sa.Text(), nullable=False),
        sa.Column("primary_html_snapshot", sa.Text(), nullable=True),
        sa.Column("primary_media_path_snapshot", sa.String(length=500), nullable=True),
        sa.Column("primary_media_kind_snapshot", sa.String(length=20), nullable=True),
        sa.Column("primary_media_name_snapshot", sa.String(length=255), nullable=True),
        sa.Column("primary_media_size_snapshot", sa.Integer(), nullable=True),
        sa.Column("total_tasks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("excluded_recent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("excluded_reserved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("consent_confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["campaign_id"], ["direct_campaigns.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["subscriber_list_id"], ["subscriber_lists.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_direct_launches_status",
        "direct_launches",
        ["status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_direct_launches_campaign",
        "direct_launches",
        ["campaign_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "subscribers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.String(length=32), nullable=True),
        sa.Column("is_premium", sa.Boolean(), nullable=True),
        sa.Column("premium_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("do_not_contact", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("do_not_contact_reason", sa.String(length=200), nullable=True),
        sa.Column("last_contacted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_delivery_uncertain_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("reserved_direct_launch_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["reserved_direct_launch_id"], ["direct_launches.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("telegram_user_id", name="uq_subscribers_telegram_user_id"),
    )
    op.create_index(
        "ix_subscribers_premium",
        "subscribers",
        ["is_premium", "do_not_contact"],
        unique=False,
    )
    op.create_index("ix_subscribers_username", "subscribers", ["username"], unique=False)
    op.create_index("ix_subscribers_contacted", "subscribers", ["last_contacted_at"], unique=False)
    op.create_index(
        "ix_subscribers_delivery_uncertain",
        "subscribers",
        ["last_delivery_uncertain_at"],
        unique=False,
    )
    op.create_index(
        "ix_subscribers_reservation",
        "subscribers",
        ["reserved_direct_launch_id"],
        unique=False,
    )

    op.create_table(
        "subscriber_list_members",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("subscriber_list_id", sa.Integer(), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["subscriber_list_id"], ["subscriber_lists.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "subscriber_list_id", "subscriber_id", name="uq_subscriber_list_member"
        ),
    )
    op.create_index(
        "ix_subscriber_list_members_subscriber",
        "subscriber_list_members",
        ["subscriber_id"],
        unique=False,
    )

    op.create_table(
        "subscriber_account_peers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("access_hash", sa.BigInteger(), nullable=True),
        sa.Column("send_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("blocked_reason", sa.String(length=100), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("subscriber_id", "account_id", name="uq_subscriber_account_peer"),
    )
    op.create_index(
        "ix_subscriber_account_peer_block",
        "subscriber_account_peers",
        ["account_id", "send_blocked"],
        unique=False,
    )

    op.create_table(
        "direct_message_tasks",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("launch_id", sa.Integer(), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=False),
        sa.Column("sender_account_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="QUEUED"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=100), nullable=True),
        sa.Column("lease_token", sa.String(length=32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_message_id", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["launch_id"], ["direct_launches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["sender_account_id"], ["accounts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("launch_id", "subscriber_id", name="uq_direct_message_recipient"),
    )
    op.create_index(
        "ix_direct_message_tasks_due",
        "direct_message_tasks",
        ["status", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "ix_direct_message_tasks_launch",
        "direct_message_tasks",
        ["launch_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_direct_message_tasks_lease",
        "direct_message_tasks",
        ["status", "lease_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_direct_message_tasks_lease", table_name="direct_message_tasks")
    op.drop_index("ix_direct_message_tasks_launch", table_name="direct_message_tasks")
    op.drop_index("ix_direct_message_tasks_due", table_name="direct_message_tasks")
    op.drop_table("direct_message_tasks")
    op.drop_index("ix_subscriber_account_peer_block", table_name="subscriber_account_peers")
    op.drop_table("subscriber_account_peers")
    op.drop_index("ix_subscriber_list_members_subscriber", table_name="subscriber_list_members")
    op.drop_table("subscriber_list_members")
    op.drop_index("ix_subscribers_reservation", table_name="subscribers")
    op.drop_index("ix_subscribers_delivery_uncertain", table_name="subscribers")
    op.drop_index("ix_subscribers_contacted", table_name="subscribers")
    op.drop_index("ix_subscribers_username", table_name="subscribers")
    op.drop_index("ix_subscribers_premium", table_name="subscribers")
    op.drop_table("subscribers")
    op.drop_index("ix_direct_launches_campaign", table_name="direct_launches")
    op.drop_index("ix_direct_launches_status", table_name="direct_launches")
    op.drop_table("direct_launches")
    op.drop_table("direct_campaign_accounts")
    op.drop_index("ix_direct_campaigns_enabled", table_name="direct_campaigns")
    op.drop_table("direct_campaigns")
    op.drop_index("ix_subscriber_lists_status", table_name="subscriber_lists")
    op.drop_table("subscriber_lists")
