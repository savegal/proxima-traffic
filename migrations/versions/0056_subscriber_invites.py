"""Add durable consent-based invitations from subscriber lists.

Revision ID: 0056_subscriber_invites
Revises: 0055_direct_subscriber_campaigns
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0056_subscriber_invites"
down_revision: str | None = "0055_direct_subscriber_campaigns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscriber_invite_campaigns",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("subscriber_list_id", sa.Integer(), nullable=False),
        sa.Column("destination_ref", sa.String(length=255), nullable=False),
        sa.Column("destination_key", sa.String(length=255), nullable=False),
        sa.Column("destination_title", sa.String(length=255), nullable=True),
        sa.Column("segment", sa.String(length=16), nullable=False, server_default="ALL"),
        sa.Column("recipient_limit", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("contact_cooldown_days", sa.Integer(), nullable=False, server_default="90"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "recipient_limit >= 1 AND recipient_limit <= 1000000",
            name="valid_subscriber_invite_recipient_limit",
        ),
        sa.CheckConstraint(
            "contact_cooldown_days >= 0 AND contact_cooldown_days <= 3650",
            name="valid_subscriber_invite_cooldown",
        ),
        sa.ForeignKeyConstraint(
            ["subscriber_list_id"], ["subscriber_lists.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(
        "ix_subscriber_invite_campaigns_enabled",
        "subscriber_invite_campaigns",
        ["enabled", "created_at"],
        unique=False,
    )
    op.create_table(
        "subscriber_invite_campaign_accounts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["campaign_id"], ["subscriber_invite_campaigns.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "campaign_id", "account_id", name="uq_subscriber_invite_campaign_account"
        ),
    )
    op.create_table(
        "subscriber_invite_launches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("subscriber_list_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="QUEUED"),
        sa.Column("destination_ref_snapshot", sa.String(length=255), nullable=False),
        sa.Column("destination_key_snapshot", sa.String(length=255), nullable=False),
        sa.Column("destination_title_snapshot", sa.String(length=255), nullable=True),
        sa.Column("segment_snapshot", sa.String(length=16), nullable=False),
        sa.Column("recipient_limit_snapshot", sa.Integer(), nullable=False),
        sa.Column("contact_cooldown_days_snapshot", sa.Integer(), nullable=False),
        sa.Column("account_ids_snapshot_json", sa.Text(), nullable=False),
        sa.Column("total_tasks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("excluded_recent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("excluded_reserved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("consent_confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["campaign_id"], ["subscriber_invite_campaigns.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["subscriber_list_id"], ["subscriber_lists.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_subscriber_invite_launches_status",
        "subscriber_invite_launches",
        ["status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_subscriber_invite_launches_campaign",
        "subscriber_invite_launches",
        ["campaign_id", "created_at"],
        unique=False,
    )
    op.add_column(
        "subscribers", sa.Column("reserved_invite_launch_id", sa.Integer(), nullable=True)
    )
    op.add_column(
        "subscribers",
        sa.Column("last_invited_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("last_invite_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "subscribers", sa.Column("last_invited_target_ref", sa.String(length=255), nullable=True)
    )
    op.add_column(
        "subscribers",
        sa.Column("invite_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_subscribers_invite_reservation",
        "subscribers",
        ["reserved_invite_launch_id"],
        unique=False,
    )
    op.create_index(
        "ix_subscribers_invited", "subscribers", ["last_invite_attempt_at"], unique=False
    )
    op.create_table(
        "subscriber_invite_account_routes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("access_hash", sa.BigInteger(), nullable=True),
        sa.Column("invite_blocked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("blocked_reason", sa.String(length=100), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "subscriber_id", "account_id", name="uq_subscriber_invite_account_route"
        ),
    )
    op.create_index(
        "ix_subscriber_invite_route_block",
        "subscriber_invite_account_routes",
        ["account_id", "invite_blocked"],
        unique=False,
    )
    op.create_table(
        "subscriber_invite_tasks",
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
        sa.Column("already_participant", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["launch_id"], ["subscriber_invite_launches.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["sender_account_id"], ["accounts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("launch_id", "subscriber_id", name="uq_subscriber_invite_recipient"),
    )
    op.create_index(
        "ix_subscriber_invite_tasks_due",
        "subscriber_invite_tasks",
        ["status", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "ix_subscriber_invite_tasks_launch",
        "subscriber_invite_tasks",
        ["launch_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_subscriber_invite_tasks_lease",
        "subscriber_invite_tasks",
        ["status", "lease_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_subscriber_invite_tasks_lease", table_name="subscriber_invite_tasks")
    op.drop_index("ix_subscriber_invite_tasks_launch", table_name="subscriber_invite_tasks")
    op.drop_index("ix_subscriber_invite_tasks_due", table_name="subscriber_invite_tasks")
    op.drop_table("subscriber_invite_tasks")
    op.drop_index("ix_subscriber_invite_route_block", table_name="subscriber_invite_account_routes")
    op.drop_table("subscriber_invite_account_routes")
    op.drop_index("ix_subscribers_invited", table_name="subscribers")
    op.drop_index("ix_subscribers_invite_reservation", table_name="subscribers")
    op.drop_column("subscribers", "invite_count")
    op.drop_column("subscribers", "last_invited_target_ref")
    op.drop_column("subscribers", "last_invite_attempt_at")
    op.drop_column("subscribers", "last_invited_at")
    op.drop_column("subscribers", "reserved_invite_launch_id")
    op.drop_index("ix_subscriber_invite_launches_campaign", table_name="subscriber_invite_launches")
    op.drop_index("ix_subscriber_invite_launches_status", table_name="subscriber_invite_launches")
    op.drop_table("subscriber_invite_launches")
    op.drop_table("subscriber_invite_campaign_accounts")
    op.drop_index(
        "ix_subscriber_invite_campaigns_enabled", table_name="subscriber_invite_campaigns"
    )
    op.drop_table("subscriber_invite_campaigns")
