"""Add encrypted proxy pool, groups and assignments.

Revision ID: 0005_proxy_management
Revises: 0004_run_notifications
Create Date: 2026-07-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_proxy_management"
down_revision: str | None = "0004_run_notifications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "proxy_groups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "proxy_endpoints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("label", sa.String(length=100), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("protocol", sa.String(length=7), nullable=False),
        sa.Column("host", sa.String(length=255), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("password_encrypted", sa.Text(), nullable=True),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(length=11), nullable=False),
        sa.Column("success_count", sa.Integer(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("observed_ip", sa.String(length=64), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("port >= 1 AND port <= 65535", name="valid_proxy_port"),
        sa.CheckConstraint(
            "success_count >= 0 AND failure_count >= 0 AND consecutive_failures >= 0",
            name="non_negative_proxy_counters",
        ),
        sa.ForeignKeyConstraint(["group_id"], ["proxy_groups.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("fingerprint", name="uq_proxy_endpoints_fingerprint"),
    )
    op.create_index("ix_proxy_endpoints_status_enabled", "proxy_endpoints", ["status", "enabled"])
    op.create_index("ix_proxy_endpoints_group", "proxy_endpoints", ["group_id"])
    op.create_table(
        "proxy_assignments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("proxy_id", sa.Integer(), nullable=False),
        sa.Column("assignment_key", sa.String(length=150), nullable=False),
        sa.Column("purpose", sa.String(length=250), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["proxy_id"], ["proxy_endpoints.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("proxy_id", name="uq_proxy_assignments_proxy"),
        sa.UniqueConstraint("assignment_key", name="uq_proxy_assignments_key"),
    )


def downgrade() -> None:
    op.drop_table("proxy_assignments")
    op.drop_index("ix_proxy_endpoints_group", table_name="proxy_endpoints")
    op.drop_index("ix_proxy_endpoints_status_enabled", table_name="proxy_endpoints")
    op.drop_table("proxy_endpoints")
    op.drop_table("proxy_groups")
