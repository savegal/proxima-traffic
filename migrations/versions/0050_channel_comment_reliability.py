"""Make channel comments archival, bounded and mention-rotation aware.

Revision ID: 0050_channel_comment_reliability
Revises: 0049_audit_followup_fixes
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0050_channel_comment_reliability"
down_revision: str | None = "0049_audit_followup_fixes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def upgrade() -> None:
    op.drop_index("ix_channel_comment_rules_scan", table_name="channel_comment_rules")
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table(
            "channel_comment_rules",
            recreate="always",
            naming_convention=_NAMING_CONVENTION,
        ) as batch_op:
            batch_op.add_column(sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True))
            batch_op.add_column(
                sa.Column(
                    "mention_cooldown_days",
                    sa.Integer(),
                    nullable=False,
                    server_default="30",
                )
            )
            batch_op.drop_constraint("uq_channel_comment_rules_name", type_="unique")
            batch_op.drop_constraint("uq_channel_comment_rules_channel_ref", type_="unique")
    else:
        op.add_column(
            "channel_comment_rules",
            sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        )
        op.add_column(
            "channel_comment_rules",
            sa.Column(
                "mention_cooldown_days",
                sa.Integer(),
                nullable=False,
                server_default="30",
            ),
        )
        inspector = sa.inspect(bind)
        for constraint in inspector.get_unique_constraints("channel_comment_rules"):
            columns = set(constraint.get("column_names") or ())
            name = constraint.get("name")
            if name and columns in ({"name"}, {"channel_ref"}):
                op.drop_constraint(name, "channel_comment_rules", type_="unique")

    op.add_column(
        "channel_comment_runs",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "channel_comment_runs",
        sa.Column("mention_cooldown_days_snapshot", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_channel_comment_rules_scan",
        "channel_comment_rules",
        ["enabled", "archived_at", "last_scanned_at"],
        unique=False,
    )
    op.create_index(
        "uq_channel_comment_rules_active_name",
        "channel_comment_rules",
        ["name"],
        unique=True,
        sqlite_where=sa.text("archived_at IS NULL"),
        postgresql_where=sa.text("archived_at IS NULL"),
    )
    op.create_index(
        "uq_channel_comment_rules_active_channel",
        "channel_comment_rules",
        ["channel_ref"],
        unique=True,
        sqlite_where=sa.text("archived_at IS NULL"),
        postgresql_where=sa.text("archived_at IS NULL"),
    )
    op.create_table(
        "channel_comment_mention_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("last_mentioned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("mention_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reserved_run_id", sa.Integer(), nullable=True),
        sa.Column("reserved_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["rule_id"],
            ["channel_comment_rules.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["reserved_run_id"],
            ["channel_comment_runs.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "rule_id",
            "telegram_user_id",
            name="uq_channel_comment_mention_state_user",
        ),
    )
    op.create_index(
        "ix_channel_comment_mention_state_cooldown",
        "channel_comment_mention_states",
        ["rule_id", "last_mentioned_at"],
        unique=False,
    )
    op.create_index(
        "ix_channel_comment_mention_state_reservation",
        "channel_comment_mention_states",
        ["reserved_run_id", "reserved_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_channel_comment_mention_state_reservation",
        table_name="channel_comment_mention_states",
    )
    op.drop_index(
        "ix_channel_comment_mention_state_cooldown",
        table_name="channel_comment_mention_states",
    )
    op.drop_table("channel_comment_mention_states")
    op.drop_index(
        "uq_channel_comment_rules_active_channel",
        table_name="channel_comment_rules",
    )
    op.drop_index(
        "uq_channel_comment_rules_active_name",
        table_name="channel_comment_rules",
    )
    op.drop_index("ix_channel_comment_rules_scan", table_name="channel_comment_rules")
    op.drop_column("channel_comment_runs", "mention_cooldown_days_snapshot")
    op.drop_column("channel_comment_runs", "expires_at")

    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table(
            "channel_comment_rules",
            recreate="always",
            naming_convention=_NAMING_CONVENTION,
        ) as batch_op:
            batch_op.drop_column("mention_cooldown_days")
            batch_op.drop_column("archived_at")
            batch_op.create_unique_constraint(
                "uq_channel_comment_rules_name",
                ["name"],
            )
            batch_op.create_unique_constraint(
                "uq_channel_comment_rules_channel_ref",
                ["channel_ref"],
            )
    else:
        op.drop_column("channel_comment_rules", "mention_cooldown_days")
        op.drop_column("channel_comment_rules", "archived_at")
        op.create_unique_constraint(
            "uq_channel_comment_rules_name",
            "channel_comment_rules",
            ["name"],
        )
        op.create_unique_constraint(
            "uq_channel_comment_rules_channel_ref",
            "channel_comment_rules",
            ["channel_ref"],
        )
    op.create_index(
        "ix_channel_comment_rules_scan",
        "channel_comment_rules",
        ["enabled", "last_scanned_at"],
        unique=False,
    )
