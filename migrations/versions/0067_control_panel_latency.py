"""Add exact subscriber counters maintained in the subscriber transaction."""

import sqlalchemy as sa
from alembic import op

revision = "0067_control_panel_latency"
down_revision = "0066_direct_message_reliability"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "subscriber_stats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("subscriber_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("premium_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("username_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("id = 1", name="ck_subscriber_stats_singleton"),
    )
    op.execute(
        "INSERT INTO subscriber_stats (id, subscriber_count, premium_count, username_count) "
        "SELECT 1, COUNT(*), COALESCE(SUM(is_premium IS 1), 0), "
        "COALESCE(SUM(username IS NOT NULL), 0) FROM subscribers"
    )
    op.execute(
        "CREATE TRIGGER trg_subscriber_stats_insert AFTER INSERT ON subscribers BEGIN "
        "UPDATE subscriber_stats SET "
        "subscriber_count = subscriber_count + 1, "
        "premium_count = premium_count + (NEW.is_premium IS 1), "
        "username_count = username_count + (NEW.username IS NOT NULL) WHERE id = 1; END"
    )
    op.execute(
        "CREATE TRIGGER trg_subscriber_stats_delete AFTER DELETE ON subscribers BEGIN "
        "UPDATE subscriber_stats SET "
        "subscriber_count = subscriber_count - 1, "
        "premium_count = premium_count - (OLD.is_premium IS 1), "
        "username_count = username_count - (OLD.username IS NOT NULL) WHERE id = 1; END"
    )
    op.execute(
        "CREATE TRIGGER trg_subscriber_stats_update "
        "AFTER UPDATE OF is_premium, username ON subscribers BEGIN "
        "UPDATE subscriber_stats SET "
        "premium_count = premium_count + (NEW.is_premium IS 1) - (OLD.is_premium IS 1), "
        "username_count = username_count + (NEW.username IS NOT NULL) "
        "- (OLD.username IS NOT NULL) WHERE id = 1; END"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_subscriber_stats_update")
    op.execute("DROP TRIGGER IF EXISTS trg_subscriber_stats_delete")
    op.execute("DROP TRIGGER IF EXISTS trg_subscriber_stats_insert")
    op.drop_table("subscriber_stats")
