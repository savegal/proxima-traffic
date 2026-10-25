"""Preserve an operator pause independently of Telegram's restriction state."""

import sqlalchemy as sa
from alembic import op

revision = "0078_account_operator_pause"
down_revision = "0077_campaign_execution_safety"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "accounts",
        sa.Column("operator_paused", sa.Boolean(), nullable=False, server_default="0"),
    )
    accounts = sa.table(
        "accounts",
        sa.column("status"),
        sa.column("cooldown_reason"),
        sa.column("operator_paused"),
    )
    op.execute(
        accounts.update()
        .where(
            accounts.c.status == "PAUSED",
            accounts.c.cooldown_reason.is_(None),
        )
        .values(operator_paused=True)
    )


def downgrade() -> None:
    op.drop_column("accounts", "operator_paused")
