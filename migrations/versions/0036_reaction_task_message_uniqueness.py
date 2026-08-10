"""Make reaction idempotency specific to the reacted message.

Revision ID: 0036_reaction_task_message_uniqueness
Revises: 0035_campaign_reactions
"""

from collections.abc import Sequence

revision: str = "0036_reaction_task_message_uniqueness"
down_revision: str | None = "0035_campaign_reactions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Keep the schema created by revision 0035.

    Revision 0035 already creates ``uq_reaction_task_message_reactor``.
    The former migration tried to replace a constraint that never existed,
    which made a clean Alembic upgrade fail on SQLite.
    """


def downgrade() -> None:
    """Revision 0035 already has the desired schema, so there is nothing to undo."""
