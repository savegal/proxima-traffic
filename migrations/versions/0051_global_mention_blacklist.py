"""Add the global hidden-mention blacklist.

Revision ID: 0051_global_mention_blacklist
Revises: 0050_channel_comment_reliability
"""

import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import op

revision: str = "0051_global_mention_blacklist"
down_revision: str | None = "0050_channel_comment_reliability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_BLACKLIST_ASSET = Path(__file__).resolve().parents[1] / "data" / "global_mention_blacklist_ids.txt"
_BLACKLIST_ASSET_SHA256 = "b8fb051310de5175eb1b7fb3166638946a4a77db93f146bd63ed894135e89784"
_BLACKLIST_ENTRY_COUNT = 1057


def _blacklisted_telegram_user_ids() -> tuple[int, ...]:
    raw = _BLACKLIST_ASSET.read_bytes()
    if hashlib.sha256(raw).hexdigest() != _BLACKLIST_ASSET_SHA256:
        raise RuntimeError("global mention blacklist migration asset failed integrity validation")
    values = tuple(int(value) for value in raw.decode("utf-8").splitlines() if value.strip())
    if len(values) != _BLACKLIST_ENTRY_COUNT or len(set(values)) != len(values):
        raise RuntimeError("global mention blacklist migration asset has invalid IDs")
    if any(value <= 0 for value in values):
        raise RuntimeError("global mention blacklist IDs must be positive")
    return values


def upgrade() -> None:
    table = op.create_table(
        "mention_blacklist_entries",
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("telegram_user_id"),
    )
    created_at = datetime(2026, 8, 6, tzinfo=UTC)
    op.bulk_insert(
        table,
        [
            {
                "telegram_user_id": user_id,
                "source": "operator_files_2026_08_06",
                "created_at": created_at,
            }
            for user_id in _blacklisted_telegram_user_ids()
        ],
    )


def downgrade() -> None:
    op.drop_table("mention_blacklist_entries")
