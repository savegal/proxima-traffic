"""Tune saved broadcast profiles for adaptive visibility.

Revision ID: 0029_adaptive_broadcast_profiles
Revises: 0028_content_variants_and_albums
Create Date: 2026-08-02
"""

from alembic import op

revision = "0029_adaptive_broadcast_profiles"
down_revision = "0028_content_variants_and_albums"
branch_labels = None
depends_on = None


def upgrade() -> None:
    _replace_profile(
        old=(24 * 3600, 15 * 60, 50, 300, 3600),
        new=(24 * 3600, 5 * 60, 10, 60, 15 * 60),
    )
    _replace_profile(
        old=(48 * 3600, 45 * 60, 100, 600, 3 * 3600),
        new=(48 * 3600, 20 * 60, 30, 3 * 60, 3600),
    )
    _replace_profile(
        old=(72 * 3600, 120 * 60, 200, 900, 6 * 3600),
        new=(72 * 3600, 60 * 60, 75, 10 * 60, 3 * 3600),
    )


def downgrade() -> None:
    _replace_profile(
        old=(24 * 3600, 5 * 60, 10, 60, 15 * 60),
        new=(24 * 3600, 15 * 60, 50, 300, 3600),
    )
    _replace_profile(
        old=(48 * 3600, 20 * 60, 30, 3 * 60, 3600),
        new=(48 * 3600, 45 * 60, 100, 600, 3 * 3600),
    )
    _replace_profile(
        old=(72 * 3600, 60 * 60, 75, 10 * 60, 3 * 3600),
        new=(72 * 3600, 120 * 60, 200, 900, 6 * 3600),
    )


def _replace_profile(
    *,
    old: tuple[int, int, int, int, int],
    new: tuple[int, int, int, int, int],
) -> None:
    connection = op.get_bind()
    connection.exec_driver_sql(
        """
        UPDATE campaigns
        SET smart_duration_seconds = ?,
            smart_no_timer_interval_seconds = ?,
            min_messages_between_posts = ?,
            activity_check_interval_seconds = ?,
            visibility_max_wait_seconds = ?
        WHERE mode = 'BROADCAST'
          AND smart_timer_mode = 1
          AND activity_aware = 1
          AND smart_duration_seconds = ?
          AND smart_no_timer_interval_seconds = ?
          AND min_messages_between_posts = ?
          AND activity_check_interval_seconds = ?
          AND visibility_max_wait_seconds = ?
        """,
        (*new, *old),
    )
