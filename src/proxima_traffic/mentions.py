"""Safe construction and stable sampling of hidden Telegram user mentions."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from html import escape

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from proxima_traffic.models import (
    Account,
    MentionBlacklistEntry,
    MentionBlacklistUsernameEntry,
    Subscriber,
)

HIDDEN_MENTION_MINIMUM = 5
HIDDEN_MENTION_MAXIMUM = 10
HIDDEN_MENTION_CHARACTER = "\u200b"
TELEGRAM_MESSAGE_LIMIT = 4096


async def load_mention_exclusion_user_ids(session: AsyncSession) -> set[int]:
    """Load every owned or explicitly blacklisted Telegram user ID."""

    owned = await session.scalars(
        select(Account.telegram_user_id).where(
            Account.ownership_confirmed.is_(True),
            Account.telegram_user_id.is_not(None),
        )
    )
    blacklisted = await session.scalars(select(MentionBlacklistEntry.telegram_user_id))
    matched_usernames = await session.scalars(
        select(Subscriber.telegram_user_id).where(
            func.lower(Subscriber.username).in_(select(MentionBlacklistUsernameEntry.username))
        )
    )
    return {
        int(user_id)
        for user_id in (*owned.all(), *blacklisted.all(), *matched_usernames.all())
        if user_id is not None and int(user_id) > 0
    }


def normalize_user_ids(
    values: Iterable[object],
    *,
    exclude_user_id: int | None = None,
    exclude_user_ids: Iterable[object] = (),
) -> tuple[int, ...]:
    """Return distinct, positive Telegram user IDs in their first-seen order."""

    result: list[int] = []
    seen: set[int] = set()
    excluded: set[int] = set()
    for value in exclude_user_ids:
        try:
            normalized_exclusion = int(value)
        except (TypeError, ValueError):
            continue
        if normalized_exclusion > 0:
            excluded.add(normalized_exclusion)
    if exclude_user_id is not None:
        excluded.add(int(exclude_user_id))
    for value in values:
        try:
            user_id = int(value)
        except (TypeError, ValueError):
            continue
        if user_id <= 0 or user_id in excluded or user_id in seen:
            continue
        seen.add(user_id)
        result.append(user_id)
    return tuple(result)


def select_hidden_mentions(
    candidates: Iterable[object],
    *,
    selection_key: str,
    maximum_count: int = HIDDEN_MENTION_MAXIMUM,
    exclude_user_id: int | None = None,
    exclude_user_ids: Iterable[object] = (),
) -> tuple[int, ...]:
    """Choose a reproducible five-to-ten person subset."""

    maximum_count = max(0, min(HIDDEN_MENTION_MAXIMUM, int(maximum_count)))
    if maximum_count < HIDDEN_MENTION_MINIMUM:
        return ()
    normalized = normalize_user_ids(
        candidates,
        exclude_user_id=exclude_user_id,
        exclude_user_ids=exclude_user_ids,
    )
    if len(normalized) < HIDDEN_MENTION_MINIMUM:
        return ()
    ranked = sorted(
        normalized,
        key=lambda user_id: hashlib.sha256(f"{selection_key}:{user_id}".encode()).digest(),
    )
    desired_count = HIDDEN_MENTION_MINIMUM + (
        int.from_bytes(
            hashlib.sha256(selection_key.encode("utf-8")).digest()[:2],
            byteorder="big",
        )
        % (HIDDEN_MENTION_MAXIMUM - HIDDEN_MENTION_MINIMUM + 1)
    )
    return tuple(ranked[: min(desired_count, maximum_count, len(ranked))])


def hidden_mention_capacity(text: str, *, message_limit: int = TELEGRAM_MESSAGE_LIMIT) -> int:
    """How many zero-width mention anchors can fit in a Telegram message."""

    return max(0, int(message_limit) - len(text))


def append_hidden_mentions(
    *,
    text: str,
    html_text: str | None,
    user_ids: Iterable[object],
    message_limit: int = TELEGRAM_MESSAGE_LIMIT,
) -> str | None:
    """Append invisible ``tg://user`` anchors while preserving formatting."""

    normalized = normalize_user_ids(user_ids)[:HIDDEN_MENTION_MAXIMUM]
    if len(normalized) < HIDDEN_MENTION_MINIMUM:
        return html_text
    capacity = hidden_mention_capacity(text, message_limit=message_limit)
    if capacity < HIDDEN_MENTION_MINIMUM:
        return html_text
    normalized = normalized[:capacity]
    if len(normalized) < HIDDEN_MENTION_MINIMUM:
        return html_text
    anchors = "".join(
        f'<a href="tg://user?id={user_id}">{HIDDEN_MENTION_CHARACTER}</a>' for user_id in normalized
    )
    return f"{html_text if html_text is not None else escape(text)}{anchors}"
