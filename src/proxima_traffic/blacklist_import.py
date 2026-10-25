"""Plain-text blacklist imports and shared ID/username exclusion checks."""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from proxima_traffic.models import MentionBlacklistEntry, MentionBlacklistUsernameEntry, Subscriber

MAX_BLACKLIST_IMPORT_BYTES = 2 * 1024 * 1024
MAX_BLACKLIST_IMPORT_ENTRIES = 100_000
TELEGRAM_USER_ID_MAX = (1 << 63) - 1
USERNAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{3,31}")


@dataclass(frozen=True, slots=True)
class BlacklistImport:
    telegram_user_ids: tuple[int, ...]
    usernames: tuple[str, ...]
    duplicates: int = 0


def normalize_blacklist_username(value: str) -> str:
    candidate = value.strip().removeprefix("@").casefold()
    if USERNAME_PATTERN.fullmatch(candidate) is None:
        raise ValueError(f"некорректный username: {value[:40]}")
    return candidate


def parse_blacklist_import(value: str) -> BlacklistImport:
    """Accept ID and @username lists with newlines, spaces, commas or semicolons."""

    tokens = [token for token in re.split(r"[\s,;]+", value.removeprefix("\ufeff")) if token]
    if not tokens:
        raise ValueError("укажите хотя бы один Telegram ID или @username")
    if len(tokens) > MAX_BLACKLIST_IMPORT_ENTRIES:
        raise ValueError(f"за один раз можно добавить до {MAX_BLACKLIST_IMPORT_ENTRIES:,} записей")
    ids: dict[int, None] = {}
    usernames: dict[str, None] = {}
    duplicates = 0
    for position, token in enumerate(tokens, start=1):
        try:
            if token.startswith("@") or not token.lstrip("+-").isdigit():
                username = normalize_blacklist_username(token)
                duplicates += username in usernames
                usernames[username] = None
            else:
                user_id = int(token)
                if not 1 <= user_id <= TELEGRAM_USER_ID_MAX:
                    raise ValueError(f"Telegram ID вне допустимого диапазона: {token[:30]}")
                duplicates += user_id in ids
                ids[user_id] = None
        except ValueError as exc:
            raise ValueError(f"запись {position}: {exc}") from exc
    return BlacklistImport(tuple(ids), tuple(usernames), duplicates)


def decode_blacklist_document(content: bytes) -> str:
    if len(content) > MAX_BLACKLIST_IMPORT_BYTES:
        raise ValueError("файл стоп-листа должен быть не больше 2 МБ")
    encoding = "utf-16" if content.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    try:
        return content.decode(encoding)
    except UnicodeDecodeError as exc:
        raise ValueError("сохраните TXT в UTF-8 или UTF-16 с BOM") from exc


def subscriber_blacklisted_condition() -> ColumnElement[bool]:
    """Keep audience selection and replacement selection consistent."""

    return or_(
        Subscriber.telegram_user_id.in_(select(MentionBlacklistEntry.telegram_user_id)),
        func.coalesce(func.lower(Subscriber.username), "").in_(
            select(MentionBlacklistUsernameEntry.username)
        ),
    )


async def user_is_blacklisted(
    session: AsyncSession, telegram_user_id: int, username: str | None
) -> bool:
    if await session.get(MentionBlacklistEntry, telegram_user_id) is not None:
        return True
    return bool(
        username
        and await session.get(MentionBlacklistUsernameEntry, username.lstrip("@").casefold())
        is not None
    )
