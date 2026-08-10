from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import SplitResult, parse_qs, urlsplit

TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog"}
USERNAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{3,31}")
SLUG_PATTERN = re.compile(r"[A-Za-z0-9_-]{5,128}")
NUMERIC_CHAT_PATTERN = re.compile(r"-[1-9][0-9]{0,19}")
RESERVED_PATHS = {
    "addemoji",
    "addlist",
    "addstickers",
    "apps",
    "c",
    "confirmphone",
    "faq",
    "iv",
    "joinchat",
    "login",
    "proxy",
    "setlanguage",
    "share",
    "socks",
}


class TargetReferenceKind(StrEnum):
    PUBLIC_USERNAME = "public_username"
    NUMERIC_CHAT_ID = "numeric_chat_id"
    PRIVATE_INVITE = "private_invite"
    CHAT_FOLDER = "chat_folder"


class TargetReferenceError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ParsedTargetReference:
    original: str
    canonical: str
    kind: TargetReferenceKind
    slug: str | None = None

    @property
    def deduplication_key(self) -> str:
        if self.kind == TargetReferenceKind.PUBLIC_USERNAME:
            return self.canonical.casefold()
        return self.canonical


@dataclass(frozen=True, slots=True)
class TargetImportIssue:
    line: int
    value: str
    reason: str


@dataclass(frozen=True, slots=True)
class TargetImportResult:
    targets: tuple[ParsedTargetReference, ...]
    folders: tuple[ParsedTargetReference, ...]
    issues: tuple[TargetImportIssue, ...]
    duplicates: int
    ignored: int

    @property
    def canonical_targets(self) -> list[str]:
        return [target.canonical for target in self.targets]


def parse_target_reference(value: str) -> ParsedTargetReference:
    raw = value.strip().strip("<>\"'")
    if not raw:
        raise TargetReferenceError("пустое значение")
    if len(raw) > 512:
        raise TargetReferenceError("ссылка длиннее 512 символов")

    if NUMERIC_CHAT_PATTERN.fullmatch(raw):
        return ParsedTargetReference(raw, raw, TargetReferenceKind.NUMERIC_CHAT_ID)
    if raw.isdigit():
        raise TargetReferenceError("числовой chat ID группы должен быть отрицательным")
    if raw.startswith("@"):
        return _public_username(raw[1:], raw)

    parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    if parsed.scheme.lower() == "tg":
        return _parse_tg_link(parsed, raw)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if host not in TELEGRAM_HOSTS:
        raise TargetReferenceError("поддерживаются только Telegram-ссылки, @username и chat ID")
    path = parsed.path.strip("/")
    if not path:
        raise TargetReferenceError("в Telegram-ссылке отсутствует адрес чата")
    parts = path.split("/")
    head = parts[0]

    if head.lower() == "s":
        if len(parts) < 2:
            raise TargetReferenceError("в ссылке на публикацию не указан публичный чат")
        return _public_username(parts[1], raw)
    if head.lower() == "addlist":
        if len(parts) != 2 or not SLUG_PATTERN.fullmatch(parts[1]):
            raise TargetReferenceError("некорректная ссылка на папку Telegram")
        return ParsedTargetReference(
            raw,
            f"https://t.me/addlist/{parts[1]}",
            TargetReferenceKind.CHAT_FOLDER,
            parts[1],
        )
    if head.lower() == "joinchat":
        if len(parts) != 2 or not SLUG_PATTERN.fullmatch(parts[1]):
            raise TargetReferenceError("некорректная приватная invite-ссылка")
        return _private_invite(raw, parts[1])
    if head.startswith("+"):
        slug = head[1:]
        if len(parts) != 1 or not SLUG_PATTERN.fullmatch(slug):
            raise TargetReferenceError("некорректная приватная invite-ссылка")
        return _private_invite(raw, slug)
    if head.lower() in RESERVED_PATHS:
        raise TargetReferenceError("ссылка ведёт не на чат или канал")
    return _public_username(head, raw)


def parse_target_batch(lines: list[str]) -> TargetImportResult:
    targets: list[ParsedTargetReference] = []
    folders: list[ParsedTargetReference] = []
    issues: list[TargetImportIssue] = []
    seen: set[str] = set()
    duplicates = 0
    ignored = 0
    for line_number, raw_value in enumerate(lines, start=1):
        value = raw_value.strip().removeprefix("\ufeff")
        if not value or value.startswith("#"):
            ignored += 1
            continue
        try:
            parsed = parse_target_reference(value)
        except TargetReferenceError as exc:
            issues.append(TargetImportIssue(line_number, value[:120], str(exc)))
            continue
        if parsed.deduplication_key in seen:
            duplicates += 1
            continue
        seen.add(parsed.deduplication_key)
        if parsed.kind == TargetReferenceKind.CHAT_FOLDER:
            folders.append(parsed)
        else:
            targets.append(parsed)
    return TargetImportResult(
        targets=tuple(targets),
        folders=tuple(folders),
        issues=tuple(issues),
        duplicates=duplicates,
        ignored=ignored,
    )


def _public_username(username: str, original: str) -> ParsedTargetReference:
    if not USERNAME_PATTERN.fullmatch(username):
        raise TargetReferenceError("некорректный публичный username")
    return ParsedTargetReference(
        original,
        f"@{username.lower()}",
        TargetReferenceKind.PUBLIC_USERNAME,
    )


def _private_invite(original: str, slug: str) -> ParsedTargetReference:
    return ParsedTargetReference(
        original,
        f"https://t.me/+{slug}",
        TargetReferenceKind.PRIVATE_INVITE,
        slug,
    )


def _parse_tg_link(parsed: SplitResult, original: str) -> ParsedTargetReference:
    host = (parsed.netloc or parsed.path).lower()
    query = parse_qs(parsed.query)
    if host == "resolve":
        domain = (query.get("domain") or [""])[0]
        return _public_username(domain, original)
    if host == "join":
        slug = (query.get("invite") or [""])[0]
        if not SLUG_PATTERN.fullmatch(slug):
            raise TargetReferenceError("некорректная приватная invite-ссылка")
        return _private_invite(original, slug)
    if host == "addlist":
        slug = (query.get("slug") or [""])[0]
        if not SLUG_PATTERN.fullmatch(slug):
            raise TargetReferenceError("некорректная ссылка на папку Telegram")
        return ParsedTargetReference(
            original,
            f"https://t.me/addlist/{slug}",
            TargetReferenceKind.CHAT_FOLDER,
            slug,
        )
    raise TargetReferenceError("неподдерживаемая Telegram deep link")
