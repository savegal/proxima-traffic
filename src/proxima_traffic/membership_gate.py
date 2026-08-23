"""Recognition rules for Telegram bots that require channel subscriptions.

The live gateway turns Telethon's reply markup into the small, transport-free
``InlineButton`` representation below.  Keeping the recognition policy here
makes it explicit which buttons may cause an account to join a channel.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse


@dataclass(frozen=True, slots=True)
class InlineButton:
    """Only the button data relevant to a membership gate."""

    text: str
    url: str | None = None
    callback_data: bytes | None = None
    is_web_app: bool = False


@dataclass(frozen=True, slots=True)
class MembershipGate:
    """A supported, subscription-only gate extracted from a bot response."""

    notice: str
    channel_refs: tuple[str, ...]
    verification_callbacks: tuple[bytes, ...]


_SUBSCRIPTION_MARKERS = (
    "подпис",
    "підпис",
    "подпиш",
    "підпиш",
    "вступ",
    "присоедин",
    "приєдна",
    "участник",
    "учасник",
    "subscribe",
    "join the channel",
    "join channel",
    "membership",
    "become a member",
    "follow the channel",
    "follow channel",
    "join the group",
    "be a member",
    "member of the channel",
    "member of channel",
    "required channel",
    "required channels",
    "join our channel",
    "join our channels",
    "subscribe to the following",
    "subscription required",
    "subscription is required",
    "must be subscribed",
    "must join",
    "access after subscription",
    "доступ после подпис",
    "доступ лише після підпис",
    "доступ только после подпис",
    "нужно состоять",
    "необходимо состоять",
    "потрібно бути учасником",
    "необхідно бути учасником",
)
_CAPTCHA_OR_MANUAL_MARKERS = (
    "капч",
    "captcha",
    "не бот",
    "я не робот",
    "пройди провер",
    "пройдите провер",
    "i am not a bot",
    "human check",
    "verify yourself",
    "human verification",
)
_VERIFY_BUTTON_MARKERS = (
    "провер",
    "подтверд",
    "підтверд",
    "готово",
    "я подпис",
    "я підпис",
    "я вступ",
    "я приєдна",
    "уже подпис",
    "вже підпис",
    "получить доступ",
    "отримати доступ",
    "открыть доступ",
    "відкрити доступ",
    "разблокир",
    "розблокув",
    "продолжить",
    "продовжити",
    "активир",
    "verify",
    "check",
    "done",
    "i subscribed",
    "already subscribed",
    "i joined",
    "get access",
    "unlock",
    "continue",
)
_INVITE_SLUG = re.compile(r"[A-Za-z0-9_-]{5,128}")
_PUBLIC_USERNAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{4,31}")
_TEXT_TELEGRAM_URL = re.compile(
    r"(?i)(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/[^\s<>()[\]{}\"']+"
    r"|tg://resolve\?[^\s<>()[\]{}\"']+"
)
_TEXT_TELEGRAM_USERNAME = re.compile(r"(?<![\w@])@([A-Za-z][A-Za-z0-9_]{4,31})\b")
_URL_TRAILING_PUNCTUATION = ".,;:!?…»”')]}"


def parse_membership_gate(
    text: str,
    buttons: tuple[InlineButton, ...],
    *,
    text_links: tuple[str, ...] = (),
) -> MembershipGate | None:
    """Return a supported channel-subscription gate, otherwise ``None``.

    Telegram channel links may be placed in an inline keyboard, visible text,
    or a text-link entity.  A notice that asks for a captcha or another manual
    check is deliberately rejected: the caller treats it as ordinary
    moderation rather than attempting to bypass it.
    """

    notice = " ".join(text.split())[:500]
    normalized = notice.casefold()
    if not notice or not any(marker in normalized for marker in _SUBSCRIPTION_MARKERS):
        return None
    if any(marker in normalized for marker in _CAPTCHA_OR_MANUAL_MARKERS):
        return None

    channel_refs: list[str] = []
    callbacks: list[bytes] = []
    for button in buttons:
        if button.is_web_app:
            continue
        if button.url:
            channel_ref = telegram_channel_ref(button.url)
            if channel_ref is not None and channel_ref not in channel_refs:
                channel_refs.append(channel_ref)
        if (
            button.callback_data is not None
            and any(marker in button.text.casefold() for marker in _VERIFY_BUTTON_MARKERS)
            and button.callback_data not in callbacks
        ):
            callbacks.append(button.callback_data)

    for url in (*_telegram_urls_in_text(notice), *text_links):
        channel_ref = telegram_channel_ref(url)
        if channel_ref is not None and channel_ref not in channel_refs:
            channel_refs.append(channel_ref)

    # Moderator bots commonly write a public channel handle directly in the
    # gate text instead of adding a URL button.  This remains narrow: the
    # surrounding message has already been required to ask for a subscription,
    # and the gateway will still verify that the resolved peer is a Channel
    # before any join request is made.
    for match in _TEXT_TELEGRAM_USERNAME.finditer(notice):
        channel_ref = f"@{match.group(1).casefold()}"
        if channel_ref not in channel_refs:
            channel_refs.append(channel_ref)

    if not channel_refs:
        return None
    return MembershipGate(
        notice=notice,
        channel_refs=tuple(channel_refs),
        # A subscription gate should need one confirmation only.  Avoid
        # executing a chain of bot callbacks if a message exposes several.
        verification_callbacks=tuple(callbacks[:1]),
    )


def telegram_channel_ref(url: str) -> str | None:
    """Normalize a Telegram channel URL from an inline button.

    Public usernames and ordinary private invite links are supported.  Links
    to posts, folders, shares, mini apps and non-Telegram sites are rejected.
    """

    normalized_url = url.strip().rstrip(_URL_TRAILING_PUNCTUATION)
    if normalized_url.casefold().startswith(("t.me/", "www.t.me/", "telegram.me/")):
        normalized_url = f"https://{normalized_url}"
    parsed = urlparse(normalized_url)
    host = parsed.netloc.casefold().removeprefix("www.")
    path = unquote(parsed.path).strip("/")
    if parsed.scheme.casefold() in {"http", "https"} and host in {"t.me", "telegram.me"}:
        parts = path.split("/") if path else []
        if len(parts) == 1 and parts[0].startswith("+") and _INVITE_SLUG.fullmatch(parts[0][1:]):
            return f"https://t.me/{parts[0]}"
        if (
            len(parts) == 2
            and parts[0].casefold() == "joinchat"
            and _INVITE_SLUG.fullmatch(parts[1])
        ):
            return f"https://t.me/joinchat/{parts[1]}"
        # Moderation bots sometimes include a link to a concrete post rather
        # than to the channel header.  A public post still identifies one
        # channel precisely, so reduce it to that channel's @username.  The
        # private /c/ form has no public username and remains intentionally
        # unsupported.
        public_username: str | None = None
        if len(parts) in {1, 2} and parts and _PUBLIC_USERNAME.fullmatch(parts[0]):
            if len(parts) == 1 or parts[1].isdigit():
                public_username = parts[0]
        elif (
            len(parts) in {2, 3}
            and parts[0].casefold() == "s"
            and _PUBLIC_USERNAME.fullmatch(parts[1])
            and (len(parts) == 2 or parts[2].isdigit())
        ):
            public_username = parts[1]
        if public_username is not None:
            return f"@{public_username.casefold()}"
        if (
            not path
            or "/" in path
            or path.casefold()
            in {
                "addlist",
                "share",
                "iv",
                "c",
                "s",
            }
        ):
            return None
        return f"@{path.casefold()}" if _PUBLIC_USERNAME.fullmatch(path) else None
    if parsed.scheme.casefold() == "tg" and parsed.netloc.casefold() == "resolve":
        domain = (parse_qs(parsed.query).get("domain") or [""])[0].strip().lstrip("@")
        if _PUBLIC_USERNAME.fullmatch(domain):
            return f"@{domain.casefold()}"
    if parsed.scheme.casefold() == "tg" and parsed.netloc.casefold() == "join":
        invite = (parse_qs(parsed.query).get("invite") or [""])[0].strip()
        if _INVITE_SLUG.fullmatch(invite):
            return f"https://t.me/+{invite}"
    return None


def _telegram_urls_in_text(text: str) -> tuple[str, ...]:
    """Extract only syntactically explicit Telegram URLs from a gate notice."""

    return tuple(match.group(0) for match in _TEXT_TELEGRAM_URL.finditer(text))
