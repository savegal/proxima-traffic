"""Recognition rules for Telegram bots that require channel subscriptions.

The live gateway turns Telethon's reply markup into the small, transport-free
``InlineButton`` representation below.  Keeping the recognition policy here
makes it explicit which buttons may cause an account to join a channel.
"""

from __future__ import annotations

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
    "subscribe",
    "join the channel",
    "join channel",
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
    "підтверд",
    "готово",
    "verify",
    "check",
    "done",
)


def parse_membership_gate(text: str, buttons: tuple[InlineButton, ...]) -> MembershipGate | None:
    """Return a supported channel-subscription gate, otherwise ``None``.

    Only ``t.me``/``telegram.me`` links are accepted.  A notice that asks for
    a captcha or another manual check is deliberately rejected: the caller
    treats it as ordinary moderation rather than attempting to bypass it.
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

    parsed = urlparse(url.strip())
    host = parsed.netloc.casefold().removeprefix("www.")
    path = unquote(parsed.path).strip("/")
    if parsed.scheme.casefold() in {"http", "https"} and host in {"t.me", "telegram.me"}:
        if path.startswith("+") and len(path) > 1:
            return f"https://t.me/{path}"
        if path.startswith("joinchat/") and len(path.removeprefix("joinchat/")) > 0:
            return f"https://t.me/{path}"
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
        return f"@{path}"
    if parsed.scheme.casefold() == "tg" and parsed.netloc.casefold() == "resolve":
        domain = (parse_qs(parsed.query).get("domain") or [""])[0].strip().lstrip("@")
        if domain and "/" not in domain:
            return f"@{domain}"
    return None
