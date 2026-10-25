"""Small, reusable presentation primitives for the Telegram control panel.

The panel uses language, hierarchy and predictable navigation to stay readable
across Telegram clients. This module keeps those details
in one place and deliberately leaves callback payloads untouched.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from functools import wraps
from html import unescape
from html.parser import HTMLParser

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

_EXACT_BUTTON_LABELS = {
    "🏠 Главная": "⌂ Главная",
    "⌂ Главная": "⌂ Главная",
    "⌂ На главную": "⌂ Главная",
    "🔄 Обновить": "↻ Обновить",
    "↻ Обновить данные": "↻ Обновить",
    "🔄 Обновить список": "↻ Обновить список",
    "🔄 Обновить состояние": "↻ Обновить состояние",
    "✖️ Отмена": "× Отменить",
    "✖️ Отменить": "× Отменить",
    "× Отмена": "× Отменить",
    "✕ Отменить ввод": "× Отменить",
    "✖️ Не запускать": "‹ Без запуска",
    "× Не запускать": "‹ Без запуска",
    "⬅️": "‹ Назад",
    "➡️": "Далее ›",
    "⏮": "«",
    "⏭": "»",
    "‹": "‹",
    "›": "›",
}

# Keep operator-provided names readable; this is our presentation limit.
MAX_INLINE_BUTTON_TEXT_LENGTH = 64


def _text_units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


class _PanelHtml(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.parts: list[tuple[str, int, str | None, bool]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.parts.append((self.get_starttag_text(), 0, tag, False))

    def handle_endtag(self, tag: str) -> None:
        self.parts.append((f"</{tag}>", 0, tag, True))

    def handle_data(self, data: str) -> None:
        self.parts.append((data, _text_units(data), None, False))

    def handle_entityref(self, name: str) -> None:
        raw = f"&{name};"
        self.parts.append((raw, _text_units(unescape(raw)), "entity", False))

    def handle_charref(self, name: str) -> None:
        self.handle_entityref(f"#{name}")


def fit_panel_text(text: str, limit: int = 4096) -> str:
    """Bound visible UTF-16 text without cutting an HTML tag or an entity."""

    if _text_units(text) <= limit:
        return text
    parser = _PanelHtml()
    parser.feed(text)
    parser.close()
    if sum(units for _, units, _, _ in parser.parts) <= limit:
        return text
    footer = "\n\n<i>Часть подробностей скрыта из-за лимита Telegram.</i>"
    remaining = max(
        0, limit - _text_units(unescape("\n\nЧасть подробностей скрыта из-за лимита Telegram."))
    )
    result: list[str] = []
    opened: list[str] = []
    for raw, units, tag, closing in parser.parts:
        if units > remaining:
            if tag is None:
                prefix: list[str] = []
                for character in raw:
                    size = _text_units(character)
                    if size > remaining:
                        break
                    prefix.append(character)
                    remaining -= size
                result.append("".join(prefix))
            break
        result.append(raw)
        remaining -= units
        if tag is not None and tag != "entity":
            if closing:
                if opened and opened[-1] == tag:
                    opened.pop()
            else:
                opened.append(tag)
    result.extend(f"</{tag}>" for tag in reversed(opened))
    return "".join(result).rstrip() + footer


# Keep the theme compatible with the older aiogram versions allowed by the app.
_BUTTON_STYLES_SUPPORTED = "style" in InlineKeyboardButton.model_fields
_PRIMARY_ACTIONS = frozenset(
    {"order:new", "dm:new", "dm:service:new", "inv:new", "comments:add", "pr:add", "story:new"}
)
_CONFIRMED_DESTRUCTIVE_ACTION = re.compile(
    r":(?:delete:confirm|deleteok|listdelok|purge:confirm|session:confirm|"
    r"launchcancelok|stopall:confirm|stop_all:confirm)(?::|$)"
)


def _button_style(button: InlineKeyboardButton) -> str | None:
    """Colour actions by their route, never by an operator-provided name."""

    explicit_style = getattr(button, "style", None)
    if explicit_style is not None:
        return explicit_style
    callback = button.callback_data or ""
    if (
        _CONFIRMED_DESTRUCTIVE_ACTION.search(callback)
        or callback.startswith("launch:cancel:")
        or callback == "account:about:bulk:clear:confirm"
    ):
        return "danger"
    if callback in _PRIMARY_ACTIONS or callback.endswith(":import"):
        return "primary"
    return None


def screen_title(title: str, subtitle: str | None = None) -> str:
    """Show a clear heading and supporting copy; inputs are trusted HTML."""

    heading = f"<b>{title}</b>"
    return f"{heading}\n{subtitle}" if subtitle else heading


def wizard_title(title: str, step: int, total: int) -> str:
    """Show the current step with a short, readable position indicator."""

    return screen_title(title, f"Шаг {step} из {total}")


def page_summary(total: int, page: int, page_size: int) -> str:
    """Use the same count, page and visible range on collection screens."""

    pages = max(1, (total + page_size - 1) // page_size)
    summary = f"Всего: {total} · страница {page + 1}/{pages}"
    first = page * page_size + 1
    last = min(total, (page + 1) * page_size)
    if first <= last:
        summary += f" · показано: {first}–{last}"
    return summary


def section(title: str) -> str:
    """Make information groups scannable without decorative separators."""

    return f"<b>{title}</b>"


def profile_symbol(key: str) -> str:
    """Text symbols distinguish pacing profiles without colour-dependent icons."""

    return {"active": "↗", "balanced": "≈", "gentle": "○"}.get(key, "≡")


def empty_state(title: str, body: str) -> str:
    """Standard empty-state copy: useful next action, no dead-end wording."""

    return f"<b>{title}</b>\n{body}"


def polish_button_text(text: str, callback_data: str | None = None) -> str:
    """Normalise generic button language while preserving feature-specific labels."""

    normalized = " ".join(text.split())
    if normalized in _EXACT_BUTTON_LABELS:
        normalized = _EXACT_BUTTON_LABELS[normalized]
    elif normalized.startswith("⬅️ "):
        normalized = f"‹ {normalized.removeprefix('⬅️ ').strip()}"
    elif normalized.startswith("➡️ "):
        normalized = f"{normalized.removeprefix('➡️ ').strip()} ›"
    elif normalized.startswith("✖️ "):
        normalized = f"× {normalized.removeprefix('✖️ ').strip()}"
    elif normalized.startswith("🔄 "):
        normalized = f"↻ {normalized.removeprefix('🔄 ').strip()}"

    if len(normalized) <= MAX_INLINE_BUTTON_TEXT_LENGTH:
        return normalized
    return f"{normalized[: MAX_INLINE_BUTTON_TEXT_LENGTH - 1].rstrip()}…"


def _is_navigation_row(buttons: list[InlineKeyboardButton]) -> bool:
    labels = {
        "‹",
        "›",
        "«",
        "»",
        "‹‹",
        "››",
        "‹ Предыдущая",
        "Следующая ›",
        "‹ Назад",
        "Вперёд ›",
        "« В начало",
        "В конец »",
    }
    if not 2 <= len(buttons) <= 5:
        return False
    has_counter = False
    for button in buttons:
        if button.text in labels:
            continue
        if (
            button.callback_data == "noop" or (button.callback_data or "").startswith("rr:g:")
        ) and re.fullmatch(
            r"[0-9][0-9 ,]*/[0-9 ,]+(?: ▾)?",
            button.text,
        ):
            has_counter = True
            continue
        return False
    return has_counter or all(button.text in labels for button in buttons)


def polish_markup(markup: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    """Apply the control-panel button language to an existing keyboard.

    Creating copies keeps the function safe for aiogram/Pydantic models and makes
    it suitable for every independently implemented module.
    """

    rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        if not row:
            continue
        buttons = []
        for button in row:
            updates: dict[str, object] = {
                "text": polish_button_text(button.text, button.callback_data)
            }
            if _BUTTON_STYLES_SUPPORTED and (style := _button_style(button)) is not None:
                updates["style"] = style
            buttons.append(button.model_copy(update=updates))
        compact = all(
            len(button.text) <= 8
            and (
                len(button.text.removeprefix("✓ ")) <= 2
                or button.callback_data == "noop"
                or re.fullmatch(r"(?:✓ )?\d+(?:\s*(?:с|сек|м|мин|ч|д|%))?", button.text)
            )
            for button in buttons
        )
        navigation = _is_navigation_row(buttons)
        column_limit = 5 if compact or navigation else 2
        current: list[InlineKeyboardButton] = []
        for button in buttons:
            destructive = _button_style(button) == "danger" or bool(
                re.search(r":(?:delete|stop|archive|cancel)(?::|$)", button.callback_data or "")
            )
            if destructive:
                if current:
                    rows.append(current)
                    current = []
                rows.append([button])
                continue
            if (
                current
                and not navigation
                and (
                    len(current) >= column_limit
                    or sum(len(item.text) for item in current) + len(button.text) > 48
                    or len(button.text) > 24
                    or any(len(item.text) > 24 for item in current)
                )
            ):
                rows.append(current)
                current = []
            current.append(button)
        if current:
            rows.append(current)
    return markup.model_copy(update={"inline_keyboard": rows})


def themed_keyboard[**P](
    factory: Callable[P, InlineKeyboardMarkup],
) -> Callable[P, InlineKeyboardMarkup]:
    """Decorate a keyboard factory without changing its public signature."""

    @wraps(factory)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> InlineKeyboardMarkup:
        return polish_markup(factory(*args, **kwargs))

    return wrapped
