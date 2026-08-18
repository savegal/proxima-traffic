"""Small, reusable presentation primitives for the Telegram control panel.

Telegram does not offer custom button colours or fonts, so consistency comes from
language, hierarchy and predictable navigation.  This module keeps those details
in one place and deliberately leaves callback payloads untouched.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

_EXACT_BUTTON_LABELS = {
    "🏠 Главная": "⌂ На главную",
    "🔄 Обновить": "↻ Обновить данные",
    "🔄 Обновить список": "↻ Обновить список",
    "🔄 Обновить состояние": "↻ Обновить состояние",
    "✖️ Отмена": "✕ Отменить ввод",
    "✖️ Отменить": "✕ Отменить",
    "✖️ Не запускать": "← Вернуться без запуска",
    "⬅️": "← Назад",
    "➡️": "Вперёд →",
    "⏮": "«",
    "⏭": "»",
    "‹": "←",
    "›": "→",
}


def screen_title(title: str, subtitle: str | None = None) -> str:
    """Return the restrained title treatment used by dashboard-like screens."""

    heading = f"<b>{title}</b>"
    return f"{heading}\n<i>{subtitle}</i>" if subtitle else heading


def section(title: str) -> str:
    """Make information groups scannable without decorative separators."""

    return f"<b>{title}</b>"


def empty_state(title: str, body: str) -> str:
    """Standard empty-state copy: useful next action, no dead-end wording."""

    return f"<b>{title}</b>\n{body}"


def polish_button_text(text: str, callback_data: str | None = None) -> str:
    """Normalise generic button language while preserving feature-specific labels."""

    normalized = " ".join(text.split())
    if normalized in _EXACT_BUTTON_LABELS:
        return _EXACT_BUTTON_LABELS[normalized]
    if normalized.startswith("⬅️ "):
        return f"← {normalized.removeprefix('⬅️ ').strip()}"
    if normalized.startswith("➡️ "):
        return f"{normalized.removeprefix('➡️ ').strip()} →"
    if normalized.startswith("✖️ "):
        return f"✕ {normalized.removeprefix('✖️ ').strip()}"
    if normalized.startswith("🔄 ") and normalized != "🔄 Обновить отчёт":
        return f"↻ {normalized.removeprefix('🔄 ').strip()}"
    if callback_data == "noop" and "/" in normalized:
        return normalized.replace(" / ", " / ")
    return normalized


def polish_markup(markup: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    """Apply the control-panel button language to an existing keyboard.

    Creating copies keeps the function safe for aiogram/Pydantic models and makes
    it suitable for every independently implemented module.
    """

    rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        rows.append(
            [
                button.model_copy(
                    update={"text": polish_button_text(button.text, button.callback_data)}
                )
                for button in row
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def themed_keyboard[**P](
    factory: Callable[P, InlineKeyboardMarkup],
) -> Callable[P, InlineKeyboardMarkup]:
    """Decorate a keyboard factory without changing its public signature."""

    @wraps(factory)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> InlineKeyboardMarkup:
        return polish_markup(factory(*args, **kwargs))

    return wrapped
