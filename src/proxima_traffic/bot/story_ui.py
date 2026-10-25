"""Pagination for the story campaign wizards."""

from zoneinfo import ZoneInfo

from aiogram.types import InlineKeyboardButton

from proxima_traffic.config import get_settings
from proxima_traffic.models import ensure_utc, utc_now


def story_next_run_text(value) -> str:
    moment = ensure_utc(value)
    if moment is None or moment <= utc_now():
        return ""
    timezone = get_settings().timezone
    local = moment.astimezone(ZoneInfo(timezone))
    return f"Следующая проверка очереди: <b>{local:%d.%m.%Y %H:%M}</b> ({timezone})"


def story_mode_label(simulated: bool | None, live_label: str) -> str:
    if simulated is None:
        return "Режим старой кампании неизвестен; для продолжения создайте новую"
    return "Симуляция" if simulated else live_label


def add_story_pages(builder, page: int, total: int, prefix: str, size: int = 20) -> None:
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton(text="« Назад", callback_data=f"{prefix}:{page - 1}"))
    if (page + 1) * size < total:
        buttons.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"{prefix}:{page + 1}"))
    if buttons:
        builder.row(*buttons)
