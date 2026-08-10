from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from proxima_traffic.bot.repository import LaunchView, Page, TargetListView
from proxima_traffic.content import (
    MAX_ALBUM_ITEMS,
    MAX_CONTENT_VARIANTS,
    BroadcastContentVariant,
)
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignMode,
    CampaignRun,
    MessageTemplate,
    ProxyGroup,
    RunStatus,
    Target,
)
from proxima_traffic.proxies import ProxyPage, ProxyView
from proxima_traffic.scheduling import BROADCAST_PROFILES, detect_broadcast_profile


def _compact_button_label(value: str, limit: int = 46) -> str:
    normalized = " ".join(value.split())
    return normalized if len(normalized) <= limit else f"{normalized[: limit - 1].rstrip()}…"


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Создать рассылку",
                    callback_data="order:new",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📊 Запуски и результаты",
                    callback_data="menu:runs:0",
                ),
                InlineKeyboardButton(text="📣 Мои рассылки", callback_data="menu:campaigns:0"),
            ],
            [
                InlineKeyboardButton(text="💬 Автокомментарии", callback_data="comments:home:0"),
            ],
            [
                InlineKeyboardButton(text="👍 Авто-реакции в чатах", callback_data="pr:home:0"),
            ],
            [
                InlineKeyboardButton(text="🎯 Чаты и списки", callback_data="audiences:home"),
                InlineKeyboardButton(text="👥 Аккаунты", callback_data="menu:accounts:0"),
            ],
            [
                InlineKeyboardButton(text="✉️ Базы подписчиков и ЛС", callback_data="dm:home"),
            ],
            [
                InlineKeyboardButton(text="📝 Шаблоны", callback_data="menu:templates:0"),
                InlineKeyboardButton(text="🌐 Прокси", callback_data="menu:proxies:0"),
            ],
            [
                InlineKeyboardButton(text="❓ Как начать", callback_data="menu:guide"),
                InlineKeyboardButton(text="⚙️ Настройки и сервис", callback_data="menu:settings"),
            ],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:home")],
        ]
    )


def guide_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 · Аккаунты", callback_data="menu:accounts:0"),
                InlineKeyboardButton(text="2 · Чаты", callback_data="audiences:home"),
            ],
            [
                InlineKeyboardButton(
                    text="3 · Создать и проверить рассылку",
                    callback_data="order:new",
                )
            ],
            [InlineKeyboardButton(text="4 · Посмотреть результаты", callback_data="menu:runs:0")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def order_type_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Обычная рассылка",
                    callback_data="order:draft:broadcast",
                )
            ],
            [
                InlineKeyboardButton(
                    text="💬 Диалог от нескольких аккаунтов",
                    callback_data="order:draft:dialogue",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📣 Открыть рассылки",
                    callback_data="menu:campaigns:0",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def order_draft_menu(data: dict[str, object]) -> InlineKeyboardMarkup:
    mode = str(data.get("order_mode", "broadcast"))
    has_audience = data.get("target_id") is not None or data.get("target_list_id") is not None
    if mode == "broadcast":
        sender_count = len(data.get("sender_account_ids", []))
        content_ready = _has_broadcast_content(data)
        senders_ready = sender_count > 0
    else:
        sender_count = len(
            {
                int(step.get("account_id") or 0)
                for step in data.get("dialogue_steps", [])
                if isinstance(step, dict)
            }
        )
        content_ready = len(data.get("dialogue_steps", [])) >= 2
        senders_ready = sender_count >= 2
    ready = has_audience and senders_ready and content_ready
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✏️ Название",
                callback_data="order:edit:name",
            ),
            InlineKeyboardButton(
                text=("✅ Чаты" if has_audience else "⚠️ Чаты"),
                callback_data="order:edit:audience",
            ),
        ]
    ]
    independent = False
    if mode == "broadcast":
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{'✅' if sender_count else '⚠️'} Отправители · {sender_count}",
                    callback_data="order:edit:senders",
                ),
                InlineKeyboardButton(
                    text=(
                        f"✅ Пост · {_draft_media_label(data)}"
                        if content_ready and _draft_media_label(data)
                        else "✅ Пост"
                        if content_ready
                        else "⚠️ Пост"
                    ),
                    callback_data="order:edit:text",
                ),
            ]
        )
        independent = str(data.get("sender_mode") or "distributed") == "independent"
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🚀 Независимо · {sender_count} аккаунта на чат"
                        if independent
                        else "🤖 Умный выбор отправителя"
                    ),
                    callback_data="order:sendermode:toggle",
                )
            ]
        )
    else:
        step_count = len(data.get("dialogue_steps", []))
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{'✅' if content_ready else '⚠️'} Диалог · {step_count} сообщ.",
                    callback_data="order:edit:chain",
                )
            ]
        )
    smart = bool(data.get("smart_timer_mode", False))
    rows.append(
        [
            InlineKeyboardButton(
                text=(
                    "♻️ Повторять диалоги автоматически"
                    if smart and mode == "dialogue"
                    else "♻️ Повторять автоматически"
                    if smart
                    else "1️⃣ Один проход"
                ),
                callback_data="order:schedule:toggle",
            )
        ]
    )
    if smart:
        selected_profile = str(data.get("broadcast_profile", "active"))
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{'✅ ' if selected_profile == 'active' else ''}⚡ Активная",
                    callback_data="order:profile:active",
                ),
                InlineKeyboardButton(
                    text=f"{'✅ ' if selected_profile == 'balanced' else ''}⚖️ Средняя",
                    callback_data="order:profile:balanced",
                ),
                InlineKeyboardButton(
                    text=f"{'✅ ' if selected_profile == 'gentle' else ''}🌙 Бережная",
                    callback_data="order:profile:gentle",
                ),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚙️ Настроить частоту и условия",
                    callback_data="order:advanced",
                )
            ]
        )
    elif mode == "broadcast":
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🕒 Разложить за · {int(data.get('spread_hours', 0))} ч",
                    callback_data="order:edit:spread",
                )
            ]
        )
    if ready:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔎 Проверить и запустить",
                    callback_data="order:launch",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="💾 Сохранить на потом",
                    callback_data="order:save",
                )
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚠️ Показать, что осталось",
                    callback_data="order:check",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="✖️ Отменить создание", callback_data="order:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _has_broadcast_content(data: dict[str, object]) -> bool:
    """A media-only post is valid too (notably voice notes and video notes)."""

    return bool(
        str(data.get("primary_text") or "").strip()
        or str(data.get("primary_media_path") or "").strip()
    )


def _draft_media_label(data: dict[str, object]) -> str:
    if not data.get("primary_media_path"):
        return ""
    return {
        "photo": "фото",
        "video": "видео",
        "document": "файл",
        "voice": "голосовое",
        "video_note": "кружок",
    }.get(str(data.get("primary_media_kind") or ""), "медиа")


def order_advanced_menu(data: dict[str, object]) -> InlineKeyboardMarkup:
    """Fine-tuning controls kept outside the short, task-focused draft card."""

    default_profile = BROADCAST_PROFILES["active"]
    visibility_depth = int(
        data.get("min_messages_between_posts", default_profile.min_messages_between_posts)
    )
    visibility_max_wait_minutes = (
        int(data.get("visibility_max_wait_seconds", default_profile.visibility_max_wait_seconds))
        // 60
    )
    check_seconds = int(
        data.get("activity_check_interval_seconds", default_profile.activity_check_interval_seconds)
    )
    check_label = f"{check_seconds // 60} мин" if check_seconds >= 60 else f"{check_seconds} сек"
    no_timer_interval_minutes = int(
        data.get("smart_no_timer_interval_minutes", default_profile.interval_minutes)
    )
    rows = [
        [
            InlineKeyboardButton(
                text=f"👁 До повтора · {visibility_depth} сообщ.",
                callback_data="order:edit:visibility",
            ),
            InlineKeyboardButton(
                text=f"⌛ Ожидание · {visibility_max_wait_minutes} мин",
                callback_data="order:edit:maxwait",
            ),
        ],
        [
            InlineKeyboardButton(
                text=(
                    "🧠 Подстраиваться · ВКЛ"
                    if bool(data.get("activity_aware", True))
                    else "🧠 Подстраиваться · ВЫКЛ"
                ),
                callback_data="order:activity:toggle",
            ),
            InlineKeyboardButton(
                text=f"🔎 Проверять · {check_label}",
                callback_data="order:edit:check",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"⏱ Без таймера · {no_timer_interval_minutes} мин",
                callback_data="order:edit:interval",
            ),
            InlineKeyboardButton(
                text=f"🗓 Срок · {int(data.get('smart_duration_hours', 24))} ч",
                callback_data="order:edit:duration",
            ),
        ],
    ]
    if (
        data.get("order_mode") == "broadcast"
        and str(data.get("sender_mode") or "distributed") != "independent"
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "🛟 Автозамена аккаунта · ВКЛ"
                        if bool(data.get("auto_sender_failover", True))
                        else "🛟 Автозамена аккаунта · ВЫКЛ"
                    ),
                    callback_data="order:failover:toggle",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К черновику", callback_data="order:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_launch_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔎 Проверить и начать",
                    callback_data="order:launch:confirm",
                )
            ],
            [InlineKeyboardButton(text="⬅️ Вернуться к настройке", callback_data="order:back")],
        ]
    )


def order_interval_presets() -> InlineKeyboardMarkup:
    return _order_number_presets(
        "interval",
        [(3, "3 мин"), (5, "5 мин ✓"), (10, "10 мин"), (20, "20 мин"), (60, "1 час")],
    )


def order_duration_presets() -> InlineKeyboardMarkup:
    return _order_number_presets(
        "duration",
        [
            (6, "6 ч"),
            (12, "12 ч"),
            (24, "24 ч ✓"),
            (48, "2 суток"),
            (72, "3 суток"),
            (168, "7 суток"),
        ],
    )


def order_spread_presets() -> InlineKeyboardMarkup:
    return _order_number_presets(
        "spread",
        [(0, "Сразу"), (1, "1 ч"), (3, "3 ч"), (6, "6 ч"), (12, "12 ч"), (24, "24 ч")],
    )


def _order_number_presets(
    kind: str,
    values: list[tuple[int, str]],
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for value, label in values:
        builder.button(text=label, callback_data=f"order:preset:{kind}:{value}")
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="⬅️ К рассылке", callback_data="order:back"))
    return builder.as_markup()


def order_visibility_presets() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for value in (5, 10, 20, 50):
        builder.button(text=f"{value} сообщений", callback_data=f"order:preset:visibility:{value}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="⬅️ К рассылке", callback_data="order:back"))
    return builder.as_markup()


def order_activity_check_presets() -> InlineKeyboardMarkup:
    return _order_number_presets(
        "check",
        [(30, "30 сек"), (60, "60 сек ✓"), (120, "2 мин"), (300, "5 мин")],
    )


def order_visibility_wait_presets() -> InlineKeyboardMarkup:
    return _order_number_presets(
        "maxwait",
        [(5, "5 мин"), (10, "10 мин"), (15, "15 мин ✓"), (30, "30 мин"), (60, "1 час")],
    )


def order_sender_picker(
    accounts: list[Account],
    selected_ids: set[int],
    *,
    page: int = 0,
    page_size: int = 7,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    pages = max(1, (len(accounts) + page_size - 1) // page_size)
    normalized_page = min(max(page, 0), pages - 1)
    start = normalized_page * page_size
    for account in accounts[start : start + page_size]:
        selected = account.id in selected_ids
        builder.button(
            text=f"{'✅' if selected else '▫️'} {account.label}",
            callback_data=f"order:sender:toggle:{account.id}",
        )
    builder.adjust(1)
    if pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if normalized_page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="‹",
                    callback_data=f"order:sender:page:{normalized_page - 1}",
                )
            )
        navigation.append(
            InlineKeyboardButton(
                text=f"{normalized_page + 1} / {pages}",
                callback_data="noop",
            )
        )
        if normalized_page + 1 < pages:
            navigation.append(
                InlineKeyboardButton(
                    text="›",
                    callback_data=f"order:sender:page:{normalized_page + 1}",
                )
            )
        builder.row(*navigation)
        jumps: list[InlineKeyboardButton] = []
        if normalized_page > 1:
            jumps.append(
                InlineKeyboardButton(text="⏮ В начало", callback_data="order:sender:page:0")
            )
        if normalized_page > 10:
            jumps.append(
                InlineKeyboardButton(
                    text="−10",
                    callback_data=f"order:sender:page:{max(0, normalized_page - 10)}",
                )
            )
        if normalized_page + 10 < pages - 1:
            jumps.append(
                InlineKeyboardButton(
                    text="+10",
                    callback_data=(f"order:sender:page:{min(pages - 1, normalized_page + 10)}"),
                )
            )
        if normalized_page < pages - 2:
            jumps.append(
                InlineKeyboardButton(
                    text="В конец ⏭",
                    callback_data=f"order:sender:page:{pages - 1}",
                )
            )
        if jumps:
            builder.row(*jumps)
    builder.row(
        InlineKeyboardButton(text="Выбрать все", callback_data="order:sender:all"),
        InlineKeyboardButton(text="Очистить", callback_data="order:sender:clear"),
    )
    builder.row(InlineKeyboardButton(text="✅ Готово", callback_data="order:sender:done"))
    builder.row(InlineKeyboardButton(text="⬅️ К рассылке", callback_data="order:back"))
    return builder.as_markup()


def order_back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ К рассылке", callback_data="order:back")]]
    )


def order_audience_source_menu(
    *,
    standalone_count: int,
    list_count: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if list_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📚 Списки чатов · {list_count}",
                    callback_data="order:audience:lists:0",
                )
            ]
        )
    if standalone_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📌 Отдельные чаты · {standalone_count}",
                    callback_data="order:audience:chats:0",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К рассылке", callback_data="order:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_target_picker(page: Page[Target]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target in page.items:
        builder.button(
            text=f"📌 {_compact_button_label(target.title, 54)}",
            callback_data=f"order:audience:target:{target.id}",
        )
    builder.adjust(1)
    _order_audience_pagination(builder, "chats", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти чат",
            callback_data="order:audience:search:chats",
        )
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ К выбору аудитории", callback_data="order:edit:audience")
    )
    return builder.as_markup()


def order_target_list_picker(page: Page[TargetListView]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for item in page.items:
        builder.button(
            text=f"📚 {_compact_button_label(item.name, 38)} · {item.approved_count} чатов",
            callback_data=f"order:audience:list:{item.id}",
        )
    builder.adjust(1)
    _order_audience_pagination(builder, "lists", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти список",
            callback_data="order:audience:search:lists",
        )
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ К выбору аудитории", callback_data="order:edit:audience")
    )
    return builder.as_markup()


def order_audience_search_menu(
    kind: str,
    results: list[tuple[str, str]] | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for label, callback_data in results or []:
        compact = label if len(label) <= 60 else f"{label[:57]}…"
        builder.row(InlineKeyboardButton(text=compact, callback_data=callback_data))
    if results is not None:
        builder.row(
            InlineKeyboardButton(
                text="🔎 Искать ещё",
                callback_data=f"order:audience:search:{kind}",
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ Вернуться к списку",
            callback_data=f"order:audience:{kind}:0",
        )
    )
    return builder.as_markup()


def _order_audience_pagination(
    builder: InlineKeyboardBuilder,
    kind: str,
    page: int,
    pages: int,
) -> None:
    if pages <= 1:
        return
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(
                text="‹",
                callback_data=f"order:audience:{kind}:{page - 1}",
            )
        )
    navigation.append(InlineKeyboardButton(text=f"{page + 1} / {pages}", callback_data="noop"))
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(
                text="›",
                callback_data=f"order:audience:{kind}:{page + 1}",
            )
        )
    builder.row(*navigation)
    jumps: list[InlineKeyboardButton] = []
    if page > 1:
        jumps.append(
            InlineKeyboardButton(text="⏮ В начало", callback_data=f"order:audience:{kind}:0")
        )
    if page > 10:
        jumps.append(
            InlineKeyboardButton(
                text="−10",
                callback_data=f"order:audience:{kind}:{max(0, page - 10)}",
            )
        )
    if page + 10 < pages - 1:
        jumps.append(
            InlineKeyboardButton(
                text="+10",
                callback_data=f"order:audience:{kind}:{min(pages - 1, page + 10)}",
            )
        )
    if page < pages - 2:
        jumps.append(
            InlineKeyboardButton(
                text="В конец ⏭",
                callback_data=f"order:audience:{kind}:{pages - 1}",
            )
        )
    if jumps:
        builder.row(*jumps)


def audiences_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📚 Списки чатов",
                    callback_data="menu:targetlists:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📌 Отдельные чаты",
                    callback_data="menu:targets:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Исключённые чаты",
                    callback_data="target:bad:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📥 Импортировать список",
                    callback_data="target:import",
                ),
                InlineKeyboardButton(
                    text="➕ Добавить чат",
                    callback_data="target:add",
                ),
            ],
            [InlineKeyboardButton(text="🔄 Обновить состояние", callback_data="audiences:home")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def proxies_menu(page: ProxyPage) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    filter_labels = [
        ("all", "Все"),
        ("healthy", "🟢 Рабочие"),
        ("problem", "🔴 Ошибки"),
        ("unknown", "⚪️ Новые"),
        ("free", "Без аккаунтов"),
    ]
    builder.row(
        *[
            InlineKeyboardButton(
                text=("✓ " if page.filter_name.value == value else "") + label,
                callback_data=f"proxy:list:{value}:0",
            )
            for value, label in filter_labels[:3]
        ]
    )
    builder.row(
        *[
            InlineKeyboardButton(
                text=("✓ " if page.filter_name.value == value else "") + label,
                callback_data=f"proxy:list:{value}:0",
            )
            for value, label in filter_labels[3:]
        ]
    )
    icons = {
        "unknown": "⚪️",
        "healthy": "🟢",
        "unhealthy": "🔴",
        "quarantined": "🟠",
        "disabled": "⏸",
    }
    for item in page.items:
        proxy = item.proxy
        tail = f" · {item.assignment_count} акк." if item.assignment_count else ""
        builder.button(
            text=f"{icons[proxy.status.value]} #{proxy.id} {proxy.label}{tail}",
            callback_data=f"proxy:details:{proxy.id}:{page.page}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="➕ Добавить прокси", callback_data="proxy:add"),
        InlineKeyboardButton(text="🩺 Проверить все", callback_data=f"proxy:checkall:{page.page}"),
    )
    builder.row(
        InlineKeyboardButton(text="📄 Импорт TXT", callback_data="proxy:import"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"menu:proxies:{page.page}"),
    )
    _proxy_pagination(builder, page)
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def proxy_details_menu(item: ProxyView, page: int) -> InlineKeyboardMarkup:
    proxy = item.proxy
    rows = [
        [
            InlineKeyboardButton(
                text="🩺 Проверить", callback_data=f"proxy:check:{proxy.id}:{page}"
            ),
            InlineKeyboardButton(
                text="⏸ Выключить" if proxy.enabled else "▶️ Включить",
                callback_data=f"proxy:toggle:{proxy.id}:{page}",
            ),
        ]
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text="🔑 Адрес и пароль",
                callback_data=f"proxy:edit:{proxy.id}:{page}",
            ),
            InlineKeyboardButton(
                text="🏷 Название",
                callback_data=f"proxy:meta:{proxy.id}:{page}",
            ),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="🔁 Сменить IP у всех" if item.assignment_count > 1 else "🔁 Сменить IP",
                callback_data=f"proxy:rotate:{proxy.id}:{page}",
            ),
            InlineKeyboardButton(
                text="🔗 Ссылка ротации",
                callback_data=f"proxy:rotation:{proxy.id}:{page}",
            ),
        ]
    )
    if item.assignment_key:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔓 Снять со всех аккаунтов",
                    callback_data=f"proxy:release:{proxy.id}:{page}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🗑 Удалить", callback_data=f"proxy:delete:ask:{proxy.id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К прокси",
                    callback_data=f"menu:proxies:{page}",
                )
            ],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_variants_menu(
    campaign: Campaign,
    variants: list[BroadcastContentVariant],
    page: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for index, variant in enumerate(variants, start=1):
        content_label = (
            " · 📇 контакт"
            if variant.contact is not None
            else f" · {len(variant.media)} медиа"
            if variant.media
            else ""
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"{'🟢' if variant.enabled else '⚪️'} {index}. "
                        f"{variant.name}{content_label}"
                    ),
                    callback_data=(
                        f"campaign:variant:view:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                )
            ]
        )
    if len(variants) < MAX_CONTENT_VARIANTS:
        rows.append(
            [
                InlineKeyboardButton(
                    text="➕ Текст / медиа",
                    callback_data=f"campaign:variant:add:{campaign.id}:{page}",
                ),
                InlineKeyboardButton(
                    text="📇 Контакт",
                    callback_data=f"campaign:variant:contact:add:{campaign.id}:{page}",
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К рассылке",
                callback_data=f"campaign:details:{campaign.id}:{page}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_variant_menu(
    campaign: Campaign,
    variant: BroadcastContentVariant,
    *,
    variant_count: int,
    page: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="👁 Отправить предпросмотр",
                callback_data=(f"campaign:preview:{variant.variant_id}:{campaign.id}:{page}"),
            )
        ],
    ]
    if variant.contact is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="✏️ Изменить контакт",
                    callback_data=(
                        f"campaign:variant:contact:edit:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                ),
                InlineKeyboardButton(
                    text="🏷 Название",
                    callback_data=(
                        f"campaign:variant:rename:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                ),
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="✏️ Изменить текст",
                    callback_data=(
                        f"campaign:variant:edit:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                ),
                InlineKeyboardButton(
                    text="🏷 Название",
                    callback_data=(
                        f"campaign:variant:rename:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=("⏸ Выключить вариант" if variant.enabled else "▶️ Включить вариант"),
                callback_data=(
                    f"campaign:variant:toggle:{campaign.id}:{variant.variant_id}:{page}"
                ),
            )
        ]
    )
    if variant.contact is None and len(variant.media) < MAX_ALBUM_ITEMS:
        rows.append(
            [
                InlineKeyboardButton(
                    text="➕ Добавить медиа в альбом",
                    callback_data=(
                        f"campaign:variant:media:add:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                )
            ]
        )
    for index, item in enumerate(variant.media):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"✖️ Убрать {index + 1}. {item.name[:32]}",
                    callback_data=(
                        "campaign:variant:media:remove:"
                        f"{campaign.id}:{variant.variant_id}:{index}:{page}"
                    ),
                )
            ]
        )
    if variant_count > 1:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🗑 Удалить вариант",
                    callback_data=(
                        f"campaign:variant:delete:ask:{campaign.id}:{variant.variant_id}:{page}"
                    ),
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Ко всем вариантам",
                callback_data=f"campaign:variants:{campaign.id}:{page}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_variant_delete_confirmation(
    campaign_id: int,
    variant_id: str,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить вариант",
                    callback_data=(
                        f"campaign:variant:delete:confirm:{campaign_id}:{variant_id}:{page}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="✖️ Отмена",
                    callback_data=f"campaign:variant:view:{campaign_id}:{variant_id}:{page}",
                )
            ],
        ]
    )


def campaign_variant_input_cancel(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✖️ Отмена",
                    callback_data=f"campaign:variants:{campaign_id}:{page}",
                )
            ]
        ]
    )


def proxy_delete_confirmation(proxy_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить",
                    callback_data=f"proxy:delete:confirm:{proxy_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить", callback_data=f"proxy:details:{proxy_id}:{page}"
                )
            ],
        ]
    )


def proxy_groups_menu(groups: list[ProxyGroup]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for group in groups:
        builder.row(
            InlineKeyboardButton(
                text=f"🗂 {group.name}",
                callback_data=f"proxy:groupfilter:{group.id}:0",
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"proxy:group:delete:ask:{group.id}"),
        )
    builder.row(InlineKeyboardButton(text="➕ Создать группу", callback_data="proxy:group:add"))
    builder.row(InlineKeyboardButton(text="⬅️ К прокси", callback_data="menu:proxies:0"))
    return builder.as_markup()


def accounts_menu(page: Page[Account]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in page.items:
        icon = _account_icon(account)
        builder.button(
            text=f"{icon} {account.label}",
            callback_data=f"account:details:{account.id}:{page.page}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(
            text="🎲 Имена для аккаунтов",
            callback_data="account:names:menu",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📝 Описания профилей",
            callback_data="account:about:bulk",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📥 Импорт TData (.zip)",
            callback_data="account:import:tdata",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📦 Архив аккаунтов (.zip)",
            callback_data="account:import:archive",
        )
    )
    builder.row(
        InlineKeyboardButton(text="🗂 .session Telethon", callback_data="account:import:telethon"),
        InlineKeyboardButton(text="🗂 .session Pyrogram", callback_data="account:import:pyrogram"),
    )
    builder.row(
        InlineKeyboardButton(text="📄 Импорт .json", callback_data="account:import:json"),
        InlineKeyboardButton(text="🔑 Строка сессии", callback_data="account:import:string"),
    )
    builder.row(InlineKeyboardButton(text="➕ Аккаунт без сессии", callback_data="account:add"))
    _pagination(builder, "accounts", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"menu:accounts:{page.page}")
    )
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def account_import_result_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📦 Импортировать ещё архив",
                    callback_data="account:import:archive",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts:0")],
        ]
    )


def _account_source_value(account: Account) -> str:
    source = getattr(account, "session_source", None)
    return source.value if source is not None else "none"


def account_details_menu(
    account: Account,
    page: int,
    *,
    has_proxy: bool,
    shared_proxy_accounts: int = 0,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    has_session = _account_source_value(account) != "none"
    builder.row(
        InlineKeyboardButton(
            text="🏷 Название в боте",
            callback_data=f"account:label:edit:{account.id}:{page}",
        )
    )
    if has_session:
        builder.row(
            InlineKeyboardButton(
                text="🔍 Проверить сессию",
                callback_data=f"account:check:{account.id}:{page}",
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="✏️ Имя Telegram",
                callback_data=f"account:name:edit:{account.id}:{page}",
            ),
            InlineKeyboardButton(
                text="🎲 Имя Telegram из каталога",
                callback_data=f"account:name:catalog:{account.id}:{page}",
            ),
        )
        builder.row(
            InlineKeyboardButton(
                text="🖼 Аватар Telegram",
                callback_data=f"account:avatar:menu:{account.id}:{page}",
            ),
            InlineKeyboardButton(
                text="📝 Описание",
                callback_data=f"account:about:edit:{account.id}:{page}",
            ),
        )
    builder.row(
        InlineKeyboardButton(
            text="⏸ Пауза" if account.status.value == "active" else "▶️ Включить",
            callback_data=f"account:toggle:{account.id}:{page}",
        ),
        InlineKeyboardButton(
            text="🔓 Убрать прокси" if has_proxy else "🌐 Назначить прокси",
            callback_data=(
                f"account:proxy:release:{account.id}:{page}"
                if has_proxy
                else f"account:proxy:auto:{account.id}:{page}"
            ),
        ),
    )
    if has_proxy:
        builder.row(
            InlineKeyboardButton(
                text=(
                    f"🔁 Сменить IP у всех ({shared_proxy_accounts})"
                    if shared_proxy_accounts > 1
                    else "🔁 Сменить IP"
                ),
                callback_data=f"account:proxy:rotate:{account.id}:{page}",
            )
        )
    if has_session:
        builder.row(
            InlineKeyboardButton(
                text="🔐 Стереть сессию и доступ",
                callback_data=f"account:session:ask:{account.id}:{page}",
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="🗑 Удалить запись", callback_data=f"account:delete:ask:{account.id}:{page}"
        )
    )
    builder.row(InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data=f"menu:accounts:{page}"))
    return builder.as_markup()


def account_delete_confirmation(account_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить и стереть сессию",
                    callback_data=f"account:delete:confirm:{account_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"account:details:{account_id}:{page}",
                )
            ],
        ]
    )


def account_session_clear_confirmation(account_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔐 Да, стереть доступ",
                    callback_data=f"account:session:confirm:{account_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"account:details:{account_id}:{page}",
                )
            ],
        ]
    )


def account_name_confirmation(account_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Изменить имя Telegram",
                    callback_data=f"account:name:confirm:{account_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def account_about_confirmation(account_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Сохранить описание",
                    callback_data=f"account:about:confirm:{account_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def account_about_bulk_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✍️ Установить всем", callback_data="account:about:bulk:set"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🧹 Очистить все", callback_data="account:about:bulk:clear:ask"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="menu:accounts:0")],
        ]
    )


def account_about_bulk_confirmation(*, clear: bool = False) -> InlineKeyboardMarkup:
    label = "🧹 Да, очистить у всех" if clear else "✅ Установить всем"
    action = "clear" if clear else "apply"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=label, callback_data=f"account:about:bulk:{action}:confirm"
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="account:about:bulk")],
        ]
    )


def account_name_catalog_menu(
    *,
    account_id: int = 0,
    page: int = 0,
) -> InlineKeyboardMarkup:
    suffix = f":{account_id}:{page}"
    back = f"account:details:{account_id}:{page}" if account_id else f"menu:accounts:{page}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🎲 Смешанные · рекомендуется",
                    callback_data=f"account:names:preview:mixed{suffix}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🇷🇺 Русские",
                    callback_data=f"account:names:preview:ru{suffix}",
                ),
                InlineKeyboardButton(
                    text="🌍 Латиницей",
                    callback_data=f"account:names:preview:en{suffix}",
                ),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back)],
        ]
    )


def account_generated_names_confirmation(
    *,
    locale: str,
    account_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Применить в Telegram",
                    callback_data="account:names:apply",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎲 Другие варианты",
                    callback_data=(f"account:names:preview:{locale}:{account_id}:{page}"),
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def account_avatar_menu(
    account_id: int,
    page: int,
    *,
    has_avatar: bool,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="📤 Загрузить новый аватар",
                callback_data=f"account:avatar:upload:{account_id}:{page}",
            )
        ]
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text=("🗑 Удалить все аватары" if has_avatar else "🧹 Очистить аватары в Telegram"),
                callback_data=f"account:avatar:delete:ask:{account_id}:{page}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К аккаунту",
                callback_data=f"account:details:{account_id}:{page}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def account_avatar_delete_confirmation(account_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить все аватары",
                    callback_data=f"account:avatar:delete:confirm:{account_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"account:avatar:menu:{account_id}:{page}",
                )
            ],
        ]
    )


def _account_icon(account: Account) -> str:
    if account.status.value != "active":
        return "⏸"
    if _account_source_value(account) == "none":
        return "📄"
    return "🟢" if getattr(account, "authorized", False) else "🟡"


def proxy_group_delete_confirmation(group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Удалить только группу",
                    callback_data=f"proxy:group:delete:confirm:{group_id}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="proxy:groups")],
        ]
    )


def targets_menu(page: Page[Target]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target in page.items:
        icon = (
            "🚫"
            if target.quality_status == "bad"
            else "🔴"
            if target.last_error_code
            else "🟢"
            if target.approved_by_operator
            else "⚪️"
        )
        builder.button(
            text=f"{icon} {_compact_button_label(target.title, 54)}",
            callback_data=f"target:details:{target.id}:{page.page}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="➕ Добавить чат", callback_data="target:add"),
        InlineKeyboardButton(text="🔎 Найти", callback_data="search:start:targets"),
    )
    if page.total:
        builder.row(
            InlineKeyboardButton(
                text="▶️ Включить все доступные",
                callback_data=f"targets:bulk:approve:{page.page}",
            ),
            InlineKeyboardButton(
                text="⏸ Выключить все",
                callback_data=f"targets:bulk:revoke:{page.page}",
            ),
        )
    _pagination(builder, "targets", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"menu:targets:{page.page}")
    )
    builder.row(InlineKeyboardButton(text="⬅️ К чатам и спискам", callback_data="audiences:home"))
    return builder.as_markup()


def target_details_menu(target: Target, page: int) -> InlineKeyboardMarkup:
    action = "revoke" if target.approved_by_operator else "approve"
    state_row = (
        [
            InlineKeyboardButton(
                text="♻️ Вернуть чат в работу",
                callback_data=f"target:quality:restore:{target.id}:{page}:targets",
            )
        ]
        if target.quality_status == "bad"
        else [
            InlineKeyboardButton(
                text=(
                    "⏸ Не использовать чат" if target.approved_by_operator else "▶️ Использовать чат"
                ),
                callback_data=f"target:{action}:{target.id}:{page}",
            )
        ]
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🕒 Интервал сообщений",
                    callback_data=f"target:cooldown:{target.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="♻️ Сбросить память банов",
                    callback_data=f"target:unblock:{target.id}:{page}",
                )
            ],
            state_row,
            [
                InlineKeyboardButton(
                    text="🗑 Удалить чат",
                    callback_data=f"target:delete:ask:{target.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К отдельным чатам", callback_data=f"menu:targets:{page}"
                )
            ],
        ]
    )


def bad_targets_menu(page: Page[Target]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target in page.items:
        builder.button(
            text=f"🚫 {target.title} · {target.quality_score}/100",
            callback_data=f"target:bad:details:{target.id}:{page.page}",
        )
    builder.adjust(1)
    _pagination(builder, "badtargets", page.page, page.pages)
    builder.row(InlineKeyboardButton(text="⬅️ К чатам и спискам", callback_data="audiences:home"))
    return builder.as_markup()


def bad_target_details_menu(target: Target, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="♻️ Вернуть чат в работу",
                    callback_data=f"target:quality:restore:{target.id}:{page}:bad",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К исключённым чатам",
                    callback_data=f"target:bad:{page}",
                )
            ],
        ]
    )


def target_cooldown_picker() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for minutes, label in [
        (0, "Только Telegram"),
        (30, "30 мин"),
        (60, "1 ч"),
        (120, "2 ч"),
        (360, "6 ч"),
        (1440, "24 ч"),
    ]:
        builder.button(
            text=label,
            callback_data=f"tcooldown:set:{minutes}",
        )
    builder.adjust(1, 2, 3)
    builder.row(InlineKeyboardButton(text="✖️ Отменить изменения", callback_data="tcooldown:cancel"))
    return builder.as_markup()


def target_delete_confirmation(target_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить чат",
                    callback_data=f"target:delete:confirm:{target_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить", callback_data=f"target:details:{target_id}:{page}"
                )
            ],
        ]
    )


def campaigns_menu(page: Page[Campaign]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for campaign in page.items:
        icon = "📢" if getattr(campaign.mode, "value", "dialogue") == "broadcast" else "💬"
        status = "" if campaign.enabled else " · ⏸"
        builder.button(
            text=f"{icon} {campaign.name}{status}",
            callback_data=f"campaign:details:{campaign.id}:{page.page}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="🚀 Новая рассылка", callback_data="order:draft:broadcast"),
        InlineKeyboardButton(text="💬 Новый диалог", callback_data="order:draft:dialogue"),
    )
    builder.row(
        InlineKeyboardButton(text="🔎 Найти рассылку", callback_data="search:start:campaigns")
    )
    builder.row(
        InlineKeyboardButton(text="📊 Запуски и результаты", callback_data="menu:runs:0"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"menu:campaigns:{page.page}"),
    )
    _pagination(builder, "campaigns", page.page, page.pages)
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def campaign_details_menu(campaign: Campaign, page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if campaign.enabled:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🚀 Запустить",
                    callback_data=f"campaign:run:ask:{campaign.id}:{page}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="🩺 Проверить перед отправкой",
                callback_data=f"campaign:preflight:{campaign.id}:{page}",
            )
        ]
    )
    is_broadcast = getattr(campaign.mode, "value", campaign.mode) == "broadcast"
    selected_profile = (
        detect_broadcast_profile(
            duration_seconds=campaign.smart_duration_seconds,
            interval_seconds=campaign.smart_no_timer_interval_seconds,
            min_messages_between_posts=campaign.min_messages_between_posts,
            activity_check_interval_seconds=campaign.activity_check_interval_seconds,
            visibility_max_wait_seconds=campaign.visibility_max_wait_seconds,
        )
        if campaign.smart_timer_mode and campaign.activity_aware
        else None
    )
    selected_profile_key = selected_profile.key if selected_profile is not None else None
    if is_broadcast:
        independent = getattr(campaign.sender_mode, "value", campaign.sender_mode) == "independent"
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "🚀 Аккаунты работают независимо"
                        if independent
                        else "🤖 Умный выбор отправителя"
                    ),
                    callback_data=f"campaign:sendermode:toggle:{campaign.id}:{page}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="👥 Изменить отправителей",
                    callback_data=f"campaign:edit:senders:{campaign.id}:{page}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="🎨 Варианты постов и медиа",
                    callback_data=f"campaign:variants:{campaign.id}:{page}",
                ),
                InlineKeyboardButton(
                    text="👁 Посмотреть пост",
                    callback_data=f"campaign:preview:primary:{campaign.id}:{page}",
                ),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        "🔔 Отметки участников · ВКЛ"
                        if campaign.hidden_mentions_enabled
                        else "🔔 Отметки участников · ВЫКЛ"
                    ),
                    callback_data=f"campaign:mentions:toggle:{campaign.id}:{page}",
                ),
                InlineKeyboardButton(
                    text=f"🔄 {campaign.mention_cooldown_days} дн.",
                    callback_data=f"campaign:mentions:cooldown:{campaign.id}:{page}",
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=("✏️ Основное сообщение" if is_broadcast else "🧩 Изменить цепочку"),
                callback_data=f"campaign:edit:content:{campaign.id}:{page}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="🎯 Изменить чаты",
                callback_data=f"campaign:edit:target:{campaign.id}:{page}",
            )
        ]
    )
    if campaign.smart_timer_mode:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{'✓ ' if selected_profile_key == 'active' else ''}⚡ Активная",
                    callback_data=f"campaign:profile:active:{campaign.id}:{page}",
                ),
                InlineKeyboardButton(
                    text=f"{'✓ ' if selected_profile_key == 'balanced' else ''}⚖️ Средняя",
                    callback_data=f"campaign:profile:balanced:{campaign.id}:{page}",
                ),
                InlineKeyboardButton(
                    text=f"{'✓ ' if selected_profile_key == 'gentle' else ''}🌙 Бережная",
                    callback_data=f"campaign:profile:gentle:{campaign.id}:{page}",
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=(
                    "👍 Реакции на публикации · ВКЛ"
                    if campaign.reactions_enabled
                    else "👍 Реакции на публикации · ВЫКЛ"
                ),
                callback_data=f"campaign:reactions:{campaign.id}:{page}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="🔁 Изменить режим отправки",
                callback_data=f"campaign:edit:schedule:{campaign.id}:{page}",
            )
        ]
    )
    if campaign.smart_timer_mode:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚙️ Точные настройки",
                    callback_data=f"campaign:advanced:{campaign.id}:{page}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⏸ Остановить" if campaign.enabled else "▶️ Включить",
                callback_data=f"campaign:toggle:{campaign.id}:{page}",
            ),
            InlineKeyboardButton(
                text="🗑 Удалить",
                callback_data=f"campaign:delete:ask:{campaign.id}:{page}",
            ),
        ]
    )
    rows.append(
        [InlineKeyboardButton(text="⬅️ К рассылкам", callback_data=f"menu:campaigns:{page}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_mention_cooldown_menu(
    campaign_id: int,
    *,
    page: int,
    current_days: int,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for values in ((1, 3, 7), (14, 30, 60), (90, 180, 365)):
        builder.row(
            *(
                InlineKeyboardButton(
                    text=f"{'✓ ' if days == current_days else ''}{days} дн.",
                    callback_data=f"campaign:mentions:days:{campaign_id}:{page}:{days}",
                )
                for days in values
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="✍️ Другое значение",
            callback_data=f"campaign:mentions:custom:{campaign_id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К рассылке",
            callback_data=f"campaign:details:{campaign_id}:{page}",
        )
    )
    return builder.as_markup()


def campaign_advanced_menu(campaign: Campaign, page: int) -> InlineKeyboardMarkup:
    default_profile = BROADCAST_PROFILES["active"]
    check_seconds = (
        campaign.activity_check_interval_seconds or default_profile.activity_check_interval_seconds
    )
    interval_seconds = (
        campaign.smart_no_timer_interval_seconds or default_profile.interval_minutes * 60
    )
    duration_seconds = campaign.smart_duration_seconds or default_profile.duration_hours * 3600
    depth = campaign.min_messages_between_posts or default_profile.min_messages_between_posts
    max_wait_seconds = (
        campaign.visibility_max_wait_seconds or default_profile.visibility_max_wait_seconds
    )
    check_label = f"{check_seconds // 60} мин" if check_seconds >= 60 else f"{check_seconds} сек"
    rows = [
        [
            InlineKeyboardButton(
                text=f"⏱ Интервал · {interval_seconds // 60} мин",
                callback_data=f"campaign:advanced:edit:interval:{campaign.id}:{page}",
            ),
            InlineKeyboardButton(
                text=f"🗓 Срок · {duration_seconds // 3600} ч",
                callback_data=f"campaign:advanced:edit:duration:{campaign.id}:{page}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"👁 Глубина · {depth}",
                callback_data=f"campaign:advanced:edit:depth:{campaign.id}:{page}",
            ),
            InlineKeyboardButton(
                text=f"⌛ Лимит · {max_wait_seconds // 60} мин",
                callback_data=f"campaign:advanced:edit:maxwait:{campaign.id}:{page}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=f"🔎 Проверка · {check_label}",
                callback_data=f"campaign:advanced:edit:check:{campaign.id}:{page}",
            )
        ],
        [
            InlineKeyboardButton(
                text=(
                    "🧠 Адаптивность · ВКЛ" if campaign.activity_aware else "🧠 Адаптивность · ВЫКЛ"
                ),
                callback_data=f"campaign:advanced:toggle:activity:{campaign.id}:{page}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="⬅️ К рассылке",
                callback_data=f"campaign:details:{campaign.id}:{page}",
            )
        ],
    ]
    if getattr(campaign.mode, "value", campaign.mode) == "broadcast":
        rows[3].append(
            InlineKeyboardButton(
                text=(
                    "🚀 Потоки независимы"
                    if getattr(campaign.sender_mode, "value", campaign.sender_mode) == "independent"
                    else "🛟 Автозамена · ВКЛ"
                    if campaign.auto_sender_failover
                    else "🛟 Автозамена · ВЫКЛ"
                ),
                callback_data=(
                    "noop"
                    if getattr(campaign.sender_mode, "value", campaign.sender_mode) == "independent"
                    else f"campaign:advanced:toggle:failover:{campaign.id}:{page}"
                ),
            )
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_reactions_menu(campaign: Campaign, page: int, step_count: int) -> InlineKeyboardMarkup:
    enabled_label = "🟢 Реакции включены" if campaign.reactions_enabled else "⚪ Реакции выключены"
    rows = [
        [
            InlineKeyboardButton(
                text=enabled_label,
                callback_data=f"campaign:reactions:toggle:state:{campaign.id}:{page}",
            )
        ]
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text=("✓ " if campaign.reaction_emoji == emoji else "") + emoji,
                callback_data=f"campaign:reactions:emoji:{key}:{campaign.id}:{page}",
            )
            for key, emoji in (("like", "👍"), ("fire", "🔥"), ("heart", "❤️"), ("bolt", "⚡"))
        ]
    )
    for counts in ((1, 2, 3, 4, 5), (6, 7, 8, 9, 10)):
        rows.append(
            [
                InlineKeyboardButton(
                    text=("✓ " if campaign.reaction_count == count else "") + str(count),
                    callback_data=f"campaign:reactions:count:{count}:{campaign.id}:{page}",
                )
                for count in counts
            ]
        )
    if campaign.mode == CampaignMode.DIALOGUE and step_count > 1:
        for first_step in range(1, step_count + 1, 4):
            rows.append(
                [
                    InlineKeyboardButton(
                        text=("✓ " if campaign.reaction_dialogue_step == step else "")
                        + f"Шаг {step}",
                        callback_data=(f"campaign:reactions:step:{step}:{campaign.id}:{page}"),
                    )
                    for step in range(first_step, min(first_step + 4, step_count + 1))
                ]
            )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К рассылке", callback_data=f"campaign:details:{campaign.id}:{page}"
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_edit_target_source_menu(
    *,
    standalone_count: int,
    list_count: int,
    campaign_id: int,
    campaign_page: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if list_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📚 Списки чатов · {list_count}",
                    callback_data=(f"campaign:edit:browse:lists:{campaign_id}:{campaign_page}:0"),
                )
            ]
        )
    if standalone_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📌 Отдельные чаты · {standalone_count}",
                    callback_data=(f"campaign:edit:browse:chats:{campaign_id}:{campaign_page}:0"),
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Не менять",
                callback_data=f"campaign:details:{campaign_id}:{campaign_page}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_edit_target_picker(
    page: Page[Target] | Page[TargetListView],
    *,
    kind: str,
    campaign_id: int,
    campaign_page: int,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if kind == "lists":
        for item in page.items:
            builder.button(
                text=f"📚 {item.name} ({item.approved_count})",  # type: ignore[union-attr]
                callback_data=f"cedit:list:{item.id}",
            )
    else:
        for item in page.items:
            builder.button(
                text=f"📌 {item.title}",  # type: ignore[union-attr]
                callback_data=f"cedit:target:{item.id}",
            )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти список" if kind == "lists" else "🔎 Найти чат",
            callback_data=(f"campaign:edit:search:{kind}:{campaign_id}:{campaign_page}"),
        )
    )
    if page.pages > 1:
        prefix = f"campaign:edit:browse:{kind}:{campaign_id}:{campaign_page}:"
        pagination: list[InlineKeyboardButton] = []
        if page.page > 1:
            pagination.append(InlineKeyboardButton(text="⏮", callback_data=f"{prefix}0"))
        if page.page > 10:
            pagination.append(
                InlineKeyboardButton(text="−10", callback_data=f"{prefix}{page.page - 10}")
            )
        if page.page > 0:
            pagination.append(
                InlineKeyboardButton(text="‹", callback_data=f"{prefix}{page.page - 1}")
            )
        pagination.append(
            InlineKeyboardButton(text=f"{page.page + 1}/{page.pages}", callback_data="noop")
        )
        if page.page + 1 < page.pages:
            pagination.append(
                InlineKeyboardButton(text="›", callback_data=f"{prefix}{page.page + 1}")
            )
        if page.page + 10 < page.pages - 1:
            pagination.append(
                InlineKeyboardButton(text="+10", callback_data=f"{prefix}{page.page + 10}")
            )
        if page.page < page.pages - 2:
            pagination.append(
                InlineKeyboardButton(text="⏭", callback_data=f"{prefix}{page.pages - 1}")
            )
        builder.row(*pagination)
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К выбору аудитории",
            callback_data=f"campaign:edit:target:{campaign_id}:{campaign_page}",
        )
    )
    return builder.as_markup()


def campaign_content_edit_cancel(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Не менять",
                    callback_data=f"campaign:content:cancel:{campaign_id}:{page}",
                )
            ]
        ]
    )


def campaign_delete_confirmation(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить черновик",
                    callback_data=f"campaign:delete:confirm:{campaign_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"campaign:details:{campaign_id}:{page}",
                )
            ],
        ]
    )


def target_lists_menu(page: Page[TargetListView]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for item in page.items:
        builder.button(
            text=(
                f"📚 {_compact_button_label(item.name, 38)} "
                f"({item.approved_count}/{item.item_count})"
            ),
            callback_data=f"targetlist:details:{item.id}:{page.page}",
        )
    builder.adjust(1)
    _pagination(builder, "targetlists", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(text="📄 Создать из TXT", callback_data="target:import"),
        InlineKeyboardButton(text="🔎 Найти", callback_data="search:start:targetlists"),
    )
    builder.row(
        InlineKeyboardButton(
            text="🔄 Обновить список",
            callback_data=f"menu:targetlists:{page.page}",
        )
    )
    builder.row(InlineKeyboardButton(text="⬅️ К чатам и спискам", callback_data="audiences:home"))
    return builder.as_markup()


def target_list_details_menu(
    target_list: TargetListView,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👁 Чаты этого списка",
                    callback_data=f"tlv:{target_list.id}:{page}:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Переименовать список",
                    callback_data=f"targetlist:rename:{target_list.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="▶️ Использовать все чаты",
                    callback_data=f"targetlist:approve:{target_list.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⏸ Не использовать все чаты",
                    callback_data=f"targetlist:revoke:{target_list.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить список",
                    callback_data=f"targetlist:delete:ask:{target_list.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К спискам чатов", callback_data=f"menu:targetlists:{page}"
                )
            ],
        ]
    )


def target_list_targets_menu(
    target_list_id: int,
    list_page: int,
    page: Page[Target],
    *,
    filter_name: str = "all",
    target_list: TargetListView | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    counts = {
        "all": target_list.item_count if target_list is not None else None,
        "enabled": target_list.approved_count if target_list is not None else None,
        "disabled": target_list.disabled_count if target_list is not None else None,
        "bad": target_list.bad_count if target_list is not None else None,
        "problem": target_list.problem_count if target_list is not None else None,
    }

    def filter_button(value: str, label: str) -> InlineKeyboardButton:
        count = counts[value]
        suffix = f" {count}" if count is not None else ""
        return InlineKeyboardButton(
            text=("✓ " if filter_name == value else "") + label + suffix,
            callback_data=f"tlf:{target_list_id}:{list_page}:{value}:0",
        )

    builder.row(
        filter_button("all", "Все"),
        filter_button("enabled", "🟢 Вкл."),
        filter_button("disabled", "⚪️ Выкл."),
    )
    builder.row(
        filter_button("bad", "🚫 Исключённые"),
        filter_button("problem", "🔴 Ошибки"),
    )
    if target_list is not None:
        all_chats_buttons: list[InlineKeyboardButton] = []
        if target_list.disabled_count:
            all_chats_buttons.append(
                InlineKeyboardButton(
                    text="▶️ Включить все доступные",
                    callback_data=(
                        f"tlall:approve:{target_list_id}:{list_page}:{filter_name}:{page.page}"
                    ),
                )
            )
        if target_list.approved_count:
            all_chats_buttons.append(
                InlineKeyboardButton(
                    text="⏸ Выключить все",
                    callback_data=(
                        f"tlall:revoke:{target_list_id}:{list_page}:{filter_name}:{page.page}"
                    ),
                )
            )
        if all_chats_buttons:
            builder.row(*all_chats_buttons)
    for target in page.items:
        icon = (
            "🚫"
            if target.quality_status == "bad"
            else "🔴"
            if target.last_error_code
            else "🟢"
            if target.approved_by_operator
            else "⚪️"
        )
        builder.row(
            InlineKeyboardButton(
                text=f"{icon} {_compact_button_label(target.title, 54)}",
                callback_data=(
                    f"tld:{target.id}:{target_list_id}:{list_page}:{page.page}:{filter_name}"
                ),
            )
        )
    if page.items:
        bulk_buttons: list[InlineKeyboardButton] = []
        if filter_name != "enabled":
            bulk_buttons.append(
                InlineKeyboardButton(
                    text="▶️ Включить страницу",
                    callback_data=(
                        f"tlbulk:approve:{target_list_id}:{list_page}:{filter_name}:{page.page}"
                    ),
                )
            )
        if filter_name not in {"disabled", "bad"}:
            bulk_buttons.append(
                InlineKeyboardButton(
                    text="⏸ Выключить страницу",
                    callback_data=(
                        f"tlbulk:revoke:{target_list_id}:{list_page}:{filter_name}:{page.page}"
                    ),
                )
            )
        if bulk_buttons:
            builder.row(*bulk_buttons)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти чат в базе",
            callback_data=f"tls:{target_list_id}:{list_page}",
        )
    )
    if page.pages > 1:
        pagination: list[InlineKeyboardButton] = []
        if page.page > 1:
            pagination.append(
                InlineKeyboardButton(
                    text="⏮",
                    callback_data=f"tlf:{target_list_id}:{list_page}:{filter_name}:0",
                )
            )
        if page.page > 0:
            pagination.append(
                InlineKeyboardButton(
                    text="‹",
                    callback_data=(
                        f"tlf:{target_list_id}:{list_page}:{filter_name}:{page.page - 1}"
                    ),
                )
            )
        pagination.append(
            InlineKeyboardButton(
                text=f"{page.page + 1} / {page.pages}",
                callback_data="noop",
            )
        )
        if page.page + 1 < page.pages:
            pagination.append(
                InlineKeyboardButton(
                    text="›",
                    callback_data=(
                        f"tlf:{target_list_id}:{list_page}:{filter_name}:{page.page + 1}"
                    ),
                )
            )
        if page.page < page.pages - 2:
            pagination.append(
                InlineKeyboardButton(
                    text="⏭",
                    callback_data=(
                        f"tlf:{target_list_id}:{list_page}:{filter_name}:{page.pages - 1}"
                    ),
                )
            )
        builder.row(*pagination)
        jumps: list[InlineKeyboardButton] = []
        if page.page > 10:
            jumps.append(
                InlineKeyboardButton(
                    text="−10 страниц",
                    callback_data=(
                        f"tlf:{target_list_id}:{list_page}:{filter_name}:{max(0, page.page - 10)}"
                    ),
                )
            )
        if page.page + 10 < page.pages - 1:
            jumps.append(
                InlineKeyboardButton(
                    text="+10 страниц",
                    callback_data=(
                        f"tlf:{target_list_id}:{list_page}:{filter_name}:"
                        f"{min(page.pages - 1, page.page + 10)}"
                    ),
                )
            )
        if jumps:
            builder.row(*jumps)
    builder.row(
        InlineKeyboardButton(
            text="⬅️ Карточка списка",
            callback_data=f"targetlist:details:{target_list_id}:{list_page}",
        )
    )
    return builder.as_markup()


def target_list_target_details_menu(
    target: Target,
    target_list_id: int,
    list_page: int,
    chat_page: int,
    filter_name: str = "all",
) -> InlineKeyboardMarkup:
    action = "revoke" if target.approved_by_operator else "approve"
    state_row = (
        [
            InlineKeyboardButton(
                text="🚫 Автоматически исключён",
                callback_data="noop",
            )
        ]
        if target.quality_status == "bad"
        else [
            InlineKeyboardButton(
                text=(
                    "⏸ Не использовать чат" if target.approved_by_operator else "▶️ Использовать чат"
                ),
                callback_data=(
                    f"tlt:{action}:{target.id}:{target_list_id}:{list_page}:{chat_page}"
                    f":{filter_name}"
                ),
            )
        ]
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🕒 Интервал сообщений",
                    callback_data=(
                        f"tlcd:{target.id}:{target_list_id}:{list_page}:{chat_page}:{filter_name}"
                    ),
                )
            ],
            state_row,
            [
                InlineKeyboardButton(
                    text="➖ Убрать из этого списка",
                    callback_data=(
                        f"tlra:{target.id}:{target_list_id}:{list_page}:{chat_page}:{filter_name}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К чатам списка",
                    callback_data=(f"tlf:{target_list_id}:{list_page}:{filter_name}:{chat_page}"),
                )
            ],
        ]
    )


def target_list_target_remove_confirmation(
    target_id: int,
    target_list_id: int,
    list_page: int,
    chat_page: int,
    filter_name: str = "all",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➖ Да, убрать из списка",
                    callback_data=(
                        f"tlrc:{target_id}:{target_list_id}:{list_page}:{chat_page}:{filter_name}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=(
                        f"tld:{target_id}:{target_list_id}:{list_page}:{chat_page}:{filter_name}"
                    ),
                )
            ],
        ]
    )


def target_list_delete_confirmation(target_list_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить список",
                    callback_data=f"targetlist:delete:confirm:{target_list_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"targetlist:details:{target_list_id}:{page}",
                )
            ],
        ]
    )


def templates_menu(page: Page[MessageTemplate]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for template in page.items:
        builder.row(
            InlineKeyboardButton(
                text=f"📝 {template.name}",
                callback_data=f"template:details:{template.id}:{page.page}",
            ),
            InlineKeyboardButton(
                text="⏸ Пауза" if template.enabled else "▶️ Включить",
                callback_data=f"template:toggle:{template.id}:{page.page}",
            ),
        )
    builder.row(InlineKeyboardButton(text="➕ Создать шаблон", callback_data="template:add"))
    _pagination(builder, "templates", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"menu:templates:{page.page}")
    )
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def template_details_menu(
    template: MessageTemplate,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить тексты",
                    callback_data=f"template:edit:{template.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⏸ Пауза" if template.enabled else "▶️ Включить",
                    callback_data=f"template:toggle:{template.id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить шаблон",
                    callback_data=f"template:delete:ask:{template.id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К шаблонам", callback_data=f"menu:templates:{page}")],
        ]
    )


def template_delete_confirmation(template_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить шаблон",
                    callback_data=f"template:delete:confirm:{template_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, оставить",
                    callback_data=f"template:details:{template_id}:{page}",
                )
            ],
        ]
    )


def launches_menu(
    page: Page[LaunchView],
    *,
    archived: bool = False,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for launch in page.items:
        icon = _launch_icon(launch)
        result = (
            f"отправлено {launch.sent_publications}"
            if launch.smart_timer_mode
            else f"{launch.finished}/{launch.total}"
        )
        builder.row(
            InlineKeyboardButton(
                text=(f"{icon} {launch.campaign_name} · {result}"),
                callback_data=f"launch:details:{launch.anchor_run_id}:{page.page}",
            )
        )
    section = "archive" if archived else "runs"
    _pagination(builder, section, page.page, page.pages)
    builder.row(
        InlineKeyboardButton(
            text="🔄 Обновить",
            callback_data=f"menu:{section}:{page.page}",
        ),
        InlineKeyboardButton(
            text="🔎 Найти рассылку",
            callback_data=f"search:start:{section}",
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text="📊 Работа и результаты" if archived else "🗄 Архив результатов",
            callback_data="menu:runs:0" if archived else "menu:archive:0",
        )
    )
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def search_prompt_menu(
    section: str,
    *,
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Вернуться к списку",
                    callback_data=back_callback or f"menu:{section}:0",
                )
            ]
        ]
    )


def search_results_menu(
    results: list[tuple[str, str]],
    *,
    section: str,
    new_search_callback: str | None = None,
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for label, callback_data in results:
        compact = label if len(label) <= 60 else f"{label[:57]}…"
        builder.row(InlineKeyboardButton(text=compact, callback_data=callback_data))
    builder.row(
        InlineKeyboardButton(
            text="🔎 Новый поиск",
            callback_data=new_search_callback or f"search:start:{section}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ Вернуться к списку",
            callback_data=back_callback or f"menu:{section}:0",
        )
    )
    return builder.as_markup()


def launch_details_menu(launch: LaunchView, page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="🔄 Обновить отчёт",
                callback_data=f"launch:details:{launch.anchor_run_id}:{page}",
            ),
            InlineKeyboardButton(
                text="📣 Настройки рассылки",
                callback_data=f"campaign:details:{launch.campaign_id}:0",
            ),
        ]
    ]
    if launch.archived:
        rows.append(
            [
                InlineKeyboardButton(
                    text="♻️ Вернуть в результаты",
                    callback_data=f"launch:restore:{launch.anchor_run_id}:{page}",
                )
            ]
        )
    elif launch.pending_work:
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"🛑 Остановить отправку ({launch.pending_work})"
                        if launch.active
                        else f"🛑 Остановить реакции ({launch.reaction_active})"
                    ),
                    callback_data=f"launch:cancel:ask:{launch.anchor_run_id}:{page}",
                )
            ]
        )
        if launch.active and launch.mode.value == "broadcast":
            rows.append(
                [
                    InlineKeyboardButton(
                        text="✏️ Изменить будущие посты",
                        callback_data=f"launch:edit:text:{launch.anchor_run_id}:{page}",
                    )
                ]
            )
        if launch.active and launch.smart_timer_mode:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="⏱ Частота постов",
                        callback_data=(f"launch:edit:interval:{launch.anchor_run_id}:{page}"),
                    ),
                    InlineKeyboardButton(
                        text="🗓 Срок работы",
                        callback_data=(f"launch:edit:duration:{launch.anchor_run_id}:{page}"),
                    ),
                ]
            )
    if launch.failed and not launch.archived:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🔁 Повторить неотправленные ({launch.failed})",
                    callback_data=f"launch:retry:ask:{launch.anchor_run_id}:{page}",
                )
            ]
        )
    if not launch.pending_work and not launch.archived:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🗄 Убрать из результатов",
                    callback_data=f"launch:archive:{launch.anchor_run_id}:{page}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К работе и результатам",
                callback_data=(f"menu:archive:{page}" if launch.archived else f"menu:runs:{page}"),
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def launch_edit_cancel(anchor_run_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Не менять",
                    callback_data=f"launch:edit:cancel:{anchor_run_id}:{page}",
                )
            ]
        ]
    )


def launch_cancel_confirmation(anchor_run_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🛑 Да, остановить отправку",
                    callback_data=f"launch:cancel:confirm:{anchor_run_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, продолжить",
                    callback_data=f"launch:details:{anchor_run_id}:{page}",
                )
            ],
        ]
    )


def launch_retry_confirmation(anchor_run_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔁 Да, повторить ошибки",
                    callback_data=f"launch:retry:confirm:{anchor_run_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Нет, вернуться",
                    callback_data=f"launch:details:{anchor_run_id}:{page}",
                )
            ],
        ]
    )


def template_edit_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Сохранить изменения", callback_data="template:edit:save"
                )
            ],
            [InlineKeyboardButton(text="✖️ Отменить изменения", callback_data="fsm:cancel")],
        ]
    )


def runs_menu(page: Page[CampaignRun]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for run in page.items:
        row = [
            InlineKeyboardButton(
                text=f"#{run.id} · {_run_status_label(run.status.value)}",
                callback_data=f"run:details:{run.id}:{page.page}",
            )
        ]
        if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}:
            row.append(
                InlineKeyboardButton(
                    text="🛑 Стоп", callback_data=f"run:cancel:{run.id}:{page.page}"
                )
            )
        builder.row(*row)
    _pagination(builder, "runs", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"menu:runs:{page.page}")
    )
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def audit_menu(page: Page[object]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    _pagination(builder, "audit", page.page, page.pages)
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def settings_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💾 Резервная копия",
                    callback_data="maintenance:backup",
                ),
                InlineKeyboardButton(text="🩺 Диагностика", callback_data="maintenance:doctor"),
            ],
            [
                InlineKeyboardButton(
                    text="🛑 Остановить все отправки", callback_data="queue:stopall:ask"
                )
            ],
            [InlineKeyboardButton(text="📜 Журнал действий", callback_data="menu:audit:0")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:settings")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def stop_all_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🛑 Да, остановить всё", callback_data="queue:stopall:confirm"
                )
            ],
            [InlineKeyboardButton(text="⬅️ Нет, вернуться", callback_data="menu:settings")],
        ]
    )


def cancel_menu(cancel_callback: str = "fsm:cancel") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel_callback)]]
    )


def ownership_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да, принадлежит мне", callback_data="account:confirm:yes"
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def account_picker(
    accounts: list[Account],
    role: str,
    *,
    cancel_callback: str = "fsm:cancel",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=f"{_account_icon(account)} {account.label}",
            callback_data=f"campaign:pick:{role}:{account.id}",
        )
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data=cancel_callback))
    return builder.as_markup()


def target_folder_account_picker(accounts: list[Account]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.button(
            text=f"{_account_icon(account)} {account.label}",
            callback_data=f"target:folder:account:{account.id}",
        )
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel"))
    return builder.as_markup()


def campaign_sender_picker(
    accounts: list[Account],
    selected_ids: set[int],
    *,
    cancel_callback: str = "fsm:cancel",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        marker = "✅" if account.id in selected_ids else "☐"
        builder.button(
            text=f"{marker} {account.label}",
            callback_data=f"campaign:sender:toggle:{account.id}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="☑️ Выбрать все", callback_data="campaign:sender:all"),
        InlineKeyboardButton(text="✖️ Снять все", callback_data="campaign:sender:clear"),
    )
    builder.row(
        InlineKeyboardButton(
            text=f"✅ Продолжить ({len(selected_ids)})",
            callback_data="campaign:sender:done",
        )
    )
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel_callback))
    return builder.as_markup()


def target_picker(targets: list[Target]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target in targets:
        builder.button(text=f"🎯 {target.title}", callback_data=f"campaign:pick:target:{target.id}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel"))
    return builder.as_markup()


def campaign_target_picker(
    targets_page: Page[Target] | list[Target],
    target_lists: list[TargetListView],
) -> InlineKeyboardMarkup:
    if isinstance(targets_page, list):
        targets_page = Page(
            items=targets_page,
            page=0,
            page_size=len(targets_page) or 1,
            total=len(targets_page),
        )
    builder = InlineKeyboardBuilder()
    for target_list in target_lists:
        builder.button(
            text=f"📚 {target_list.name} ({target_list.approved_count})",
            callback_data=f"campaign:pick:targetlist:{target_list.id}",
        )
    for target in targets_page.items:
        builder.button(text=f"🎯 {target.title}", callback_data=f"campaign:pick:target:{target.id}")
    builder.adjust(1)
    if targets_page.pages > 1:
        pagination: list[InlineKeyboardButton] = []
        if targets_page.page > 0:
            pagination.append(
                InlineKeyboardButton(
                    text="←",
                    callback_data=f"campaign:pick:page:{targets_page.page - 1}",
                )
            )
        pagination.append(
            InlineKeyboardButton(
                text=f"{targets_page.page + 1}/{targets_page.pages}",
                callback_data="noop",
            )
        )
        if targets_page.page + 1 < targets_page.pages:
            pagination.append(
                InlineKeyboardButton(
                    text="→",
                    callback_data=f"campaign:pick:page:{targets_page.page + 1}",
                )
            )
        builder.row(*pagination)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel"))
    return builder.as_markup()


def campaign_audience_source_menu(
    *,
    standalone_count: int,
    list_count: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if list_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📚 Выбрать список · {list_count}",
                    callback_data="campaign:targetsource:lists:0",
                )
            ]
        )
    if standalone_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📌 Выбрать отдельный чат · {standalone_count}",
                    callback_data="campaign:targetsource:chats:0",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_standalone_target_picker(page: Page[Target]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target in page.items:
        builder.button(
            text=f"📌 {_compact_button_label(target.title, 54)}",
            callback_data=f"campaign:pick:target:{target.id}",
        )
    builder.adjust(1)
    _campaign_audience_pagination(builder, "chats", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти чат",
            callback_data="campaign:targetsearch:chats",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К выбору аудитории",
            callback_data="campaign:targetsource:home:0",
        )
    )
    return builder.as_markup()


def campaign_target_list_picker(page: Page[TargetListView]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for target_list in page.items:
        builder.button(
            text=(
                f"📚 {_compact_button_label(target_list.name, 38)} · "
                f"{target_list.approved_count} чатов"
            ),
            callback_data=f"campaign:pick:targetlist:{target_list.id}",
        )
    builder.adjust(1)
    _campaign_audience_pagination(builder, "lists", page.page, page.pages)
    builder.row(
        InlineKeyboardButton(
            text="🔎 Найти список",
            callback_data="campaign:targetsearch:lists",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К выбору аудитории",
            callback_data="campaign:targetsource:home:0",
        )
    )
    return builder.as_markup()


def _campaign_audience_pagination(
    builder: InlineKeyboardBuilder,
    kind: str,
    page: int,
    pages: int,
) -> None:
    if pages <= 1:
        return
    row: list[InlineKeyboardButton] = []
    if page > 1:
        row.append(
            InlineKeyboardButton(
                text="⏮",
                callback_data=f"campaign:targetsource:{kind}:0",
            )
        )
    if page > 0:
        row.append(
            InlineKeyboardButton(
                text="‹",
                callback_data=f"campaign:targetsource:{kind}:{page - 1}",
            )
        )
    row.append(InlineKeyboardButton(text=f"{page + 1} / {pages}", callback_data="noop"))
    if page + 1 < pages:
        row.append(
            InlineKeyboardButton(
                text="›",
                callback_data=f"campaign:targetsource:{kind}:{page + 1}",
            )
        )
    if page < pages - 2:
        row.append(
            InlineKeyboardButton(
                text="⏭",
                callback_data=f"campaign:targetsource:{kind}:{pages - 1}",
            )
        )
    builder.row(*row)


def template_picker(templates: list[MessageTemplate]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for template in templates:
        builder.button(
            text=template.name,
            callback_data=f"campaign:pick:template:{template.id}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(
            text="✍️ Ввести тексты вручную", callback_data="campaign:pick:template:manual"
        )
    )
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel"))
    return builder.as_markup()


def template_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Сохранить текст", callback_data="template:confirm")],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def campaign_mode_picker() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Рассылка",
                    callback_data="campaign:pick:mode:broadcast",
                )
            ],
            [
                InlineKeyboardButton(
                    text="💬 Диалог",
                    callback_data="campaign:pick:mode:dialogue",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def campaign_repeat_mode_picker() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="1️⃣ Одноразовая · один пост в чат",
                    callback_data="campaign:repeat:once",
                )
            ],
            [
                InlineKeyboardButton(
                    text="♻️ Умная непрерывная",
                    callback_data="campaign:repeat:smart",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def campaign_one_off_window_presets() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for hours, label in [
        (0, "⚡ Сразу"),
        (1, "1 ч"),
        (6, "6 ч"),
        (12, "12 ч"),
        (24, "24 ч"),
        (72, "3 суток"),
        (168, "7 суток"),
    ]:
        builder.button(
            text=label,
            callback_data=f"campaign:once:window:{hours}",
        )
    builder.adjust(1, 3, 3)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel"))
    return builder.as_markup()


def campaign_smart_duration_presets(
    *,
    cancel_callback: str = "fsm:cancel",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for hours, label in [
        (1, "1 ч"),
        (6, "6 ч"),
        (12, "12 ч"),
        (24, "24 ч"),
        (48, "2 суток"),
        (72, "3 суток"),
        (168, "7 суток"),
    ]:
        builder.button(
            text=label,
            callback_data=f"campaign:smart:duration:{hours}",
        )
    builder.adjust(1, 3, 3)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel_callback))
    return builder.as_markup()


def campaign_smart_interval_presets(
    *,
    cancel_callback: str = "fsm:cancel",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for minutes, label in [
        (3, "3 мин"),
        (5, "⭐ 5 мин · активная"),
        (10, "10 мин"),
        (15, "15 мин"),
        (30, "30 мин"),
        (60, "1 ч"),
        (180, "3 ч"),
        (360, "6 ч"),
        (1440, "24 ч"),
    ]:
        builder.button(
            text=label,
            callback_data=f"campaign:smart:interval:{minutes}",
        )
    builder.adjust(2, 1, 3, 2)
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel_callback))
    return builder.as_markup()


def campaign_repeat_interval_presets(prefix: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for minutes, label in [
        (1, "⏱ По таймеру чата"),
        (30, "30 мин"),
        (60, "1 ч"),
        (120, "2 ч"),
        (360, "6 ч"),
        (720, "12 ч"),
        (1440, "24 ч"),
    ]:
        builder.button(
            text=label,
            callback_data=f"{prefix}:interval:{minutes}",
        )
    builder.adjust(1, 3, 3)
    cancel = "cschedule:cancel" if prefix == "cschedule" else "fsm:cancel"
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel))
    return builder.as_markup()


def campaign_repeat_count_presets(prefix: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for count in (2, 3, 6, 12, 24):
        builder.button(
            text=str(count),
            callback_data=f"{prefix}:count:{count}",
        )
    builder.adjust(5)
    cancel = "cschedule:cancel" if prefix == "cschedule" else "fsm:cancel"
    builder.row(InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel))
    return builder.as_markup()


def campaign_schedule_mode_picker(
    *,
    smart_timer_mode: bool = False,
    allow_finite_repeats: bool = True,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="1️⃣ Сделать одноразовой",
                callback_data="cschedule:once",
            )
        ]
    ]
    if not smart_timer_mode and allow_finite_repeats:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔁 Настроить конечные повторы",
                    callback_data="cschedule:smart",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text=(
                        "♻️ Изменить умную рассылку"
                        if smart_timer_mode
                        else "♻️ Сделать умной непрерывной"
                    ),
                    callback_data="cschedule:continuous",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="cschedule:cancel")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def dialogue_chain_menu(
    step_count: int,
    *,
    cancel_callback: str = "fsm:cancel",
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="➕ Добавить сообщение",
                callback_data="campaign:chain:add",
            )
        ]
    ]
    if step_count > 0:
        rows.append(
            [
                InlineKeyboardButton(
                    text="↩️ Удалить последнее",
                    callback_data="campaign:chain:remove",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text=f"✅ Цепочка готова · {step_count} сообщений",
                    callback_data="campaign:chain:done",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data=cancel_callback)],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def campaign_confirmation() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Сохранить и открыть список",
                    callback_data="campaign:confirm",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def campaign_run_confirmation(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Да, поставить в очередь",
                    callback_data=f"campaign:run:confirm:{campaign_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✖️ Отмена", callback_data=f"campaign:details:{campaign_id}:{page}"
                )
            ],
        ]
    )


def run_details_menu(run: CampaignRun, page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🛑 Остановить", callback_data=f"run:cancel:{run.id}:{page}"
                )
            ]
        )
    rows.append(
        [InlineKeyboardButton(text="⬅️ К работе и результатам", callback_data=f"menu:runs:{page}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _pagination(builder: InlineKeyboardBuilder, section: str, page: int, pages: int) -> None:
    if pages <= 1:
        return
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(text="‹", callback_data=f"menu:{section}:{page - 1}")
        )
    navigation.append(InlineKeyboardButton(text=f"{page + 1} / {pages}", callback_data="noop"))
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="›", callback_data=f"menu:{section}:{page + 1}")
        )
    builder.row(*navigation)
    jumps: list[InlineKeyboardButton] = []
    if page > 1:
        jumps.append(InlineKeyboardButton(text="⏮ В начало", callback_data=f"menu:{section}:0"))
    if page > 10:
        jumps.append(
            InlineKeyboardButton(
                text="−10",
                callback_data=f"menu:{section}:{max(0, page - 10)}",
            )
        )
    if page + 10 < pages - 1:
        jumps.append(
            InlineKeyboardButton(
                text="+10",
                callback_data=f"menu:{section}:{min(pages - 1, page + 10)}",
            )
        )
    if page < pages - 2:
        jumps.append(
            InlineKeyboardButton(
                text="В конец ⏭",
                callback_data=f"menu:{section}:{pages - 1}",
            )
        )
    if jumps:
        builder.row(*jumps)


def _proxy_pagination(builder: InlineKeyboardBuilder, page: ProxyPage) -> None:
    if page.pages <= 1:
        return

    def callback_for(target_page: int) -> str:
        return (
            f"proxy:groupfilter:{page.group_id}:{target_page}"
            if page.group_id is not None
            else f"proxy:list:{page.filter_name.value}:{target_page}"
        )

    navigation: list[InlineKeyboardButton] = []
    if page.page > 0:
        navigation.append(InlineKeyboardButton(text="‹", callback_data=callback_for(page.page - 1)))
    navigation.append(
        InlineKeyboardButton(text=f"{page.page + 1} / {page.pages}", callback_data="noop")
    )
    if page.page + 1 < page.pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=callback_for(page.page + 1)))
    builder.row(*navigation)
    jumps: list[InlineKeyboardButton] = []
    if page.page > 1:
        jumps.append(InlineKeyboardButton(text="⏮ В начало", callback_data=callback_for(0)))
    if page.page > 10:
        jumps.append(
            InlineKeyboardButton(
                text="−10",
                callback_data=callback_for(max(0, page.page - 10)),
            )
        )
    if page.page + 10 < page.pages - 1:
        jumps.append(
            InlineKeyboardButton(
                text="+10",
                callback_data=callback_for(min(page.pages - 1, page.page + 10)),
            )
        )
    if page.page < page.pages - 2:
        jumps.append(
            InlineKeyboardButton(text="В конец ⏭", callback_data=callback_for(page.pages - 1))
        )
    if jumps:
        builder.row(*jumps)


def _run_status_label(value: str) -> str:
    return {
        "queued": "ожидает",
        "running": "в работе",
        "succeeded": "готово",
        "failed": "ошибка",
        "cancelled": "отменён",
    }[value]


def _launch_icon(launch: LaunchView) -> str:
    if launch.running:
        return "🔵"
    if launch.queued:
        return "🕒"
    if launch.failed:
        return "❌"
    if launch.succeeded == launch.total:
        return "✅"
    if launch.cancelled == launch.total:
        return "🚫"
    return "🟠"
