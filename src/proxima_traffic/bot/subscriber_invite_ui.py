"""Control-bot screens for consent-based subscriber invitations."""

from __future__ import annotations

from datetime import UTC, datetime
from html import escape
from math import ceil

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from proxima_traffic.bot.ui import screen_title, section, themed_keyboard
from proxima_traffic.failure_policy import failure_hint, failure_label
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    SubscriberList,
    SubscriberSegment,
)
from proxima_traffic.subscriber_invites import (
    SubscriberInviteCampaignView,
    SubscriberInviteLaunchView,
)


def subscriber_invites_home_text() -> str:
    return (
        screen_title(
            "Приглашения в канал или группу",
            "Отдельный инструмент для вашей базы подписчиков.",
        )
        + "\n\n"
        f"{section('Как это работает')}\n"
        "01 — Выберите базу и сегмент.\n"
        "02 — Укажите свою цель и аккаунты-администраторы.\n"
        "03 — Перед запуском сервис заново проверит права и доступность.\n\n"
        "Он приглашает только там, где Telegram разрешает это выбранному администратору. "
        "Приватность человека, стоп-лист и «не писать» всегда сильнее запуска.\n\n"
        "• не добавляет людей в контакты и не обходит ограничения;\n"
        "• проверяет права каждого выбранного аккаунта до создания очереди;\n"
        "• не приглашает одного человека двумя запусками одновременно;\n"
        "• помнит попытку и соблюдает выбранный срок защиты от повтора;\n"
        "• при неясном результате не повторяет приглашение автоматически."
    )


def subscriber_invites_home_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Создать приглашение", callback_data="inv:new")],
            [
                InlineKeyboardButton(text="📝 Заготовки", callback_data="inv:campaigns:0"),
                InlineKeyboardButton(text="📊 Запуски", callback_data="inv:launches:0"),
            ],
            [InlineKeyboardButton(text="🚫 Стоп-лист приглашений", callback_data="inv:blacklist")],
            [InlineKeyboardButton(text="✉️ Вернуться к личным сообщениям", callback_data="dm:home")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="inv:home")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def _invite_wizard_title(step: int) -> str:
    return screen_title("Новое приглашение", f"Шаг {step} из 6")


def invite_destination_prompt_text() -> str:
    return (
        _invite_wizard_title(1) + "\n\n"
        "Отправьте <b>@username</b> своего канала/супергруппы или публичную ссылку "
        "<code>https://t.me/name</code> (также подходят ссылки на пост "
        "или <code>/s/name</code>).\n\n"
        "Для закрытой цели укажите invite-ссылку только если выбранные аккаунты уже "
        "являются администраторами. Перед запуском сервис всё равно проверит права."
    )


def invite_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="inv:home")]]
    )


def invite_list_picker_text(total: int, page: int, page_size: int) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        _invite_wizard_title(2) + "\n\n"
        f"{section('Выберите базу подписчиков')}\n"
        f"Готовых баз: <b>{total:,}</b> · страница <b>{page + 1}/{pages}</b>\n\n"
        "Используйте только базу людей, которые согласились получать приглашения."
    )


def invite_list_picker(
    items: list[SubscriberList], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"📂 {item.name[:28]} · {item.member_count:,}",
                callback_data=f"inv:new:list:{item.id}",
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv:new:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"inv:new:lists:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Обновить список",
                    callback_data=f"inv:new:lists:{page}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_segment_text(item: SubscriberList) -> str:
    return (
        _invite_wizard_title(3) + "\n\n"
        f"База: <b>{escape(item.name)}</b>\n\n"
        "Выберите получателей приглашения. Стоп-лист, отписавшиеся, недавние и занятые "
        "другим запуском будут исключены автоматически.\n\n"
        f"👤 Все: <b>{item.member_count:,}</b>\n"
        f"⭐ Premium: <b>{item.premium_count:,}</b>\n"
        f"Обычные: <b>{item.non_premium_count:,}</b>"
    )


def invite_segment_menu(list_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Все", callback_data=f"inv:new:seg:{list_id}:all")],
            [
                InlineKeyboardButton(
                    text="⭐ Только Premium", callback_data=f"inv:new:seg:{list_id}:premium"
                ),
                InlineKeyboardButton(
                    text="Обычные", callback_data=f"inv:new:seg:{list_id}:non_premium"
                ),
            ],
            [InlineKeyboardButton(text="⬅️ К базам", callback_data="inv:new:lists:0")],
        ]
    )


def invite_limit_prompt_text(segment: SubscriberSegment) -> str:
    title = {
        SubscriberSegment.ALL: "всем",
        SubscriberSegment.PREMIUM: "Premium-подписчикам",
        SubscriberSegment.NON_PREMIUM: "обычным подписчикам",
    }[segment]
    return (
        _invite_wizard_title(4) + "\n\n"
        f"{section(f'Сколько пригласить {title}?')}\n"
        "Отправьте число от 1 до 1 000 000 либо слово <code>все</code>.\n"
        "Очередь будет сформирована только из доступных людей."
    )


def invite_cooldown_text() -> str:
    return (
        _invite_wizard_title(5) + "\n\n"
        f"{section('Защита от повторного приглашения')}\n"
        "Сколько дней не пытаться пригласить этого человека повторно в любой из ваших "
        "каналов или групп? Рекомендуется 90 дней."
    )


def invite_cooldown_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="7 дней", callback_data="inv:new:cooldown:7"),
                InlineKeyboardButton(text="30 дней", callback_data="inv:new:cooldown:30"),
            ],
            [
                InlineKeyboardButton(text="✅ 90 дней", callback_data="inv:new:cooldown:90"),
                InlineKeyboardButton(text="0 дней", callback_data="inv:new:cooldown:0"),
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="inv:home")],
        ]
    )


def invite_account_picker_text(selected_count: int, total: int, page: int, page_size: int) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        _invite_wizard_title(6) + "\n\n"
        f"{section('Выберите аккаунты-администраторы')}\n"
        f"Выбрано: <b>{selected_count}</b> из <b>{total}</b>\n"
        f"Страница <b>{page + 1}/{pages}</b>\n\n"
        "До запуска каждый аккаунт проверяется на доступ и право приглашать в эту цель. "
        "Неготовые не остановят готовые аккаунты."
    )


def invite_account_picker(
    accounts: list[Account], selected_ids: set[int], *, page: int, page_size: int
) -> InlineKeyboardMarkup:
    start = page * page_size
    rows: list[list[InlineKeyboardButton]] = []
    for account in accounts[start : start + page_size]:
        marker = "✅" if account.id in selected_ids else "▫️"
        label = (account.label or f"Аккаунт #{account.id}")[:32]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{marker} {label}", callback_data=f"inv:new:account:{account.id}"
                )
            ]
        )
    pages = max(1, ceil(len(accounts) / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"inv:new:accounts:page:{page - 1}")
        )
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="➡️", callback_data=f"inv:new:accounts:page:{page + 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(text="✅ Выбрать всех", callback_data="inv:new:accounts:all"),
                InlineKeyboardButton(text="🧹 Очистить", callback_data="inv:new:accounts:none"),
            ],
            [InlineKeyboardButton(text="➡️ Продолжить", callback_data="inv:new:accounts:done")],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_campaign_text(view: SubscriberInviteCampaignView, *, confirm: bool = False) -> str:
    campaign = view.campaign
    accounts = ", ".join(escape(label) for label in view.account_labels) or "—"
    action = (
        "<b>Подтвердите запуск</b> — очередь создастся только из прошедших фильтры людей."
        if confirm
        else "Можно запустить после подтверждения согласия и прав на цель."
    )
    return (
        screen_title(escape(campaign.name), "Проверьте параметры приглашения") + "\n\n"
        f"🎯 Цель: <code>{escape(campaign.destination_ref)}</code>\n"
        f"📂 База: <b>{escape(view.list_name)}</b>\n"
        f"👤 Сегмент: <b>{escape(campaign.segment.value)}</b>\n"
        f"🔢 Лимит: <b>{campaign.recipient_limit:,}</b>\n"
        f"🛡 Повтор: не ранее <b>{campaign.contact_cooldown_days} дн.</b>\n"
        f"👤 Администраторы: <b>{accounts}</b>\n\n"
        f"Проходят фильтры сейчас: <b>{view.eligible_count:,}</b>\n"
        f"Исключено недавно: <b>{view.recent_count:,}</b>\n"
        f"Занято другим запуском: <b>{view.reserved_count:,}</b>\n\n"
        f"{action}\n"
        "<i>Это не гарантия приглашения: окончательный результат определяет Telegram "
        "для каждого человека и выбранного администратора. Приватность не обходится.</i>"
    )


def invite_campaign_confirmation_menu(campaign_id: int, token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтверждаю согласие и права",
                    callback_data=f"inv:launch:{campaign_id}:{token}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="inv:home")],
        ]
    )


def invite_campaign_details_menu(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Запустить", callback_data=f"inv:campaignrun:{campaign_id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К заготовкам", callback_data=f"inv:campaigns:{page}")],
        ]
    )


def invite_campaigns_text(
    items: list[SubscriberInviteCampaignView], total: int, page: int, page_size: int
) -> str:
    pages = max(1, ceil(total / page_size))
    lines = [
        screen_title("Заготовки приглашений", f"Всего: {total:,} · страница {page + 1}/{pages}"),
        "",
    ]
    for view in items:
        lines.append(
            f"👥 <b>{escape(view.campaign.name)}</b>\n"
            f"   {escape(view.campaign.destination_ref)} · до {view.campaign.recipient_limit:,}"
        )
    if not items:
        lines.append(
            "<b>С чего начать</b>\n"
            "Создайте приглашение: оно сохранится как заготовка после проверки."
        )
    else:
        lines.append(
            "\n<i>Откройте заготовку, чтобы проверить доступную выборку и запустить её.</i>"
        )
    return "\n".join(lines)


def invite_campaigns_menu(
    items: list[SubscriberInviteCampaignView], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"👥 {view.campaign.name[:35]}",
                callback_data=f"inv:campaign:{view.campaign.id}:{page}",
            )
        ]
        for view in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv:campaigns:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"inv:campaigns:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="🚀 Создать приглашение", callback_data="inv:new")],
            [
                InlineKeyboardButton(
                    text="🔄 Обновить список",
                    callback_data=f"inv:campaigns:{page}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К приглашениям", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_text(view: SubscriberInviteLaunchView) -> str:
    launch = view.launch
    status = {
        DirectLaunchStatus.QUEUED: "🔵 в очереди",
        DirectLaunchStatus.RUNNING: "🟡 выполняется",
        DirectLaunchStatus.SUCCEEDED: "🟢 завершён",
        DirectLaunchStatus.PARTIAL: "🟡 завершён частично",
        DirectLaunchStatus.CANCELLED: "⚪ остановлен",
        DirectLaunchStatus.FAILED: "🔴 завершён с ошибкой",
    }.get(launch.status, escape(launch.status.value))
    timing = ""
    if view.next_scheduled_for is not None:
        timing = (
            "\n⏱ Ближайшая безопасная попытка: "
            f"<code>{_invite_date_time(view.next_scheduled_for)}</code>"
        )
    issue_summary = ""
    if view.issues:
        status_labels = {
            DirectMessageTaskStatus.QUEUED: "отложено",
            DirectMessageTaskStatus.FAILED: "ошибка / неясный исход",
            DirectMessageTaskStatus.SKIPPED: "пропущено без приглашения",
        }
        issue_summary = "\n\n<b>📌 Частые причины — до 3</b>\n" + "\n".join(
            "• "
            f"<b>{count:,}</b> · {escape(failure_label(code))} "
            f"<i>({status_labels.get(status, status.value)})</i>"
            for status, code, count in view.issues
        )
    problem = ""
    if view.last_error_code:
        problem = "\n\n<b>⚠️ Последняя зафиксированная ситуация</b>"
        if view.last_error_account_label:
            problem += f"\nПриглашающий аккаунт: <b>{escape(view.last_error_account_label)}</b>"
        if view.last_error_recipient:
            problem += f"\nПодписчик: <code>{escape(view.last_error_recipient)}</code>"
        problem += f"\nПричина: <b>{escape(failure_label(view.last_error_code))}</b>" + (
            f"\n<i>{escape(failure_hint(view.last_error_code))}</i>"
            if failure_hint(view.last_error_code)
            else ""
        )
        if view.last_error_status == DirectMessageTaskStatus.QUEUED:
            problem += "\nСистема: попытка отложена; остальные безопасные приглашения продолжаются."
        elif view.last_error_status == DirectMessageTaskStatus.SKIPPED:
            problem += "\nСистема: этот контакт пропущен; приглашение ему не подтверждено."
        elif view.last_error_status == DirectMessageTaskStatus.FAILED:
            if view.last_error_code == "invite_delivery_unconfirmed":
                problem += (
                    "\nСистема: Telegram не подтвердил итог. Автоматического повтора нет, "
                    "потому что человек уже мог быть добавлен."
                )
            else:
                problem += "\nСистема: задача завершена без небезопасного повтора."
        if view.last_error:
            problem += f"\nТехнически: <code>{escape(view.last_error)}</code>"
    accounted = (
        view.invited
        + view.already_participant
        + view.queued
        + view.running
        + view.skipped
        + view.failed
        + view.cancelled
    )
    integrity_note = ""
    if accounted != launch.total_tasks:
        integrity_note = (
            "\n\n⚠️ <b>Сводка задач расходится с запуском:</b> "
            f"учтено {accounted:,} из {launch.total_tasks:,}. Обновите экран; "
            "если расхождение останется — проверьте журнал."
        )
    return (
        screen_title(f"Приглашение #{launch.id}", f"Статус: {status}") + "\n\n"
        f"🎯 Цель: <code>{escape(launch.destination_ref_snapshot)}</code>\n"
        f"📂 База: <b>{escape(view.list_name)}</b>\n"
        f"📝 Заготовка: <b>{escape(view.campaign_name)}</b>\n\n"
        f"{section('Результат')}\n"
        f"Всего задач запуска: <b>{launch.total_tasks:,}</b>\n"
        f"✅ Telegram подтвердил приглашение: <b>{view.invited:,}</b>\n"
        f"ℹ️ Уже были участниками: <b>{view.already_participant:,}</b>\n"
        f"🕒 В очереди: <b>{view.queued:,}</b>\n"
        f"🔵 Выполняется сейчас: <b>{view.running:,}</b>\n"
        f"⏭ Пропущено без приглашения: <b>{view.skipped:,}</b>\n"
        f"⚠️ Ошибка или неясный результат: <b>{view.failed:,}</b>\n"
        f"⏹ Отменено до выполнения: <b>{view.cancelled:,}</b>\n\n"
        "<i>«Подтверждено» означает успешный результат операции Telegram. "
        "Неподтверждённые исходы в этот счётчик не входят.</i>"
        f"{timing}"
        f"{integrity_note}"
        f"{issue_summary}"
        f"{problem}"
    )


def _invite_date_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%d.%m %H:%M UTC")


def invite_launches_text(
    items: list[SubscriberInviteLaunchView], total: int, page: int, page_size: int
) -> str:
    pages = max(1, ceil(total / page_size))
    lines = [
        screen_title("Запуски приглашений", f"Всего: {total:,} · страница {page + 1}/{pages}"),
        "",
    ]
    for view in items:
        lines.append(
            f"👥 <b>#{view.launch.id} · {escape(view.campaign_name)}</b>\n"
            f"   ✅ приглашено {view.invited:,} · ℹ️ уже были {view.already_participant:,}\n"
            f"   ⏳ активно {view.active:,} · ⏭ пропущено {view.skipped:,} · "
            f"⚠️ ошибка/неясно {view.failed:,} · ⏹ отменено {view.cancelled:,}"
        )
    if not items:
        lines.append("Запусков ещё нет. После старта здесь появятся статус и причины пропусков.")
    return "\n".join(lines)


def invite_launches_menu(
    items: list[SubscriberInviteLaunchView], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"📊 #{view.launch.id} · {view.campaign_name[:31]}",
                callback_data=f"inv:launchview:{view.launch.id}:{page}",
            )
        ]
        for view in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv:launches:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"inv:launches:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"inv:launches:{page}")],
            [InlineKeyboardButton(text="⬅️ К приглашениям", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_details_menu(view: SubscriberInviteLaunchView, page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if view.active:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⏹ Остановить", callback_data=f"inv:launchcancel:{view.launch.id}:{page}"
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Обновить", callback_data=f"inv:launchview:{view.launch.id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К запускам", callback_data=f"inv:launches:{page}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_cancel_menu(launch_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏹ Да, остановить", callback_data=f"inv:launchcancelok:{launch_id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data=f"inv:launchview:{launch_id}:{page}"
                )
            ],
        ]
    )


for _factory_name, _factory in tuple(globals().items()):
    if (
        not _factory_name.startswith("_")
        and callable(_factory)
        and _factory_name.endswith(("menu", "picker", "confirmation", "cancel"))
    ):
        globals()[_factory_name] = themed_keyboard(_factory)
