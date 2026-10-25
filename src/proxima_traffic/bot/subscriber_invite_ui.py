"""Control-bot screens for consent-based subscriber invitations."""

from __future__ import annotations

from datetime import UTC, datetime
from html import escape
from math import ceil

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from proxima_traffic.bot.ui import (
    page_summary,
    screen_title,
    section,
    themed_keyboard,
    wizard_title,
)
from proxima_traffic.failure_policy import failure_hint, failure_label
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    SubscriberInviteCampaign,
    SubscriberList,
    SubscriberSegment,
)
from proxima_traffic.subscriber_invites import (
    SubscriberInviteCampaignView,
    SubscriberInviteLaunchView,
)


def subscriber_invites_home_text() -> str:
    return (
        screen_title("Приглашения", "В свои каналы и группы") + "\n\n"
        f"{section('Как это работает')}\n"
        "Выберите получателей из общей базы, укажите цель и аккаунты-администраторы. "
        "Перед запуском сервис проверит доступ и права.\n\n"
        f"{section('Права доступа')}\n"
        "Нужно право <b>«Добавлять участников / Invite users»</b>. "
        "Его можно подготовить в мастере через главного администратора. "
        "Invite-ссылка сама по себе права не выдаёт.\n\n"
        f"{section('Результаты')}\n"
        "Приглашение засчитывается после проверки участия. "
        "Новые участники, уже состоявшие и неподтверждённые приглашения учитываются отдельно. "
        "Неясный результат автоматически не повторяется.\n\n"
        "Стоп-лист, приватность и защита от повторов учитываются перед приглашением. "
        "Паузы по умолчанию: 60–600 секунд. При FloodWait — минимум 500 секунд "
        "или дольше по требованию Telegram."
    )


def subscriber_invites_home_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="＋ Создать приглашение", callback_data="inv:new")],
            [
                InlineKeyboardButton(text="Сохранённые задачи", callback_data="inv:campaigns:0"),
                InlineKeyboardButton(text="Запуски", callback_data="inv:launches:0"),
            ],
            [
                InlineKeyboardButton(text="Стоп-лист", callback_data="inv:blacklist"),
                InlineKeyboardButton(text="ЛС-рассылки", callback_data="dm:home"),
            ],
            [
                InlineKeyboardButton(
                    text="✓ Приглашённые / результаты", callback_data="rr:v:i:0:s:0:0:0"
                ),
                InlineKeyboardButton(text="⌕ Люди в базе", callback_data="rr:v:b:0:a:0:1:0"),
            ],
            [
                InlineKeyboardButton(text="↻ Обновить", callback_data="inv:home"),
                InlineKeyboardButton(text="⌂ Главная", callback_data="menu:home"),
            ],
        ]
    )


def _invite_wizard_title(step: int) -> str:
    return wizard_title("Новое приглашение", step, 6)


def invite_destination_prompt_text() -> str:
    return (
        _invite_wizard_title(1) + "\n\n"
        "Отправьте <b>@username</b> своего канала/супергруппы или публичную ссылку "
        "<code>https://t.me/name</code> (также подходят ссылки на пост "
        "или <code>/s/name</code>).\n\n"
        "<b>Важно перед запуском:</b> права можно подготовить прямо в мастере: один главный "
        "администратор с правом <b>«Добавлять администраторов / Add new admins»</b> выдаст "
        "выбранным аккаунтам только <b>«Добавлять участников / Invite users»</b>.\n\n"
        "Для закрытой цели invite-ссылка нужна только для проверки адреса. Она <b>не</b> "
        "вступает за аккаунт и не выдаёт права: сначала добавьте нужные аккаунты в админы, "
        "затем вставьте ссылку."
    )


def invite_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="× Отмена", callback_data="inv:home")]]
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
                text=f"{item.name[:28]} · {item.member_count:,}",
                callback_data=f"inv:new:list:{item.id}",
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹", callback_data=f"inv:new:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=f"inv:new:lists:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="↻ Обновить список",
                    callback_data=f"inv:new:lists:{page}",
                )
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_segment_text(item: SubscriberList) -> str:
    return (
        _invite_wizard_title(3) + "\n\n"
        f"Общая база: <b>{item.member_count:,}</b> человек\n\n"
        "Выберите получателей приглашения. Стоп-лист, отписавшиеся, недавние и занятые "
        "другим запуском будут исключены автоматически.\n\n"
        f"Все: <b>{item.member_count:,}</b>\n"
        f"☆ Premium: <b>{item.premium_count:,}</b>\n"
        f"Обычные: <b>{item.non_premium_count:,}</b>\n"
        "Статус Premium неизвестен: "
        f"<b>{max(0, item.member_count - item.premium_count - item.non_premium_count):,}</b>\n"
        "«Все» включает людей с неизвестным статусом."
    )


def invite_segment_menu(list_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="○ Все", callback_data=f"inv:new:seg:{list_id}:all")],
            [
                InlineKeyboardButton(
                    text="☆ Только Premium", callback_data=f"inv:new:seg:{list_id}:premium"
                ),
                InlineKeyboardButton(
                    text="Обычные", callback_data=f"inv:new:seg:{list_id}:non_premium"
                ),
            ],
            [InlineKeyboardButton(text="‹ К цели", callback_data="inv:new")],
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
        "каналов или групп? Рекомендуется 90 дней.\n\n"
        "Это срок защиты контакта, отдельно от паузы между действиями аккаунта. "
        "Даже при 0 дней неясный результат защищён от повтора минимум сутки."
    )


def invite_cooldown_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="7 дней", callback_data="inv:new:cooldown:7"),
                InlineKeyboardButton(text="30 дней", callback_data="inv:new:cooldown:30"),
            ],
            [
                InlineKeyboardButton(text="✓ 90 дней", callback_data="inv:new:cooldown:90"),
                InlineKeyboardButton(text="0 дней", callback_data="inv:new:cooldown:0"),
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="inv:home")],
        ]
    )


def invite_account_picker_text(selected_count: int, total: int, page: int, page_size: int) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        _invite_wizard_title(6) + "\n\n"
        f"{section('Выберите аккаунты-администраторы')}\n"
        f"Выбрано: <b>{selected_count}</b> из <b>{total}</b>\n"
        f"Страница <b>{page + 1}/{pages}</b>\n\n"
        "В строке показаны имя аккаунта и его @username/телефон, чтобы было понятно, кого "
        "вы выбираете. Если права ещё не выданы — нажмите <b>«Подготовить права через "
        "одного админа»</b>. Главному аккаунту потребуется только право <b>«Добавлять "
        "администраторов / Add new admins»</b>.\n\n"
        "Перед запуском сервис проверит права каждого аккаунта без их изменения. "
        "Неготовые будут показаны с причиной и не остановят готовые аккаунты."
    )


def _account_picker_label(account: Account) -> str:
    identity = f"@{account.username.lstrip('@')}" if account.username else account.phone
    base = account.label or f"Аккаунт #{account.id}"
    return f"{base} · {identity}" if identity else base


def invite_account_picker(
    accounts: list[Account], selected_ids: set[int], *, page: int, page_size: int
) -> InlineKeyboardMarkup:
    start = page * page_size
    rows: list[list[InlineKeyboardButton]] = []
    for account in accounts[start : start + page_size]:
        marker = "✓" if account.id in selected_ids else "○"
        label = _account_picker_label(account)[:52]
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
            InlineKeyboardButton(text="‹", callback_data=f"inv:new:accounts:page:{page - 1}")
        )
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="›", callback_data=f"inv:new:accounts:page:{page + 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(text="✓ Выбрать всех", callback_data="inv:new:accounts:all"),
                InlineKeyboardButton(text="× Очистить", callback_data="inv:new:accounts:none"),
            ],
            [
                InlineKeyboardButton(
                    text="↗ Подготовить права через одного админа",
                    callback_data="inv:new:accounts:prepare",
                )
            ],
            [InlineKeyboardButton(text="› Продолжить", callback_data="inv:new:accounts:done")],
            [InlineKeyboardButton(text="× Отмена", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_admin_manager_text(selected_count: int, total: int, page: int, page_size: int) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        screen_title("Подготовить права", "Выберите главного администратора") + "\n\n"
        f"Рабочих аккаунтов: <b>{selected_count}</b> · главных на выбор: <b>{total}</b>\n"
        f"Страница <b>{page + 1}/{pages}</b>\n\n"
        "Этот аккаунт должен уже быть администратором цели с правом <b>«Добавлять "
        "администраторов / Add new admins»</b>. После подтверждения сервис попробует выдать "
        "каждому выбранному аккаунту только право <b>«Добавлять участников / Invite users»</b>.\n\n"
        "Главный аккаунт не обязан участвовать в приглашениях. Если вы отметили и его, ему "
        "тоже должно быть выдано право Invite users.\n\n"
        "Для закрытой цели Telegram должен уметь видеть рабочие аккаунты: используйте их "
        "актуальные @username или сначала добавьте их в цель. Ошибка одного аккаунта не "
        "прервёт подготовку остальных."
    )


def invite_admin_manager_picker(
    accounts: list[Account], *, page: int, page_size: int
) -> InlineKeyboardMarkup:
    start = page * page_size
    rows = [
        [
            InlineKeyboardButton(
                text=f"{_account_picker_label(account)[:50]}",
                callback_data=f"inv:new:manager:{account.id}",
            )
        ]
        for account in accounts[start : start + page_size]
    ]
    pages = max(1, ceil(len(accounts) / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(
            InlineKeyboardButton(text="‹", callback_data=f"inv:new:managerpage:{page - 1}")
        )
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="›", callback_data=f"inv:new:managerpage:{page + 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="‹ К аккаунтам", callback_data="inv:new:accounts:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_admin_setup_confirm_text(manager: Account, selected_count: int) -> str:
    return (
        screen_title("Подготовить права", "Проверьте действие") + "\n\n"
        f"Главный администратор: <b>{escape(_account_picker_label(manager))}</b>\n"
        f"Получат право приглашать: <b>{selected_count}</b> аккаунтов\n\n"
        "Будет выдано только право <b>«Добавлять участников / Invite users»</b>; существующие "
        "права выбранных администраторов сохраняются. Telegram может отказать, если главный "
        "аккаунт не владелец/не имеет Add new admins или не может найти целевой аккаунт.\n\n"
        "Уже готовые аккаунты будут проверены без изменения прав. "
        "Если главному аккаунту нужна пауза, оставшиеся права можно подготовить позже; "
        "причина и время ожидания появятся в результате."
    )


def invite_admin_setup_confirm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✓ Выдать права", callback_data="inv:new:adminsetup:run")],
            [
                InlineKeyboardButton(
                    text="‹ Выбрать другого", callback_data="inv:new:accounts:prepare"
                )
            ],
            [InlineKeyboardButton(text="‹ К аккаунтам", callback_data="inv:new:accounts:back")],
        ]
    )


def invite_admin_setup_progress_text(checked: int, total: int, granted: int, failed: int) -> str:
    return (
        "<b>Telegram настраивает права администраторов…</b>\n\n"
        f"Проверено: <b>{checked}/{total}</b>\n"
        f"✓ Права готовы: <b>{granted}</b> · ! Не удалось: <b>{failed}</b>\n\n"
        "Приглашения людям ещё не отправляются."
    )


def invite_campaign_text(view: SubscriberInviteCampaignView, *, confirm: bool = False) -> str:
    campaign = view.campaign
    accounts = ", ".join(escape(label) for label in view.account_labels) or "—"
    action = (
        "<b>Подтвердите запуск</b> — будут приглашены только люди из доступной выборки."
        if confirm
        else "Можно запустить после подтверждения согласия и прав на цель."
    )
    return (
        screen_title(escape(campaign.name), "Проверьте параметры приглашения") + "\n\n"
        f"Цель: <code>{escape(campaign.destination_ref)}</code>\n"
        f"База: <b>{escape(view.list_name)}</b>\n"
        f"Сегмент: <b>{_invite_segment_label(campaign.segment)}</b>\n"
        f"Лимит: <b>{campaign.recipient_limit:,}</b>\n"
        f"Защита от повтора: <b>{campaign.contact_cooldown_days} дн.</b>\n"
        f"Администраторы: <b>{accounts}</b>\n\n"
        f"Доступно сейчас: <b>{view.eligible_count:,}</b>\n"
        f"Защищено от повтора: <b>{view.recent_count:,}</b>\n"
        f"Занято другим запуском: <b>{view.reserved_count:,}</b>\n\n"
        f"{action}\n"
        "<i>Перед подтверждением: выбранные аккаунты уже админы цели и имеют право "
        "приглашать участников. Проверка не приглашает людей и не меняет доступы.</i>\n"
        "<i>Приватность человека Telegram не обходится.</i>"
    )


def _invite_segment_label(segment: SubscriberSegment) -> str:
    return {
        SubscriberSegment.ALL: "Все",
        SubscriberSegment.PREMIUM: "Только Premium",
        SubscriberSegment.NON_PREMIUM: "Без Premium",
    }[segment]


def invite_preflight_progress_text(checked: int, total: int, ready: int, unavailable: int) -> str:
    return (
        "<b>Проверяю права и формирую очередь приглашений…</b>\n\n"
        f"Проверено аккаунтов: <b>{checked}/{total}</b>\n"
        f"✓ Готовы: <b>{ready}</b> · ! Неготовы: <b>{unavailable}</b>\n\n"
        "Приглашения ещё не отправляются. "
        "Если хотя бы один администратор готов, остальные не остановят запуск."
    )


def invite_campaign_confirmation_menu(campaign_id: int, token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✓ Подтверждаю согласие и права",
                    callback_data=f"inv:launch:{campaign_id}:{token}",
                )
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="inv:home")],
        ]
    )


def invite_campaign_details_menu(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="› Запустить", callback_data=f"inv:campaignrun:{campaign_id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="‹ К приглашениям", callback_data=f"inv:campaigns:{page}")],
        ]
    )


def invite_campaigns_text(
    items: list[SubscriberInviteCampaignView] | list[SubscriberInviteCampaign],
    total: int,
    page: int,
    page_size: int,
) -> str:
    lines = [
        screen_title("Сохранённые приглашения", page_summary(total, page, page_size)),
        "",
    ]
    for item in items:
        campaign = item.campaign if isinstance(item, SubscriberInviteCampaignView) else item
        lines.append(
            f"<b>{escape(campaign.name)}</b>\n"
            f"  {escape(campaign.destination_ref)} · "
            f"до {campaign.recipient_limit:,} получателей\n"
        )
    if not items:
        lines.append(
            "<b>С чего начать</b>\n"
            "Создайте приглашение: после проверки его можно будет запускать повторно."
        )
    else:
        lines.append(
            "\n<i>Откройте сохранённое приглашение, "
            "чтобы проверить получателей и запустить его.</i>"
        )
    return "\n".join(lines)


def invite_campaigns_menu(
    items: list[SubscriberInviteCampaignView] | list[SubscriberInviteCampaign],
    total: int,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"○ {campaign.name[:35]}",
                callback_data=f"inv:campaign:{campaign.id}:{page}",
            )
        ]
        for item in items
        for campaign in [item.campaign if isinstance(item, SubscriberInviteCampaignView) else item]
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹", callback_data=f"inv:campaigns:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=f"inv:campaigns:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="↗ Создать приглашение", callback_data="inv:new")],
            [
                InlineKeyboardButton(
                    text="↻ Обновить список",
                    callback_data=f"inv:campaigns:{page}",
                )
            ],
            [InlineKeyboardButton(text="‹ К приглашениям", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_text(view: SubscriberInviteLaunchView) -> str:
    launch = view.launch
    invite_success_label = (
        "Смоделировано приглашений"
        if view.simulated is True
        else "✓ Приглашено"
        if view.simulated is False
        else "? Успешных задач (режим запуска не сохранён)"
    )
    status = {
        DirectLaunchStatus.QUEUED: "в очереди",
        DirectLaunchStatus.RUNNING: "выполняется",
        DirectLaunchStatus.SUCCEEDED: "завершён",
        DirectLaunchStatus.PARTIAL: "завершён частично",
        DirectLaunchStatus.CANCELLED: "остановлен",
        DirectLaunchStatus.FAILED: "ошибка",
    }.get(launch.status, escape(launch.status.value))
    if launch.status == DirectLaunchStatus.FAILED and view.uncertain:
        status = "? завершён, есть неподтверждённые приглашения"
    timing = ""
    if view.next_scheduled_for is not None:
        timing = (
            "\n◷ Следующая проверка очереди: "
            f"<code>{_invite_date_time(view.next_scheduled_for)}</code>"
        )
    issue_summary = ""
    if view.issues:
        status_labels = {
            DirectMessageTaskStatus.QUEUED: "отложено",
            DirectMessageTaskStatus.FAILED: "ошибка",
            DirectMessageTaskStatus.SKIPPED: "пропущено",
        }
        uncertain_codes = {"invite_delivery_unconfirmed", "recipient_identity_mismatch"}
        issue_lines = []
        for issue_status, code, count in view.issues:
            label = (
                "не подтверждено"
                if code in uncertain_codes and issue_status == DirectMessageTaskStatus.FAILED
                else status_labels.get(issue_status, issue_status.value)
            )
            issue_lines.append(
                f"• <b>{count:,}</b> · {escape(failure_label(code))} <i>({label})</i>"
            )
        issue_summary = "\n\n<b>Основные причины</b>\n" + "\n".join(issue_lines)
    problem = ""
    if view.last_error_code:
        problem = "\n\n<b>! Последняя ситуация</b>"
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
            problem += "\nСистема: этот контакт пропущен; повтор не выполняется автоматически."
        elif view.last_error_status == DirectMessageTaskStatus.FAILED:
            problem += "\nСистема: эта задача завершена без небезопасного повтора."
        if view.last_error:
            problem += f"\nТехнически: <code>{escape(view.last_error)}</code>"
    admin_summary = (
        "Прошли проверку перед запуском: "
        f"<b>{view.ready_admin_count}/{view.selected_admin_count}</b>"
        if view.selected_admin_count
        else "Администраторы: <b>—</b>"
    )
    if view.selected_admin_count > view.ready_admin_count:
        admin_summary += (
            "\n<i>Неготовые аккаунты исключены до запуска; готовые продолжают работу.</i>"
        )
    if view.blocked_admin_count:
        admin_summary += f"\nИсключены в ходе работы: <b>{view.blocked_admin_count}</b>"
    if view.preflight_failures:
        admin_summary += "\n\n<b>Не прошли проверку</b>\n" + "\n".join(
            f"• {escape(label[:60])} — {escape(reason[:150])}"
            for label, reason in view.preflight_failures[:4]
        )
        if len(view.preflight_failures) > 4:
            admin_summary += f"\nЕщё: {len(view.preflight_failures) - 4}"
    return (
        screen_title(f"Приглашение #{launch.id}", f"Статус: {status}") + "\n\n"
        f"Цель: <code>{escape(launch.destination_ref_snapshot)}</code>\n"
        f"База: <b>{escape(view.list_name)}</b>\n"
        f"Сегмент: <b>{_invite_segment_label(launch.segment_snapshot)}</b>\n"
        f"Приглашение: <b>{escape(view.campaign_name)}</b>\n\n"
        f"{admin_summary}\n\n"
        f"{section('Результат')}\n"
        f"{invite_success_label}: "
        f"<b>{view.invited:,}</b>\n"
        f"· Уже участники: <b>{view.already_participant:,}</b>\n"
        f"◷ В очереди: <b>{view.queued:,}</b> · выполняется: <b>{view.running:,}</b>\n"
        f"›› Пропущено: <b>{view.skipped:,}</b> · "
        f"× ошибки: <b>{view.failed - view.uncertain:,}</b>\n"
        f"? Результат не подтверждён: <b>{view.uncertain:,}</b>"
        f"\nОтменено: <b>{view.cancelled:,}</b> · всего в запуске: <b>{launch.total_tasks:,}</b>"
        f"{timing}"
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
    lines = [
        screen_title("Запуски приглашений", page_summary(total, page, page_size)),
        "",
    ]
    for view in items:
        icon = "?" if view.launch.status == DirectLaunchStatus.FAILED and view.uncertain else "○"
        result_icon = "◇" if view.simulated is True else "✓" if view.simulated is False else "?"
        result_label = (
            "Симуляция"
            if view.simulated is True
            else "Приглашено"
            if view.simulated is False
            else "Режим неизвестен"
        )
        lines.append(
            f"{icon} <b>#{view.launch.id} · {escape(view.campaign_name)}</b>\n"
            f"  {result_icon} {result_label}: "
            f"{view.invited:,} · в работе {view.active:,}\n"
            f"  ошибок {view.failed - view.uncertain:,} · не подтверждено {view.uncertain:,}\n"
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
                text=f"#{view.launch.id} · {view.campaign_name[:31]}",
                callback_data=f"inv:launchview:{view.launch.id}:{page}",
            )
        ]
        for view in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹", callback_data=f"inv:launches:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=f"inv:launches:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="↻ Обновить список", callback_data=f"inv:launches:{page}")],
            [InlineKeyboardButton(text="‹ К приглашениям", callback_data="inv:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_details_menu(view: SubscriberInviteLaunchView, page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✓ Кого пригласили · список",
                callback_data=f"rr:v:i:{view.launch.id}:s:0:{page}:0",
            )
        ]
    ]
    if view.already_participant:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Уже участники · список",
                    callback_data=f"rr:v:i:{view.launch.id}:p:0:{page}:0",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="Все результаты · причины",
                callback_data=f"rr:v:i:{view.launch.id}:a:0:{page}:0",
            )
        ]
    )
    if view.active:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Ⅱ Остановить", callback_data=f"inv:launchcancel:{view.launch.id}:{page}"
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="↻ Обновить", callback_data=f"inv:launchview:{view.launch.id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="‹ К запускам", callback_data=f"inv:launches:{page}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def invite_launch_cancel_menu(launch_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Ⅱ Да, остановить", callback_data=f"inv:launchcancelok:{launch_id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="‹ Назад", callback_data=f"inv:launchview:{launch_id}:{page}"
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
