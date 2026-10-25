"""Control-bot presentation for subscriber databases and personal messages."""

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
from proxima_traffic.direct_actions import DIRECT_SERVICE_ACTIONS, service_action_label
from proxima_traffic.direct_messages import (
    DirectCampaignView,
    DirectLaunchView,
    DirectMessageDashboard,
    SubscriberImportResult,
)
from proxima_traffic.failure_policy import failure_hint, failure_label
from proxima_traffic.models import (
    Account,
    DirectCampaign,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
)


def direct_messages_home_text(dashboard: DirectMessageDashboard) -> str:
    live_successes = max(
        0,
        dashboard.succeeded_tasks
        - dashboard.simulated_succeeded_tasks
        - dashboard.unknown_mode_succeeded_tasks,
    )
    if dashboard.subscriber_count == 0:
        next_step = "Импортируйте первую базу подписчиков."
    elif dashboard.campaign_count == 0:
        next_step = "Создайте ЛС-рассылку: выберите сегмент, отправителей и готовый пост."
    elif dashboard.active_launches:
        next_step = "Откройте «Запуски», чтобы посмотреть прогресс и понятные причины пропусков."
    else:
        next_step = "Можно создать новый запуск или повторно использовать сохранённую задачу."
    lines = [
        screen_title(
            "Личные сообщения",
            f"Текущий режим: {'симуляция' if dashboard.simulated else 'реальная отправка'}",
        ),
        "",
        section("База подписчиков"),
        f"Уникальных людей: <b>{dashboard.subscriber_count:,}</b>",
        f"Premium: <b>{dashboard.premium_count:,}</b>"
        f" · с username: <b>{dashboard.username_count:,}</b>",
        f"Стоп-лист: <b>{dashboard.blacklist_count:,}</b>"
        f" · карантин: <b>{dashboard.quarantined_recipients:,}</b>",
        "",
        section("Сейчас"),
        f"Сохранённых задач: <b>{dashboard.campaign_count:,}</b>"
        f" · запусков в работе: <b>{dashboard.active_launches:,}</b>",
        f"В очереди: <b>{dashboard.queued_tasks:,}</b>"
        f" · выполняется: <b>{dashboard.running_tasks:,}</b>",
        "",
        section("Результаты"),
        f"Отправлено и подтверждено: <b>{live_successes:,}</b>",
        f"Пропущено: <b>{dashboard.skipped_tasks:,}</b>"
        f" · ошибок: <b>{max(0, dashboard.failed_tasks - dashboard.uncertain_tasks):,}</b>",
        f"Не подтверждено: <b>{dashboard.uncertain_tasks:,}</b>",
    ]
    if dashboard.service_succeeded_tasks:
        lines.append(
            f"Настроек диалогов подтверждено: <b>{dashboard.service_succeeded_tasks:,}</b>"
        )
    if dashboard.simulated_succeeded_tasks or dashboard.simulated:
        lines.append(f"Симуляция: <b>{dashboard.simulated_succeeded_tasks:,}</b>")
    if dashboard.service_simulated_tasks:
        lines.append(
            f"Сервисных действий смоделировано: <b>{dashboard.service_simulated_tasks:,}</b>"
        )
    if dashboard.unknown_mode_succeeded_tasks:
        lines.append(
            f"Прежние запуски без режима: <b>{dashboard.unknown_mode_succeeded_tasks:,}</b>"
        )
    lines.extend(
        [
            "",
            section("Следующий шаг"),
            next_step,
            "",
            "Stars автоматически не оплачиваются. "
            "Стоп-лист и защита от повторов действуют во всех запусках.",
        ]
    )
    return "\n".join(lines)


def direct_messages_home_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="＋ Создать ЛС-рассылку", callback_data="dm:new")],
            [InlineKeyboardButton(text="◷ Автоудаление", callback_data="dm:service:new")],
            [
                InlineKeyboardButton(text="База подписчиков", callback_data="dm:lists:0"),
                InlineKeyboardButton(text="↓ Импортировать", callback_data="dm:import"),
            ],
            [
                InlineKeyboardButton(text="Сохранённые задачи", callback_data="dm:campaigns:0"),
                InlineKeyboardButton(text="Запуски", callback_data="dm:launches:0"),
            ],
            [
                InlineKeyboardButton(text="Стоп-лист", callback_data="dm:blacklist"),
                InlineKeyboardButton(text="Приглашения", callback_data="inv:home"),
            ],
            [InlineKeyboardButton(text="◷ Карантин получателей", callback_data="dm:quarantine:0")],
            [
                InlineKeyboardButton(
                    text="✓ Получатели / результаты", callback_data="rr:v:d:0:s:0:0:0"
                ),
                InlineKeyboardButton(text="⌕ Люди в базе", callback_data="rr:v:b:0:a:0:0:0"),
            ],
            [
                InlineKeyboardButton(text="↻ Обновить", callback_data="dm:home"),
                InlineKeyboardButton(text="⌂ Главная", callback_data="menu:home"),
            ],
        ]
    )


def _direct_wizard_title(step: int, service_action: bool = False) -> str:
    if service_action:
        return wizard_title("Автоудаление", step, 5)
    return wizard_title("Новая ЛС-рассылка", step, 6)


def direct_service_action_text() -> str:
    return (
        _direct_wizard_title(1, True) + "\n\n"
        "Выберите настройку личного диалога. Текст и вложения не нужны.\n\n"
        "• Таймер автоудаления: включить на 1, 7 или 30 дней либо выключить.\n"
        "• Запрет копирования и пересылки: включить. Для этого нужен Premium "
        "у выполняющего аккаунта.\n\n"
        "Получателей ищем по @username с проверкой Telegram ID. "
        "Если настройка уже установлена, нового сервисного сообщения не будет. "
        "Получение push-уведомления человеком проверить нельзя."
    )


def direct_service_action_menu() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"◷ Автоудаление · {label}",
                callback_data=f"dm:service:set:auto_delete:{seconds}",
            )
        ]
        for seconds, label in ((86400, "1 день"), (604800, "7 дней"), (2592000, "30 дней"))
    ]
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Выключить автоудаление", callback_data="dm:service:set:auto_delete:0"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Запретить копирование · Premium",
                    callback_data="dm:service:set:protect_content:0",
                )
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_quarantine_text(
    recipients: list[Subscriber], total: int, page: int, *, notice: str | None = None
) -> str:
    parts = [
        screen_title("Карантин получателей ЛС"),
        f"Исключено из новых запусков: <b>{total:,}</b>. Страница {page + 1}.",
        "Повторная проверка только читает свежие данные Telegram через исходный аккаунт отказа. "
        "Сообщения не отправляются, Stars не списываются. Импорт и ожидание не снимают карантин. "
        "Если причина не опровергнута свежими данными, получатель останется исключённым.",
    ]
    if notice:
        parts.append(f"<b>{escape(notice)}</b>")
    for recipient in recipients:
        username = f" · @{escape(recipient.username)}" if recipient.username else ""
        reason = failure_label(recipient.direct_quarantine_code or "gateway_error")
        text = (
            f"<code>{recipient.telegram_user_id}</code>{username}\n"
            f"Причина: <b>{escape(reason)}</b>\n"
            f"Аккаунт отказа: #{recipient.direct_quarantine_account_id or 'неизвестен'}"
        )
        if recipient.direct_quarantine_at:
            text += f"\nС {recipient.direct_quarantine_at:%d.%m.%Y %H:%M} UTC"
        if recipient.direct_quarantine_error:
            text += f"\n{escape(recipient.direct_quarantine_error[:150])}"
        if recipient.direct_quarantine_checked_at:
            text += f"\nПроверка: {recipient.direct_quarantine_checked_at:%d.%m.%Y %H:%M} UTC"
        if recipient.direct_quarantine_check_note:
            text += f"\n{escape(recipient.direct_quarantine_check_note[:180])}"
        parts.append(text)
    if not total:
        parts.append("Получателей в карантине нет.")
    return "\n\n".join(parts)


def direct_quarantine_menu(
    recipients: list[Subscriber], total: int, page: int, page_size: int = 5
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(
            text=f"↻ Проверить {recipient.telegram_user_id}",
            callback_data=f"dm:quarantinecheck:{recipient.id}:{page}",
        )]
        for recipient in recipients
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="‹", callback_data=f"dm:quarantine:{page - 1}"))
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton(text="›", callback_data=f"dm:quarantine:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="↻ Обновить", callback_data=f"dm:quarantine:{page}")])
    rows.append([InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_blacklist_text(total: int) -> str:
    return (
        screen_title("Глобальный стоп-лист", "Запрет по Telegram ID и @username") + "\n\n"
        f"Сейчас в стоп-листе: <b>{total:,}</b> записей.\n\n"
        "Этим людям сервис не будет писать в ЛС, приглашать в канал или группу и не будет "
        "отмечать их в рассылках по чатам или комментариях. Запрет проверяется при "
        "выборе аудитории и ещё раз "
        "перед самой отправкой. Он действует и для ID и @username, которых пока нет в базе."
    )


def direct_blacklist_menu(
    *,
    add_callback: str = "dm:blacklist:add",
    back_callback: str = "dm:home",
    back_text: str = "‹ К личным сообщениям",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="＋ Добавить / импорт TXT", callback_data=add_callback)],
            [
                InlineKeyboardButton(
                    text="⌕ Собрать ID из чата", callback_data="dm:blacklist:parse"
                )
            ],
            [InlineKeyboardButton(text=back_text, callback_data=back_callback)],
        ]
    )


def direct_blacklist_add_prompt_text() -> str:
    return (
        screen_title("Добавить в стоп-лист") + "\n\n"
        "Отправьте <b>файл .txt</b> или вставьте список текстом. "
        "Поддерживаются числовые Telegram ID и <b>@username</b>: по одному на строку, "
        "через пробел, запятую или точку с запятой.\n\n"
        "До 100 000 записей и 2 МБ; TXT в UTF-8 или UTF-16 с BOM. "
        "Повторы пропускаются, регистр username не важен.\n\n"
        "Пример: <code>123456789\n@username</code>\n\n"
        "После импорта можно сразу отправить следующий файл. "
        "Нажмите «Отмена», чтобы вернуться."
    )


def direct_blacklist_parse_prompt_text() -> str:
    return (
        screen_title("Собрать ID из чата", "Добавление в стоп-лист подтверждается отдельно")
        + "\n\n"
        "Отправьте <b>@username</b>, публичную ссылку <code>https://t.me/name</code> или "
        "числовой ID доступного вашим аккаунтам чата.\n\n"
        "Сообщения в чат не отправляются. Публичный чат сначала читается без вступления. "
        "Если для доступа нужно вступить, используется один аккаунт с учётом лимитов. "
        "При заявке на вступление сервис остановится до её одобрения. После чтения бот "
        "отправит файл с уникальными Telegram ID и спросит, добавлять ли их в глобальный "
        "стоп-лист.\n\n"
        "Для приватного чата можно использовать invite-ссылку <code>t.me/+…</code> или его "
        "числовой ID, если он уже доступен выбранному аккаунту Telegram."
    )


def direct_blacklist_parse_mode_text(chat_ref: str) -> str:
    return (
        screen_title("Собрать ID из чата", "Выберите источник") + "\n\n"
        f"Чат: <code>{escape(chat_ref)}</code>\n\n"
        "<b>Авто</b> — сначала открытый список участников; если он скрыт или пуст, собирает "
        "уникальных авторов сообщений.\n"
        "<b>Участники</b> — полный список, если Telegram его предоставляет аккаунту.\n"
        "<b>Авторы сообщений</b> — участники, замеченные в последних сообщениях; это не полный "
        "состав чата."
    )


def direct_blacklist_parse_mode_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="≈ Авто", callback_data="dm:blacklist:parsemode:auto")],
            [
                InlineKeyboardButton(
                    text="○ Участники", callback_data="dm:blacklist:parsemode:members"
                ),
                InlineKeyboardButton(
                    text="↳ Авторы сообщений", callback_data="dm:blacklist:parsemode:messages"
                ),
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="dm:blacklist")],
        ]
    )


def direct_blacklist_parse_progress_text(chat_ref: str) -> str:
    return (
        "<b>Читаю чат через подключённые аккаунты…</b>\n\n"
        f"Чат: <code>{escape(chat_ref)}</code>\n\n"
        "Чтение может занять время для большого чата. Сообщения не отправляются, изменения "
        "доступов не выполняются."
    )


def direct_blacklist_parse_result_text(
    *,
    chat_ref: str,
    account_label: str,
    mode: str,
    count: int,
    fallback_used: bool,
    reached_limit: bool,
    joined_chat: bool,
    scan_limit: int,
) -> str:
    mode_label = "участники" if mode == "members" else "авторы сообщений"
    caveat = (
        f"\n! Достигнут лимит чтения {scan_limit:,} записей; файл может быть неполным."
        if reached_limit
        else ""
    )
    fallback = (
        "\n· Список участников скрыт/пуст: использованы авторы сообщений." if fallback_used else ""
    )
    joined = "\n✓ Аккаунт вступил в чат перед чтением." if joined_chat else ""
    return (
        "<b>ID собраны, но ещё не добавлены в стоп-лист</b>\n\n"
        f"Чат: <code>{escape(chat_ref)}</code>\n"
        f"Аккаунт чтения: <b>{escape(account_label)}</b>\n"
        f"Источник: <b>{mode_label}</b>\n"
        f"Уникальных ID: <b>{count:,}</b>\n"
        f"{fallback}{joined}{caveat}\n\n"
        "Файл отправлен отдельным документом. Добавить эти ID в глобальный стоп-лист?"
    )


def direct_blacklist_parse_result_menu(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="× Добавить все в стоп-лист",
                    callback_data=f"dm:blacklist:parseadd:{token}",
                )
            ],
            [InlineKeyboardButton(text="× Не добавлять", callback_data="dm:blacklist")],
        ]
    )


def subscriber_import_prompt_text(max_upload_bytes: int) -> str:
    return (
        screen_title("Импорт базы подписчиков", "Безопасная построчная обработка файла") + "\n\n"
        "Отправьте документ в формате:\n"
        "• <code>.jsonl</code>\n"
        "• <code>.jsonl.gz</code>\n"
        "• <code>.zip</code> с одной или несколькими JSONL-частями\n\n"
        "Поддерживаемая строка:\n"
        '<code>{"telegram_id":"123", "username":"name", '
        '"is_premium":true}</code>\n\n'
        f"Лимит загружаемого файла: <b>{max_upload_bytes // (1024 * 1024)} МБ</b>; "
        "после распаковки — 256 МБ или 2 000 000 строк. Обработка идёт построчно, "
        "дубликаты объединяются по Telegram ID. Каждый новый файл дополняет одну общую "
        "базу и обновляет данные уже известных людей. Старые данные не удаляются."
    )


def subscriber_import_result_text(result: SubscriberImportResult) -> str:
    subscriber_list = result.subscriber_list
    issue_note = ""
    if result.invalid:
        issue_note = (
            f"\n! Некорректных строк: <b>{result.invalid:,}</b>. "
            "Они пропущены без остановки остальных данных."
        )
        examples = [
            f"• {escape(str(issue.source))}:{issue.line_number} — {escape(str(issue.reason))}"
            for issue in result.issues[:3]
        ]
        if examples:
            issue_note += "\n<b>Примеры ошибок:</b>\n" + "\n".join(examples)
    return (
        screen_title("База импортирована", "Результат обработки") + "\n\n"
        f"База: <b>{escape(subscriber_list.name)}</b>\n"
        f"Строк прочитано: <b>{result.total_lines:,}</b>\n"
        f"Корректных записей: <b>{result.valid_records:,}</b>\n"
        f"Добавлено в эту базу: <b>{result.added_members:,}</b>\n"
        f"Новых людей глобально: <b>{result.inserted_profiles:,}</b>\n"
        f"Обновлено профилей: <b>{result.updated_profiles:,}</b>\n"
        f"Дубликатов в файле: <b>{result.duplicates:,}</b>\n\n"
        "<b>Готовые сегменты</b>\n"
        f"Всего: <b>{subscriber_list.member_count:,}</b>\n"
        f"☆ Premium: <b>{subscriber_list.premium_count:,}</b>\n"
        f"Обычные: <b>{subscriber_list.non_premium_count:,}</b>\n"
        f"С username: <b>{subscriber_list.username_count:,}</b>{issue_note}"
    )


def subscriber_import_progress_text(
    *, total_lines: int, valid_records: int, added_members: int, updated_profiles: int,
    duplicates: int, invalid: int,
) -> str:
    return (
        screen_title("Импорт общей базы", "Файл обрабатывается") + "\n\n"
        f"Прочитано строк: <b>{total_lines:,}</b>\n"
        f"Корректных: <b>{valid_records:,}</b>\n"
        f"Новых в базе: <b>{added_members:,}</b> · обновлено: <b>{updated_profiles:,}</b>\n"
        f"Дубли: <b>{duplicates:,}</b> · ошибки: <b>{invalid:,}</b>\n\n"
        "Можно продолжать пользоваться ботом. Итог появится здесь после завершения."
    )


def subscriber_import_result_menu(list_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="↗ Создать ЛС-рассылку",
                    callback_data=f"dm:new:list:{list_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="○ Пригласить в канал / группу",
                    callback_data=f"inv:new:fromlist:{list_id}",
                )
            ],
            [InlineKeyboardButton(text="Открыть базу", callback_data=f"dm:list:{list_id}:0")],
            [InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")],
        ]
    )


def subscriber_lists_text(
    items: list[SubscriberList], total: int, page: int, page_size: int
) -> str:
    lines = [
        screen_title("Базы подписчиков", page_summary(total, page, page_size)),
        "",
    ]
    for item in items:
        state = {
            SubscriberListStatus.READY: "●",
            SubscriberListStatus.IMPORTING: "↻",
            SubscriberListStatus.FAILED: "×",
        }[item.status]
        lines.append(
            f"{state} <b>{escape(item.name)}</b>\n"
            f"  подписчиков {item.member_count:,} · ☆ Premium {item.premium_count:,}"
            f" · с username {item.username_count:,}\n"
        )
    if not items:
        lines.append("<b>С чего начать</b>\nИмпортируйте первый JSONL-файл.")
    else:
        lines.append(
            "\n<i>Откройте базу, чтобы сразу создать ЛС-рассылку или приглашение в свой канал.</i>"
        )
    return "\n".join(lines)


def subscriber_lists_menu(
    items: list[SubscriberList], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"{item.name[:35]} · {item.member_count:,}",
                callback_data=f"dm:list:{item.id}:{page}",
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹", callback_data=f"dm:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=f"dm:lists:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="↓ Импортировать базу", callback_data="dm:import")],
            [InlineKeyboardButton(text="↻ Обновить список", callback_data=f"dm:lists:{page}")],
            [InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def subscriber_list_details_text(item: SubscriberList) -> str:
    status = {
        SubscriberListStatus.READY: "готова",
        SubscriberListStatus.IMPORTING: "импортируется",
        SubscriberListStatus.FAILED: "ошибка импорта",
    }[item.status]
    error = (
        f"\n\n! Последний импорт: {escape(item.last_error)}. "
        "Сохранённые подписчики доступны. Попробуйте исправленный файл."
        if item.last_error
        else ""
    )
    return (
        screen_title(escape(item.name), f"Состояние: {status}") + "\n\n"
        f"Всего: <b>{item.member_count:,}</b>\n"
        f"☆ Premium: <b>{item.premium_count:,}</b>\n"
        f"Обычные: <b>{item.non_premium_count:,}</b>\n"
        f"С username: <b>{item.username_count:,}</b>\n"
        f"Только ID: <b>{max(0, item.member_count - item.username_count):,}</b>\n\n"
        "<b>Последний импорт</b>\n"
        f"Прочитано: <b>{item.last_import_total:,}</b> · добавлено: "
        f"<b>{item.last_import_added:,}</b> · обновлено: <b>{item.last_import_updated:,}</b>\n"
        f"Дубли: <b>{item.last_import_duplicates:,}</b> · ошибки: "
        f"<b>{item.last_import_invalid:,}</b>{error}"
    )


def subscriber_list_details_menu(item: SubscriberList, page: int) -> InlineKeyboardMarkup:
    rows = []
    if item.status == SubscriberListStatus.READY and item.member_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text="↗ Создать ЛС-рассылку",
                    callback_data=f"dm:new:list:{item.id}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="○ Пригласить в канал / группу",
                    callback_data=f"inv:new:fromlist:{item.id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="↓ Дополнить базу", callback_data="dm:import")],
            [InlineKeyboardButton(text="⌕ Люди / поиск в общей базе",
                                  callback_data="rr:v:b:0:a:0:0:0")],
            [InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def subscriber_list_delete_menu(list_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="× Да, удалить",
                    callback_data=f"dm:listdelok:{list_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="‹ Отмена", callback_data=f"dm:list:{list_id}:{page}")],
        ]
    )


def direct_list_picker_text(total: int, page: int, page_size: int) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        _direct_wizard_title(1) + "\n\n"
        "Выберите базу подписчиков. Один Telegram ID получит не больше одного задания "
        "в этом запуске.\n\n"
        f"Доступных баз: <b>{total:,}</b> · страница <b>{page + 1}/{pages}</b>"
    )


def direct_list_picker(
    items: list[SubscriberList],
    total: int,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"{item.name[:32]} · {item.member_count:,}",
                callback_data=f"dm:new:list:{item.id}",
            )
        ]
        for item in items
        if item.status == SubscriberListStatus.READY and item.member_count > 0
    ]
    pages = max(1, ceil(total / page_size))
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹‹", callback_data="dm:new:lists:0"))
        navigation.append(InlineKeyboardButton(text="‹", callback_data=f"dm:new:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="›", callback_data=f"dm:new:lists:{page + 1}"))
        navigation.append(
            InlineKeyboardButton(text="››", callback_data=f"dm:new:lists:{pages - 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="× Отмена", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_segment_text(item: SubscriberList, *, service_action: bool = False) -> str:
    return (
        _direct_wizard_title(2, service_action) + "\n\n"
        f"Общая база: <b>{item.member_count:,}</b> человек\n\n"
        f"☆ Premium: <b>{item.premium_count:,}</b>\n"
        f"Обычные: <b>{item.non_premium_count:,}</b>\n"
        "Статус Premium неизвестен: "
        f"<b>{max(0, item.member_count - item.premium_count - item.non_premium_count):,}</b>\n\n"
        "Выберите сегмент. «Все» включает людей с неизвестным статусом Premium."
    )


def direct_segment_menu(list_id: int, *, service_action: bool = False) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="○ Всем", callback_data=f"dm:new:seg:{list_id}:all")],
            [
                InlineKeyboardButton(
                    text="☆ Только Premium", callback_data=f"dm:new:seg:{list_id}:premium"
                )
            ],
            *(
                []
                if service_action
                else [
                    [
                        InlineKeyboardButton(
                            text="Обычные", callback_data=f"dm:new:seg:{list_id}:non_premium"
                        )
                    ]
                ]
            ),
            [InlineKeyboardButton(text="× Отмена", callback_data="dm:home")],
        ]
    )


def direct_limit_text(segment: SubscriberSegment, *, service_action: bool = False) -> str:
    segment_label = {
        SubscriberSegment.ALL: "все",
        SubscriberSegment.PREMIUM: "только Premium",
        SubscriberSegment.NON_PREMIUM: "без Premium",
    }[segment]
    return (
        _direct_wizard_title(3, service_action) + "\n\n"
        f"Сегмент: <b>{segment_label}</b>.\n"
        "Введите количество получателей от 1 до 1 000 000. Можно написать "
        "<code>все</code> — будет взят весь доступный сегмент."
        + ("\nДля сервисных действий подходят только люди с @username." if service_action else "")
    )


def direct_cooldown_text(*, service_action: bool = False) -> str:
    if service_action:
        return (
            _direct_wizard_title(4, True) + "\n\n"
            "Как долго не обращаться к одному человеку повторно? "
            "Защита общая для ЛС и сервисных действий.\n\n"
            "Интервалы действий каждого аккаунта: случайные 60–600 секунд. "
            "После FloodWait — минимум 500 секунд или дольше по требованию Telegram."
        )
    return (
        _direct_wizard_title(4) + "\n\n"
        "Как долго не писать одному человеку повторно? Защита действует между всеми "
        "ЛС-рассылками и параллельными запусками.\n\n"
        "<i>30 дней — безопасный вариант по умолчанию.</i>"
    )


def direct_cooldown_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Без паузы", callback_data="dm:new:cooldown:0"),
                InlineKeyboardButton(text="7 дней", callback_data="dm:new:cooldown:7"),
            ],
            [
                InlineKeyboardButton(text="✓ 30 дней", callback_data="dm:new:cooldown:30"),
                InlineKeyboardButton(text="90 дней", callback_data="dm:new:cooldown:90"),
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="dm:home")],
        ]
    )


def direct_account_picker_text(
    selected_count: int,
    total: int,
    page: int,
    page_size: int,
    *,
    service_action: bool = False,
    restricted_count: int = 0,
) -> str:
    pages = max(1, ceil(total / page_size))
    text = (
        _direct_wizard_title(5, service_action) + "\n\n"
        f"Выбрано отправителей: <b>{selected_count}</b>.\n"
        "Задания распределяются автоматически. Используются только готовые аккаунты "
        "со свободным дневным лимитом; паузы и ограничения Telegram соблюдаются."
        + (
            "\nКаждый человек закрепляется за одним выбранным аккаунтом. "
            "При ошибке или паузе аккаунт автоматически не заменяется."
            if service_action
            else ""
        )
        + f"\n\nГотовых аккаунтов: <b>{total}</b> · страница <b>{page + 1}/{pages}</b>"
    )
    if restricted_count > 0:
        text += (
            f"\n\n⚠️ <b>Внимание:</b> {restricted_count} аккаунт(ов) имеют спамблок Telegram для ЛС (отмечены ⛔). "
            "Холодные сообщения с них будут отклонены Telegram (PeerFloodError). Выбирайте аккаунты без ограничений."
        )
    return text


def direct_account_picker(
    accounts: list[Account],
    selected_ids: set[int],
    *,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    total = len(accounts)
    pages = max(1, ceil(total / page_size))
    page = max(0, min(page, pages - 1))
    start = page * page_size
    visible_accounts = accounts[start : start + page_size]
    rows = []
    for account in visible_accounts:
        restricted = getattr(account, "spambot_status", None) == "active_restriction"
        badge = " ⛔ [Спамблок]" if restricted else ""
        label = f"{'✓' if int(account.id) in selected_ids else '○'} {account.label[:22]}{badge}"
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"dm:new:account:{account.id}",
                )
            ]
        )
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="‹‹", callback_data="dm:new:accounts:page:0"))
        navigation.append(
            InlineKeyboardButton(text="‹", callback_data=f"dm:new:accounts:page:{page - 1}")
        )
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="›", callback_data=f"dm:new:accounts:page:{page + 1}")
        )
        navigation.append(
            InlineKeyboardButton(text="››", callback_data=f"dm:new:accounts:page:{pages - 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(text="✓ Выбрать всех", callback_data="dm:new:accounts:all"),
                InlineKeyboardButton(text="× Очистить", callback_data="dm:new:accounts:none"),
            ],
            [InlineKeyboardButton(text="› Продолжить", callback_data="dm:new:accounts:done")],
            [InlineKeyboardButton(text="× Отмена", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_content_prompt_text() -> str:
    return (
        _direct_wizard_title(6) + "\n\n"
        "Отправьте готовый пост: текст, фото, видео, документ, голосовое или видеокружок. "
        "У видеокружка подписи быть не может; для остальных вложений она допустима. "
        "Форматирование Telegram и Spintax сохраняются.\n\n"
        "<b>Для понятного ЛС:</b> коротко представьтесь, объясните повод обращения, "
        "сформулируйте одно предложение и дайте человеку простой способ отказаться. "
        "Проверьте текст перед запуском в следующем шаге.\n\n"
        "<i>Перед созданием заданий будет отдельное подтверждение получателей, "
        "лимита и согласия.</i>"
    )


def direct_campaign_confirmation_text(view: DirectCampaignView) -> str:
    campaign = view.campaign
    if campaign.operation_kind in DIRECT_SERVICE_ACTIONS:
        return direct_service_confirmation_text(view)
    segment = {
        SubscriberSegment.ALL: "все подписчики",
        SubscriberSegment.PREMIUM: "только Premium",
        SubscriberSegment.NON_PREMIUM: "без Premium",
    }[campaign.segment]
    preview = campaign.primary_text
    if len(preview) > 500:
        preview = preview[:497] + "…"
    preview = escape(preview)
    media_kind = {
        "photo": "фото",
        "video": "видео",
        "document": "документ",
        "voice": "голосовое",
        "video_note": "видеокружок",
    }.get(campaign.primary_media_kind or "", campaign.primary_media_kind or "")
    media = f"\nВложение: <b>{escape(media_kind)}</b>" if campaign.primary_media_path else ""
    content_preview = preview or "<i>без подписи — будет отправлено только вложение</i>"
    return (
        screen_title("Проверьте ЛС-рассылку", "Перед созданием задач") + "\n\n"
        f"Название: <b>{escape(campaign.name)}</b>\n"
        f"База: <b>{escape(view.list_name)}</b>\n"
        f"Сегмент: <b>{segment}</b>\n"
        f"Запрошено: <b>{campaign.recipient_limit:,}</b>\n"
        f"Сейчас доступно: <b>{view.eligible_count:,}</b>\n"
        f"С username: <b>{view.username_count:,}</b>\n"
        f"На паузе повтора: <b>{view.recent_count:,}</b>\n"
        f"Уже заняты другим запуском: <b>{view.reserved_count:,}</b>\n"
        f"Отправителей: <b>{len(view.account_ids)}</b>\n"
        f"Не писать повторно: <b>{campaign.contact_cooldown_days} дн.</b>{media}\n\n"
        f"<blockquote>{content_preview}</blockquote>\n"
        "Предпросмотр можно отправить себе перед запуском. Вариант Spintax при реальной "
        "отправке может отличаться.\n"
        "! Запускайте только по людям, которые согласились получать ваши сообщения. "
        "Stars за платные сообщения автоматически не списываются."
    )


def direct_campaign_confirmation_menu(
    campaign_id: int, launch_token: str, *, service_action: bool = False
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            *(
                []
                if service_action
                else [
                    [
                        InlineKeyboardButton(
                            text="Предпросмотр себе", callback_data=f"dm:preview:{campaign_id}"
                        )
                    ]
                ]
            ),
            [
                InlineKeyboardButton(
                    text="✓ Подтверждаю согласие и запускаю",
                    callback_data=f"dm:launch:{campaign_id}:{launch_token}",
                )
            ],
            [InlineKeyboardButton(text="× Не запускать", callback_data="dm:home")],
        ]
    )


def direct_campaigns_text(
    items: list[DirectCampaign], total: int, page: int, page_size: int
) -> str:
    lines = [
        screen_title("Сохранённые задачи ЛС", page_summary(total, page, page_size)),
        "",
    ]
    for campaign in items:
        segment = {
            SubscriberSegment.ALL: "все",
            SubscriberSegment.PREMIUM: "Premium",
            SubscriberSegment.NON_PREMIUM: "обычные",
        }[campaign.segment]
        lines.append(
            f"{'●' if campaign.enabled else 'Ⅱ'} <b>{escape(campaign.name)}</b>\n"
            f"{
                escape(service_action_label(campaign.operation_kind, campaign.auto_delete_seconds))
            }\n"
            f"  {segment} · до {campaign.recipient_limit:,} получателей · "
            f"повтор через {campaign.contact_cooldown_days} дн.\n"
        )
    if not items:
        lines.append(
            "<b>С чего начать</b>\n"
            "Создайте ЛС-рассылку — после подтверждения её можно будет запускать повторно."
        )
    else:
        lines.append(
            "\n<i>Откройте сохранённую задачу, чтобы запустить её с новой проверкой доступа.</i>"
        )
    return "\n".join(lines)


def direct_campaigns_menu(
    items: list[DirectCampaign], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"{item.name[:38]}", callback_data=f"dm:campaign:{item.id}:{page}"
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="‹", callback_data=f"dm:campaigns:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="›", callback_data=f"dm:campaigns:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.extend(
        [
            [InlineKeyboardButton(text="↗ Создать ЛС-рассылку", callback_data="dm:new")],
            [InlineKeyboardButton(text="↻ Обновить список", callback_data=f"dm:campaigns:{page}")],
            [InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_campaign_details_menu(
    campaign_id: int, page: int, *, service_action: bool = False
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            *(
                []
                if service_action
                else [
                    [
                        InlineKeyboardButton(
                            text="Предпросмотр себе", callback_data=f"dm:preview:{campaign_id}"
                        )
                    ]
                ]
            ),
            [
                InlineKeyboardButton(
                    text="↗ Запустить снова", callback_data=f"dm:campaignrun:{campaign_id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="‹ К сохранённым задачам", callback_data=f"dm:campaigns:{page}"
                )
            ],
        ]
    )


def direct_launches_text(
    items: list[DirectLaunchView], total: int, page: int, page_size: int
) -> str:
    lines = [
        screen_title("Запуски ЛС", page_summary(total, page, page_size)),
        "",
    ]
    icons = {
        DirectLaunchStatus.QUEUED: "◷",
        DirectLaunchStatus.RUNNING: "↻",
        DirectLaunchStatus.SUCCEEDED: "✓",
        DirectLaunchStatus.PARTIAL: "◷",
        DirectLaunchStatus.FAILED: "×",
        DirectLaunchStatus.CANCELLED: "Ⅱ",
    }
    for view in items:
        icon = (
            "?"
            if view.launch.status == DirectLaunchStatus.FAILED and view.uncertain
            else icons[view.launch.status]
        )
        result_label = (
            "◇ Симуляция"
            if view.simulated is True
            else "✓ Настройка подтверждена"
            if view.simulated is False
            and view.launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
            else "✓ Отправлено"
            if view.simulated is False
            else "? Режим неизвестен"
        )
        lines.append(
            f"{icon} <b>{escape(view.campaign_name)}</b> · #{view.launch.id}\n"
            f"  {result_label}: "
            f"{view.succeeded} · в работе {view.active}\n"
            f"  пропущено {view.skipped} · ошибок {max(0, view.failed - view.uncertain)} "
            f"· не подтверждено {view.uncertain}\n"
        )
    if not items:
        lines.append("Запусков пока нет. После старта здесь появятся прогресс и причины пропусков.")
    return "\n".join(lines)


def direct_launches_menu(
    items: list[DirectLaunchView], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"#{view.launch.id} · {view.campaign_name[:30]}",
                callback_data=f"dm:launchview:{view.launch.id}:{page}",
            )
        ]
        for view in items
    ]
    pages = max(1, ceil(total / page_size))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="‹", callback_data=f"dm:launches:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="›", callback_data=f"dm:launches:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.extend(
        [
            [InlineKeyboardButton(text="↻ Обновить список", callback_data=f"dm:launches:{page}")],
            [InlineKeyboardButton(text="‹ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_launch_details_text(view: DirectLaunchView) -> str:
    launch = view.launch
    if launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS:
        return direct_service_launch_text(view)
    all_senders_restricted = bool(view.sender_states) and all(
        sender.reason == "account_restricted" for sender in view.sender_states
    )
    status = {
        DirectLaunchStatus.QUEUED: "готовится к отправке",
        DirectLaunchStatus.RUNNING: "выполняется",
        DirectLaunchStatus.SUCCEEDED: "✓ завершён",
        DirectLaunchStatus.PARTIAL: "завершён частично",
        DirectLaunchStatus.FAILED: "× не выполнен",
        DirectLaunchStatus.CANCELLED: "Ⅱ остановлен",
    }[launch.status]
    if launch.status == DirectLaunchStatus.FAILED and view.uncertain:
        status = "? завершён, есть неподтверждённые отправки"
    if (
        view.queued
        and not view.running
        and view.next_scheduled_for is not None
        and view.next_scheduled_for > datetime.now(UTC)
    ):
        status = "◷ ожидает доступности отправителей"
    issue = ""
    if view.last_error_code in {
        "account_pacing",
        "account_operation_busy",
    }:
        issue = (
            "\n\n<i>◷ Часть задач ждёт безопасного окна аккаунта. "
            "Это штатная пауза: очередь продолжится сама, а доступные аккаунты "
            "будут использоваться без ожидания.</i>"
        )
    elif view.last_error_code:
        issue = "\n\n<b>! Требуется внимание</b>"
        if view.last_error_account_label:
            issue += f"\nАккаунт: <b>{escape(view.last_error_account_label)}</b>"
        if view.last_error_recipient:
            issue += f"\nПолучатель: <code>{escape(view.last_error_recipient)}</code>"
        issue += f"\nПричина: <b>{escape(failure_label(view.last_error_code))}</b>"
        if view.last_error:
            issue += f"\nДетали: {escape(view.last_error[:180])}"
        hint = failure_hint(view.last_error_code)
        if hint:
            issue += f"\nЧто делать: {escape(hint)}"
        if view.last_error_status == DirectMessageTaskStatus.QUEUED:
            if all_senders_restricted:
                issue += "\n<i>Все выбранные отправители на паузе; очередь ожидает.</i>"
            else:
                issue += (
                    "\n<i>Попытка перенесена; доступные отправители могут продолжать работу.</i>"
                )
        elif view.last_error_status == DirectMessageTaskStatus.SKIPPED:
            if view.replacement_tasks:
                issue += (
                    "\n<i>Этот получатель пропущен; сервис добавил следующего "
                    "доступного человека из той же базы.</i>"
                )
            else:
                issue += "\n<i>Этот получатель пропущен; остальные задачи продолжаются.</i>"
        elif view.last_error_status == DirectMessageTaskStatus.FAILED:
            issue += "\n<i>Задача завершена без небезопасного повтора.</i>"
    exclusions: list[str] = []
    if launch.excluded_recent:
        exclusions.append(f"не повторяем сейчас: <b>{launch.excluded_recent:,}</b>")
    if launch.excluded_reserved:
        exclusions.append(f"заняты в другом запуске: <b>{launch.excluded_reserved:,}</b>")
    excluded_text = (
        "\n\n<i>При старте не включены — " + " · ".join(exclusions) + ".</i>" if exclusions else ""
    )
    replacement_text = (
        f"\nЗаменено недоступных получателей: <b>{view.replacement_tasks:,}</b>"
        if view.replacement_tasks
        else ""
    )
    timing_text = (
        f"\n◷ Следующая проверка очереди: <b>{_direct_date_time(view.next_scheduled_for)}</b>"
        if view.next_scheduled_for is not None and view.active
        else ""
    )
    if view.last_success_at:
        success_label = (
            "Последняя симуляция"
            if view.simulated is True
            else "Последняя подтверждённая отправка"
            if view.simulated is False
            else "Последняя успешная задача старого запуска"
        )
        timing_text += f"\n{success_label}: {_direct_date_time(view.last_success_at)}"
    sender_text = ""
    if view.sender_states:
        sender_lines = []
        for sender in view.sender_states[:6]:
            reason = failure_label(sender.reason) if sender.reason else "готов"
            until = f" до {_direct_date_time(sender.resume_at)}" if sender.resume_at else ""
            blocked = (
                f"; нет маршрута к {sender.blocked_recipients} получ."
                if sender.blocked_recipients
                else ""
            )
            sender_lines.append(
                f"• <b>{escape(sender.label[:35])}</b>: ✓ {sender.succeeded} · "
                f"{escape(reason)}{until}{blocked}"
            )
        if len(view.sender_states) > 6:
            sender_lines.append(f"Ещё аккаунтов: {len(view.sender_states) - 6}")
        sender_text = "\n\n<b>Отправители · текущее состояние</b>\n" + "\n".join(sender_lines)
        local_restrictions = [
            sender
            for sender in view.sender_states
            if sender.reason == "account_restricted"
            and sender.cooldown_reason in {"account_restricted", "peer_flood"}
        ]
        if local_restrictions and view.active:
            sender_text += (
                "\n<i>После отказа Telegram действует защитная пауза программы. "
                "Указанное время — следующая проверка, а не подтверждённый срок "
                "снятия ограничения Telegram.</i>"
            )
            if any(
                sender.spambot_status == "free" and sender.spambot_checked_at is not None
                for sender in local_restrictions
            ):
                sender_text += (
                    "\n<i>@SpamBot сообщил об отсутствии общих ограничений. "
                    "Это не подтверждает доступность отправки после PeerFloodError "
                    "и не снимает защитную паузу.</i>"
                )
    history_text = ""
    if view.recent_failures:
        history_text = "\n\n<b>Последние причины сбоев</b>\n" + "\n".join(
            f"• Акк. #{event.get('account_id')}: "
            f"{escape(str(event.get('error') or failure_label(str(event.get('code'))))[:150])}"
            for event in view.recent_failures
        )
    issue_text = ""
    if view.issues:
        issue_lines = []
        for issue_status, code, count in view.issues:
            if (
                code
                in {
                    "delivery_unconfirmed",
                    "delivery_outcome_unknown",
                    "cancelled_outcome_unknown",
                    "recipient_identity_mismatch",
                }
                and issue_status == DirectMessageTaskStatus.FAILED
            ):
                outcome = "не подтверждено"
            elif issue_status == DirectMessageTaskStatus.SKIPPED:
                outcome = "пропущено"
            elif issue_status == DirectMessageTaskStatus.QUEUED:
                outcome = "ожидает"
            else:
                outcome = "ошибка"
            issue_lines.append(
                f"• <b>{count:,}</b> · {escape(failure_label(code))} <i>({outcome})</i>"
            )
        issue_text = "\n\n<b>Основные причины</b>\n" + "\n".join(issue_lines)
    message_preview = _direct_message_preview(launch.primary_text_snapshot)
    success_label = (
        "Смоделировано"
        if view.simulated is True
        else "✓ Отправлено и подтверждено"
        if view.simulated is False
        else "? Успешных задач (режим запуска не сохранён)"
    )
    return (
        screen_title(f"Запуск #{launch.id}", f"Состояние: {status}") + "\n"
        f"Задача: <b>{escape(view.campaign_name)}</b>\n"
        f"База: <b>{escape(view.list_name)}</b>\n\n"
        f"{section('Сообщение людям')}\n"
        f"{message_preview}\n\n"
        f"{section('Результат')}\n"
        f"Цель: <b>{launch.total_tasks:,}</b> отправок · "
        f"<b>{min(100, round(100 * view.succeeded / max(1, launch.total_tasks)))}%</b>\n"
        f"{success_label}: "
        f"<b>{view.succeeded:,}</b>\n"
        f"В очереди и работе: <b>{view.active:,}</b>\n"
        f"›› Пропущено: <b>{view.skipped:,}</b>\n"
        f"× Ошибок: <b>{max(0, view.failed - view.uncertain):,}</b>\n"
        f"? Результат не подтверждён: <b>{view.uncertain:,}</b>\n"
        f"Ⅱ Отменено: <b>{view.cancelled:,}</b>"
        f"{replacement_text}{timing_text}{sender_text}{excluded_text}"
        f"{issue_text}{issue}{history_text}"
    )


def direct_service_confirmation_text(view: DirectCampaignView) -> str:
    campaign = view.campaign
    action = escape(service_action_label(campaign.operation_kind, campaign.auto_delete_seconds))
    segment = (
        "только Premium" if campaign.segment == SubscriberSegment.PREMIUM else "все с @username"
    )
    accounts = "\n".join(f"• {escape(label)}" for label in view.account_labels[:12])
    if len(view.account_ids) > 12:
        accounts += f"\nЕщё аккаунтов: {len(view.account_ids) - 12}"
    return (
        screen_title("Проверьте сервисное действие", "Перед созданием задач") + "\n\n"
        f"Действие: <b>{action}</b>\n"
        f"База: <b>{escape(view.list_name)}</b>\n"
        f"Получатели: <b>{segment}</b>\n"
        f"Запрошено: <b>{campaign.recipient_limit:,}</b>"
        f" · доступно: <b>{view.eligible_count:,}</b>\n"
        f"На паузе повтора: <b>{view.recent_count:,}</b> · занято: <b>{view.reserved_count:,}</b>\n"
        f"Повторное обращение через: <b>{campaign.contact_cooldown_days} дн.</b>\n\n"
        f"<b>Выбранные аккаунты · {len(view.account_ids)}</b>\n{accounts}\n\n"
        "Интервалы: <b>60–600 секунд</b> на аккаунт, случайные, включая первое действие.\n"
        "FloodWait: минимум <b>500 секунд</b>; более долгие требования Telegram соблюдаются.\n"
        "Автозамены аккаунтов нет. Общий стоп-лист действует.\n\n"
        "Текст и вложения не отправляются. Уже установленная настройка будет пропущена. "
        "В отчёте отдельно покажем подтверждённые настройки и сервисные сообщения. "
        "Push-уведомление и прочтение человеком проверить нельзя."
    )


def direct_service_launch_text(view: DirectLaunchView) -> str:
    launch = view.launch
    action = escape(
        service_action_label(launch.operation_kind_snapshot, launch.auto_delete_seconds_snapshot)
    )
    status = {
        DirectLaunchStatus.QUEUED: "в очереди",
        DirectLaunchStatus.RUNNING: "выполняется",
        DirectLaunchStatus.SUCCEEDED: "завершён",
        DirectLaunchStatus.PARTIAL: "завершён частично",
        DirectLaunchStatus.FAILED: "завершён без подтверждённых изменений",
        DirectLaunchStatus.CANCELLED: "остановлен",
    }[launch.status]
    label = (
        "Смоделировано"
        if view.simulated is True
        else "Настройка подтверждена"
        if view.simulated is False
        else "Успешные задачи · режим не сохранён"
    )
    lines = [
        screen_title(f"Автоудаление · запуск #{launch.id}", status),
        f"Задача: <b>{escape(view.campaign_name)}</b>",
        f"Действие: <b>{action}</b>",
        f"Получателей в очереди запуска: <b>{launch.total_tasks:,}</b>",
        "",
        f"{label}: <b>{view.succeeded:,}</b>",
    ]
    if view.simulated is False:
        lines.extend(
            [
                f"Сервисное сообщение проверено в диалоге: <b>{view.service_notifications:,}</b>",
                "Настройка подтверждена, сервисное сообщение не подтверждено: "
                f"<b>{max(0, view.succeeded - view.service_notifications):,}</b>",
            ]
        )
    lines.extend(
        [
            f"В очереди: <b>{view.queued:,}</b> · выполняется: <b>{view.running:,}</b>",
            f"Пропущено: <b>{view.skipped:,}</b> · "
            f"ошибок: <b>{max(0, view.failed - view.uncertain):,}</b>",
            f"Результат действия неизвестен: <b>{view.uncertain:,}</b>"
            f" · отменено: <b>{view.cancelled:,}</b>",
        ]
    )
    if view.next_scheduled_for is not None and view.queued:
        lines.append(f"Ближайшая задача: <b>{_direct_date_time(view.next_scheduled_for)}</b>")
    if view.issues:
        lines.extend(["", "<b>Причины и паузы</b>"])
        lines.extend(
            f"• {count:,} · {escape(failure_label(code))}" for _, code, count in view.issues
        )
    if view.last_error and view.last_error_code != "service_already_set":
        lines.extend(["", f"Последняя причина: {escape(view.last_error[:200])}"])
    lines.extend(
        [
            "",
            "Интервалы 60–600 секунд · FloodWait от 500 секунд. "
            "Задачи ждут свой аккаунт; автозамены нет.",
            "Сервисное сообщение в диалоге не подтверждает получение push-уведомления "
            "или прочтение.",
        ]
    )
    return "\n".join(lines)


def _direct_message_preview(text: str, *, limit: int = 350) -> str:
    """Render a safe, readable snapshot without overflowing a Telegram card."""

    normalized = text.strip() or "(пустой текст)"
    if len(normalized) > limit:
        normalized = normalized[: limit - 1].rstrip() + "…"
    return escape(normalized)


def _direct_date_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%d.%m %H:%M UTC")


def direct_launch_details_menu(view: DirectLaunchView, page: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=(
                    "✓ Результаты действий · список"
                    if view.launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
                    else "✓ Кому отправлено · список"
                ),
                callback_data=f"rr:v:d:{view.launch.id}:s:0:{page}:0",
            )
        ],
        [
            InlineKeyboardButton(
                text="↻ Обновить", callback_data=f"dm:launchview:{view.launch.id}:{page}"
            )
        ],
    ]
    if view.launch.status in {DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING}:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Ⅱ Остановить", callback_data=f"dm:launchcancel:{view.launch.id}:{page}"
                )
            ]
        )
    if view.last_error_code == "proxy_unavailable":
        rows.append(
            [
                InlineKeyboardButton(
                    text="⇄ Проверить прокси",
                    callback_data="menu:proxies:0",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="‹ К запускам", callback_data=f"dm:launches:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_launch_cancel_menu(launch_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Ⅱ Да, остановить", callback_data=f"dm:launchcancelok:{launch_id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="‹ Не останавливать", callback_data=f"dm:launchview:{launch_id}:{page}"
                )
            ],
        ]
    )


def direct_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="× Отмена", callback_data="dm:home")]]
    )


for _factory_name, _factory in tuple(globals().items()):
    if (
        not _factory_name.startswith("_")
        and callable(_factory)
        and _factory_name.endswith(("menu", "picker", "confirmation", "cancel"))
    ):
        globals()[_factory_name] = themed_keyboard(_factory)
