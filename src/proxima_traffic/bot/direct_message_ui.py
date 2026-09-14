"""Control-bot presentation for subscriber databases and personal messages."""

from __future__ import annotations

from html import escape
from math import ceil

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from proxima_traffic.bot.ui import screen_title, section, themed_keyboard
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
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
)


def direct_messages_home_text(dashboard: DirectMessageDashboard) -> str:
    if dashboard.list_count == 0:
        next_step = "📥 Импортируйте первую базу подписчиков."
    elif dashboard.campaign_count == 0:
        next_step = "✉️ Создайте ЛС-рассылку: выберите базу, сегмент, отправителей и готовый пост."
    elif dashboard.active_launches:
        next_step = "📊 Откройте «Запуски», чтобы посмотреть прогресс и понятные причины пропусков."
    else:
        next_step = "🚀 Можно создать новый запуск или повторно использовать готовую заготовку."
    return (
        screen_title(
            "Личные сообщения",
            "Базы подписчиков, готовые посты и контролируемая отправка.",
        )
        + "\n\n"
        f"{section('Следующий шаг')}\n"
        f"{next_step}\n\n"
        f"{section('База подписчиков')}\n"
        f"📂 Списков: <b>{dashboard.list_count:,}</b>\n"
        f"👤 Уникальных людей: <b>{dashboard.subscriber_count:,}</b>\n"
        f"⭐ Premium: <b>{dashboard.premium_count:,}</b>\n"
        f"🔗 С username: <b>{dashboard.username_count:,}</b>\n\n"
        f"🚫 В глобальном стоп-листе: <b>{dashboard.blacklist_count:,}</b>\n\n"
        f"{section('Основные статусы задач ЛС')}\n"
        f"📝 Заготовок: <b>{dashboard.campaign_count:,}</b>\n"
        f"🔵 Активных запусков: <b>{dashboard.active_launches:,}</b>\n"
        f"🕒 В очереди: <b>{dashboard.queued_tasks:,}</b>"
        f" · выполняется: <b>{dashboard.running_tasks:,}</b>\n"
        f"✅ Telegram подтвердил отправку: <b>{dashboard.succeeded_tasks:,}</b>\n"
        f"⏭ Пропущено без отправки: <b>{dashboard.skipped_tasks:,}</b>\n"
        f"⚠️ Ошибка или неясный результат: <b>{dashboard.failed_tasks:,}</b>\n\n"
        "<i>На главной показаны основные статусы; отменённые задачи видны внутри конкретного "
        "запуска. Успешная отправка не означает, что человек прочитал сообщение. Сервис "
        "не списывает Stars и соблюдает заданную защиту от повтора.</i>"
    )


def direct_messages_home_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Создать ЛС-рассылку", callback_data="dm:new")],
            [
                InlineKeyboardButton(
                    text="👥 Пригласить в свой канал/группу", callback_data="inv:home"
                )
            ],
            [
                InlineKeyboardButton(text="📂 Базы подписчиков", callback_data="dm:lists:0"),
                InlineKeyboardButton(text="📥 Импортировать", callback_data="dm:import"),
            ],
            [
                InlineKeyboardButton(text="📝 Заготовки", callback_data="dm:campaigns:0"),
                InlineKeyboardButton(text="📊 Запуски", callback_data="dm:launches:0"),
            ],
            [InlineKeyboardButton(text="🚫 Глобальный стоп-лист", callback_data="dm:blacklist")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="dm:home")],
            [InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home")],
        ]
    )


def _direct_wizard_title(step: int) -> str:
    return screen_title("Новая ЛС-рассылка", f"Шаг {step} из 6")


def direct_blacklist_text(total: int) -> str:
    return (
        screen_title("Глобальный стоп-лист", "Запрет на любые контакты по Telegram ID") + "\n\n"
        f"Сейчас запрещено: <b>{total:,}</b> Telegram ID.\n\n"
        "Этим людям сервис не будет писать в ЛС, приглашать в канал или группу и не будет "
        "отмечать их в рассылках по чатам или комментариях. Запрет проверяется при "
        "выборе аудитории и ещё раз "
        "перед самой отправкой. Он действует и для ID, которых пока нет ни в одной базе."
    )


def direct_blacklist_menu(
    *,
    add_callback: str = "dm:blacklist:add",
    back_callback: str = "dm:home",
    back_text: str = "⬅️ К личным сообщениям",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить ID", callback_data=add_callback)],
            [InlineKeyboardButton(text=back_text, callback_data=back_callback)],
        ]
    )


def direct_blacklist_add_prompt_text() -> str:
    return (
        screen_title("Добавить в стоп-лист") + "\n\n"
        "Отправьте один ID или несколько ID через пробел, запятую либо каждый с новой "
        "строки. За один раз — до 500 ID.\n\n"
        "Пример: <code>123456789, 987654321</code>"
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
        "дубликаты объединяются по Telegram ID. Повторный файл с тем же логическим "
        "названием добавляет новые записи и обновляет найденные профили; люди, которых нет "
        "в новом файле, автоматически из существующей базы не удаляются."
    )


def subscriber_import_result_text(result: SubscriberImportResult) -> str:
    subscriber_list = result.subscriber_list
    issue_note = ""
    if result.invalid:
        issue_note = (
            f"\n⚠️ Некорректных строк: <b>{result.invalid:,}</b>. "
            "Они пропущены без остановки остальных данных."
        )
    return (
        screen_title("База импортирована", "Фактический результат обработки") + "\n\n"
        f"Название: <b>{escape(subscriber_list.name)}</b>\n"
        f"Строк прочитано: <b>{result.total_lines:,}</b>\n"
        f"Корректных уникальных записей: <b>{result.valid_records:,}</b>\n"
        f"Добавлено участников именно в эту базу: <b>{result.added_members:,}</b>\n"
        f"Новых людей в общей таблице: <b>{result.inserted_profiles:,}</b>\n"
        f"Существующих профилей обработано: <b>{result.updated_profiles:,}</b>\n"
        f"Дубликатов Telegram ID внутри файла: <b>{result.duplicates:,}</b>{issue_note}\n\n"
        "<b>Текущее состояние базы после импорта</b>\n"
        f"👤 Всего: <b>{subscriber_list.member_count:,}</b>\n"
        f"⭐ Premium: <b>{subscriber_list.premium_count:,}</b>\n"
        f"Обычные: <b>{subscriber_list.non_premium_count:,}</b>\n"
        f"🔗 С username: <b>{subscriber_list.username_count:,}</b>\n\n"
        "<i>Импорт работает как добавление/обновление, а не как полная замена базы.</i>"
    )


def subscriber_import_result_menu(list_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✉️ Создать ЛС-рассылку",
                    callback_data=f"dm:new:list:{list_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👥 Пригласить в канал / группу",
                    callback_data=f"inv:new:fromlist:{list_id}",
                )
            ],
            [InlineKeyboardButton(text="📂 Открыть базу", callback_data=f"dm:list:{list_id}:0")],
            [InlineKeyboardButton(text="⬅️ К личным сообщениям", callback_data="dm:home")],
        ]
    )


def subscriber_lists_text(
    items: list[SubscriberList], total: int, page: int, page_size: int
) -> str:
    pages = max(1, ceil(total / page_size))
    lines = [
        screen_title("Базы подписчиков", f"Списков: {total:,} · страница {page + 1}/{pages}"),
        "",
    ]
    for item in items:
        state = {
            SubscriberListStatus.READY: "🟢",
            SubscriberListStatus.IMPORTING: "🔵",
            SubscriberListStatus.FAILED: "🔴",
        }[item.status]
        lines.append(
            f"{state} <b>{escape(item.name)}</b>\n"
            f"   👤 {item.member_count:,} · ⭐ {item.premium_count:,}"
            f" · 🔗 {item.username_count:,}"
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
                text=f"📂 {item.name[:35]} · {item.member_count:,}",
                callback_data=f"dm:list:{item.id}:{page}",
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"dm:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"dm:lists:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [InlineKeyboardButton(text="📥 Импортировать базу", callback_data="dm:import")],
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"dm:lists:{page}")],
            [InlineKeyboardButton(text="⬅️ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def subscriber_list_details_text(item: SubscriberList) -> str:
    status = {
        SubscriberListStatus.READY: "🟢 готова",
        SubscriberListStatus.IMPORTING: "🔵 импортируется",
        SubscriberListStatus.FAILED: "🔴 ошибка импорта",
    }[item.status]
    error = f"\n\nОшибка: <code>{escape(item.last_error)}</code>" if item.last_error else ""
    return (
        screen_title(escape(item.name), f"Состояние: {status}") + "\n\n"
        f"👤 Всего: <b>{item.member_count:,}</b>\n"
        f"⭐ Premium: <b>{item.premium_count:,}</b>\n"
        f"Обычные: <b>{item.non_premium_count:,}</b>\n"
        f"🔗 С username: <b>{item.username_count:,}</b>\n"
        f"Только ID: <b>{max(0, item.member_count - item.username_count):,}</b>\n\n"
        "<b>Последний импорт</b>\n"
        f"Прочитано строк: <b>{item.last_import_total:,}</b> · добавлено участников: "
        f"<b>{item.last_import_added:,}</b>\n"
        f"Существующих профилей обработано: <b>{item.last_import_updated:,}</b>\n"
        f"Дубли: <b>{item.last_import_duplicates:,}</b> · некорректные строки: "
        f"<b>{item.last_import_invalid:,}</b>{error}"
    )


def subscriber_list_details_menu(item: SubscriberList, page: int) -> InlineKeyboardMarkup:
    rows = []
    if item.status == SubscriberListStatus.READY and item.member_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text="✉️ Создать ЛС-рассылку",
                    callback_data=f"dm:new:list:{item.id}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="👥 Пригласить в канал / группу",
                    callback_data=f"inv:new:fromlist:{item.id}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🗑 Удалить базу", callback_data=f"dm:listdel:{item.id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К базам", callback_data=f"dm:lists:{page}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def subscriber_list_delete_menu(list_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить",
                    callback_data=f"dm:listdelok:{list_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"dm:list:{list_id}:{page}")],
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
                text=f"📂 {item.name[:32]} · {item.member_count:,}",
                callback_data=f"dm:new:list:{item.id}",
            )
        ]
        for item in items
        if item.status == SubscriberListStatus.READY and item.member_count > 0
    ]
    pages = max(1, ceil(total / page_size))
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⏮", callback_data="dm:new:lists:0"))
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"dm:new:lists:{page - 1}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"dm:new:lists:{page + 1}"))
        navigation.append(InlineKeyboardButton(text="⏭", callback_data=f"dm:new:lists:{pages - 1}"))
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_segment_text(item: SubscriberList) -> str:
    return (
        _direct_wizard_title(2) + "\n\n"
        f"База: <b>{escape(item.name)}</b> · {item.member_count:,} человек\n\n"
        "Кому подготовить задания? Premium берётся из поля "
        "<code>is_premium</code> и будет уточняться после успешного разрешения профиля."
    )


def direct_segment_menu(list_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Всем", callback_data=f"dm:new:seg:{list_id}:all")],
            [
                InlineKeyboardButton(
                    text="⭐ Только Premium", callback_data=f"dm:new:seg:{list_id}:premium"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Обычные", callback_data=f"dm:new:seg:{list_id}:non_premium"
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="dm:home")],
        ]
    )


def direct_limit_text(segment: SubscriberSegment) -> str:
    segment_label = {
        SubscriberSegment.ALL: "все",
        SubscriberSegment.PREMIUM: "только Premium",
        SubscriberSegment.NON_PREMIUM: "без Premium",
    }[segment]
    return (
        _direct_wizard_title(3) + "\n\n"
        f"Сегмент: <b>{segment_label}</b>.\n"
        "Введите количество получателей от 1 до 1 000 000. Можно написать "
        "<code>все</code> — будет взят весь доступный сегмент."
    )


def direct_cooldown_text() -> str:
    return (
        _direct_wizard_title(4) + "\n\n"
        "Как долго не писать одному человеку повторно? Защита действует глобально "
        "между всеми базами и параллельными запусками.\n\n"
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
                InlineKeyboardButton(text="✅ 30 дней", callback_data="dm:new:cooldown:30"),
                InlineKeyboardButton(text="90 дней", callback_data="dm:new:cooldown:90"),
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="dm:home")],
        ]
    )


def direct_account_picker_text(
    selected_count: int,
    total: int,
    page: int,
    page_size: int,
) -> str:
    pages = max(1, ceil(total / page_size))
    return (
        _direct_wizard_title(5) + "\n\n"
        f"Выбрано отправителей: <b>{selected_count}</b>.\n"
        "Задания распределяются автоматически. Используются только готовые аккаунты "
        "со свободным дневным лимитом; паузы и ограничения Telegram соблюдаются."
        f"\n\nГотовых аккаунтов: <b>{total}</b> · страница <b>{page + 1}/{pages}</b>"
    )


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
    rows = [
        [
            InlineKeyboardButton(
                text=f"{'✅' if int(account.id) in selected_ids else '▫️'} {account.label[:34]}",
                callback_data=f"dm:new:account:{account.id}",
            )
        ]
        for account in visible_accounts
    ]
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton(text="⏮", callback_data="dm:new:accounts:page:0"))
        navigation.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"dm:new:accounts:page:{page - 1}")
        )
    if page + 1 < pages:
        navigation.append(
            InlineKeyboardButton(text="➡️", callback_data=f"dm:new:accounts:page:{page + 1}")
        )
        navigation.append(
            InlineKeyboardButton(text="⏭", callback_data=f"dm:new:accounts:page:{pages - 1}")
        )
    if navigation:
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(text="✅ Выбрать всех", callback_data="dm:new:accounts:all"),
                InlineKeyboardButton(text="🧹 Очистить", callback_data="dm:new:accounts:none"),
            ],
            [InlineKeyboardButton(text="➡️ Продолжить", callback_data="dm:new:accounts:done")],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_content_prompt_text() -> str:
    return (
        _direct_wizard_title(6) + "\n\n"
        "Отправьте готовый пост: текст, фото, видео, документ, голосовое или видеокружок. "
        "У видеокружка подписи быть не может; для остальных вложений она допустима. "
        "Форматирование Telegram и Spintax сохраняются.\n\n"
        "<i>Перед созданием заданий будет отдельное подтверждение получателей, "
        "лимита и согласия.</i>"
    )


def direct_campaign_confirmation_text(view: DirectCampaignView) -> str:
    campaign = view.campaign
    segment = {
        SubscriberSegment.ALL: "все подписчики",
        SubscriberSegment.PREMIUM: "только Premium",
        SubscriberSegment.NON_PREMIUM: "без Premium",
    }[campaign.segment]
    preview = escape(campaign.primary_text)
    if len(preview) > 500:
        preview = preview[:497] + "…"
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
        f"Проходят фильтры сейчас: <b>{view.eligible_count:,}</b>\n"
        f"С username: <b>{view.username_count:,}</b>\n"
        f"На паузе повтора: <b>{view.recent_count:,}</b>\n"
        f"Уже заняты другим запуском: <b>{view.reserved_count:,}</b>\n"
        f"Отправителей: <b>{len(view.account_ids)}</b>\n"
        f"Не писать повторно: <b>{campaign.contact_cooldown_days} дн.</b>{media}\n\n"
        f"<blockquote>{content_preview}</blockquote>\n"
        "⚠️ Запускайте только по людям, которые согласились получать ваши сообщения. "
        "Прохождение фильтров не гарантирует отправку: Telegram может отклонить конкретного "
        "получателя или маршрут. Stars автоматически не списываются."
    )


def direct_campaign_confirmation_menu(campaign_id: int, launch_token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтверждаю согласие и запускаю",
                    callback_data=f"dm:launch:{campaign_id}:{launch_token}",
                )
            ],
            [InlineKeyboardButton(text="✖️ Не запускать", callback_data="dm:home")],
        ]
    )


def direct_campaigns_text(
    items: list[DirectCampaign], total: int, page: int, page_size: int
) -> str:
    pages = max(1, ceil(total / page_size))
    lines = [
        screen_title("Заготовки ЛС-рассылок", f"Всего: {total} · страница {page + 1}/{pages}"),
        "",
    ]
    for campaign in items:
        segment = {
            SubscriberSegment.ALL: "все",
            SubscriberSegment.PREMIUM: "Premium",
            SubscriberSegment.NON_PREMIUM: "обычные",
        }[campaign.segment]
        lines.append(
            f"{'🟢' if campaign.enabled else '⏸'} <b>{escape(campaign.name)}</b>\n"
            f"   {segment} · до {campaign.recipient_limit:,} · "
            f"повтор {campaign.contact_cooldown_days} дн."
        )
    if not items:
        lines.append(
            "<b>С чего начать</b>\n"
            "Создайте ЛС-рассылку — после подтверждения она станет заготовкой."
        )
    else:
        lines.append(
            "\n<i>Откройте заготовку, чтобы запустить её повторно с новой проверкой доступа.</i>"
        )
    return "\n".join(lines)


def direct_campaigns_menu(
    items: list[DirectCampaign], total: int, page: int, page_size: int
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"📝 {item.name[:38]}", callback_data=f"dm:campaign:{item.id}:{page}"
            )
        ]
        for item in items
    ]
    pages = max(1, ceil(total / page_size))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"dm:campaigns:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"dm:campaigns:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.extend(
        [
            [InlineKeyboardButton(text="🚀 Создать ЛС-рассылку", callback_data="dm:new")],
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"dm:campaigns:{page}")],
            [InlineKeyboardButton(text="⬅️ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_campaign_details_menu(campaign_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Запустить снова", callback_data=f"dm:campaignrun:{campaign_id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К заготовкам", callback_data=f"dm:campaigns:{page}")],
        ]
    )


def direct_launches_text(
    items: list[DirectLaunchView], total: int, page: int, page_size: int
) -> str:
    pages = max(1, ceil(total / page_size))
    lines = [
        screen_title("Запуски ЛС-рассылок", f"Всего: {total} · страница {page + 1}/{pages}"),
        "",
    ]
    icons = {
        DirectLaunchStatus.QUEUED: "🕒",
        DirectLaunchStatus.RUNNING: "🔵",
        DirectLaunchStatus.SUCCEEDED: "✅",
        DirectLaunchStatus.PARTIAL: "🟡",
        DirectLaunchStatus.FAILED: "❌",
        DirectLaunchStatus.CANCELLED: "⏸",
    }
    for view in items:
        lines.append(
            f"{icons[view.launch.status]} <b>{escape(view.campaign_name)}</b> · #{view.launch.id}\n"
            f"   ✅ отправлено {view.succeeded:,} · ⏳ активно {view.active:,}\n"
            f"   ⏭ пропущено {view.skipped:,} · ⚠️ ошибка/неясно {view.failed:,} · "
            f"⏹ отменено {view.cancelled:,}"
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
                text=f"📊 #{view.launch.id} · {view.campaign_name[:30]}",
                callback_data=f"dm:launchview:{view.launch.id}:{page}",
            )
        ]
        for view in items
    ]
    pages = max(1, ceil(total / page_size))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"dm:launches:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"dm:launches:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.extend(
        [
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"dm:launches:{page}")],
            [InlineKeyboardButton(text="⬅️ К личным сообщениям", callback_data="dm:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_launch_details_text(view: DirectLaunchView) -> str:
    launch = view.launch
    status = {
        DirectLaunchStatus.QUEUED: "🕒 готовится к отправке",
        DirectLaunchStatus.RUNNING: "🔵 выполняется",
        DirectLaunchStatus.SUCCEEDED: "✅ завершён",
        DirectLaunchStatus.PARTIAL: "🟡 завершён частично",
        DirectLaunchStatus.FAILED: "❌ завершён с ошибкой",
        DirectLaunchStatus.CANCELLED: "⏸ остановлен",
    }[launch.status]
    issue = ""
    if view.last_error_code in {"account_pacing", "account_operation_busy"}:
        issue = (
            "\n\n<i>⏳ Следующее сообщение ждёт безопасного окна аккаунта. "
            "Это штатная пауза — сервис продолжит сам.</i>"
        )
    elif view.last_error_code:
        issue = "\n\n<b>⚠️ Последняя зафиксированная ситуация</b>"
        if view.last_error_account_label:
            issue += f"\nАккаунт: <b>{escape(view.last_error_account_label)}</b>"
        if view.last_error_recipient:
            issue += f"\nПолучатель: <code>{escape(view.last_error_recipient)}</code>"
        issue += f"\nПричина: <b>{escape(failure_label(view.last_error_code))}</b>"
        hint = failure_hint(view.last_error_code)
        if hint:
            issue += f"\nЧто делать: {escape(hint)}"
        if view.last_error_status == DirectMessageTaskStatus.QUEUED:
            issue += (
                "\n<i>Попытка перенесена; остальные доступные отправители продолжают работу.</i>"
            )
        elif view.last_error_status == DirectMessageTaskStatus.SKIPPED:
            issue += "\n<i>Получатель пропущен; подтверждённой отправки ему нет.</i>"
        elif view.last_error_status == DirectMessageTaskStatus.FAILED:
            if view.last_error_code in {
                "delivery_unconfirmed",
                "step_timeout",
                "cancelled_outcome_unknown",
            }:
                issue += (
                    "\n<i>Итог отправки не подтверждён. Система не считает это успешной "
                    "отправкой и не делает небезопасный повтор.</i>"
                )
            else:
                issue += "\n<i>Задача завершена с ошибкой без небезопасного повтора.</i>"
    exclusions: list[str] = []
    if launch.excluded_recent:
        exclusions.append(f"не повторяем сейчас: <b>{launch.excluded_recent:,}</b>")
    if launch.excluded_reserved:
        exclusions.append(f"заняты в другом запуске: <b>{launch.excluded_reserved:,}</b>")
    excluded_text = (
        "\n\n<i>При старте не включены — " + " · ".join(exclusions) + ".</i>" if exclusions else ""
    )
    accounted = view.succeeded + view.queued + view.running + view.skipped + view.failed + view.cancelled
    integrity_note = ""
    if accounted != launch.total_tasks:
        integrity_note = (
            "\n\n⚠️ <b>Сводка задач расходится с запуском:</b> "
            f"учтено {accounted:,} из {launch.total_tasks:,}. Обновите экран; "
            "если расхождение останется — проверьте журнал."
        )
    return (
        screen_title(f"Запуск #{launch.id}", f"Состояние: {status}") + "\n"
        f"Заготовка: <b>{escape(view.campaign_name)}</b>\n"
        f"База: <b>{escape(view.list_name)}</b>\n\n"
        f"{section('Фактический результат')}\n"
        f"Всего задач запуска: <b>{launch.total_tasks:,}</b>\n"
        f"✅ Telegram подтвердил отправку: <b>{view.succeeded:,}</b>\n"
        f"🕒 В очереди: <b>{view.queued:,}</b>\n"
        f"🔵 Выполняется сейчас: <b>{view.running:,}</b>\n"
        f"⏭ Пропущено без отправки: <b>{view.skipped:,}</b>\n"
        f"⚠️ Ошибка или неясный результат: <b>{view.failed:,}</b>\n"
        f"⏹ Отменено до отправки: <b>{view.cancelled:,}</b>\n\n"
        "<i>«Telegram подтвердил отправку» означает успешный ответ операции для нужного "
        "получателя; это не означает, что человек прочитал сообщение. Неясные исходы в этот "
        "счётчик не входят.</i>"
        f"{integrity_note}{excluded_text}{issue}"
    )


def direct_launch_details_menu(view: DirectLaunchView, page: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="🔄 Обновить", callback_data=f"dm:launchview:{view.launch.id}:{page}"
            )
        ]
    ]
    if view.launch.status in {DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING}:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⏸ Остановить", callback_data=f"dm:launchcancel:{view.launch.id}:{page}"
                )
            ]
        )
    if view.last_error_code == "proxy_unavailable":
        rows.append(
            [
                InlineKeyboardButton(
                    text="🌐 Проверить прокси",
                    callback_data="menu:proxies:0",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К запускам", callback_data=f"dm:launches:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def direct_launch_cancel_menu(launch_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏸ Да, остановить", callback_data=f"dm:launchcancelok:{launch_id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Не останавливать", callback_data=f"dm:launchview:{launch_id}:{page}"
                )
            ],
        ]
    )


def direct_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="dm:home")]]
    )


for _factory_name, _factory in tuple(globals().items()):
    if (
        not _factory_name.startswith("_")
        and callable(_factory)
        and _factory_name.endswith(("menu", "picker", "confirmation", "cancel"))
    ):
        globals()[_factory_name] = themed_keyboard(_factory)
