"""Control bot UI and router for Telegram Story Tagging campaigns."""

from __future__ import annotations

import asyncio
import logging
from html import escape
from io import BytesIO
from typing import TYPE_CHECKING

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from proxima_traffic.bot.middleware import (
    acknowledge_callback,
    callback_is_current,
    mark_callback_data_ready,
    mark_callback_screen_updated,
    record_callback_telegram_api,
    start_callback_telegram_api,
)
from proxima_traffic.bot.panel import remember_recovered_panel, screen_is_uneditable
from proxima_traffic.bot.states import StoryTaggingCreateState
from proxima_traffic.bot.story_ui import add_story_pages, story_mode_label, story_next_run_text
from proxima_traffic.bot.ui import fit_panel_text, polish_markup, screen_title, section
from proxima_traffic.config import Settings
from proxima_traffic.content import ContentError, StoredMedia, store_media
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.models import StoryCampaignStatus, SubscriberList
from proxima_traffic.story_content import prepare_story_photo
from proxima_traffic.story_tagging import (
    StoryTaggingCampaignView,
    StoryTaggingDashboard,
    StoryTaggingService,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass

story_router = Router(name="story_tagging")

PAGE_SIZE = 8


# ---------------------------------------------------------------------------
# Presentation & Layouts
# ---------------------------------------------------------------------------


def story_tagging_home_text(dashboard: StoryTaggingDashboard) -> str:
    pending = max(
        0,
        dashboard.total_tasks
        - dashboard.succeeded_tasks
        - dashboard.failed_tasks
        - dashboard.skipped_tasks,
    )
    lines = [
        screen_title(
            "📖 Теггинг в историях Telegram",
            "Публикация историй с отметками людей из базы подписчиков",
        ),
        "",
        "Отметка пользователей в публичных историях аккаунтов. "
        "Telegram обрабатывает отметки с учётом приватности и настроек уведомлений получателя.",
        "",
        section("Статистика теггинга"),
        f"• Активных кампаний: <b>{dashboard.active_campaign_count}</b>"
        f" / {dashboard.campaign_count}",
        f"• Опубликовано историй: <b>{dashboard.succeeded_tasks}</b>",
        f"• Отметок пользователей: <b>{dashboard.total_tagged_users:,}</b>",
        f"• Задач в очереди: <b>{pending}</b>",
        f"• Пропущено задач: <b>{dashboard.skipped_tasks}</b>",
        f"• Режим отправки: <b>{'Симуляция' if dashboard.simulated else 'Реальная публикация'}</b>",
    ]
    return "\n".join(lines)


def story_home_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="＋ Создать кампанию теггинга",
            callback_data="story:new",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📋 Список кампаний",
            callback_data="story:list:0",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="« Главное меню",
            callback_data="menu:home",
        )
    )
    return polish_markup(builder.as_markup())


def story_list_keyboard(
    campaigns: list[StoryTaggingCampaignView], page: int, total: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for camp in campaigns:
        status_emoji = (
            "🟢"
            if camp.status == StoryCampaignStatus.ACTIVE
            else "⏸"
            if camp.status == StoryCampaignStatus.PAUSED
            else "🏁"
        )
        builder.row(
            InlineKeyboardButton(
                text=f"{status_emoji} {camp.name} ({camp.succeeded_tasks} историй)",
                callback_data=f"story:view:{camp.id}",
            )
        )

    # Navigation
    nav_buttons: list[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(text="« Назад", callback_data=f"story:list:{page - 1}")
        )
    if (page + 1) * PAGE_SIZE < total:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперёд »", callback_data=f"story:list:{page + 1}")
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    builder.row(
        InlineKeyboardButton(text="＋ Новая кампания", callback_data="story:new"),
        InlineKeyboardButton(text="« Меню теггинга", callback_data="story:home"),
    )
    return polish_markup(builder.as_markup())


def story_campaign_text(campaign: StoryTaggingCampaignView) -> str:
    status_ru = {
        StoryCampaignStatus.ACTIVE: "🟢 Активна (публикуется)",
        StoryCampaignStatus.PAUSED: "⏸ Приостановлена",
        StoryCampaignStatus.COMPLETED: "🏁 Завершена",
        StoryCampaignStatus.DRAFT: "📝 Черновик",
    }.get(campaign.status, str(campaign.status))

    lines = [
        screen_title(
            f"Кампания: {campaign.name}",
            f"Статус: {status_ru}",
        ),
        "",
        section("Параметры кампании"),
        f"• Режим: <b>{story_mode_label(campaign.simulated, 'Реальная публикация')}</b>",
        f"• База подписчиков: <b>{escape(campaign.subscriber_list_name)}</b>",
        f"• Формат медиа: <b>{campaign.media_kind.upper()}</b>"
        f" ({escape(campaign.media_name or 'файл')})",
        f"• Отметок на историю: <b>{campaign.tags_per_story}</b>",
        f"• Цель отметок: <b>{campaign.total_target_tags}</b>",
        f"• Лимит на аккаунт в день: <b>{campaign.daily_limit_per_account}</b> историй",
        f"• Пауза аккаунта между историями: <b>{campaign.cooldown_seconds} сек.</b>",
        "",
        section("Прогресс выполнения"),
        f"• Опубликовано историй: <b>{campaign.succeeded_tasks}</b> / {campaign.total_tasks}",
        f"• Всего отмечено людей: <b>{campaign.tagged_count}</b>",
        f"• Ошибок / повторов: <b>{campaign.failed_tasks}</b>",
        f"• Пропущено задач: <b>{campaign.skipped_tasks}</b>",
    ]

    if campaign.last_error:
        lines.extend(
            ["", section("Последняя ошибка / причина ожидания"), escape(campaign.last_error)]
        )

    if scheduled := story_next_run_text(campaign.next_run_at):
        lines.extend(["", scheduled])
    if campaign.caption:
        preview = (
            campaign.caption if len(campaign.caption) <= 150 else campaign.caption[:147] + "..."
        )
        lines.extend(["", section("Текст / подпись"), f"<i>{escape(preview)}</i>"])

    if campaign.last_story_url:
        lines.extend(["", f"🔗 Последняя история: {campaign.last_story_url}"])

    return "\n".join(lines)


def story_campaign_keyboard(campaign: StoryTaggingCampaignView) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if campaign.status == StoryCampaignStatus.ACTIVE:
        builder.row(
            InlineKeyboardButton(
                text="⏸ Приостановить",
                callback_data=f"story:pause:{campaign.id}",
            )
        )
    elif campaign.status == StoryCampaignStatus.PAUSED:
        builder.row(
            InlineKeyboardButton(
                text="▶ Возобновить",
                callback_data=f"story:resume:{campaign.id}",
            )
        )

    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"story:view:{campaign.id}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"story:delete:{campaign.id}"),
    )
    builder.row(
        InlineKeyboardButton(text="« К списку кампаний", callback_data="story:list:0"),
    )
    return polish_markup(builder.as_markup())


def story_select_list_keyboard(
    lists: list[SubscriberList], page: int = 0, total: int = 0
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for sub_list in lists[:20]:
        builder.row(
            InlineKeyboardButton(
                text=f"📁 {sub_list.name} ({sub_list.member_count:,} чел.)",
                callback_data=f"story:set_list:{sub_list.id}",
            )
        )
    add_story_pages(builder, page, total, "story:lists")
    builder.row(InlineKeyboardButton(text="« Отмена", callback_data="story:cancel"))
    return polish_markup(builder.as_markup())


def story_tags_per_story_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="3", callback_data="story:tags:3"),
        InlineKeyboardButton(text="5 (рекомендуется)", callback_data="story:tags:5"),
        InlineKeyboardButton(text="10", callback_data="story:tags:10"),
    )
    builder.row(
        InlineKeyboardButton(text="15", callback_data="story:tags:15"),
        InlineKeyboardButton(text="20", callback_data="story:tags:20"),
    )
    builder.row(InlineKeyboardButton(text="« Отмена", callback_data="story:cancel"))
    return polish_markup(builder.as_markup())


def story_total_tags_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="50", callback_data="story:total:50"),
        InlineKeyboardButton(text="100", callback_data="story:total:100"),
        InlineKeyboardButton(text="250", callback_data="story:total:250"),
    )
    builder.row(
        InlineKeyboardButton(text="500", callback_data="story:total:500"),
        InlineKeyboardButton(text="1 000", callback_data="story:total:1000"),
        InlineKeyboardButton(text="5 000", callback_data="story:total:5000"),
    )
    builder.row(InlineKeyboardButton(text="« Отмена", callback_data="story:cancel"))
    return polish_markup(builder.as_markup())


def story_confirm_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🚀 Запустить теггинг",
            callback_data="story:confirm:launch",
        )
    )
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="story:cancel"))
    return polish_markup(builder.as_markup())


def story_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="« Отмена", callback_data="story:cancel"))
    return polish_markup(builder.as_markup())


# ---------------------------------------------------------------------------
# Helper Helpers
# ---------------------------------------------------------------------------


async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool | None = None,
) -> bool:
    return await acknowledge_callback(callback, text, show_alert=show_alert)


async def _edit_screen(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    *,
    answer: bool = True,
) -> None:
    mark_callback_data_ready()
    if not callback_is_current(callback):
        if answer:
            await _safe_callback_answer(callback)
        return
    if not isinstance(callback.message, Message):
        if answer:
            await _safe_callback_answer(callback)
        return
    markup = polish_markup(reply_markup) if reply_markup else None
    fitted_text = fit_panel_text(text)
    answer_task = asyncio.create_task(_safe_callback_answer(callback)) if answer else None
    api_started_at = start_callback_telegram_api()
    try:
        await callback.message.edit_text(fitted_text, reply_markup=markup)
    except TelegramBadRequest as exc:
        if screen_is_uneditable(exc):
            restored = await callback.message.answer(fitted_text, reply_markup=markup)
            if isinstance(restored, Message):
                await remember_recovered_panel(restored)
        elif "message is not modified" not in str(exc).lower():
            logger.warning("TelegramBadRequest in story_tagging _edit_screen: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error in story_tagging _edit_screen: %s", exc)
    finally:
        record_callback_telegram_api(api_started_at, operation="edit")
        mark_callback_screen_updated()
        if answer_task is not None:
            await answer_task


async def _extract_story_media(
    message: Message,
    bot: Bot,
    settings: Settings,
) -> StoredMedia:
    """Download and persist uploaded photo or video for story."""
    downloadable: object | None = None
    media_kind: str | None = None
    media_name = ""
    file_size: int | None = None

    if message.photo:
        downloadable = message.photo[-1]
        media_kind = "photo"
        media_name = f"story_{message.photo[-1].file_unique_id}.jpg"
        file_size = message.photo[-1].file_size
    elif message.video is not None:
        downloadable = message.video
        media_kind = "video"
        media_name = message.video.file_name or f"story_{message.video.file_unique_id}.mp4"
        file_size = message.video.file_size
    elif message.document is not None:
        mime = message.document.mime_type or ""
        if mime.startswith("video/"):
            media_kind = "video"
            media_name = (
                message.document.file_name or f"story_{message.document.file_unique_id}.mp4"
            )
        elif mime.startswith("image/"):
            media_kind = "photo"
            media_name = (
                message.document.file_name or f"story_{message.document.file_unique_id}.jpg"
            )
        else:
            raise ContentError("Для историй поддерживаются только фото или видео файлы.")
        downloadable = message.document
        file_size = message.document.file_size

    if downloadable is None or media_kind is None:
        raise ContentError("Пожалуйста, отправьте фото или видео файл для публикации в историю.")

    if file_size is not None and file_size > min(settings.media_max_bytes, 30 * 1024 * 1024):
        raise ContentError("Файл истории превышает допустимый размер (до 30 МБ).")

    buffer = BytesIO()
    await bot.download(downloadable, destination=buffer)
    raw_bytes = buffer.getvalue()
    if len(raw_bytes) > min(settings.media_max_bytes, 30 * 1024 * 1024):
        raise ContentError("Файл истории превышает допустимый размер")
    if media_kind == "photo":
        raw_bytes = await asyncio.to_thread(prepare_story_photo, raw_bytes)
        media_name = media_name.rsplit(".", 1)[0] + ".jpg"
    if len(raw_bytes) > min(settings.media_max_bytes, 30 * 1024 * 1024):
        raise ContentError("Файл истории должен быть не больше 30 МБ")
    return await asyncio.to_thread(
        store_media,
        settings,
        raw_bytes,
        kind=media_kind,
        original_name=media_name,
    )


# ---------------------------------------------------------------------------
# Router Handlers
# ---------------------------------------------------------------------------


@story_router.callback_query(F.data == "story:home")
async def handle_story_home(
    callback: CallbackQuery,
    state: FSMContext,
    story_tagging: StoryTaggingService,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    dashboard = await story_tagging.get_dashboard()
    text = story_tagging_home_text(dashboard)
    await _edit_screen(callback, text, story_home_keyboard())


@story_router.callback_query(F.data == "story:cancel")
async def handle_story_cancel(
    callback: CallbackQuery,
    state: FSMContext,
    story_tagging: StoryTaggingService,
) -> None:
    await _safe_callback_answer(callback, "Отменено")
    await state.clear()
    dashboard = await story_tagging.get_dashboard()
    text = story_tagging_home_text(dashboard)
    await _edit_screen(callback, text, story_home_keyboard())


@story_router.callback_query(F.data.regexp(r"^story:list:\d+$"))
async def handle_story_list(
    callback: CallbackQuery,
    state: FSMContext,
    story_tagging: StoryTaggingService,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    page = int(callback.data.split(":")[-1]) if callback.data else 0
    campaigns = await story_tagging.list_campaigns(limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    dashboard = await story_tagging.get_dashboard()

    lines = [
        screen_title("📋 Кампании теггинга в историях"),
        "",
        f"Всего кампаний: <b>{dashboard.campaign_count}</b>"
        f" (активных: {dashboard.active_campaign_count})",
        "",
        "Выберите кампанию для просмотра деталей, статистики и управления:",
    ]
    if not campaigns:
        lines.append("\n<i>Кампаний пока нет. Нажмите «＋ Создать кампанию теггинга».</i>")

    text = "\n".join(lines)
    keyboard = story_list_keyboard(campaigns, page, dashboard.campaign_count)
    await _edit_screen(callback, text, keyboard)


@story_router.callback_query(F.data.regexp(r"^story:view:\d+$"))
async def handle_story_view(
    callback: CallbackQuery,
    story_tagging: StoryTaggingService,
) -> None:
    await _safe_callback_answer(callback)
    campaign_id = int(callback.data.split(":")[-1])
    campaign = await story_tagging.get_campaign(campaign_id)
    if campaign is None:
        await _safe_callback_answer(callback, "Кампания не найдена")
        return

    text = story_campaign_text(campaign)
    keyboard = story_campaign_keyboard(campaign)
    await _edit_screen(callback, text, keyboard)


@story_router.callback_query(F.data.regexp(r"^story:pause:\d+$"))
async def handle_story_pause(
    callback: CallbackQuery,
    story_tagging: StoryTaggingService,
) -> None:
    campaign_id = int(callback.data.split(":")[-1])
    success = await story_tagging.pause_campaign(campaign_id)
    await _safe_callback_answer(
        callback, "Кампания приостановлена ⏸" if success else "Кампания уже не активна"
    )
    campaign = await story_tagging.get_campaign(campaign_id)
    if campaign is not None:
        await _edit_screen(
            callback, story_campaign_text(campaign), story_campaign_keyboard(campaign)
        )


@story_router.callback_query(F.data.regexp(r"^story:resume:\d+$"))
async def handle_story_resume(
    callback: CallbackQuery,
    story_tagging: StoryTaggingService,
) -> None:
    campaign_id = int(callback.data.split(":")[-1])
    success = await story_tagging.resume_campaign(campaign_id)
    await _safe_callback_answer(
        callback, "Кампания возобновлена ▶" if success else "Проверьте статус и режим кампании"
    )
    campaign = await story_tagging.get_campaign(campaign_id)
    if campaign is not None:
        await _edit_screen(
            callback, story_campaign_text(campaign), story_campaign_keyboard(campaign)
        )


@story_router.callback_query(F.data.regexp(r"^story:delete:\d+$"))
async def handle_story_delete(
    callback: CallbackQuery,
    story_tagging: StoryTaggingService,
) -> None:
    campaign_id = int(callback.data.split(":")[-1])
    try:
        await story_tagging.delete_campaign(campaign_id)
    except ValueError as exc:
        await _safe_callback_answer(callback, str(exc), show_alert=True)
        return
    await _safe_callback_answer(callback, "Кампания удалена 🗑")
    # Redirect to list
    campaigns = await story_tagging.list_campaigns(limit=PAGE_SIZE, offset=0)
    dashboard = await story_tagging.get_dashboard()
    text = "📋 <b>Кампании теггинга</b>\n\nКампания успешно удалена."
    keyboard = story_list_keyboard(campaigns, 0, dashboard.campaign_count)
    await _edit_screen(callback, text, keyboard)


# ---------------------------------------------------------------------------
# Wizard: Create Campaign
# ---------------------------------------------------------------------------


@story_router.callback_query(F.data == "story:new")
async def begin_story_creation(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    await state.set_state(StoryTaggingCreateState.name)

    lines = [
        screen_title("＋ Новая кампания теггинга", "Шаг 1 из 5"),
        "",
        "Введите понятное <b>название</b> для кампании теггинга в историях "
        "(например: <i>Теггинг Крипта 21.10</i>):",
    ]
    await _edit_screen(callback, "\n".join(lines), story_cancel_keyboard())


@story_router.message(StoryTaggingCreateState.name)
async def process_story_name(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    name = (message.text or "").strip()
    if not (2 <= len(name) <= 100):
        await message.answer("Название должно содержать от 2 до 100 символов. Попробуйте снова:")
        return

    await state.update_data(name=name)
    await state.set_state(StoryTaggingCreateState.subscriber_list)

    lists, total = await direct_messages.list_ready_subscriber_lists(0, 20)
    if not lists:
        await message.answer(
            "Нет готовых баз. Загрузите базу в разделе «База подписчиков».",
            reply_markup=story_cancel_keyboard(),
        )
        return

    lines = [
        screen_title("＋ Выбор базы подписчиков", "Шаг 2 из 5"),
        "",
        f"Кампания: <b>{escape(name)}</b>",
        "",
        "Выберите базу, из которой брать пользователей для отметок в историях:",
    ]
    await message.answer(
        "\n".join(lines), reply_markup=story_select_list_keyboard(lists, total=total)
    )


@story_router.callback_query(
    StoryTaggingCreateState.subscriber_list, F.data.regexp(r"^story:lists:\d+$")
)
async def handle_story_list_page(
    callback: CallbackQuery, direct_messages: DirectMessageService
) -> None:
    await _safe_callback_answer(callback)
    page = int(callback.data.split(":")[-1])
    lists, total = await direct_messages.list_ready_subscriber_lists(page, 20)
    await _edit_screen(
        callback,
        screen_title("Выбор базы подписчиков"),
        story_select_list_keyboard(lists, page, total),
    )


@story_router.callback_query(
    StoryTaggingCreateState.subscriber_list, F.data.regexp(r"^story:set_list:\d+$")
)
async def process_story_list_selection(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    await _safe_callback_answer(callback)
    list_id = int(callback.data.split(":")[-1])
    sub_list = await direct_messages.get_subscriber_list(list_id)
    if sub_list is None:
        await _safe_callback_answer(callback, "База не найдена")
        return

    await state.update_data(list_id=list_id, list_name=sub_list.name)
    await state.set_state(StoryTaggingCreateState.media)

    lines = [
        screen_title("＋ Загрузка истории", "Шаг 3 из 5"),
        "",
        f"База: <b>{escape(sub_list.name)}</b>",
        "",
        "Отправьте <b>фото</b> или <b>видео</b>, которое будет опубликовано в истории.",
        "<i>Форматы: JPG, PNG, MP4. Фото будет подготовлено в 9:16 без обрезки, с полями.</i>",
    ]
    await _edit_screen(callback, "\n".join(lines), story_cancel_keyboard())


@story_router.message(StoryTaggingCreateState.media)
async def process_story_media(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
) -> None:
    try:
        stored = await _extract_story_media(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc), reply_markup=story_cancel_keyboard())
        return

    await state.update_data(
        media_path=stored.path,
        media_kind=stored.kind,
        media_name=stored.name,
        media_size=stored.size,
    )
    await state.set_state(StoryTaggingCreateState.caption)

    lines = [
        screen_title("＋ Подпись к истории", "Шаг 4 из 5"),
        "",
        f"Файл истории успешно загружен: <b>{stored.kind.upper()}</b>",
        "",
        "Введите подпись к истории. Для истории только с отметками отправьте <code>/skip</code>:",
    ]
    await message.answer("\n".join(lines), reply_markup=story_cancel_keyboard())


@story_router.message(StoryTaggingCreateState.caption)
async def process_story_caption(
    message: Message,
    state: FSMContext,
) -> None:
    text = (message.text or "").strip()
    caption = "" if text == "/skip" else text
    if message.text is None or len(caption.encode("utf-16-le")) // 2 > 2014:
        await message.answer("Отправьте подпись текстом до 2014 символов или /skip.")
        return

    await state.update_data(caption=caption)
    await state.set_state(StoryTaggingCreateState.tags_per_story)

    lines = [
        screen_title("＋ Количество отметок на одну историю", "Шаг 5 из 5"),
        "",
        "Сколько пользователей отмечать в <b>каждой</b> опубликованной истории?",
        "<i>Если подпись и отметки не помещаются, база будет разбита на большее число историй.</i>",
    ]
    await message.answer("\n".join(lines), reply_markup=story_tags_per_story_keyboard())


@story_router.callback_query(
    StoryTaggingCreateState.tags_per_story, F.data.regexp(r"^story:tags:\d+$")
)
async def process_story_tags_choice(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    tags_per_story = int(callback.data.split(":")[-1])
    await state.update_data(tags_per_story=tags_per_story)
    await state.set_state(StoryTaggingCreateState.total_tags)

    lines = [
        screen_title("＋ Общая цель отметок"),
        "",
        f"Отметок на историю: <b>{tags_per_story}</b>",
        "",
        "Сколько всего человек из базы необходимо отметить?",
        "<i>База будет разбита на истории и распределена между активными аккаунтами.</i>",
    ]
    await _edit_screen(callback, "\n".join(lines), story_total_tags_keyboard())


@story_router.callback_query(
    StoryTaggingCreateState.total_tags, F.data.regexp(r"^story:total:\d+$")
)
async def process_story_total_choice(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    total_tags = int(callback.data.split(":")[-1])
    data = await state.get_data()

    await state.update_data(total_tags=total_tags)
    await state.set_state(StoryTaggingCreateState.confirm)

    tags_per_story = data.get("tags_per_story", 5)
    stories_needed = max(1, (total_tags + tags_per_story - 1) // tags_per_story)

    lines = [
        screen_title("🚀 Подтверждение запуска теггинга"),
        "",
        f"• Название: <b>{escape(data.get('name', 'Теггинг'))}</b>",
        f"• База: <b>{escape(data.get('list_name', ''))}</b>",
        f"• Медиа: <b>{data.get('media_kind', 'photo').upper()}</b>",
        f"• Отметок на историю: <b>{tags_per_story}</b>",
        f"• Всего отметок: <b>{total_tags}</b>",
        f"• Будет создано историй: <b>~{stories_needed}</b>",
        "• Отправители: <b>Все активные аккаунты</b>",
        "",
        "Доставка уведомлений зависит от настроек Telegram получателя.",
        "Нажмите <b>«Запустить теггинг»</b> для старта:",
    ]
    await _edit_screen(callback, "\n".join(lines), story_confirm_keyboard())


@story_router.callback_query(StoryTaggingCreateState.confirm, F.data == "story:confirm:launch")
async def process_story_launch(
    callback: CallbackQuery,
    state: FSMContext,
    story_tagging: StoryTaggingService,
) -> None:
    await _safe_callback_answer(callback, "Создание кампании…")
    data = await state.get_data()
    if not all(key in data for key in ("name", "list_id", "media_path")):
        await state.clear()
        await _edit_screen(
            callback, "Мастер устарел. Создайте кампанию заново.", story_home_keyboard()
        )
        return
    try:
        campaign = await story_tagging.create_campaign(
            name=data["name"],
            subscriber_list_id=data["list_id"],
            media_path=data["media_path"],
            media_kind=data.get("media_kind", "photo"),
            media_name=data.get("media_name"),
            media_size=data.get("media_size"),
            caption=data.get("caption", ""),
            tags_per_story=data.get("tags_per_story", 5),
            total_target_tags=data.get("total_tags", 50),
            daily_limit_per_account=3,
            cooldown_seconds=120,
        )
    except Exception as exc:
        await _safe_callback_answer(callback, "Ошибка при создании")
        await _edit_screen(
            callback,
            f"❌ <b>Не удалось создать кампанию</b>: {escape(str(exc))}",
            story_home_keyboard(),
        )
        return

    await state.clear()
    await _safe_callback_answer(callback, "Кампания успешно запущена! 🚀")
    text = f"✅ <b>Кампания теггинга запущена!</b>\n\n{story_campaign_text(campaign)}"
    keyboard = story_campaign_keyboard(campaign)
    await _edit_screen(callback, text, keyboard)
