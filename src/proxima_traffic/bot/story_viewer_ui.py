"""Control bot UI and router for Telegram Story Viewing & Reaction campaigns."""

from __future__ import annotations

import asyncio
import logging
from html import escape
from typing import TYPE_CHECKING

from aiogram import F, Router
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
from proxima_traffic.bot.states import StoryViewerCreateState
from proxima_traffic.bot.story_ui import add_story_pages, story_mode_label, story_next_run_text
from proxima_traffic.bot.ui import fit_panel_text, polish_markup, screen_title, section
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.models import (
    Account,
    StoryViewerCampaignStatus,
    SubscriberList,
    ensure_utc,
    utc_now,
)
from proxima_traffic.story_content import normalize_story_reactions
from proxima_traffic.story_viewer import (
    StoryViewerAccountView,
    StoryViewerCampaignView,
    StoryViewerDashboard,
    StoryViewerService,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass

story_viewer_router = Router(name="story_viewer")

PAGE_SIZE = 8


# ---------------------------------------------------------------------------
# Presentation & Layouts
# ---------------------------------------------------------------------------


def story_viewer_home_text(dashboard: StoryViewerDashboard) -> str:
    pending = max(
        0,
        dashboard.total_tasks
        - dashboard.succeeded_tasks
        - dashboard.skipped_tasks
        - dashboard.failed_tasks,
    )
    lines = [
        screen_title(
            "👀 Просмотр и реакции на истории",
            "Массовый просмотр историй и реакции из базы подписчиков",
        ),
        "",
        "Просмотр активных историй из базы с настраиваемыми задержками и реакциями. "
        "Уведомления о реакциях зависят от настроек Telegram получателя. "
        "что привлекает целевой трафик на ваш профиль.",
        "",
        section("Статистика просмотров"),
        f"• Активных кампаний: <b>{dashboard.active_campaign_count}</b>"
        f" / {dashboard.campaign_count}",
        f"• Просмотрено историй: <b>{dashboard.total_stories_viewed:,}</b>",
        f"• Отправлено реакций: <b>{dashboard.total_reactions_sent:,}</b>",
        f"• Успешных задач: <b>{dashboard.succeeded_tasks:,}</b>",
        f"• Пропущено (нет историй/приватность): <b>{dashboard.skipped_tasks:,}</b>",
        f"• В очереди на просмотр: <b>{pending:,}</b>",
        f"• Режим работы: <b>{'Симуляция' if dashboard.simulated else 'Реальные просмотры'}</b>",
    ]
    return "\n".join(lines)


def story_viewer_home_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="＋ Создать кампанию просмотра",
            callback_data="story_view:new",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📋 Список кампаний",
            callback_data="story_view:list:0",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="« Главное меню",
            callback_data="menu:home",
        )
    )
    return polish_markup(builder.as_markup())


def story_viewer_list_keyboard(
    campaigns: list[StoryViewerCampaignView], page: int, total: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for camp in campaigns:
        status_emoji = (
            "🟢"
            if camp.status == StoryViewerCampaignStatus.ACTIVE
            else "⏸"
            if camp.status == StoryViewerCampaignStatus.PAUSED
            else "🏁"
        )
        builder.row(
            InlineKeyboardButton(
                text=(
                    f"{status_emoji} {camp.name} ({camp.stories_viewed} 👀, "
                    f"{camp.reactions_sent} {camp.reaction_emoji})"
                ),
                callback_data=f"story_view:view:{camp.id}",
            )
        )

    nav_buttons: list[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(text="« Назад", callback_data=f"story_view:list:{page - 1}")
        )
    if (page + 1) * PAGE_SIZE < total:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперёд »", callback_data=f"story_view:list:{page + 1}")
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    builder.row(
        InlineKeyboardButton(text="＋ Новая кампания", callback_data="story_view:new"),
        InlineKeyboardButton(text="« Меню просмотров", callback_data="story_view:home"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_campaign_text(campaign: StoryViewerCampaignView) -> str:
    reaction = f"Да ({escape(campaign.reaction_emoji)})" if campaign.send_reaction else "Нет"
    checked = campaign.succeeded_tasks + campaign.skipped_tasks + campaign.failed_tasks
    status_ru = {
        StoryViewerCampaignStatus.ACTIVE: "🟢 Активна (просматривается)",
        StoryViewerCampaignStatus.PAUSED: "⏸ Приостановлена",
        StoryViewerCampaignStatus.COMPLETED: "🏁 Завершена",
        StoryViewerCampaignStatus.DRAFT: "📝 Черновик",
    }.get(campaign.status, str(campaign.status))

    lines = [
        screen_title(
            f"Кампания: {campaign.name}",
            f"Статус: {status_ru}",
        ),
        "",
        section("Параметры кампании"),
        f"• База подписчиков: <b>{escape(campaign.subscriber_list_name)}</b>",
        f"• Отправка реакций: <b>{reaction}</b>",
        f"• Задержка: <b>{campaign.delay_min_seconds} – {campaign.delay_max_seconds} сек.</b>",
        f"• Лимит на аккаунт в день: <b>{campaign.daily_limit_per_account}</b> историй",
        f"• Цель выборки: <b>{campaign.max_targets:,} чел.</b>",
        "",
        section("Прогресс выполнения"),
        f"• Проверено пользователей: <b>{checked:,}</b> / {campaign.total_tasks:,}",
        f"• Просмотрено историй: <b>{campaign.stories_viewed:,}</b>",
        f"• Поставлено реакций: <b>{campaign.reactions_sent:,}</b>",
        f"• Пропущено (нет активных историй/приватность): <b>{campaign.skipped_tasks:,}</b>",
        f"• Ошибок: <b>{campaign.failed_tasks}</b>",
    ]

    if campaign.last_error:
        lines.extend(
            ["", section("Последняя ошибка / причина ожидания"), escape(campaign.last_error)]
        )
    lines.append(
        f"Режим кампании: <b>{story_mode_label(campaign.simulated, 'Реальные просмотры')}</b>"
    )
    if scheduled := story_next_run_text(campaign.next_run_at):
        lines.extend(["", scheduled])
    if campaign.accounts:
        lines.append("")
        lines.append(section(f"Аккаунты в кампании ({len(campaign.accounts)} шт.)"))
        now = utc_now()
        for a in campaign.accounts[:12]:
            badge = "🔴" if a.is_paused else ("⏸" if not a.enabled else "🟢")
            label = a.label or a.phone
            cooldown = ""
            until = ensure_utc(a.cooldown_until)
            if until and until > now:
                mins = int((until - now).total_seconds() // 60) + 1
                cooldown = f" <i>(пауза {mins} мин)</i>"
            lines.append(
                f"• {badge} <b>{escape(label)}</b>: {a.views_today} 👀"
                f" / {a.reactions_today} ❤️{cooldown}"
            )
        if len(campaign.accounts) > 12:
            lines.append(f"<i>...и ещё {len(campaign.accounts) - 12} аккаунтов</i>")

    return "\n".join(lines)


def story_viewer_campaign_keyboard(campaign: StoryViewerCampaignView) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if campaign.status == StoryViewerCampaignStatus.ACTIVE:
        builder.row(
            InlineKeyboardButton(
                text="⏸ Приостановить",
                callback_data=f"story_view:pause:{campaign.id}",
            )
        )
    elif campaign.status == StoryViewerCampaignStatus.PAUSED:
        builder.row(
            InlineKeyboardButton(
                text="▶ Возобновить",
                callback_data=f"story_view:resume:{campaign.id}",
            )
        )

    builder.row(
        InlineKeyboardButton(
            text="👥 Аккаунты", callback_data=f"story_view:manage_accs:{campaign.id}"
        ),
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"story_view:view:{campaign.id}"),
    )
    builder.row(
        InlineKeyboardButton(
            text="🗑 Удалить кампанию", callback_data=f"story_view:delete:{campaign.id}"
        ),
    )
    builder.row(
        InlineKeyboardButton(text="« К списку кампаний", callback_data="story_view:list:0"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_manage_accs_keyboard(
    campaign_id: int, accounts: list[StoryViewerAccountView], page: int = 0
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for acc in accounts[page * 20 : (page + 1) * 20]:
        badge = "🟢" if acc.enabled else "⚪"
        label = acc.label or acc.phone
        builder.row(
            InlineKeyboardButton(
                text=f"{badge} {label} ({acc.views_today} 👀)",
                callback_data=f"story_view:toggle_acc:{campaign_id}:{acc.account_id}:{page}",
            )
        )
    add_story_pages(builder, page, len(accounts), f"story_view:manage_accs:{campaign_id}:page")
    builder.row(
        InlineKeyboardButton(
            text="« Назад к кампании", callback_data=f"story_view:view:{campaign_id}"
        ),
    )
    return polish_markup(builder.as_markup())


def story_viewer_select_list_keyboard(
    lists: list[SubscriberList], page: int = 0, total: int = 0
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for sub_list in lists[:20]:
        builder.row(
            InlineKeyboardButton(
                text=f"📁 {sub_list.name} ({sub_list.member_count:,} чел.)",
                callback_data=f"story_view:set_list:{sub_list.id}",
            )
        )
    add_story_pages(builder, page, total, "story_view:lists")
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_reaction_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔥 Просмотр + Реакция (Рекомендуется)",
            callback_data="story_view:reaction:yes",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="👀 Только просмотр историй",
            callback_data="story_view:reaction:no",
        )
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_emoji_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔥 Огонь", callback_data="story_view:emoji:🔥"),
        InlineKeyboardButton(text="❤️ Сердце", callback_data="story_view:emoji:❤️"),
        InlineKeyboardButton(text="👍 Лайк", callback_data="story_view:emoji:👍"),
    )
    builder.row(
        InlineKeyboardButton(text="⚡ Молния", callback_data="story_view:emoji:⚡"),
        InlineKeyboardButton(text="🎉 Праздник", callback_data="story_view:emoji:🎉"),
        InlineKeyboardButton(text="😍 Влюблён", callback_data="story_view:emoji:😍"),
    )
    builder.row(
        InlineKeyboardButton(
            text="🎲 Случайная из набора",
            callback_data="story_view:emoji:random",
        )
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_delays_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="⚡ 3 – 8 сек. (Быстрый)",
            callback_data="story_view:delay:3:8",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⚡ 5 – 15 сек. (Оптимальный - Рекомендуется)",
            callback_data="story_view:delay:5:15",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="🛡 10 – 25 сек. (Безопасный)",
            callback_data="story_view:delay:10:25",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="🛡 20 – 45 сек. (Максимально плавный)",
            callback_data="story_view:delay:20:45",
        )
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_limits_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="50 / день", callback_data="story_view:limit:50"),
        InlineKeyboardButton(text="100 / день (Стандарт)", callback_data="story_view:limit:100"),
    )
    builder.row(
        InlineKeyboardButton(text="250 / день", callback_data="story_view:limit:250"),
        InlineKeyboardButton(text="500 / день (Максимум)", callback_data="story_view:limit:500"),
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_targets_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="100 чел.", callback_data="story_view:targets:100"),
        InlineKeyboardButton(text="250 чел.", callback_data="story_view:targets:250"),
        InlineKeyboardButton(text="500 чел.", callback_data="story_view:targets:500"),
    )
    builder.row(
        InlineKeyboardButton(text="1 000 чел.", callback_data="story_view:targets:1000"),
        InlineKeyboardButton(text="2 500 чел.", callback_data="story_view:targets:2500"),
        InlineKeyboardButton(text="5 000 чел.", callback_data="story_view:targets:5000"),
    )
    builder.row(
        InlineKeyboardButton(
            text="Все доступные (до 20 000)", callback_data="story_view:targets:20000"
        ),
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_accounts_pick_keyboard(
    accounts: list[Account], selected_ids: set[int], page: int = 0
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=f"🚀 Все активные аккаунты ({len(accounts)} шт.)",
            callback_data="story_view:acc:all",
        )
    )
    for acc in accounts[page * 20 : (page + 1) * 20]:
        is_sel = acc.id in selected_ids
        icon = "🔘" if is_sel else "⚪"
        label = acc.label or acc.phone
        builder.row(
            InlineKeyboardButton(
                text=f"{icon} {label}",
                callback_data=f"story_view:acc:toggle:{acc.id}",
            )
        )
    add_story_pages(builder, page, len(accounts), "story_view:acc:page")
    if selected_ids:
        builder.row(
            InlineKeyboardButton(
                text=f"✅ Подтвердить выбранные ({len(selected_ids)} шт.) »",
                callback_data="story_view:acc:done",
            )
        )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


def story_viewer_confirm_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🚀 Запустить просмотр историй",
            callback_data="story_view:launch",
        )
    )
    builder.row(
        InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"),
    )
    return polish_markup(builder.as_markup())


# ---------------------------------------------------------------------------
# Helpers
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
            logger.warning("TelegramBadRequest in story_viewer _edit_screen: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error in story_viewer _edit_screen: %s", exc)
    finally:
        record_callback_telegram_api(api_started_at, operation="edit")
        mark_callback_screen_updated()
        if answer_task is not None:
            await answer_task


# ---------------------------------------------------------------------------
# Router Handlers
# ---------------------------------------------------------------------------


@story_viewer_router.callback_query(F.data == "story_view:home")
async def handle_story_viewer_home(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    dashboard = await story_viewer.get_dashboard()
    text = story_viewer_home_text(dashboard)
    await _edit_screen(callback, text, story_viewer_home_keyboard())


@story_viewer_router.callback_query(F.data == "story_view:cancel")
async def handle_story_viewer_cancel(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback, "Отменено")
    await state.clear()
    dashboard = await story_viewer.get_dashboard()
    text = story_viewer_home_text(dashboard)
    await _edit_screen(callback, text, story_viewer_home_keyboard())


@story_viewer_router.callback_query(F.data.startswith("story_view:list:"))
async def handle_story_viewer_list(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    parts = callback.data.split(":") if callback.data else []
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0

    campaigns = await story_viewer.list_campaigns(limit=PAGE_SIZE, offset=page * PAGE_SIZE)
    dashboard = await story_viewer.get_dashboard()

    lines = [
        screen_title("📋 Кампании просмотра историй", f"Страница {page + 1}"),
        "",
        f"Всего кампаний: <b>{dashboard.campaign_count}</b>"
        f" (активных: {dashboard.active_campaign_count})",
        f"Всего просмотрено историй: <b>{dashboard.total_stories_viewed:,}</b>",
        f"Всего отправлено реакций: <b>{dashboard.total_reactions_sent:,}</b>",
        "",
    ]
    if not campaigns:
        lines.append("<i>Кампаний пока нет. Создайте первую кампанию для запуска просмотра!</i>")
    else:
        lines.append("Выберите кампанию для просмотра деталей или управления:")

    await _edit_screen(
        callback,
        "\n".join(lines),
        story_viewer_list_keyboard(campaigns, page, dashboard.campaign_count),
    )


@story_viewer_router.callback_query(F.data.startswith("story_view:view:"))
async def handle_story_viewer_view(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return

    camp_id = int(parts[2])
    campaign = await story_viewer.get_campaign(camp_id)
    if campaign is None:
        await _safe_callback_answer(callback, "Кампания не найдена")
        return

    text = story_viewer_campaign_text(campaign)
    await _edit_screen(callback, text, story_viewer_campaign_keyboard(campaign))


@story_viewer_router.callback_query(F.data.startswith("story_view:manage_accs:"))
async def handle_story_viewer_manage_accs(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return
    camp_id = int(parts[2])
    accounts = await story_viewer.list_accounts_for_campaign(camp_id)
    page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
    text = (
        screen_title("👥 Управление аккаунтами кампании")
        + "\n\n"
        + "Нажмите на аккаунт для включения (🟢) или отключения (⚪):"
    )
    await _edit_screen(callback, text, story_viewer_manage_accs_keyboard(camp_id, accounts, page))


@story_viewer_router.callback_query(F.data.startswith("story_view:toggle_acc:"))
async def handle_story_viewer_toggle_acc(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 4 or not parts[2].isdigit() or not parts[3].isdigit():
        return
    camp_id = int(parts[2])
    acc_id = int(parts[3])
    page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
    enabled = await story_viewer.toggle_campaign_account(camp_id, acc_id)
    await _safe_callback_answer(callback, "Включён" if enabled else "Отключён")
    accounts = await story_viewer.list_accounts_for_campaign(camp_id)
    text = (
        screen_title("👥 Управление аккаунтами кампании")
        + "\n\n"
        + "Нажмите на аккаунт для включения (🟢) или отключения (⚪):"
    )
    await _edit_screen(callback, text, story_viewer_manage_accs_keyboard(camp_id, accounts, page))


@story_viewer_router.callback_query(F.data.startswith("story_view:pause:"))
async def handle_story_viewer_pause(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return
    camp_id = int(parts[2])
    success = await story_viewer.pause_campaign(camp_id)
    await _safe_callback_answer(callback, "Кампания приостановлена" if success else "Ошибка")
    campaign = await story_viewer.get_campaign(camp_id)
    if campaign:
        await _edit_screen(
            callback,
            story_viewer_campaign_text(campaign),
            story_viewer_campaign_keyboard(campaign),
        )


@story_viewer_router.callback_query(F.data.startswith("story_view:resume:"))
async def handle_story_viewer_resume(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return
    camp_id = int(parts[2])
    success = await story_viewer.resume_campaign(camp_id)
    await _safe_callback_answer(callback, "Кампания возобновлена" if success else "Ошибка")
    campaign = await story_viewer.get_campaign(camp_id)
    if campaign:
        await _edit_screen(
            callback,
            story_viewer_campaign_text(campaign),
            story_viewer_campaign_keyboard(campaign),
        )


@story_viewer_router.callback_query(F.data.startswith("story_view:delete:"))
async def handle_story_viewer_delete(
    callback: CallbackQuery,
    story_viewer: StoryViewerService,
) -> None:
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return
    camp_id = int(parts[2])
    try:
        await story_viewer.delete_campaign(camp_id)
    except ValueError as exc:
        await _safe_callback_answer(callback, str(exc), show_alert=True)
        return
    await _safe_callback_answer(callback, "Кампания удалена")

    campaigns = await story_viewer.list_campaigns(limit=PAGE_SIZE, offset=0)
    dashboard = await story_viewer.get_dashboard()
    lines = [
        screen_title("📋 Кампании просмотра историй", "Страница 1"),
        "",
        "Кампания успешно удалена.",
    ]
    await _edit_screen(
        callback,
        "\n".join(lines),
        story_viewer_list_keyboard(campaigns, 0, dashboard.campaign_count),
    )


# ---------------------------------------------------------------------------
# Creation Wizard
# ---------------------------------------------------------------------------


@story_viewer_router.callback_query(F.data == "story_view:new")
async def handle_story_viewer_new(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    await state.clear()
    await state.set_state(StoryViewerCreateState.name)

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="« Отмена", callback_data="story_view:cancel"))

    text = (
        screen_title("Шаг 1 из 7: Название кампании")
        + "\n\n"
        + "Введите название кампании (например, <code>Просмотр ЦА Инвесторы</code>):"
    )
    await _edit_screen(callback, text, polish_markup(builder.as_markup()))


@story_viewer_router.message(StoryViewerCreateState.name)
async def handle_story_viewer_name_input(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    name = (message.text or "").strip()
    if not 2 <= len(name) <= 100:
        await message.answer("Пожалуйста, введите корректное название (от 2 до 100 символов).")
        return

    await state.update_data(name=name)
    await state.set_state(StoryViewerCreateState.subscriber_list)

    lists, total = await direct_messages.list_ready_subscriber_lists(0, 20)
    if not lists:
        await message.answer(
            "Нет готовых баз. Загрузите базу в разделе «База подписчиков».",
            reply_markup=polish_markup(
                InlineKeyboardBuilder()
                .row(InlineKeyboardButton(text="« Назад", callback_data="story_view:home"))
                .as_markup()
            ),
        )
        await state.clear()
        return

    text = (
        screen_title("Шаг 2 из 7: Выбор базы подписчиков")
        + "\n\n"
        + f"Кампания: <b>{escape(name)}</b>\n\n"
        + "Выберите базу подписчиков, чьи активные истории будут просматриваться:"
    )
    await message.answer(
        fit_panel_text(text),
        reply_markup=story_viewer_select_list_keyboard(lists, total=total),
    )


@story_viewer_router.callback_query(
    StoryViewerCreateState.subscriber_list, F.data.regexp(r"^story_view:lists:\d+$")
)
async def handle_story_view_list_page(
    callback: CallbackQuery, direct_messages: DirectMessageService
) -> None:
    await _safe_callback_answer(callback)
    page = int(callback.data.split(":")[-1])
    lists, total = await direct_messages.list_ready_subscriber_lists(page, 20)
    await _edit_screen(
        callback,
        screen_title("Выбор базы подписчиков"),
        story_viewer_select_list_keyboard(lists, page, total),
    )


@story_viewer_router.callback_query(
    StoryViewerCreateState.subscriber_list, F.data.startswith("story_view:set_list:")
)
async def handle_story_viewer_set_list(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 3 or not parts[2].isdigit():
        return
    list_id = int(parts[2])

    sub_list = await direct_messages.get_subscriber_list(list_id)
    if sub_list is None:
        await _safe_callback_answer(callback, "База не найдена")
        return

    await state.update_data(subscriber_list_id=list_id, subscriber_list_name=sub_list.name)
    await state.set_state(StoryViewerCreateState.send_reaction)

    text = (
        screen_title("Шаг 3 из 7: Режим реакций")
        + "\n\n"
        + f"База подписчиков: <b>{escape(sub_list.name)}</b> ({sub_list.member_count:,} чел.)\n\n"
        + "Желаете ли вы отправлять реакцию (лайк) на активную историю пользователя?\n\n"
        + "• <b>Просмотр + Реакция</b>: просмотр и реакция на последнюю выбранную историю. "
        "Максимальная конверсия и внимание к вашему профилю!\n"
        + "• <b>Только просмотр</b>: засчитывается просмотр истории без реакции."
    )
    await _edit_screen(callback, text, story_viewer_reaction_keyboard())


@story_viewer_router.callback_query(
    StoryViewerCreateState.send_reaction, F.data.startswith("story_view:reaction:")
)
async def handle_story_viewer_reaction_mode(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    send_rx = parts[2] == "yes"
    await state.update_data(send_reaction=send_rx)

    if send_rx:
        await state.set_state(StoryViewerCreateState.reaction_emoji)
        text = (
            screen_title("Шаг 3.1: Выбор эмодзи реакции")
            + "\n\n"
            + "Выберите эмодзи реакции, которая будет ставиться на просмотренные истории, "
            + "или напишите свой эмодзи (или несколько через запятую):"
        )
        await _edit_screen(callback, text, story_viewer_emoji_keyboard())
    else:
        await state.update_data(reaction_emoji="🔥")
        await state.set_state(StoryViewerCreateState.delay_range)
        text = (
            screen_title("Шаг 4 из 7: Задержка между просмотрами")
            + "\n\n"
            + "Выберите интервал случайных задержек между просмотрами историй с одного аккаунта:"
        )
        await _edit_screen(callback, text, story_viewer_delays_keyboard())


@story_viewer_router.callback_query(
    StoryViewerCreateState.reaction_emoji, F.data.startswith("story_view:emoji:")
)
async def handle_story_viewer_emoji_choice(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    chosen = parts[2] if len(parts) > 2 else "🔥"
    await state.update_data(reaction_emoji=chosen)

    await state.set_state(StoryViewerCreateState.delay_range)
    text = (
        screen_title("Шаг 4 из 7: Задержка между действиями")
        + "\n\n"
        + f"Выбрана реакция: <b>{chosen}</b>\n\n"
        + "Выберите интервал случайных задержек между действиями с одного аккаунта:"
    )
    await _edit_screen(callback, text, story_viewer_delays_keyboard())


@story_viewer_router.message(StoryViewerCreateState.reaction_emoji)
async def handle_story_viewer_emoji_text_input(
    message: Message,
    state: FSMContext,
) -> None:
    custom_emoji = (message.text or "").strip()
    try:
        custom_emoji = normalize_story_reactions(custom_emoji)
    except ValueError as exc:
        await message.answer(escape(str(exc)), reply_markup=story_viewer_emoji_keyboard())
        return
    await state.update_data(reaction_emoji=custom_emoji)

    await state.set_state(StoryViewerCreateState.delay_range)
    text = (
        screen_title("Шаг 4 из 7: Задержка между действиями")
        + "\n\n"
        + f"Выбрана реакция: <b>{custom_emoji}</b>\n\n"
        + "Выберите интервал случайных задержек между действиями с одного аккаунта:"
    )
    await message.answer(
        fit_panel_text(text),
        reply_markup=story_viewer_delays_keyboard(),
    )


@story_viewer_router.callback_query(
    StoryViewerCreateState.delay_range, F.data.startswith("story_view:delay:")
)
async def handle_story_viewer_delay_choice(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    min_delay = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 5
    max_delay = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 15

    await state.update_data(delay_min=min_delay, delay_max=max_delay)
    await state.set_state(StoryViewerCreateState.daily_limit)

    text = (
        screen_title("Шаг 5 из 7: Дневной лимит на аккаунт")
        + "\n\n"
        + f"Задержка: <b>{min_delay} – {max_delay} сек.</b>\n\n"
        + "Сколько просмотров историй в сутки может совершать один аккаунт? "
        + "(При достижении лимита аккаунт безопасно отдыхает до следующего дня, "
        + "а работа продолжается другими активными аккаунтами):"
    )
    await _edit_screen(callback, text, story_viewer_limits_keyboard())


@story_viewer_router.callback_query(
    StoryViewerCreateState.daily_limit, F.data.startswith("story_view:limit:")
)
async def handle_story_viewer_limit_choice(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    limit = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 100
    await state.update_data(daily_limit=limit)

    await state.set_state(StoryViewerCreateState.max_targets)
    text = (
        screen_title("Шаг 6 из 7: Объем пользователей для проверки")
        + "\n\n"
        + f"Лимит на аккаунт: <b>{limit} историй/день</b>\n\n"
        + "Сколько пользователей из выбранной базы проверить на наличие активных историй?"
    )
    await _edit_screen(callback, text, story_viewer_targets_keyboard())


@story_viewer_router.callback_query(
    StoryViewerCreateState.max_targets, F.data.startswith("story_view:targets:")
)
async def handle_story_viewer_targets_choice(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    parts = callback.data.split(":") if callback.data else []
    targets = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 500
    await state.update_data(max_targets=targets)

    # Move to accounts selection step
    await state.set_state(StoryViewerCreateState.accounts)
    accounts = await story_viewer.list_available_accounts()

    text = (
        screen_title("Шаг 7 из 7: Выбор аккаунтов для просмотра")
        + "\n\n"
        + f"Доступно активных аккаунтов: <b>{len(accounts)} шт.</b>\n\n"
        + "Выберите, какие аккаунты будут смотреть истории и ставить реакции. "
        + "Вы можете использовать сразу все аккаунты или выбрать конкретные:"
    )
    selected_ids = {a.id for a in accounts}
    await state.update_data(selected_account_ids=list(selected_ids))
    await _edit_screen(
        callback, text, story_viewer_accounts_pick_keyboard(list(accounts), selected_ids)
    )


@story_viewer_router.callback_query(
    StoryViewerCreateState.accounts, F.data.regexp(r"^story_view:acc:page:\d+$")
)
async def handle_story_viewer_account_page(
    callback: CallbackQuery, state: FSMContext, story_viewer: StoryViewerService
) -> None:
    await _safe_callback_answer(callback)
    page = int(callback.data.split(":")[-1])
    accounts = await story_viewer.list_available_accounts()
    data = await state.get_data()
    await state.update_data(account_page=page)
    await _edit_screen(
        callback,
        screen_title("Выбор аккаунтов"),
        story_viewer_accounts_pick_keyboard(
            accounts, set(data.get("selected_account_ids", [])), page
        ),
    )


@story_viewer_router.callback_query(StoryViewerCreateState.accounts, F.data == "story_view:acc:all")
async def handle_story_viewer_acc_all(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback)
    accounts = await story_viewer.list_available_accounts()

    all_ids = [a.id for a in accounts]
    await state.update_data(account_ids=all_ids)
    await _show_confirm_screen(callback, state)


@story_viewer_router.callback_query(
    StoryViewerCreateState.accounts, F.data.startswith("story_view:acc:toggle:")
)
async def handle_story_viewer_acc_toggle(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    parts = callback.data.split(":") if callback.data else []
    if len(parts) < 4 or not parts[3].isdigit():
        return
    acc_id = int(parts[3])

    data = await state.get_data()
    selected_ids = set(data.get("selected_account_ids", []))
    if acc_id in selected_ids:
        selected_ids.remove(acc_id)
        await _safe_callback_answer(callback, "Аккаунт убран")
    else:
        selected_ids.add(acc_id)
        await _safe_callback_answer(callback, "Аккаунт добавлен")

    await state.update_data(selected_account_ids=list(selected_ids))

    accounts = await story_viewer.list_available_accounts()

    text = (
        screen_title("Шаг 7 из 7: Выбор аккаунтов для просмотра")
        + "\n\n"
        + f"Выбрано аккаунтов: <b>{len(selected_ids)}</b> из {len(accounts)}\n\n"
        + "Нажимайте на аккаунты для переключения, затем нажмите «Подтвердить выбранные»:"
    )
    await _edit_screen(
        callback,
        text,
        story_viewer_accounts_pick_keyboard(
            list(accounts), selected_ids, data.get("account_page", 0)
        ),
    )


@story_viewer_router.callback_query(
    StoryViewerCreateState.accounts, F.data == "story_view:acc:done"
)
async def handle_story_viewer_acc_done(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await _safe_callback_answer(callback)
    data = await state.get_data()
    selected_ids = data.get("selected_account_ids", [])
    if not selected_ids:
        await _safe_callback_answer(callback, "Выберите хотя бы один аккаунт!")
        return

    await state.update_data(account_ids=selected_ids)
    await _show_confirm_screen(callback, state)


async def _show_confirm_screen(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(StoryViewerCreateState.confirm)

    name = data.get("name", "Кампания")
    list_name = data.get("subscriber_list_name", "")
    send_rx = data.get("send_reaction", True)
    emoji = data.get("reaction_emoji", "🔥")
    delay_min = data.get("delay_min", 5)
    delay_max = data.get("delay_max", 15)
    daily_limit = data.get("daily_limit", 100)
    targets = data.get("max_targets", 500)
    acc_ids = data.get("account_ids", [])

    lines = [
        screen_title("Подтверждение запуска кампании"),
        "",
        section("Параметры запуска"),
        f"• Название: <b>{escape(name)}</b>",
        f"• База подписчиков: <b>{escape(list_name)}</b>",
        f"• Режим реакций: <b>{'Да (' + escape(emoji) + ')' if send_rx else 'Только просмотр'}</b>",
        f"• Задержка между действиями: <b>{delay_min} – {delay_max} сек.</b>",
        f"• Лимит на аккаунт: <b>{daily_limit} историй/день</b>",
        f"• Объем выборки: <b>{targets:,} чел.</b>",
        f"• Выделено аккаунтов: <b>{len(acc_ids)} шт.</b>",
        "",
        "Воркер запустит параллельный просмотр историй с автоматической ротацией, "
        "чередованием активных аккаунтов и защитой от Flood Wait.",
        "Нажмите кнопку ниже для старта:",
    ]
    await _edit_screen(callback, "\n".join(lines), story_viewer_confirm_keyboard())


@story_viewer_router.callback_query(StoryViewerCreateState.confirm, F.data == "story_view:launch")
async def handle_story_viewer_launch(
    callback: CallbackQuery,
    state: FSMContext,
    story_viewer: StoryViewerService,
) -> None:
    await _safe_callback_answer(callback, "Создание кампании...")
    data = await state.get_data()
    if not all(key in data for key in ("name", "subscriber_list_id", "account_ids")):
        await state.clear()
        await _edit_screen(
            callback, "Мастер устарел. Создайте кампанию заново.", story_viewer_home_keyboard()
        )
        return

    name = data["name"]
    list_id = data["subscriber_list_id"]
    send_rx = data.get("send_reaction", True)
    emoji = data.get("reaction_emoji", "🔥")
    delay_min = data.get("delay_min", 5)
    delay_max = data.get("delay_max", 15)
    daily_limit = data.get("daily_limit", 100)
    targets = data.get("max_targets", 500)
    acc_ids = data.get("account_ids")

    try:
        campaign = await story_viewer.create_campaign(
            name=name,
            subscriber_list_id=list_id,
            send_reaction=send_rx,
            reaction_emoji=emoji,
            delay_min_seconds=delay_min,
            delay_max_seconds=delay_max,
            daily_limit_per_account=daily_limit,
            max_targets=targets,
            account_ids=acc_ids,
            auto_start=True,
        )
    except Exception as exc:  # noqa: BLE001
        await _edit_screen(
            callback,
            screen_title("Ошибка создания кампании")
            + f"\n\nНе удалось запустить кампанию: {escape(str(exc))}",
            polish_markup(
                InlineKeyboardBuilder()
                .row(
                    InlineKeyboardButton(text="« Меню просмотров", callback_data="story_view:home")
                )
                .as_markup()
            ),
        )
        return

    await state.clear()
    text = (
        screen_title("🚀 Кампания успешно запущена!")
        + "\n\n"
        + story_viewer_campaign_text(campaign)
    )
    await _edit_screen(callback, text, story_viewer_campaign_keyboard(campaign))
