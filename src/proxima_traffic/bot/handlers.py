from __future__ import annotations

import asyncio
import logging
import re
from contextlib import suppress
from dataclasses import dataclass
from html import escape
from io import BytesIO
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)

from proxima_traffic.accounts import (
    MAX_PROFILE_PHOTO_BYTES,
    AccountImportError,
    AccountProfileError,
    AccountService,
    ChatFolderError,
)
from proxima_traffic.bot.channel_comment_ui import (
    channel_comment_account_picker,
    channel_comment_chain_menu,
    channel_comment_confirmation_menu,
    channel_comment_delete_menu,
    channel_comment_details_menu,
    channel_comment_details_text,
    channel_comment_draft_text,
    channel_comment_mention_cooldown_menu,
    channel_comment_mode_menu,
    channel_comment_reaction_account_ids,
    channel_comment_reaction_account_picker,
    channel_comment_reactions_menu,
    channel_comment_reactions_text,
    channel_comment_runs_text,
    channel_comments_menu,
    channel_comments_text,
)
from proxima_traffic.bot.direct_message_ui import (
    direct_account_picker,
    direct_account_picker_text,
    direct_blacklist_add_prompt_text,
    direct_blacklist_menu,
    direct_blacklist_text,
    direct_campaign_confirmation_menu,
    direct_campaign_confirmation_text,
    direct_campaign_details_menu,
    direct_campaigns_menu,
    direct_campaigns_text,
    direct_cancel_menu,
    direct_content_prompt_text,
    direct_cooldown_menu,
    direct_cooldown_text,
    direct_launch_cancel_menu,
    direct_launch_details_menu,
    direct_launch_details_text,
    direct_launches_menu,
    direct_launches_text,
    direct_limit_text,
    direct_list_picker,
    direct_list_picker_text,
    direct_messages_home_menu,
    direct_messages_home_text,
    direct_segment_menu,
    direct_segment_text,
    subscriber_import_prompt_text,
    subscriber_import_result_menu,
    subscriber_import_result_text,
    subscriber_list_delete_menu,
    subscriber_list_details_menu,
    subscriber_list_details_text,
    subscriber_lists_menu,
    subscriber_lists_text,
)
from proxima_traffic.bot.keyboards import (
    account_about_bulk_confirmation,
    account_about_bulk_menu,
    account_about_confirmation,
    account_avatar_delete_confirmation,
    account_avatar_menu,
    account_delete_confirmation,
    account_details_menu,
    account_generated_names_confirmation,
    account_import_result_menu,
    account_import_sources_menu,
    account_name_catalog_menu,
    account_name_confirmation,
    account_picker,
    account_session_clear_confirmation,
    accounts_menu,
    audiences_menu,
    audit_menu,
    bad_target_details_menu,
    bad_targets_menu,
    campaign_advanced_menu,
    campaign_audience_source_menu,
    campaign_confirmation,
    campaign_content_edit_cancel,
    campaign_delete_confirmation,
    campaign_details_menu,
    campaign_edit_target_picker,
    campaign_edit_target_source_menu,
    campaign_mention_cooldown_menu,
    campaign_mode_picker,
    campaign_one_off_window_presets,
    campaign_reactions_menu,
    campaign_repeat_count_presets,
    campaign_repeat_interval_presets,
    campaign_repeat_mode_picker,
    campaign_run_confirmation,
    campaign_schedule_mode_picker,
    campaign_sender_picker,
    campaign_smart_duration_presets,
    campaign_smart_interval_presets,
    campaign_standalone_target_picker,
    campaign_target_list_picker,
    campaign_target_picker,
    campaign_variant_delete_confirmation,
    campaign_variant_input_cancel,
    campaign_variant_menu,
    campaign_variants_menu,
    campaigns_menu,
    cancel_menu,
    dialogue_chain_menu,
    guide_menu,
    launch_cancel_confirmation,
    launch_details_menu,
    launch_edit_cancel,
    launch_retry_confirmation,
    launches_menu,
    main_menu,
    order_activity_check_presets,
    order_advanced_menu,
    order_audience_search_menu,
    order_audience_source_menu,
    order_back_menu,
    order_draft_menu,
    order_duration_presets,
    order_interval_presets,
    order_launch_confirmation,
    order_sender_picker,
    order_spread_presets,
    order_target_list_picker,
    order_target_picker,
    order_type_menu,
    order_visibility_presets,
    order_visibility_wait_presets,
    ownership_confirmation,
    proxies_menu,
    proxy_delete_confirmation,
    proxy_details_menu,
    proxy_group_delete_confirmation,
    proxy_groups_menu,
    run_details_menu,
    search_prompt_menu,
    search_results_menu,
    settings_menu,
    stop_all_confirmation,
    target_cooldown_picker,
    target_delete_confirmation,
    target_details_menu,
    target_folder_account_picker,
    target_list_delete_confirmation,
    target_list_details_menu,
    target_list_target_details_menu,
    target_list_target_remove_confirmation,
    target_list_targets_menu,
    target_lists_menu,
    targets_menu,
    template_confirmation,
    template_delete_confirmation,
    template_details_menu,
    template_edit_confirmation,
    template_picker,
    templates_menu,
)
from proxima_traffic.bot.participant_reaction_ui import (
    participant_reaction_account_picker,
    participant_reaction_confirm_menu,
    participant_reaction_delete_menu,
    participant_reaction_details_menu,
    participant_reaction_details_text,
    participant_reaction_draft_text,
    participant_reaction_emoji_menu,
    participant_reaction_list_picker,
    participant_reaction_runs_text,
    participant_reaction_sample_menu,
    participant_reactions_menu,
    participant_reactions_text,
)
from proxima_traffic.bot.renderers import (
    account_avatar_text,
    account_details_text,
    account_generated_names_preview_text,
    account_name_catalog_text,
    account_name_confirmation_text,
    account_names_result_text,
    accounts_text,
    audiences_text,
    audit_text,
    bad_targets_text,
    campaign_advanced_text,
    campaign_confirmation_text,
    campaign_details_text,
    campaign_preflight_text,
    campaign_reactions_text,
    campaign_run_confirmation_text,
    campaign_variant_text,
    campaign_variants_text,
    campaigns_text,
    dashboard_text,
    guide_text,
    launch_cancel_confirmation_text,
    launch_details_text,
    launches_text,
    order_advanced_text,
    order_draft_text,
    order_type_text,
    proxies_text,
    proxy_details_text,
    proxy_groups_text,
    run_details_text,
    settings_text,
    target_details_text,
    target_list_details_text,
    target_list_targets_text,
    target_lists_text,
    targets_text,
    template_confirmation_text,
    template_details_text,
    templates_text,
    wizard_text,
)
from proxima_traffic.bot.repository import AdminRepository, DashboardStats, TargetSeed
from proxima_traffic.bot.states import (
    AccountAboutBulkState,
    AccountAboutEditState,
    AccountAvatarEditState,
    AccountBulkNameState,
    AccountCreateState,
    AccountImportState,
    AccountLabelEditState,
    AccountNameEditState,
    CampaignContentEditState,
    CampaignCreateState,
    CampaignEditState,
    CampaignScheduleEditState,
    CampaignVariantState,
    ChannelCommentCreateState,
    DirectBlacklistState,
    DirectCampaignCreateState,
    LaunchEditState,
    ListSearchState,
    OrderDraftState,
    ParticipantReactionCreateState,
    ParticipantReactionEditState,
    ProxyAddState,
    ProxyAssignState,
    ProxyAutoAssignState,
    ProxyEditState,
    ProxyGroupCreateState,
    ProxyImportState,
    ProxyMetadataEditState,
    ProxyRotationState,
    SubscriberImportState,
    SubscriberInviteCreateState,
    TargetCooldownState,
    TargetCreateState,
    TargetImportState,
    TargetListRenameState,
    TemplateCreateState,
    TemplateEditState,
)
from proxima_traffic.bot.subscriber_invite_ui import (
    invite_account_picker,
    invite_account_picker_text,
    invite_campaign_confirmation_menu,
    invite_campaign_details_menu,
    invite_campaign_text,
    invite_campaigns_menu,
    invite_campaigns_text,
    invite_cancel_menu,
    invite_cooldown_menu,
    invite_cooldown_text,
    invite_destination_prompt_text,
    invite_launch_cancel_menu,
    invite_launch_details_menu,
    invite_launch_text,
    invite_launches_menu,
    invite_launches_text,
    invite_limit_prompt_text,
    invite_list_picker,
    invite_list_picker_text,
    invite_segment_menu,
    invite_segment_text,
    subscriber_invites_home_menu,
    subscriber_invites_home_text,
)
from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.config import Settings
from proxima_traffic.content import (
    BroadcastContentVariant,
    ContentError,
    StoredContact,
    StoredMedia,
    new_stored_contact,
    render_spintax,
    resolve_media_path,
    store_media,
    validate_spintax,
)
from proxima_traffic.dialogues import DialogueStep, parse_dialogue_steps
from proxima_traffic.direct_messages import (
    DirectMessageService,
    parse_direct_blacklist_ids,
)
from proxima_traffic.maintenance import (
    create_sqlite_backup,
    inspect_sqlite,
    verify_sqlite_backup,
)
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    BroadcastSenderMode,
    Campaign,
    CampaignMode,
    CampaignRun,
    ChannelCommentMode,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.participant_reactions import ParticipantReactionService
from proxima_traffic.preflight import CampaignPreflightSummary, PreflightService
from proxima_traffic.profile_names import (
    NameLocale,
    ProfileName,
    available_profile_name_capacity,
    generate_profile_names,
)
from proxima_traffic.proxies import (
    ProxyFilter,
    ProxyInputError,
    ProxyNetworkRoute,
    ProxyService,
    parse_proxy,
    validate_rotation_url,
)
from proxima_traffic.scheduling import broadcast_profile, estimate_capacity
from proxima_traffic.services import ConflictError, NotFoundError, RunQueue
from proxima_traffic.spambot import SpamBotStatus
from proxima_traffic.spambot_checks import SpamBotCheckService
from proxima_traffic.subscriber_import import (
    SubscriberImportError,
    subscriber_file_supported,
    subscriber_list_name,
)
from proxima_traffic.subscriber_invites import (
    SubscriberInviteService,
    normalize_invite_destination_ref,
)
from proxima_traffic.target_import import (
    TargetReferenceError,
    TargetReferenceKind,
    parse_target_batch,
    parse_target_reference,
)
from proxima_traffic.worker_supervision import WorkerSupervisor

router = Router(name="control-bot")
logger = logging.getLogger(__name__)
PAGE_SIZE = 7
AUDIENCE_PAGE_SIZE = 10
TARGET_LIST_CHAT_PAGE_SIZE = 10
DIRECT_MESSAGE_PAGE_SIZE = 7
DIRECT_MESSAGE_PICKER_PAGE_SIZE = 7


@dataclass(frozen=True, slots=True)
class AccountArchiveFailure:
    source_name: str
    error: str


@dataclass(frozen=True, slots=True)
class AccountArchiveWarning:
    source_name: str
    warning: str


@dataclass(frozen=True, slots=True)
class AccountArchiveImportResult:
    total: int
    imported: tuple[Account, ...]
    failures: tuple[AccountArchiveFailure, ...]
    warnings: tuple[AccountArchiveWarning, ...]


def _dashboard(
    stats: DashboardStats,
    settings: Settings,
    worker_supervisor: WorkerSupervisor,
) -> str:
    return dashboard_text(
        stats,
        dry_run=settings.dry_run,
        require_proxy=settings.account_require_proxy,
        worker_health=worker_supervisor.snapshots(),
    )


def _channel_comment_delay(value: str) -> int:
    normalized = value.strip().lower().replace(" ", "")
    match = re.fullmatch(r"(\d+)([smh]?)", normalized)
    if match is None:
        raise ValueError("введите 0, 30s, 5m или 2h")
    amount = int(match.group(1))
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600}[match.group(2)]
    seconds = amount * multiplier
    if not 0 <= seconds <= 24 * 3600:
        raise ValueError("задержка должна быть от 0 до 24h")
    return seconds


def _campaign_title(mode: object) -> str:
    return "📢 Новая рассылка" if mode == "broadcast" else "💬 Новый диалог"


def _campaign_total_steps(mode: object) -> int:
    return 7


async def _dialogue_chain_builder_text(
    data: dict[str, object],
    repository: AdminRepository,
) -> str:
    steps = [dict(item) for item in data.get("dialogue_steps", []) if isinstance(item, dict)]
    accounts = await repository.active_owned_accounts()
    labels = {account.id: account.label for account in accounts}
    lines = [
        "<b>🧩 Цепочка диалога</b>",
        f"<code>СООБЩЕНИЙ · {len(steps)} / 20</code>\n",
        "Каждое следующее сообщение отвечает на предыдущее. Перед отправкой софт "
        "проверит личный таймер выбранного аккаунта в конкретном чате и, если нужно, "
        "сам дождётся разрешённого времени.\n",
    ]
    for index, step in enumerate(steps, start=1):
        account_id = int(step.get("account_id") or 0)
        label = escape(labels.get(account_id, f"аккаунт #{account_id}"))
        delay = int(step.get("delay_seconds") or 0)
        delay_note = "старт" if index == 1 else f"через {delay} сек."
        text = escape(str(step.get("text") or ""))
        preview = text if len(text) <= 100 else f"{text[:97]}…"
        media_label = {
            "photo": "фото",
            "video": "видео",
            "document": "документ",
            "voice": "голосовое",
            "video_note": "видеокружок",
        }.get(str(step.get("media_kind") or ""))
        content_label = f" · {media_label}" if media_label else ""
        lines.append(
            f"<b>{index}. {label}</b> · {delay_note}{content_label}\n"
            f"<blockquote>{preview or 'без подписи'}</blockquote>"
        )
    lines.append(
        "<i>Можно собрать до 20 сообщений. В списке чатов такая отдельная цепочка "
        "будет запущена в каждом включённом чате.</i>"
    )
    return "\n".join(lines)


def _dialogue_chain_cancel_callback(data: dict[str, object]) -> str:
    if data.get("order_draft_active"):
        return "order:back"
    campaign_id = data.get("edit_chain_campaign_id")
    if campaign_id is None:
        return "fsm:cancel"
    return (
        f"campaign:content:cancel:{int(campaign_id)}:{int(data.get('edit_chain_campaign_page', 0))}"
    )


def _campaign_delivery_mode_text(*, template_name: str | None = None) -> str:
    selected_text = (
        f"Выбран текст <b>{escape(template_name)}</b> (для рассылки берётся первое сообщение).\n\n"
        if template_name is not None
        else ""
    )
    return wizard_text(
        "📢 Новая рассылка",
        5,
        7,
        selected_text + "Как должна работать рассылка?\n\n"
        "• <b>Одноразовая</b> — один пост в каждый чат. После выбора зададите окно, "
        "за которое разложить отправки.\n"
        "• <b>Умная непрерывная</b> — повторяет посты до конца срока активности: "
        "в чатах с таймером отправляет сразу после его окончания, а в чатах без "
        "таймера — по вашему интервалу (активный режим по умолчанию — 5 минут).",
        hint=(
            "Автозамена в умном режиме использует только отмеченные вами аккаунты. "
            "Аккаунт вне выбранного пула никогда не подключится к рассылке."
        ),
    )


@router.message(Command("start", "admin"))
async def start_panel(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    worker_supervisor: WorkerSupervisor,
) -> None:
    await state.clear()
    stats = await repository.dashboard()
    await message.answer(
        _dashboard(stats, settings, worker_supervisor),
        reply_markup=main_menu(),
    )


@router.message(Command("cancel"))
async def cancel_command(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    worker_supervisor: WorkerSupervisor,
) -> None:
    await state.clear()
    stats = await repository.dashboard()
    await message.answer(
        "Ввод отменён.\n\n" + _dashboard(stats, settings, worker_supervisor),
        reply_markup=main_menu(),
    )


@router.message(Command("proxies", "proxy"))
async def open_proxies_command(
    message: Message,
    state: FSMContext,
    proxy_service: ProxyService,
) -> None:
    await state.clear()
    page = await proxy_service.list_proxies(0, PAGE_SIZE)
    await message.answer(proxies_text(page), reply_markup=proxies_menu(page))


@router.message(Command("help"))
async def help_command(message: Message) -> None:
    await message.answer(
        "<b>Proxima: быстрый старт</b>\n\n"
        "• <code>/admin</code> — пульт рассылок.\n"
        "• <code>/proxies</code> — прокси и ротация IP.\n"
        "• <code>/cancel</code> — выйти из мастера без сохранения.\n\n"
        "Порядок работы: добавить аккаунты, назначить прокси, загрузить чаты, "
        "создать рассылку и нажать «Запустить рассылку». "
        "Пароли и ссылки ротации после сохранения не показываются."
    )


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data == "fsm:cancel")
async def cancel_fsm(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    worker_supervisor: WorkerSupervisor,
) -> None:
    await callback.answer("Отменено")
    await state.clear()
    stats = await repository.dashboard()
    await _edit_callback(
        callback,
        _dashboard(stats, settings, worker_supervisor),
        main_menu(),
        answer=False,
    )


@router.callback_query(F.data == "order:new")
async def open_new_order(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _edit_callback(callback, order_type_text(), order_type_menu())


async def _comment_accounts(
    repository: AdminRepository,
    settings: Settings,
) -> tuple[list[Account], dict[int, str]]:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    return accounts, {account.id: account.label for account in accounts}


async def _show_channel_comments(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    *,
    page: int,
) -> None:
    requested_page = max(0, page)
    views, total = await channel_comments.list_rules(requested_page, PAGE_SIZE)
    last_page = max(0, (total - 1) // PAGE_SIZE)
    active_page = min(requested_page, last_page)
    if active_page != requested_page:
        views, total = await channel_comments.list_rules(active_page, PAGE_SIZE)
    await _edit_callback(
        callback,
        channel_comments_text(views, total),
        channel_comments_menu(views, page=active_page, total=total, page_size=PAGE_SIZE),
        answer=False,
    )


async def _show_channel_comment_details(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
    *,
    rule_id: int,
    page: int,
) -> None:
    view = await channel_comments.get_rule_view(rule_id)
    _, labels = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        channel_comment_details_text(view, labels),
        channel_comment_details_menu(view, page=page),
        answer=False,
    )


async def _show_channel_comment_reactions(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
    *,
    rule_id: int,
    page: int,
) -> None:
    view = await channel_comments.get_rule_view(rule_id)
    _, labels = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        channel_comment_reactions_text(view, labels),
        channel_comment_reactions_menu(view, page=page),
        answer=False,
    )


async def _show_participant_reactions(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    *,
    page: int,
) -> None:
    requested_page = max(0, page)
    views, total = await participant_reactions.list_rules(requested_page, PAGE_SIZE)
    active_page = min(requested_page, max(0, (total - 1) // PAGE_SIZE))
    if active_page != requested_page:
        views, total = await participant_reactions.list_rules(active_page, PAGE_SIZE)
    await _edit_callback(
        callback,
        participant_reactions_text(views, total),
        participant_reactions_menu(views, page=active_page, total=total, page_size=PAGE_SIZE),
        answer=False,
    )


async def _show_participant_reaction_details(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
    *,
    rule_id: int,
    page: int,
) -> None:
    view = await participant_reactions.get_rule_view(rule_id)
    accounts, labels = await _comment_accounts(repository, settings)
    del accounts
    await _edit_callback(
        callback,
        participant_reaction_details_text(view, labels),
        participant_reaction_details_menu(view, page=page),
        answer=False,
    )


async def _show_participant_account_picker(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    *,
    page: int,
) -> None:
    accounts, _ = await _comment_accounts(repository, settings)
    selected = {int(value) for value in (await state.get_data()).get("pr_account_ids", [])}
    max_page = max(0, (len(accounts) - 1) // 8)
    active_page = min(max(0, page), max_page)
    await _edit_callback(
        callback,
        "<b>👍 Аккаунты для реакций</b>\n\n"
        "Выберите один или несколько аккаунтов. На одно сообщение сервис выберет "
        "только один из них.",
        participant_reaction_account_picker(accounts, selected, page=active_page),
        answer=False,
    )


@router.callback_query(F.data.startswith("pr:home:"))
async def open_participant_reactions(
    callback: CallbackQuery,
    state: FSMContext,
    participant_reactions: ParticipantReactionService,
) -> None:
    await state.clear()
    _, _, raw_page = (callback.data or "").split(":")
    await _show_participant_reactions(callback, participant_reactions, page=int(raw_page))


@router.callback_query(F.data == "pr:add")
async def begin_participant_reaction_rule(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ParticipantReactionCreateState.name)
    await _edit_callback(
        callback,
        "<b>👍 Новое правило авто-реакций</b>\n\n"
        "Напишите короткое название, например <code>Чаты про крипту · лайки</code>.",
        cancel_menu("pr:home:0"),
    )


@router.message(ParticipantReactionCreateState.name)
async def participant_reaction_name(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    name = " ".join((message.text or "").split())
    if not 1 <= len(name) <= 150:
        await message.answer("Название — от 1 до 150 символов. Попробуйте ещё раз.")
        return
    await state.update_data(pr_name=name)
    await state.set_state(ParticipantReactionCreateState.target_list)
    page = await repository.list_available_target_lists(0, PAGE_SIZE)
    await message.answer(
        "<b>Выберите список чатов</b>\n\n"
        "В ней будут использоваться только включённые и разрешённые чаты.",
        reply_markup=participant_reaction_list_picker(page),
    )


@router.callback_query(ParticipantReactionEditState.target_list, F.data.startswith("pr:list:page:"))
@router.callback_query(
    ParticipantReactionCreateState.target_list, F.data.startswith("pr:list:page:")
)
async def participant_reaction_list_page(
    callback: CallbackQuery, repository: AdminRepository
) -> None:
    page = await repository.list_available_target_lists(
        int((callback.data or "").rsplit(":", 1)[-1]), PAGE_SIZE
    )
    await _edit_callback(
        callback,
        "<b>Выберите список чатов</b>",
        participant_reaction_list_picker(page),
        answer=False,
    )


@router.callback_query(ParticipantReactionEditState.target_list, F.data.startswith("pr:list:pick:"))
@router.callback_query(
    ParticipantReactionCreateState.target_list, F.data.startswith("pr:list:pick:")
)
async def participant_reaction_pick_list(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    target_list_id = int((callback.data or "").rsplit(":", 1)[-1])
    try:
        selected = await repository.get_target_list_view(target_list_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.update_data(
        pr_target_list_id=target_list_id, pr_target_list_name=selected.name, pr_account_ids=[]
    )
    if (await state.get_data()).get("pr_edit_rule_id") is not None:
        await state.set_state(ParticipantReactionEditState.accounts)
    else:
        await state.set_state(ParticipantReactionCreateState.accounts)
    await _show_participant_account_picker(callback, state, repository, settings, page=0)


@router.callback_query(ParticipantReactionEditState.accounts, F.data.startswith("pr:account:page:"))
@router.callback_query(
    ParticipantReactionCreateState.accounts, F.data.startswith("pr:account:page:")
)
async def participant_reaction_account_page(
    callback: CallbackQuery, state: FSMContext, repository: AdminRepository, settings: Settings
) -> None:
    await _show_participant_account_picker(
        callback, state, repository, settings, page=int((callback.data or "").rsplit(":", 1)[-1])
    )


@router.callback_query(
    ParticipantReactionEditState.accounts, F.data.regexp(r"^pr:account:\d+:\d+$")
)
@router.callback_query(
    ParticipantReactionCreateState.accounts, F.data.regexp(r"^pr:account:\d+:\d+$")
)
async def participant_reaction_toggle_account(
    callback: CallbackQuery, state: FSMContext, repository: AdminRepository, settings: Settings
) -> None:
    _, _, account_id, page = (callback.data or "").split(":")
    selected = {int(value) for value in (await state.get_data()).get("pr_account_ids", [])}
    identifier = int(account_id)
    if identifier in selected:
        selected.remove(identifier)
    else:
        selected.add(identifier)
    await state.update_data(pr_account_ids=sorted(selected))
    await _show_participant_account_picker(callback, state, repository, settings, page=int(page))


@router.callback_query(ParticipantReactionEditState.accounts, F.data == "pr:account:done")
@router.callback_query(ParticipantReactionCreateState.accounts, F.data == "pr:account:done")
async def participant_reaction_accounts_done(callback: CallbackQuery, state: FSMContext) -> None:
    if not (await state.get_data()).get("pr_account_ids"):
        await callback.answer("Выберите хотя бы один аккаунт.", show_alert=True)
        return
    if (await state.get_data()).get("pr_edit_rule_id") is not None:
        await state.set_state(ParticipantReactionEditState.sample)
    else:
        await state.set_state(ParticipantReactionCreateState.sample)
    await _edit_callback(
        callback,
        "<b>Как часто реагировать?</b>\n\nЭто доля только новых сообщений участников. "
        "По умолчанию лучше начать с 1 из 10.",
        participant_reaction_sample_menu(),
        answer=False,
    )


@router.callback_query(ParticipantReactionEditState.sample, F.data.regexp(r"^pr:sample:\d+:\d+$"))
@router.callback_query(ParticipantReactionCreateState.sample, F.data.regexp(r"^pr:sample:\d+:\d+$"))
async def participant_reaction_pick_sample(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, numerator, denominator = (callback.data or "").split(":")
    await state.update_data(pr_numerator=int(numerator), pr_denominator=int(denominator))
    if (await state.get_data()).get("pr_edit_rule_id") is not None:
        await state.set_state(ParticipantReactionEditState.emoji)
    else:
        await state.set_state(ParticipantReactionCreateState.emoji)
    await _edit_callback(
        callback, "<b>Выберите реакцию</b>", participant_reaction_emoji_menu(), answer=False
    )


@router.callback_query(ParticipantReactionEditState.emoji, F.data.startswith("pr:emoji:"))
@router.callback_query(ParticipantReactionCreateState.emoji, F.data.startswith("pr:emoji:"))
async def participant_reaction_pick_emoji(
    callback: CallbackQuery, state: FSMContext, repository: AdminRepository, settings: Settings
) -> None:
    values = {"thumb": "👍", "fire": "🔥", "heart": "❤️", "bolt": "⚡"}
    emoji = values.get((callback.data or "").rsplit(":", 1)[-1])
    if emoji is None:
        await callback.answer("Неизвестная реакция", show_alert=True)
        return
    await state.update_data(pr_emoji=emoji)
    data = await state.get_data()
    accounts, labels = await _comment_accounts(repository, settings)
    del accounts
    is_edit = data.get("pr_edit_rule_id") is not None
    await state.set_state(
        ParticipantReactionEditState.confirm if is_edit else ParticipantReactionCreateState.confirm
    )
    await _edit_callback(
        callback,
        participant_reaction_draft_text(data, str(data["pr_target_list_name"]), labels),
        participant_reaction_confirm_menu(
            "pr:save" if is_edit else "pr:create",
            label="✅ Сохранить настройки" if is_edit else "✅ Сохранить и включить",
        ),
        answer=False,
    )


@router.callback_query(ParticipantReactionCreateState.confirm, F.data == "pr:create")
async def create_participant_reaction_rule(
    callback: CallbackQuery,
    state: FSMContext,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    data = await state.get_data()
    try:
        rule = await participant_reactions.create_rule(
            name=str(data["pr_name"]),
            target_list_id=int(data["pr_target_list_id"]),
            account_ids=[int(value) for value in data["pr_account_ids"]],
            emoji=str(data["pr_emoji"]),
            sample_numerator=int(data.get("pr_numerator", 1)),
            sample_denominator=int(data.get("pr_denominator", 10)),
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Правило включено")
    await _show_participant_reaction_details(
        callback,
        participant_reactions,
        repository,
        settings,
        rule_id=rule.id,
        page=0,
    )


@router.callback_query(F.data.regexp(r"^pr:edit:\d+:\d+$"))
async def begin_edit_participant_reaction_rule(
    callback: CallbackQuery,
    state: FSMContext,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    try:
        view = await participant_reactions.get_rule_view(int(rule_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    try:
        import json

        account_ids = [int(value) for value in json.loads(view.rule.account_ids_json)]
    except (TypeError, ValueError):
        account_ids = []
    await state.clear()
    await state.update_data(
        pr_edit_rule_id=view.rule.id,
        pr_edit_page=int(page),
        pr_name=view.rule.name,
        pr_target_list_id=view.rule.target_list_id,
        pr_target_list_name=view.target_list_name,
        pr_account_ids=account_ids,
        pr_numerator=view.rule.sample_numerator,
        pr_denominator=view.rule.sample_denominator,
        pr_emoji=view.rule.emoji,
    )
    await state.set_state(ParticipantReactionEditState.target_list)
    lists = await repository.list_available_target_lists(0, PAGE_SIZE)
    await _edit_callback(
        callback,
        "<b>⚙️ Настройка правила</b>\n\n"
        "Выберите список чатов. Очередь со старыми настройками будет отменена.",
        participant_reaction_list_picker(lists),
        answer=False,
    )


@router.callback_query(ParticipantReactionEditState.confirm, F.data == "pr:save")
async def save_participant_reaction_rule(
    callback: CallbackQuery,
    state: FSMContext,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    data = await state.get_data()
    try:
        rule = await participant_reactions.update_rule(
            int(data["pr_edit_rule_id"]),
            target_list_id=int(data["pr_target_list_id"]),
            account_ids=[int(value) for value in data["pr_account_ids"]],
            emoji=str(data["pr_emoji"]),
            sample_numerator=int(data["pr_numerator"]),
            sample_denominator=int(data["pr_denominator"]),
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    page = int(data.get("pr_edit_page", 0))
    await state.clear()
    await callback.answer("Настройки сохранены")
    await _show_participant_reaction_details(
        callback,
        participant_reactions,
        repository,
        settings,
        rule_id=int(rule.id),
        page=page,
    )


@router.callback_query(F.data.regexp(r"^pr:details:\d+:\d+$"))
async def participant_reaction_details(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    try:
        await _show_participant_reaction_details(
            callback,
            participant_reactions,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.regexp(r"^pr:toggle:\d+:\d+$"))
async def participant_reaction_toggle(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    try:
        view = await participant_reactions.get_rule_view(int(rule_id))
        await participant_reactions.set_rule_enabled(int(rule_id), not view.rule.enabled)
        await _show_participant_reaction_details(
            callback,
            participant_reactions,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.regexp(r"^pr:scan:\d+:\d+$"))
async def participant_reaction_scan(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    await callback.answer("Проверяю новые сообщения…")
    try:
        await participant_reactions.scan_rule(int(rule_id))
        await _show_participant_reaction_details(
            callback,
            participant_reactions,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.regexp(r"^pr:runs:\d+:\d+$"))
async def participant_reaction_runs(
    callback: CallbackQuery,
    participant_reactions: ParticipantReactionService,
    repository: AdminRepository,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    tasks = await participant_reactions.recent_tasks(int(rule_id))
    account_labels = await repository.account_labels_by_ids(
        {task.reactor_account_id for task in tasks}
    )
    targets = await repository.targets_by_ids({task.target_id for task in tasks})
    await _edit_callback(
        callback,
        participant_reaction_runs_text(
            tasks,
            account_labels=account_labels,
            target_labels={target_id: target.chat_ref for target_id, target in targets.items()},
        ),
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ К правилу", callback_data=f"pr:details:{rule_id}:{page}"
                    )
                ]
            ]
        ),
        answer=False,
    )


@router.callback_query(F.data.regexp(r"^pr:delete:ask:\d+:\d+$"))
async def ask_delete_participant_reaction(callback: CallbackQuery) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    await _edit_callback(
        callback,
        "<b>Удалить правило?</b>\n\nЖурнал и очередь этого правила будут удалены.",
        participant_reaction_delete_menu(int(rule_id), int(page)),
        answer=False,
    )


@router.callback_query(F.data.regexp(r"^pr:delete:confirm:\d+:\d+$"))
async def delete_participant_reaction(
    callback: CallbackQuery, participant_reactions: ParticipantReactionService
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        await participant_reactions.delete_rule(int(rule_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Правило удалено")
    await _show_participant_reactions(callback, participant_reactions, page=int(page))


@router.callback_query(F.data.startswith("comments:home:"))
async def open_channel_comments(
    callback: CallbackQuery,
    state: FSMContext,
    channel_comments: ChannelCommentService,
) -> None:
    await state.clear()
    _, _, raw_page = (callback.data or "").split(":")
    await _show_channel_comments(callback, channel_comments, page=max(0, int(raw_page)))


@router.callback_query(F.data == "comments:add")
async def begin_channel_comment_rule(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _edit_callback(
        callback,
        "<b>💬 Новый канал для комментариев</b>\n\n"
        "Выберите формат. Обычный режим пишет один комментарий под каждым новым постом. "
        "Цепочка добавляет ответы выбранных аккаунтов с заданными паузами. "
        "Режим с редактированием сначала публикует исходный комментарий, а затем "
        "заменяет его текст.",
        channel_comment_mode_menu(),
    )


@router.callback_query(F.data.startswith("comments:draft:"))
async def choose_channel_comment_mode(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, raw_mode = (callback.data or "").split(":")
    mode = ChannelCommentMode(raw_mode)
    await state.clear()
    await state.update_data(comment_mode=mode.value, comment_steps=[])
    await state.set_state(ChannelCommentCreateState.name)
    await _edit_callback(
        callback,
        "<b>1 / 5 · Название</b>\n\n"
        "Напишите короткое название для себя: например <code>Вакансии · IT</code>.",
        cancel_menu(),
    )


@router.message(ChannelCommentCreateState.name)
async def channel_comment_name(message: Message, state: FSMContext) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 150:
        await message.answer("Название — от 1 до 150 символов. Попробуйте ещё раз.")
        return
    await state.update_data(comment_name=" ".join(value.split()))
    await state.set_state(ChannelCommentCreateState.channel)
    await message.answer(
        "<b>2 / 5 · Канал</b>\n\n"
        "Пришли публичную ссылку или @username канала.\n"
        "Сервис будет отслеживать только новые посты после включения.",
        reply_markup=cancel_menu(),
    )


@router.message(ChannelCommentCreateState.channel)
async def channel_comment_channel(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    value = (message.text or "").strip()
    if not value:
        await message.answer("Нужна публичная ссылка на канал или @username.")
        return
    accounts, _ = await _comment_accounts(repository, settings)
    if not accounts:
        await message.answer("Сначала добавьте и подготовьте хотя бы один активный аккаунт.")
        return
    await state.update_data(channel_ref=value)
    await state.set_state(ChannelCommentCreateState.primary_account)
    await message.answer(
        "<b>3 / 5 · Первый аккаунт</b>\n\nОн напишет первый комментарий под новым постом.",
        reply_markup=channel_comment_account_picker(
            accounts,
            callback_prefix="comments:pick:primary",
            back_callback="comments:add",
            pager_callback_prefix="comments:picker:primary",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.primary_account,
    F.data.startswith("comments:picker:primary:"),
)
async def page_channel_comment_primary_account(
    callback: CallbackQuery,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        page = max(0, int((callback.data or "").rsplit(":", 1)[1]))
    except (IndexError, ValueError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    accounts, _ = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        "<b>3 / 5 · Первый аккаунт</b>\n\nОн напишет первый комментарий под новым постом.",
        channel_comment_account_picker(
            accounts,
            callback_prefix="comments:pick:primary",
            back_callback="comments:add",
            page=page,
            pager_callback_prefix="comments:picker:primary",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.primary_account,
    F.data.startswith("comments:pick:primary:"),
)
async def channel_comment_primary_account(callback: CallbackQuery, state: FSMContext) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(primary_account_id=account_id)
    await state.set_state(ChannelCommentCreateState.primary_text)
    mode = ChannelCommentMode(str((await state.get_data()).get("comment_mode", "single")))
    if mode == ChannelCommentMode.MESSAGE_EDIT:
        await _edit_callback(
            callback,
            "<b>4 / 6 · Исходный комментарий</b>\n\n"
            "Отправьте короткий текст, который появится сразу под новым постом. "
            "Этот текст позднее будет заменён, а не дополнен новым сообщением.",
            cancel_menu(),
        )
        return
    await _edit_callback(
        callback,
        "<b>4 / 5 · Первый комментарий</b>\n\n"
        "Отправьте текст, фото, видео, документ, голосовое или видеокружок. "
        "Кружок отправляется без подписи; остальные вложения можно отправить с подписью.",
        cancel_menu(),
    )


@router.message(ChannelCommentCreateState.primary_text)
async def channel_comment_primary_text(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    bot: Bot,
) -> None:
    data = await state.get_data()
    mode = ChannelCommentMode(str(data.get("comment_mode", "single")))
    if mode != ChannelCommentMode.MESSAGE_EDIT:
        try:
            text, _html_text, media = await _extract_broadcast_content(message, bot, settings)
        except ContentError as exc:
            await message.answer(str(exc))
            return
        await state.update_data(
            primary_text=text,
            primary_media_path=media.path if media else None,
            primary_media_kind=media.kind if media else None,
            primary_media_name=media.name if media else None,
            primary_media_size=media.size if media else None,
        )
        if mode == ChannelCommentMode.SINGLE:
            await state.set_state(ChannelCommentCreateState.initial_delay)
            await message.answer(
                "<b>5 / 5 · Задержка</b>\n\n"
                "Через сколько писать после нового поста?\n"
                "Примеры: <code>0</code>, <code>30s</code>, <code>5m</code>, <code>2h</code>.",
                reply_markup=cancel_menu(),
            )
            return
        first_step = {
            "account_id": int(data["primary_account_id"]),
            "text": text,
            "delay_seconds": 0,
            "reply_to_previous": False,
            "media_path": media.path if media else None,
            "media_kind": media.kind if media else None,
            "media_name": media.name if media else None,
            "media_size": media.size if media else None,
        }
        await state.update_data(comment_steps=[first_step])
        _, labels = await _comment_accounts(repository, settings)
        await state.set_state(ChannelCommentCreateState.chain_account)
        await message.answer(
            channel_comment_draft_text(await state.get_data(), labels),
            reply_markup=channel_comment_chain_menu(1),
        )
        return

    text = (message.text or "").strip()
    if not 1 <= len(text) <= 4096:
        await message.answer("Текст должен быть от 1 до 4096 символов.")
        return
    if mode == ChannelCommentMode.MESSAGE_EDIT:
        await state.update_data(message_edit_initial_text=text)
        await state.set_state(ChannelCommentCreateState.message_edit_final_text)
        await message.answer(
            "<b>5 / 6 · Текст после редактирования</b>\n\n"
            "Отправьте финальный текст. Он заменит исходный комментарий в той же ветке.",
            reply_markup=cancel_menu(),
        )
        return
    await state.update_data(primary_text=text)


@router.message(ChannelCommentCreateState.message_edit_final_text)
async def channel_comment_message_edit_final_text(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not 1 <= len(text) <= 4096:
        await message.answer("Текст должен быть от 1 до 4096 символов.")
        return
    await state.update_data(primary_text=text)
    await state.set_state(ChannelCommentCreateState.message_edit_delay)
    await message.answer(
        "<b>6 / 6 · Задержка редактирования</b>\n\n"
        "Через сколько заменить исходный комментарий? Допустимо от <b>5m</b> до <b>30m</b>.\n"
        "Примеры: <code>5m</code>, <code>15m</code>, <code>30m</code>.",
        reply_markup=cancel_menu(),
    )


@router.message(ChannelCommentCreateState.message_edit_delay)
async def channel_comment_message_edit_delay(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        delay = _channel_comment_delay(message.text or "")
    except ValueError as exc:
        await message.answer(str(exc))
        return
    if not 5 * 60 <= delay <= 30 * 60:
        await message.answer("Для режима с редактированием укажите задержку от 5m до 30m.")
        return
    await state.update_data(
        initial_delay_seconds=0,
        message_edit_delay_seconds=delay,
    )
    _, labels = await _comment_accounts(repository, settings)
    await state.set_state(ChannelCommentCreateState.confirm)
    await message.answer(
        channel_comment_draft_text(await state.get_data(), labels)
        + "\n\n<i>Исходный комментарий будет опубликован сразу после нового поста, "
        "а финальный текст заменит его через заданный интервал.</i>",
        reply_markup=channel_comment_confirmation_menu(),
    )


@router.callback_query(F.data.startswith("order:draft:"))
async def begin_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    mode = (callback.data or "").rsplit(":", 1)[1]
    if mode not in {"broadcast", "dialogue"}:
        await callback.answer("Неизвестный формат рассылки", show_alert=True)
        return
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    minimum = 1 if mode == "broadcast" else 2
    if len(accounts) < minimum:
        await callback.answer(
            f"Для этой рассылки нужно готовых аккаунтов: {minimum}",
            show_alert=True,
        )
        return
    default_profile = broadcast_profile("active")
    await state.clear()
    await _remember_origin(callback, state)
    short_id = uuid4().hex[:5].upper()
    await state.update_data(
        order_draft_active=True,
        order_require_authorized_session=not settings.dry_run,
        order_mode=mode,
        order_name=(f"Рассылка {short_id}" if mode == "broadcast" else f"Диалог {short_id}"),
        sender_account_ids=([account.id for account in accounts] if mode == "broadcast" else []),
        dialogue_steps=[],
        target_id=None,
        target_list_id=None,
        primary_text="",
        primary_html=None,
        primary_media_path=None,
        primary_media_kind=None,
        primary_media_name=None,
        primary_media_size=None,
        broadcast_profile="active",
        smart_timer_mode=False,
        smart_duration_hours=default_profile.duration_hours,
        smart_no_timer_interval_minutes=default_profile.interval_minutes,
        activity_aware=False,
        min_messages_between_posts=default_profile.min_messages_between_posts,
        activity_check_interval_seconds=default_profile.activity_check_interval_seconds,
        visibility_max_wait_seconds=default_profile.visibility_max_wait_seconds,
        auto_sender_failover=mode == "broadcast",
        sender_mode=(BroadcastSenderMode.DISTRIBUTED.value if mode == "broadcast" else None),
        spread_hours=0,
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


async def _show_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    *,
    answer: bool = True,
) -> None:
    view = await _build_order_draft_view(state, repository)
    await _edit_callback(
        callback,
        order_draft_text(view),
        order_draft_menu(view),
        answer=answer,
    )


async def _build_order_draft_view(
    state: FSMContext,
    repository: AdminRepository,
) -> dict[str, object]:
    data = await state.get_data()
    view = dict(data)
    require_live = bool(data.get("order_require_authorized_session", False))
    accounts = await repository.active_owned_accounts(require_authorized_session=require_live)
    valid_account_ids = {account.id for account in accounts}
    sender_ids = {int(item) for item in data.get("sender_account_ids", [])}
    if data.get("order_mode") == "broadcast":
        valid_sender_ids = sender_ids & valid_account_ids
        if valid_sender_ids != sender_ids:
            await state.update_data(sender_account_ids=sorted(valid_sender_ids))
            view["sender_account_ids"] = sorted(valid_sender_ids)
            sender_ids = valid_sender_ids
    view["sender_labels"] = [account.label for account in accounts if account.id in sender_ids]
    view["dialogue_account_labels"] = {account.id: account.label for account in accounts}

    view["audience_label"] = None
    view["audience_target_count"] = 0
    if data.get("target_id") is not None:
        try:
            target = await repository.get_target(int(data["target_id"]))
            if not target.approved_by_operator or not target.is_standalone:
                raise NotFoundError("чат больше не доступен для отдельной рассылки")
            view["audience_label"] = f"Чат · {target.title}"
            view["audience_target_count"] = 1
        except NotFoundError:
            await state.update_data(target_id=None)
            view["target_id"] = None
    elif data.get("target_list_id") is not None:
        try:
            target_list = await repository.get_target_list_view(int(data["target_list_id"]))
            if not target_list.approved_count:
                raise NotFoundError("в базе больше нет включённых чатов")
            view["audience_label"] = (
                f"База · {target_list.name} · {target_list.approved_count} чатов"
            )
            view["audience_target_count"] = target_list.approved_count
        except NotFoundError:
            await state.update_data(target_list_id=None)
            view["target_list_id"] = None
    return view


@router.callback_query(F.data == "order:back")
async def back_to_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    if not data.get("order_draft_active"):
        await callback.answer("Черновик рассылки уже закрыт", show_alert=True)
        return
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.callback_query(F.data == "order:cancel")
async def cancel_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    worker_supervisor: WorkerSupervisor,
) -> None:
    await state.clear()
    stats = await repository.dashboard()
    await callback.answer("Черновик закрыт")
    await _edit_callback(
        callback,
        _dashboard(stats, settings, worker_supervisor),
        main_menu(),
        answer=False,
    )


@router.callback_query(OrderDraftState.action, F.data == "order:check")
async def explain_incomplete_order(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    missing: list[str] = []
    if data.get("target_id") is None and data.get("target_list_id") is None:
        missing.append("аудитория")
    if data.get("order_mode") == "broadcast":
        if not data.get("sender_account_ids"):
            missing.append("аккаунты")
        if not (
            str(data.get("primary_text") or "").strip()
            or str(data.get("primary_media_path") or "").strip()
        ):
            missing.append("пост: текст или вложение")
    else:
        steps = [item for item in data.get("dialogue_steps", []) if isinstance(item, dict)]
        dialogue_accounts = {int(item.get("account_id") or 0) for item in steps}
        if len(steps) < 2 or len(dialogue_accounts) < 2:
            missing.append("цепочка минимум из 2 сообщений от разных аккаунтов")
    await callback.answer("Нужно заполнить: " + ", ".join(missing), show_alert=True)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:name")
async def begin_order_name_edit(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.set_state(OrderDraftState.name)
    await _edit_callback(
        callback,
        wizard_text(
            "✏️ Название рассылки",
            1,
            1,
            f"Текущее: <b>{escape(str(data.get('order_name') or ''))}</b>\n\n"
            "Отправьте короткое понятное название. Оно будет видно в списке и отчётах.",
        ),
        order_back_menu(),
    )


@router.message(OrderDraftState.name)
async def save_order_name(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 150:
        await message.answer("Название должно содержать от 1 до 150 символов.")
        return
    await state.update_data(order_name=value)
    await state.set_state(OrderDraftState.action)
    view = await _build_order_draft_view(state, repository)
    await _edit_origin(
        bot,
        state,
        order_draft_text(view),
        order_draft_menu(view),
    )


@router.callback_query(OrderDraftState.action, F.data == "order:edit:text")
async def begin_order_text_edit(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.text)
    await _edit_callback(
        callback,
        wizard_text(
            "✏️ Рекламный текст",
            1,
            1,
            "Отправьте готовый рекламный пост: обычный или форматированный текст, "
            "фото, видео, документ, голосовое или видеокружок. Фото, видео, документ и "
            "голосовое можно отправить с подписью; видеокружок отправляется без неё.",
        ),
        order_back_menu(),
    )


@router.message(OrderDraftState.text)
async def save_order_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        value, html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc))
        return
    await state.update_data(
        primary_text=value,
        primary_html=html_text,
        primary_media_path=media.path if media else None,
        primary_media_kind=media.kind if media else None,
        primary_media_name=media.name if media else None,
        primary_media_size=media.size if media else None,
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


async def _show_order_draft_from_message(
    bot: Bot,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    view = await _build_order_draft_view(state, repository)
    await _edit_origin(
        bot,
        state,
        order_draft_text(view),
        order_draft_menu(view),
    )


@router.callback_query(OrderDraftState.action, F.data == "order:schedule:toggle")
async def toggle_order_schedule(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    smart = not bool(data.get("smart_timer_mode", False))
    is_broadcast = data.get("order_mode") == "broadcast"
    distributed = (
        str(data.get("sender_mode") or BroadcastSenderMode.DISTRIBUTED.value)
        == BroadcastSenderMode.DISTRIBUTED.value
    )
    await state.update_data(
        smart_timer_mode=smart,
        activity_aware=smart,
        auto_sender_failover=(
            bool(data.get("auto_sender_failover", True)) if is_broadcast and distributed else False
        ),
    )
    await _show_order_draft(callback, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:sendermode:toggle")
async def toggle_order_sender_mode(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    current = str(data.get("sender_mode") or BroadcastSenderMode.DISTRIBUTED.value)
    selected = (
        BroadcastSenderMode.DISTRIBUTED
        if current == BroadcastSenderMode.INDEPENDENT.value
        else BroadcastSenderMode.INDEPENDENT
    )
    await state.update_data(
        sender_mode=selected.value,
        auto_sender_failover=(selected == BroadcastSenderMode.DISTRIBUTED),
    )
    await _show_order_draft(callback, state, repository)


@router.callback_query(OrderDraftState.action, F.data.startswith("order:profile:"))
async def choose_order_profile(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    key = (callback.data or "").rsplit(":", 1)[-1]
    try:
        profile = broadcast_profile(key)
    except ValueError:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    data = await state.get_data()
    distributed = (
        str(data.get("sender_mode") or BroadcastSenderMode.DISTRIBUTED.value)
        == BroadcastSenderMode.DISTRIBUTED.value
    )
    await state.update_data(
        broadcast_profile=profile.key,
        smart_timer_mode=True,
        smart_duration_hours=profile.duration_hours,
        smart_no_timer_interval_minutes=profile.interval_minutes,
        activity_aware=True,
        min_messages_between_posts=profile.min_messages_between_posts,
        activity_check_interval_seconds=profile.activity_check_interval_seconds,
        visibility_max_wait_seconds=profile.visibility_max_wait_seconds,
        auto_sender_failover=(data.get("order_mode") == "broadcast" and distributed),
    )
    await _show_order_draft(callback, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:failover:toggle")
async def toggle_order_failover(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    await state.update_data(auto_sender_failover=not bool(data.get("auto_sender_failover", True)))
    await _show_order_draft(callback, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:advanced")
async def open_order_advanced_settings(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    data = await state.get_data()
    await callback.answer()
    await _edit_callback(
        callback,
        order_advanced_text(data),
        order_advanced_menu(data),
        answer=False,
    )


@router.callback_query(OrderDraftState.action, F.data == "order:edit:interval")
async def begin_order_interval(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.interval_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "⏱ Частота без таймера",
            1,
            1,
            "Выберите готовый вариант ниже или отправьте целое число минут от "
            "<b>1</b> до <b>10080</b>. "
            "В чатах с Telegram-таймером софт отправит сразу после его окончания; "
            "это значение используется только когда таймера нет.",
            hint="Активное значение по умолчанию — 5 минут.",
        ),
        order_interval_presets(),
    )


@router.callback_query(
    OrderDraftState.interval_minutes,
    F.data.startswith("order:preset:interval:"),
)
async def save_order_interval_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    value = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(smart_no_timer_interval_minutes=value, broadcast_profile="manual")
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.interval_minutes)
async def save_order_interval(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        value = 0
    if not 1 <= value <= 10080:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    await state.update_data(smart_no_timer_interval_minutes=value, broadcast_profile="manual")
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:visibility")
async def begin_order_visibility(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.visibility_messages)
    await _edit_callback(
        callback,
        wizard_text(
            "👁 Глубина адаптивной видимости",
            1,
            1,
            "Повтор появится после указанного количества новых сообщений в чате. "
            "Выберите вариант ниже или отправьте целое число от <b>1</b> до <b>100000</b>. "
            "Если пост быстро уйдёт вверх, следующий цикл сможет начаться раньше. Отдельный "
            "лимит ожидания не даст тихому чату зависнуть. Пока ваш пост последний, "
            "повтор не отправится при любых настройках.",
            hint="10 сообщений — быстрый активный режим по умолчанию.",
        ),
        order_visibility_presets(),
    )


@router.callback_query(
    OrderDraftState.visibility_messages,
    F.data.startswith("order:preset:visibility:"),
)
async def save_order_visibility_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    value = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(
        activity_aware=True,
        min_messages_between_posts=value,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.visibility_messages)
async def save_order_visibility(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        value = 0
    if not 1 <= value <= 100000:
        await message.answer("Введите целое число новых сообщений от 1 до 100000.")
        return
    await state.update_data(
        activity_aware=True,
        min_messages_between_posts=value,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:activity:toggle")
async def toggle_order_activity(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    await state.update_data(
        activity_aware=not bool(data.get("activity_aware", True)),
        broadcast_profile="manual",
    )
    await _show_order_draft(callback, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:check")
async def begin_order_activity_check(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.activity_check_seconds)
    await _edit_callback(
        callback,
        wizard_text(
            "🔎 Частота проверки активности",
            1,
            1,
            "Как часто проверять, ушёл ли рекламный пост вверх? Выберите вариант или "
            "отправьте число секунд от <b>30</b> до <b>3600</b>.",
            hint="60 секунд — быстрый и разумный вариант по умолчанию.",
        ),
        order_activity_check_presets(),
    )


@router.callback_query(
    OrderDraftState.activity_check_seconds,
    F.data.startswith("order:preset:check:"),
)
async def save_order_activity_check_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    seconds = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(
        activity_check_interval_seconds=seconds,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.activity_check_seconds)
async def save_order_activity_check(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        seconds = int((message.text or "").strip())
    except ValueError:
        seconds = 0
    if not 30 <= seconds <= 3600:
        await message.answer("Введите целое число секунд от 30 до 3600.")
        return
    await state.update_data(
        activity_check_interval_seconds=seconds,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:maxwait")
async def begin_order_visibility_max_wait(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await state.set_state(OrderDraftState.visibility_max_wait_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "⌛ Максимальное ожидание видимости",
            1,
            1,
            "Через сколько минут разрешить повтор, даже если нужное число новых сообщений "
            "ещё не набралось? Выберите вариант или отправьте число от <b>1</b> до "
            "<b>10080</b> минут. Это работает только если после публикации уже появились "
            "сообщения: пока ваш пост последний, повтор всегда ждёт.",
            hint="15 минут — активный режим по умолчанию.",
        ),
        order_visibility_wait_presets(),
    )


@router.callback_query(
    OrderDraftState.visibility_max_wait_minutes,
    F.data.startswith("order:preset:maxwait:"),
)
async def save_order_visibility_max_wait_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    minutes = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(
        visibility_max_wait_seconds=minutes * 60,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.visibility_max_wait_minutes)
async def save_order_visibility_max_wait(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        minutes = int((message.text or "").strip())
    except ValueError:
        minutes = 0
    if not 1 <= minutes <= 10080:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    await state.update_data(
        visibility_max_wait_seconds=minutes * 60,
        broadcast_profile="manual",
    )
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:duration")
async def begin_order_duration(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.duration_hours)
    await _edit_callback(
        callback,
        wizard_text(
            "🗓 Срок работы",
            1,
            1,
            "Выберите срок ниже или отправьте целое число часов от <b>1</b> до "
            "<b>168</b>. По окончании "
            "срока новые публикации создаваться не будут.",
        ),
        order_duration_presets(),
    )


@router.callback_query(
    OrderDraftState.duration_hours,
    F.data.startswith("order:preset:duration:"),
)
async def save_order_duration_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    value = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(smart_duration_hours=value, broadcast_profile="manual")
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.duration_hours)
async def save_order_duration(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        value = 0
    if not 1 <= value <= 168:
        await message.answer("Введите целое число часов от 1 до 168.")
        return
    await state.update_data(smart_duration_hours=value, broadcast_profile="manual")
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:spread")
async def begin_order_spread(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(OrderDraftState.spread_hours)
    await _edit_callback(
        callback,
        wizard_text(
            "🕒 Окно одного прохода",
            1,
            1,
            "За сколько часов разложить первую отправку по всем чатам? Выберите "
            "вариант ниже или отправьте число от <b>0</b> до <b>168</b>; 0 означает "
            "поставить все отправки сразу.",
        ),
        order_spread_presets(),
    )


@router.callback_query(
    OrderDraftState.spread_hours,
    F.data.startswith("order:preset:spread:"),
)
async def save_order_spread_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    value = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(spread_hours=value)
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.message(OrderDraftState.spread_hours)
async def save_order_spread(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        value = -1
    if not 0 <= value <= 168:
        await message.answer("Введите целое число часов от 0 до 168.")
        return
    await state.update_data(spread_hours=value)
    await state.set_state(OrderDraftState.action)
    await _show_order_draft_from_message(bot, state, repository)


@router.callback_query(OrderDraftState.action, F.data == "order:edit:senders")
async def begin_order_senders(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    if not data.get("order_draft_active"):
        await callback.answer("Черновик рассылки уже закрыт", show_alert=True)
        return
    accounts = await repository.active_owned_accounts(
        require_authorized_session=bool(data.get("order_require_authorized_session", False))
    )
    selected = {int(item) for item in data.get("sender_account_ids", [])}
    await state.update_data(sender_page=0)
    await state.set_state(OrderDraftState.sender_accounts)
    await _edit_callback(
        callback,
        "<b>👥 Кто будет отправлять</b>\n"
        "<i>Выберите аккаунты для этой рассылки.</i>\n\n"
        "Отмеченные аккаунты делят чаты между собой. Если один аккаунт недоступен "
        "или забанен в конкретном чате, автозамена использует другой отмеченный "
        "аккаунт. По умолчанию выбраны все готовые аккаунты.",
        order_sender_picker(accounts, selected, page=0),
    )


@router.callback_query(
    OrderDraftState.sender_accounts,
    F.data.startswith("order:sender:toggle:"),
)
async def toggle_order_sender(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    selected = {int(item) for item in data.get("sender_account_ids", [])}
    if account_id in selected:
        selected.remove(account_id)
    else:
        selected.add(account_id)
    await state.update_data(sender_account_ids=sorted(selected))
    page = int(data.get("sender_page", 0))
    accounts = await repository.active_owned_accounts(
        require_authorized_session=bool(data.get("order_require_authorized_session", False))
    )
    await _edit_callback(
        callback,
        "<b>👥 Кто будет отправлять</b>\n\nОтметьте отправителей и нажмите «Готово».",
        order_sender_picker(accounts, selected, page=page),
    )


@router.callback_query(
    OrderDraftState.sender_accounts,
    F.data.startswith("order:sender:page:"),
)
async def paginate_order_senders(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[1]))
    data = await state.get_data()
    accounts = await repository.active_owned_accounts(
        require_authorized_session=bool(data.get("order_require_authorized_session", False))
    )
    selected = {int(item) for item in data.get("sender_account_ids", [])}
    pages = max(1, (len(accounts) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages - 1)
    await state.update_data(sender_page=page)
    await _edit_callback(
        callback,
        "<b>👥 Кто будет отправлять</b>\n\n"
        f"Выбрано: <b>{len(selected)}</b>. Отметьте отправителей и нажмите «Готово».",
        order_sender_picker(accounts, selected, page=page, page_size=PAGE_SIZE),
    )


@router.callback_query(
    OrderDraftState.sender_accounts,
    F.data.in_({"order:sender:all", "order:sender:clear"}),
)
async def bulk_order_senders(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    accounts = await repository.active_owned_accounts(
        require_authorized_session=bool(data.get("order_require_authorized_session", False))
    )
    selected = (
        {account.id for account in accounts} if (callback.data or "").endswith(":all") else set()
    )
    await state.update_data(sender_account_ids=sorted(selected))
    await _edit_callback(
        callback,
        "<b>👥 Кто будет отправлять</b>\n\nОтметьте отправителей и нажмите «Готово».",
        order_sender_picker(
            accounts,
            selected,
            page=int(data.get("sender_page", 0)),
            page_size=PAGE_SIZE,
        ),
    )


@router.callback_query(OrderDraftState.sender_accounts, F.data == "order:sender:done")
async def finish_order_senders(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    if not data.get("sender_account_ids"):
        await callback.answer("Выберите хотя бы один аккаунт", show_alert=True)
        return
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.callback_query(F.data == "order:edit:audience")
async def begin_order_audience(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    if not data.get("order_draft_active"):
        await callback.answer("Черновик рассылки уже закрыт", show_alert=True)
        return
    chats = await repository.list_approved_targets(0, 1)
    lists = await repository.list_available_target_lists(0, 1)
    await state.set_state(OrderDraftState.audience)
    await _edit_callback(
        callback,
        "<b>🎯 Куда отправлять</b>\n"
        "<i>Выберите один чат или готовый список чатов.</i>\n\n"
        "Выберите список целиком или один вручную добавленный чат. Чаты из Telegram-"
        "папок не показываются россыпью — они доступны только внутри своего списка.",
        order_audience_source_menu(
            standalone_count=chats.total,
            list_count=lists.total,
        ),
    )


@router.callback_query(
    OrderDraftState.audience,
    F.data.startswith("order:audience:chats:"),
)
async def page_order_chats(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    page = int((callback.data or "").rsplit(":", 1)[1])
    targets = await repository.list_approved_targets(page, AUDIENCE_PAGE_SIZE)
    if targets.page >= targets.pages:
        targets = await repository.list_approved_targets(
            targets.pages - 1,
            AUDIENCE_PAGE_SIZE,
        )
    await _edit_callback(
        callback,
        "<b>📌 Отдельные чаты</b>\n\nВыберите один вручную добавленный чат.",
        order_target_picker(targets),
    )


@router.callback_query(
    OrderDraftState.audience,
    F.data.startswith("order:audience:search:"),
)
async def begin_order_audience_search(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    kind = (callback.data or "").rsplit(":", 1)[-1]
    if kind not in {"chats", "lists"}:
        await callback.answer("Неизвестный тип аудитории", show_alert=True)
        return
    await state.update_data(order_audience_search_kind=kind)
    await state.set_state(OrderDraftState.audience_search)
    await _edit_callback(
        callback,
        f"<b>🔎 Найти {'чат' if kind == 'chats' else 'список чатов'}</b>\n\n"
        "Отправьте часть названия, username/ссылку или точный числовой ID. "
        "Покажу до 20 включённых совпадений.",
        order_audience_search_menu(kind),
    )


@router.message(OrderDraftState.audience_search)
async def search_order_audience(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    query = (message.text or "").strip()
    if not 1 <= len(query) <= 120:
        await message.answer("Отправьте запрос длиной от 1 до 120 символов.")
        return
    data = await state.get_data()
    kind = str(data.get("order_audience_search_kind") or "")
    if kind == "chats":
        items = await repository.search_targets(query, approved_only=True)
        results = [(f"📌 {item.title}", f"order:audience:target:{item.id}") for item in items]
    elif kind == "lists":
        items = await repository.search_target_lists(query, approved_only=True)
        results = [
            (
                f"📚 {item.name} · {item.approved_count} чатов",
                f"order:audience:list:{item.id}",
            )
            for item in items
        ]
    else:
        await state.set_state(OrderDraftState.action)
        await message.answer("Поиск устарел. Вернитесь в карточку рассылки.")
        return
    await state.set_state(OrderDraftState.audience)
    await _edit_origin(
        bot,
        state,
        "<b>🔎 Результаты поиска</b>\n"
        f"<code>{escape(query)}</code> · найдено <b>{len(results)}</b>\n\n"
        + ("Выберите аудиторию." if results else "Совпадений нет — попробуйте короче."),
        order_audience_search_menu(kind, results),
    )


@router.callback_query(
    OrderDraftState.audience,
    F.data.startswith("order:audience:lists:"),
)
async def page_order_lists(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    page = int((callback.data or "").rsplit(":", 1)[1])
    target_lists = await repository.list_available_target_lists(page, AUDIENCE_PAGE_SIZE)
    if target_lists.page >= target_lists.pages:
        target_lists = await repository.list_available_target_lists(
            target_lists.pages - 1,
            AUDIENCE_PAGE_SIZE,
        )
    await _edit_callback(
        callback,
        "<b>📚 Списки чатов</b>\n\nВыберите список; справа указано число включённых чатов.",
        order_target_list_picker(target_lists),
    )


@router.callback_query(
    OrderDraftState.audience,
    F.data.startswith("order:audience:target:"),
)
async def choose_order_target(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    target_id = int((callback.data or "").rsplit(":", 1)[1])
    target = await repository.get_target(target_id)
    if not target.is_standalone or not target.approved_by_operator:
        await callback.answer("Этот отдельный чат сейчас недоступен", show_alert=True)
        return
    await state.update_data(target_id=target.id, target_list_id=None)
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


@router.callback_query(
    OrderDraftState.audience,
    F.data.startswith("order:audience:list:"),
)
async def choose_order_target_list(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    target_list_id = int((callback.data or "").rsplit(":", 1)[1])
    target_list = await repository.get_target_list_view(target_list_id)
    if not target_list.approved_count:
        await callback.answer("В этой базе нет включённых чатов", show_alert=True)
        return
    await state.update_data(target_id=None, target_list_id=target_list.id)
    await state.set_state(OrderDraftState.action)
    await _show_order_draft(callback, state, repository)


async def _create_campaign_from_order_draft(
    data: dict[str, object],
    repository: AdminRepository,
) -> Campaign:
    mode = (
        CampaignMode.BROADCAST if data.get("order_mode") == "broadcast" else CampaignMode.DIALOGUE
    )
    target_id = int(data["target_id"]) if data.get("target_id") is not None else None
    target_list_id = int(data["target_list_id"]) if data.get("target_list_id") is not None else None
    if (target_id is None) == (target_list_id is None):
        raise ConflictError("выберите аудиторию")
    default_profile = broadcast_profile("active")
    smart = bool(data.get("smart_timer_mode", False))
    schedule_values = {
        "smart_timer_mode": smart,
        "smart_duration_seconds": (
            int(data.get("smart_duration_hours", default_profile.duration_hours)) * 3600
            if smart
            else 0
        ),
        "smart_no_timer_interval_seconds": (
            int(
                data.get(
                    "smart_no_timer_interval_minutes",
                    default_profile.interval_minutes,
                )
            )
            * 60
        ),
        "activity_aware": smart and bool(data.get("activity_aware", True)),
        "min_messages_between_posts": (
            int(
                data.get(
                    "min_messages_between_posts",
                    default_profile.min_messages_between_posts,
                )
            )
            if smart
            else 0
        ),
        "activity_check_interval_seconds": int(
            data.get(
                "activity_check_interval_seconds",
                default_profile.activity_check_interval_seconds,
            )
        ),
        "visibility_max_wait_seconds": int(
            data.get(
                "visibility_max_wait_seconds",
                default_profile.visibility_max_wait_seconds,
            )
        ),
    }
    if mode == CampaignMode.BROADCAST:
        sender_ids = [int(item) for item in data.get("sender_account_ids", [])]
        if not sender_ids:
            raise ConflictError("выберите хотя бы один аккаунт")
        primary_text = str(data.get("primary_text") or "").strip()
        primary_media_path = (
            str(data["primary_media_path"]) if data.get("primary_media_path") else None
        )
        if not primary_text and not primary_media_path:
            raise ConflictError("добавьте текст или вложение к посту")
        sender_mode = BroadcastSenderMode(
            str(data.get("sender_mode") or BroadcastSenderMode.DISTRIBUTED.value)
        )
        return await repository.create_campaign(
            name=str(data["order_name"]),
            mode=mode,
            primary_account_id=sender_ids[0],
            reply_account_id=None,
            target_id=target_id,
            target_list_id=target_list_id,
            primary_text=primary_text,
            primary_html=(str(data["primary_html"]) if data.get("primary_html") else None),
            primary_media_path=primary_media_path,
            primary_media_kind=(
                str(data["primary_media_kind"]) if data.get("primary_media_kind") else None
            ),
            primary_media_name=(
                str(data["primary_media_name"]) if data.get("primary_media_name") else None
            ),
            primary_media_size=(
                int(data["primary_media_size"])
                if data.get("primary_media_size") is not None
                else None
            ),
            spread_seconds=(0 if smart else int(data.get("spread_hours", 0)) * 3600),
            repeat_count=1,
            repeat_interval_seconds=0,
            **schedule_values,
            auto_sender_failover=(
                bool(data.get("auto_sender_failover", True))
                and sender_mode == BroadcastSenderMode.DISTRIBUTED
            ),
            sender_mode=sender_mode,
            sender_account_ids=sender_ids,
        )
    steps = [dict(item) for item in data.get("dialogue_steps", []) if isinstance(item, dict)]
    if len(steps) < 2:
        raise ConflictError("соберите цепочку минимум из двух сообщений")
    return await repository.create_campaign(
        name=str(data["order_name"]),
        mode=mode,
        primary_account_id=int(steps[0]["account_id"]),
        reply_account_id=int(steps[1]["account_id"]),
        target_id=target_id,
        target_list_id=target_list_id,
        primary_text=str(steps[0]["text"]),
        reply_text=str(steps[1]["text"]),
        reply_delay_seconds=int(steps[1].get("delay_seconds") or 0),
        repeat_count=1,
        repeat_interval_seconds=0,
        auto_sender_failover=False,
        **schedule_values,
        dialogue_steps=steps,
    )


@router.callback_query(OrderDraftState.action, F.data == "order:save")
async def save_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    try:
        campaign = await _create_campaign_from_order_draft(data, repository)
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Рассылка сохранена")
    await _edit_callback(
        callback,
        "✅ <b>Рассылка сохранена. Отправка ещё не началась.</b>\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, 0),
        answer=False,
    )


@router.callback_query(OrderDraftState.action, F.data == "order:launch")
async def ask_launch_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    view = await _build_order_draft_view(state, repository)
    await _edit_callback(
        callback,
        order_draft_text(
            view,
            launch_confirmation=True,
            dry_run=settings.dry_run,
        ),
        order_launch_confirmation(),
    )


@router.callback_query(OrderDraftState.action, F.data == "order:launch:confirm")
async def launch_order_draft(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    preflight_service: PreflightService,
    run_queue: RunQueue,
) -> None:
    data = await state.get_data()
    try:
        campaign = await _create_campaign_from_order_draft(data, repository)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Проверяю аккаунты и чаты…")
    preflight_summary = None
    try:
        preflight_summary, runs = await _preflight_and_enqueue_order(
            campaign.id,
            preflight_service,
            run_queue,
            request_key=uuid4().hex,
        )
    except (ConflictError, NotFoundError) as exc:
        sender_ids = await repository.campaign_sender_ids(campaign.id)
        await state.clear()
        readiness = (
            "\n\n" + campaign_preflight_text(preflight_summary)
            if preflight_summary is not None
            else ""
        )
        await _edit_callback(
            callback,
            "⚠️ <b>Проверка завершена, отправка не началась.</b>\n\n"
            f"Причина: <code>{escape(str(exc))}</code>\n\n"
            "Рассылка сохранена. Исправьте причину и повторите запуск из этой карточки."
            + readiness
            + "\n\n"
            + campaign_details_text(campaign, sender_ids),
            campaign_details_menu(campaign, 0),
            answer=False,
        )
        return
    try:
        launch, launch_runs = await repository.get_launch(max(run.id for run in runs))
        targets = await repository.targets_by_ids({run.target_id for run in launch_runs})
    except (ConflictError, NotFoundError) as exc:
        sender_ids = await repository.campaign_sender_ids(campaign.id)
        await state.clear()
        await _edit_callback(
            callback,
            "🚀 <b>Рассылка запущена, но подробный отчёт пока не открылся.</b>\n\n"
            f"В очередь поставлено направлений: <b>{len(runs)}</b>. "
            "Повторно запускать рассылку не нужно.\n"
            f"Причина отображения: <code>{escape(str(exc))}</code>\n\n"
            + campaign_details_text(campaign, sender_ids),
            campaign_details_menu(campaign, 0),
            answer=False,
        )
        return
    await state.clear()
    await _edit_callback(
        callback,
        "🚀 <b>Рассылка началась.</b> В этой карточке видны публикации, проблемы и настройки.\n\n"
        + launch_details_text(launch, launch_runs, targets),
        launch_details_menu(launch, 0),
        answer=False,
    )


async def _preflight_and_enqueue_order(
    campaign_id: int,
    preflight_service: PreflightService,
    run_queue: RunQueue,
    *,
    request_key: str,
) -> tuple[CampaignPreflightSummary, list[CampaignRun]]:
    """Never expose a newly created order to the queue before a fresh readiness check."""

    summary = await preflight_service.inspect_campaign(campaign_id)
    runs = await run_queue.enqueue_campaign(campaign_id, request_key)
    return summary, runs


@router.callback_query(F.data == "audiences:home")
async def open_audiences(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await state.clear()
    stats = await repository.dashboard()
    await _edit_callback(callback, audiences_text(stats), audiences_menu())


@router.callback_query(F.data.startswith("search:start:"))
async def begin_list_search(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    section = (callback.data or "").rsplit(":", 1)[-1]
    labels = {
        "targets": "отдельным чатам",
        "targetlists": "спискам чатов",
        "campaigns": "сценариям",
        "runs": "работе и отчётам",
        "archive": "архиву результатов",
    }
    if section not in labels:
        await callback.answer("Этот раздел не поддерживает поиск", show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(search_section=section)
    await state.set_state(ListSearchState.query)
    await _edit_callback(
        callback,
        "<b>🔎 Быстрый поиск</b>\n\n"
        f"Ищем по {labels[section]}. Отправьте часть названия, ссылку/username "
        "чата или точный числовой ID.\n\n"
        "<i>Покажу до 20 самых новых совпадений — листать весь список не нужно.</i>",
        search_prompt_menu(section),
    )


@router.callback_query(F.data.startswith("tls:"))
async def begin_target_list_chat_search(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, target_list_id, list_page = (callback.data or "").split(":")
    try:
        target_list = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(
        search_section="targetlist_targets",
        search_target_list_id=target_list.id,
        search_target_list_page=int(list_page),
    )
    await state.set_state(ListSearchState.query)
    back_callback = f"tlf:{target_list.id}:{list_page}:all:0"
    await _edit_callback(
        callback,
        f"<b>🔎 Найти чат в базе «{escape(target_list.name)}»</b>\n\n"
        "Отправьте часть названия, username, ссылку или точный ID чата. "
        "Поиск выполняется только внутри этого списка и покажет до 20 совпадений.",
        search_prompt_menu("targetlist_targets", back_callback=back_callback),
    )


@router.message(ListSearchState.query)
async def perform_list_search(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    query = (message.text or "").strip()
    if not 1 <= len(query) <= 120:
        await message.answer("Отправьте запрос длиной от 1 до 120 символов.")
        return
    data = await state.get_data()
    section = str(data.get("search_section") or "")
    results: list[tuple[str, str]] = []
    new_search_callback: str | None = None
    back_callback: str | None = None
    if section == "targets":
        items = await repository.search_targets(query)
        results = [
            (
                f"{'🟢' if item.approved_by_operator else '⚪️'} {item.title}",
                f"target:details:{item.id}:0",
            )
            for item in items
        ]
    elif section == "targetlists":
        items = await repository.search_target_lists(query)
        results = [
            (
                f"📚 {item.name} ({item.approved_count}/{item.item_count})",
                f"targetlist:details:{item.id}:0",
            )
            for item in items
        ]
    elif section == "targetlist_targets":
        target_list_id = int(data["search_target_list_id"])
        list_page = int(data.get("search_target_list_page", 0))
        try:
            items = await repository.search_target_list_targets(target_list_id, query)
        except NotFoundError as exc:
            await state.clear()
            await message.answer(str(exc))
            return
        results = [
            (
                (
                    "🚫"
                    if item.quality_status == "bad"
                    else "🔴"
                    if item.last_error_code
                    else "🟢"
                    if item.approved_by_operator
                    else "⚪️"
                )
                + f" {item.title} · {item.chat_ref}",
                f"tld:{item.id}:{target_list_id}:{list_page}:0:all",
            )
            for item in items
        ]
        new_search_callback = f"tls:{target_list_id}:{list_page}"
        back_callback = f"tlf:{target_list_id}:{list_page}:all:0"
    elif section == "campaigns":
        items = await repository.search_campaigns(query)
        results = [
            (
                f"{'📢' if item.mode == CampaignMode.BROADCAST else '💬'} {item.name}",
                f"campaign:details:{item.id}:0",
            )
            for item in items
        ]
    elif section in {"runs", "archive"}:
        items = await repository.search_launches(
            query,
            archived=section == "archive",
        )
        results = [
            (
                f"{'📢' if item.mode == CampaignMode.BROADCAST else '💬'} "
                f"{item.campaign_name} · {item.finished}/{item.total}",
                f"launch:details:{item.anchor_run_id}:0",
            )
            for item in items
        ]
    else:
        await state.clear()
        await message.answer("Раздел поиска устарел. Откройте список заново.")
        return
    await _edit_origin_data(
        bot,
        data,
        "<b>🔎 Результаты поиска</b>\n"
        f"<code>{escape(query)}</code> · найдено <b>{len(results)}</b>\n\n"
        + (
            "Выберите нужную карточку."
            if results
            else "Совпадений нет. Попробуйте часть названия или числовой ID."
        ),
        search_results_menu(
            results,
            section=section,
            new_search_callback=new_search_callback,
            back_callback=back_callback,
        ),
    )
    await state.clear()


@router.callback_query(F.data.startswith("menu:"))
async def open_menu(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    proxy_service: ProxyService,
    worker_supervisor: WorkerSupervisor,
) -> None:
    await callback.answer()
    await state.clear()
    parts = (callback.data or "menu:home").split(":")
    section = parts[1]
    page = _page_from(parts)
    if section == "home":
        stats = await repository.dashboard()
        await _edit_callback(
            callback,
            _dashboard(stats, settings, worker_supervisor),
            main_menu(),
            answer=False,
        )
    elif section == "guide":
        await _edit_callback(callback, guide_text(), guide_menu(), answer=False)
    elif section == "accounts":
        await _show_accounts(callback, repository, page)
    elif section == "proxies":
        await _show_proxies(callback, proxy_service, page)
    elif section == "targets":
        await _show_targets(callback, repository, page)
    elif section == "badtargets":
        await _show_bad_targets(callback, repository, page)
    elif section == "targetlists":
        await _show_target_lists(callback, repository, page)
    elif section == "templates":
        await _show_templates(callback, repository, page)
    elif section == "campaigns":
        await _show_campaigns(callback, repository, page)
    elif section == "runs":
        await _show_runs(callback, repository, page)
    elif section == "archive":
        await _show_archived_runs(callback, repository, page)
    elif section == "audit":
        audit_page = await repository.list_audit(page, PAGE_SIZE)
        await _edit_callback(callback, audit_text(audit_page), audit_menu(audit_page), answer=False)
    elif section == "settings":
        stats = await repository.dashboard()
        await _edit_callback(
            callback,
            settings_text(settings, stats),
            settings_menu(),
            answer=False,
        )


@router.callback_query(F.data == "queue:stopall:ask")
async def ask_stop_all(callback: CallbackQuery) -> None:
    await callback.answer()
    await _edit_callback(
        callback,
        "<b>Остановить все отправки?</b>\n\n"
        "Ожидающие отправки сразу остановятся. Для выполняющихся будет выставлен "
        "флаг отмены, и следующий шаг не запустится. Правила комментариев и реакций "
        "участников будут поставлены на паузу; их нужно будет включить вручную.",
        stop_all_confirmation(),
        answer=False,
    )


@router.callback_query(F.data == "queue:stopall:confirm")
async def confirm_stop_all(
    callback: CallbackQuery,
    repository: AdminRepository,
    run_queue: RunQueue,
    settings: Settings,
) -> None:
    count = await run_queue.cancel_all()
    await callback.answer(f"Остановка запрошена для задач: {count}")
    stats = await repository.dashboard()
    await _edit_callback(
        callback,
        settings_text(settings, stats),
        settings_menu(),
        answer=False,
    )


@router.callback_query(F.data == "maintenance:backup")
async def create_backup(
    callback: CallbackQuery,
    settings: Settings,
    repository: AdminRepository,
) -> None:
    try:
        path = create_sqlite_backup(settings.database_url, Path(settings.backup_storage_path))
        verification = verify_sqlite_backup(path)
        await repository.record_backup_created(
            path=str(path),
            size_bytes=verification.size_bytes,
            sha256=verification.sha256,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Резервная копия создана", show_alert=True)
    if isinstance(callback.message, Message):
        await callback.message.answer(
            f"💾 Backup: <code>{escape(str(path))}</code>\n"
            f"Размер: <b>{verification.size_bytes}</b> байт · "
            f"SHA-256: <code>{verification.sha256[:16]}…</code>"
        )


@router.callback_query(F.data == "maintenance:doctor")
async def run_doctor(callback: CallbackQuery, settings: Settings) -> None:
    try:
        report = inspect_sqlite(settings.database_url)
    except (RuntimeError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Проверка завершена")
    icon = "✅" if report.healthy else "❌"
    await _edit_callback(
        callback,
        f"<b>{icon} Диагностика списка</b>\n"
        f"<code>{'СИСТЕМА ИСПРАВНА' if report.healthy else 'НУЖНО ВНИМАНИЕ'}</code>\n\n"
        f"Целостность: <code>{escape(report.integrity)}</code>\n"
        f"Нарушения связей: <b>{report.foreign_key_violations}</b>\n"
        f"Версия схемы: <code>{escape(report.migration_revision or '—')}</code>"
        f" · ожидается <code>{escape(report.expected_migration_revision)}</code>\n"
        f"Миграция актуальна: <b>{'да' if report.migration_current else 'нет'}</b>\n"
        f"Отсутствующие таблицы: "
        f"<code>{escape(', '.join(report.missing_tables) or '—')}</code>\n"
        f"Отсутствующие поля: "
        f"<code>{escape(', '.join(report.missing_columns) or '—')}</code>",
        settings_menu(),
        answer=False,
    )


@router.callback_query(F.data == "proxy:add")
async def begin_proxy_add(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(ProxyAddState.url)
    await _edit_callback(
        callback,
        wizard_text(
            "🌐 Новый прокси",
            1,
            3,
            "Отправьте адрес прокси:\n\n"
            "<code>http://login:password@host:port</code>\n"
            "<code>socks5://login:password@host:port</code>\n"
            "<code>host:port:login:password</code>\n"
            "<code>host:port</code>",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyAddState.url)
async def proxy_add_url(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    try:
        parsed = parse_proxy(value)
    except ProxyInputError as exc:
        await message.answer(f"Некорректный адрес: {escape(str(exc))}")
        return
    await state.update_data(proxy_url=value)
    await state.set_state(ProxyAddState.label)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🌐 Новый прокси",
            2,
            3,
            f"Адрес распознан:\n<code>{escape(parsed.masked_endpoint)}</code>\n\n"
            "Введите короткое название, например <code>Мобильный Proxy-Seller</code>. "
            "Отправьте <code>-</code> для автоматического названия.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAddState.label)
async def proxy_add_label(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if value != "-" and not 1 <= len(value) <= 100:
        await message.answer(
            "Название должно содержать от 1 до 100 символов или быть <code>-</code>."
        )
        return
    await state.update_data(proxy_label=None if value == "-" else value, proxy_group=None)
    await state.set_state(ProxyAddState.rotation_url)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🌐 Новый прокси",
            3,
            3,
            "Если это мобильный прокси, отправьте ссылку ротации IP от провайдера.\n\n"
            "Если ротация не нужна, отправьте <code>-</code>.",
            hint="Ссылка сохранится зашифрованно и не будет показываться обратно в интерфейсе.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAddState.group)
async def proxy_add_group(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    value = (message.text or "").strip()
    if value != "-" and not 1 <= len(value) <= 100:
        await message.answer(
            "Имя группы должно содержать от 1 до 100 символов или быть <code>-</code>."
        )
        return
    await state.update_data(proxy_group=None if value == "-" else value)
    await state.set_state(ProxyAddState.rotation_url)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🌐 Новый прокси",
            4,
            4,
            "Если это мобильный прокси, отправьте ссылку ротации IP от провайдера.\n\n"
            "Если ротация не нужна, отправьте <code>-</code>.",
            hint="Ссылка сохранится зашифрованно и не будет показываться обратно в интерфейсе.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAddState.rotation_url)
async def proxy_add_rotation_url(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    value = (message.text or "").strip()
    try:
        rotation_url = None if value == "-" else validate_rotation_url(value)
    except ProxyInputError as exc:
        await message.answer(f"Некорректная ссылка ротации: {escape(str(exc))}")
        return
    data = await state.get_data()
    try:
        view = await proxy_service.add_proxy(
            str(data["proxy_url"]),
            label=data.get("proxy_label"),
            group_name=data.get("proxy_group"),
            rotation_url=rotation_url,
        )
    except (ProxyInputError, ConflictError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Прокси добавлен. Пароль сохранён в зашифрованном виде.\n\n" + proxy_details_text(view),
        proxy_details_menu(view, 0),
    )


@router.callback_query(F.data == "proxy:import")
async def begin_proxy_import(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(proxy_group=None)
    await state.set_state(ProxyImportState.document)
    await _edit_callback(
        callback,
        wizard_text(
            "📄 Импорт прокси",
            1,
            1,
            "Прикрепите UTF-8 файл <code>.txt</code> — один прокси на строку.\n\n"
            "Поддерживаются HTTP(S), SOCKS5, <code>host:port</code> и "
            "<code>host:port:login:password</code>.\n"
            "Лимит: <b>2000 строк · 2 МБ</b>.",
            hint="Пустые строки и комментарии с # будут пропущены.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyImportState.group)
async def proxy_import_group(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if value != "-" and not 1 <= len(value) <= 100:
        await message.answer(
            "Имя группы должно содержать от 1 до 100 символов или быть <code>-</code>."
        )
        return
    await state.update_data(proxy_group=None if value == "-" else value)
    await state.set_state(ProxyImportState.document)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "📄 Импорт прокси",
            2,
            2,
            "Прикрепите UTF-8 файл <code>.txt</code> — один адрес на строку.\n\n"
            "Поддерживаются HTTP(S), SOCKS5, <code>host:port</code> и "
            "<code>host:port:login:password</code>.\n"
            "Лимит: <b>2000 строк · 2 МБ</b>.",
            hint="Пустые строки и комментарии с # будут пропущены.",
        ),
        cancel_menu(),
    )


@router.message(ProxyImportState.document)
async def import_proxy_document(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    document = message.document
    if document is None or not (document.file_name or "").lower().endswith(".txt"):
        await message.answer("Нужен документ с расширением <code>.txt</code>.")
        return
    if document.file_size is not None and document.file_size > 2_097_152:
        await message.answer("Файл больше 2 МБ.")
        return
    buffer = BytesIO()
    await bot.download(document, destination=buffer)
    try:
        content = buffer.getvalue().decode("utf-8-sig")
    except UnicodeDecodeError:
        await message.answer("Файл должен быть сохранён в UTF-8.")
        return
    values = content.splitlines()
    meaningful = [line for line in values if line.strip() and not line.lstrip().startswith("#")]
    if len(meaningful) > 2000:
        await message.answer("В одном файле разрешено не более 2000 непустых строк.")
        return
    data = await state.get_data()
    try:
        result = await proxy_service.import_proxies(values, group_name=data.get("proxy_group"))
    except ProxyInputError as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    page = await proxy_service.list_proxies(0, PAGE_SIZE)
    error_preview = ""
    if result.errors:
        error_preview = "\n\nПервые ошибки:\n<code>" + escape("\n".join(result.errors)) + "</code>"
    await _edit_origin_data(
        bot,
        data,
        f"✅ Импорт завершён: добавлено <b>{result.added}</b>, "
        f"дубликатов <b>{result.skipped}</b>, некорректных <b>{result.invalid}</b>."
        f"{error_preview}\n\n{proxies_text(page)}",
        proxies_menu(page),
    )


@router.callback_query(F.data.startswith("proxy:list:"))
async def filter_proxies(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, filter_name, page = (callback.data or "").split(":")
    try:
        selected_filter = ProxyFilter(filter_name)
    except ValueError:
        await callback.answer("Неизвестный фильтр", show_alert=True)
        return
    proxy_page = await proxy_service.list_proxies(int(page), PAGE_SIZE, filter_name=selected_filter)
    await _edit_callback(callback, proxies_text(proxy_page), proxies_menu(proxy_page))


@router.callback_query(F.data.startswith("proxy:details:"))
async def show_proxy_details(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        item = await proxy_service.get_proxy(int(proxy_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(callback, proxy_details_text(item), proxy_details_menu(item, int(page)))


@router.callback_query(F.data.startswith("proxy:edit:"))
async def begin_proxy_edit(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(edit_proxy_id=int(proxy_id), edit_proxy_page=int(page))
    await state.set_state(ProxyEditState.url)
    await _edit_callback(
        callback,
        wizard_text(
            "🔑 Адрес и пароль",
            1,
            1,
            "Отправьте полный новый адрес прокси. Название и назначения сохранятся, "
            "а результат проверки будет сброшен.\n\n"
            "🔐 Пароль после сохранения снова станет скрытым.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyEditState.url)
async def save_proxy_edit(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    value = (message.text or "").strip()
    try:
        parse_proxy(value)
    except ProxyInputError as exc:
        await message.answer(f"Некорректный адрес: {escape(str(exc))}")
        return
    data = await state.get_data()
    try:
        item = await proxy_service.replace_proxy_url(int(data["edit_proxy_id"]), value)
        await _close_rotated_account_client(item, gateway)
    except (NotFoundError, ConflictError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Адрес и учётные данные заменены. Прокси нужно проверить заново.\n\n"
        + proxy_details_text(item),
        proxy_details_menu(item, int(data["edit_proxy_page"])),
    )


@router.callback_query(F.data.startswith("proxy:rotation:"))
async def begin_proxy_rotation_edit(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(rotation_proxy_id=int(proxy_id), rotation_proxy_page=int(page))
    await state.set_state(ProxyRotationState.url)
    await _edit_callback(
        callback,
        wizard_text(
            "🔗 Ссылка ротации",
            1,
            1,
            "Отправьте HTTP/HTTPS ссылку провайдера для смены IP. "
            "Она будет сохранена зашифрованно и не будет показана обратно.\n\n"
            "Отправьте <code>-</code>, чтобы очистить ссылку.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyRotationState.url)
async def save_proxy_rotation_url(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    value = (message.text or "").strip()
    data = await state.get_data()
    try:
        item = await proxy_service.set_proxy_rotation_url(
            int(data["rotation_proxy_id"]),
            None if value == "-" else value,
        )
    except (NotFoundError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Ссылка ротации обновлена.\n\n" + proxy_details_text(item),
        proxy_details_menu(item, int(data["rotation_proxy_page"])),
    )


@router.callback_query(F.data.startswith("proxy:rotate:"))
async def rotate_proxy(
    callback: CallbackQuery, proxy_service: ProxyService, gateway: object | None = None
) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    await callback.answer("Отправляю запрос ротации…")
    try:
        before = await proxy_service.get_proxy(int(proxy_id))
        await _close_rotated_account_client(before, gateway)
        item = await proxy_service.rotate_proxy(int(proxy_id))
    except (NotFoundError, ConflictError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    await _edit_callback(
        callback,
        "✅ Ротация выполнена и новый маршрут автоматически проверен.\n\n"
        + proxy_details_text(item),
        proxy_details_menu(item, int(page)),
        answer=False,
    )


async def _close_rotated_account_client(item: object, gateway: object | None) -> None:
    assignment_keys = getattr(item, "assignment_keys", None) or ()
    if not assignment_keys:
        assignment_keys = (getattr(item, "assignment_key", None) or "",)
    for assignment_key in assignment_keys:
        await _close_assignment_account_client(assignment_key, gateway)


async def _close_assignment_account_client(
    assignment_key: str,
    gateway: object | None,
) -> None:
    if not assignment_key.startswith("account:"):
        return
    try:
        account_id = int(assignment_key.split(":", 1)[1])
    except ValueError:
        return
    await _close_account_client(account_id, gateway)


async def _close_account_client(account_id: int, gateway: object | None) -> None:
    if gateway is None:
        return
    close_account = getattr(gateway, "close_account", None)
    if close_account is not None:
        await close_account(account_id)


@router.callback_query(F.data.startswith("proxy:meta:"))
async def begin_proxy_metadata_edit(
    callback: CallbackQuery,
    state: FSMContext,
    proxy_service: ProxyService,
) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        item = await proxy_service.get_proxy(int(proxy_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(
        meta_proxy_id=int(proxy_id),
        meta_proxy_page=int(page),
        meta_group_name=item.group_name,
    )
    await state.set_state(ProxyMetadataEditState.label)
    await _edit_callback(
        callback,
        wizard_text(
            "🏷 Название прокси",
            1,
            1,
            "Введите новое понятное название, например <code>Мобильный основной</code>.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyMetadataEditState.label)
async def proxy_metadata_label(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 100:
        await message.answer("Название должно содержать от 1 до 100 символов.")
        return
    data = await state.get_data()
    try:
        item = await proxy_service.update_proxy_metadata(
            int(data["meta_proxy_id"]),
            label=value,
            group_name=data.get("meta_group_name"),
        )
    except (NotFoundError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Название обновлено.\n\n" + proxy_details_text(item),
        proxy_details_menu(item, int(data["meta_proxy_page"])),
    )


@router.message(ProxyMetadataEditState.group)
async def save_proxy_metadata(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    group = (message.text or "").strip()
    if group != "-" and not 1 <= len(group) <= 100:
        await message.answer("Имя группы должно содержать от 1 до 100 символов или быть «-».")
        return
    data = await state.get_data()
    try:
        item = await proxy_service.update_proxy_metadata(
            int(data["meta_proxy_id"]),
            label=str(data["meta_label"]),
            group_name=None if group == "-" else group,
        )
    except (NotFoundError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Название и папка обновлены.\n\n" + proxy_details_text(item),
        proxy_details_menu(item, int(data["meta_proxy_page"])),
    )


@router.callback_query(F.data.startswith("proxy:toggle:"))
async def toggle_proxy(
    callback: CallbackQuery,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        item = await proxy_service.toggle_proxy(int(proxy_id))
        await _close_rotated_account_client(item, gateway)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Статус изменён")
    await _edit_callback(
        callback, proxy_details_text(item), proxy_details_menu(item, int(page)), answer=False
    )


@router.callback_query(F.data.startswith("proxy:check:") & ~F.data.startswith("proxy:checkall:"))
async def check_proxy(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    await callback.answer("Проверяю соединение…")
    try:
        result = await proxy_service.check_proxy(int(proxy_id))
        item = await proxy_service.get_proxy(int(proxy_id))
    except (NotFoundError, ConflictError, ProxyInputError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(escape(str(exc)))
        return
    verdict = "✅ прокси исправен" if result.healthy else "❌ соединение не установлено"
    await _edit_callback(
        callback,
        f"<b>{verdict}</b>\n\n{proxy_details_text(item)}",
        proxy_details_menu(item, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("proxy:checkall:"))
async def check_all_proxies(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    page_number = int((callback.data or "").rsplit(":", 1)[1])
    await callback.answer("Запустил параллельную проверку")
    results = await proxy_service.check_all()
    healthy = sum(result.healthy for result in results)
    page = await proxy_service.list_proxies(page_number, PAGE_SIZE)
    await _edit_callback(
        callback,
        f"<b>🩺 Проверка завершена: {healthy}/{len(results)} исправны</b>\n\n" + proxies_text(page),
        proxies_menu(page),
        answer=False,
    )


@router.callback_query(F.data == "proxy:autoassign")
async def begin_proxy_auto_assign(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(ProxyAutoAssignState.group)
    await _edit_callback(
        callback,
        wizard_text(
            "⚡ Подбор лучшего прокси",
            1,
            3,
            "Введите группу, из которой нужно выбрать самый быстрый рабочий "
            "прокси. Отправьте <code>-</code> для поиска по всему пулу.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyAutoAssignState.group)
async def proxy_auto_group(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if value != "-" and not 1 <= len(value) <= 100:
        await message.answer("Имя группы должно содержать от 1 до 100 символов или быть «-».")
        return
    await state.update_data(auto_group=None if value == "-" else value)
    await state.set_state(ProxyAutoAssignState.assignment_key)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "⚡ Подбор лучшего прокси",
            2,
            3,
            "Введите уникальный ключ назначения, например <code>worker-eu-01</code>.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAutoAssignState.assignment_key)
async def proxy_auto_key(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 150:
        await message.answer("Ключ должен содержать от 1 до 150 символов.")
        return
    await state.update_data(assignment_key=value)
    await state.set_state(ProxyAutoAssignState.purpose)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "⚡ Подбор лучшего прокси",
            3,
            3,
            "Опишите назначение или отправьте <code>-</code> без описания.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAutoAssignState.purpose)
async def finish_proxy_auto_assign(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    purpose = (message.text or "").strip()
    if purpose != "-" and len(purpose) > 250:
        await message.answer("Описание не должно быть длиннее 250 символов.")
        return
    data = await state.get_data()
    try:
        item = await proxy_service.allocate_proxy(
            str(data["assignment_key"]),
            "" if purpose == "-" else purpose,
            group_name=data.get("auto_group"),
        )
        await _close_assignment_account_client(str(data["assignment_key"]), gateway)
    except (ConflictError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Выбран самый быстрый рабочий прокси.\n\n" + proxy_details_text(item),
        proxy_details_menu(item, 0),
    )


@router.callback_query(F.data.startswith("proxy:assign:"))
async def begin_proxy_assign(callback: CallbackQuery, state: FSMContext) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(assign_proxy_id=int(proxy_id), assign_proxy_page=int(page))
    await state.set_state(ProxyAssignState.assignment_key)
    await _edit_callback(
        callback,
        wizard_text(
            "📌 Резервирование прокси",
            1,
            2,
            "Введите уникальный ключ назначения, например <code>worker-eu-01</code>. "
            "Один прокси можно назначать нескольким аккаунтам.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyAssignState.assignment_key)
async def proxy_assignment_key(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 150:
        await message.answer("Ключ должен содержать от 1 до 150 символов.")
        return
    await state.update_data(assignment_key=value)
    await state.set_state(ProxyAssignState.purpose)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "📌 Резервирование прокси",
            2,
            2,
            "Опишите назначение или отправьте <code>-</code> без описания.",
        ),
        cancel_menu(),
    )


@router.message(ProxyAssignState.purpose)
async def proxy_assignment_purpose(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    purpose = (message.text or "").strip()
    if purpose != "-" and len(purpose) > 250:
        await message.answer("Описание не должно быть длиннее 250 символов.")
        return
    data = await state.get_data()
    try:
        item = await proxy_service.assign_proxy(
            int(data["assign_proxy_id"]),
            str(data["assignment_key"]),
            "" if purpose == "-" else purpose,
        )
        await _close_assignment_account_client(str(data["assignment_key"]), gateway)
    except (NotFoundError, ConflictError, ProxyInputError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ Прокси зарезервирован.\n\n" + proxy_details_text(item),
        proxy_details_menu(item, int(data["assign_proxy_page"])),
    )


@router.callback_query(F.data.startswith("proxy:release:"))
async def release_proxy(
    callback: CallbackQuery,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        before = await proxy_service.get_proxy(int(proxy_id))
        item = await proxy_service.release_proxy(int(proxy_id))
        await _close_rotated_account_client(before, gateway)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Прокси освобождён")
    await _edit_callback(
        callback, proxy_details_text(item), proxy_details_menu(item, int(page)), answer=False
    )


@router.callback_query(F.data.startswith("proxy:delete:ask:"))
async def ask_delete_proxy(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        item = await proxy_service.get_proxy(int(proxy_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>Удалить прокси #{item.proxy.id}?</b>\n\n"
        "Действие необратимо. Назначенный прокси сначала нужно освободить.",
        proxy_delete_confirmation(item.proxy.id, int(page)),
    )


@router.callback_query(F.data.startswith("proxy:delete:confirm:"))
async def confirm_delete_proxy(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, _, proxy_id, page = (callback.data or "").split(":")
    try:
        await proxy_service.delete_proxy(int(proxy_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Прокси удалён")
    await _show_proxies(callback, proxy_service, int(page))


@router.callback_query(F.data == "proxy:groups")
async def show_proxy_groups(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    groups = await proxy_service.list_groups()
    await _edit_callback(callback, proxy_groups_text(groups), proxy_groups_menu(groups))


@router.callback_query(F.data.startswith("proxy:groupfilter:"))
async def filter_proxy_group(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    _, _, group_id, page = (callback.data or "").split(":")
    proxy_page = await proxy_service.list_proxies(int(page), PAGE_SIZE, group_id=int(group_id))
    await _edit_callback(callback, proxies_text(proxy_page), proxies_menu(proxy_page))


@router.callback_query(F.data.startswith("proxy:group:delete:ask:"))
async def ask_delete_proxy_group(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    group_id = int((callback.data or "").rsplit(":", 1)[1])
    groups = await proxy_service.list_groups()
    group = next((item for item in groups if item.id == group_id), None)
    if group is None:
        await callback.answer("Группа не найдена", show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить группу «{escape(group.name)}»?</b>\n\n"
        "Прокси сохранятся и перейдут в раздел «без группы». Это действие не удаляет адреса.",
        proxy_group_delete_confirmation(group_id),
    )


@router.callback_query(F.data.startswith("proxy:group:delete:confirm:"))
async def delete_proxy_group(callback: CallbackQuery, proxy_service: ProxyService) -> None:
    group_id = int((callback.data or "").rsplit(":", 1)[1])
    try:
        await proxy_service.delete_group(group_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    groups = await proxy_service.list_groups()
    await callback.answer("Группа удалена; прокси оставлены без группы")
    await _edit_callback(
        callback,
        proxy_groups_text(groups),
        proxy_groups_menu(groups),
        answer=False,
    )


@router.callback_query(F.data == "proxy:group:add")
async def begin_proxy_group_create(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(ProxyGroupCreateState.name)
    await _edit_callback(
        callback,
        wizard_text(
            "🗂 Новая группа",
            1,
            2,
            "Введите уникальное короткое имя, например <code>EU</code>.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(ProxyGroupCreateState.name)
async def proxy_group_name(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 100:
        await message.answer("Имя должно содержать от 1 до 100 символов.")
        return
    await state.update_data(proxy_group_name=value)
    await state.set_state(ProxyGroupCreateState.description)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🗂 Новая группа",
            2,
            2,
            "Введите назначение группы или отправьте <code>-</code> без описания.",
        ),
        cancel_menu(),
    )


@router.message(ProxyGroupCreateState.description)
async def proxy_group_description(
    message: Message,
    state: FSMContext,
    bot: Bot,
    proxy_service: ProxyService,
) -> None:
    value = (message.text or "").strip()
    if value != "-" and len(value) > 500:
        await message.answer("Описание не должно быть длиннее 500 символов.")
        return
    data = await state.get_data()
    try:
        await proxy_service.create_group(
            str(data["proxy_group_name"]), "" if value == "-" else value
        )
    except (ProxyInputError, ConflictError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    groups = await proxy_service.list_groups()
    await _edit_origin_data(
        bot,
        data,
        "✅ Группа создана.\n\n" + proxy_groups_text(groups),
        proxy_groups_menu(groups),
    )


@router.callback_query(F.data == "account:add")
async def begin_account_create(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(AccountCreateState.label)
    await _edit_callback(
        callback,
        wizard_text(
            "👥 Новая запись аккаунта",
            1,
            3,
            "Введите понятное название. Эта запись будет без сессии и не сможет "
            "участвовать в рабочей отправке.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(AccountCreateState.label)
async def account_label(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 100:
        await message.answer("Название должно содержать от 1 до 100 символов.")
        return
    await state.update_data(label=value)
    await state.set_state(AccountCreateState.telegram_user_id)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "👥 Новая запись аккаунта",
            2,
            3,
            "Введите числовой Telegram User ID или отправьте <code>-</code>.",
        ),
        cancel_menu(),
    )


@router.message(AccountCreateState.telegram_user_id)
async def account_telegram_id(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if value == "-":
        telegram_user_id = None
    else:
        try:
            telegram_user_id = int(value)
        except ValueError:
            await message.answer("Нужен числовой ID или символ <code>-</code>.")
            return
        if telegram_user_id <= 0:
            await message.answer("Telegram User ID должен быть положительным.")
            return
    await state.update_data(telegram_user_id=telegram_user_id)
    await state.set_state(AccountCreateState.confirm)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "👥 Новая запись аккаунта",
            3,
            3,
            "Подтвердите, что аккаунт принадлежит вам. Подключение и авторизация "
            "на этом этапе не выполняются.",
        ),
        ownership_confirmation(),
    )


@router.callback_query(AccountCreateState.confirm, F.data == "account:confirm:yes")
async def confirm_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    try:
        account = await repository.create_account(
            label=str(data["label"]),
            telegram_user_id=data.get("telegram_user_id"),
            ownership_confirmed=True,
        )
    except ConflictError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Аккаунт добавлен")
    page = await repository.list_accounts(0, PAGE_SIZE)
    await _edit_callback(
        callback,
        f"✅ Аккаунт <b>{escape(account.label)}</b> добавлен.\n\n{accounts_text(page)}",
        accounts_menu(page),
        answer=False,
    )


@router.callback_query(F.data.startswith("account:toggle:"))
async def toggle_account(
    callback: CallbackQuery,
    repository: AdminRepository,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    _, _, account_id, page = (callback.data or "").split(":")
    try:
        await repository.toggle_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Статус аккаунта изменён")
    await _show_account_details(
        callback, account_service, proxy_service, int(account_id), int(page)
    )


@router.callback_query(F.data.startswith("account:details:"))
async def account_details(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    _, _, account_id, page = (callback.data or "").split(":")
    await _show_account_details(
        callback, account_service, proxy_service, int(account_id), int(page), answer=True
    )


@router.callback_query(F.data.startswith("account:check:"))
async def check_account(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    spambot_checks: SpamBotCheckService | None = None,
) -> None:
    _, _, account_id, page = (callback.data or "").split(":")
    await callback.answer("Проверяю сессию…")
    try:
        result = await account_service.check_account(int(account_id))
    except (NotFoundError, AccountImportError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    if result.error and result.authorized:
        verdict = "⚠️ Сессия была активна, но текущая проверка связи не выполнена"
    else:
        verdict = "✅ Сессия активна" if result.authorized else "❌ Сессия недоступна"
    detail = ""
    if result.error:
        detail = f"\n<code>{escape(result.error)}</code>"
    if result.authorized and not result.error and spambot_checks is not None:
        restriction = await spambot_checks.check_account(
            int(account_id),
            trigger="manual_check",
            force=True,
        )
        if restriction is not None:
            restriction_text = {
                SpamBotStatus.FREE: "✅ Ограничений @SpamBot нет",
                SpamBotStatus.TEMPORARY_RESTRICTION: "⏳ @SpamBot: временное ограничение",
                SpamBotStatus.PERMANENT_RESTRICTION: "⛔ @SpamBot: постоянное ограничение",
                SpamBotStatus.UNKNOWN: "⚠️ Ответ @SpamBot не удалось однозначно распознать",
            }[restriction.status]
            detail += f"\n<b>{restriction_text}</b>"
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner=f"<b>{verdict}</b>{detail}\n\n",
    )


def _profile_name_account_ready(account: Account) -> bool:
    return bool(
        account.session_source != AccountSessionSource.NONE
        and account.session_encrypted
        and account.api_id
        and account.api_hash_encrypted
    )


@router.callback_query(F.data == "account:names:menu")
async def show_bulk_account_names(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    accounts = [
        account
        for account in await repository.active_owned_accounts()
        if _profile_name_account_ready(account)
    ]
    if not accounts:
        await callback.answer(
            "Нет активных аккаунтов с загруженной Telegram-сессией",
            show_alert=True,
        )
        return
    await state.clear()
    await _edit_callback(
        callback,
        account_name_catalog_text(accounts),
        account_name_catalog_menu(),
    )


@router.callback_query(F.data.startswith("account:name:catalog:"))
async def show_single_account_name_catalog(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not _profile_name_account_ready(account):
        await callback.answer("У аккаунта нет готовой Telegram-сессии", show_alert=True)
        return
    await state.clear()
    await _edit_callback(
        callback,
        account_name_catalog_text([account], single_account=account),
        account_name_catalog_menu(account_id=account.id, page=int(page)),
    )


@router.callback_query(F.data.startswith("account:names:preview:"))
async def preview_generated_account_names(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    account_service: AccountService,
) -> None:
    _, _, _, locale, account_id, page = (callback.data or "").split(":")
    if locale not in {"mixed", "ru", "en"}:
        await callback.answer("Неизвестный каталог имён", show_alert=True)
        return
    selected_account_id = int(account_id)
    if selected_account_id:
        try:
            account = await account_service.get_account(selected_account_id)
        except NotFoundError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        candidates = [account] if _profile_name_account_ready(account) else []
    else:
        candidates = [
            account
            for account in await repository.active_owned_accounts()
            if _profile_name_account_ready(account)
        ]
    if not candidates:
        await callback.answer("Нет аккаунтов, готовых к изменению профиля", show_alert=True)
        return
    if len(candidates) > 200:
        await callback.answer(
            "За одну операцию поддерживается не более 200 активных аккаунтов",
            show_alert=True,
        )
        return

    used_names = await repository.existing_account_profile_names()
    name_locale: NameLocale = locale
    available_count = available_profile_name_capacity(name_locale, used_names=used_names)
    if available_count < len(candidates):
        await callback.answer(
            "В выбранном каталоге осталось только "
            f"{available_count} свободных имён для {len(candidates)} аккаунтов. "
            "Выберите смешанный каталог или меняйте имена по одному.",
            show_alert=True,
        )
        return
    try:
        generated = generate_profile_names(
            len(candidates),
            locale=name_locale,
            used_names=used_names,
        )
    except ValueError:
        # A concurrent name update may consume a catalogue entry after the pre-check.
        await callback.answer(
            "Не удалось подобрать свободные имена. Обновите список и попробуйте снова.",
            show_alert=True,
        )
        return
    assignments = [
        {
            "account_id": account.id,
            "label": account.label,
            "first_name": profile_name.first_name,
            "last_name": profile_name.last_name,
            "locale": profile_name.locale,
            "style": profile_name.style,
        }
        for account, profile_name in zip(candidates, generated, strict=True)
    ]
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(
        profile_name_assignments=assignments,
        profile_name_locale=locale,
        profile_name_account_id=selected_account_id,
        profile_name_page=int(page),
    )
    await state.set_state(AccountBulkNameState.confirm)
    await _edit_callback(
        callback,
        account_generated_names_preview_text(assignments, locale=name_locale),
        account_generated_names_confirmation(
            locale=locale,
            account_id=selected_account_id,
            page=int(page),
        ),
    )


@router.callback_query(AccountBulkNameState.confirm, F.data == "account:names:apply")
async def apply_generated_account_names(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    data = await state.get_data()
    raw_assignments = [
        dict(item) for item in data.get("profile_name_assignments", []) if isinstance(item, dict)
    ]
    if not raw_assignments:
        await callback.answer("Превью устарело — сформируйте имена заново", show_alert=True)
        return
    assignments = {
        int(item["account_id"]): ProfileName(
            first_name=str(item["first_name"]),
            last_name=str(item.get("last_name") or ""),
            locale=str(item["locale"]),  # type: ignore[arg-type]
            style=str(item["style"]),  # type: ignore[arg-type]
        )
        for item in raw_assignments
    }
    labels = {int(item["account_id"]): str(item["label"]) for item in raw_assignments}
    await callback.answer("Применяю имена через Telegram…")
    if isinstance(callback.message, Message):
        await callback.message.edit_text(
            "<b>⏳ Обновляю профили Telegram</b>\n\n"
            f"Аккаунтов: <b>{len(assignments)}</b>. Сбой одного аккаунта не остановит остальные.",
            reply_markup=None,
        )
    result = await account_service.bulk_update_profile_names(assignments)
    failures = [
        (labels.get(failure.account_id, f"аккаунт #{failure.account_id}"), failure.error)
        for failure in result.failures
    ]
    result_text = account_names_result_text(
        updated_count=len(result.updated),
        failures=failures,
    )
    selected_account_id = int(data.get("profile_name_account_id") or 0)
    page_number = int(data.get("profile_name_page") or 0)
    await state.clear()
    if selected_account_id:
        await _show_account_details(
            callback,
            account_service,
            proxy_service,
            selected_account_id,
            page_number,
            banner=result_text + "\n\n",
        )
        return
    page_data = await repository.list_accounts(page_number, PAGE_SIZE)
    await _edit_callback(
        callback,
        result_text + "\n\n" + accounts_text(page_data),
        accounts_menu(page_data),
        answer=False,
    )


@router.callback_query(F.data.startswith("account:name:edit:"))
async def begin_account_name_edit(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if account.session_source.value == "none":
        await callback.answer("У аккаунта нет Telegram-сессии", show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(profile_account_id=int(account_id), profile_page=int(page))
    await state.set_state(AccountNameEditState.first_name)
    current = escape(account.first_name or "не загружено")
    await _edit_callback(
        callback,
        wizard_text(
            "✏️ Имя Telegram",
            1,
            2,
            f"Текущее имя: <b>{current}</b>\n\nВведите новое имя Telegram.",
        ),
        cancel_menu(),
    )


@router.callback_query(F.data.startswith("account:label:edit:"))
async def begin_account_label_edit(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(label_account_id=int(account_id), label_page=int(page))
    await state.set_state(AccountLabelEditState.label)
    await _edit_callback(
        callback,
        wizard_text(
            "🏷 Название в боте",
            1,
            1,
            f"Сейчас: <b>{escape(account.label)}</b>\n\n"
            "Введите удобное название до <b>100 символов</b>. Это только ваша подпись "
            "в панели: имя профиля Telegram не изменится.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(AccountLabelEditState.label)
async def save_account_label(
    message: Message,
    state: FSMContext,
    bot: Bot,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    data = await state.get_data()
    account_id = int(data.get("label_account_id", 0))
    page = int(data.get("label_page", 0))
    try:
        account = await account_service.rename_account_label(
            account_id,
            label=message.text or "",
        )
    except (NotFoundError, ConflictError, AccountImportError) as exc:
        await message.answer(f"❌ {escape(str(exc))}")
        return
    proxy_slot, shared_proxy_accounts = await _account_proxy_summary(proxy_service, account_id)
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>Название в боте обновлено. Профиль Telegram не менялся.</b>\n\n"
        + account_details_text(
            account,
            proxy_slot=proxy_slot,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
        account_details_menu(
            account,
            page,
            has_proxy=proxy_slot is not None,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
    )


@router.message(AccountNameEditState.first_name)
async def account_profile_first_name(message: Message, state: FSMContext, bot: Bot) -> None:
    try:
        first_name = _profile_name_input(message.text or "", required=True)
    except ValueError as exc:
        await message.answer(escape(str(exc)))
        return
    await state.update_data(profile_first_name=first_name)
    await state.set_state(AccountNameEditState.last_name)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "✏️ Имя Telegram",
            2,
            2,
            "Введите фамилию или отправьте <code>-</code>, чтобы оставить поле пустым.",
        ),
        cancel_menu(),
    )


@router.message(AccountNameEditState.last_name)
async def account_profile_last_name(message: Message, state: FSMContext, bot: Bot) -> None:
    raw_value = (message.text or "").strip()
    try:
        last_name = "" if raw_value == "-" else _profile_name_input(raw_value, required=False)
    except ValueError as exc:
        await message.answer(escape(str(exc)))
        return
    await state.update_data(profile_last_name=last_name)
    await state.set_state(AccountNameEditState.confirm)
    data = await state.get_data()
    await _edit_origin(
        bot,
        state,
        account_name_confirmation_text(data),
        account_name_confirmation(int(data["profile_account_id"]), int(data["profile_page"])),
    )


@router.callback_query(
    AccountNameEditState.confirm,
    F.data.startswith("account:name:confirm:"),
)
async def confirm_account_name_edit(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    data = await state.get_data()
    if int(data.get("profile_account_id", 0)) != int(account_id):
        await callback.answer("Карточка аккаунта устарела", show_alert=True)
        return
    await callback.answer("Меняю имя в Telegram…")
    try:
        await account_service.update_profile_name(
            int(account_id),
            first_name=str(data["profile_first_name"]),
            last_name=str(data.get("profile_last_name") or ""),
        )
    except (NotFoundError, AccountImportError, AccountProfileError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    await state.clear()
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner="✅ <b>Имя Telegram обновлено.</b>\n\n",
    )


@router.callback_query(F.data.startswith("account:about:edit:"))
async def begin_account_about_edit(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not _profile_name_account_ready(account):
        await callback.answer("У аккаунта нет готовой Telegram-сессии", show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(profile_account_id=int(account_id), profile_page=int(page))
    await state.set_state(AccountAboutEditState.text)
    current = escape(account.about or "не задано")
    await _edit_callback(
        callback,
        wizard_text(
            "📝 Описание Telegram",
            1,
            1,
            f"Текущее описание: <code>{current}</code>\n\n"
            "Отправьте новый текст до <b>70 символов</b>. Ссылка вида "
            "<code>https://site.example</code> станет кликабельной в профиле. "
            "Отправьте <code>-</code>, чтобы очистить описание.",
        ),
        cancel_menu(),
    )


@router.message(AccountAboutEditState.text)
async def account_profile_about(message: Message, state: FSMContext, bot: Bot) -> None:
    try:
        about = _profile_about_input(message.text or "")
    except ValueError as exc:
        await message.answer(escape(str(exc)))
        return
    await state.update_data(profile_about=about)
    data = await state.get_data()
    await state.set_state(AccountAboutEditState.confirm)
    preview = escape(about or "— будет очищено —")
    await _edit_origin(
        bot,
        state,
        f"<b>📝 Новое описание</b>\n\n<code>{preview}</code>\n\nСохранить в Telegram?",
        account_about_confirmation(int(data["profile_account_id"]), int(data["profile_page"])),
    )


@router.callback_query(
    AccountAboutEditState.confirm,
    F.data.startswith("account:about:confirm:"),
)
async def confirm_account_about_edit(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    data = await state.get_data()
    if int(data.get("profile_account_id", 0)) != int(account_id):
        await callback.answer("Карточка аккаунта устарела", show_alert=True)
        return
    await callback.answer("Сохраняю описание в Telegram…")
    try:
        await account_service.update_profile_about(
            int(account_id), about=str(data["profile_about"])
        )
    except (NotFoundError, AccountImportError, AccountProfileError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    await state.clear()
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner="✅ <b>Описание Telegram обновлено.</b>\n\n",
    )


@router.callback_query(F.data == "account:about:bulk")
async def show_bulk_account_about(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _edit_callback(
        callback,
        "<b>📝 Описания профилей</b>\n\n"
        "Можно поставить один текст всем активным аккаунтам или очистить все описания. "
        "У каждого аккаунта также есть отдельная кнопка «Описание».",
        account_about_bulk_menu(),
    )


@router.callback_query(F.data == "account:about:bulk:set")
async def begin_bulk_account_about(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _remember_origin(callback, state)
    await state.set_state(AccountAboutBulkState.text)
    await _edit_callback(
        callback,
        wizard_text(
            "📝 Описание для всех",
            1,
            1,
            "Отправьте текст до <b>70 символов</b>. Он будет установлен у всех активных "
            "аккаунтов с готовой Telegram-сессией. Обычная ссылка <code>https://…</code> "
            "будет кликабельной в профиле.",
        ),
        cancel_menu(),
    )


@router.message(AccountAboutBulkState.text)
async def receive_bulk_account_about(message: Message, state: FSMContext, bot: Bot) -> None:
    try:
        about = _profile_about_input(message.text or "")
    except ValueError as exc:
        await message.answer(escape(str(exc)))
        return
    if not about:
        await message.answer("Для очистки используйте кнопку «Очистить все».")
        return
    await state.update_data(profile_about_bulk=about)
    await state.set_state(AccountAboutBulkState.confirm)
    await _edit_origin(
        bot,
        state,
        f"<b>📝 Установить это описание всем?</b>\n\n<code>{escape(about)}</code>",
        account_about_bulk_confirmation(),
    )


async def _apply_bulk_account_about(
    callback: CallbackQuery,
    *,
    about: str,
    repository: AdminRepository,
    account_service: AccountService,
) -> None:
    accounts = [
        account
        for account in await repository.active_owned_accounts()
        if _profile_name_account_ready(account)
    ]
    if not accounts:
        await callback.answer("Нет аккаунтов с готовой Telegram-сессией", show_alert=True)
        return
    if len(accounts) > 200:
        await callback.answer("За один раз можно изменить не более 200 аккаунтов", show_alert=True)
        return
    await callback.answer("Обновляю описания через Telegram…")
    result = await account_service.bulk_update_profile_about(
        [int(account.id) for account in accounts], about=about
    )
    failures = "\n".join(
        f"• #{item.account_id}: {escape(item.error)}" for item in result.failures[:5]
    )
    await _edit_callback(
        callback,
        f"<b>✅ Описания обработаны</b>\n\n"
        f"Обновлено: <b>{len(result.updated)}</b>\n"
        f"Не удалось: <b>{len(result.failures)}</b>"
        + (f"\n\n<b>Первые ошибки</b>\n{failures}" if failures else ""),
        account_about_bulk_menu(),
        answer=False,
    )


@router.callback_query(AccountAboutBulkState.confirm, F.data == "account:about:bulk:apply:confirm")
async def confirm_bulk_account_about(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    account_service: AccountService,
) -> None:
    about = str((await state.get_data()).get("profile_about_bulk") or "")
    if not about:
        await callback.answer("Введите описание заново", show_alert=True)
        return
    await state.clear()
    await _apply_bulk_account_about(
        callback, about=about, repository=repository, account_service=account_service
    )


@router.callback_query(F.data == "account:about:bulk:clear:ask")
async def ask_clear_bulk_account_about(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _edit_callback(
        callback,
        "<b>🧹 Очистить все описания?</b>\n\n"
        "Описание будет удалено у всех активных аккаунтов с готовой Telegram-сессией.",
        account_about_bulk_confirmation(clear=True),
    )


@router.callback_query(F.data == "account:about:bulk:clear:confirm")
async def confirm_clear_bulk_account_about(
    callback: CallbackQuery,
    repository: AdminRepository,
    account_service: AccountService,
) -> None:
    await _apply_bulk_account_about(
        callback, about="", repository=repository, account_service=account_service
    )


@router.callback_query(F.data.startswith("account:avatar:menu:"))
async def show_account_avatar_menu(
    callback: CallbackQuery,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        account_avatar_text(account),
        account_avatar_menu(
            account.id,
            int(page),
            has_avatar=bool(account.has_profile_photo),
        ),
    )


@router.callback_query(F.data.startswith("account:avatar:upload:"))
async def begin_account_avatar_upload(
    callback: CallbackQuery,
    state: FSMContext,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if account.session_source.value == "none":
        await callback.answer("У аккаунта нет Telegram-сессии", show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(profile_account_id=int(account_id), profile_page=int(page))
    await state.set_state(AccountAvatarEditState.photo)
    await _edit_callback(
        callback,
        wizard_text(
            "🖼 Новый аватар Telegram",
            1,
            1,
            "Отправьте фотографию или файл <b>JPG/PNG</b> до <b>10 МБ</b>. "
            "Telegram сделает изображение текущим аватаром.",
        ),
        cancel_menu(),
    )


@router.message(AccountAvatarEditState.photo)
async def account_avatar_photo(
    message: Message,
    state: FSMContext,
    bot: Bot,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    try:
        photo_bytes, file_name = await _download_profile_photo(message, bot)
    except AccountProfileError as exc:
        await message.answer(f"❌ {escape(str(exc))}")
        return
    data = await state.get_data()
    account_id = int(data["profile_account_id"])
    try:
        account = await account_service.update_profile_photo(
            account_id,
            photo_bytes=photo_bytes,
            file_name=file_name,
        )
    except (NotFoundError, AccountImportError, AccountProfileError) as exc:
        await message.answer(f"❌ {escape(str(exc))}")
        return
    proxy_slot, shared_proxy_accounts = await _account_proxy_summary(proxy_service, account_id)
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>Аватар Telegram обновлён.</b>\n\n"
        + account_details_text(
            account,
            proxy_slot=proxy_slot,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
        account_details_menu(
            account,
            int(data["profile_page"]),
            has_proxy=proxy_slot is not None,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
    )


@router.callback_query(F.data.startswith("account:avatar:delete:ask:"))
async def ask_delete_account_avatars(
    callback: CallbackQuery,
    account_service: AccountService,
) -> None:
    _, _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить все аватары «{escape(account.label)}»?</b>\n\n"
        "Telegram удалит текущую фотографию и всю историю предыдущих аватаров. "
        "Действие необратимо.",
        account_avatar_delete_confirmation(account.id, int(page)),
    )


@router.callback_query(F.data.startswith("account:avatar:delete:confirm:"))
async def confirm_delete_account_avatars(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
) -> None:
    _, _, _, _, account_id, page = (callback.data or "").split(":")
    await callback.answer("Удаляю аватары в Telegram…")
    try:
        _, deleted = await account_service.delete_profile_photos(int(account_id))
    except (NotFoundError, AccountImportError, AccountProfileError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner=f"✅ <b>Удалено аватаров: {deleted}.</b>\n\n",
    )


@router.callback_query(F.data.startswith("account:proxy:auto:"))
async def account_bind_proxy(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        await proxy_service.allocate_proxy(
            f"account:{account_id}", purpose=f"account #{account_id}"
        )
        await _close_account_client(int(account_id), gateway)
    except (ConflictError, ProxyInputError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Назначен самый быстрый рабочий прокси")
    await _show_account_details(
        callback, account_service, proxy_service, int(account_id), int(page)
    )


@router.callback_query(F.data.startswith("account:proxy:release:"))
async def account_release_proxy(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    released = await proxy_service.release_assignment_key(f"account:{account_id}")
    if not released:
        await callback.answer("У аккаунта нет назначенного прокси", show_alert=True)
        return
    await _close_account_client(int(account_id), gateway)
    await callback.answer("Прокси освобождён")
    await _show_account_details(
        callback, account_service, proxy_service, int(account_id), int(page)
    )


@router.callback_query(F.data.startswith("account:proxy:rotate:"))
async def account_rotate_proxy(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    item = await proxy_service.proxy_for_assignment(f"account:{account_id}")
    if item is None:
        await callback.answer("У аккаунта нет назначенного прокси", show_alert=True)
        return
    await callback.answer("Отправляю запрос ротации…")
    try:
        await _close_rotated_account_client(item, gateway)
        await proxy_service.rotate_proxy(item.proxy.id)
    except (NotFoundError, ConflictError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")
        return
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner=(
            "✅ <b>Ротация подтверждена. Прокси проверен, все аккаунты на нём "
            "переподключатся через новый маршрут.</b>\n\n"
        ),
    )


@router.callback_query(F.data.startswith("account:delete:ask:"))
async def ask_delete_account(
    callback: CallbackQuery,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить запись «{escape(account.label)}»?</b>\n\n"
        "Запись удалится только если она не используется сохранённым сценарием или "
        "историей. Для связанного аккаунта используйте «Стереть сессию и доступ».",
        account_delete_confirmation(int(account_id), int(page)),
    )


@router.callback_query(F.data.startswith("account:session:ask:"))
async def ask_clear_account_session(
    callback: CallbackQuery,
    account_service: AccountService,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        account = await account_service.get_account(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🔐 Стереть доступ «{escape(account.label)}»?</b>\n\n"
        "Зашифрованная Telegram-сессия, API-данные и назначение прокси будут удалены. "
        "Историческая запись останется отключённой для журнала. Действие необратимо.",
        account_session_clear_confirmation(account.id, int(page)),
    )


@router.callback_query(F.data.startswith("account:session:confirm:"))
async def confirm_clear_account_session(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    gateway: object | None = None,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        await account_service.clear_session(int(account_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _close_account_client(int(account_id), gateway)
    await callback.answer("Сессия и доступ стёрты")
    await _show_account_details(
        callback,
        account_service,
        proxy_service,
        int(account_id),
        int(page),
        banner="✅ <b>Доступ удалён; запись оставлена только для истории.</b>\n\n",
    )


@router.callback_query(F.data.startswith("account:delete:confirm:"))
async def confirm_delete_account(
    callback: CallbackQuery,
    repository: AdminRepository,
    account_service: AccountService,
    gateway: object | None = None,
) -> None:
    _, _, _, account_id, page = (callback.data or "").split(":")
    try:
        await account_service.delete_account(int(account_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _close_account_client(int(account_id), gateway)
    await callback.answer("Аккаунт удалён")
    await _show_accounts(callback, repository, int(page))


@router.callback_query(F.data.startswith("account:import:"))
async def begin_account_import(
    callback: CallbackQuery,
    state: FSMContext,
    settings: Settings,
) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) not in {3, 4} or parts[:2] != ["account", "import"]:
        await callback.answer("Неизвестный тип импорта", show_alert=True)
        return
    kind = parts[2]
    try:
        source_page = max(0, int(parts[3])) if len(parts) == 4 else 0
    except ValueError:
        await callback.answer("Не удалось открыть импорт", show_alert=True)
        return
    if kind == "menu":
        await callback.answer()
        await _edit_callback(
            callback,
            "<b>📥 Импорт аккаунта</b>\n\n"
            "Выберите, что у вас есть. Бот сам распознает формат файла .session; "
            "исходные сессии и строки после обработки удаляются из чата.",
            account_import_sources_menu(source_page),
            answer=False,
        )
        return
    if kind not in {"archive", "tdata", "session", "telethon", "pyrogram", "json", "string"}:
        await callback.answer("Неизвестный тип импорта", show_alert=True)
        return
    if settings.telegram_api is None:
        await callback.answer(
            "Сначала задайте PROXIMA_TELEGRAM_API_ID и PROXIMA_TELEGRAM_API_HASH. "
            "Они нужны для отдельной безопасной сессии сервиса.",
            show_alert=True,
        )
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(import_kind=kind, account_import_page=source_page)
    await state.set_state(AccountImportState.payload)
    prompts = {
        "archive": (
            "📦 Пакетный импорт аккаунтов",
            "Прикрепите <code>.zip</code> с файлами <code>.session</code> Telethon или "
            "Pyrogram. Парный <code>.json</code> с тем же именем будет использован как "
            "резервный источник и для пароля 2FA, если он указан в экспорте.\n\n"
            "Пример: <code>7085388912.session</code> + <code>7085388912.json</code>. "
            "Поддерживается до <b>100 аккаунтов</b>, ZIP до <b>25 МБ</b>.\n\n"
            "Аккаунты обрабатываются по одному. Для каждого берётся любой исправный прокси "
            "из существующего пула; один прокси может использоваться несколькими аккаунтами. "
            "Ошибка одного файла не остановит остальные.\n\n"
            "🧹 После обработки архив будет удалён из чата.",
        ),
        "tdata": (
            "📥 Импорт tdata",
            "Прикрепите <code>.zip</code>-архив папки <code>tdata</code> из Telegram Desktop.\n\n"
            "Внутри должен быть файл <code>key_datas</code> и папка аккаунта. "
            "Лимит: <b>25 МБ</b>.\n\n"
            "Если это свежескачанный архив и вы его нигде не открывали — закрывать "
            "ничего не нужно. Telegram Desktop нужно закрыть только тогда, когда именно "
            "этот tdata сейчас открыт в нём.\n\n"
            "Сервис создаст новый отдельный ключ через рабочий прокси; исходный ключ "
            "сохраняться не будет.",
        ),
        "session": (
            "🗂 Импорт .session",
            "Прикрепите <code>.session</code> Telethon или Pyrogram. Формат определится "
            "автоматически. Если скачанный файл нигде не запущен — ничего закрывать не нужно.\n\n"
            "🧹 После обработки файл будет удалён из чата.",
        ),
        "telethon": (
            "🗂 Импорт .session Telethon",
            "Прикрепите файл Telethon <code>.session</code>. Если вы только скачали его и "
            "нигде не запускали — ничего закрывать не нужно. Закройте другой клиент только "
            "если он прямо сейчас использует эту же сессию.\n\n"
            "🧹 После обработки файл будет удалён из чата.",
        ),
        "pyrogram": (
            "🗂 Импорт .session Pyrogram",
            "Прикрепите файл Pyrogram <code>.session</code>. Бот распознает его структуру "
            "автоматически. Если файл нигде не запущен — ничего закрывать не нужно.\n\n"
            "🧹 После обработки файл будет удалён из чата.",
        ),
        "json": (
            "📄 Импорт .json",
            "Прикрепите <code>.json</code> с session string либо с полями "
            "<code>dc_id</code> и <code>auth_key</code>. Если продавец выдаёт JSON только с "
            "телефоном и данными профиля, авторизоваться по нему нельзя — бот сообщит об этом.\n\n"
            "🧹 После обработки файл будет удалён из чата.",
        ),
        "string": (
            "🔑 Импорт строки сессии",
            "Отправьте строку сессии Telethon или Pyrogram одним сообщением. Если она "
            "нигде не используется прямо сейчас — ничего останавливать не нужно.\n\n"
            "Сервис создаст отдельный зашифрованный <code>auth_key</code>, а исходное "
            "сообщение удалит из чата.",
        ),
    }
    title, body = prompts[kind]
    naming_note = (
        "Названия аккаунтов в боте будут взяты из их текущих имён профиля Telegram. "
        "Позже их можно изменить отдельно, не меняя профили."
        if kind == "archive"
        else "Название в боте будет взято из текущего имени профиля Telegram. "
        "Позже его можно изменить отдельно, не меняя профиль."
    )
    await _edit_callback(
        callback,
        wizard_text(
            title,
            1,
            1,
            body + f"\n\n{naming_note}",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(AccountImportState.payload)
async def account_import_payload(
    message: Message,
    state: FSMContext,
    bot: Bot,
    account_service: AccountService,
    proxy_service: ProxyService,
    settings: Settings,
) -> None:
    data = await state.get_data()
    kind = str(data.get("import_kind"))
    temporary_proxy_key: str | None = None
    temporary_proxy_moved = False
    import_proxy_route = None
    account = None
    archive_result: AccountArchiveImportResult | None = None
    try:
        if kind == "archive":
            archive_result = await _import_account_archive_message(
                message,
                bot,
                account_service,
                proxy_service,
                settings,
                origin_data=data,
            )
        else:
            if settings.account_require_proxy:
                temporary_proxy_key = f"account-import:{uuid4().hex}"
                await proxy_service.allocate_proxy(
                    temporary_proxy_key,
                    purpose=f"безопасный импорт аккаунта · {kind[:40]}",
                )
                import_proxy_route = await proxy_service.require_route(temporary_proxy_key)
            if kind == "tdata":
                account = await _import_tdata_message(
                    message,
                    bot,
                    account_service,
                    proxy_route=import_proxy_route,
                )
            elif kind in {"session", "telethon", "pyrogram"}:
                account = await _import_session_message(
                    message,
                    bot,
                    account_service,
                    proxy_route=import_proxy_route,
                )
            elif kind == "json":
                account = await _import_json_message(
                    message,
                    bot,
                    account_service,
                    proxy_route=import_proxy_route,
                )
            elif kind == "string":
                account = await _import_string_message(
                    message,
                    account_service,
                    proxy_route=import_proxy_route,
                )
            else:  # pragma: no cover - guarded on entry
                await message.answer("Неизвестный тип импорта.")
                return
            if account is not None and temporary_proxy_key is not None:
                await proxy_service.move_assignment(
                    temporary_proxy_key,
                    f"account:{account.id}",
                    purpose=f"account #{account.id}",
                )
                temporary_proxy_moved = True
    except (AccountImportError, ConflictError, NotFoundError, ProxyInputError) as exc:
        await message.answer(f"❌ {escape(str(exc))}")
        return
    finally:
        try:
            if temporary_proxy_key is not None and not temporary_proxy_moved:
                await proxy_service.release_assignment_key(temporary_proxy_key)
        finally:
            await _delete_sensitive_import_message(message)
    if archive_result is not None:
        await state.clear()
        await _edit_origin_data(
            bot,
            data,
            _account_archive_result_text(archive_result),
            account_import_result_menu(int(data.get("account_import_page", 0))),
        )
        return
    if account is None:
        return
    proxy_note = ""
    if temporary_proxy_moved:
        proxy_note = "🌐 Рабочий прокси назначен автоматически.\n"
    elif settings.account_require_proxy:
        try:
            await proxy_service.allocate_proxy(
                f"account:{account.id}", purpose=f"account #{account.id}"
            )
            proxy_note = "🌐 Рабочий прокси назначен автоматически.\n"
        except (ConflictError, ProxyInputError):
            proxy_note = "⚠️ Рабочий прокси не найден; назначьте его из карточки.\n"
    check = await account_service.check_account(account.id)
    account = await account_service.get_account(account.id)
    proxy_slot = await proxy_service.account_proxy_endpoint(account.id)
    proxy_view = await proxy_service.proxy_for_assignment(f"account:{account.id}")
    shared_proxy_accounts = (
        sum(key.startswith("account:") for key in proxy_view.assignment_keys)
        if proxy_view is not None
        else 0
    )
    await state.clear()
    if check.authorized:
        banner = f"✅ <b>Аккаунт загружен и сессия подтверждена.</b>\n{proxy_note}\n"
    else:
        note = f"\n<code>{escape(check.error)}</code>" if check.error else ""
        banner = (
            "🟡 <b>Аккаунт загружен, но сессию подтвердить не удалось.</b>\n"
            f"{proxy_note}"
            "После настройки прокси повторите проверку из карточки аккаунта."
            f"{note}\n\n"
        )
    await _edit_origin_data(
        bot,
        data,
        banner
        + account_details_text(
            account,
            proxy_slot=proxy_slot,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
        account_details_menu(
            account,
            int(data.get("account_import_page", 0)),
            has_proxy=proxy_slot is not None,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
    )


async def _import_account_archive_message(
    message: Message,
    bot: Bot,
    account_service: AccountService,
    proxy_service: ProxyService,
    settings: Settings,
    *,
    origin_data: dict[str, object],
) -> AccountArchiveImportResult | None:
    document = message.document
    if document is None or not (document.file_name or "").lower().endswith(".zip"):
        await message.answer("Нужен <code>.zip</code>-архив с файлами .session/.json.")
        return None
    if document.file_size is not None and document.file_size > 25 * 1024 * 1024:
        await message.answer("Архив аккаунтов больше 25 МБ.")
        return None

    buffer = BytesIO()
    await bot.download(document, destination=buffer)
    entries = await asyncio.to_thread(
        account_service.read_account_archive,
        buffer.getvalue(),
    )
    imported: list[Account] = []
    failures: list[AccountArchiveFailure] = []
    warnings: list[AccountArchiveWarning] = []
    total = len(entries)

    for index, entry in enumerate(entries, start=1):
        with suppress(TelegramAPIError):
            await _edit_origin_data(
                bot,
                origin_data,
                wizard_text(
                    "📦 Пакетный импорт",
                    index,
                    total,
                    "Обрабатывается "
                    f"<code>{escape(_compact_archive_name(entry.preferred_file_name))}</code>.\n\n"
                    f"✅ Загружено: <b>{len(imported)}</b>\n"
                    f"⚠️ Пропущено: <b>{len(failures)}</b>\n\n"
                    "Не закрывайте бот до появления итогового отчёта.",
                ),
                None,
            )
        temporary_key: str | None = None
        temporary_moved = False
        account: Account | None = None
        proxy_error: str | None = None
        entry_error: str | None = None
        try:
            proxy_route = None
            if settings.account_require_proxy:
                temporary_key = f"account-import:{uuid4().hex}"
                await proxy_service.allocate_proxy(
                    temporary_key,
                    purpose=f"пакетный импорт · {entry.source_name[:80]}",
                )
                proxy_route = await proxy_service.require_route(temporary_key)
            account = await account_service.import_archive_entry(
                entry,
                proxy_route=proxy_route,
            )
            if temporary_key is not None:
                try:
                    await proxy_service.move_assignment(
                        temporary_key,
                        f"account:{account.id}",
                        purpose=f"account #{account.id}",
                    )
                    temporary_moved = True
                except (ConflictError, NotFoundError, ProxyInputError) as exc:
                    proxy_error = _compact_archive_error(exc)
        except (AccountImportError, ConflictError, NotFoundError, ProxyInputError) as exc:
            entry_error = _compact_archive_error(exc)
        except Exception:  # noqa: BLE001 - one broken entry must not abort the archive
            logger.exception("unexpected account archive import failure for %s", entry.source_name)
            entry_error = "внутренняя ошибка импорта; подробности записаны в журнал"
        finally:
            if temporary_key is not None and not temporary_moved:
                with suppress(ConflictError, NotFoundError, ProxyInputError):
                    await proxy_service.release_assignment_key(temporary_key)

        if account is None:
            failures.append(
                AccountArchiveFailure(
                    source_name=entry.preferred_file_name,
                    error=entry_error or "аккаунт не был импортирован",
                )
            )
            continue

        if settings.account_require_proxy and not temporary_moved:
            try:
                await proxy_service.allocate_proxy(
                    f"account:{account.id}",
                    purpose=f"account #{account.id}",
                )
                proxy_error = None
            except (ConflictError, NotFoundError, ProxyInputError) as exc:
                proxy_error = _compact_archive_error(exc)
        imported.append(account)
        if proxy_error:
            warnings.append(
                AccountArchiveWarning(
                    source_name=entry.preferred_file_name,
                    warning=f"аккаунт импортирован, но прокси не назначен: {proxy_error}",
                )
            )

    return AccountArchiveImportResult(
        total=total,
        imported=tuple(imported),
        failures=tuple(failures),
        warnings=tuple(warnings),
    )


def _compact_archive_error(error: Exception, *, limit: int = 240) -> str:
    normalized = " ".join(str(error).split()) or type(error).__name__
    return normalized if len(normalized) <= limit else f"{normalized[: limit - 1].rstrip()}…"


def _compact_archive_name(value: str, *, limit: int = 120) -> str:
    normalized = " ".join(value.split()) or "без имени"
    return normalized if len(normalized) <= limit else f"…{normalized[-(limit - 1) :]}"


def _account_archive_result_text(result: AccountArchiveImportResult) -> str:
    lines = [
        "<b>📦 Пакетный импорт завершён</b>",
        f"<code>АККАУНТОВ В АРХИВЕ · {result.total}</code>\n",
        f"✅ Импортировано: <b>{len(result.imported)}</b>",
        f"⏭ Пропущено с ошибкой: <b>{len(result.failures)}</b>",
    ]
    if result.warnings:
        lines.append(f"⚠️ Требуют внимания: <b>{len(result.warnings)}</b>")

    def append_line(line: str, *, limit: int = 3400) -> bool:
        if sum(len(existing) + 1 for existing in lines) + len(line) > limit:
            return False
        lines.append(line)
        return True

    if result.imported:
        append_line("\n<b>Добавленные аккаунты</b>")
        shown = 0
        for account in result.imported:
            label = escape(_compact_archive_name(account.label, limit=80))
            if not append_line(f"• ✅ {label} · <code>#{account.id}</code>"):
                break
            shown += 1
        if shown < len(result.imported):
            append_line(f"• …и ещё {len(result.imported) - shown}")
    if result.failures:
        append_line("\n<b>Не удалось импортировать</b>")
        shown = 0
        for failure in result.failures:
            name = escape(_compact_archive_name(failure.source_name))
            if not append_line(f"• <code>{name}</code> — {escape(failure.error)}"):
                break
            shown += 1
        if shown < len(result.failures):
            append_line(f"• …и ещё {len(result.failures) - shown}")
    if result.warnings:
        append_line("\n<b>Предупреждения</b>")
        shown = 0
        for warning in result.warnings:
            name = escape(_compact_archive_name(warning.source_name))
            if not append_line(f"• <code>{name}</code> — {escape(warning.warning)}"):
                break
            shown += 1
        if shown < len(result.warnings):
            append_line(f"• …и ещё {len(result.warnings) - shown}")
    lines.append(
        "\n<i>Архив удалён из чата. Исходные сессии не сохраняются; "
        "для каждого аккаунта создана отдельная зашифрованная сессия Proxima.</i>"
    )
    return "\n".join(lines)


async def _delete_sensitive_import_message(message: Message) -> None:
    """Best-effort removal of uploaded account credentials from the bot chat."""
    with suppress(TelegramAPIError):
        await message.delete()


async def _import_tdata_message(
    message: Message,
    bot: Bot,
    account_service: AccountService,
    *,
    proxy_route: ProxyNetworkRoute | None = None,
):
    document = message.document
    if document is None or not (document.file_name or "").lower().endswith(".zip"):
        await message.answer("Нужен <code>.zip</code>-архив папки tdata.")
        return None
    if document.file_size is not None and document.file_size > 25 * 1024 * 1024:
        await message.answer("Архив больше 25 МБ.")
        return None
    buffer = BytesIO()
    await bot.download(document, destination=buffer)
    return await account_service.import_tdata(
        archive_bytes=buffer.getvalue(),
        proxy_route=proxy_route,
    )


async def _import_session_message(
    message: Message,
    bot: Bot,
    account_service: AccountService,
    *,
    proxy_route: ProxyNetworkRoute | None = None,
):
    document = message.document
    if document is None or not (document.file_name or "").lower().endswith(".session"):
        await message.answer("Нужен файл Telethon или Pyrogram <code>.session</code>.")
        return None
    if document.file_size is not None and document.file_size > 5 * 1024 * 1024:
        await message.answer("Файл .session больше 5 МБ.")
        return None
    buffer = BytesIO()
    await bot.download(document, destination=buffer)
    return await account_service.import_session_file(
        session_bytes=buffer.getvalue(),
        proxy_route=proxy_route,
    )


async def _import_string_message(
    message: Message,
    account_service: AccountService,
    *,
    proxy_route: ProxyNetworkRoute | None = None,
):
    value = (message.text or "").strip()
    if not value:
        await message.answer("Отправьте строку StringSession текстом.")
        return None
    return await account_service.import_session_string(
        session_string=value,
        proxy_route=proxy_route,
    )


async def _import_json_message(
    message: Message,
    bot: Bot,
    account_service: AccountService,
    *,
    proxy_route: ProxyNetworkRoute | None = None,
):
    document = message.document
    if document is None or not (document.file_name or "").lower().endswith(".json"):
        await message.answer("Нужен файл <code>.json</code> с данными авторизации.")
        return None
    if document.file_size is not None and document.file_size > 5 * 1024 * 1024:
        await message.answer("Файл .json больше 5 МБ.")
        return None
    buffer = BytesIO()
    await bot.download(document, destination=buffer)
    return await account_service.import_json(
        json_bytes=buffer.getvalue(),
        proxy_route=proxy_route,
    )


@router.callback_query(F.data == "target:add")
async def begin_target_create(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(TargetCreateState.chat_ref)
    await _edit_callback(
        callback,
        wizard_text(
            "🎯 Новый чат",
            1,
            2,
            "Введите @username, ссылку или числовой chat ID чата.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.callback_query(F.data == "target:import")
async def begin_target_import(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(TargetImportState.name)
    await _edit_callback(
        callback,
        wizard_text(
            "📚 Новый список чатов",
            1,
            2,
            "Введите название списка, например <code>Чаты на сутки</code>. "
            "На следующем шаге можно прикрепить TXT или вставить список сообщением.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(TargetImportState.name)
async def target_list_name(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 150:
        await message.answer("Название должно содержать от 1 до 150 символов.")
        return
    await state.update_data(target_list_name=value)
    await state.set_state(TargetImportState.document)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "📚 Новый список чатов",
            2,
            2,
            "Прикрепите UTF-8 <code>.txt</code> или вставьте список сообщением: "
            "одна ссылка, @username или chat ID на строку.\n\n"
            "Поддерживаются публичные ссылки, приватные invite-ссылки и ссылки на папки. "
            "Лимит: <b>500 строк · 1 МБ</b>.",
            hint="Ссылки нормализуются, дубликаты и ошибки попадут в отчёт.",
        ),
        cancel_menu(),
    )


@router.message(TargetImportState.document)
async def import_target_document(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    document = message.document
    if document is not None:
        if not (document.file_name or "").lower().endswith(".txt"):
            await message.answer("Нужен UTF-8 документ <code>.txt</code> или текстовый список.")
            return
        if document.file_size is not None and document.file_size > 1_048_576:
            await message.answer("Файл больше 1 МБ.")
            return
        buffer = BytesIO()
        await bot.download(document, destination=buffer)
        try:
            content = buffer.getvalue().decode("utf-8-sig")
        except UnicodeDecodeError:
            await message.answer("Файл должен быть сохранён в UTF-8.")
            return
    elif message.text:
        content = message.text
    else:
        await message.answer("Прикрепите TXT-файл или вставьте список обычным сообщением.")
        return
    lines = content.splitlines()
    if len(lines) > 500:
        await message.answer("За один импорт разрешено не более 500 строк.")
        return
    parsed = parse_target_batch(lines)
    if not parsed.targets and not parsed.folders:
        preview = "\n".join(f"строка {issue.line}: {issue.reason}" for issue in parsed.issues[:5])
        await message.answer(
            "Не найдено корректных Telegram-чатов."
            + (f"\n\n<code>{escape(preview)}</code>" if preview else "")
        )
        return
    if len(parsed.folders) > 10:
        await message.answer("За один импорт разрешено не более 10 ссылок на папки Telegram.")
        return
    issues = [f"строка {issue.line}: {issue.reason}" for issue in parsed.issues]
    await state.update_data(
        target_direct_refs=parsed.canonical_targets,
        target_folder_slugs=[folder.slug for folder in parsed.folders if folder.slug],
        target_import_duplicates=parsed.duplicates,
        target_import_issues=issues,
    )
    if parsed.folders:
        accounts = await repository.active_owned_accounts(require_authorized_session=True)
        if not accounts:
            await message.answer(
                "Для чтения папки <code>t.me/addlist/...</code> нужен хотя бы один "
                "активный аккаунт с проверенной Telegram-сессией."
            )
            return
        await state.set_state(TargetImportState.folder_account)
        await _edit_origin(
            bot,
            state,
            _target_folder_account_picker_text(len(parsed.folders), len(accounts), page=0),
            target_folder_account_picker(accounts, page=0, page_size=PAGE_SIZE),
        )
        return
    try:
        await _finish_target_import(
            bot=bot,
            state=state,
            repository=repository,
            folder_entries=[],
            folder_stats={},
            folder_errors=[],
        )
    except ConflictError as exc:
        await message.answer(str(exc))


def _target_folder_account_picker_text(folder_count: int, account_count: int, *, page: int) -> str:
    pages = max(1, (account_count + PAGE_SIZE - 1) // PAGE_SIZE)
    active_page = min(max(0, page), pages - 1)
    return wizard_text(
        "📚 Импорт папки Telegram",
        3,
        3,
        f"Найдено ссылок на папки: <b>{folder_count}</b>.\n"
        f"Доступно проверенных аккаунтов: <b>{account_count}</b> · "
        f"страница <b>{active_page + 1}/{pages}</b>.\n\n"
        "Выберите аккаунт, через который прочитать состав. На этом шаге бот "
        "никуда не вступает; аккаунт нужен только для MTProto-предпросмотра.",
    )


@router.callback_query(
    TargetImportState.folder_account,
    F.data.regexp(r"^target:folder:accounts:page:\d+$"),
)
async def page_target_folder_accounts(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    requested_page = int((callback.data or "").rsplit(":", 1)[1])
    accounts = await repository.active_owned_accounts(require_authorized_session=True)
    if not accounts:
        await callback.answer("Нет доступных аккаунтов. Импорт отменён.", show_alert=True)
        return
    page = min(max(0, requested_page), max(0, (len(accounts) - 1) // PAGE_SIZE))
    data = await state.get_data()
    folder_count = len([value for value in data.get("target_folder_slugs", []) if value])
    await _edit_callback(
        callback,
        _target_folder_account_picker_text(folder_count, len(accounts), page=page),
        target_folder_account_picker(accounts, page=page, page_size=PAGE_SIZE),
        answer=False,
    )


@router.callback_query(
    TargetImportState.folder_account,
    F.data.startswith("target:folder:account:"),
)
async def import_target_folders(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    account_service: AccountService,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    slugs = [str(value) for value in data.get("target_folder_slugs", [])][:10]
    await callback.answer("Читаю папки через Telegram…")
    entries: dict[str, TargetSeed] = {}
    folder_errors: list[str] = []
    stats = {
        "folders": len(slugs),
        "groups": 0,
        "channels": 0,
        "restricted": 0,
        "inaccessible": 0,
    }
    for slug in slugs:
        try:
            preview = await account_service.inspect_chat_folder(account_id, slug=slug)
        except (NotFoundError, AccountImportError, ChatFolderError) as exc:
            folder_errors.append(f"{slug[:10]}…: {str(exc)}")
            continue
        stats["channels"] += preview.skipped_channels
        stats["restricted"] += preview.restricted_groups
        stats["inaccessible"] += preview.inaccessible
        for item in preview.importable_groups:
            entries[item.chat_ref.casefold()] = TargetSeed(
                chat_ref=item.chat_ref,
                title=item.title,
                chat_type=item.chat_type,
                folder_slug=preview.slug,
                telegram_peer_id=item.peer_id,
            )
    stats["groups"] = len(entries)
    try:
        await _finish_target_import(
            bot=bot,
            state=state,
            repository=repository,
            folder_entries=list(entries.values()),
            folder_stats=stats,
            folder_errors=folder_errors,
        )
    except ConflictError as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"❌ {escape(str(exc))}")


async def _finish_target_import(
    *,
    bot: Bot,
    state: FSMContext,
    repository: AdminRepository,
    folder_entries: list[TargetSeed],
    folder_stats: dict[str, int],
    folder_errors: list[str],
) -> None:
    data = await state.get_data()
    direct_refs = [str(value) for value in data.get("target_direct_refs", [])]
    if not direct_refs and not folder_entries:
        detail = folder_errors[0] if folder_errors else "в папках нет доступных групп"
        raise ConflictError(detail)
    target_list, added, reused = await repository.create_target_list(
        name=str(data["target_list_name"]),
        chat_refs=direct_refs,
        entries=folder_entries,
    )
    await state.clear()
    page = await repository.list_target_lists(0, PAGE_SIZE)
    issues = [str(value) for value in data.get("target_import_issues", [])]
    issues_preview = "\n".join(issues[:5])
    report = (
        f"✅ <b>Умный импорт завершён</b>\n\n"
        f"Добавлено: <b>{added}</b>\n"
        f"Уже существовало: <b>{reused}</b>\n"
        f"Дубликатов в списке: <b>{int(data.get('target_import_duplicates', 0))}</b>\n"
        f"Ошибок строк: <b>{len(issues)}</b>"
    )
    if folder_stats:
        report += (
            "\n\n<b>Папки Telegram</b>\n"
            f"Групп добавлено: <b>{folder_stats.get('groups', 0)}</b>\n"
            f"Каналов пропущено (импортируются только группы): "
            f"<b>{folder_stats.get('channels', 0)}</b>\n"
            f"Групп с запретом сообщений пропущено: "
            f"<b>{folder_stats.get('restricted', 0)}</b>\n"
            f"Недоступных чатов: <b>{folder_stats.get('inaccessible', 0)}</b>"
        )
    if folder_errors:
        report += (
            "\n\n<b>Ошибки папок</b>\n<code>" + escape("\n".join(folder_errors[:5])) + "</code>"
        )
    if issues_preview:
        report += f"\n\n<b>Первые ошибки</b>\n<code>{escape(issues_preview)}</code>"
    report += (
        f"\n\nБаза: <b>{escape(target_list.name)}</b>\n"
        "Чаты пока выключены. Откройте карточку списка и нажмите "
        "«Использовать все чаты»."
    )
    await _edit_origin_data(
        bot,
        data,
        report + "\n\n" + target_lists_text(page),
        target_lists_menu(page),
    )


@router.message(TargetCreateState.chat_ref)
async def target_chat_ref(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    try:
        parsed = parse_target_reference(value)
    except TargetReferenceError as exc:
        await message.answer(f"Не удалось распознать чат: {escape(str(exc))}")
        return
    if parsed.kind == TargetReferenceKind.CHAT_FOLDER:
        await message.answer(
            "Это ссылка на папку. Используйте «📄 Импорт .txt» — там бот покажет "
            "отдельный отчёт по папкам."
        )
        return
    await state.update_data(chat_ref=parsed.canonical)
    await state.set_state(TargetCreateState.title)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🎯 Новый чат",
            2,
            2,
            "Введите понятное название. Чат останется закрытым, пока вы отдельно не откроете его.",
        ),
        cancel_menu(),
    )


@router.message(TargetCreateState.title)
async def target_title(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 150:
        await message.answer("Название должно содержать от 1 до 150 символов.")
        return
    data = await state.get_data()
    try:
        target = await repository.create_target(chat_ref=str(data["chat_ref"]), title=value)
    except ConflictError as exc:
        await message.answer(str(exc))
        return
    await state.clear()
    page = await repository.list_targets(0, PAGE_SIZE)
    await _edit_origin_data(
        bot,
        data,
        f"✅ Чат <b>{escape(target.title)}</b> добавлен и пока закрыт.\n\n{targets_text(page)}",
        targets_menu(page),
    )


@router.callback_query(F.data.startswith("target:approve:") | F.data.startswith("target:revoke:"))
async def change_target_approval(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, action, target_id, page = (callback.data or "").split(":")
    try:
        target = await repository.set_target_approval(int(target_id), action == "approve")
        timings = await repository.target_timings(target.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Чат включён" if target.approved_by_operator else "Чат выключен")
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        target_details_menu(target, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("targets:bulk:"))
async def change_all_standalone_targets_approval(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, action, page = (callback.data or "").split(":")
    approved = action == "approve"
    changed, skipped_bad = await repository.set_standalone_targets_approval(approved)
    if approved:
        message = f"Включено чатов: {changed}"
        if skipped_bad:
            message += f" · плохих оставил выключенными: {skipped_bad}"
    else:
        message = f"Выключено чатов: {changed}"
    await callback.answer(message, show_alert=True)
    await _show_targets(callback, repository, int(page))


@router.callback_query(F.data.startswith("target:bad:details:"))
async def bad_target_details(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_id, page = (callback.data or "").split(":")
    try:
        target = await repository.get_target(int(target_id))
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        bad_target_details_menu(target, int(page)),
    )


@router.callback_query(F.data.startswith("target:bad:"))
async def bad_targets(callback: CallbackQuery, repository: AdminRepository) -> None:
    page = int((callback.data or "").rsplit(":", 1)[1])
    await _show_bad_targets(callback, repository, page, answer=True)


@router.callback_query(F.data.startswith("target:quality:restore:"))
async def restore_target_quality(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_id, page, origin = (callback.data or "").split(":")
    try:
        target = await repository.restore_target_quality(int(target_id))
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Чат возвращён в работу; рейтинг и баны сброшены", show_alert=True)
    if origin == "bad":
        await _show_bad_targets(callback, repository, int(page), answer=False)
        return
    await _edit_callback(
        callback,
        "✅ <b>Чат возвращён в работу.</b> Статистика качества начата заново.\n\n"
        + target_details_text(target, timings),
        target_details_menu(target, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("target:details:"))
async def target_details(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, target_id, page = (callback.data or "").split(":")
    try:
        target = await repository.get_target(int(target_id))
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        target_details_menu(target, int(page)),
    )


@router.callback_query(F.data.startswith("target:unblock:"))
async def unblock_target_accounts(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, target_id, page = (callback.data or "").split(":")
    try:
        count = await repository.unblock_target_accounts(int(target_id))
        target = await repository.get_target(int(target_id))
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    message = f"Сброшено блокировок: {count}" if count else "Запомненных блокировок нет"
    await callback.answer(message, show_alert=bool(count))
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        target_details_menu(target, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("target:cooldown:"))
async def begin_target_cooldown_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, target_id, page = (callback.data or "").split(":")
    try:
        target = await repository.get_target(int(target_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(cooldown_target_id=target.id, cooldown_target_page=int(page))
    await state.set_state(TargetCooldownState.minutes)
    current = target.manual_cooldown_seconds // 60
    await _edit_callback(
        callback,
        wizard_text(
            f"🕒 Интервал · {target.title}",
            1,
            1,
            f"Текущий интервал: <b>{current} мин.</b>\n\n"
            "Введите минимальное число минут между сообщениями одного аккаунта в этот чат: "
            "от <b>1</b> до <b>10080</b>. Введите <b>0</b>, чтобы учитывать только "
            "таймер Telegram и общие лимиты аккаунта.",
        ),
        target_cooldown_picker(),
        answer=False,
    )


@router.callback_query(F.data.startswith("tlcd:"))
async def begin_target_list_cooldown_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    _, target_id, target_list_id, list_page, chat_page = parts[:5]
    filter_name = parts[5] if len(parts) > 5 else "all"
    try:
        target = await repository.get_target_list_target(int(target_list_id), int(target_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(
        cooldown_target_id=target.id,
        cooldown_target_page=0,
        cooldown_target_list_id=int(target_list_id),
        cooldown_target_list_page=int(list_page),
        cooldown_target_chat_page=int(chat_page),
        cooldown_target_list_filter=filter_name,
    )
    await state.set_state(TargetCooldownState.minutes)
    current = target.manual_cooldown_seconds // 60
    await _edit_callback(
        callback,
        wizard_text(
            f"🕒 Интервал · {target.title}",
            1,
            1,
            f"Текущий интервал: <b>{current} мин.</b>\n\n"
            "Введите минимальное число минут между сообщениями одного аккаунта в этот чат: "
            "от <b>1</b> до <b>10080</b>. Введите <b>0</b>, чтобы учитывать только "
            "таймер Telegram и общие лимиты аккаунта.",
        ),
        target_cooldown_picker(),
        answer=False,
    )


@router.callback_query(
    TargetCooldownState.minutes,
    F.data.startswith("tcooldown:set:"),
)
async def save_target_cooldown_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    minutes = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    target_list_id = data.get("cooldown_target_list_id")
    try:
        target = await repository.set_target_manual_cooldown(
            int(data["cooldown_target_id"]), minutes * 60
        )
        if target_list_id is not None:
            target = await repository.get_target_list_target(
                int(target_list_id),
                target.id,
            )
        timings = await repository.target_timings(target.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    target_list_id = data.get("cooldown_target_list_id")
    if target_list_id is not None:
        menu = target_list_target_details_menu(
            target,
            int(target_list_id),
            int(data["cooldown_target_list_page"]),
            int(data["cooldown_target_chat_page"]),
            str(data.get("cooldown_target_list_filter") or "all"),
        )
    else:
        menu = target_details_menu(target, int(data.get("cooldown_target_page", 0)))
    await state.clear()
    await callback.answer("Интервал чата обновлён")
    await _edit_callback(
        callback,
        "✅ Интервал чата обновлён.\n\n" + target_details_text(target, timings),
        menu,
        answer=False,
    )


@router.message(TargetCooldownState.minutes)
async def save_target_cooldown(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        minutes = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число минут от 0 до 10080.")
        return
    if not 0 <= minutes <= 10080:
        await message.answer("Интервал должен быть от 0 до 10080 минут.")
        return
    data = await state.get_data()
    target_list_id = data.get("cooldown_target_list_id")
    try:
        target = await repository.set_target_manual_cooldown(
            int(data["cooldown_target_id"]), minutes * 60
        )
        if target_list_id is not None:
            target = await repository.get_target_list_target(
                int(target_list_id),
                target.id,
            )
        timings = await repository.target_timings(target.id)
    except (NotFoundError, ConflictError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    if target_list_id is not None:
        await _edit_origin_data(
            bot,
            data,
            "✅ Интервал чата обновлён.\n\n" + target_details_text(target, timings),
            target_list_target_details_menu(
                target,
                int(target_list_id),
                int(data["cooldown_target_list_page"]),
                int(data["cooldown_target_chat_page"]),
                str(data.get("cooldown_target_list_filter") or "all"),
            ),
        )
        return
    await _edit_origin_data(
        bot,
        data,
        "✅ Интервал чата обновлён.\n\n" + target_details_text(target, timings),
        target_details_menu(target, int(data["cooldown_target_page"])),
    )


@router.callback_query(F.data == "tcooldown:cancel")
async def cancel_target_cooldown_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    target_id = data.get("cooldown_target_id")
    target_list_id = data.get("cooldown_target_list_id")
    await state.clear()
    if target_id is None:
        await callback.answer("Изменения отменены")
        return
    try:
        target = (
            await repository.get_target_list_target(
                int(target_list_id),
                int(target_id),
            )
            if target_list_id is not None
            else await repository.get_target(int(target_id))
        )
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if target_list_id is not None:
        menu = target_list_target_details_menu(
            target,
            int(target_list_id),
            int(data["cooldown_target_list_page"]),
            int(data["cooldown_target_chat_page"]),
            str(data.get("cooldown_target_list_filter") or "all"),
        )
    else:
        menu = target_details_menu(target, int(data.get("cooldown_target_page", 0)))
    await callback.answer("Изменения отменены")
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        menu,
        answer=False,
    )


@router.callback_query(F.data.startswith("target:delete:ask:"))
async def ask_delete_target(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_id, page = (callback.data or "").split(":")
    try:
        target = await repository.get_target(int(target_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить чат «{escape(target.title)}»?</b>\n\n"
        "Удаление возможно только если чат не входит в список и не используется "
        "сохранённой рассылкой или диалогом.",
        target_delete_confirmation(target.id, int(page)),
    )


@router.callback_query(F.data.startswith("target:delete:confirm:"))
async def confirm_delete_target(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_id, page = (callback.data or "").split(":")
    try:
        await repository.delete_target(int(target_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Чат удалён")
    await _show_targets(callback, repository, int(page))


@router.callback_query(F.data.startswith("targetlist:details:"))
async def target_list_details(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, target_list_id, page = (callback.data or "").split(":")
    try:
        item = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        target_list_details_text(item),
        target_list_details_menu(item, int(page)),
    )


@router.callback_query(F.data.startswith("targetlist:rename:"))
async def begin_target_list_rename(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, target_list_id, page = (callback.data or "").split(":")
    try:
        item = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await _remember_origin(callback, state)
    await state.update_data(target_list_rename_id=item.id, target_list_rename_page=int(page))
    await state.set_state(TargetListRenameState.name)
    await _edit_callback(
        callback,
        f"<b>✏️ Переименовать список «{escape(item.name)}»</b>\n\n"
        "Отправьте новое понятное название длиной до 150 символов. "
        "Состав списка и сохранённые рассылки не изменятся.",
        cancel_menu(),
    )


@router.message(TargetListRenameState.name)
async def save_target_list_name(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    name = " ".join((message.text or "").split())
    data = await state.get_data()
    try:
        item = await repository.rename_target_list(int(data["target_list_rename_id"]), name)
    except (NotFoundError, ConflictError) as exc:
        await message.answer(str(exc))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>База переименована.</b>\n\n" + target_list_details_text(item),
        target_list_details_menu(item, int(data["target_list_rename_page"])),
    )


@router.callback_query(F.data.startswith("tlv:"))
async def target_list_targets(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, target_list_id, list_page, chat_page = (callback.data or "").split(":")
    await _show_target_list_targets(
        callback,
        repository,
        int(target_list_id),
        int(list_page),
        int(chat_page),
        filter_name="all",
    )


@router.callback_query(F.data.startswith("tlf:"))
async def filter_target_list_targets(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, target_list_id, list_page, filter_name, chat_page = (callback.data or "").split(":")
    await _show_target_list_targets(
        callback,
        repository,
        int(target_list_id),
        int(list_page),
        int(chat_page),
        filter_name=filter_name,
    )


@router.callback_query(F.data.startswith("tlbulk:"))
async def bulk_change_target_list_page(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, action, target_list_id, list_page, filter_name, chat_page = (callback.data or "").split(":")
    try:
        page = await repository.list_target_list_targets(
            int(target_list_id),
            int(chat_page),
            TARGET_LIST_CHAT_PAGE_SIZE,
            filter_name=filter_name,
        )
        changed, skipped_bad = await repository.set_target_list_targets_approval(
            int(target_list_id),
            [target.id for target in page.items],
            action == "approve",
        )
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    message = f"Изменено чатов на странице: {changed}"
    if skipped_bad:
        message += f" · плохих пропущено: {skipped_bad}"
    await callback.answer(message, show_alert=True)
    await _show_target_list_targets(
        callback,
        repository,
        int(target_list_id),
        int(list_page),
        int(chat_page),
        filter_name=filter_name,
        answer=False,
    )


@router.callback_query(F.data.startswith("tlall:"))
async def bulk_change_all_target_list_chats(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, action, target_list_id, list_page, filter_name, chat_page = (callback.data or "").split(":")
    approved = action == "approve"
    try:
        changed = await repository.set_target_list_approval(int(target_list_id), approved)
        target_list = await repository.get_target_list_view(int(target_list_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if approved:
        message = f"Включено чатов: {changed}"
        if target_list.bad_count:
            message += f" · плохих оставил выключенными: {target_list.bad_count}"
    else:
        message = f"Выключено чатов: {changed}"
    await callback.answer(message, show_alert=True)
    await _show_target_list_targets(
        callback,
        repository,
        int(target_list_id),
        int(list_page),
        int(chat_page),
        filter_name=filter_name,
        answer=False,
    )


@router.callback_query(F.data.startswith("tld:"))
async def target_list_target_details(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    _, target_id, target_list_id, list_page, chat_page = parts[:5]
    filter_name = parts[5] if len(parts) > 5 else "all"
    try:
        target = await repository.get_target_list_target(int(target_list_id), int(target_id))
        timings = await repository.target_timings(target.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        target_list_target_details_menu(
            target,
            int(target_list_id),
            int(list_page),
            int(chat_page),
            filter_name,
        ),
    )


@router.callback_query(F.data.startswith("tlt:"))
async def change_target_list_target_approval(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    _, action, target_id, target_list_id, list_page, chat_page = parts[:6]
    filter_name = parts[6] if len(parts) > 6 else "all"
    try:
        target = await repository.set_target_list_target_approval(
            int(target_list_id),
            int(target_id),
            action == "approve",
        )
        timings = await repository.target_timings(target.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Чат включён" if target.approved_by_operator else "Чат выключен")
    await _edit_callback(
        callback,
        target_details_text(target, timings),
        target_list_target_details_menu(
            target,
            int(target_list_id),
            int(list_page),
            int(chat_page),
            filter_name,
        ),
        answer=False,
    )


@router.callback_query(F.data.startswith("tlra:"))
async def ask_remove_target_from_list(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    _, target_id, target_list_id, list_page, chat_page = parts[:5]
    filter_name = parts[5] if len(parts) > 5 else "all"
    try:
        target = await repository.get_target_list_target(int(target_list_id), int(target_id))
        target_list = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>➖ Убрать «{escape(target.title)}» из списка "
        f"«{escape(target_list.name)}»?</b>\n\n"
        "Чат исчезнет из этого списка. Если он больше не входит в другие списки, не выбран "
        "отдельной рассылкой и не нужен истории результатов, его техническая запись тоже "
        "будет удалена автоматически.",
        target_list_target_remove_confirmation(
            int(target_id),
            int(target_list_id),
            int(list_page),
            int(chat_page),
            filter_name,
        ),
    )


@router.callback_query(F.data.startswith("tlrc:"))
async def confirm_remove_target_from_list(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    _, target_id, target_list_id, list_page, chat_page = parts[:5]
    filter_name = parts[5] if len(parts) > 5 else "all"
    try:
        await repository.remove_target_from_list(int(target_list_id), int(target_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Чат убран из списка")
    await _show_target_list_targets(
        callback,
        repository,
        int(target_list_id),
        int(list_page),
        int(chat_page),
        filter_name=filter_name,
        answer=False,
    )


@router.callback_query(
    F.data.startswith("targetlist:approve:") | F.data.startswith("targetlist:revoke:")
)
async def change_target_list_approval(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, action, target_list_id, page = (callback.data or "").split(":")
    try:
        count = await repository.set_target_list_approval(int(target_list_id), action == "approve")
        item = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if action == "approve":
        message = f"Включено чатов: {count}"
        if item.bad_count:
            message += f" · плохих оставил выключенными: {item.bad_count}"
    else:
        message = f"Выключено чатов: {count}"
    await callback.answer(message)
    await _edit_callback(
        callback,
        target_list_details_text(item),
        target_list_details_menu(item, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("targetlist:delete:ask:"))
async def ask_delete_target_list(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_list_id, page = (callback.data or "").split(":")
    try:
        item = await repository.get_target_list_view(int(target_list_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить список «{escape(item.name)}»?</b>\n\n"
        "Список и его связи будут удалены. Чаты, которые существовали только внутри этого "
        "списка и не нужны истории отправок, также будут очищены. Вручную добавленные и "
        "общие для нескольких баз чаты сохранятся.\n\n"
        "Если список используется сохранённой рассылкой, бот остановит удаление и точно "
        "укажет причину.",
        target_list_delete_confirmation(item.id, int(page)),
    )


@router.callback_query(F.data.startswith("targetlist:delete:confirm:"))
async def confirm_delete_target_list(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, target_list_id, page = (callback.data or "").split(":")
    try:
        await repository.delete_target_list(int(target_list_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("База удалена; лишние импортированные чаты очищены")
    await _show_target_lists(callback, repository, int(page))


@router.callback_query(F.data == "template:add")
async def begin_template_create(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _remember_origin(callback, state)
    await state.set_state(TemplateCreateState.name)
    await _edit_callback(
        callback,
        wizard_text(
            "📝 Новый шаблон",
            1,
            3,
            "Введите название, например <code>Приветствие</code>.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(TemplateCreateState.name)
async def template_name(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 150:
        await message.answer("Название должно содержать от 1 до 150 символов.")
        return
    await state.update_data(name=value)
    await state.set_state(TemplateCreateState.primary_text)
    await _edit_origin(
        bot,
        state,
        wizard_text("📝 Новый шаблон", 2, 3, "Введите первое сообщение."),
        cancel_menu(),
    )


@router.message(TemplateCreateState.primary_text)
async def template_primary_text(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 4096:
        await message.answer("Текст должен содержать от 1 до 4096 символов.")
        return
    await state.update_data(primary_text=value)
    await state.set_state(TemplateCreateState.reply_text)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "📝 Новый шаблон",
            3,
            3,
            "Введите ответ для режима диалога или отправьте <code>-</code>, "
            "если этот текст нужен только для рассылки.",
        ),
        cancel_menu(),
    )


@router.message(TemplateCreateState.reply_text)
async def template_reply_text(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 4096:
        await message.answer("Отправьте ответ, символ <code>-</code> или текст до 4096 символов.")
        return
    await state.update_data(reply_text="" if value == "-" else value)
    await state.set_state(TemplateCreateState.confirm)
    await _edit_origin(
        bot,
        state,
        template_confirmation_text(await state.get_data()),
        template_confirmation(),
    )


@router.callback_query(TemplateCreateState.confirm, F.data == "template:confirm")
async def confirm_template(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    try:
        template = await repository.create_template(
            name=str(data["name"]),
            primary_text=str(data["primary_text"]),
            reply_text=str(data["reply_text"]),
        )
    except ConflictError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Текст сохранён")
    page = await repository.list_templates(0, PAGE_SIZE)
    await _edit_callback(
        callback,
        f"✅ Текст <b>{escape(template.name)}</b> сохранён.\n\n{templates_text(page)}",
        templates_menu(page),
        answer=False,
    )


@router.callback_query(F.data.startswith("template:toggle:"))
async def toggle_template(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, template_id, page = (callback.data or "").split(":")
    try:
        template = await repository.toggle_template(int(template_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Текст включён" if template.enabled else "Текст на паузе")
    await _edit_callback(
        callback,
        template_details_text(template),
        template_details_menu(template, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("template:details:"))
async def template_details(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, template_id, page = (callback.data or "").split(":")
    try:
        template = await repository.get_template(int(template_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        template_details_text(template),
        template_details_menu(template, int(page)),
    )


@router.callback_query(F.data.startswith("template:delete:ask:"))
async def ask_delete_template(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, template_id, page = (callback.data or "").split(":")
    try:
        template = await repository.get_template(int(template_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить шаблон «{escape(template.name)}»?</b>\n\n"
        "Уже сохранённые рассылки и диалоги не изменятся: в них хранится собственная копия.",
        template_delete_confirmation(template.id, int(page)),
    )


@router.callback_query(F.data.startswith("template:delete:confirm:"))
async def confirm_delete_template(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, template_id, page = (callback.data or "").split(":")
    try:
        await repository.delete_template(int(template_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Текст удалён")
    await _show_templates(callback, repository, int(page))


@router.callback_query(F.data.startswith("template:edit:") & ~F.data.endswith(":save"))
async def begin_template_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) != 4:
        await callback.answer("Некорректная команда", show_alert=True)
        return
    template_id, page = int(parts[2]), int(parts[3])
    try:
        template = await repository.get_template(template_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(
        edit_template_id=template.id,
        edit_template_page=page,
        edit_template_name=template.name,
    )
    await state.set_state(TemplateEditState.primary_text)
    await _edit_callback(
        callback,
        wizard_text(
            f"✏️ Редактирование · {template.name}",
            1,
            2,
            "Введите новое первое сообщение.",
        ),
        cancel_menu(),
        answer=False,
    )


@router.message(TemplateEditState.primary_text)
async def edit_template_primary(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 4096:
        await message.answer("Текст должен содержать от 1 до 4096 символов.")
        return
    await state.update_data(edit_primary_text=value)
    await state.set_state(TemplateEditState.reply_text)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "✏️ Редактирование текста",
            2,
            2,
            "Введите новый ответ для режима диалога или отправьте <code>-</code>, "
            "чтобы оставить только текст рассылки.",
        ),
        cancel_menu(),
    )


@router.message(TemplateEditState.reply_text)
async def edit_template_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 4096:
        await message.answer("Отправьте ответ, символ <code>-</code> или текст до 4096 символов.")
        return
    await state.update_data(edit_reply_text="" if value == "-" else value)
    await state.set_state(TemplateEditState.confirm)
    data = await state.get_data()
    await _edit_origin(
        bot,
        state,
        "<b>✅ Сохранить новые тексты?</b>\n\n"
        f"<b>1 · Первое сообщение</b>\n"
        f"<blockquote>{escape(str(data['edit_primary_text']))}</blockquote>\n"
        f"<b>2 · Адресный ответ</b>\n"
        f"<blockquote>{escape(str(data['edit_reply_text']))}</blockquote>",
        template_edit_confirmation(),
    )


@router.callback_query(TemplateEditState.confirm, F.data == "template:edit:save")
async def save_template_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    try:
        template = await repository.update_template_texts(
            int(data["edit_template_id"]),
            primary_text=str(data["edit_primary_text"]),
            reply_text=str(data["edit_reply_text"]),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    page = int(data["edit_template_page"])
    await state.clear()
    await callback.answer("Текст обновлён")
    await _edit_callback(
        callback,
        template_details_text(template),
        template_details_menu(template, page),
        answer=False,
    )


@router.callback_query(F.data == "campaign:add")
async def begin_campaign_create(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    await _begin_campaign_create(callback, state, repository, settings)


@router.callback_query(F.data.startswith("campaign:add:"))
async def begin_typed_campaign_create(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    mode = (callback.data or "").rsplit(":", 1)[1]
    if mode not in {"broadcast", "dialogue"}:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    await _begin_campaign_create(callback, state, repository, settings, mode=mode)


async def _begin_campaign_create(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    *,
    mode: str | None = None,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    targets = await repository.approved_targets()
    target_lists = await repository.available_target_lists()
    if not accounts:
        detail = (
            "Нужен минимум один активный аккаунт с проверенной сессией"
            if not settings.dry_run
            else "Нужен минимум один активный подтверждённый аккаунт"
        )
        await callback.answer(detail, show_alert=True)
        return
    if mode == "dialogue" and len(accounts) < 2:
        detail = (
            "Для диалога нужны минимум два активных аккаунта с проверенной сессией"
            if not settings.dry_run
            else "Для диалога нужны минимум два аккаунта"
        )
        await callback.answer(detail, show_alert=True)
        return
    if not targets and not target_lists:
        await callback.answer("Сначала откройте чат или список чатов", show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    if mode is not None:
        await state.update_data(mode=mode)
    await state.set_state(CampaignCreateState.name)
    title = (
        "📢 Новая рассылка"
        if mode == "broadcast"
        else "💬 Новый диалог"
        if mode == "dialogue"
        else "🚀 Новая рассылка"
    )
    body = (
        "Введите название для себя, например <code>Рассылка 24ч</code>. "
        "Потом выберем аккаунты, список чатов, текст и окно отправки."
        if mode == "broadcast"
        else (
            "Введите название для себя, например <code>Диалог тест</code>. "
            "Потом выберем два аккаунта, чат или список, тексты и задержку ответа."
        )
        if mode == "dialogue"
        else "Введите название, затем выберите: рассылка или диалог."
    )
    await _edit_callback(
        callback,
        wizard_text(title, 1, _campaign_total_steps(mode), body),
        cancel_menu(),
        answer=False,
    )


@router.message(CampaignCreateState.name)
async def campaign_name(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    value = (message.text or "").strip()
    if not value or len(value) > 150:
        await message.answer("Название должно содержать от 1 до 150 символов.")
        return
    await state.update_data(name=value)
    data = await state.get_data()
    mode = data.get("mode")
    if mode in {"broadcast", "dialogue"}:
        accounts = await repository.active_owned_accounts(
            require_authorized_session=not settings.dry_run
        )
        if mode == "broadcast":
            default_sender_ids = [account.id for account in accounts]
            await state.update_data(sender_account_ids=default_sender_ids)
            await state.set_state(CampaignCreateState.sender_accounts)
            await _edit_origin(
                bot,
                state,
                wizard_text(
                    "📢 Новая рассылка",
                    2,
                    7,
                    "По умолчанию отмечены все готовые аккаунты — это рабочий пул и "
                    "резерв автозамены. Снимите лишние. База распределится между ними "
                    "по кругу; можно отметить один аккаунт или сразу все. В умном режиме "
                    "автозамена возьмёт резерв только из этого выбранного пула.",
                ),
                campaign_sender_picker(accounts, set(default_sender_ids)),
            )
            return
        await state.set_state(CampaignCreateState.primary_account)
        title = _campaign_title(mode)
        await _edit_origin(
            bot,
            state,
            wizard_text(
                title,
                2,
                _campaign_total_steps(mode),
                "Выберите аккаунт для первого сообщения. Показываются только активные аккаунты.",
            ),
            account_picker(accounts, "primary"),
        )
        return
    await state.set_state(CampaignCreateState.mode)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🚀 Новая рассылка",
            2,
            7,
            "Выберите режим:\n"
            "• <b>Рассылка</b> — одно сообщение в каждый чат из списка\n"
            "• <b>Диалог</b> — пост одним аккаунтом + ответ другим с reply",
        ),
        campaign_mode_picker(),
    )


@router.callback_query(CampaignCreateState.mode, F.data.startswith("campaign:pick:mode:"))
async def campaign_mode(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    mode = (callback.data or "").rsplit(":", 1)[1]
    if mode not in {"broadcast", "dialogue"}:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    if mode == "dialogue" and len(accounts) < 2:
        detail = (
            "Для диалога нужно минимум два активных аккаунта с проверенной сессией"
            if not settings.dry_run
            else "Для диалога нужно минимум два аккаунта"
        )
        await callback.answer(detail, show_alert=True)
        return
    await state.update_data(mode=mode)
    if mode == "broadcast":
        default_sender_ids = [account.id for account in accounts]
        await state.update_data(sender_account_ids=default_sender_ids)
        await state.set_state(CampaignCreateState.sender_accounts)
        await _edit_callback(
            callback,
            wizard_text(
                "📢 Новая рассылка",
                2,
                7,
                "По умолчанию отмечены все готовые аккаунты — это рабочий пул и "
                "резерв автозамены. Снимите лишние. База распределится между ними "
                "по кругу; можно отметить один аккаунт или сразу все. В умном режиме "
                "автозамена возьмёт резерв только из этого выбранного пула.",
            ),
            campaign_sender_picker(accounts, set(default_sender_ids)),
        )
        return
    await state.set_state(CampaignCreateState.primary_account)
    await _edit_callback(
        callback,
        wizard_text(
            _campaign_title(mode),
            2,
            _campaign_total_steps(mode),
            "Выберите аккаунт для первого сообщения.",
        ),
        account_picker(accounts, "primary"),
        answer=False,
    )


@router.callback_query(
    CampaignCreateState.primary_account, F.data.startswith("campaign:pick:primary:")
)
async def campaign_primary(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    await callback.answer()
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    await state.update_data(primary_account_id=account_id)
    if data.get("mode") == "broadcast":
        await state.update_data(sender_account_ids=[account_id])
        await state.set_state(CampaignCreateState.sender_accounts)
        accounts = await repository.active_owned_accounts(
            require_authorized_session=not settings.dry_run
        )
        await _edit_callback(
            callback,
            wizard_text(
                "📢 Новая рассылка",
                2,
                7,
                "Отметьте аккаунты, которые будут отправлять. Первый уже выбран. "
                "Если выбрать несколько, чаты распределятся между ними по кругу.",
            ),
            campaign_sender_picker(accounts, {account_id}),
            answer=False,
        )
        return
    await state.set_state(CampaignCreateState.reply_account)
    accounts = [
        item
        for item in await repository.active_owned_accounts(
            require_authorized_session=not settings.dry_run
        )
        if item.id != account_id
    ]
    await _edit_callback(
        callback,
        wizard_text(
            "💬 Новый диалог",
            3,
            7,
            "Выберите второй аккаунт: он отправит ответ через reply.",
        ),
        account_picker(accounts, "reply"),
        answer=False,
    )


@router.callback_query(
    CampaignCreateState.sender_accounts, F.data.startswith("campaign:sender:toggle:")
)
async def campaign_sender_toggle(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    selected_ids = [int(item) for item in data.get("sender_account_ids", [])]
    if account_id in selected_ids:
        selected_ids = [item for item in selected_ids if item != account_id]
    else:
        selected_ids.append(account_id)
    await state.update_data(sender_account_ids=selected_ids)
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    await _edit_callback(
        callback,
        wizard_text(
            "📢 Новая рассылка",
            2,
            7,
            "Выберите аккаунты-отправители. При начале отправки чаты распределятся между "
            "ними по кругу. Автозамена умной рассылки не выйдет за пределы этого пула.",
        ),
        campaign_sender_picker(accounts, set(selected_ids)),
    )


@router.callback_query(
    CampaignCreateState.sender_accounts,
    F.data.in_({"campaign:sender:all", "campaign:sender:clear"}),
)
async def campaign_sender_bulk(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    selected_ids = (
        [account.id for account in accounts] if (callback.data or "").endswith(":all") else []
    )
    await state.update_data(sender_account_ids=selected_ids)
    await _edit_callback(
        callback,
        wizard_text(
            "📢 Новая рассылка",
            2,
            7,
            "Выберите аккаунты-отправители. При начале отправки чаты распределятся между "
            "ними по кругу. Автозамена умной рассылки не выйдет за пределы этого пула.",
        ),
        campaign_sender_picker(accounts, set(selected_ids)),
    )


@router.callback_query(CampaignCreateState.sender_accounts, F.data == "campaign:sender:done")
async def campaign_sender_done(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    selected = [int(item) for item in data.get("sender_account_ids", [])]
    if not selected:
        await callback.answer("Выберите хотя бы одного отправителя", show_alert=True)
        return
    await state.update_data(primary_account_id=selected[0])
    await _show_campaign_audience_source(callback, state, repository)


@router.callback_query(CampaignCreateState.reply_account, F.data.startswith("campaign:pick:reply:"))
async def campaign_reply_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await callback.answer()
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    if account_id == data.get("primary_account_id"):
        await callback.answer("Аккаунты должны различаться", show_alert=True)
        return
    await state.update_data(reply_account_id=account_id)
    await _show_campaign_audience_source(callback, state, repository, answer=False)


async def _show_campaign_audience_source(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    *,
    answer: bool = True,
) -> None:
    await state.set_state(CampaignCreateState.target)
    targets_page = await repository.list_approved_targets(0, 1)
    target_lists_page = await repository.list_available_target_lists(0, 1)
    data = await state.get_data()
    mode = data.get("mode")
    await _edit_callback(
        callback,
        wizard_text(
            _campaign_title(mode),
            3 if mode == "broadcast" else 4,
            7,
            "Сначала выберите <b>тип аудитории</b>. Списки и вручную добавленные "
            "чаты больше не смешиваются в одном длинном списке.\n\n"
            "• <b>Список</b> — отправка запустится отдельно во всех включённых чатах.\n"
            "• <b>Отдельный чат</b> — только один вручную добавленный чат.",
        ),
        campaign_audience_source_menu(
            standalone_count=targets_page.total,
            list_count=target_lists_page.total,
        ),
        answer=answer,
    )


@router.callback_query(
    CampaignCreateState.target,
    F.data.startswith("campaign:targetsource:"),
)
async def campaign_audience_source(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, kind, raw_page = (callback.data or "").split(":")
    page = int(raw_page)
    if kind == "home":
        await _show_campaign_audience_source(callback, state, repository)
        return
    data = await state.get_data()
    title = _campaign_title(data.get("mode"))
    step = 3 if data.get("mode") == "broadcast" else 4
    if kind == "chats":
        targets_page = await repository.list_approved_targets(page, AUDIENCE_PAGE_SIZE)
        await _edit_callback(
            callback,
            wizard_text(
                title,
                step,
                7,
                "Выберите отдельный чат. Здесь показываются только чаты, которые "
                "вы добавили вручную.",
            ),
            campaign_standalone_target_picker(targets_page),
        )
        return
    if kind == "lists":
        lists_page = await repository.list_available_target_lists(page, AUDIENCE_PAGE_SIZE)
        await _edit_callback(
            callback,
            wizard_text(
                title,
                step,
                7,
                "Выберите список. Число справа — сколько включённых чатов получат отправку.",
            ),
            campaign_target_list_picker(lists_page),
        )
        return
    await callback.answer("Неизвестный тип аудитории", show_alert=True)


@router.callback_query(
    CampaignCreateState.target,
    F.data.startswith("campaign:targetsearch:"),
)
async def begin_campaign_target_search(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    kind = (callback.data or "").rsplit(":", 1)[-1]
    if kind not in {"chats", "lists"}:
        await callback.answer("Неизвестный тип аудитории", show_alert=True)
        return
    await _remember_origin(callback, state)
    await state.update_data(campaign_target_search_kind=kind)
    await state.set_state(CampaignCreateState.target_search)
    await _edit_callback(
        callback,
        f"<b>🔎 Найти {'чат' if kind == 'chats' else 'список чатов'}</b>\n\n"
        "Отправьте часть названия, username/ссылку или точный ID. "
        "Покажу до 20 доступных совпадений.",
        search_prompt_menu(
            "campaign",
            back_callback=f"campaign:targetsource:{kind}:0",
        ),
    )


@router.message(CampaignCreateState.target_search)
async def search_campaign_target(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    query = (message.text or "").strip()
    if not 1 <= len(query) <= 120:
        await message.answer("Отправьте запрос длиной от 1 до 120 символов.")
        return
    data = await state.get_data()
    kind = str(data.get("campaign_target_search_kind") or "")
    if kind == "chats":
        items = await repository.search_targets(query, approved_only=True)
        results = [
            (
                f"📌 {item.title} · {item.chat_ref}",
                f"campaign:pick:target:{item.id}",
            )
            for item in items
        ]
    elif kind == "lists":
        items = await repository.search_target_lists(query, approved_only=True)
        results = [
            (
                f"📚 {item.name} · {item.approved_count} чатов",
                f"campaign:pick:targetlist:{item.id}",
            )
            for item in items
        ]
    else:
        await state.clear()
        await message.answer("Поиск устарел. Откройте создание рассылки заново.")
        return
    await state.set_state(CampaignCreateState.target)
    await _edit_origin(
        bot,
        state,
        "<b>🔎 Результаты поиска аудитории</b>\n"
        f"<code>{escape(query)}</code> · найдено <b>{len(results)}</b>\n\n"
        + ("Выберите нужную аудиторию." if results else "Совпадений нет."),
        search_results_menu(
            results,
            section="campaign",
            new_search_callback=f"campaign:targetsearch:{kind}",
            back_callback=f"campaign:targetsource:{kind}:0",
        ),
    )


@router.callback_query(
    CampaignCreateState.target_search,
    F.data.startswith("campaign:targetsource:"),
)
async def cancel_campaign_target_search(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await state.set_state(CampaignCreateState.target)
    await campaign_audience_source(callback, state, repository)


@router.callback_query(CampaignCreateState.target, F.data.startswith("campaign:pick:page:"))
async def campaign_target_page(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    page = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    selected_senders = data.get("sender_account_ids", [])
    mode = data.get("mode")

    targets_page = await repository.list_approved_targets(page, AUDIENCE_PAGE_SIZE)
    target_lists = await repository.available_target_lists()

    if mode == "broadcast":
        title = "📢 Новая рассылка"
        step = 3
        text_body = (
            f"Отправителей выбрано: <b>{len(selected_senders)}</b>.\n\n"
            "Теперь выберите один чат или список чатов."
        )
    else:
        title = "💬 Новый диалог"
        step = 4
        text_body = (
            "Выберите чат или список чатов. Для списка будет создана отдельная отправка "
            "на каждый открытый чат."
        )

    await _edit_callback(
        callback,
        wizard_text(title, step, 7, text_body),
        campaign_target_picker(targets_page, target_lists),
    )


@router.callback_query(CampaignCreateState.target, F.data.startswith("campaign:pick:target:"))
async def campaign_target(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await callback.answer()
    target_id = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(target_id=target_id, target_list_id=None)
    await _offer_campaign_templates(callback, state, repository)


@router.callback_query(CampaignCreateState.target, F.data.startswith("campaign:pick:targetlist:"))
async def campaign_target_list(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await callback.answer()
    target_list_id = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(target_id=None, target_list_id=target_list_id)
    await _offer_campaign_templates(callback, state, repository)


async def _offer_campaign_templates(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    mode = data.get("mode")
    title = _campaign_title(mode)
    total = _campaign_total_steps(mode)
    step = 4 if mode == "broadcast" else 5
    templates = await repository.enabled_templates(require_reply=mode == "dialogue")
    if not templates:
        await state.set_state(CampaignCreateState.primary_text)
        await _edit_callback(
            callback,
            wizard_text(
                title,
                step,
                total,
                "Готовых текстов пока нет. Введите первое сообщение вручную.",
            ),
            cancel_menu(),
            answer=False,
        )
        return
    await state.set_state(CampaignCreateState.template)
    await _edit_callback(
        callback,
        wizard_text(
            title,
            step,
            total,
            "Выберите готовый текст или ручной ввод. Рассылка возьмёт первое сообщение, "
            "диалог возьмёт первое сообщение и ответ.",
        ),
        template_picker(templates),
        answer=False,
    )


@router.callback_query(CampaignCreateState.template, F.data.startswith("campaign:pick:template:"))
async def campaign_template(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    value = (callback.data or "").rsplit(":", 1)[1]
    if value == "manual":
        await state.set_state(CampaignCreateState.primary_text)
        data = await state.get_data()
        await _edit_callback(
            callback,
            wizard_text(
                _campaign_title(data.get("mode")),
                4 if data.get("mode") == "broadcast" else 5,
                _campaign_total_steps(data.get("mode")),
                "Введите первое сообщение.",
            ),
            cancel_menu(),
        )
        return
    try:
        template = await repository.get_template(int(value))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not template.enabled:
        await callback.answer("Текст поставлен на паузу", show_alert=True)
        return
    data = await state.get_data()
    if data.get("mode") == "dialogue" and not template.reply_text.strip():
        await callback.answer("В этом тексте не задан ответ для диалога", show_alert=True)
        return
    await state.update_data(
        template_id=template.id,
        primary_text=template.primary_text,
        reply_text=template.reply_text,
    )
    if data.get("mode") == "broadcast":
        await state.set_state(CampaignCreateState.repeat_mode)
        await _edit_callback(
            callback,
            _campaign_delivery_mode_text(template_name=template.name),
            campaign_repeat_mode_picker(),
        )
        return
    await state.set_state(CampaignCreateState.reply_delay)
    await _edit_callback(
        callback,
        wizard_text(
            "💬 Новый диалог",
            7,
            7,
            f"Выбран текст <b>{escape(template.name)}</b>.\n\n"
            "Введите задержку ответа: от <b>0</b> до <b>3600</b> секунд.",
        ),
        cancel_menu(),
    )


@router.message(CampaignCreateState.primary_text)
async def campaign_primary_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
) -> None:
    data = await state.get_data()
    if data.get("mode") == "broadcast":
        try:
            value, html_text, media = await _extract_broadcast_content(message, bot, settings)
        except ContentError as exc:
            await message.answer(str(exc))
            return
        await state.update_data(
            primary_text=value,
            primary_html=html_text,
            primary_media_path=media.path if media else None,
            primary_media_kind=media.kind if media else None,
            primary_media_name=media.name if media else None,
            primary_media_size=media.size if media else None,
        )
    else:
        try:
            value, _html_text, media = await _extract_broadcast_content(message, bot, settings)
        except ContentError as exc:
            await message.answer(str(exc))
            return
        await state.update_data(
            primary_text=value,
            primary_media_path=media.path if media else None,
            primary_media_kind=media.kind if media else None,
            primary_media_name=media.name if media else None,
            primary_media_size=media.size if media else None,
        )
    data = await state.get_data()
    if data.get("mode") == "broadcast":
        await state.set_state(CampaignCreateState.repeat_mode)
        await _edit_origin(
            bot,
            state,
            _campaign_delivery_mode_text(),
            campaign_repeat_mode_picker(),
        )
        return
    await state.set_state(CampaignCreateState.reply_text)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "💬 Новый диалог",
            6,
            7,
            "Отправьте ответ: текст, фото, видео, документ, голосовое или видеокружок. "
            "Голосовое и кружок можно отправить без подписи.",
        ),
        cancel_menu(),
    )


@router.message(CampaignCreateState.reply_text)
async def campaign_reply_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
) -> None:
    try:
        value, _html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc))
        return
    await state.update_data(
        reply_text=value,
        reply_media_path=media.path if media else None,
        reply_media_kind=media.kind if media else None,
        reply_media_name=media.name if media else None,
        reply_media_size=media.size if media else None,
    )
    await state.set_state(CampaignCreateState.reply_delay)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "💬 Новый диалог",
            7,
            7,
            "Введите задержку ответа: от <b>0</b> до <b>3600</b> секунд.",
        ),
        cancel_menu(),
    )


@router.message(CampaignCreateState.reply_delay)
async def campaign_delay(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число от 0 до 3600.")
        return
    if not 0 <= value <= 3600:
        await message.answer("Задержка должна быть от 0 до 3600 секунд.")
        return
    data = await state.get_data()
    steps = [
        {
            "account_id": int(data["primary_account_id"]),
            "text": str(data["primary_text"]),
            "delay_seconds": 0,
            "reply_to_previous": False,
            "media_path": data.get("primary_media_path"),
            "media_kind": data.get("primary_media_kind"),
            "media_name": data.get("primary_media_name"),
            "media_size": data.get("primary_media_size"),
        },
        {
            "account_id": int(data["reply_account_id"]),
            "text": str(data["reply_text"]),
            "delay_seconds": value,
            "reply_to_previous": True,
            "media_path": data.get("reply_media_path"),
            "media_kind": data.get("reply_media_kind"),
            "media_name": data.get("reply_media_name"),
            "media_size": data.get("reply_media_size"),
        },
    ]
    await state.update_data(
        reply_delay_seconds=value,
        dialogue_steps=steps,
    )
    await state.set_state(CampaignCreateState.chain_action)
    data = await state.get_data()
    await _edit_origin(
        bot,
        state,
        await _dialogue_chain_builder_text(data, repository),
        dialogue_chain_menu(
            len(steps),
            cancel_callback=_dialogue_chain_cancel_callback(data),
        ),
    )


@router.callback_query(OrderDraftState.action, F.data == "order:edit:chain")
async def begin_order_dialogue_chain(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await state.set_state(CampaignCreateState.chain_action)
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    await _edit_callback(
        callback,
        await _dialogue_chain_builder_text(data, repository),
        dialogue_chain_menu(
            len(steps),
            cancel_callback="order:back",
        ),
    )


@router.callback_query(
    CampaignCreateState.chain_action,
    F.data == "campaign:chain:add",
)
async def add_dialogue_chain_step(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    if len(steps) >= 20:
        await callback.answer("В одной цепочке максимум 20 сообщений", show_alert=True)
        return
    previous_account_id = int(steps[-1]["account_id"]) if steps else None
    accounts = [
        account
        for account in await repository.active_owned_accounts(
            require_authorized_session=bool(data.get("order_require_authorized_session", False))
        )
        if previous_account_id is None or account.id != previous_account_id
    ]
    if not accounts:
        await callback.answer("Нет другого активного аккаунта", show_alert=True)
        return
    await state.set_state(CampaignCreateState.chain_account)
    await _edit_callback(
        callback,
        wizard_text(
            "🧩 Новое сообщение цепочки",
            len(steps) + 1,
            20,
            "Выберите аккаунт для следующего сообщения. Подряд один и тот же "
            "аккаунт не используется, чтобы диалог выглядел естественно.",
        ),
        account_picker(
            accounts,
            "chain",
            cancel_callback=_dialogue_chain_cancel_callback(data),
        ),
    )


@router.callback_query(
    CampaignCreateState.chain_action,
    F.data == "campaign:chain:remove",
)
async def remove_dialogue_chain_step(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    if not steps:
        await callback.answer("Цепочка уже пустая", show_alert=True)
        return
    steps.pop()
    await state.update_data(dialogue_steps=steps)
    data = await state.get_data()
    await _edit_callback(
        callback,
        await _dialogue_chain_builder_text(data, repository),
        dialogue_chain_menu(
            len(steps),
            cancel_callback=_dialogue_chain_cancel_callback(data),
        ),
    )


@router.callback_query(
    CampaignCreateState.chain_action,
    F.data == "campaign:chain:done",
)
async def finish_dialogue_chain(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    if len(steps) < 2:
        await callback.answer("В диалоге должно быть хотя бы два сообщения", show_alert=True)
        return
    if data.get("order_draft_active"):
        await state.set_state(OrderDraftState.action)
        await _show_order_draft(callback, state, repository)
        return
    edit_campaign_id = data.get("edit_chain_campaign_id")
    if edit_campaign_id is not None:
        page = int(data.get("edit_chain_campaign_page", 0))
        try:
            campaign = await repository.update_campaign_dialogue_steps(
                int(edit_campaign_id),
                steps,
            )
            sender_ids = await repository.campaign_sender_ids(campaign.id)
        except (ConflictError, NotFoundError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.clear()
        await callback.answer("Цепочка обновлена")
        await _edit_callback(
            callback,
            campaign_details_text(campaign, sender_ids),
            campaign_details_menu(campaign, page),
            answer=False,
        )
        return
    await state.set_state(CampaignCreateState.confirm)
    await _edit_callback(
        callback,
        campaign_confirmation_text(data),
        campaign_confirmation(),
    )


@router.callback_query(
    CampaignCreateState.chain_account,
    F.data.startswith("campaign:pick:chain:"),
)
async def choose_dialogue_chain_account(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    if steps and int(steps[-1]["account_id"]) == account_id:
        await callback.answer("Выберите другой аккаунт", show_alert=True)
        return
    await state.update_data(chain_account_id=account_id)
    await state.set_state(CampaignCreateState.chain_text)
    await _edit_callback(
        callback,
        wizard_text(
            "🧩 Текст сообщения",
            len(steps) + 1,
            20,
            "Отправьте следующее сообщение: текст, фото, видео, документ, голосовое или "
            "видеокружок. Фото, видео и документу нужна подпись; голосовому и кружку — нет.",
        ),
        order_back_menu() if data.get("order_draft_active") else cancel_menu(),
    )


@router.message(CampaignCreateState.chain_text)
async def dialogue_chain_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        value, _html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc))
        return
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    if not steps:
        steps.append(
            {
                "account_id": int(data["chain_account_id"]),
                "text": value,
                "delay_seconds": 0,
                "reply_to_previous": False,
                "media_path": media.path if media else None,
                "media_kind": media.kind if media else None,
                "media_name": media.name if media else None,
                "media_size": media.size if media else None,
            }
        )
        await state.update_data(dialogue_steps=steps)
        await state.set_state(CampaignCreateState.chain_action)
        data = await state.get_data()
        await _edit_origin(
            bot,
            state,
            await _dialogue_chain_builder_text(data, repository),
            dialogue_chain_menu(
                len(steps),
                cancel_callback=_dialogue_chain_cancel_callback(data),
            ),
        )
        return
    await state.update_data(
        chain_text=value,
        chain_media_path=media.path if media else None,
        chain_media_kind=media.kind if media else None,
        chain_media_name=media.name if media else None,
        chain_media_size=media.size if media else None,
    )
    await state.set_state(CampaignCreateState.chain_delay)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🧩 Задержка сообщения",
            1,
            1,
            "Через сколько секунд после предыдущего сообщения отправить это? "
            "Введите от <b>0</b> до <b>86400</b> (24 часа). Telegram-таймер "
            "конкретного аккаунта имеет приоритет и при необходимости увеличит ожидание.",
        ),
        order_back_menu() if data.get("order_draft_active") else cancel_menu(),
    )


@router.message(CampaignCreateState.chain_delay)
async def dialogue_chain_delay(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        delay_seconds = int((message.text or "").strip())
    except ValueError:
        delay_seconds = -1
    if not 0 <= delay_seconds <= 86400:
        await message.answer("Введите целое число секунд от 0 до 86400.")
        return
    data = await state.get_data()
    steps = [dict(item) for item in data.get("dialogue_steps", [])]
    steps.append(
        {
            "account_id": int(data["chain_account_id"]),
            "text": str(data["chain_text"]),
            "delay_seconds": delay_seconds,
            "reply_to_previous": True,
            "media_path": data.get("chain_media_path"),
            "media_kind": data.get("chain_media_kind"),
            "media_name": data.get("chain_media_name"),
            "media_size": data.get("chain_media_size"),
        }
    )
    await state.update_data(dialogue_steps=steps)
    await state.set_state(CampaignCreateState.chain_action)
    data = await state.get_data()
    await _edit_origin(
        bot,
        state,
        await _dialogue_chain_builder_text(data, repository),
        dialogue_chain_menu(
            len(steps),
            cancel_callback=_dialogue_chain_cancel_callback(data),
        ),
    )


@router.message(CampaignCreateState.spread_hours)
async def campaign_spread_hours(message: Message, state: FSMContext, bot: Bot) -> None:
    try:
        value = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число от 0 до 168 (часов).")
        return
    if not 0 <= value <= 168:
        await message.answer("Окно рассылки должно быть от 0 до 168 часов (7 суток).")
        return
    await state.update_data(
        spread_hours=value,
        reply_text="",
        reply_delay_seconds=0,
        repeat_count=1,
        repeat_interval_minutes=0,
        smart_timer_mode=False,
        smart_duration_hours=0,
        smart_no_timer_interval_minutes=0,
        auto_sender_failover=False,
    )
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_origin(
        bot,
        state,
        campaign_confirmation_text(data),
        campaign_confirmation(),
    )


@router.callback_query(
    CampaignCreateState.spread_hours,
    F.data.startswith("campaign:once:window:"),
)
async def campaign_spread_hours_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    hours = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(
        spread_hours=hours,
        reply_text="",
        reply_delay_seconds=0,
        repeat_count=1,
        repeat_interval_minutes=0,
        smart_timer_mode=False,
        smart_duration_hours=0,
        smart_no_timer_interval_minutes=0,
        auto_sender_failover=False,
    )
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_callback(
        callback,
        campaign_confirmation_text(data),
        campaign_confirmation(),
    )


@router.callback_query(
    CampaignCreateState.repeat_mode,
    F.data.in_({"campaign:repeat:once", "campaign:repeat:smart"}),
)
async def campaign_repeat_mode(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if (callback.data or "").endswith(":once"):
        await state.update_data(
            repeat_count=1,
            repeat_interval_minutes=0,
            smart_timer_mode=False,
            smart_duration_hours=0,
            smart_no_timer_interval_minutes=0,
            auto_sender_failover=False,
        )
        await state.set_state(CampaignCreateState.spread_hours)
        await _edit_callback(
            callback,
            wizard_text(
                "📢 Одноразовая рассылка",
                6,
                7,
                "За сколько часов разложить один пост в каждый чат?\n\n"
                "• <b>0</b> — все отправки готовы сразу; аккаунты всё равно соблюдают "
                "свои безопасные паузы.\n"
                "• <b>24</b> — чаты равномерно распределятся на сутки.\n\n"
                "Выберите готовый вариант или введите целое число часов от "
                "<b>0</b> до <b>168</b>.",
            ),
            campaign_one_off_window_presets(),
        )
        return
    default_profile = broadcast_profile("active")
    await state.update_data(
        spread_hours=0,
        repeat_count=1,
        repeat_interval_minutes=0,
        smart_timer_mode=True,
        smart_no_timer_interval_minutes=default_profile.interval_minutes,
        min_messages_between_posts=default_profile.min_messages_between_posts,
        activity_check_interval_seconds=default_profile.activity_check_interval_seconds,
        visibility_max_wait_seconds=default_profile.visibility_max_wait_seconds,
        auto_sender_failover=True,
    )
    await state.set_state(CampaignCreateState.smart_duration_hours)
    await _edit_callback(
        callback,
        wizard_text(
            "♻️ Умная непрерывная рассылка",
            6,
            7,
            "Сколько часов рассылка должна оставаться активной? В течение этого срока "
            "софт будет снова писать в каждый чат по его собственному расписанию.\n\n"
            "Выберите срок или введите целое число часов от <b>1</b> до <b>168</b>.",
            hint=(
                "Остановка срока не обрывает уже отправляемое сообщение, но новые повторы "
                "после конца срока не создаются."
            ),
        ),
        campaign_smart_duration_presets(),
    )


@router.callback_query(
    CampaignCreateState.smart_duration_hours,
    F.data.startswith("campaign:smart:duration:"),
)
async def campaign_smart_duration_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    hours = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(smart_duration_hours=hours)
    await state.set_state(CampaignCreateState.smart_no_timer_interval_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "♻️ Умная непрерывная рассылка",
            7,
            7,
            f"Срок активности: <b>{hours} ч.</b>\n\n"
            "Как часто писать в чат, если у него <b>нет</b> ни Telegram slow mode, "
            "ни ручного таймера? Выберите вариант или введите от <b>1</b> до "
            "<b>10080</b> минут. Активное значение — <b>5 минут</b>.\n\n"
            "Если таймер есть, этот интервал не используется: отправка становится готова "
            "сразу по окончании таймера конкретного чата.",
            hint=(
                "При блокировке отправителя автозамена ищет резерв только среди аккаунтов, "
                "которые вы отметили на шаге 2."
            ),
        ),
        campaign_smart_interval_presets(),
    )


@router.message(CampaignCreateState.smart_duration_hours)
async def campaign_smart_duration(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    try:
        hours = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число часов от 1 до 168.")
        return
    if not 1 <= hours <= 168:
        await message.answer("Срок активности должен быть от 1 часа до 7 суток.")
        return
    await state.update_data(smart_duration_hours=hours)
    await state.set_state(CampaignCreateState.smart_no_timer_interval_minutes)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "♻️ Умная непрерывная рассылка",
            7,
            7,
            f"Срок активности: <b>{hours} ч.</b>\n\n"
            "Как часто писать в чат, если у него <b>нет</b> ни Telegram slow mode, "
            "ни ручного таймера? Выберите вариант или введите от <b>1</b> до "
            "<b>10080</b> минут. Активное значение — <b>5 минут</b>.\n\n"
            "Если таймер есть, этот интервал не используется: отправка становится готова "
            "сразу по окончании таймера конкретного чата.",
            hint=(
                "При блокировке отправителя автозамена ищет резерв только среди аккаунтов, "
                "которые вы отметили на шаге 2."
            ),
        ),
        campaign_smart_interval_presets(),
    )


@router.callback_query(
    CampaignCreateState.smart_no_timer_interval_minutes,
    F.data.startswith("campaign:smart:interval:"),
)
async def campaign_smart_interval_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    minutes = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(smart_no_timer_interval_minutes=minutes)
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_callback(
        callback,
        campaign_confirmation_text(data),
        campaign_confirmation(),
    )


@router.message(CampaignCreateState.smart_no_timer_interval_minutes)
async def campaign_smart_interval(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    try:
        minutes = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    if not 1 <= minutes <= 10080:
        await message.answer("Интервал должен быть от 1 минуты до 7 суток.")
        return
    await state.update_data(smart_no_timer_interval_minutes=minutes)
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_origin(bot, state, campaign_confirmation_text(data), campaign_confirmation())


@router.callback_query(
    CampaignCreateState.repeat_count,
    F.data.startswith("crepeat:count:"),
)
async def campaign_repeat_count_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    count = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(repeat_count=count)
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_callback(
        callback,
        campaign_confirmation_text(data),
        campaign_confirmation(),
    )


@router.message(CampaignCreateState.repeat_count)
async def campaign_repeat_count(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    try:
        count = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число проходов от 2 до 100.")
        return
    if not 2 <= count <= 100:
        await message.answer("Число проходов должно быть от 2 до 100.")
        return
    await state.update_data(repeat_count=count)
    await state.set_state(CampaignCreateState.confirm)
    data = await state.get_data()
    await _edit_origin(bot, state, campaign_confirmation_text(data), campaign_confirmation())


@router.callback_query(CampaignCreateState.confirm, F.data == "campaign:confirm")
async def confirm_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    mode = CampaignMode.BROADCAST if data.get("mode") == "broadcast" else CampaignMode.DIALOGUE
    smart_timer_mode = mode == CampaignMode.BROADCAST and bool(data.get("smart_timer_mode", False))
    spread_hours = int(data.get("spread_hours", 0))
    smart_duration_hours = int(data.get("smart_duration_hours", 0))
    smart_no_timer_interval_minutes = int(data.get("smart_no_timer_interval_minutes", 0))
    default_profile = broadcast_profile("active")
    try:
        campaign = await repository.create_campaign(
            name=str(data["name"]),
            mode=mode,
            primary_account_id=int(data["primary_account_id"]),
            reply_account_id=(
                int(data["reply_account_id"]) if data.get("reply_account_id") is not None else None
            ),
            target_id=(int(data["target_id"]) if data.get("target_id") is not None else None),
            target_list_id=(
                int(data["target_list_id"]) if data.get("target_list_id") is not None else None
            ),
            primary_text=str(data["primary_text"]),
            primary_html=(
                str(data["primary_html"])
                if mode == CampaignMode.BROADCAST and data.get("primary_html")
                else None
            ),
            primary_media_path=(
                str(data["primary_media_path"])
                if mode == CampaignMode.BROADCAST and data.get("primary_media_path")
                else None
            ),
            primary_media_kind=(
                str(data["primary_media_kind"])
                if mode == CampaignMode.BROADCAST and data.get("primary_media_kind")
                else None
            ),
            primary_media_name=(
                str(data["primary_media_name"])
                if mode == CampaignMode.BROADCAST and data.get("primary_media_name")
                else None
            ),
            primary_media_size=(
                int(data["primary_media_size"])
                if mode == CampaignMode.BROADCAST
                and data.get("primary_media_size") is not None
                else None
            ),
            reply_text=str(data.get("reply_text") or ""),
            reply_delay_seconds=int(data.get("reply_delay_seconds") or 0),
            spread_seconds=0 if smart_timer_mode else spread_hours * 3600,
            repeat_count=1,
            repeat_interval_seconds=0,
            smart_timer_mode=smart_timer_mode,
            smart_duration_seconds=(smart_duration_hours * 3600 if smart_timer_mode else 0),
            smart_no_timer_interval_seconds=(
                smart_no_timer_interval_minutes * 60 if smart_timer_mode else 0
            ),
            activity_aware=smart_timer_mode,
            min_messages_between_posts=(
                int(
                    data.get(
                        "min_messages_between_posts",
                        default_profile.min_messages_between_posts,
                    )
                )
                if smart_timer_mode
                else 0
            ),
            activity_check_interval_seconds=int(
                data.get(
                    "activity_check_interval_seconds",
                    default_profile.activity_check_interval_seconds,
                )
            ),
            visibility_max_wait_seconds=int(
                data.get(
                    "visibility_max_wait_seconds",
                    default_profile.visibility_max_wait_seconds,
                )
            ),
            auto_sender_failover=smart_timer_mode,
            sender_mode=(
                BroadcastSenderMode.INDEPENDENT
                if mode == CampaignMode.BROADCAST
                else BroadcastSenderMode.DISTRIBUTED
            ),
            sender_account_ids=[int(item) for item in data.get("sender_account_ids", [])],
            dialogue_steps=(
                [dict(item) for item in data.get("dialogue_steps", [])]
                if mode == CampaignMode.DIALOGUE
                else None
            ),
        )
    except ConflictError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Сценарий сохранён")
    page = await repository.list_campaigns(0, PAGE_SIZE)
    await _edit_callback(
        callback,
        f"✅ «{escape(campaign.name)}» сохранено. Теперь можно запустить из списка.\n\n"
        f"{campaigns_text(page)}",
        campaigns_menu(page),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:edit:content:"))
async def begin_campaign_content_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _remember_origin(callback, state)
    if campaign.mode == CampaignMode.BROADCAST:
        await state.update_data(
            edit_content_campaign_id=campaign.id,
            edit_content_campaign_page=int(page),
        )
        await state.set_state(CampaignContentEditState.primary_text)
        await _edit_callback(
            callback,
            wizard_text(
                f"✏️ Пост · {campaign.name}",
                1,
                1,
                "Отправьте новый текст или медиа с подписью. Форматирование сохранится. "
                "Пост будет использоваться в новых "
                "рассылках. Уже активную рассылку меняйте отдельно из её карточки в "
                "разделе «Ход и отчёты».",
            ),
            campaign_content_edit_cancel(campaign.id, int(page)),
        )
        return
    try:
        steps = parse_dialogue_steps(
            campaign.dialogue_steps_json,
            primary_account_id=campaign.primary_account_id,
            primary_text=campaign.primary_text,
            reply_account_id=campaign.reply_account_id,
            reply_text=campaign.reply_text,
            reply_delay_seconds=campaign.reply_delay_seconds,
        )
    except ValueError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.update_data(
        dialogue_steps=[step.as_dict() for step in steps],
        edit_chain_campaign_id=campaign.id,
        edit_chain_campaign_page=int(page),
    )
    await state.set_state(CampaignCreateState.chain_action)
    data = await state.get_data()
    await _edit_callback(
        callback,
        await _dialogue_chain_builder_text(data, repository),
        dialogue_chain_menu(
            len(steps),
            cancel_callback=_dialogue_chain_cancel_callback(data),
        ),
    )


@router.message(CampaignContentEditState.primary_text)
async def save_campaign_content_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        value, html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc))
        return
    data = await state.get_data()
    campaign_id = int(data["edit_content_campaign_id"])
    page = int(data["edit_content_campaign_page"])
    try:
        campaign = await repository.update_campaign_primary_text(
            campaign_id,
            value,
            html_text=html_text,
            media_path=media.path if media else None,
            media_kind=media.kind if media else None,
            media_name=media.name if media else None,
            media_size=media.size if media else None,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>Текст будущих рассылок обновлён.</b>\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
    )
    await state.clear()


@router.callback_query(F.data.startswith("campaign:content:cancel:"))
async def cancel_campaign_content_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    await state.clear()
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Изменения отменены")
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:edit:target:"))
async def begin_campaign_target_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    targets_page = await repository.list_approved_targets(0, 1)
    target_lists = await repository.list_available_target_lists(0, 1)
    if not targets_page.total and not target_lists.total:
        await callback.answer("Нет включённых чатов или баз", show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(edit_campaign_id=campaign.id, edit_campaign_page=int(page))
    await state.set_state(CampaignEditState.target)
    await _edit_callback(
        callback,
        wizard_text(
            f"🎯 Куда отправлять · {campaign.name}",
            1,
            1,
            "Выберите новый чат или список. Уже созданные отправки и история не изменятся; "
            "новая цель будет использована в следующей рассылке.",
        ),
        campaign_edit_target_source_menu(
            standalone_count=targets_page.total,
            list_count=target_lists.total,
            campaign_id=campaign.id,
            campaign_page=int(page),
        ),
        answer=False,
    )


@router.callback_query(
    CampaignEditState.target,
    F.data.startswith("campaign:edit:browse:"),
)
async def browse_campaign_edit_targets(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, kind, campaign_id_str, campaign_page_str, target_page_str = (
        callback.data or ""
    ).split(":")
    campaign_id = int(campaign_id_str)
    campaign_page = int(campaign_page_str)
    target_page = int(target_page_str)
    try:
        campaign = await repository.get_campaign(campaign_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if kind == "lists":
        page = await repository.list_available_target_lists(target_page, AUDIENCE_PAGE_SIZE)
        if page.page >= page.pages:
            page = await repository.list_available_target_lists(
                page.pages - 1,
                AUDIENCE_PAGE_SIZE,
            )
        description = "Выберите один список. В скобках показано число включённых чатов."
    elif kind == "chats":
        page = await repository.list_approved_targets(target_page, AUDIENCE_PAGE_SIZE)
        if page.page >= page.pages:
            page = await repository.list_approved_targets(
                page.pages - 1,
                AUDIENCE_PAGE_SIZE,
            )
        description = "Выберите один вручную добавленный чат."
    else:
        await callback.answer("Неизвестный тип аудитории", show_alert=True)
        return
    await _edit_callback(
        callback,
        wizard_text(
            f"🎯 Куда отправлять · {campaign.name}",
            1,
            1,
            description + " Уже созданные отправки и история не изменятся; новые чаты "
            "будут использованы только в следующей рассылке.",
        ),
        campaign_edit_target_picker(
            page,
            kind=kind,
            campaign_id=campaign.id,
            campaign_page=campaign_page,
        ),
    )


@router.callback_query(
    CampaignEditState.target,
    F.data.startswith("campaign:edit:search:"),
)
async def begin_campaign_edit_target_search(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    _, _, _, kind, campaign_id, campaign_page = (callback.data or "").split(":")
    if kind not in {"chats", "lists"}:
        await callback.answer("Неизвестный тип аудитории", show_alert=True)
        return
    await state.update_data(edit_target_search_kind=kind)
    await state.set_state(CampaignEditState.search)
    back_callback = f"campaign:edit:browse:{kind}:{campaign_id}:{campaign_page}:0"
    await _edit_callback(
        callback,
        f"<b>🔎 Найти {'чат' if kind == 'chats' else 'список чатов'}</b>\n\n"
        "Отправьте часть названия, username/ссылку или точный ID. "
        "Поиск затронет только доступные аудитории.",
        search_prompt_menu("campaign", back_callback=back_callback),
    )


@router.message(CampaignEditState.search)
async def search_campaign_edit_target(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    query = (message.text or "").strip()
    if not 1 <= len(query) <= 120:
        await message.answer("Отправьте запрос длиной от 1 до 120 символов.")
        return
    data = await state.get_data()
    kind = str(data.get("edit_target_search_kind") or "")
    campaign_id = int(data["edit_campaign_id"])
    campaign_page = int(data["edit_campaign_page"])
    if kind == "chats":
        items = await repository.search_targets(query, approved_only=True)
        results = [
            (f"📌 {item.title} · {item.chat_ref}", f"cedit:target:{item.id}") for item in items
        ]
    elif kind == "lists":
        items = await repository.search_target_lists(query, approved_only=True)
        results = [
            (f"📚 {item.name} · {item.approved_count} чатов", f"cedit:list:{item.id}")
            for item in items
        ]
    else:
        await state.clear()
        await message.answer("Поиск устарел. Откройте редактирование аудитории заново.")
        return
    await state.set_state(CampaignEditState.target)
    await _edit_origin(
        bot,
        state,
        "<b>🔎 Результаты поиска аудитории</b>\n"
        f"<code>{escape(query)}</code> · найдено <b>{len(results)}</b>\n\n"
        + ("Выберите новую аудиторию." if results else "Совпадений нет."),
        search_results_menu(
            results,
            section="campaign",
            new_search_callback=(f"campaign:edit:search:{kind}:{campaign_id}:{campaign_page}"),
            back_callback=(f"campaign:edit:browse:{kind}:{campaign_id}:{campaign_page}:0"),
        ),
    )


@router.callback_query(
    CampaignEditState.search,
    F.data.startswith("campaign:edit:browse:"),
)
async def cancel_campaign_edit_target_search(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    await state.set_state(CampaignEditState.target)
    await browse_campaign_edit_targets(callback, repository)


@router.callback_query(CampaignEditState.target, F.data.startswith("cedit:"))
async def save_campaign_target_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, kind, raw_id = (callback.data or "").split(":")
    data = await state.get_data()
    target_id = int(raw_id) if kind == "target" else None
    target_list_id = int(raw_id) if kind == "list" else None
    if target_id is None and target_list_id is None:
        await callback.answer("Некорректная цель", show_alert=True)
        return
    try:
        campaign = await repository.update_campaign_target(
            int(data["edit_campaign_id"]),
            target_id=target_id,
            target_list_id=target_list_id,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    page = int(data["edit_campaign_page"])
    await state.clear()
    await callback.answer("Цель обновлена")
    await _edit_callback(
        callback,
        "✅ <b>Чат или список изменены для следующих рассылок.</b>\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:edit:schedule:"))
async def begin_campaign_schedule_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer()
    await _remember_origin(callback, state)
    await state.update_data(
        schedule_campaign_id=campaign.id,
        schedule_campaign_page=int(page),
        schedule_campaign_mode=campaign.mode.value,
    )
    await state.set_state(CampaignScheduleEditState.mode)
    if campaign.smart_timer_mode:
        current = (
            "умная непрерывная · срок "
            f"{campaign.smart_duration_seconds // 3600} ч. · без таймера каждые "
            f"{campaign.smart_no_timer_interval_seconds // 60} мин."
        )
    elif campaign.repeat_count == 1:
        current = "одноразовая · один проход"
    else:
        current = (
            f"конечная · {campaign.repeat_count} проходов · минимум "
            f"{campaign.repeat_interval_seconds // 60} мин."
        )
    await _edit_callback(
        callback,
        wizard_text(
            f"🧠 Расписание · {escape(campaign.name)}",
            1,
            3,
            f"Сейчас: <b>{current}</b>.\n\n"
            "Можно сделать сценарий одноразовым или настроить умную непрерывную работу. "
            + (
                "Для обычной рассылки также доступны конечные повторы. "
                if campaign.mode == CampaignMode.BROADCAST
                else "Новый диалог начнётся только после полного завершения предыдущей цепочки. "
            )
            + "Изменение повлияет только "
            "на новые рассылки.",
        ),
        campaign_schedule_mode_picker(
            smart_timer_mode=campaign.smart_timer_mode,
            allow_finite_repeats=campaign.mode == CampaignMode.BROADCAST,
        ),
        answer=False,
    )


@router.callback_query(
    CampaignScheduleEditState.mode,
    F.data.in_({"cschedule:once", "cschedule:smart", "cschedule:continuous"}),
)
async def choose_campaign_schedule_mode(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    if callback.data == "cschedule:once":
        try:
            campaign = await repository.update_campaign_schedule(
                int(data["schedule_campaign_id"]),
                repeat_count=1,
                repeat_interval_seconds=0,
            )
            sender_ids = await repository.campaign_sender_ids(campaign.id)
        except (NotFoundError, ConflictError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        page = int(data["schedule_campaign_page"])
        await state.clear()
        await callback.answer("Оставлен один проход")
        await _edit_callback(
            callback,
            "✅ <b>Расписание обновлено.</b>\n\n" + campaign_details_text(campaign, sender_ids),
            campaign_details_menu(campaign, page),
            answer=False,
        )
        return
    if callback.data == "cschedule:continuous":
        await state.set_state(CampaignScheduleEditState.smart_duration_hours)
        await _edit_callback(
            callback,
            wizard_text(
                "♻️ Умная непрерывная рассылка",
                2,
                3,
                "Укажите новый срок активности: выберите вариант или введите целое "
                "число часов от <b>1</b> до <b>168</b>.\n\n"
                "В течение этого срока каждый чат работает независимо: чат с таймером "
                "готов к отправке сразу после его окончания.",
            ),
            campaign_smart_duration_presets(cancel_callback="cschedule:cancel"),
        )
        return
    await state.set_state(CampaignScheduleEditState.interval_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "🧠 Умное расписание",
            2,
            3,
            "Введите минимальный интервал между проходами: от <b>1</b> до "
            "<b>10080</b> минут. Таймер Telegram или интервал самого чата сможет "
            "перенести конкретную отправку позже. «По таймеру чата» означает минимум "
            "1 минуту; для чатов без slow mode задайте интервал в карточке чата.",
        ),
        campaign_repeat_interval_presets("cschedule"),
    )


@router.callback_query(
    CampaignScheduleEditState.smart_duration_hours,
    F.data.startswith("campaign:smart:duration:"),
)
async def edit_campaign_smart_duration_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    hours = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(schedule_smart_duration_hours=hours)
    await state.set_state(CampaignScheduleEditState.smart_no_timer_interval_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "♻️ Умная непрерывная рассылка",
            3,
            3,
            f"Срок активности: <b>{hours} ч.</b>\n\n"
            "Задайте интервал только для чатов <b>без</b> Telegram slow mode и "
            "без ручного таймера: от <b>1</b> до <b>10080</b> минут. "
            "Активный режим по умолчанию — <b>5 минут</b>.\n\n"
            "Чат с таймером будет готов к отправке сразу по окончании своего таймера.",
            hint=(
                "Автозамена отправителя останется включена только внутри выбранного "
                "для этой рассылки пула аккаунтов."
            ),
        ),
        campaign_smart_interval_presets(cancel_callback="cschedule:cancel"),
    )


@router.message(CampaignScheduleEditState.smart_duration_hours)
async def edit_campaign_smart_duration(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    try:
        hours = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число часов от 1 до 168.")
        return
    if not 1 <= hours <= 168:
        await message.answer("Срок активности должен быть от 1 часа до 7 суток.")
        return
    await state.update_data(schedule_smart_duration_hours=hours)
    await state.set_state(CampaignScheduleEditState.smart_no_timer_interval_minutes)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "♻️ Умная непрерывная рассылка",
            3,
            3,
            f"Срок активности: <b>{hours} ч.</b>\n\n"
            "Задайте интервал только для чатов <b>без</b> Telegram slow mode и "
            "без ручного таймера: от <b>1</b> до <b>10080</b> минут. "
            "Активный режим по умолчанию — <b>5 минут</b>.\n\n"
            "Чат с таймером будет готов к отправке сразу по окончании своего таймера.",
            hint=(
                "Автозамена отправителя останется включена только внутри выбранного "
                "для этой рассылки пула аккаунтов."
            ),
        ),
        campaign_smart_interval_presets(cancel_callback="cschedule:cancel"),
    )


@router.callback_query(
    CampaignScheduleEditState.smart_no_timer_interval_minutes,
    F.data.startswith("campaign:smart:interval:"),
)
async def save_campaign_smart_schedule_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    minutes = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    try:
        campaign = await repository.update_campaign_smart_schedule(
            int(data["schedule_campaign_id"]),
            smart_duration_seconds=int(data["schedule_smart_duration_hours"]) * 3600,
            smart_no_timer_interval_seconds=minutes * 60,
            auto_sender_failover=data.get("schedule_campaign_mode") == "broadcast",
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    page = int(data["schedule_campaign_page"])
    await state.clear()
    await callback.answer("Умная непрерывная рассылка сохранена")
    await _edit_callback(
        callback,
        "✅ <b>Умная непрерывная рассылка обновлена.</b>\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
        answer=False,
    )


@router.message(CampaignScheduleEditState.smart_no_timer_interval_minutes)
async def save_campaign_smart_schedule(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        minutes = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    if not 1 <= minutes <= 10080:
        await message.answer("Интервал должен быть от 1 минуты до 7 суток.")
        return
    data = await state.get_data()
    try:
        campaign = await repository.update_campaign_smart_schedule(
            int(data["schedule_campaign_id"]),
            smart_duration_seconds=int(data["schedule_smart_duration_hours"]) * 3600,
            smart_no_timer_interval_seconds=minutes * 60,
            auto_sender_failover=data.get("schedule_campaign_mode") == "broadcast",
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (NotFoundError, ConflictError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>Умная непрерывная рассылка обновлена.</b>\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(data["schedule_campaign_page"])),
    )


@router.callback_query(
    CampaignScheduleEditState.interval_minutes,
    F.data.startswith("cschedule:interval:"),
)
async def edit_campaign_schedule_interval_preset(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    minutes = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(schedule_interval_minutes=minutes)
    await state.set_state(CampaignScheduleEditState.repeat_count)
    await _edit_callback(
        callback,
        wizard_text(
            "🧠 Умное расписание",
            3,
            3,
            f"Интервал: <b>{minutes} мин.</b>\n\n"
            "Выберите число проходов или введите своё значение от <b>2</b> до "
            "<b>100</b>.",
        ),
        campaign_repeat_count_presets("cschedule"),
    )


@router.message(CampaignScheduleEditState.interval_minutes)
async def edit_campaign_schedule_interval(
    message: Message,
    state: FSMContext,
    bot: Bot,
) -> None:
    try:
        minutes = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    if not 1 <= minutes <= 10080:
        await message.answer("Интервал должен быть от 1 минуты до 7 суток.")
        return
    await state.update_data(schedule_interval_minutes=minutes)
    await state.set_state(CampaignScheduleEditState.repeat_count)
    await _edit_origin(
        bot,
        state,
        wizard_text(
            "🧠 Умное расписание",
            3,
            3,
            "Введите общее число проходов от <b>2</b> до <b>100</b>.",
        ),
        campaign_repeat_count_presets("cschedule"),
    )


@router.callback_query(
    CampaignScheduleEditState.repeat_count,
    F.data.startswith("cschedule:count:"),
)
async def save_campaign_schedule_preset(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    count = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    try:
        campaign = await repository.update_campaign_schedule(
            int(data["schedule_campaign_id"]),
            repeat_count=count,
            repeat_interval_seconds=int(data["schedule_interval_minutes"]) * 60,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    page = int(data["schedule_campaign_page"])
    await state.clear()
    await callback.answer("Умное расписание сохранено")
    await _edit_callback(
        callback,
        "✅ <b>Умное расписание сохранено.</b>\n\n" + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
        answer=False,
    )


@router.message(CampaignScheduleEditState.repeat_count)
async def save_campaign_schedule(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
) -> None:
    try:
        count = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите целое число проходов от 2 до 100.")
        return
    if not 2 <= count <= 100:
        await message.answer("Число проходов должно быть от 2 до 100.")
        return
    data = await state.get_data()
    try:
        campaign = await repository.update_campaign_schedule(
            int(data["schedule_campaign_id"]),
            repeat_count=count,
            repeat_interval_seconds=int(data["schedule_interval_minutes"]) * 60,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (NotFoundError, ConflictError) as exc:
        await message.answer(escape(str(exc)))
        return
    await state.clear()
    await _edit_origin_data(
        bot,
        data,
        "✅ <b>Умное расписание сохранено.</b>\n\n" + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(data["schedule_campaign_page"])),
    )


@router.callback_query(F.data == "cschedule:cancel")
async def cancel_campaign_schedule_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    campaign_id = data.get("schedule_campaign_id")
    page = int(data.get("schedule_campaign_page", 0))
    await state.clear()
    if campaign_id is None:
        await callback.answer("Изменения отменены")
        return
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Изменения отменены")
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:toggle:"))
async def toggle_campaign(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.toggle_campaign(int(campaign_id))
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Сценарий включён" if campaign.enabled else "Сценарий на паузе")
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:details:"))
async def campaign_details(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
    )


async def _show_campaign_sender_editor(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
    *,
    answer: bool = True,
) -> None:
    data = await state.get_data()
    campaign_id = int(data["edit_campaign_id"])
    page = int(data["edit_campaign_page"])
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    selected_ids = {int(item) for item in data.get("edit_sender_ids", [])}
    await _edit_callback(
        callback,
        wizard_text(
            "👥 Отправители",
            1,
            1,
            "Выберите аккаунты для будущих отправок этой рассылки. Уже начатые "
            "отправки не изменятся и продолжат работать со своим исходным пулом.",
            hint="Можно выбрать один аккаунт или несколько. В независимом режиме каждый "
            "выбранный аккаунт работает в чате отдельно.",
        ),
        campaign_sender_picker(
            accounts,
            selected_ids,
            cancel_callback=f"campaign:details:{campaign_id}:{page}",
        ),
        answer=answer,
    )


@router.callback_query(F.data.startswith("campaign:edit:senders:"))
async def begin_campaign_sender_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        if campaign.mode != CampaignMode.BROADCAST:
            await callback.answer("Пул отправителей доступен только для рассылки", show_alert=True)
            return
        selected_ids = await repository.campaign_sender_ids(campaign.id)
        accounts = await repository.active_owned_accounts(
            require_authorized_session=not settings.dry_run
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not accounts:
        await callback.answer("Нет готовых аккаунтов для выбора", show_alert=True)
        return
    available_ids = {account.id for account in accounts}
    await state.clear()
    await state.update_data(
        edit_campaign_id=int(campaign_id),
        edit_campaign_page=int(page),
        edit_sender_ids=[account_id for account_id in selected_ids if account_id in available_ids],
    )
    await state.set_state(CampaignEditState.senders)
    await _show_campaign_sender_editor(callback, state, repository, settings)


@router.callback_query(
    CampaignEditState.senders,
    F.data.startswith("campaign:sender:toggle:"),
)
async def edit_campaign_sender_toggle(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    selected_ids = [int(item) for item in data.get("edit_sender_ids", [])]
    if account_id in selected_ids:
        selected_ids.remove(account_id)
    else:
        selected_ids.append(account_id)
    await state.update_data(edit_sender_ids=selected_ids)
    await _show_campaign_sender_editor(callback, state, repository, settings)


@router.callback_query(
    CampaignEditState.senders,
    F.data.in_({"campaign:sender:all", "campaign:sender:clear"}),
)
async def edit_campaign_sender_bulk(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    selected_ids = (
        [account.id for account in accounts] if (callback.data or "").endswith(":all") else []
    )
    await state.update_data(edit_sender_ids=selected_ids)
    await _show_campaign_sender_editor(callback, state, repository, settings)


@router.callback_query(CampaignEditState.senders, F.data == "campaign:sender:done")
async def save_campaign_sender_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    selected_ids = [int(item) for item in data.get("edit_sender_ids", [])]
    if not selected_ids:
        await callback.answer("Выберите хотя бы одного отправителя", show_alert=True)
        return
    campaign_id = int(data["edit_campaign_id"])
    page = int(data["edit_campaign_page"])
    try:
        campaign = await repository.update_campaign_senders(campaign_id, selected_ids)
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Пул изменён для будущих отправок")
    await _edit_callback(
        callback,
        "✅ <b>Отправители обновлены.</b> Уже начатые отправки не изменились.\n\n"
        + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, page),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:variants:"))
async def campaign_variants(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        variants = await repository.campaign_content_variants(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await _edit_callback(
        callback,
        campaign_variants_text(campaign, variants),
        campaign_variants_menu(campaign, variants, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:variant:view:"))
async def campaign_variant_details(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, variants, variant = await _campaign_variant_context(
            repository,
            int(campaign_id),
            variant_id,
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        campaign_variant_text(campaign, variant),
        campaign_variant_menu(
            campaign,
            variant,
            variant_count=len(variants),
            page=int(page),
        ),
    )


@router.callback_query(F.data.startswith("campaign:variant:add:"))
async def start_campaign_variant_add(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        variants = await repository.campaign_content_variants(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if len(variants) >= 10:
        await callback.answer("Достигнут лимит: 10 вариантов", show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_edit_mode="add",
    )
    await state.set_state(CampaignVariantState.name)
    await _edit_callback(
        callback,
        "<b>➕ Новый вариант · шаг 1 из 2</b>\n\n"
        "Отправьте короткое понятное название, например "
        "<code>Оффер A</code> или <code>Вечерний текст</code>.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.message(CampaignVariantState.name)
async def receive_campaign_variant_name(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    name = (message.text or "").strip()
    data = await state.get_data()
    campaign_id = int(data.get("variant_campaign_id") or 0)
    page = int(data.get("variant_campaign_page") or 0)
    if not 1 <= len(name) <= 80:
        await message.answer(
            "Название должно содержать от 1 до 80 символов.",
            reply_markup=campaign_variant_input_cancel(campaign_id, page),
        )
        return
    if data.get("variant_edit_mode") == "rename":
        variant_id = str(data.get("variant_id") or "")
        try:
            variant = await repository.rename_campaign_content_variant(
                campaign_id, variant_id, name
            )
            campaign, variants, _ = await _campaign_variant_context(
                repository, campaign_id, variant_id
            )
        except (ConflictError, NotFoundError) as exc:
            await message.answer(
                f"<b>Не удалось изменить название:</b> {escape(str(exc))}",
                reply_markup=campaign_variant_input_cancel(campaign_id, page),
            )
            return
        await state.clear()
        await message.answer(
            "✅ <b>Название изменено.</b>\n\n" + campaign_variant_text(campaign, variant),
            reply_markup=campaign_variant_menu(
                campaign, variant, variant_count=len(variants), page=page
            ),
        )
        return
    await state.update_data(variant_name=name)
    await state.set_state(CampaignVariantState.content)
    await message.answer(
        "<b>➕ Новый вариант · шаг 2 из 2</b>\n\n"
        "Отправьте готовый рекламный пост. Можно обычный или форматированный текст, "
        "а также одно фото, видео или документ с подписью. После создания в карточке "
        "варианта можно собрать альбом до 10 фото/видео.",
        reply_markup=campaign_variant_input_cancel(campaign_id, page),
    )


@router.message(CampaignVariantState.content)
async def receive_campaign_variant_content(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    bot: Bot,
    settings: Settings,
) -> None:
    data = await state.get_data()
    campaign_id = int(data.get("variant_campaign_id") or 0)
    page = int(data.get("variant_campaign_page") or 0)
    edit_mode = str(data.get("variant_edit_mode") or "add")
    try:
        if edit_mode == "text" and (
            message.photo or message.video is not None or message.document is not None
        ):
            raise ContentError(
                "здесь меняется только текст; альбом редактируется отдельной кнопкой"
            )
        text, html_text, media = await _extract_broadcast_content(message, bot, settings)
        if edit_mode == "text":
            variant = await repository.update_campaign_variant_text(
                campaign_id,
                str(data.get("variant_id") or ""),
                text=text,
                html_text=html_text,
            )
        else:
            variant = await repository.add_campaign_content_variant(
                campaign_id,
                name=str(data.get("variant_name") or "Новый вариант"),
                text=text,
                html_text=html_text,
                media=media,
            )
        campaign, variants, _ = await _campaign_variant_context(
            repository,
            campaign_id,
            variant.variant_id,
        )
    except (ContentError, ConflictError, NotFoundError) as exc:
        await message.answer(
            f"<b>Не удалось создать вариант:</b> {escape(str(exc))}",
            reply_markup=campaign_variant_input_cancel(campaign_id, page),
        )
        return
    await state.clear()
    await message.answer(
        (
            "✅ <b>Текст варианта обновлён.</b>\n\n"
            if edit_mode == "text"
            else "✅ <b>Вариант добавлен и включён в ротацию.</b>\n\n"
        )
        + campaign_variant_text(campaign, variant),
        reply_markup=campaign_variant_menu(
            campaign,
            variant,
            variant_count=len(variants),
            page=page,
        ),
    )


@router.callback_query(F.data.startswith("campaign:variant:contact:add:"))
async def start_campaign_contact_variant_add(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        variants = await repository.campaign_content_variants(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if len(variants) >= 10:
        await callback.answer("Достигнут лимит: 10 вариантов", show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_edit_mode="contact_add",
    )
    await state.set_state(CampaignVariantState.contact)
    await _edit_callback(
        callback,
        "<b>📇 Новый контакт</b>\n\n"
        "Самый быстрый способ — отправьте контакт через скрепку Telegram. "
        "Либо пришлите одной строкой:\n"
        "<code>+79991234567 | Алиса | Соколова</code>\n\n"
        "Фамилию можно не указывать. Контакт станет отдельной публикацией и "
        "автоматически войдёт в ротацию рассылки.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:variant:contact:edit:"))
async def start_campaign_contact_variant_edit(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, _, variant = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if variant.contact is None:
        await callback.answer("Этот вариант не является контактом", show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_id=variant.variant_id,
        variant_edit_mode="contact_edit",
    )
    await state.set_state(CampaignVariantState.contact)
    await _edit_callback(
        callback,
        f"<b>✏️ Контакт · {escape(variant.name)}</b>\n\n"
        "Отправьте новый контакт через скрепку Telegram или строкой:\n"
        "<code>+79991234567 | Алиса | Соколова</code>\n\n"
        "Фамилию можно не указывать.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.message(CampaignVariantState.contact)
async def receive_campaign_variant_contact(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    data = await state.get_data()
    campaign_id = int(data.get("variant_campaign_id") or 0)
    page = int(data.get("variant_campaign_page") or 0)
    edit_mode = str(data.get("variant_edit_mode") or "contact_add")
    try:
        contact = _contact_from_message(message)
        if edit_mode == "contact_edit":
            variant = await repository.update_campaign_variant_contact(
                campaign_id,
                str(data.get("variant_id") or ""),
                contact,
            )
        else:
            variant = await repository.add_campaign_contact_variant(campaign_id, contact)
        campaign, variants, _ = await _campaign_variant_context(
            repository,
            campaign_id,
            variant.variant_id,
        )
    except (ContentError, ConflictError, NotFoundError) as exc:
        await message.answer(
            f"<b>Не удалось сохранить контакт:</b> {escape(str(exc))}\n\n"
            "Отправьте контакт через скрепку или строкой "
            "<code>+79991234567 | Алиса | Соколова</code>.",
            reply_markup=campaign_variant_input_cancel(campaign_id, page),
        )
        return
    await state.clear()
    await message.answer(
        (
            "✅ <b>Контакт обновлён.</b>\n\n"
            if edit_mode == "contact_edit"
            else "✅ <b>Контакт добавлен в ротацию.</b>\n\n"
        )
        + campaign_variant_text(campaign, variant),
        reply_markup=campaign_variant_menu(
            campaign,
            variant,
            variant_count=len(variants),
            page=page,
        ),
    )


@router.callback_query(F.data.startswith("campaign:variant:rename:"))
async def start_campaign_variant_rename(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, _, variant = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_id=variant.variant_id,
        variant_edit_mode="rename",
    )
    await state.set_state(CampaignVariantState.name)
    await _edit_callback(
        callback,
        f"<b>🏷 Название варианта</b>\n\nСейчас: <b>{escape(variant.name)}</b>. "
        "Отправьте новое название длиной до 80 символов.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:variant:edit:"))
async def start_campaign_variant_text_edit(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, _, variant = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_id=variant.variant_id,
        variant_edit_mode="text",
    )
    await state.set_state(CampaignVariantState.content)
    await _edit_callback(
        callback,
        f"<b>✏️ Текст · {escape(variant.name)}</b>\n\n"
        "Отправьте новый текст. Форматирование Telegram сохранится. Альбом останется "
        "без изменений; при наличии медиа подпись ограничена 1024 символами.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:variant:toggle:"))
async def toggle_campaign_variant(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        variant = await repository.toggle_campaign_content_variant(int(campaign_id), variant_id)
        campaign, variants, _ = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Вариант включён" if variant.enabled else "Вариант выключен")
    await _edit_callback(
        callback,
        campaign_variant_text(campaign, variant),
        campaign_variant_menu(campaign, variant, variant_count=len(variants), page=int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:variant:media:add:"))
async def start_campaign_variant_media_add(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, _, variant = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await state.update_data(
        variant_campaign_id=campaign.id,
        variant_campaign_page=int(page),
        variant_id=variant.variant_id,
    )
    await state.set_state(CampaignVariantState.media)
    await _edit_callback(
        callback,
        f"<b>🖼 Альбом · {escape(variant.name)}</b>\n\n"
        "Отправьте одно фото или видео. Повторите действие для каждого следующего "
        "файла. В альбоме может быть до 10 элементов. Документ доступен только как "
        "одиночный файл.",
        campaign_variant_input_cancel(campaign.id, int(page)),
    )


@router.message(CampaignVariantState.media)
async def receive_campaign_variant_media(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    bot: Bot,
    settings: Settings,
) -> None:
    data = await state.get_data()
    campaign_id = int(data.get("variant_campaign_id") or 0)
    page = int(data.get("variant_campaign_page") or 0)
    variant_id = str(data.get("variant_id") or "")
    try:
        media = await _extract_media_only(message, bot, settings)
        variant = await repository.add_campaign_variant_media(campaign_id, variant_id, media)
        campaign, variants, _ = await _campaign_variant_context(repository, campaign_id, variant_id)
    except (ContentError, ConflictError, NotFoundError) as exc:
        await message.answer(
            f"<b>Не удалось добавить медиа:</b> {escape(str(exc))}",
            reply_markup=campaign_variant_input_cancel(campaign_id, page),
        )
        return
    await state.clear()
    await message.answer(
        "✅ <b>Медиа добавлено в альбом.</b>\n\n" + campaign_variant_text(campaign, variant),
        reply_markup=campaign_variant_menu(
            campaign, variant, variant_count=len(variants), page=page
        ),
    )


@router.callback_query(F.data.startswith("campaign:variant:media:remove:"))
async def remove_campaign_variant_media(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, _, campaign_id, variant_id, media_index, page = (callback.data or "").split(":")
    try:
        variant = await repository.remove_campaign_variant_media(
            int(campaign_id), variant_id, int(media_index)
        )
        campaign, variants, _ = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Медиа убрано из альбома")
    await _edit_callback(
        callback,
        campaign_variant_text(campaign, variant),
        campaign_variant_menu(campaign, variant, variant_count=len(variants), page=int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:variant:delete:ask:"))
async def ask_delete_campaign_variant(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        campaign, _, variant = await _campaign_variant_context(
            repository, int(campaign_id), variant_id
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить вариант «{escape(variant.name)}»?</b>\n\n"
        "Он перестанет использоваться в новых рассылках. Уже созданные рассылки сохранят "
        "свой снимок контента и останутся неизменными.",
        campaign_variant_delete_confirmation(campaign.id, variant_id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:variant:delete:confirm:"))
async def confirm_delete_campaign_variant(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, _, campaign_id, variant_id, page = (callback.data or "").split(":")
    try:
        await repository.delete_campaign_content_variant(int(campaign_id), variant_id)
        campaign = await repository.get_campaign(int(campaign_id))
        variants = await repository.campaign_content_variants(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Вариант удалён")
    await _edit_callback(
        callback,
        campaign_variants_text(campaign, variants),
        campaign_variants_menu(campaign, variants, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:preview:"))
async def preview_campaign_variant(
    callback: CallbackQuery,
    repository: AdminRepository,
    bot: Bot,
    settings: Settings,
) -> None:
    _, _, variant_id, campaign_id, _page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        variants = await repository.campaign_content_variants(campaign.id)
        variant = variants[0] if variant_id == "primary" else _find_variant(variants, variant_id)
        await _send_variant_preview(bot, callback.from_user.id, variant, settings)
    except (ContentError, ConflictError, NotFoundError, TelegramBadRequest) as exc:
        await callback.answer(f"Предпросмотр не отправлен: {exc}", show_alert=True)
        return
    await callback.answer("Предпросмотр отправлен вам в этот чат")


@router.callback_query(F.data.startswith("campaign:preflight:"))
async def campaign_preflight(
    callback: CallbackQuery,
    repository: AdminRepository,
    preflight_service: PreflightService,
) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    campaign_id_value = int(campaign_id)
    page_value = int(page)
    try:
        campaign = await repository.get_campaign(campaign_id_value)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Проверка аккаунтов и чатов началась")
    await _edit_callback(
        callback,
        "<b>🩺 Проверяю готовность</b>\n\n"
        "Сессии, членство, право писать и таймеры проверяются без вступлений и отправок.",
        campaign_details_menu(campaign, page_value),
        answer=False,
    )
    try:
        summary = await preflight_service.inspect_campaign(campaign_id_value)
    except (ConflictError, NotFoundError) as exc:
        await _edit_callback(
            callback,
            f"<b>❌ Проверка не выполнена</b>\n\n{escape(str(exc))}",
            campaign_details_menu(campaign, page_value),
            answer=False,
        )
        return
    await _edit_callback(
        callback,
        campaign_preflight_text(summary),
        campaign_details_menu(campaign, page_value),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:sendermode:toggle:"))
async def toggle_campaign_sender_mode(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        current = await repository.get_campaign(int(campaign_id))
        selected = (
            BroadcastSenderMode.DISTRIBUTED
            if current.sender_mode == BroadcastSenderMode.INDEPENDENT
            else BroadcastSenderMode.INDEPENDENT
        )
        campaign = await repository.update_campaign_sender_mode(int(campaign_id), selected)
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    label = "независимые потоки" if selected == BroadcastSenderMode.INDEPENDENT else "распределение"
    await callback.answer(f"Режим аккаунтов: {label}")
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:mentions:toggle:"))
async def toggle_campaign_hidden_mentions(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        current = await repository.get_campaign(int(campaign_id))
        campaign = await repository.update_campaign_hidden_mentions(
            current.id,
            enabled=not current.hidden_mentions_enabled,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(
        "Отметки участников включены"
        if campaign.hidden_mentions_enabled
        else "Отметки участников выключены"
    )
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:mentions:cooldown:"))
async def campaign_mention_cooldown(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        "<b>🔄 Ротация отметок рассылки</b>\n\n"
        "Один и тот же участник не будет повторно отмечен этой рассылкой в одном "
        "и том же чате, пока не пройдёт выбранное число дней. Разные чаты ведут "
        "историю независимо.",
        campaign_mention_cooldown_menu(
            campaign.id,
            page=int(page),
            current_days=int(campaign.mention_cooldown_days),
        ),
    )


@router.callback_query(F.data.startswith("campaign:mentions:days:"))
async def set_campaign_mention_cooldown_preset(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, campaign_id, page, days = (callback.data or "").split(":")
    try:
        campaign = await repository.update_campaign_mention_cooldown_days(
            int(campaign_id),
            int(days),
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(
        f"Повторная отметка — не раньше чем через {campaign.mention_cooldown_days} дн."
    )
    await _edit_callback(
        callback,
        campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:mentions:custom:"))
async def begin_custom_campaign_mention_cooldown(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    await state.clear()
    await state.update_data(
        campaign_mention_campaign_id=int(campaign_id),
        campaign_mention_page=int(page),
    )
    await state.set_state(CampaignEditState.mention_cooldown)
    await _edit_callback(
        callback,
        "<b>Срок ротации отметок</b>\n\nОтправьте целое число дней от <b>1</b> до <b>365</b>.",
        cancel_menu(),
    )


@router.message(CampaignEditState.mention_cooldown)
async def custom_campaign_mention_cooldown(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    raw_value = (message.text or "").strip()
    if not raw_value.isdigit() or not 1 <= int(raw_value) <= 365:
        await message.answer("Укажите целое число дней от 1 до 365.")
        return
    data = await state.get_data()
    campaign_id = int(data["campaign_mention_campaign_id"])
    page = int(data["campaign_mention_page"])
    try:
        campaign = await repository.update_campaign_mention_cooldown_days(
            campaign_id,
            int(raw_value),
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await state.clear()
    await message.answer(
        campaign_details_text(campaign, sender_ids),
        reply_markup=campaign_details_menu(campaign, page),
    )


@router.callback_query(F.data.startswith("campaign:profile:"))
async def apply_campaign_profile(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, profile_key, campaign_id, page = (callback.data or "").split(":")
    try:
        profile = broadcast_profile(profile_key)
        campaign = await repository.update_campaign_smart_schedule(
            int(campaign_id),
            smart_duration_seconds=profile.duration_hours * 3600,
            smart_no_timer_interval_seconds=profile.interval_minutes * 60,
            auto_sender_failover=True,
            activity_aware=True,
            min_messages_between_posts=profile.min_messages_between_posts,
            activity_check_interval_seconds=profile.activity_check_interval_seconds,
            visibility_max_wait_seconds=profile.visibility_max_wait_seconds,
        )
        sender_ids = await repository.campaign_sender_ids(campaign.id)
    except (ValueError, ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Режим «{profile.title}» применён")
    await _edit_callback(
        callback,
        f"✅ <b>{profile.icon} {profile.title} рассылка настроена.</b>\n"
        f"{escape(profile.description)}.\n\n" + campaign_details_text(campaign, sender_ids),
        campaign_details_menu(campaign, int(page)),
        answer=False,
    )


@router.callback_query(F.data.regexp(r"^campaign:advanced:\d+:\d+$"))
async def campaign_advanced_settings(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not campaign.smart_timer_mode:
        await callback.answer("Точная настройка доступна для умного сценария", show_alert=True)
        return
    await state.clear()
    await _edit_callback(
        callback,
        campaign_advanced_text(campaign),
        campaign_advanced_menu(campaign, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:advanced:edit:"))
async def begin_campaign_advanced_edit(
    callback: CallbackQuery,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    _, _, _, field, campaign_id, page = (callback.data or "").split(":")
    specs = {
        "interval": (
            "⏱ Базовый интервал",
            "Отправьте число минут от 1 до 10080. Для активного режима по умолчанию — 5.",
        ),
        "duration": (
            "🗓 Срок работы",
            "Отправьте число часов от 1 до 168. По умолчанию активный режим работает 24 часа.",
        ),
        "depth": (
            "👁 Глубина видимости",
            "Отправьте число новых сообщений от 1 до 100000. Быстрое значение по умолчанию — 10.",
        ),
        "check": (
            "🔎 Частота проверки",
            "Отправьте число секунд от 30 до 3600. По умолчанию — 60 секунд.",
        ),
        "maxwait": (
            "⌛ Максимальное ожидание",
            "Отправьте число минут от 1 до 10080. Активный дедлайн по умолчанию — 15 минут.",
        ),
    }
    spec = specs.get(field)
    if spec is None:
        await callback.answer("Неизвестная настройка", show_alert=True)
        return
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await state.update_data(
        schedule_campaign_id=campaign.id,
        schedule_campaign_page=int(page),
        schedule_advanced_field=field,
    )
    await state.set_state(CampaignScheduleEditState.advanced_value)
    await _edit_callback(
        callback,
        f"<b>{spec[0]} · {escape(campaign.name)}</b>\n\n{spec[1]}",
        campaign_advanced_menu(campaign, int(page)),
    )


@router.message(CampaignScheduleEditState.advanced_value)
async def save_campaign_advanced_value(
    message: Message,
    repository: AdminRepository,
    state: FSMContext,
) -> None:
    data = await state.get_data()
    field = str(data.get("schedule_advanced_field") or "")
    campaign_id = int(data.get("schedule_campaign_id") or 0)
    page = int(data.get("schedule_campaign_page") or 0)
    try:
        value = int((message.text or "").strip())
    except ValueError:
        value = 0
    ranges = {
        "interval": (1, 10080, "минут"),
        "duration": (1, 168, "часов"),
        "depth": (1, 100000, "сообщений"),
        "check": (30, 3600, "секунд"),
        "maxwait": (1, 10080, "минут"),
    }
    minimum, maximum, unit = ranges.get(field, (1, 0, ""))
    if not minimum <= value <= maximum:
        await message.answer(f"Введите целое число {unit} от {minimum} до {maximum}.")
        return
    try:
        current = await repository.get_campaign(campaign_id)
        changes: dict[str, object] = {}
        duration = current.smart_duration_seconds
        interval = current.smart_no_timer_interval_seconds
        if field == "duration":
            duration = value * 3600
        elif field == "interval":
            interval = value * 60
        elif field == "depth":
            changes["min_messages_between_posts"] = value
        elif field == "check":
            changes["activity_check_interval_seconds"] = value
        elif field == "maxwait":
            changes["visibility_max_wait_seconds"] = value * 60
        campaign = await repository.update_campaign_smart_schedule(
            campaign_id,
            smart_duration_seconds=duration,
            smart_no_timer_interval_seconds=interval,
            **changes,
        )
    except (ConflictError, NotFoundError) as exc:
        await message.answer(f"Не удалось сохранить настройку: {escape(str(exc))}")
        return
    await state.clear()
    await message.answer(
        "✅ <b>Настройка сохранена.</b>\n\n" + campaign_advanced_text(campaign),
        reply_markup=campaign_advanced_menu(campaign, page),
    )


@router.callback_query(F.data.startswith("campaign:advanced:toggle:"))
async def toggle_campaign_advanced_setting(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, field, campaign_id, page = (callback.data or "").split(":")
    try:
        current = await repository.get_campaign(int(campaign_id))
        changes: dict[str, object] = {}
        if field == "activity":
            changes["activity_aware"] = not current.activity_aware
            if not current.activity_aware and current.min_messages_between_posts < 1:
                changes["min_messages_between_posts"] = broadcast_profile(
                    "active"
                ).min_messages_between_posts
        elif field == "failover":
            changes["auto_sender_failover"] = not current.auto_sender_failover
        else:
            await callback.answer("Неизвестная настройка", show_alert=True)
            return
        campaign = await repository.update_campaign_smart_schedule(
            current.id,
            smart_duration_seconds=current.smart_duration_seconds,
            smart_no_timer_interval_seconds=current.smart_no_timer_interval_seconds,
            **changes,
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Настройка изменена")
    await _edit_callback(
        callback,
        campaign_advanced_text(campaign),
        campaign_advanced_menu(campaign, int(page)),
        answer=False,
    )


def _campaign_reaction_step_count(campaign) -> int:
    from proxima_traffic.dialogues import parse_dialogue_steps  # noqa: PLC0415

    return len(
        parse_dialogue_steps(
            campaign.dialogue_steps_json,
            primary_account_id=campaign.primary_account_id,
            primary_text=campaign.primary_text,
            reply_account_id=campaign.reply_account_id,
            reply_text=campaign.reply_text,
            reply_delay_seconds=campaign.reply_delay_seconds,
        )
    )


@router.callback_query(F.data.regexp(r"^campaign:reactions:\d+:\d+$"))
async def campaign_reactions_settings(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        step_count = _campaign_reaction_step_count(campaign)
    except (NotFoundError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        campaign_reactions_text(campaign, step_count),
        campaign_reactions_menu(campaign, int(page), step_count),
    )


@router.callback_query(F.data.startswith("campaign:reactions:"))
async def update_campaign_reactions(callback: CallbackQuery, repository: AdminRepository) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) != 6:
        await callback.answer("Неизвестная настройка реакций", show_alert=True)
        return
    _, _, action, value, campaign_id, page = parts
    emojis = {"like": "👍", "fire": "🔥", "heart": "❤️", "bolt": "⚡"}
    try:
        current = await repository.get_campaign(int(campaign_id))
        if action == "toggle":
            campaign = await repository.update_campaign_reactions(
                current.id, enabled=not current.reactions_enabled
            )
        elif action == "emoji":
            campaign = await repository.update_campaign_reactions(current.id, emoji=emojis[value])
        elif action == "count":
            campaign = await repository.update_campaign_reactions(current.id, count=int(value))
        elif action == "step":
            campaign = await repository.update_campaign_reactions(
                current.id, dialogue_step=int(value)
            )
        else:
            await callback.answer("Неизвестная настройка реакций", show_alert=True)
            return
        step_count = _campaign_reaction_step_count(campaign)
    except (KeyError, ValueError, ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Настройка реакций сохранена")
    await _edit_callback(
        callback,
        campaign_reactions_text(campaign, step_count),
        campaign_reactions_menu(campaign, int(page), step_count),
        answer=False,
    )


@router.callback_query(F.data.startswith("campaign:delete:ask:"))
async def ask_delete_campaign(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🗑 Удалить «{escape(campaign.name)}»?</b>\n\n"
        "Удалить можно только рассылку, которая ещё ни разу не начиналась. "
        "История выполненных отправок не стирается.",
        campaign_delete_confirmation(campaign.id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:delete:confirm:"))
async def confirm_delete_campaign(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        await repository.delete_campaign(int(campaign_id))
    except (NotFoundError, ConflictError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Сценарий удалён")
    await _show_campaigns(callback, repository, int(page))


@router.callback_query(F.data.startswith("campaign:run:ask:"))
async def ask_enqueue_campaign(
    callback: CallbackQuery,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    try:
        campaign = await repository.get_campaign(int(campaign_id))
        sender_ids = await repository.campaign_sender_ids(campaign.id)
        target_count = None
        if campaign.target_list_id is not None:
            target_list = await repository.get_target_list_view(campaign.target_list_id)
            target_count = target_list.approved_count
            if target_count == 0:
                await callback.answer("В базе нет включённых чатов", show_alert=True)
                return
        elif campaign.target_id is not None:
            target = await repository.get_target(campaign.target_id)
            if not target.approved_by_operator:
                await callback.answer("Выбранный чат сейчас выключен", show_alert=True)
                return
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not campaign.enabled:
        await callback.answer("Эта рассылка остановлена. Сначала включите её.", show_alert=True)
        return
    account_factor = (
        len(sender_ids)
        if campaign.mode == CampaignMode.BROADCAST
        and campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
        else 1
    )
    planned_tasks = account_factor * (
        (target_count or 1)
        if campaign.smart_timer_mode
        else (target_count or 1) * campaign.repeat_count
    )
    if planned_tasks > 5000:
        await callback.answer(
            "Слишком много отправок: уменьшите число проходов или чатов до 5000",
            show_alert=True,
        )
        return
    capacity_warning: str | None = None
    if campaign.smart_timer_mode and target_count:
        sender_count = max(1, len(sender_ids))
        capacity = estimate_capacity(
            account_count=(
                1 if campaign.sender_mode == BroadcastSenderMode.INDEPENDENT else sender_count
            ),
            target_count=target_count,
            interval_seconds=campaign.smart_no_timer_interval_seconds,
            pace_seconds=settings.account_pace_max_seconds,
            daily_limit=settings.account_daily_limit,
            micro_break_every=settings.account_micro_break_every,
            micro_break_seconds=settings.account_micro_break_max_seconds,
        )
        if (
            capacity.full_round_seconds is not None
            and capacity.full_round_seconds > campaign.smart_no_timer_interval_seconds
        ):
            daily_line = (
                f" Реальная мощность — около {capacity.messages_per_day} публикаций в сутки, "
                f"а выбранная частота требует около {capacity.requested_per_day}."
                if capacity.messages_per_day is not None
                else ""
            )
            coverage_line = (
                f" Выполнимо примерно {capacity.coverage_ratio * 100:.1f}% плана."
                if capacity.coverage_ratio is not None
                else ""
            )
            capacity_warning = (
                "⚠️ <b>Текущего пула может не хватить для выбранной частоты.</b> "
                f"Полный круг по {target_count} чатам займёт примерно "
                f"{ceil(capacity.full_round_seconds / 60)} мин.{daily_line}{coverage_line} "
                "Увеличьте интервал, сократите аудиторию или используйте больше готовых "
                "аккаунтов."
            )
    await _edit_callback(
        callback,
        campaign_run_confirmation_text(
            campaign,
            sender_count=len(sender_ids),
            target_count=target_count,
            capacity_warning=capacity_warning,
        ),
        campaign_run_confirmation(campaign.id, int(page)),
    )


@router.callback_query(F.data.startswith("campaign:run:confirm:"))
async def enqueue_campaign(
    callback: CallbackQuery,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    _, _, _, campaign_id, page = (callback.data or "").split(":")
    launch_key = uuid4().hex
    try:
        runs = await run_queue.enqueue_campaign(int(campaign_id), launch_key)
        launch, launch_runs = await repository.get_launch(max(run.id for run in runs))
        targets = await repository.targets_by_ids({run.target_id for run in launch_runs})
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Отправка началась: направлений {len(runs)}")
    await _edit_callback(
        callback,
        "✅ <b>Рассылка поставлена в очередь.</b>\n\n"
        + launch_details_text(launch, launch_runs, targets),
        launch_details_menu(launch, 0),
        answer=False,
    )


@router.callback_query(F.data.startswith("launch:details:"))
async def launch_details(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, runs = await repository.get_launch(int(anchor_run_id))
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        launch_details_text(launch, runs, targets),
        launch_details_menu(launch, int(page)),
    )


@router.callback_query(F.data.startswith("launch:edit:text:"))
async def begin_active_launch_text_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, _ = await repository.get_launch(int(anchor_run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not launch.active:
        await callback.answer("В этой рассылке больше нет будущих отправок", show_alert=True)
        return
    await _remember_origin(callback, state)
    await state.update_data(
        edit_launch_anchor_id=launch.anchor_run_id,
        edit_launch_page=int(page),
    )
    await state.set_state(LaunchEditState.primary_text)
    await _edit_callback(
        callback,
        wizard_text(
            "✏️ Будущие посты активной рассылки",
            1,
            1,
            "Отправьте новый рекламный текст. Он попадёт только в ещё не "
            "ещё не отправленные сообщения; опубликованные посты и отчёт "
            "останутся без изменений.",
            hint="Если сообщение уже уходит, оно может завершиться со старым текстом.",
        ),
        launch_edit_cancel(launch.anchor_run_id, int(page)),
    )


@router.message(LaunchEditState.primary_text)
async def save_active_launch_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    value = (message.text or "").strip()
    if not 1 <= len(value) <= 4096:
        await message.answer("Текст должен содержать от 1 до 4096 символов.")
        return
    data = await state.get_data()
    anchor_run_id = int(data["edit_launch_anchor_id"])
    page = int(data["edit_launch_page"])
    try:
        count = await run_queue.update_active_launch(
            anchor_run_id,
            primary_text=value,
        )
        launch, runs = await repository.get_launch(anchor_run_id)
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await _edit_origin_data(
        bot,
        data,
        f"✅ <b>Новый текст применён к будущим отправкам: {count}.</b>\n\n"
        + launch_details_text(launch, runs, targets),
        launch_details_menu(launch, page),
    )
    await state.clear()


@router.callback_query(F.data.startswith("launch:edit:interval:"))
async def begin_active_launch_interval_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, _ = await repository.get_launch(int(anchor_run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not launch.active or not launch.smart_timer_mode:
        await callback.answer("Автоматическая рассылка уже завершена", show_alert=True)
        return
    await _remember_origin(callback, state)
    await state.update_data(
        edit_launch_anchor_id=launch.anchor_run_id,
        edit_launch_page=int(page),
    )
    await state.set_state(LaunchEditState.no_timer_interval_minutes)
    await _edit_callback(
        callback,
        wizard_text(
            "⏱ Частота активной рассылки",
            1,
            1,
            "Введите интервал в минутах для чатов без Telegram-таймера: "
            "от <b>1</b> до <b>10080</b>. Для чатов с таймером софт по-прежнему "
            "отправит сразу после разрешённого Telegram времени.",
            hint="Для активной рекламы по умолчанию используется 5 минут.",
        ),
        launch_edit_cancel(launch.anchor_run_id, int(page)),
    )


@router.message(LaunchEditState.no_timer_interval_minutes)
async def save_active_launch_interval(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    raw = (message.text or "").strip()
    try:
        minutes = int(raw)
    except ValueError:
        minutes = 0
    if not 1 <= minutes <= 10080:
        await message.answer("Введите целое число минут от 1 до 10080.")
        return
    data = await state.get_data()
    anchor_run_id = int(data["edit_launch_anchor_id"])
    page = int(data["edit_launch_page"])
    try:
        count = await run_queue.update_active_launch(
            anchor_run_id,
            no_timer_interval_seconds=minutes * 60,
        )
        launch, runs = await repository.get_launch(anchor_run_id)
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await _edit_origin_data(
        bot,
        data,
        f"✅ <b>Интервал изменён на {minutes} мин. для {count} будущих отправок.</b>\n\n"
        + launch_details_text(launch, runs, targets),
        launch_details_menu(launch, page),
    )
    await state.clear()


@router.callback_query(F.data.startswith("launch:edit:duration:"))
async def begin_active_launch_duration_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, _ = await repository.get_launch(int(anchor_run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not launch.active or not launch.smart_timer_mode:
        await callback.answer("Автоматическая рассылка уже завершена", show_alert=True)
        return
    await _remember_origin(callback, state)
    await state.update_data(
        edit_launch_anchor_id=launch.anchor_run_id,
        edit_launch_page=int(page),
    )
    await state.set_state(LaunchEditState.remaining_duration_hours)
    await _edit_callback(
        callback,
        wizard_text(
            "🗓 Время работы активной рассылки",
            1,
            1,
            "Сколько часов рассылке работать <b>с текущего момента</b>? "
            "Введите целое число от <b>1</b> до <b>168</b>.",
            hint="Например, 24 продлит или сократит работу рассылки до следующих суток.",
        ),
        launch_edit_cancel(launch.anchor_run_id, int(page)),
    )


@router.message(LaunchEditState.remaining_duration_hours)
async def save_active_launch_duration(
    message: Message,
    state: FSMContext,
    bot: Bot,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    raw = (message.text or "").strip()
    try:
        hours = int(raw)
    except ValueError:
        hours = 0
    if not 1 <= hours <= 168:
        await message.answer("Введите целое число часов от 1 до 168.")
        return
    data = await state.get_data()
    anchor_run_id = int(data["edit_launch_anchor_id"])
    page = int(data["edit_launch_page"])
    try:
        count = await run_queue.update_active_launch(
            anchor_run_id,
            remaining_duration_seconds=hours * 3600,
        )
        launch, runs = await repository.get_launch(anchor_run_id)
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await _edit_origin_data(
        bot,
        data,
        f"✅ <b>Рассылка будет активна ещё {hours} ч.; обновлено отправок: {count}.</b>\n\n"
        + launch_details_text(launch, runs, targets),
        launch_details_menu(launch, page),
    )
    await state.clear()


@router.callback_query(F.data.startswith("launch:edit:cancel:"))
async def cancel_active_launch_edit(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    await state.clear()
    try:
        launch, runs = await repository.get_launch(int(anchor_run_id))
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        launch_details_text(launch, runs, targets),
        launch_details_menu(launch, int(page)),
    )


@router.callback_query(F.data.startswith("launch:archive:"))
async def archive_launch(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        count = await repository.set_launch_archived(int(anchor_run_id), True)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Рассылка убрана в архив · отправок {count}")
    await _show_runs(callback, repository, int(page))


@router.callback_query(F.data.startswith("launch:restore:"))
async def restore_launch(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        count = await repository.set_launch_archived(int(anchor_run_id), False)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Рассылка возвращена · отправок {count}")
    await _show_archived_runs(callback, repository, int(page))


@router.callback_query(F.data.startswith("launch:cancel:ask:"))
async def ask_cancel_launch(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, _ = await repository.get_launch(int(anchor_run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not launch.pending_work:
        await callback.answer("Партия уже завершена", show_alert=True)
        return
    await _edit_callback(
        callback,
        launch_cancel_confirmation_text(launch),
        launch_cancel_confirmation(launch.anchor_run_id, int(page)),
    )


@router.callback_query(F.data.startswith("launch:cancel:confirm:"))
async def confirm_cancel_launch(
    callback: CallbackQuery,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        count = await run_queue.cancel_launch(int(anchor_run_id))
        launch, runs = await repository.get_launch(int(anchor_run_id))
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Остановка запрошена для отправок: {count}")
    await _edit_callback(
        callback,
        launch_details_text(launch, runs, targets),
        launch_details_menu(launch, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("launch:retry:ask:"))
async def ask_retry_failed_launch(
    callback: CallbackQuery,
    repository: AdminRepository,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        launch, _ = await repository.get_launch(int(anchor_run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if not launch.failed:
        await callback.answer("Ошибок для повтора нет", show_alert=True)
        return
    await _edit_callback(
        callback,
        f"<b>🔁 Повторить {launch.failed} неотправленных?</b>\n\n"
        "Успешные чаты не затрагиваются. Перед повтором исправьте причину ошибки: "
        "прокси, доступ аккаунта или ограничения чата.",
        launch_retry_confirmation(launch.anchor_run_id, int(page)),
    )


@router.callback_query(F.data.startswith("launch:retry:confirm:"))
async def confirm_retry_failed_launch(
    callback: CallbackQuery,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    _, _, _, anchor_run_id, page = (callback.data or "").split(":")
    try:
        count = await run_queue.retry_failed_launch(int(anchor_run_id))
        launch, runs = await repository.get_launch(int(anchor_run_id))
        targets = await repository.targets_by_ids({run.target_id for run in runs})
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    if count == 0:
        await callback.answer("Ошибок для повтора уже нет", show_alert=True)
        return
    await callback.answer(f"Возвращено в очередь: {count}")
    await _edit_callback(
        callback,
        "✅ <b>Неотправленные чаты возвращены в очередь.</b>\n\n"
        + launch_details_text(launch, runs, targets),
        launch_details_menu(launch, int(page)),
        answer=False,
    )


@router.callback_query(F.data.startswith("run:details:"))
async def run_details(callback: CallbackQuery, repository: AdminRepository) -> None:
    _, _, run_id, page = (callback.data or "").split(":")
    try:
        run, actions = await repository.get_run(int(run_id))
        target = await repository.get_target(run.target_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        run_details_text(run, actions, target),
        run_details_menu(run, int(page)),
    )


@router.callback_query(F.data.startswith("run:cancel:"))
async def cancel_run(
    callback: CallbackQuery,
    repository: AdminRepository,
    run_queue: RunQueue,
) -> None:
    _, _, run_id, page = (callback.data or "").split(":")
    try:
        run = await run_queue.cancel(int(run_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(f"Отправка #{run.id}: {run.status.value}")
    await _show_runs(callback, repository, int(page))


@router.callback_query(
    ChannelCommentCreateState.chain_account,
    F.data == "comments:chain:add",
)
async def channel_comment_chain_add(
    callback: CallbackQuery,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts, _ = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        "<b>Следующий шаг</b>\n\nВыберите аккаунт, который ответит на предыдущий комментарий.",
        channel_comment_account_picker(
            accounts,
            callback_prefix="comments:pick:chain",
            back_callback="comments:chain:show",
            pager_callback_prefix="comments:picker:chain",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.chain_account,
    F.data.startswith("comments:picker:chain:"),
)
async def page_channel_comment_chain_account(
    callback: CallbackQuery,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        page = max(0, int((callback.data or "").rsplit(":", 1)[1]))
    except (IndexError, ValueError):
        await callback.answer("Некорректная страница", show_alert=True)
        return
    accounts, _ = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        "<b>Следующий шаг</b>\n\nВыберите аккаунт, который ответит на предыдущий комментарий.",
        channel_comment_account_picker(
            accounts,
            callback_prefix="comments:pick:chain",
            back_callback="comments:chain:show",
            page=page,
            pager_callback_prefix="comments:picker:chain",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.chain_account,
    F.data == "comments:chain:show",
)
async def channel_comment_chain_show(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, labels = await _comment_accounts(repository, settings)
    data = await state.get_data()
    await _edit_callback(
        callback,
        channel_comment_draft_text(data, labels),
        channel_comment_chain_menu(len(data.get("comment_steps", []))),
    )


@router.callback_query(
    ChannelCommentCreateState.chain_account,
    F.data.startswith("comments:pick:chain:"),
)
async def channel_comment_chain_account(callback: CallbackQuery, state: FSMContext) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    await state.update_data(chain_account_id=account_id)
    await state.set_state(ChannelCommentCreateState.chain_text)
    await _edit_callback(
        callback,
        "Отправьте текст, фото, видео, документ, голосовое или видеокружок. "
        "Это будет ответом на предыдущий комментарий; у кружка подписи быть не может.",
        cancel_menu(),
    )


@router.message(ChannelCommentCreateState.chain_text)
async def channel_comment_chain_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
) -> None:
    try:
        text, _html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(str(exc))
        return
    await state.update_data(
        chain_text=text,
        chain_media_path=media.path if media else None,
        chain_media_kind=media.kind if media else None,
        chain_media_name=media.name if media else None,
        chain_media_size=media.size if media else None,
    )
    await state.set_state(ChannelCommentCreateState.chain_delay)
    await message.answer(
        "Через сколько после прошлого комментария отправить этот ответ?\n"
        "Примеры: <code>30s</code>, <code>5m</code>, <code>1h</code>.",
        reply_markup=cancel_menu(),
    )


@router.message(ChannelCommentCreateState.chain_delay)
async def channel_comment_chain_delay(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        delay = _channel_comment_delay(message.text or "")
    except ValueError as exc:
        await message.answer(str(exc))
        return
    data = await state.get_data()
    steps = [dict(item) for item in data.get("comment_steps", []) if isinstance(item, dict)]
    if len(steps) >= 20:
        await message.answer("В цепочке уже максимум 20 сообщений.")
        return
    steps.append(
        {
            "account_id": int(data["chain_account_id"]),
            "text": str(data["chain_text"]),
            "delay_seconds": delay,
            "reply_to_previous": True,
            "media_path": (
                str(data["chain_media_path"]) if data.get("chain_media_path") else None
            ),
            "media_kind": (
                str(data["chain_media_kind"]) if data.get("chain_media_kind") else None
            ),
            "media_name": (
                str(data["chain_media_name"]) if data.get("chain_media_name") else None
            ),
            "media_size": (
                int(data["chain_media_size"])
                if data.get("chain_media_size") is not None
                else None
            ),
        }
    )
    await state.update_data(comment_steps=steps)
    await state.set_state(ChannelCommentCreateState.chain_account)
    _, labels = await _comment_accounts(repository, settings)
    await message.answer(
        channel_comment_draft_text(await state.get_data(), labels),
        reply_markup=channel_comment_chain_menu(len(steps)),
    )


@router.callback_query(
    ChannelCommentCreateState.chain_account,
    F.data == "comments:chain:finish",
)
async def channel_comment_chain_finish(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if len(data.get("comment_steps", [])) < 2:
        await callback.answer("Добавьте хотя бы один ответ к первому комментарию.", show_alert=True)
        return
    await state.set_state(ChannelCommentCreateState.initial_delay)
    await _edit_callback(
        callback,
        "<b>Задержка первого комментария</b>\n\n"
        "Через сколько после нового поста начать всю цепочку?\n"
        "Примеры: <code>0</code>, <code>30s</code>, <code>5m</code>, <code>2h</code>.",
        cancel_menu(),
    )


@router.message(ChannelCommentCreateState.initial_delay)
async def channel_comment_initial_delay(
    message: Message,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        delay = _channel_comment_delay(message.text or "")
    except ValueError as exc:
        await message.answer(str(exc))
        return
    await state.update_data(initial_delay_seconds=delay)
    _, labels = await _comment_accounts(repository, settings)
    await state.set_state(ChannelCommentCreateState.confirm)
    await message.answer(
        channel_comment_draft_text(await state.get_data(), labels)
        + (
            "\n\n<i>После включения сервис не возьмёт старые посты. "
            "Он начнёт только со следующих.</i>"
        ),
        reply_markup=channel_comment_confirmation_menu(),
    )


@router.callback_query(
    ChannelCommentCreateState.confirm,
    F.data == "comments:create:confirm",
)
async def create_channel_comment_rule(
    callback: CallbackQuery,
    state: FSMContext,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    data = await state.get_data()
    mode = ChannelCommentMode(str(data["comment_mode"]))
    try:
        steps = [
            DialogueStep(**item) for item in data.get("comment_steps", []) if isinstance(item, dict)
        ]
        rule = await channel_comments.create_rule(
            name=str(data["comment_name"]),
            channel_ref=str(data["channel_ref"]),
            mode=mode,
            primary_account_id=int(data["primary_account_id"]),
            primary_text=str(data["primary_text"]),
            initial_delay_seconds=int(data.get("initial_delay_seconds", 0)),
            primary_media_path=(
                str(data["primary_media_path"]) if data.get("primary_media_path") else None
            ),
            primary_media_kind=(
                str(data["primary_media_kind"]) if data.get("primary_media_kind") else None
            ),
            primary_media_name=(
                str(data["primary_media_name"]) if data.get("primary_media_name") else None
            ),
            primary_media_size=(
                int(data["primary_media_size"])
                if data.get("primary_media_size") is not None
                else None
            ),
            dialogue_steps=steps if mode == ChannelCommentMode.DIALOGUE else None,
            message_edit_initial_text=(
                str(data["message_edit_initial_text"])
                if mode == ChannelCommentMode.MESSAGE_EDIT
                else None
            ),
            message_edit_delay_seconds=(
                int(data["message_edit_delay_seconds"])
                if mode == ChannelCommentMode.MESSAGE_EDIT
                else None
            ),
        )
    except (ConflictError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Канал включён: новые посты будут отслеживаться")
    await _show_channel_comment_details(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=rule.id,
        page=0,
    )


@router.callback_query(F.data.startswith("comments:details:"))
async def channel_comment_details(
    callback: CallbackQuery,
    state: FSMContext,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    await state.clear()
    _, _, rule_id, page = (callback.data or "").split(":")
    try:
        await _show_channel_comment_details(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.startswith("comments:mentions:toggle:"))
async def toggle_channel_comment_hidden_mentions(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        current = await channel_comments.get_rule_view(int(rule_id))
        rule = await channel_comments.set_rule_hidden_mentions_enabled(
            current.rule.id,
            not current.rule.hidden_mentions_enabled,
        )
        await callback.answer(
            "Отметки участников включены"
            if rule.hidden_mentions_enabled
            else "Отметки участников выключены"
        )
        await _show_channel_comment_details(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=rule.id,
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.startswith("comments:mentions:cooldown:"))
async def channel_comment_mention_cooldown(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        view = await channel_comments.get_rule_view(int(rule_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        "<b>🔄 Ротация отметок</b>\n\n"
        "Один и тот же участник не будет повторно отмечен в этом канале, пока "
        "не пройдёт выбранное число дней. Уже зарезервированные комментарии сохранят "
        "свой безопасный список.",
        channel_comment_mention_cooldown_menu(
            view.rule.id,
            page=int(page),
            current_days=int(view.rule.mention_cooldown_days),
        ),
    )


@router.callback_query(F.data.startswith("comments:mentions:days:"))
async def set_channel_comment_mention_cooldown_preset(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page, days = (callback.data or "").split(":")
    try:
        rule = await channel_comments.set_rule_mention_cooldown_days(
            int(rule_id),
            int(days),
        )
        await _show_channel_comment_details(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=rule.id,
            page=int(page),
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer(
        f"Повторная отметка — не раньше чем через {rule.mention_cooldown_days} дн."
    )


@router.callback_query(F.data.startswith("comments:mentions:custom:"))
async def begin_custom_channel_comment_mention_cooldown(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    await state.clear()
    await state.update_data(
        comment_mention_rule_id=int(rule_id),
        comment_mention_page=int(page),
    )
    await state.set_state(ChannelCommentCreateState.mention_cooldown)
    await _edit_callback(
        callback,
        "<b>Срок ротации отметок</b>\n\nОтправьте целое число дней от <b>1</b> до <b>365</b>.",
        cancel_menu(),
    )


@router.message(ChannelCommentCreateState.mention_cooldown)
async def custom_channel_comment_mention_cooldown(
    message: Message,
    state: FSMContext,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    raw_value = (message.text or "").strip()
    if not raw_value.isdigit() or not 1 <= int(raw_value) <= 365:
        await message.answer("Укажите целое число дней от 1 до 365.")
        return
    data = await state.get_data()
    rule_id = int(data["comment_mention_rule_id"])
    page = int(data["comment_mention_page"])
    try:
        await channel_comments.set_rule_mention_cooldown_days(rule_id, int(raw_value))
        view = await channel_comments.get_rule_view(rule_id)
    except (ConflictError, NotFoundError) as exc:
        await message.answer(str(exc))
        return
    await state.clear()
    _, labels = await _comment_accounts(repository, settings)
    await message.answer(
        channel_comment_details_text(view, labels),
        reply_markup=channel_comment_details_menu(view, page=page),
    )


@router.callback_query(F.data.startswith("comments:toggle:"))
async def toggle_channel_comment_rule(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    try:
        current = await channel_comments.get_rule_view(int(rule_id))
        await channel_comments.set_rule_enabled(int(rule_id), not current.rule.enabled)
        await _show_channel_comment_details(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.startswith("comments:check:"))
async def check_channel_comment_rule(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    await _safe_callback_answer(callback, "Проверяю канал через Telegram…")
    try:
        await channel_comments.preflight_rule(int(rule_id))
        await channel_comments.scan_rule(int(rule_id))
        await _show_channel_comment_details(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except (ConflictError, NotFoundError) as exc:
        if isinstance(callback.message, Message):
            await callback.message.answer(f"Проверка канала не выполнена: {exc}")


@router.callback_query(F.data.startswith("comments:account:replace:"))
async def begin_replace_channel_comment_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    accounts, _ = await _comment_accounts(repository, settings)
    if not accounts:
        await callback.answer("Нет активных аккаунтов для замены", show_alert=True)
        return
    await state.clear()
    await state.update_data(comment_replace_rule_id=int(rule_id), comment_replace_page=int(page))
    await state.set_state(ChannelCommentCreateState.replace_primary_account)
    await _edit_callback(
        callback,
        "<b>Сменить первый аккаунт</b>\n\n"
        "Новый аккаунт будет писать первый комментарий. Неначатые посты текущей очереди "
        "безопасно отменятся, а сервис продолжит только со следующих постов.",
        channel_comment_account_picker(
            accounts,
            callback_prefix="comments:account:select",
            back_callback=f"comments:details:{rule_id}:{page}",
            pager_callback_prefix="comments:picker:replace",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.replace_primary_account,
    F.data.startswith("comments:picker:replace:"),
)
async def page_replace_channel_comment_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    try:
        account_page = max(0, int((callback.data or "").rsplit(":", 1)[1]))
        data = await state.get_data()
        rule_id = int(data["comment_replace_rule_id"])
        rule_page = int(data["comment_replace_page"])
    except (IndexError, KeyError, TypeError, ValueError):
        await state.clear()
        await callback.answer("Черновик замены устарел. Откройте правило заново.", show_alert=True)
        return
    accounts, _ = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        "<b>Сменить первый аккаунт</b>\n\n"
        "Новый аккаунт будет писать первый комментарий. Неначатые посты текущей очереди "
        "безопасно отменятся, а сервис продолжит только со следующих постов.",
        channel_comment_account_picker(
            accounts,
            callback_prefix="comments:account:select",
            back_callback=f"comments:details:{rule_id}:{rule_page}",
            page=account_page,
            pager_callback_prefix="comments:picker:replace",
        ),
    )


@router.callback_query(
    ChannelCommentCreateState.replace_primary_account,
    F.data.startswith("comments:account:select:"),
)
async def replace_channel_comment_account(
    callback: CallbackQuery,
    state: FSMContext,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[1])
    data = await state.get_data()
    rule_id = int(data["comment_replace_rule_id"])
    page = int(data["comment_replace_page"])
    try:
        await channel_comments.replace_primary_account(rule_id, account_id)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await state.clear()
    await callback.answer("Первый аккаунт заменён")
    await _show_channel_comment_details(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=rule_id,
        page=page,
    )


@router.callback_query(F.data.startswith("comments:reactions:open:"))
async def open_channel_comment_reactions(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        await _show_channel_comment_reactions(
            callback,
            channel_comments,
            repository,
            settings,
            rule_id=int(rule_id),
            page=int(page),
        )
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)


@router.callback_query(F.data.startswith("comments:reactions:toggle:"))
async def toggle_channel_comment_reactions(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        view = await channel_comments.get_rule_view(int(rule_id))
        await channel_comments.update_rule_reactions(
            int(rule_id),
            enabled=not view.rule.reactions_enabled,
        )
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _show_channel_comment_reactions(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=int(rule_id),
        page=int(page),
    )


@router.callback_query(F.data.startswith("comments:reactions:emoji:"))
async def set_channel_comment_reaction_emoji(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page, key = (callback.data or "").split(":")
    emoji = {"like": "👍", "fire": "🔥", "heart": "❤️", "bolt": "⚡"}.get(key)
    if emoji is None:
        await callback.answer("Неизвестная реакция", show_alert=True)
        return
    try:
        await channel_comments.update_rule_reactions(int(rule_id), emoji=emoji)
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _show_channel_comment_reactions(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=int(rule_id),
        page=int(page),
    )


@router.callback_query(F.data.startswith("comments:reactions:count:"))
async def set_channel_comment_reaction_count(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page, raw_count = (callback.data or "").split(":")
    try:
        await channel_comments.update_rule_reactions(int(rule_id), count=int(raw_count))
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _show_channel_comment_reactions(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=int(rule_id),
        page=int(page),
    )


@router.callback_query(F.data.startswith("comments:reactions:step:"))
async def cycle_channel_comment_reaction_step(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) not in {5, 6}:
        await callback.answer("Неизвестный шаг комментария", show_alert=True)
        return
    _, _, _, rule_id, page, *selected = parts
    try:
        view = await channel_comments.get_rule_view(int(rule_id))
        rule = view.rule
        steps = parse_dialogue_steps(
            rule.dialogue_steps_json,
            primary_account_id=rule.primary_account_id,
            primary_text=rule.primary_text,
            reply_account_id=None,
            reply_text="",
            reply_delay_seconds=0,
        )
        next_step = (
            int(selected[0]) if selected else 1 + (int(rule.reaction_dialogue_step) % len(steps))
        )
        await channel_comments.update_rule_reactions(
            int(rule_id),
            dialogue_step=next_step,
        )
    except (ConflictError, NotFoundError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await _show_channel_comment_reactions(
        callback,
        channel_comments,
        repository,
        settings,
        rule_id=int(rule_id),
        page=int(page),
    )


@router.callback_query(F.data.startswith("comments:reactions:accounts:"))
async def open_channel_comment_reaction_accounts(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page, account_page = (callback.data or "").split(":")
    try:
        view = await channel_comments.get_rule_view(int(rule_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    accounts, _ = await _comment_accounts(repository, settings)
    selected = channel_comment_reaction_account_ids(view)
    await _edit_callback(
        callback,
        "<b>👥 Аккаунты для реакций</b>\n\n"
        "Выберите аккаунты, которые могут ставить реакции на собственный комментарий. "
        "Автор конкретного комментария будет исключён автоматически.",
        channel_comment_reaction_account_picker(
            accounts,
            selected,
            rule_id=int(rule_id),
            page=int(page),
            account_page=int(account_page),
        ),
        answer=False,
    )


@router.callback_query(F.data.startswith("comments:reactions:account:"))
async def toggle_channel_comment_reaction_account(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    _, _, _, rule_id, page, account_page, account_id = (callback.data or "").split(":")
    try:
        view = await channel_comments.get_rule_view(int(rule_id))
        selected = channel_comment_reaction_account_ids(view)
        if int(account_id) in selected:
            selected.remove(int(account_id))
        else:
            selected.add(int(account_id))
        await channel_comments.update_rule_reactions(
            int(rule_id),
            account_ids=sorted(selected),
        )
        refreshed = await channel_comments.get_rule_view(int(rule_id))
    except (ConflictError, NotFoundError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    accounts, _ = await _comment_accounts(repository, settings)
    await _edit_callback(
        callback,
        "<b>👥 Аккаунты для реакций</b>\n\n"
        "Выберите аккаунты, которые могут ставить реакции на собственный комментарий. "
        "Автор конкретного комментария будет исключён автоматически.",
        channel_comment_reaction_account_picker(
            accounts,
            channel_comment_reaction_account_ids(refreshed),
            rule_id=int(rule_id),
            page=int(page),
            account_page=int(account_page),
        ),
        answer=False,
    )


@router.callback_query(F.data.startswith("comments:runs:"))
async def channel_comment_runs(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    runs = await channel_comments.recent_runs(int(rule_id))
    await _edit_callback(
        callback,
        channel_comment_runs_text(runs),
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="‹ К каналу", callback_data=f"comments:details:{rule_id}:{page}"
                    )
                ]
            ]
        ),
    )


@router.callback_query(F.data.startswith("comments:delete:confirm:"))
async def confirm_delete_channel_comment_rule(
    callback: CallbackQuery,
    channel_comments: ChannelCommentService,
) -> None:
    _, _, _, rule_id, page = (callback.data or "").split(":")
    try:
        await channel_comments.delete_rule(int(rule_id))
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("Канал архивирован; история сохранена")
    await _show_channel_comments(callback, channel_comments, page=int(page))


@router.callback_query(F.data.startswith("comments:delete:"))
async def delete_channel_comment_rule(callback: CallbackQuery) -> None:
    _, _, rule_id, page = (callback.data or "").split(":")
    await _edit_callback(
        callback,
        "<b>Удалить канал?</b>\n\n"
        "Правило будет безопасно архивировано, очередь отменена, а история сохранена. "
        "Если комментарий уже отправляется, сервис зафиксирует его реальный результат.",
        channel_comment_delete_menu(int(rule_id), page=int(page)),
    )


async def _show_direct_messages_home(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    dashboard = await direct_messages.dashboard()
    await _edit_callback(
        callback,
        direct_messages_home_text(dashboard),
        direct_messages_home_menu(),
        answer=False,
    )


@router.callback_query(F.data == "dm:home")
async def open_direct_messages_home(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    await state.clear()
    await _show_direct_messages_home(callback, direct_messages)


@router.callback_query(F.data == "dm:import")
async def begin_subscriber_import(
    callback: CallbackQuery,
    state: FSMContext,
    settings: Settings,
) -> None:
    await state.clear()
    await state.set_state(SubscriberImportState.document)
    await _edit_callback(
        callback,
        subscriber_import_prompt_text(settings.direct_messages_upload_max_bytes),
        direct_cancel_menu(),
    )


@router.callback_query(F.data == "dm:blacklist")
async def open_direct_blacklist(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    await state.clear()
    dashboard = await direct_messages.dashboard()
    await _edit_callback(
        callback,
        direct_blacklist_text(dashboard.blacklist_count),
        direct_blacklist_menu(),
    )


@router.callback_query(F.data == "dm:blacklist:add")
async def begin_direct_blacklist_add(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(DirectBlacklistState.ids)
    await _edit_callback(
        callback,
        direct_blacklist_add_prompt_text(),
        direct_cancel_menu(),
    )


@router.message(DirectBlacklistState.ids)
async def add_direct_blacklist_ids(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    try:
        telegram_user_ids = parse_direct_blacklist_ids(message.text or "")
        result = await direct_messages.add_blacklist_entries(telegram_user_ids)
    except (ValueError, ConflictError) as exc:
        await message.answer(escape(str(exc)), reply_markup=direct_cancel_menu())
        return
    await state.clear()
    dashboard = await direct_messages.dashboard()
    cancelled = (
        f"\nОстановлено ожидавших заданий: <b>{result.cancelled_tasks:,}</b>"
        if result.cancelled_tasks
        else ""
    )
    await message.answer(
        "<b>✅ Стоп-лист обновлён</b>\n\n"
        f"Добавлено новых ID: <b>{result.added:,}</b>\n"
        f"Уже были запрещены: <b>{result.already_present:,}</b>"
        f"{cancelled}\n"
        f"Всего запрещено: <b>{dashboard.blacklist_count:,}</b>",
        reply_markup=direct_blacklist_menu(),
    )


@router.message(SubscriberImportState.document)
async def import_subscriber_document(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
    direct_messages: DirectMessageService,
) -> None:
    document = message.document
    file_name = Path(document.file_name or "subscribers.jsonl").name if document else ""
    if document is None or not subscriber_file_supported(file_name):
        await message.answer(
            "Отправьте документ <code>.jsonl</code>, <code>.jsonl.gz</code> или "
            "<code>.zip</code> с JSONL-частями.",
            reply_markup=direct_cancel_menu(),
        )
        return
    if (
        document.file_size is not None
        and document.file_size > settings.direct_messages_upload_max_bytes
    ):
        await message.answer(
            "Файл больше настроенного лимита загрузки. Сожмите JSONL в GZIP или ZIP "
            "без изменения его строк.",
            reply_markup=direct_cancel_menu(),
        )
        return
    progress = await message.answer(
        "⏳ Загружаю и проверяю базу построчно. Большой файл может занять несколько минут."
    )
    try:
        with TemporaryDirectory(prefix="proxima-subscribers-") as temporary_directory:
            destination = Path(temporary_directory) / file_name
            await bot.download(document, destination=destination)
            result = await direct_messages.import_subscribers(
                destination,
                list_name=subscriber_list_name(file_name),
                source_filename=file_name,
            )
    except (SubscriberImportError, ConflictError, OSError) as exc:
        await progress.edit_text(
            f"<b>Не удалось импортировать базу</b>\n\n{escape(str(exc))}",
            reply_markup=direct_cancel_menu(),
        )
        return
    await state.clear()
    await progress.edit_text(
        subscriber_import_result_text(result),
        reply_markup=subscriber_import_result_menu(int(result.subscriber_list.id)),
    )


@router.callback_query(F.data.startswith("dm:lists:"))
async def open_subscriber_lists(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    items, total = await direct_messages.list_subscriber_lists(page, DIRECT_MESSAGE_PAGE_SIZE)
    last_page = max(0, (total - 1) // DIRECT_MESSAGE_PAGE_SIZE)
    if page > last_page:
        page = last_page
        items, total = await direct_messages.list_subscriber_lists(page, DIRECT_MESSAGE_PAGE_SIZE)
    await _edit_callback(
        callback,
        subscriber_lists_text(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        subscriber_lists_menu(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("dm:list:"))
async def open_subscriber_list(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_list_id, raw_page = (callback.data or "").split(":")
    item = await direct_messages.get_subscriber_list(int(raw_list_id))
    await _edit_callback(
        callback,
        subscriber_list_details_text(item),
        subscriber_list_details_menu(item, int(raw_page)),
    )


@router.callback_query(F.data.startswith("dm:listdel:"))
async def ask_delete_subscriber_list(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_list_id, raw_page = (callback.data or "").split(":")
    item = await direct_messages.get_subscriber_list(int(raw_list_id))
    await _edit_callback(
        callback,
        "<b>Удалить базу подписчиков?</b>\n\n"
        f"База <b>{escape(item.name)}</b> содержит {item.member_count:,} записей. "
        "Общие профили, которые используются другими базами, сохранятся.",
        subscriber_list_delete_menu(item.id, int(raw_page)),
    )


@router.callback_query(F.data.startswith("dm:listdelok:"))
async def confirm_delete_subscriber_list(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_list_id, raw_page = (callback.data or "").split(":")
    try:
        await direct_messages.delete_subscriber_list(int(raw_list_id))
    except ConflictError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    await callback.answer("База удалена")
    page = int(raw_page)
    items, total = await direct_messages.list_subscriber_lists(page, DIRECT_MESSAGE_PAGE_SIZE)
    await _edit_callback(
        callback,
        subscriber_lists_text(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        subscriber_lists_menu(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        answer=False,
    )


@router.callback_query(F.data == "dm:new")
async def begin_direct_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    await state.clear()
    await _show_direct_list_picker(callback, direct_messages, page=0)


async def _show_direct_list_picker(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
    *,
    page: int,
) -> None:
    """Show a bounded page so a large operator account stays within Bot API limits."""

    page = max(0, page)
    lists, total = await direct_messages.list_ready_subscriber_lists(
        page, DIRECT_MESSAGE_PICKER_PAGE_SIZE
    )
    last_page = max(0, (total - 1) // DIRECT_MESSAGE_PICKER_PAGE_SIZE)
    if page > last_page:
        page = last_page
        lists, total = await direct_messages.list_ready_subscriber_lists(
            page, DIRECT_MESSAGE_PICKER_PAGE_SIZE
        )
    if total == 0:
        await callback.answer("Сначала импортируйте базу подписчиков", show_alert=True)
        return
    await _edit_callback(
        callback,
        direct_list_picker_text(total, page, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
        direct_list_picker(lists, total, page, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("dm:new:lists:"))
async def page_direct_subscriber_lists(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    await _show_direct_list_picker(callback, direct_messages, page=page)


@router.callback_query(F.data.startswith("dm:new:list:"))
async def select_direct_subscriber_list(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    list_id = int((callback.data or "").rsplit(":", 1)[-1])
    item = await direct_messages.get_subscriber_list(list_id)
    if item.status.value != "ready" or item.member_count <= 0:
        await callback.answer("Эта база пока не готова", show_alert=True)
        return
    await state.clear()
    await state.update_data(direct_list_id=list_id)
    await _edit_callback(callback, direct_segment_text(item), direct_segment_menu(list_id))


@router.callback_query(F.data.startswith("dm:new:seg:"))
async def select_direct_segment(callback: CallbackQuery, state: FSMContext) -> None:
    parts = (callback.data or "").split(":")
    list_id = int(parts[3])
    try:
        segment = SubscriberSegment(parts[4])
    except ValueError:
        await callback.answer("Неизвестный сегмент", show_alert=True)
        return
    await state.update_data(direct_list_id=list_id, direct_segment=segment.value)
    await state.set_state(DirectCampaignCreateState.limit)
    await _edit_callback(callback, direct_limit_text(segment), direct_cancel_menu())


@router.message(DirectCampaignCreateState.limit)
async def set_direct_recipient_limit(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    data = await state.get_data()
    list_id = int(data.get("direct_list_id") or 0)
    segment = SubscriberSegment(str(data.get("direct_segment") or "all"))
    item = await direct_messages.get_subscriber_list(list_id)
    raw = (message.text or "").strip().casefold()
    if raw in {"все", "all"}:
        limit = {
            SubscriberSegment.ALL: item.member_count,
            SubscriberSegment.PREMIUM: item.premium_count,
            SubscriberSegment.NON_PREMIUM: item.non_premium_count,
        }[segment]
    else:
        try:
            limit = int(raw.replace(" ", ""))
        except ValueError:
            limit = 0
    if not 1 <= limit <= 1_000_000:
        await message.answer(
            "Введите число от 1 до 1 000 000 либо слово <code>все</code>.",
            reply_markup=direct_cancel_menu(),
        )
        return
    await state.update_data(direct_recipient_limit=limit, direct_account_ids=[])
    await state.set_state(None)
    await message.answer(
        direct_cooldown_text(),
        reply_markup=direct_cooldown_menu(),
    )


@router.callback_query(F.data.startswith("dm:new:cooldown:"))
async def select_direct_cooldown(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    cooldown_days = int((callback.data or "").rsplit(":", 1)[-1])
    if cooldown_days not in {0, 7, 30, 90}:
        await callback.answer("Неизвестный срок защиты", show_alert=True)
        return
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    if not accounts:
        await callback.answer("Нет готовых аккаунтов-отправителей", show_alert=True)
        return
    await state.update_data(
        direct_contact_cooldown_days=cooldown_days,
        direct_account_page=0,
    )
    await _show_direct_account_picker(callback, state, repository, settings)


async def _show_direct_account_picker(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    data = await state.get_data()
    selected = {int(value) for value in data.get("direct_account_ids", [])}
    available_ids = {int(account.id) for account in accounts}
    selected &= available_ids
    page = max(0, int(data.get("direct_account_page") or 0))
    last_page = max(0, (len(accounts) - 1) // DIRECT_MESSAGE_PICKER_PAGE_SIZE)
    page = min(page, last_page)
    await state.update_data(direct_account_ids=sorted(selected), direct_account_page=page)
    await _edit_callback(
        callback,
        direct_account_picker_text(
            len(selected),
            len(accounts),
            page,
            DIRECT_MESSAGE_PICKER_PAGE_SIZE,
        ),
        direct_account_picker(
            accounts,
            selected,
            page=page,
            page_size=DIRECT_MESSAGE_PICKER_PAGE_SIZE,
        ),
    )


@router.callback_query(F.data.startswith("dm:new:accounts:page:"))
async def page_direct_accounts(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    await state.update_data(direct_account_page=page)
    await _show_direct_account_picker(callback, state, repository, settings)


@router.callback_query(F.data.startswith("dm:new:account:"))
async def toggle_direct_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[-1])
    data = await state.get_data()
    selected = {int(value) for value in data.get("direct_account_ids", [])}
    if account_id in selected:
        selected.remove(account_id)
    else:
        selected.add(account_id)
    await state.update_data(direct_account_ids=sorted(selected))
    await _show_direct_account_picker(callback, state, repository, settings)


@router.callback_query(F.data.in_({"dm:new:accounts:all", "dm:new:accounts:none"}))
async def bulk_select_direct_accounts(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    selected = [int(account.id) for account in accounts] if callback.data.endswith(":all") else []
    await state.update_data(direct_account_ids=selected)
    await _show_direct_account_picker(callback, state, repository, settings)


@router.callback_query(F.data == "dm:new:accounts:done")
async def finish_direct_account_selection(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    data = await state.get_data()
    if not data.get("direct_account_ids"):
        await callback.answer("Выберите хотя бы один аккаунт", show_alert=True)
        return
    await state.set_state(DirectCampaignCreateState.content)
    await _edit_callback(callback, direct_content_prompt_text(), direct_cancel_menu())


@router.message(DirectCampaignCreateState.content)
async def save_direct_campaign_content(
    message: Message,
    state: FSMContext,
    bot: Bot,
    settings: Settings,
    direct_messages: DirectMessageService,
) -> None:
    try:
        text, html_text, media = await _extract_broadcast_content(message, bot, settings)
    except ContentError as exc:
        await message.answer(escape(str(exc)), reply_markup=direct_cancel_menu())
        return
    data = await state.get_data()
    list_id = int(data.get("direct_list_id") or 0)
    subscriber_list = await direct_messages.get_subscriber_list(list_id)
    campaign_name = (
        f"{subscriber_list.name} · ЛС {utc_now().strftime('%d.%m %H:%M')} · {uuid4().hex[:4]}"
    )[:150]
    try:
        campaign = await direct_messages.create_campaign(
            name=campaign_name,
            subscriber_list_id=list_id,
            segment=SubscriberSegment(str(data.get("direct_segment") or "all")),
            recipient_limit=int(data.get("direct_recipient_limit") or 0),
            contact_cooldown_days=int(
                data["direct_contact_cooldown_days"]
                if data.get("direct_contact_cooldown_days") is not None
                else 30
            ),
            account_ids=[int(value) for value in data.get("direct_account_ids", [])],
            text=text,
            html_text=html_text,
            media_path=media.path if media else None,
            media_kind=media.kind if media else None,
            media_name=media.name if media else None,
            media_size=media.size if media else None,
        )
        view = await direct_messages.get_campaign_view(int(campaign.id))
    except (ConflictError, ContentError) as exc:
        await message.answer(escape(str(exc)), reply_markup=direct_cancel_menu())
        return
    launch_token = uuid4().hex[:12]
    await state.clear()
    await state.update_data(
        direct_launch_campaign_id=int(campaign.id),
        direct_launch_token=launch_token,
    )
    await message.answer(
        direct_campaign_confirmation_text(view),
        reply_markup=direct_campaign_confirmation_menu(int(campaign.id), launch_token),
    )


@router.callback_query(F.data.startswith("dm:campaigns:"))
async def open_direct_campaigns(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    items, total = await direct_messages.list_campaigns(page, DIRECT_MESSAGE_PAGE_SIZE)
    await _edit_callback(
        callback,
        direct_campaigns_text(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        direct_campaigns_menu(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("dm:campaign:"))
async def open_direct_campaign(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_campaign_id, raw_page = (callback.data or "").split(":")
    view = await direct_messages.get_campaign_view(int(raw_campaign_id))
    await _edit_callback(
        callback,
        direct_campaign_confirmation_text(view),
        direct_campaign_details_menu(view.campaign.id, int(raw_page)),
    )


@router.callback_query(F.data.startswith("dm:campaignrun:"))
async def confirm_saved_direct_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_campaign_id, _ = (callback.data or "").split(":")
    campaign_id = int(raw_campaign_id)
    view = await direct_messages.get_campaign_view(campaign_id)
    launch_token = uuid4().hex[:12]
    await state.clear()
    await state.update_data(
        direct_launch_campaign_id=campaign_id,
        direct_launch_token=launch_token,
    )
    await _edit_callback(
        callback,
        direct_campaign_confirmation_text(view),
        direct_campaign_confirmation_menu(campaign_id, launch_token),
    )


@router.callback_query(F.data.startswith("dm:launch:"))
async def launch_direct_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_campaign_id, launch_token = (callback.data or "").split(":")
    campaign_id = int(raw_campaign_id)
    data = await state.get_data()
    if (
        int(data.get("direct_launch_campaign_id") or 0) != campaign_id
        or str(data.get("direct_launch_token") or "") != launch_token
    ):
        await callback.answer("Это подтверждение уже использовано или устарело", show_alert=True)
        return
    await state.clear()
    await _edit_callback(
        callback,
        "<b>⏳ Формирую очередь личных сообщений…</b>",
        None,
    )
    try:
        launch = await direct_messages.launch_campaign(campaign_id, consent_confirmed=True)
        view = await direct_messages.get_launch_view(int(launch.id))
    except ConflictError as exc:
        await _edit_callback(
            callback,
            f"<b>Запуск не создан</b>\n\n{escape(str(exc))}",
            direct_messages_home_menu(),
            answer=False,
        )
        return
    await _edit_callback(
        callback,
        direct_launch_details_text(view),
        direct_launch_details_menu(view, 0),
        answer=False,
    )


@router.callback_query(F.data.startswith("dm:launches:"))
async def open_direct_launches(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    items, total = await direct_messages.list_launches(page, DIRECT_MESSAGE_PAGE_SIZE)
    await _edit_callback(
        callback,
        direct_launches_text(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        direct_launches_menu(items, total, page, DIRECT_MESSAGE_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("dm:launchview:"))
async def open_direct_launch(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    view = await direct_messages.get_launch_view(int(raw_launch_id))
    await _edit_callback(
        callback,
        direct_launch_details_text(view),
        direct_launch_details_menu(view, int(raw_page)),
    )


@router.callback_query(F.data.startswith("dm:launchcancel:"))
async def ask_cancel_direct_launch(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    view = await direct_messages.get_launch_view(int(raw_launch_id))
    await _edit_callback(
        callback,
        "<b>Остановить ЛС-рассылку?</b>\n\n"
        f"Ещё не завершено заданий: <b>{view.active:,}</b>. Уже доставленные сообщения "
        "останутся в Telegram; новые задания будут отменены.",
        direct_launch_cancel_menu(view.launch.id, int(raw_page)),
    )


@router.callback_query(F.data.startswith("dm:launchcancelok:"))
async def confirm_cancel_direct_launch(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    cancelled = await direct_messages.cancel_launch(int(raw_launch_id))
    view = await direct_messages.get_launch_view(int(raw_launch_id))
    await callback.answer(f"Отменено заданий: {cancelled}")
    await _edit_callback(
        callback,
        direct_launch_details_text(view),
        direct_launch_details_menu(view, int(raw_page)),
        answer=False,
    )


async def _show_subscriber_invites_home(callback: CallbackQuery) -> None:
    await _edit_callback(
        callback,
        subscriber_invites_home_text(),
        subscriber_invites_home_menu(),
    )


@router.callback_query(F.data == "inv:home")
async def open_subscriber_invites_home(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _show_subscriber_invites_home(callback)


@router.callback_query(F.data == "inv:new")
async def begin_subscriber_invite(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    await state.clear()
    await state.set_state(SubscriberInviteCreateState.destination)
    await _edit_callback(callback, invite_destination_prompt_text(), invite_cancel_menu())


@router.callback_query(F.data.startswith("inv:new:fromlist:"))
async def begin_subscriber_invite_from_list(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    try:
        list_id = int((callback.data or "").rsplit(":", 1)[1])
        item = await direct_messages.get_subscriber_list(list_id)
    except (IndexError, NotFoundError, ValueError):
        await callback.answer("База больше недоступна. Обновите список баз.", show_alert=True)
        return
    if item.status.value != "ready" or item.member_count <= 0:
        await callback.answer("Эта база пока не готова к приглашению", show_alert=True)
        return
    await state.clear()
    await state.update_data(subscriber_invite_preselected_list_id=item.id)
    await state.set_state(SubscriberInviteCreateState.destination)
    await _edit_callback(callback, invite_destination_prompt_text(), invite_cancel_menu())


@router.message(SubscriberInviteCreateState.destination)
async def set_subscriber_invite_destination(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    try:
        destination_ref, destination_key = normalize_invite_destination_ref(message.text or "")
    except ValueError as exc:
        await message.answer(escape(str(exc)), reply_markup=invite_cancel_menu())
        return
    await state.update_data(
        subscriber_invite_destination_ref=destination_ref,
        subscriber_invite_destination_key=destination_key,
    )
    await state.set_state(None)
    data = await state.get_data()
    preselected_list_id = int(data.get("subscriber_invite_preselected_list_id") or 0)
    if preselected_list_id:
        try:
            item = await direct_messages.get_subscriber_list(preselected_list_id)
        except NotFoundError:
            item = None
        if item is not None and item.status.value == "ready" and item.member_count > 0:
            await state.update_data(
                subscriber_invite_list_id=item.id,
                subscriber_invite_preselected_list_id=None,
            )
            await message.answer(
                invite_segment_text(item), reply_markup=invite_segment_menu(item.id)
            )
            return
        await state.update_data(subscriber_invite_preselected_list_id=None)
    items, total = await direct_messages.list_ready_subscriber_lists(
        0, DIRECT_MESSAGE_PICKER_PAGE_SIZE
    )
    if total == 0:
        await message.answer(
            "Сначала импортируйте готовую базу подписчиков.", reply_markup=invite_cancel_menu()
        )
        return
    await message.answer(
        invite_list_picker_text(total, 0, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
        reply_markup=invite_list_picker(items, total, 0, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
    )


async def _show_subscriber_invite_list_picker(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
    *,
    page: int,
) -> None:
    page = max(0, page)
    items, total = await direct_messages.list_ready_subscriber_lists(
        page, DIRECT_MESSAGE_PICKER_PAGE_SIZE
    )
    last_page = max(0, (total - 1) // DIRECT_MESSAGE_PICKER_PAGE_SIZE)
    if page > last_page:
        page = last_page
        items, total = await direct_messages.list_ready_subscriber_lists(
            page, DIRECT_MESSAGE_PICKER_PAGE_SIZE
        )
    if total == 0:
        await callback.answer("Сначала импортируйте готовую базу подписчиков", show_alert=True)
        return
    await _edit_callback(
        callback,
        invite_list_picker_text(total, page, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
        invite_list_picker(items, total, page, DIRECT_MESSAGE_PICKER_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("inv:new:lists:"))
async def page_subscriber_invite_lists(
    callback: CallbackQuery,
    direct_messages: DirectMessageService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    await _show_subscriber_invite_list_picker(callback, direct_messages, page=page)


@router.callback_query(F.data.startswith("inv:new:list:"))
async def select_subscriber_invite_list(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    list_id = int((callback.data or "").rsplit(":", 1)[-1])
    item = await direct_messages.get_subscriber_list(list_id)
    if item.status.value != "ready" or item.member_count <= 0:
        await callback.answer("Эта база пока не готова", show_alert=True)
        return
    await state.update_data(subscriber_invite_list_id=list_id)
    await _edit_callback(callback, invite_segment_text(item), invite_segment_menu(list_id))


@router.callback_query(F.data.startswith("inv:new:seg:"))
async def select_subscriber_invite_segment(callback: CallbackQuery, state: FSMContext) -> None:
    parts = (callback.data or "").split(":")
    try:
        list_id = int(parts[3])
        segment = SubscriberSegment(parts[4])
    except (IndexError, ValueError):
        await callback.answer("Неизвестный сегмент", show_alert=True)
        return
    await state.update_data(
        subscriber_invite_list_id=list_id,
        subscriber_invite_segment=segment.value,
    )
    await state.set_state(SubscriberInviteCreateState.limit)
    await _edit_callback(callback, invite_limit_prompt_text(segment), invite_cancel_menu())


@router.message(SubscriberInviteCreateState.limit)
async def set_subscriber_invite_limit(
    message: Message,
    state: FSMContext,
    direct_messages: DirectMessageService,
) -> None:
    data = await state.get_data()
    list_id = int(data.get("subscriber_invite_list_id") or 0)
    try:
        segment = SubscriberSegment(str(data.get("subscriber_invite_segment") or "all"))
        item = await direct_messages.get_subscriber_list(list_id)
    except (NotFoundError, ValueError):
        await state.clear()
        await message.answer("Черновик приглашения устарел. Создайте его заново.")
        return
    raw = (message.text or "").strip().casefold()
    if raw in {"все", "all"}:
        limit = {
            SubscriberSegment.ALL: item.member_count,
            SubscriberSegment.PREMIUM: item.premium_count,
            SubscriberSegment.NON_PREMIUM: item.non_premium_count,
        }[segment]
    else:
        try:
            limit = int(raw.replace(" ", ""))
        except ValueError:
            limit = 0
    if not 1 <= limit <= 1_000_000:
        await message.answer(
            "Введите число от 1 до 1 000 000 либо слово <code>все</code>.",
            reply_markup=invite_cancel_menu(),
        )
        return
    await state.update_data(
        subscriber_invite_recipient_limit=limit,
        subscriber_invite_account_ids=[],
    )
    await state.set_state(None)
    await message.answer(invite_cooldown_text(), reply_markup=invite_cooldown_menu())


@router.callback_query(F.data.startswith("inv:new:cooldown:"))
async def select_subscriber_invite_cooldown(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    cooldown_days = int((callback.data or "").rsplit(":", 1)[-1])
    if cooldown_days not in {0, 7, 30, 90}:
        await callback.answer("Неизвестный срок защиты", show_alert=True)
        return
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    if not accounts:
        await callback.answer("Нет готовых аккаунтов-администраторов", show_alert=True)
        return
    await state.update_data(
        subscriber_invite_contact_cooldown_days=cooldown_days,
        subscriber_invite_account_page=0,
    )
    await _show_subscriber_invite_account_picker(callback, state, repository, settings)


async def _show_subscriber_invite_account_picker(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    data = await state.get_data()
    selected = {int(value) for value in data.get("subscriber_invite_account_ids", [])}
    available_ids = {int(account.id) for account in accounts}
    selected &= available_ids
    page = max(0, int(data.get("subscriber_invite_account_page") or 0))
    page = min(page, max(0, (len(accounts) - 1) // DIRECT_MESSAGE_PICKER_PAGE_SIZE))
    await state.update_data(
        subscriber_invite_account_ids=sorted(selected), subscriber_invite_account_page=page
    )
    await _edit_callback(
        callback,
        invite_account_picker_text(
            len(selected), len(accounts), page, DIRECT_MESSAGE_PICKER_PAGE_SIZE
        ),
        invite_account_picker(
            accounts,
            selected,
            page=page,
            page_size=DIRECT_MESSAGE_PICKER_PAGE_SIZE,
        ),
    )


@router.callback_query(F.data.startswith("inv:new:accounts:page:"))
async def page_subscriber_invite_accounts(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    await state.update_data(
        subscriber_invite_account_page=max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    )
    await _show_subscriber_invite_account_picker(callback, state, repository, settings)


@router.callback_query(F.data.startswith("inv:new:account:"))
async def toggle_subscriber_invite_account(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    account_id = int((callback.data or "").rsplit(":", 1)[-1])
    data = await state.get_data()
    selected = {int(value) for value in data.get("subscriber_invite_account_ids", [])}
    if account_id in selected:
        selected.remove(account_id)
    else:
        selected.add(account_id)
    await state.update_data(subscriber_invite_account_ids=sorted(selected))
    await _show_subscriber_invite_account_picker(callback, state, repository, settings)


@router.callback_query(F.data.in_({"inv:new:accounts:all", "inv:new:accounts:none"}))
async def bulk_select_subscriber_invite_accounts(
    callback: CallbackQuery,
    state: FSMContext,
    repository: AdminRepository,
    settings: Settings,
) -> None:
    accounts = await repository.active_owned_accounts(
        require_authorized_session=not settings.dry_run
    )
    selected = [int(account.id) for account in accounts] if callback.data.endswith(":all") else []
    await state.update_data(subscriber_invite_account_ids=selected)
    await _show_subscriber_invite_account_picker(callback, state, repository, settings)


@router.callback_query(F.data == "inv:new:accounts:done")
async def save_subscriber_invite_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    direct_messages: DirectMessageService,
    subscriber_invites: SubscriberInviteService,
) -> None:
    data = await state.get_data()
    selected_ids = [int(value) for value in data.get("subscriber_invite_account_ids", [])]
    if not selected_ids:
        await callback.answer("Выберите хотя бы один аккаунт", show_alert=True)
        return
    list_id = int(data.get("subscriber_invite_list_id") or 0)
    destination_ref = str(data.get("subscriber_invite_destination_ref") or "")
    try:
        subscriber_list = await direct_messages.get_subscriber_list(list_id)
        campaign = await subscriber_invites.create_campaign(
            name=(
                f"{subscriber_list.name} · приглашение · "
                f"{utc_now().strftime('%d.%m %H:%M')} · {uuid4().hex[:4]}"
            )[:150],
            subscriber_list_id=list_id,
            destination_ref=destination_ref,
            segment=SubscriberSegment(str(data.get("subscriber_invite_segment") or "all")),
            recipient_limit=int(data.get("subscriber_invite_recipient_limit") or 0),
            contact_cooldown_days=int(
                data["subscriber_invite_contact_cooldown_days"]
                if data.get("subscriber_invite_contact_cooldown_days") is not None
                else 90
            ),
            account_ids=selected_ids,
        )
        view = await subscriber_invites.get_campaign_view(int(campaign.id))
    except (ConflictError, NotFoundError, ValueError) as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    token = uuid4().hex[:12]
    await state.clear()
    await state.update_data(
        subscriber_invite_launch_campaign_id=int(campaign.id),
        subscriber_invite_launch_token=token,
    )
    await _edit_callback(
        callback,
        invite_campaign_text(view, confirm=True),
        invite_campaign_confirmation_menu(int(campaign.id), token),
    )


@router.callback_query(F.data.startswith("inv:campaigns:"))
async def open_subscriber_invite_campaigns(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    campaigns, total = await subscriber_invites.list_campaigns(page, DIRECT_MESSAGE_PAGE_SIZE)
    views = [await subscriber_invites.get_campaign_view(int(item.id)) for item in campaigns]
    await _edit_callback(
        callback,
        invite_campaigns_text(views, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        invite_campaigns_menu(views, total, page, DIRECT_MESSAGE_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("inv:campaign:"))
async def open_subscriber_invite_campaign(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_campaign_id, raw_page = (callback.data or "").split(":")
    view = await subscriber_invites.get_campaign_view(int(raw_campaign_id))
    await _edit_callback(
        callback,
        invite_campaign_text(view),
        invite_campaign_details_menu(int(raw_campaign_id), int(raw_page)),
    )


@router.callback_query(F.data.startswith("inv:campaignrun:"))
async def confirm_subscriber_invite_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_campaign_id, _ = (callback.data or "").split(":")
    campaign_id = int(raw_campaign_id)
    view = await subscriber_invites.get_campaign_view(campaign_id)
    token = uuid4().hex[:12]
    await state.clear()
    await state.update_data(
        subscriber_invite_launch_campaign_id=campaign_id,
        subscriber_invite_launch_token=token,
    )
    await _edit_callback(
        callback,
        invite_campaign_text(view, confirm=True),
        invite_campaign_confirmation_menu(campaign_id, token),
    )


@router.callback_query(F.data.startswith("inv:launch:"))
async def launch_subscriber_invite_campaign(
    callback: CallbackQuery,
    state: FSMContext,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_campaign_id, token = (callback.data or "").split(":")
    campaign_id = int(raw_campaign_id)
    data = await state.get_data()
    if (
        int(data.get("subscriber_invite_launch_campaign_id") or 0) != campaign_id
        or str(data.get("subscriber_invite_launch_token") or "") != token
    ):
        await callback.answer("Это подтверждение уже использовано или устарело", show_alert=True)
        return
    await state.clear()
    await _edit_callback(callback, "<b>⏳ Проверяю права и формирую очередь приглашений…</b>", None)
    try:
        launch = await subscriber_invites.launch_campaign(campaign_id, consent_confirmed=True)
        view = await subscriber_invites.get_launch_view(int(launch.id))
    except (ConflictError, NotFoundError) as exc:
        await _edit_callback(
            callback,
            f"<b>Запуск не создан</b>\n\n{escape(str(exc))}",
            subscriber_invites_home_menu(),
            answer=False,
        )
        return
    await _edit_callback(
        callback,
        invite_launch_text(view),
        invite_launch_details_menu(view, 0),
        answer=False,
    )


@router.callback_query(F.data.startswith("inv:launches:"))
async def open_subscriber_invite_launches(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    page = max(0, int((callback.data or "").rsplit(":", 1)[-1]))
    launches, total = await subscriber_invites.list_launches(page, DIRECT_MESSAGE_PAGE_SIZE)
    await _edit_callback(
        callback,
        invite_launches_text(launches, total, page, DIRECT_MESSAGE_PAGE_SIZE),
        invite_launches_menu(launches, total, page, DIRECT_MESSAGE_PAGE_SIZE),
    )


@router.callback_query(F.data.startswith("inv:launchview:"))
async def open_subscriber_invite_launch(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    view = await subscriber_invites.get_launch_view(int(raw_launch_id))
    await _edit_callback(
        callback,
        invite_launch_text(view),
        invite_launch_details_menu(view, int(raw_page)),
    )


@router.callback_query(F.data.startswith("inv:launchcancel:"))
async def ask_cancel_subscriber_invite_launch(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    view = await subscriber_invites.get_launch_view(int(raw_launch_id))
    await _edit_callback(
        callback,
        "<b>Остановить приглашение?</b>\n\n"
        f"Ещё не завершено заданий: <b>{view.active:,}</b>. Уже созданные приглашения "
        "останутся у пользователей; новые задания будут отменены.",
        invite_launch_cancel_menu(int(raw_launch_id), int(raw_page)),
    )


@router.callback_query(F.data.startswith("inv:launchcancelok:"))
async def confirm_cancel_subscriber_invite_launch(
    callback: CallbackQuery,
    subscriber_invites: SubscriberInviteService,
) -> None:
    _, _, raw_launch_id, raw_page = (callback.data or "").split(":")
    cancelled = await subscriber_invites.cancel_launch(int(raw_launch_id))
    view = await subscriber_invites.get_launch_view(int(raw_launch_id))
    await callback.answer(f"Отменено заданий: {cancelled}")
    await _edit_callback(
        callback,
        invite_launch_text(view),
        invite_launch_details_menu(view, int(raw_page)),
        answer=False,
    )


@router.message()
async def unknown_message(message: Message) -> None:
    await message.answer(
        "Не понял сообщение. Откройте панель кнопкой ниже или используйте "
        "<code>/help</code> для краткой справки.",
        reply_markup=main_menu(),
    )


def _profile_name_input(value: str, *, required: bool) -> str:
    normalized = value.strip()
    if required and not normalized:
        raise ValueError("Имя Telegram не может быть пустым.")
    if len(normalized) > 64:
        raise ValueError("Имя или фамилия не должны быть длиннее 64 символов.")
    if any(ord(character) < 32 for character in normalized):
        raise ValueError("Имя и фамилия должны быть в одну строку.")
    return normalized


def _profile_about_input(value: str) -> str:
    normalized = value.strip()
    if normalized == "-":
        return ""
    if len(normalized) > 70:
        raise ValueError("описание Telegram должно быть до 70 символов")
    return normalized


async def _extract_broadcast_content(
    message: Message,
    bot: Bot,
    settings: Settings,
) -> tuple[str, str | None, StoredMedia | None]:
    value = (message.text or message.caption or "").strip()
    downloadable: object | None = None
    media_kind: str | None = None
    media_name = ""
    file_size: int | None = None

    if message.photo:
        downloadable = message.photo[-1]
        media_kind = "photo"
        media_name = f"{message.photo[-1].file_unique_id}.jpg"
        file_size = message.photo[-1].file_size
    elif message.video is not None:
        downloadable = message.video
        media_kind = "video"
        media_name = message.video.file_name or f"{message.video.file_unique_id}.mp4"
        file_size = message.video.file_size
    elif message.document is not None:
        downloadable = message.document
        media_kind = "document"
        media_name = message.document.file_name or f"{message.document.file_unique_id}.bin"
        file_size = message.document.file_size
    elif message.voice is not None:
        downloadable = message.voice
        media_kind = "voice"
        media_name = f"{message.voice.file_unique_id}.ogg"
        file_size = message.voice.file_size
    elif message.video_note is not None:
        downloadable = message.video_note
        media_kind = "video_note"
        media_name = f"{message.video_note.file_unique_id}.mp4"
        file_size = message.video_note.file_size

    if media_kind == "video_note" and value:
        raise ContentError("видеокружок отправляется без подписи")
    if not value and media_kind is None:
        raise ContentError("добавьте рекламный текст или подпись к медиа")
    text_limit = 1024 if downloadable is not None else 4096
    if len(value) > text_limit:
        raise ContentError(f"текст должен содержать не больше {text_limit} символов")
    html_text = message.html_text if message.text is not None else message.html_caption
    validate_spintax(value)
    if html_text is not None:
        validate_spintax(html_text)

    if downloadable is None:
        return value, html_text, None
    if file_size is not None and file_size > settings.media_max_bytes:
        raise ContentError(
            f"медиафайл больше лимита {settings.media_max_bytes // (1024 * 1024)} МБ"
        )
    buffer = BytesIO()
    await bot.download(downloadable, destination=buffer)
    stored = await asyncio.to_thread(
        store_media,
        settings,
        buffer.getvalue(),
        kind=str(media_kind),
        original_name=media_name,
    )
    return value, html_text, stored


async def _extract_media_only(
    message: Message,
    bot: Bot,
    settings: Settings,
) -> StoredMedia:
    downloadable: object | None = None
    media_kind: str | None = None
    media_name = ""
    file_size: int | None = None
    if message.photo:
        downloadable = message.photo[-1]
        media_kind = "photo"
        media_name = f"{message.photo[-1].file_unique_id}.jpg"
        file_size = message.photo[-1].file_size
    elif message.video is not None:
        downloadable = message.video
        media_kind = "video"
        media_name = message.video.file_name or f"{message.video.file_unique_id}.mp4"
        file_size = message.video.file_size
    elif message.document is not None:
        downloadable = message.document
        media_kind = "document"
        media_name = message.document.file_name or f"{message.document.file_unique_id}.bin"
        file_size = message.document.file_size
    elif message.voice is not None:
        downloadable = message.voice
        media_kind = "voice"
        media_name = f"{message.voice.file_unique_id}.ogg"
        file_size = message.voice.file_size
    elif message.video_note is not None:
        downloadable = message.video_note
        media_kind = "video_note"
        media_name = f"{message.video_note.file_unique_id}.mp4"
        file_size = message.video_note.file_size
    if downloadable is None or media_kind is None:
        raise ContentError("отправьте фото, видео, документ, голосовое или видеокружок")
    if file_size is not None and file_size > settings.media_max_bytes:
        raise ContentError(
            f"медиафайл больше лимита {settings.media_max_bytes // (1024 * 1024)} МБ"
        )
    buffer = BytesIO()
    await bot.download(downloadable, destination=buffer)
    return await asyncio.to_thread(
        store_media,
        settings,
        buffer.getvalue(),
        kind=media_kind,
        original_name=media_name,
    )


def _find_variant(
    variants: list[BroadcastContentVariant],
    variant_id: str,
) -> BroadcastContentVariant:
    for variant in variants:
        if variant.variant_id == variant_id:
            return variant
    raise NotFoundError("вариант не найден")


async def _campaign_variant_context(
    repository: AdminRepository,
    campaign_id: int,
    variant_id: str,
) -> tuple[Campaign, list[BroadcastContentVariant], BroadcastContentVariant]:
    campaign = await repository.get_campaign(campaign_id)
    variants = await repository.campaign_content_variants(campaign_id)
    return campaign, variants, _find_variant(variants, variant_id)


async def _send_variant_preview(
    bot: Bot,
    chat_id: int,
    variant: BroadcastContentVariant,
    settings: Settings,
) -> None:
    if variant.contact is not None:
        await bot.send_contact(
            chat_id,
            phone_number=variant.contact.phone_number,
            first_name=variant.contact.first_name,
            last_name=variant.contact.last_name or None,
            vcard=variant.contact.vcard or None,
        )
        return
    preview_seed = f"preview:{variant.variant_id}"
    rendered_text = render_spintax(variant.text, seed=preview_seed)
    rendered_html = (
        render_spintax(variant.html_text, seed=preview_seed)
        if variant.html_text is not None
        else None
    )
    content = rendered_html or escape(rendered_text)
    if not variant.media:
        await bot.send_message(chat_id, content, parse_mode="HTML")
        return
    paths = [resolve_media_path(settings, item.path) for item in variant.media]
    if len(variant.media) == 1:
        item = variant.media[0]
        media = FSInputFile(str(paths[0]), filename=item.name)
        if item.kind == "photo":
            await bot.send_photo(chat_id, media, caption=content, parse_mode="HTML")
        elif item.kind == "video":
            await bot.send_video(chat_id, media, caption=content, parse_mode="HTML")
        elif item.kind == "document":
            await bot.send_document(chat_id, media, caption=content, parse_mode="HTML")
        elif item.kind == "voice":
            await bot.send_voice(
                chat_id,
                media,
                caption=content or None,
                parse_mode="HTML" if content else None,
            )
        elif item.kind == "video_note":
            await bot.send_video_note(chat_id, media)
        else:
            raise ContentError("неизвестный тип медиафайла")
        return
    media_group: list[InputMediaPhoto | InputMediaVideo] = []
    for index, (item, path) in enumerate(zip(variant.media, paths, strict=True)):
        file = FSInputFile(str(path), filename=item.name)
        caption = content if index == 0 else None
        if item.kind == "photo":
            media_group.append(InputMediaPhoto(media=file, caption=caption, parse_mode="HTML"))
        elif item.kind == "video":
            media_group.append(InputMediaVideo(media=file, caption=caption, parse_mode="HTML"))
        else:
            raise ContentError("в альбом можно добавить только фото и видео")
    await bot.send_media_group(chat_id, media_group)


def _contact_from_message(message: Message) -> StoredContact:
    if message.contact is not None:
        return new_stored_contact(
            phone_number=message.contact.phone_number,
            first_name=message.contact.first_name,
            last_name=message.contact.last_name or "",
        )
    parts = [part.strip() for part in (message.text or "").split("|")]
    if len(parts) not in {2, 3}:
        raise ContentError("нужен контакт Telegram или строка из номера, имени и фамилии")
    return new_stored_contact(
        phone_number=parts[0],
        first_name=parts[1],
        last_name=parts[2] if len(parts) == 3 else "",
    )


async def _download_profile_photo(message: Message, bot: Bot) -> tuple[bytes, str]:
    downloadable: object | None = None
    file_name = "avatar.jpg"
    file_size: int | None = None
    if message.photo:
        downloadable = message.photo[-1]
        file_size = message.photo[-1].file_size
    elif message.document is not None:
        document = message.document
        suffix = Path(document.file_name or "").suffix.lower()
        if suffix not in {".jpg", ".jpeg", ".png"}:
            raise AccountProfileError("нужна фотография или файл JPG/PNG")
        if document.mime_type not in {None, "image/jpeg", "image/png"}:
            raise AccountProfileError("файл не распознан как JPG/PNG изображение")
        downloadable = document
        file_name = document.file_name or f"avatar{suffix}"
        file_size = document.file_size
    if downloadable is None:
        raise AccountProfileError("отправьте фотографию или файл JPG/PNG")
    if file_size is not None and file_size > MAX_PROFILE_PHOTO_BYTES:
        raise AccountProfileError("аватар больше 10 МБ")
    buffer = BytesIO()
    await bot.download(downloadable, destination=buffer)
    payload = buffer.getvalue()
    if not payload:
        raise AccountProfileError("не удалось скачать изображение")
    if len(payload) > MAX_PROFILE_PHOTO_BYTES:
        raise AccountProfileError("аватар больше 10 МБ")
    return payload, file_name


async def _account_proxy_summary(
    proxy_service: ProxyService,
    account_id: int,
) -> tuple[str | None, int]:
    proxy_slot = await proxy_service.account_proxy_endpoint(account_id)
    proxy_view = await proxy_service.proxy_for_assignment(f"account:{account_id}")
    shared_proxy_accounts = (
        sum(key.startswith("account:") for key in proxy_view.assignment_keys)
        if proxy_view is not None
        else 0
    )
    return proxy_slot, shared_proxy_accounts


async def _show_accounts(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_accounts(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_accounts(page.pages - 1, PAGE_SIZE)
    await _edit_callback(callback, accounts_text(page), accounts_menu(page), answer=False)


async def _show_account_details(
    callback: CallbackQuery,
    account_service: AccountService,
    proxy_service: ProxyService,
    account_id: int,
    page_number: int,
    *,
    banner: str = "",
    answer: bool = False,
) -> None:
    try:
        account = await account_service.get_account(account_id)
    except NotFoundError as exc:
        await callback.answer(str(exc), show_alert=True)
        return
    proxy_slot, shared_proxy_accounts = await _account_proxy_summary(proxy_service, account_id)
    await _edit_callback(
        callback,
        banner
        + account_details_text(
            account,
            proxy_slot=proxy_slot,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
        account_details_menu(
            account,
            page_number,
            has_proxy=proxy_slot is not None,
            shared_proxy_accounts=shared_proxy_accounts,
        ),
        answer=answer,
    )


async def _show_proxies(
    callback: CallbackQuery, proxy_service: ProxyService, page_number: int
) -> None:
    page = await proxy_service.list_proxies(page_number, PAGE_SIZE)
    await _edit_callback(callback, proxies_text(page), proxies_menu(page), answer=False)


async def _show_targets(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_targets(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_targets(page.pages - 1, PAGE_SIZE)
    await _edit_callback(callback, targets_text(page), targets_menu(page), answer=False)


async def _show_bad_targets(
    callback: CallbackQuery,
    repository: AdminRepository,
    page_number: int,
    *,
    answer: bool = False,
) -> None:
    page = await repository.list_bad_targets(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_bad_targets(page.pages - 1, PAGE_SIZE)
    await _edit_callback(
        callback,
        bad_targets_text(page),
        bad_targets_menu(page),
        answer=answer,
    )


async def _show_target_lists(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_target_lists(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_target_lists(page.pages - 1, PAGE_SIZE)
    await _edit_callback(
        callback,
        target_lists_text(page),
        target_lists_menu(page),
        answer=False,
    )


async def _show_target_list_targets(
    callback: CallbackQuery,
    repository: AdminRepository,
    target_list_id: int,
    list_page: int,
    chat_page: int,
    *,
    filter_name: str = "all",
    answer: bool = True,
) -> None:
    try:
        item = await repository.get_target_list_view(target_list_id)
        page = await repository.list_target_list_targets(
            target_list_id,
            chat_page,
            TARGET_LIST_CHAT_PAGE_SIZE,
            filter_name=filter_name,
        )
        if page.page >= page.pages:
            page = await repository.list_target_list_targets(
                target_list_id,
                page.pages - 1,
                TARGET_LIST_CHAT_PAGE_SIZE,
                filter_name=filter_name,
            )
    except (NotFoundError, ConflictError) as exc:
        if answer:
            await callback.answer(str(exc), show_alert=True)
        return
    await _edit_callback(
        callback,
        target_list_targets_text(item, page, filter_name=filter_name),
        target_list_targets_menu(
            target_list_id,
            list_page,
            page,
            filter_name=filter_name,
            target_list=item,
        ),
        answer=answer,
    )


async def _show_templates(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_templates(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_templates(page.pages - 1, PAGE_SIZE)
    await _edit_callback(callback, templates_text(page), templates_menu(page), answer=False)


async def _show_campaigns(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_campaigns(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_campaigns(page.pages - 1, PAGE_SIZE)
    await _edit_callback(callback, campaigns_text(page), campaigns_menu(page), answer=False)


async def _show_runs(
    callback: CallbackQuery, repository: AdminRepository, page_number: int
) -> None:
    page = await repository.list_launches(page_number, PAGE_SIZE)
    if page.page >= page.pages:
        page = await repository.list_launches(page.pages - 1, PAGE_SIZE)
    await _edit_callback(callback, launches_text(page), launches_menu(page), answer=False)


async def _show_archived_runs(
    callback: CallbackQuery,
    repository: AdminRepository,
    page_number: int,
) -> None:
    page = await repository.list_launches(
        page_number,
        PAGE_SIZE,
        archived=True,
    )
    if page.page >= page.pages:
        page = await repository.list_launches(
            page.pages - 1,
            PAGE_SIZE,
            archived=True,
        )
    await _edit_callback(
        callback,
        launches_text(page, archived=True),
        launches_menu(page, archived=True),
        answer=False,
    )


async def _edit_callback(
    callback: CallbackQuery,
    text: str,
    reply_markup: object | None,
    *,
    answer: bool = True,
) -> None:
    if answer:
        await _safe_callback_answer(callback)
    if not isinstance(callback.message, Message):
        return
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool | None = None,
) -> bool:
    """Acknowledge a callback while tolerating an update delayed by a restart."""

    kwargs: dict[str, object] = {}
    if text is not None:
        kwargs["text"] = text
    if show_alert is not None:
        kwargs["show_alert"] = show_alert
    try:
        await callback.answer(**kwargs)
    except TelegramBadRequest as exc:
        message = str(exc).lower()
        if "query is too old" in message or "query id is invalid" in message:
            return False
        raise
    return True


async def _remember_origin(callback: CallbackQuery, state: FSMContext) -> None:
    if isinstance(callback.message, Message):
        await state.update_data(
            origin_chat_id=callback.message.chat.id,
            origin_message_id=callback.message.message_id,
        )


async def _edit_origin(bot: Bot, state: FSMContext, text: str, reply_markup: object) -> None:
    await _edit_origin_data(bot, await state.get_data(), text, reply_markup)


async def _edit_origin_data(
    bot: Bot,
    data: dict[str, object],
    text: str,
    reply_markup: object,
) -> None:
    chat_id = data.get("origin_chat_id")
    message_id = data.get("origin_message_id")
    if chat_id is None or message_id is None:
        return
    with suppress(TelegramBadRequest):
        await bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=text,
            reply_markup=reply_markup,
        )


def _page_from(parts: list[str]) -> int:
    if len(parts) < 3:
        return 0
    with suppress(ValueError):
        return max(0, int(parts[2]))
    return 0
