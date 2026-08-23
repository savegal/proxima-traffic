from datetime import UTC, datetime, timedelta

import pytest
from aiogram.enums import ChatType
from aiogram.types import CallbackQuery, Chat, Message

from proxima_traffic.accounts import (
    AccountArchiveEntry,
    AccountBulkCheckEntry,
    AccountBulkCheckSummary,
    AccountCheckResult,
    AccountImportError,
)
from proxima_traffic.bot.app import create_dispatcher
from proxima_traffic.bot.channel_comment_ui import (
    channel_comment_account_picker,
    channel_comment_details_menu,
    channel_comment_details_text,
    channel_comment_mention_cooldown_menu,
    channel_comment_mode_menu,
    channel_comment_reactions_menu,
    channel_comment_reactions_text,
    channel_comment_runs_text,
    channel_comments_text,
)
from proxima_traffic.bot.direct_message_ui import (
    direct_account_picker,
    direct_account_picker_text,
    direct_blacklist_menu,
    direct_cooldown_menu,
    direct_launch_details_menu,
    direct_launch_details_text,
    direct_list_picker,
    direct_list_picker_text,
    direct_messages_home_menu,
    direct_messages_home_text,
    subscriber_import_result_menu,
    subscriber_list_details_menu,
)
from proxima_traffic.bot.handlers import (
    AccountArchiveFailure,
    AccountArchiveImportResult,
    AccountArchiveWarning,
    _account_archive_result_text,
    _campaign_reaction_step_count,
    _delete_sensitive_import_message,
    _import_account_archive_message,
    _order_sender_picker_text,
    _parse_channel_comment_refs,
)
from proxima_traffic.bot.keyboards import (
    account_avatar_delete_confirmation,
    account_avatar_menu,
    account_bulk_check_menu,
    account_details_menu,
    account_generated_names_confirmation,
    account_import_sources_menu,
    account_name_catalog_menu,
    account_name_confirmation,
    account_session_clear_confirmation,
    accounts_menu,
    bad_target_details_menu,
    bad_targets_menu,
    campaign_advanced_menu,
    campaign_delete_confirmation,
    campaign_details_menu,
    campaign_edit_target_picker,
    campaign_edit_target_source_menu,
    campaign_mention_cooldown_menu,
    campaign_one_off_window_presets,
    campaign_reactions_menu,
    campaign_repeat_count_presets,
    campaign_repeat_interval_presets,
    campaign_repeat_mode_picker,
    campaign_run_confirmation,
    campaign_schedule_mode_picker,
    campaign_smart_duration_presets,
    campaign_smart_interval_presets,
    campaign_standalone_target_picker,
    campaign_stop_all_confirmation,
    campaign_target_list_picker,
    campaign_variant_delete_confirmation,
    campaign_variant_menu,
    campaign_variants_menu,
    campaigns_menu,
    cancel_menu,
    guide_menu,
    launch_cancel_confirmation,
    launch_details_menu,
    launches_menu,
    main_menu,
    order_advanced_menu,
    order_audience_search_menu,
    order_audience_source_menu,
    order_draft_menu,
    order_duration_presets,
    order_interval_presets,
    order_launch_confirmation,
    order_schedule_mode_menu,
    order_sender_picker,
    order_spread_presets,
    order_target_list_activation_menu,
    order_target_list_picker,
    order_target_picker,
    order_type_menu,
    order_visibility_presets,
    order_visibility_wait_presets,
    proxies_menu,
    proxy_details_menu,
    proxy_group_delete_confirmation,
    runs_menu,
    settings_menu,
    target_cooldown_picker,
    target_delete_confirmation,
    target_details_menu,
    target_folder_account_picker,
    target_list_delete_confirmation,
    target_list_details_menu,
    target_list_target_details_menu,
    target_list_target_remove_confirmation,
    target_list_targets_menu,
    targets_menu,
    template_delete_confirmation,
    template_details_menu,
    templates_menu,
)
from proxima_traffic.bot.middleware import _is_private_control_event
from proxima_traffic.bot.participant_reaction_ui import (
    participant_reaction_details_text,
    participant_reactions_text,
)
from proxima_traffic.bot.renderers import (
    account_avatar_text,
    account_bulk_check_text,
    account_details_text,
    account_generated_names_preview_text,
    account_name_catalog_text,
    account_names_result_text,
    bad_targets_text,
    campaign_confirmation_text,
    campaign_details_text,
    campaign_launch_not_ready_text,
    campaign_launch_review_text,
    campaign_preflight_text,
    campaign_reactions_text,
    campaign_run_confirmation_text,
    campaign_variant_text,
    campaign_variants_text,
    campaigns_text,
    dashboard_text,
    guide_text,
    launch_details_text,
    launches_text,
    order_advanced_text,
    order_draft_text,
    order_schedule_mode_text,
    order_type_text,
    proxies_text,
    proxy_details_text,
    run_details_text,
    target_list_targets_text,
    template_confirmation_text,
    template_details_text,
    wizard_text,
)
from proxima_traffic.bot.repository import (
    DashboardStats,
    LaunchView,
    Page,
    SenderFailoverView,
    TargetListView,
    TargetTimingView,
)
from proxima_traffic.bot.subscriber_invite_ui import (
    invite_account_picker,
    invite_cooldown_menu,
    invite_list_picker,
    subscriber_invites_home_menu,
    subscriber_invites_home_text,
)
from proxima_traffic.channel_comments import ChannelCommentReactionSummary, ChannelCommentRuleView
from proxima_traffic.config import Settings
from proxima_traffic.content import BroadcastContentVariant, StoredContact, StoredMedia
from proxima_traffic.dialogues import DialogueStep, serialize_dialogue_steps
from proxima_traffic.direct_messages import DirectLaunchView, DirectMessageDashboard
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    ActionKind,
    ActionLog,
    ActionStatus,
    BroadcastSenderMode,
    Campaign,
    CampaignMode,
    CampaignRun,
    ChannelCommentMode,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    MessageTemplate,
    ParticipantReactionRule,
    ProxyEndpoint,
    ProxyProtocol,
    ProxyStatus,
    RunStatus,
    RunStep,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
    Target,
)
from proxima_traffic.participant_reactions import ParticipantReactionRuleView
from proxima_traffic.preflight import CampaignPreflightSummary
from proxima_traffic.proxies import ProxyPage, ProxyView
from proxima_traffic.worker_supervision import WorkerHealthSnapshot


def test_control_bot_requires_token_and_numeric_admin_allowlist() -> None:
    settings = Settings(
        bot_token="123:secret",
        bot_admin_ids="101, 202,101",
    )
    token, admin_ids = settings.require_control_bot()
    assert token == "123:secret"
    assert admin_ids == frozenset({101, 202})

    with pytest.raises(ValueError, match="at least one"):
        Settings(bot_token="123:secret", bot_admin_ids="").require_control_bot()

    with pytest.raises(ValueError, match="numeric"):
        Settings(bot_token="123:secret", bot_admin_ids="owner").require_control_bot()


def test_direct_message_section_is_reachable_and_callbacks_fit_telegram_limit() -> None:
    callbacks = [
        button.callback_data
        for menu in (
            main_menu(),
            direct_messages_home_menu(),
            direct_blacklist_menu(),
            subscriber_invites_home_menu(),
            direct_blacklist_menu(
                add_callback="inv:blacklist:add",
                back_callback="inv:home",
                back_text="⬅️ К приглашениям",
            ),
            direct_cooldown_menu(),
        )
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data is not None
    ]

    assert "dm:home" in callbacks
    assert "dm:new" in callbacks
    assert "dm:blacklist:add" in callbacks
    assert "inv:blacklist" in callbacks
    assert "inv:blacklist:add" in callbacks
    assert "dm:new:cooldown:30" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_direct_message_creation_pickers_paginate_without_losing_safe_callbacks() -> None:
    lists = [
        SubscriberList(
            id=index,
            name=f"Database {index}",
            status=SubscriberListStatus.READY,
            member_count=100,
        )
        for index in range(8, 10)
    ]
    list_menu = direct_list_picker(lists, total=9, page=1, page_size=7)
    list_callbacks = [
        button.callback_data
        for row in list_menu.inline_keyboard
        for button in row
        if button.callback_data
    ]
    assert "dm:new:lists:0" in list_callbacks
    assert "dm:new:list:8" in list_callbacks
    assert "страница <b>2/2</b>" in direct_list_picker_text(9, 1, 7)

    accounts = [Account(id=index, label=f"Sender {index}") for index in range(1, 26)]
    account_menu = direct_account_picker(accounts, {1, 25}, page=3, page_size=7)
    account_callbacks = [
        button.callback_data
        for row in account_menu.inline_keyboard
        for button in row
        if button.callback_data
    ]
    assert "dm:new:accounts:page:0" in account_callbacks
    assert "dm:new:account:25" in account_callbacks
    assert "dm:new:account:1" not in account_callbacks
    assert "страница <b>4/4</b>" in direct_account_picker_text(2, 25, 3, 7)
    assert all(len(value.encode("utf-8")) <= 64 for value in list_callbacks + account_callbacks)


def test_subscriber_invite_section_has_paginated_safe_callbacks() -> None:
    lists = [
        SubscriberList(
            id=index,
            name=f"Database {index}",
            status=SubscriberListStatus.READY,
            member_count=100,
        )
        for index in range(8, 10)
    ]
    accounts = [Account(id=index, label=f"Admin {index}") for index in range(1, 26)]
    menus = (
        subscriber_invites_home_menu(),
        invite_cooldown_menu(),
        invite_list_picker(lists, total=9, page=1, page_size=7),
        invite_account_picker(accounts, {1, 25}, page=3, page_size=7),
    )
    callbacks = [
        button.callback_data
        for menu in menus
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data
    ]
    assert "inv:new" in callbacks
    assert "inv:new:cooldown:90" in callbacks
    assert "inv:new:lists:0" in callbacks
    assert "inv:new:list:8" in callbacks
    assert "inv:new:accounts:page:2" in callbacks
    assert "inv:new:account:25" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_channel_comment_account_picker_is_paginated_and_safe_for_large_pools() -> None:
    accounts = [Account(id=index, label=f"Commenter {index}") for index in range(1, 26)]
    menu = channel_comment_account_picker(
        accounts,
        callback_prefix="comments:pick:primary",
        back_callback="comments:add",
        page=3,
        page_size=7,
        pager_callback_prefix="comments:picker:primary",
    )
    callbacks = [
        button.callback_data
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data
    ]

    assert "comments:pick:primary:25" in callbacks
    assert "comments:pick:primary:1" not in callbacks
    assert "comments:picker:primary:2" in callbacks
    assert "comments:add" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_subscriber_list_shortcuts_keep_direct_message_and_invite_actions_together() -> None:
    item = SubscriberList(
        id=57,
        name="Ready subscribers",
        status=SubscriberListStatus.READY,
        member_count=100,
    )
    menus = (subscriber_import_result_menu(item.id), subscriber_list_details_menu(item, 2))
    callbacks = {
        button.callback_data
        for menu in menus
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "dm:new:list:57" in callbacks
    assert "inv:new:fromlist:57" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


async def test_sensitive_account_import_message_is_deleted() -> None:
    class SensitiveMessage:
        deleted = False

        async def delete(self) -> None:
            self.deleted = True

    message = SensitiveMessage()
    await _delete_sensitive_import_message(message)  # type: ignore[arg-type]
    assert message.deleted is True


async def test_account_archive_import_continues_after_one_broken_account() -> None:
    class Document:
        file_name = "sessions.zip"
        file_size = 100

    class ArchiveMessage:
        document = Document()

    class ArchiveBot:
        async def download(self, document, destination) -> None:
            assert document.file_name == "sessions.zip"
            destination.write(b"archive")

    shared_route = object()

    class ArchiveAccountService:
        grouped_account_ids: list[list[int]] = []

        @staticmethod
        def read_account_archive(payload: bytes):
            assert payload == b"archive"
            return (
                AccountArchiveEntry("good", session_bytes=b"good"),
                AccountArchiveEntry("broken", session_bytes=b"broken"),
            )

        async def import_archive_entry(self, entry, *, proxy_route):
            assert proxy_route is shared_route
            if entry.source_name == "broken":
                raise AccountImportError("damaged session")
            return Account(id=101, label="Imported", ownership_confirmed=True)

        async def create_archive_import_group(self, account_ids):
            self.grouped_account_ids.append(account_ids)
            return type("Group", (), {"name": "ZIP-импорт · #101"})()

    class ArchiveProxyService:
        allocated: list[str] = []
        moved: list[tuple[str, str]] = []
        released: list[str] = []

        async def allocate_proxy(self, key, *, purpose):
            self.allocated.append(key)

        async def require_route(self, key):
            assert key in self.allocated
            return shared_route

        async def move_assignment(self, source, target, *, purpose):
            self.moved.append((source, target))

        async def release_assignment_key(self, key):
            self.released.append(key)

    proxy_service = ArchiveProxyService()
    account_service = ArchiveAccountService()
    result = await _import_account_archive_message(  # type: ignore[arg-type]
        ArchiveMessage(),
        ArchiveBot(),
        account_service,
        proxy_service,
        Settings(account_require_proxy=True),
        origin_data={},
    )

    assert result is not None
    assert result.total == 2
    assert [account.id for account in result.imported] == [101]
    assert [failure.source_name for failure in result.failures] == ["broken.session"]
    assert len(proxy_service.allocated) == 2
    assert proxy_service.moved[0][1] == "account:101"
    assert len(proxy_service.released) == 1
    assert account_service.grouped_account_ids == [[101]]
    assert result.group_name == "ZIP-импорт · #101"


def test_account_archive_report_keeps_imported_accounts_when_group_creation_failed() -> None:
    result = AccountArchiveImportResult(
        total=1,
        imported=(Account(id=101, label="Imported", ownership_confirmed=True),),
        failures=(),
        warnings=(),
        group_error="database is locked",
    )

    text = _account_archive_result_text(result)

    assert "Группа импорта не создана" in text
    assert "Аккаунты сохранены" in text
    assert "Imported" in text


def test_account_archive_report_stays_within_telegram_message_limit() -> None:
    result = AccountArchiveImportResult(
        total=100,
        imported=tuple(
            Account(id=index, label=f"Imported account {index} &", ownership_confirmed=True)
            for index in range(1, 51)
        ),
        failures=tuple(
            AccountArchiveFailure(
                source_name=f"very/long/path/account-{index}.session",
                error="<&> " * 60,
            )
            for index in range(51, 91)
        ),
        warnings=tuple(
            AccountArchiveWarning(
                source_name=f"account-{index}.session",
                warning="proxy warning <&> " * 30,
            )
            for index in range(91, 101)
        ),
    )

    text = _account_archive_result_text(result)

    assert len(text) < 4096
    assert text.endswith("</i>")
    assert "&amp;" in text


def test_dispatcher_contains_message_and_callback_handlers() -> None:
    dispatcher = create_dispatcher(frozenset({101}))
    assert dispatcher.sub_routers
    control_router = dispatcher.sub_routers[0]
    assert len(control_router.message.handlers) >= 10
    assert len(control_router.callback_query.handlers) >= 15


def test_cancel_menu_can_return_to_a_feature_section() -> None:
    default_button = cancel_menu().inline_keyboard[0][0]
    section_button = cancel_menu("pr:home:0").inline_keyboard[0][0]

    assert default_button.callback_data == "fsm:cancel"
    assert section_button.callback_data == "pr:home:0"


def test_control_bot_accepts_only_private_messages_and_callbacks() -> None:
    private_message = Message.model_construct(
        chat=Chat.model_construct(id=1, type=ChatType.PRIVATE)
    )
    group_message = Message.model_construct(
        chat=Chat.model_construct(id=-1, type=ChatType.SUPERGROUP)
    )
    callback = CallbackQuery.model_construct(message=private_message)

    assert _is_private_control_event(private_message) is True
    assert _is_private_control_event(group_message) is False
    assert _is_private_control_event(callback) is True

    group_callback = CallbackQuery.model_construct(message=group_message)
    assert _is_private_control_event(group_callback) is False


def test_channel_comment_card_has_check_and_account_replacement_actions() -> None:
    rule = ChannelCommentRule(
        id=91,
        name="Новости",
        channel_ref="@news_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=7,
        primary_text="Комментарий",
        mention_cooldown_days=30,
    )
    view = ChannelCommentRuleView(
        rule=rule,
        queued=2,
        running=0,
        succeeded=5,
        failed=1,
        skipped=1,
    )
    menu = channel_comment_details_menu(view, page=2)
    callbacks = {button.callback_data for row in menu.inline_keyboard for button in row}
    details = channel_comment_details_text(view, {7: "main-account"})

    assert "comments:check:91:2" in callbacks
    assert "comments:account:replace:91:2" in callbacks
    assert "comments:autoreplace:91:2" in callbacks
    assert "comments:mentions:cooldown:91:2" in callbacks
    assert "Автозамена первого аккаунта: <b>выключена</b>" in details
    assert "Проверка Telegram" in details
    assert "ещё не было" in details

    cooldown_menu = channel_comment_mention_cooldown_menu(
        91,
        page=2,
        current_days=30,
    )
    cooldown_callbacks = {
        button.callback_data for row in cooldown_menu.inline_keyboard for button in row
    }
    assert "comments:mentions:days:91:2:30" in cooldown_callbacks
    assert "comments:mentions:custom:91:2" in cooldown_callbacks


def test_broadcast_card_exposes_per_chat_mention_rotation() -> None:
    campaign = Campaign(
        id=93,
        name="Broadcast mentions",
        mode=CampaignMode.BROADCAST,
        primary_account_id=7,
        target_id=4,
        primary_text="Post",
        reply_text="",
        hidden_mentions_enabled=True,
        mention_cooldown_days=30,
    )
    details_callbacks = {
        button.callback_data
        for row in campaign_details_menu(campaign, 2).inline_keyboard
        for button in row
    }
    assert "campaign:mentions:toggle:93:2" in details_callbacks
    assert "campaign:mentions:cooldown:93:2" in details_callbacks

    cooldown_callbacks = {
        button.callback_data
        for row in campaign_mention_cooldown_menu(
            93,
            page=2,
            current_days=30,
        ).inline_keyboard
        for button in row
    }
    assert "campaign:mentions:days:93:2:30" in cooldown_callbacks
    assert "campaign:mentions:custom:93:2" in cooldown_callbacks


def test_campaign_card_separates_check_only_from_checked_launch_path() -> None:
    campaign = Campaign(
        id=95,
        name="Launch actions",
        mode=CampaignMode.BROADCAST,
        primary_account_id=7,
        target_id=4,
        primary_text="Post",
        enabled=True,
    )
    buttons = {
        button.text: button.callback_data
        for row in campaign_details_menu(campaign, 2).inline_keyboard
        for button in row
    }

    assert buttons["🚀 Проверить и перейти к запуску"] == "campaign:run:ask:95:2"
    assert buttons["🩺 Проверить доступ без запуска"] == "campaign:preflight:95:2"


def test_broadcast_reactions_do_not_require_a_dialogue_reply_account() -> None:
    campaign = Campaign(
        id=94,
        name="Broadcast reactions",
        mode=CampaignMode.BROADCAST,
        primary_account_id=7,
        target_id=4,
        primary_text="Post",
        reply_text="",
        reply_account_id=None,
    )

    assert _campaign_reaction_step_count(campaign) == 1


def test_channel_comment_report_links_to_the_post_and_latest_comment() -> None:
    delivered = ChannelCommentRun(
        id=94,
        rule_id=7,
        source_post_id=515,
        status=ChannelCommentRunStatus.SUCCEEDED,
        sent_steps=2,
        last_comment_message_id=84_003,
        attempt_count=0,
        hidden_mentions_enabled_snapshot=True,
        hidden_mention_resolved_count=6,
    )
    pending = ChannelCommentRun(
        id=95,
        rule_id=7,
        source_post_id=516,
        status=ChannelCommentRunStatus.QUEUED,
        attempt_count=0,
    )

    text = channel_comment_runs_text(
        [delivered, pending],
        channel_ref="@proxima_channel",
        reaction_summaries={
            94: ChannelCommentReactionSummary(succeeded=1),
            95: ChannelCommentReactionSummary(queued=1, cancelled=1),
        },
    )

    assert 'href="https://t.me/proxima_channel/515"' in text
    assert 'href="https://t.me/proxima_channel/515?comment=84003"' in text
    assert 'href="https://t.me/proxima_channel/516"' in text
    assert "Открыть последний комментарий" in text
    assert "Отметки: добавлено <b>6</b>" in text
    assert "Реакции: готово: <b>1</b>" in text
    assert "Реакции: ожидают: <b>1</b> · отменено: <b>1</b>" in text


def test_channel_comment_report_hides_legacy_album_diagnostic() -> None:
    album_item = ChannelCommentRun(
        id=96,
        rule_id=7,
        source_post_id=517,
        status=ChannelCommentRunStatus.SKIPPED,
        error_code="album_duplicate",
        error="??????? ?????-??????? ?????????? ????? ????? ????????????",
    )

    text = channel_comment_runs_text([album_item], channel_ref="@proxima_channel")

    assert "элемент альбома обработан общим комментарием" in text
    assert "????" not in text


def test_channel_comment_report_includes_an_actionable_error_hint() -> None:
    pending = ChannelCommentRun(
        id=97,
        rule_id=7,
        source_post_id=518,
        status=ChannelCommentRunStatus.QUEUED,
        error_code="proxy_unavailable",
        error="proxy route is unavailable",
    )

    text = channel_comment_runs_text([pending], channel_ref="@proxima_channel")

    assert "не найден рабочий прокси для аккаунта" in text
    assert "Добавьте или включите рабочий HTTP(S)/SOCKS5-прокси" in text


def test_channel_comment_txt_import_parser_ignores_comment_lines() -> None:
    assert _parse_channel_comment_refs(
        "# основная подборка\n@news_channel\nhttps://t.me/s/second_channel/123\n"
    ) == ["@news_channel", "https://t.me/s/second_channel/123"]


def test_message_edit_comment_card_and_mode_picker_show_both_texts() -> None:
    rule = ChannelCommentRule(
        id=92,
        name="Message Edit",
        channel_ref="@news_channel",
        mode=ChannelCommentMode.MESSAGE_EDIT,
        primary_account_id=7,
        primary_text="Финальный комментарий",
        message_edit_initial_text="Ага",
        message_edit_delay_seconds=15 * 60,
    )
    view = ChannelCommentRuleView(
        rule=rule,
        queued=1,
        running=0,
        succeeded=0,
        failed=0,
        skipped=0,
    )
    callbacks = {
        button.callback_data
        for row in channel_comment_mode_menu().inline_keyboard
        for button in row
    }
    labels = {button.text for row in channel_comment_mode_menu().inline_keyboard for button in row}
    details = channel_comment_details_text(view, {7: "main-account"})

    assert "comments:draft:message_edit" in callbacks
    assert "Исходный текст" in details
    assert "Текст после редактирования" in details
    assert "15 мин" in details
    assert all("Message Edit" not in label for label in labels)


def test_comment_mention_skip_reasons_do_not_depend_on_reactions() -> None:
    rule = ChannelCommentRule(
        id=94,
        name="Mentions",
        channel_ref="@news_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=7,
        primary_text="Комментарий",
        hidden_mentions_enabled=True,
        reactions_enabled=False,
    )
    view = ChannelCommentRuleView(
        rule=rule,
        queued=0,
        running=0,
        succeeded=1,
        failed=0,
        skipped=0,
        mentions_skipped_posts=1,
        mention_skip_reasons=(("mention_reservation_conflict", 1),),
    )

    details = channel_comment_details_text(view, {7: "main-account"})

    assert "параллельная задача" in details
    assert "worker" not in details


def test_participant_reaction_card_is_clear_and_hides_empty_error() -> None:
    rule = ParticipantReactionRule(
        id=95,
        name="Активные чаты",
        target_list_id=3,
        account_ids_json="[7, 8]",
        emoji="🔥",
        sample_numerator=1,
        sample_denominator=10,
        enabled=True,
    )
    view = ParticipantReactionRuleView(
        rule=rule,
        target_list_name="Крипто",
        target_count=12,
        queued=2,
        running=1,
        succeeded=9,
        failed=0,
        skipped=3,
    )

    details = participant_reaction_details_text(view, {7: "Alice", 8: "Bob"})
    overview = participant_reactions_text([view], 1)

    assert "примерно 1 из 10 новых сообщений" in details
    assert "Только эти аккаунты" in details
    assert "строго из выбранного набора" in overview
    assert "Последняя ошибка" not in details
    assert "Авто-реакции в чатах" in overview


def test_operator_errors_show_plain_reason_and_next_action() -> None:
    rule = ParticipantReactionRule(
        id=951,
        name="Реакции",
        target_list_id=3,
        account_ids_json="[7]",
        emoji="🔥",
        enabled=True,
        last_error="reaction: FORBIDDEN",
        last_error_code="reaction_unavailable",
    )
    view = ParticipantReactionRuleView(
        rule=rule,
        target_list_name="Крипто",
        target_count=1,
        queued=0,
        running=0,
        succeeded=0,
        failed=1,
        skipped=0,
    )
    text = participant_reaction_details_text(view, {7: "Alice"})

    assert "выбранная реакция запрещена" in text
    assert "выбранный Emoji не разрешён" in text
    assert "reaction: FORBIDDEN" in text


def test_run_error_card_separates_reason_action_and_technical_detail() -> None:
    now = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
    run = CampaignRun(
        id=952,
        campaign_id=4,
        target_id=8,
        request_key="ui-error-run",
        status=RunStatus.FAILED,
        next_step=RunStep.PRIMARY,
        attempt_count=1,
        max_attempts=5,
        cycle_index=0,
        scheduled_for=now,
        error="send: PremiumAccountRequiredError",
        error_code="target_unavailable",
    )

    text = run_details_text(run, [])

    assert "Канал или группа недоступны" in text
    assert "Проверьте адрес, доступность чата" in text
    assert "send: PremiumAccountRequiredError" in text


def test_order_intro_uses_consistent_polite_address() -> None:
    text = order_type_text()

    assert "Сначала вы всё увидите" in text
    assert "Сначала ты" not in text


def test_long_dialogues_expose_every_reaction_step_and_full_count_range() -> None:
    steps = [
        DialogueStep(
            account_id=(index % 3) + 1,
            text=f"Сообщение {index + 1}",
            delay_seconds=0,
            reply_to_previous=index > 0,
        )
        for index in range(20)
    ]
    serialized = serialize_dialogue_steps(steps)
    campaign = Campaign(
        id=96,
        name="Длинный диалог",
        mode=CampaignMode.DIALOGUE,
        primary_account_id=1,
        target_id=4,
        primary_text="Сообщение 1",
        reply_text="",
        dialogue_steps_json=serialized,
        reactions_enabled=True,
        reaction_count=5,
        reaction_dialogue_step=17,
    )
    campaign_menu = campaign_reactions_menu(campaign, 2, len(steps))
    campaign_callbacks = {
        button.callback_data
        for row in campaign_menu.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert {f"campaign:reactions:step:{step}:96:2" for step in range(1, 21)} <= campaign_callbacks
    assert {
        f"campaign:reactions:count:{count}:96:2" for count in range(1, 11)
    } <= campaign_callbacks
    campaign_text = campaign_reactions_text(campaign, len(steps))
    assert "к сообщению № 17 из 20" in campaign_text
    assert "Максимум по составу цепочки: <b>2</b>" in campaign_text
    assert "Реакций будет меньше настройки" in campaign_text

    rule = ChannelCommentRule(
        id=97,
        name="Длинная цепочка комментариев",
        channel_ref="@long_comments",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=1,
        primary_text="Сообщение 1",
        dialogue_steps_json=serialized,
        reactions_enabled=True,
        reaction_count=5,
        reaction_dialogue_step=17,
        reaction_account_ids_json="[1, 2, 3]",
    )
    view = ChannelCommentRuleView(
        rule=rule,
        queued=0,
        running=0,
        succeeded=0,
        failed=0,
        skipped=0,
    )
    comment_menu = channel_comment_reactions_menu(view, page=3)
    comment_callbacks = {
        button.callback_data
        for row in comment_menu.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert {f"comments:reactions:step:97:3:{step}" for step in range(1, 21)} <= comment_callbacks
    assert {f"comments:reactions:count:97:3:{count}" for count in range(1, 11)} <= comment_callbacks
    comment_text = channel_comment_reactions_text(view, {1: "A", 2: "B", 3: "C"})
    assert "Комментарий цепочки: <b>№ 17</b>" in comment_text
    assert "Максимум по выбранному набору: <b>2</b>" in comment_text
    assert "Реакций будет меньше настройки" in comment_text


def test_inline_callback_contracts_fit_telegram_limit() -> None:
    account = Account(
        id=123,
        label="Primary",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
        session_source=AccountSessionSource.TDATA,
        first_name="Alice",
        last_name="Operator",
        has_profile_photo=True,
    )
    target = Target(id=456, chat_ref="@owned", title="Owned", approved_by_operator=True)
    bad_target = Target(
        id=457,
        chat_ref="@blocked_ads",
        title="Blocked ads",
        approved_by_operator=True,
        quality_status="bad",
        quality_score=0,
        quality_attempts=3,
        quality_failures=3,
        quality_banned_accounts=2,
        quality_reason="бан отправки получили 2 аккаунта",
    )
    campaign = Campaign(
        id=789,
        name="Campaign",
        primary_account_id=123,
        reply_account_id=124,
        target_id=456,
        primary_text="First",
        reply_text="Reply",
        reply_delay_seconds=0,
        enabled=True,
    )
    run = CampaignRun(
        id=987,
        campaign_id=789,
        request_key="request-key",
        status=RunStatus.QUEUED,
        next_step=RunStep.PRIMARY,
        attempt_count=0,
        max_attempts=5,
        created_at=datetime(2026, 7, 10, tzinfo=UTC),
        scheduled_for=datetime(2026, 7, 10, tzinfo=UTC),
    )
    launch = LaunchView(
        anchor_run_id=987,
        campaign_id=789,
        campaign_name="Campaign",
        mode=CampaignMode.DIALOGUE,
        total=1,
        queued=1,
        running=0,
        succeeded=0,
        failed=0,
        cancelled=0,
        created_at=datetime(2026, 7, 10, tzinfo=UTC),
        first_scheduled_for=datetime(2026, 7, 10, tzinfo=UTC),
        last_scheduled_for=datetime(2026, 7, 10, tzinfo=UTC),
    )
    target_list = TargetListView(id=333, name="Owned list", item_count=1, approved_count=1)
    template = MessageTemplate(
        id=654,
        name="Pair",
        primary_text="First",
        reply_text="Reply",
        enabled=True,
    )
    proxy = ProxyEndpoint(
        id=321,
        label="EU proxy",
        protocol=ProxyProtocol.HTTP,
        host="proxy.example",
        port=8080,
        fingerprint="abc",
        enabled=True,
        status=ProxyStatus.HEALTHY,
    )
    variant = BroadcastContentVariant(
        variant_id="ab12cd34ef",
        name="Offer B",
        text="Advertisement B",
        media=(StoredMedia(path="media/a.jpg", kind="photo", name="a.jpg", size=100),),
    )
    markups = [
        main_menu(),
        order_type_menu(),
        order_launch_confirmation(),
        order_interval_presets(),
        order_duration_presets(),
        order_spread_presets(),
        order_draft_menu(
            {
                "order_mode": "broadcast",
                "target_id": target.id,
                "sender_account_ids": [account.id],
                "primary_text": "Ad",
                "smart_timer_mode": True,
            }
        ),
        order_advanced_menu(
            {
                "order_mode": "broadcast",
                "smart_timer_mode": True,
            }
        ),
        order_sender_picker([account], {account.id}),
        order_audience_source_menu(standalone_count=1, list_count=1),
        order_audience_source_menu(standalone_count=0, list_count=0),
        order_target_picker(Page([target], 0, 7, 1)),
        order_target_list_picker(Page([target_list], 0, 7, 1)),
        order_target_list_picker(
            Page(
                [TargetListView(id=334, name="Disabled list", item_count=1, approved_count=0)],
                0,
                7,
                1,
            )
        ),
        order_target_list_activation_menu(target_list.id),
        order_audience_search_menu("chats", [("Owned", f"order:audience:target:{target.id}")]),
        accounts_menu(Page([account], 0, 7, 1)),
        account_details_menu(account, 0, has_proxy=True),
        account_name_confirmation(account.id, 0),
        account_avatar_menu(account.id, 0, has_avatar=True),
        account_avatar_delete_confirmation(account.id, 0),
        account_session_clear_confirmation(account.id, 0),
        targets_menu(Page([target], 0, 7, 1)),
        target_details_menu(target, 0),
        bad_targets_menu(Page([bad_target], 0, 7, 1)),
        bad_target_details_menu(bad_target, 0),
        target_cooldown_picker(),
        target_delete_confirmation(target.id, 0),
        target_list_details_menu(target_list, 0),
        target_list_targets_menu(target_list.id, 0, Page([target], 0, 7, 1)),
        target_list_target_details_menu(target, target_list.id, 0, 0),
        target_list_target_remove_confirmation(target.id, target_list.id, 0, 0),
        target_list_delete_confirmation(target_list.id, 0),
        campaigns_menu(Page([campaign], 0, 7, 1)),
        campaign_details_menu(campaign, 0),
        campaign_advanced_menu(campaign, 0),
        campaign_variants_menu(campaign, [variant], 0),
        campaign_variant_menu(campaign, variant, variant_count=2, page=0),
        campaign_variant_delete_confirmation(campaign.id, variant.variant_id, 0),
        campaign_edit_target_source_menu(
            standalone_count=1,
            list_count=1,
            campaign_id=campaign.id,
            campaign_page=0,
        ),
        campaign_edit_target_picker(
            Page([target], 0, 7, 1),
            kind="chats",
            campaign_id=campaign.id,
            campaign_page=0,
        ),
        campaign_standalone_target_picker(Page([target], 0, 10, 1)),
        campaign_target_list_picker(Page([target_list], 0, 10, 1)),
        campaign_edit_target_picker(
            Page([target_list], 0, 7, 1),
            kind="lists",
            campaign_id=campaign.id,
            campaign_page=0,
        ),
        campaign_repeat_mode_picker(),
        campaign_one_off_window_presets(),
        campaign_smart_duration_presets(),
        campaign_smart_interval_presets(),
        campaign_smart_duration_presets(cancel_callback="cschedule:cancel"),
        campaign_smart_interval_presets(cancel_callback="cschedule:cancel"),
        campaign_repeat_interval_presets("crepeat"),
        campaign_repeat_interval_presets("cschedule"),
        campaign_repeat_count_presets("crepeat"),
        campaign_repeat_count_presets("cschedule"),
        campaign_schedule_mode_picker(),
        campaign_schedule_mode_picker(smart_timer_mode=True),
        campaign_delete_confirmation(campaign.id, 0),
        runs_menu(Page([run], 0, 7, 1)),
        launches_menu(Page([launch], 0, 7, 1)),
        launch_details_menu(launch, 0),
        launch_cancel_confirmation(launch.anchor_run_id, 0),
        templates_menu(Page([template], 0, 7, 1)),
        template_details_menu(template, 0),
        template_delete_confirmation(template.id, 0),
        proxies_menu(
            ProxyPage(
                [ProxyView(proxy, "EU", None, None)],
                0,
                7,
                1,
                pool_total=1,
                pool_healthy=1,
            )
        ),
        proxy_details_menu(ProxyView(proxy, "EU", None, None), 0),
        proxy_group_delete_confirmation(321),
        campaign_run_confirmation(789, 0),
        campaign_stop_all_confirmation(),
        settings_menu(),
    ]
    callback_data = [
        button.callback_data
        for markup in markups
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data is not None
    ]
    assert callback_data
    assert all(len(value.encode("utf-8")) <= 64 for value in callback_data)
    assert "campaign:stopall:ask" in callback_data
    assert "campaign:stopall:confirm" in callback_data
    button_labels = [
        button.text for markup in markups for row in markup.inline_keyboard for button in row
    ]
    assert all(not label.startswith("‹ ") for label in button_labels)
    assert not ({"Отмена", "Отменить", "Назад", "Обновить"} & set(button_labels))


def test_order_audience_always_offers_safe_list_creation() -> None:
    markup = order_audience_source_menu(standalone_count=0, list_count=0)
    callbacks = {
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "order:audience:create-list" in callbacks
    assert "order:back" in callbacks

    activation = order_target_list_activation_menu(123)
    assert activation.inline_keyboard[0][0].callback_data == "order:audience:list:activate:123"


def test_bad_target_card_explains_rating_and_manual_restore() -> None:
    target = Target(
        id=91,
        chat_ref="@no_ads_here",
        title="No ads here",
        approved_by_operator=True,
        quality_status="bad",
        quality_score=0,
        quality_attempts=5,
        quality_successes=1,
        quality_failures=4,
        quality_banned_accounts=2,
        quality_reason="успешно только 1 из 5 подтверждённых попыток",
    )
    page = Page([target], 0, 7, 1)

    text = bad_targets_text(page)
    menu = bad_target_details_menu(target, 0)

    assert "No ads here" in text
    assert "@no_ads_here" in text
    assert "рейтинг <b>0/100</b>" in text
    assert "успешно 1" in text
    assert "ошибок 4" in text
    assert "банов 2" in text
    assert "успешно только 1 из 5" in text
    assert menu.inline_keyboard[0][0].text == "♻️ Вернуть чат в работу"
    assert menu.inline_keyboard[0][0].callback_data == "target:quality:restore:91:0:bad"


def test_target_list_catalog_has_filters_bulk_actions_and_preserves_context() -> None:
    target_list = TargetListView(
        id=55,
        name="Large ads folder",
        item_count=243,
        approved_count=180,
        bad_count=8,
        problem_count=5,
    )
    target = Target(
        id=701,
        chat_ref="@problem_room",
        title="Problem room",
        approved_by_operator=True,
        last_error_code="target_unavailable",
    )
    page = Page([target], 12, 10, 128)

    text = target_list_targets_text(target_list, page, filter_name="problem")
    menu = target_list_targets_menu(
        target_list.id,
        2,
        page,
        filter_name="problem",
        target_list=target_list,
    )
    details = target_list_target_details_menu(
        target,
        target_list.id,
        2,
        12,
        "problem",
    )
    callbacks = {
        button.callback_data
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data
    }
    standalone_callbacks = {
        button.callback_data
        for row in targets_menu(Page([target], 4, 7, 40)).inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "ФИЛЬТР: С ОШИБКАМИ" in text
    assert "Страница <b>13/13</b>" in text
    assert "показано <b>121–128</b>" in text
    assert "tlf:55:2:enabled:0" in callbacks
    assert "tlf:55:2:disabled:0" in callbacks
    assert "tls:55:2" in callbacks
    assert "tlbulk:revoke:55:2:problem:12" in callbacks
    assert "tlall:approve:55:2:problem:12" in callbacks
    assert "tlall:revoke:55:2:problem:12" in callbacks
    assert "targets:bulk:approve:4" in standalone_callbacks
    assert "targets:bulk:revoke:4" in standalone_callbacks
    assert details.inline_keyboard[-1][0].callback_data == "tlf:55:2:problem:12"


def test_variant_cards_explain_rotation_album_and_safe_preview() -> None:
    campaign = Campaign(
        id=42,
        name="Offers",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=1,
        primary_text="Primary",
    )
    variants = [
        BroadcastContentVariant("primary", "Основной", "Primary"),
        BroadcastContentVariant(
            "offerb1234",
            "Offer B",
            "Secondary",
            media=(StoredMedia("media/a.jpg", "photo", "a.jpg", 1024),),
        ),
    ]

    list_text = campaign_variants_text(campaign, variants)
    detail_text = campaign_variant_text(campaign, variants[1])

    assert "A → B → C → A" in list_text
    assert "2 из 2" in list_text
    assert "1 из 10 файлов" in detail_text
    assert "ничего не публикует" in detail_text


def test_contact_variant_has_clear_card_controls_and_report_label() -> None:
    campaign = Campaign(
        id=43,
        name="Contact offers",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=1,
        primary_text="Primary",
    )
    contact = BroadcastContentVariant(
        variant_id="contact123",
        name="Запись к Алисе",
        text="",
        contact=StoredContact("+79991234567", "Алиса", "Соколова"),
    )

    list_text = campaign_variants_text(campaign, [contact])
    detail_text = campaign_variant_text(campaign, contact)
    markup = campaign_variant_menu(campaign, contact, variant_count=2, page=0)
    labels = [button.text for row in markup.inline_keyboard for button in row]
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]

    assert "📇 контакт" in list_text
    assert "нативная карточка Telegram" in detail_text
    assert "+79991234567" in detail_text
    assert "✏️ Изменить контакт" in labels
    assert "➕ Добавить медиа в альбом" not in labels
    assert any(value.startswith("campaign:variant:contact:edit:") for value in callbacks)
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_order_card_only_launches_when_required_fields_are_ready() -> None:
    incomplete = order_draft_menu(
        {
            "order_mode": "broadcast",
            "target_id": None,
            "target_list_id": None,
            "sender_account_ids": [],
            "primary_text": "",
        }
    )
    incomplete_callbacks = {
        button.callback_data
        for row in incomplete.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "order:check" in incomplete_callbacks
    assert "order:launch" not in incomplete_callbacks
    assert "order:save" not in incomplete_callbacks

    ready_broadcast = order_draft_menu(
        {
            "order_mode": "broadcast",
            "target_list_id": 11,
            "sender_account_ids": [1, 2],
            "primary_text": "Promotion",
            "smart_timer_mode": True,
        }
    )
    broadcast_callbacks = {
        button.callback_data
        for row in ready_broadcast.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert {"order:launch", "order:save"} <= broadcast_callbacks
    assert {
        "order:profile:active",
        "order:profile:balanced",
        "order:profile:gentle",
        "order:advanced",
    } <= broadcast_callbacks
    advanced_callbacks = {
        button.callback_data
        for row in order_advanced_menu(
            {
                "order_mode": "broadcast",
                "smart_timer_mode": True,
            }
        ).inline_keyboard
        for button in row
        if button.callback_data
    }
    assert {
        "order:edit:visibility",
        "order:edit:maxwait",
        "order:activity:toggle",
        "order:edit:check",
        "order:edit:interval",
        "order:edit:duration",
    } <= advanced_callbacks
    assert "публикация сервиса остаётся последней" in order_advanced_text(
        {"smart_timer_mode": True}
    )

    one_account_dialogue = order_draft_menu(
        {
            "order_mode": "dialogue",
            "target_id": 7,
            "dialogue_steps": [
                {"account_id": 1, "text": "One"},
                {"account_id": 1, "text": "Two"},
            ],
        }
    )
    dialogue_callbacks = {
        button.callback_data
        for row in one_account_dialogue.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "order:launch" not in dialogue_callbacks


def test_media_only_order_is_ready_and_explains_its_next_action() -> None:
    draft = {
        "order_mode": "broadcast",
        "order_name": "Voice update",
        "target_list_id": 11,
        "audience_label": "Основные чаты",
        "sender_account_ids": [1],
        "sender_labels": ["Основной"],
        "primary_text": "",
        "primary_media_path": "uploads/voice.ogg",
        "primary_media_kind": "voice",
    }
    markup = order_draft_menu(draft)
    labels = [button.text for row in markup.inline_keyboard for button in row]
    callbacks = {button.callback_data for row in markup.inline_keyboard for button in row}
    text = order_draft_text(draft)

    assert "order:launch" in callbacks
    assert "✅ Пост · голосовое" in labels
    assert "🟢 Черновик готов к проверке" in text
    assert "будет отправлено только вложение" in text


def test_home_screens_give_a_next_step_and_consistent_workflow_context() -> None:
    dashboard = DirectMessageDashboard(
        list_count=0,
        subscriber_count=0,
        premium_count=0,
        username_count=0,
        blacklist_count=0,
        campaign_count=0,
        active_launches=0,
        queued_tasks=0,
        running_tasks=0,
        succeeded_tasks=0,
        failed_tasks=0,
        skipped_tasks=0,
    )

    assert "Следующий шаг" in direct_messages_home_text(dashboard)
    assert "Импортируйте первую базу" in direct_messages_home_text(dashboard)
    assert "Как это работает" in subscriber_invites_home_text()
    assert "вашей базы" in subscriber_invites_home_text()
    assert "Как это работает" in channel_comments_text([], 0)
    assert "С чего начать" in channel_comments_text([], 0)
    assert "Как это работает" in participant_reactions_text([], 0)


def test_ready_order_still_requires_a_separate_launch_confirmation() -> None:
    draft = {
        "order_mode": "broadcast",
        "order_name": "24 hour ads",
        "audience_label": "Base · approved chats",
        "audience_target_count": 12,
        "target_list_id": 11,
        "sender_account_ids": [1, 2],
        "sender_labels": ["Primary", "Reserve"],
        "sender_mode": BroadcastSenderMode.INDEPENDENT.value,
        "primary_text": "Promotion",
        "smart_timer_mode": True,
        "smart_duration_hours": 24,
        "smart_no_timer_interval_minutes": 15,
        "auto_sender_failover": True,
    }

    draft_callbacks = {
        button.callback_data
        for row in order_draft_menu(draft).inline_keyboard
        for button in row
        if button.callback_data
    }
    confirmation = order_launch_confirmation()
    confirmation_callbacks = {
        button.callback_data
        for row in confirmation.inline_keyboard
        for button in row
        if button.callback_data
    }
    confirmation_text = order_draft_text(
        draft,
        launch_confirmation=True,
        dry_run=False,
    )

    assert "order:launch" in draft_callbacks
    assert "order:launch:confirm" not in draft_callbacks
    assert confirmation_callbacks == {"order:launch:confirm", "order:back"}
    assert "Рабочая отправка" in confirmation_text
    assert "После подтверждения" in confirmation_text
    assert "публикаций в первом проходе — <b>24</b>" in confirmation_text
    assert "12 чатов × 2 аккаунтов" in confirmation_text
    assert "сообщения попадут в очередь только после успешной проверки" in confirmation_text
    assert any(
        button.text == "🔎 Проверить готовность"
        for row in confirmation.inline_keyboard
        for button in row
    )


def test_order_menu_defaults_to_an_active_distributed_broadcast() -> None:
    draft = {
        "order_mode": "broadcast",
        "target_list_id": 11,
        "sender_account_ids": [1, 2, 3],
        "primary_text": "Promotion",
    }
    markup = order_draft_menu(draft)
    labels = [button.text for row in markup.inline_keyboard for button in row]

    assert "🤖 Умный выбор · 1 поток/чат" in labels
    assert "♻️ Активная · до 24 ч" in labels
    assert "👁 После 10 новых сообщ." in labels
    assert "✅ ⚡ Активная" in labels
    assert "Активная рассылка · повторять до конца срока" in order_draft_text(draft)
    assert "Активная рассылка выбрана по умолчанию" in order_type_text()
    callbacks = {
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "order:schedule:choose" in callbacks
    schedule_labels = [
        button.text for row in order_schedule_mode_menu(draft).inline_keyboard for button in row
    ]
    assert "✅ ♻️ Активная · до 24 ч" in schedule_labels


def test_sender_picker_explains_the_selected_mode_and_first_pass_volume() -> None:
    independent = _order_sender_picker_text(
        {
            "sender_mode": "independent",
            "audience_target_count": 9,
        },
        selected_count=4,
    )
    distributed = _order_sender_picker_text(
        {
            "sender_mode": "distributed",
            "audience_target_count": 9,
        },
        selected_count=4,
    )

    assert "Независимый режим" in independent
    assert "9 чатов × 4 аккаунтов = 36 направлений" in independent
    assert "Умный выбор" in distributed
    assert "один поток" in distributed


def test_independent_preflight_shows_ready_pairs_without_blocking_them() -> None:
    checked_at = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    campaign = Campaign(
        id=17,
        name="Independent partial",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=2,
        primary_text="Post",
        reply_text="",
        sender_mode=BroadcastSenderMode.INDEPENDENT,
    )
    summary = CampaignPreflightSummary(
        campaign_id=campaign.id,
        account_count=2,
        target_count=2,
        checked_pairs=4,
        ready_pairs=2,
        member_targets=2,
        joinable_targets=0,
        blocked_targets=0,
        unavailable_targets=0,
        oldest_checked_at=checked_at,
        newest_checked_at=checked_at,
        routable_pairs=2,
        independent_mode=True,
    )

    preflight = campaign_preflight_text(summary)
    review = campaign_launch_review_text(campaign, summary, planned_tasks=2)

    assert summary.launchable_units == 2
    assert "2/4" in preflight
    assert "готовые пары продолжат работу" in preflight
    assert "2 из 4 направлений аккаунт–чат" in review


def test_preflight_discloses_the_auto_join_budget_before_launch() -> None:
    checked_at = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    summary = CampaignPreflightSummary(
        campaign_id=19,
        account_count=2,
        target_count=3,
        checked_pairs=6,
        ready_pairs=2,
        member_targets=1,
        joinable_targets=2,
        blocked_targets=0,
        unavailable_targets=0,
        oldest_checked_at=checked_at,
        newest_checked_at=checked_at,
        auto_join_enabled=True,
        routable_pairs=4,
        joinable_pairs=3,
    )

    text = campaign_preflight_text(summary)

    assert "до <b>2</b> вступлений" in text
    assert "дневной лимит" in text


def test_preflight_result_exposes_a_safe_cached_launch_confirmation() -> None:
    checked_at = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    summary = CampaignPreflightSummary(
        campaign_id=91,
        account_count=2,
        target_count=3,
        checked_pairs=6,
        ready_pairs=4,
        member_targets=3,
        joinable_targets=0,
        blocked_targets=0,
        unavailable_targets=0,
        oldest_checked_at=checked_at,
        newest_checked_at=checked_at,
        routable_pairs=4,
        ready_now_targets=3,
    )
    text = campaign_preflight_text(summary)
    callbacks = {
        button.callback_data
        for row in campaign_run_confirmation(91, 2, ready_targets=3).inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "подтвердите запуск готовых направлений кнопкой ниже" in text
    assert "Повторной Telegram-проверки не будет" in text
    assert "Зачем нужна эта проверка" in text
    assert "без публикаций и вступлений" in text
    assert "campaign:run:confirm:91:2" in callbacks


def test_active_order_mode_is_explicit_and_supports_twelve_messages() -> None:
    active = {
        "order_mode": "broadcast",
        "smart_timer_mode": True,
        "smart_duration_hours": 24,
    }
    picker = order_schedule_mode_menu(active)
    buttons = [button for row in picker.inline_keyboard for button in row]
    labels = [button.text for button in buttons]
    callbacks = {button.callback_data for button in buttons if button.callback_data}

    assert "✅ ♻️ Активная · до 24 ч" in labels
    assert {"order:schedule:set:once", "order:schedule:set:active"} <= callbacks
    assert "10 новых сообщений после нашего поста" in order_advanced_text(active)
    assert "не пытается угадывать" in order_advanced_text(active)
    assert "Активная рассылка" in order_schedule_mode_text(active)
    assert "order:preset:visibility:12" in {
        button.callback_data
        for row in order_visibility_presets().inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "order:preset:maxwait:0" in {
        button.callback_data
        for row in order_visibility_wait_presets().inline_keyboard
        for button in row
        if button.callback_data
    }


def test_distributed_launch_volume_does_not_multiply_by_sender_count() -> None:
    confirmation_text = order_draft_text(
        {
            "order_mode": "broadcast",
            "order_name": "Safe launch",
            "audience_label": "Base · 40 chats",
            "audience_target_count": 40,
            "sender_account_ids": [1, 2, 3, 4],
            "sender_labels": ["A", "B", "C", "D"],
            "sender_mode": "distributed",
            "primary_text": "Promotion",
            "smart_timer_mode": False,
        },
        launch_confirmation=True,
        dry_run=False,
    )

    assert "публикаций в первом проходе — <b>40</b>" in confirmation_text
    assert "4 аккаунтов разделят чаты между собой" in confirmation_text
    assert "автоматических повторов не будет" in confirmation_text


def test_dashboard_explicitly_reports_dry_run() -> None:
    text = dashboard_text(
        DashboardStats(
            accounts_total=2,
            accounts_active=2,
            targets_total=1,
            targets_approved=1,
            target_lists_total=0,
            templates_total=1,
            campaigns_total=1,
            queued_runs=0,
            running_runs=0,
            succeeded_runs=3,
            failed_runs=0,
            standalone_targets_total=1,
            standalone_targets_approved=1,
        ),
        dry_run=True,
    )
    assert "Тестовый режим" in text
    assert "Аккаунты" in text
    assert "<b>🟢 Всё готово к отправке</b>" in text
    assert "Сейчас" in text
    assert "За всё время" in text


def test_worker_campaign_scope_is_visible_in_dashboard_and_campaigns() -> None:
    stats = DashboardStats(
        accounts_total=1,
        accounts_active=1,
        targets_total=1,
        targets_approved=1,
        target_lists_total=0,
        templates_total=1,
        campaigns_total=1,
        queued_runs=1,
        running_runs=0,
        succeeded_runs=0,
        failed_runs=0,
    )
    dashboard = dashboard_text(stats, dry_run=True, worker_campaign_scope=frozenset({16, 19}))
    campaign = Campaign(
        id=19,
        name="Scoped campaign",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=1,
        primary_text="Message",
        spread_seconds=0,
        repeat_count=1,
    )
    campaigns = campaigns_text(Page([campaign], 0, 7, 1), worker_campaign_scope=frozenset({16}))

    assert "#16, #19" in dashboard
    assert "Задания остальных рассылок останутся в очереди" in dashboard
    assert "#16" in campaigns
    assert "Другие запуски не начнутся" in campaigns


def test_dashboard_shows_degraded_background_worker_with_last_error() -> None:
    text = dashboard_text(
        DashboardStats(
            accounts_total=1,
            accounts_active=1,
            targets_total=1,
            targets_approved=1,
            target_lists_total=0,
            templates_total=1,
            campaigns_total=1,
            queued_runs=0,
            running_runs=0,
            succeeded_runs=0,
            failed_runs=0,
            standalone_targets_total=1,
            standalone_targets_approved=1,
        ),
        dry_run=True,
        worker_health=(
            WorkerHealthSnapshot(
                name="participant-reactions",
                state="degraded",
                consecutive_failures=3,
                restart_count=0,
                last_success_at=None,
                last_error="database unavailable <retry>",
            ),
        ),
    )

    assert "Фоновые сервисы деградировали" in text
    assert "participant-reactions" in text
    assert "database unavailable &lt;retry&gt;" in text


def test_account_profile_card_shows_telegram_name_and_avatar() -> None:
    account = Account(
        id=7,
        label="Profile account",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
        session_source=AccountSessionSource.TDATA,
        first_name="Alice <Main>",
        last_name="Operator",
        has_profile_photo=True,
        sent_today=143,
        day_window_start=datetime.now(UTC),
    )

    proxy = ProxyEndpoint(
        id=17,
        label="Account route",
        protocol=ProxyProtocol.SOCKS5,
        host="proxy.example",
        port=1080,
        fingerprint="f" * 64,
        status=ProxyStatus.HEALTHY,
        latency_ms=51,
        last_checked_at=datetime.now(UTC),
    )
    details = account_details_text(
        account,
        proxy_slot="socks5://proxy.example:1080",
        proxy_view=ProxyView(proxy, None),
        daily_limit=200,
    )
    avatar = account_avatar_text(account)

    assert "Alice &lt;Main&gt; Operator" in details
    assert "Аватар Telegram: <b>установлен</b>" in details
    assert "За 24 часа: <b>143/200</b>" in details
    assert "осталось <b>57</b>" in details
    assert "🟢 рабочий" in details
    assert "Прокси проверен:" in details
    assert "Текущий аватар: <b>🟢 установлен</b>" in avatar
    callbacks = {
        button.callback_data
        for row in account_details_menu(account, 0, has_proxy=True).inline_keyboard
        for button in row
    }
    assert {
        "account:label:edit:7:0",
        "account:name:edit:7:0",
        "account:avatar:menu:7:0",
    } <= callbacks


def test_account_card_explains_protective_spambot_pause_without_enable_button() -> None:
    account = Account(
        id=71,
        label="restricted account",
        ownership_confirmed=True,
        status=AccountStatus.PAUSED,
        session_source=AccountSessionSource.SESSION_STRING,
        session_encrypted="encrypted",
        api_id=1,
        api_hash_encrypted="encrypted",
        authorized=True,
        cooldown_reason="spambot_temp_ban",
        spambot_status="error",
        spambot_checked_at=datetime.now(UTC),
        last_error="@SpamBot: latest check timed out",
        spambot_response_json=(
            '{"check_error_code":"proxy_unavailable","previous":'
            '{"restriction_scope":"direct_outreach_and_invites",'
            '"allows_reply_to_inbound":true}}'
        ),
    )

    details = account_details_text(account, proxy_slot=None, daily_limit=200)
    callbacks = {
        button.callback_data
        for row in account_details_menu(account, 0, has_proxy=False).inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "защитная пауза @SpamBot" in details
    assert "Последняя диагностика" in details
    assert "нет доступного подключения через прокси аккаунта" in details
    assert "Telegram запретил первые личные сообщения незнакомым" in details
    assert "Это не разрешение на автоматическую рассылку" in details
    assert "account:toggle:71:0" not in callbacks


def test_bulk_account_session_check_ui_offers_delete_only_for_revoked_sessions() -> None:
    ready = AccountBulkCheckEntry(
        account_id=7,
        label="ready-account",
        result=AccountCheckResult(7, True, None, None, None, None),
    )
    revoked = AccountBulkCheckEntry(
        account_id=8,
        label="revoked-account",
        result=AccountCheckResult(8, False, None, None, None, "session revoked", "account_dead"),
        error="session revoked",
    )
    unavailable = AccountBulkCheckEntry(
        account_id=9,
        label="network-account",
        result=AccountCheckResult(9, True, None, None, None, "network timeout"),
        error="network timeout",
    )
    summary = AccountBulkCheckSummary((ready, revoked, unavailable))
    account = Account(
        id=7,
        label="ready-account",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
    )
    callbacks = {
        button.callback_data
        for row in accounts_menu(Page([account], 0, 7, 1)).inline_keyboard
        for button in row
        if button.callback_data
    }
    result_callbacks = {
        button.callback_data
        for row in account_bulk_check_menu([(8, "revoked-account")], page=2).inline_keyboard
        for button in row
        if button.callback_data
    }
    text = account_bulk_check_text(summary)

    assert "account:check:all:0" in callbacks
    assert "account:spambot:all:0" in callbacks
    assert "account:check:purge:ask:2" in result_callbacks
    assert "account:spambot:all:2" in result_callbacks
    assert "account:delete:ask:8:2" in result_callbacks
    assert "account:delete:ask:9:2" not in result_callbacks
    assert "Сессия недействительна: <b>1</b>" in text
    assert "Не удалять автоматически" in text


def test_account_name_catalog_ui_has_preview_bulk_action_and_real_change_warning() -> None:
    account = Account(
        id=7,
        label="teen-profile",
        ownership_confirmed=True,
        status=AccountStatus.ACTIVE,
        session_source=AccountSessionSource.TDATA,
        session_encrypted="encrypted",
        api_id=123,
        api_hash_encrypted="hash",
        authorized=True,
    )
    overview = account_name_catalog_text([account])
    preview = account_generated_names_preview_text(
        [
            {
                "account_id": 7,
                "label": "teen-profile",
                "first_name": "Лера",
                "last_name": "М.",
            }
        ],
        locale="mixed",
    )
    result = account_names_result_text(
        updated_count=1,
        failures=[("broken <account>", "session <expired>")],
    )
    list_menu = accounts_menu(Page([account], 0, 7, 1))
    list_callbacks = {
        button.callback_data
        for row in list_menu.inline_keyboard
        for button in row
        if button.callback_data
    }
    style_menu = account_name_catalog_menu(account_id=7, page=0)
    style_callbacks = {
        button.callback_data
        for row in style_menu.inline_keyboard
        for button in row
        if button.callback_data
    }
    confirmation = account_generated_names_confirmation(
        locale="mixed",
        account_id=7,
        page=0,
    )

    assert "Смешанный: <b>10382</b> вариантов" in overview
    assert "даже при включённом тестовом режиме рассылок" in overview
    assert "teen-profile</b> → Лера М." in preview
    assert "реальное изменение публичных профилей" in preview
    assert "account:names:menu" in list_callbacks
    source_menu = account_import_sources_menu(page=3)
    source_callbacks = {
        button.callback_data
        for row in source_menu.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "account:import:menu:0" in list_callbacks
    assert {
        "account:import:archive:3",
        "account:import:tdata:3",
        "account:import:session:3",
        "account:import:json:3",
        "account:import:string:3",
        "menu:accounts:3",
    } <= source_callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in source_callbacks)
    assert "account:names:preview:mixed:7:0" in style_callbacks
    assert confirmation.inline_keyboard[0][0].callback_data == "account:names:apply"
    assert "broken &lt;account&gt;" in result
    assert "session &lt;expired&gt;" in result


def test_target_folder_account_picker_is_paginated_for_large_account_pools() -> None:
    accounts = [
        Account(id=index, label=f"Folder account {index}", status=AccountStatus.ACTIVE)
        for index in range(1, 26)
    ]

    menu = target_folder_account_picker(accounts, page=3, page_size=7)
    callbacks = {
        button.callback_data
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "target:folder:account:25" in callbacks
    assert "target:folder:account:1" not in callbacks
    assert "target:folder:accounts:page:2" in callbacks
    assert "target:folder:accounts:page:3" in callbacks
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_reworked_bot_ui_is_operator_friendly_and_escapes_values() -> None:
    proxy = ProxyEndpoint(
        id=1,
        label="EU <primary>",
        protocol=ProxyProtocol.SOCKS5,
        host="proxy.example",
        port=1080,
        username="operator",
        fingerprint="fingerprint",
        enabled=True,
        status=ProxyStatus.HEALTHY,
        success_count=4,
        failure_count=1,
        consecutive_failures=0,
        latency_ms=52,
    )
    view = ProxyView(proxy, "EU & Main", "worker<1>", "main & backup")
    page = ProxyPage(
        [view],
        0,
        7,
        1,
        pool_total=1,
        pool_healthy=1,
        pool_assigned=1,
    )

    list_text = proxies_text(page)
    details = proxy_details_text(view)
    wizard = wizard_text("Новый <прокси>", 2, 3, "Введите <code>EU</code>.")

    assert "EU &lt;primary&gt;" in list_text
    assert "worker&lt;1&gt;" in list_text
    assert "EU &amp; Main" in details
    assert "Шаг 2 из 3" in wizard
    assert "Новый &lt;прокси&gt;" in wizard
    assert "/cancel" in wizard

    menu = main_menu()
    assert menu.inline_keyboard[0][0].text == "🚀 Создать рассылку"
    assert menu.inline_keyboard[0][0].callback_data == "order:new"
    menu_callbacks = {button.callback_data for row in menu.inline_keyboard for button in row}
    assert "comments:home:0" in menu_callbacks
    menu_labels = {button.text for row in menu.inline_keyboard for button in row}
    assert {
        "💬 Автокомментарии",
        "👍 Авто-реакции в чатах",
        "📝 Шаблоны",
        "⚙️ Настройки и сервис",
    } <= menu_labels
    guide_callbacks = {
        button.callback_data for row in guide_menu().inline_keyboard for button in row
    }
    assert {"menu:accounts:0", "audiences:home", "order:new", "menu:runs:0"} <= guide_callbacks
    assert "Четыре понятных шага" in guide_text()
    system_callbacks = {
        button.callback_data for row in settings_menu().inline_keyboard for button in row
    }
    assert {"maintenance:backup", "maintenance:doctor"} <= system_callbacks


def test_campaign_schedule_pickers_explain_continuous_mode_and_default_interval() -> None:
    create_mode = campaign_repeat_mode_picker()
    create_labels = [button.text for row in create_mode.inline_keyboard for button in row]
    assert "1️⃣ Одноразовая · один пост в чат" in create_labels
    assert "♻️ Умная непрерывная" in create_labels

    interval_picker = campaign_smart_interval_presets()
    default_button = next(
        button
        for row in interval_picker.inline_keyboard
        for button in row
        if button.callback_data == "campaign:smart:interval:5"
    )
    assert "активная" in default_button.text

    legacy_editor_callbacks = {
        button.callback_data
        for row in campaign_schedule_mode_picker().inline_keyboard
        for button in row
    }
    smart_editor_callbacks = {
        button.callback_data
        for row in campaign_schedule_mode_picker(smart_timer_mode=True).inline_keyboard
        for button in row
    }
    assert {"cschedule:smart", "cschedule:continuous"} <= legacy_editor_callbacks
    assert "cschedule:continuous" in smart_editor_callbacks
    assert "cschedule:smart" not in smart_editor_callbacks


def test_campaign_run_confirmation_text_matches_mode() -> None:
    broadcast = Campaign(
        id=1,
        name="Day blast",
        mode=CampaignMode.BROADCAST,
        primary_account_id=10,
        target_list_id=5,
        primary_text="Hi",
        spread_seconds=86400,
        repeat_count=3,
        repeat_interval_seconds=2 * 3600,
        enabled=True,
    )
    dialogue = Campaign(
        id=2,
        name="Thread",
        mode=CampaignMode.DIALOGUE,
        primary_account_id=10,
        reply_account_id=11,
        target_id=3,
        primary_text="Hi",
        reply_text="Bye",
        reply_delay_seconds=90,
        enabled=True,
    )
    broadcast_text = campaign_run_confirmation_text(broadcast)
    dialogue_text = campaign_run_confirmation_text(dialogue)

    assert "📢 посты в чатах" in broadcast_text
    assert "24 ч." in broadcast_text
    assert "3</b> проходов" in broadcast_text
    assert "2 ч." in broadcast_text
    assert "Задержка ответа" not in broadcast_text
    assert "💬 диалог" in dialogue_text
    assert "90 сек." in dialogue_text
    assert "окно отправки" not in dialogue_text
    assert "автоматически проверит доступ" in broadcast_text


def test_campaign_launch_not_ready_text_explains_result_without_technical_guard() -> None:
    checked_at = datetime(2026, 8, 13, 11, 30, tzinfo=UTC)
    text = campaign_launch_not_ready_text(
        CampaignPreflightSummary(
            campaign_id=1,
            account_count=2,
            target_count=1,
            checked_pairs=2,
            ready_pairs=0,
            member_targets=0,
            joinable_targets=0,
            blocked_targets=1,
            unavailable_targets=0,
            oldest_checked_at=checked_at,
            newest_checked_at=checked_at,
        )
    )
    buttons = [
        button.text for row in campaign_run_confirmation(1, 0).inline_keyboard for button in row
    ]

    assert "Отправка пока не началась" in text
    assert "Проверка перед запуском выполнена автоматически" in text
    assert "LIVE-запуском" not in text
    assert any("Подтвердить запуск" in button for button in buttons)


def test_campaign_launch_not_ready_text_explains_a_telegram_safety_pause() -> None:
    resume_at = datetime(2026, 8, 21, 20, 8, tzinfo=UTC)
    summary = CampaignPreflightSummary(
        campaign_id=22,
        account_count=1,
        target_count=1,
        checked_pairs=1,
        ready_pairs=1,
        member_targets=1,
        joinable_targets=0,
        blocked_targets=0,
        unavailable_targets=0,
        oldest_checked_at=resume_at,
        newest_checked_at=resume_at,
        routable_pairs=1,
        ready_now_pairs=0,
        ready_now_targets=0,
        deferred_pairs=1,
        earliest_resume_at=resume_at,
    )

    text = campaign_launch_not_ready_text(summary)

    assert "Telegram временно ограничил отправку" in text
    assert "Обычный запуск их пропустит" in text
    assert "21.08.2026 20:08 UTC" in text


def test_campaign_launch_not_ready_text_keeps_the_exact_queue_planning_reason() -> None:
    checked_at = datetime(2026, 8, 21, 20, 8, tzinfo=UTC)
    summary = CampaignPreflightSummary(
        campaign_id=22,
        account_count=1,
        target_count=1,
        checked_pairs=1,
        ready_pairs=1,
        member_targets=1,
        joinable_targets=0,
        blocked_targets=0,
        unavailable_targets=0,
        oldest_checked_at=checked_at,
        newest_checked_at=checked_at,
        routable_pairs=1,
        ready_now_pairs=1,
        ready_now_targets=1,
    )

    text = campaign_launch_not_ready_text(
        summary,
        planning_reason=("Все готовые маршруты освободятся только после окончания active-окна."),
    )

    assert "Все готовые маршруты освободятся только после окончания active-окна." in text
    assert "срок active-рассылки" in text


def test_campaign_launch_review_discloses_partial_audience_before_queueing() -> None:
    checked_at = datetime(2026, 8, 13, 11, 30, tzinfo=UTC)
    campaign = Campaign(
        id=7,
        name="Partial launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=2,
        target_list_id=None,
        primary_text="Post",
        reply_text="",
    )
    summary = CampaignPreflightSummary(
        campaign_id=campaign.id,
        account_count=2,
        target_count=10,
        checked_pairs=20,
        ready_pairs=5,
        member_targets=3,
        joinable_targets=0,
        blocked_targets=4,
        unavailable_targets=3,
        oldest_checked_at=checked_at,
        newest_checked_at=checked_at,
    )
    text = campaign_launch_review_text(campaign, summary, planned_tasks=3)
    buttons = [
        button.text
        for row in campaign_run_confirmation(campaign.id, 0, ready_targets=3).inline_keyboard
        for button in row
    ]

    assert "3 из 10" in text
    assert "Ожидаемых технических отправок: <b>3</b>" in text
    assert "без повторного Telegram-прогона" in text
    assert any("Подтвердить запуск готовых направлений (3)" in button for button in buttons)


def test_direct_launch_screen_treats_pacing_as_normal_and_surfaces_proxy_action() -> None:
    launch = DirectLaunch(
        id=31,
        campaign_id=7,
        subscriber_list_id=8,
        status=DirectLaunchStatus.RUNNING,
        segment_snapshot=SubscriberSegment.ALL,
        recipient_limit_snapshot=20,
        contact_cooldown_days_snapshot=30,
        account_ids_snapshot_json="[29]",
        primary_text_snapshot="Hello",
        consent_confirmed_at=datetime(2026, 8, 13, tzinfo=UTC),
        total_tasks=20,
        excluded_recent=4,
        excluded_reserved=1,
    )
    pacing_view = DirectLaunchView(
        launch=launch,
        campaign_name="join accounts all · ЛС",
        list_name="join accounts all",
        queued=20,
        running=0,
        succeeded=0,
        failed=0,
        skipped=0,
        cancelled=0,
        last_error_code="account_pacing",
        last_error_status=DirectMessageTaskStatus.QUEUED,
    )
    proxy_view = DirectLaunchView(
        launch=launch,
        campaign_name="join accounts all · ЛС",
        list_name="join accounts all",
        queued=0,
        running=0,
        succeeded=0,
        failed=20,
        skipped=0,
        cancelled=0,
        last_error_code="proxy_unavailable",
        last_error_status=DirectMessageTaskStatus.FAILED,
        last_error_account_label="Doris Cunningham",
    )

    pacing_text = direct_launch_details_text(pacing_view)
    proxy_callbacks = {
        button.callback_data
        for row in direct_launch_details_menu(proxy_view, 0).inline_keyboard
        for button in row
        if button.callback_data
    }

    assert "штатная пауза" in pacing_text
    assert "Последняя ситуация" not in pacing_text
    assert "Технически:" not in pacing_text
    assert "При старте не включены" in pacing_text
    assert "menu:proxies:0" in proxy_callbacks


def test_smart_dialogue_has_complete_schedule_controls() -> None:
    draft = order_draft_menu(
        {
            "order_mode": "dialogue",
            "target_id": 3,
            "dialogue_steps": [
                {"account_id": 10, "text": "Question", "delay_seconds": 0},
                {"account_id": 11, "text": "Answer", "delay_seconds": 30},
            ],
            "smart_timer_mode": True,
            "broadcast_profile": "active",
        }
    )
    draft_callbacks = {
        button.callback_data
        for row in draft.inline_keyboard
        for button in row
        if button.callback_data
    }
    assert {
        "order:edit:chain",
        "order:schedule:choose",
        "order:profile:active",
        "order:advanced",
    } <= draft_callbacks
    advanced_callbacks = {
        button.callback_data
        for row in order_advanced_menu(
            {
                "order_mode": "dialogue",
                "smart_timer_mode": True,
                "broadcast_profile": "active",
            }
        ).inline_keyboard
        for button in row
        if button.callback_data
    }
    assert {
        "order:edit:visibility",
        "order:edit:interval",
        "order:edit:duration",
    } <= advanced_callbacks
    assert "order:failover:toggle" not in advanced_callbacks

    campaign = Campaign(
        id=12,
        name="Smart dialogue",
        mode=CampaignMode.DIALOGUE,
        primary_account_id=10,
        reply_account_id=11,
        target_id=3,
        primary_text="Question",
        reply_text="Answer",
        smart_timer_mode=True,
        smart_duration_seconds=24 * 3600,
        smart_no_timer_interval_seconds=5 * 60,
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=60,
        visibility_max_wait_seconds=15 * 60,
        enabled=True,
    )
    details_callbacks = {
        button.callback_data
        for row in campaign_details_menu(campaign, 0).inline_keyboard
        for button in row
        if button.callback_data
    }
    assert "campaign:profile:active:12:0" in details_callbacks
    assert "campaign:edit:schedule:12:0" in details_callbacks
    assert "campaign:advanced:12:0" in details_callbacks
    assert "campaign:sendermode:toggle:12:0" not in details_callbacks
    detail_labels = {
        button.text for row in campaign_details_menu(campaign, 0).inline_keyboard for button in row
    }
    assert "✓ ⚡ Активная" in detail_labels

    dialogue_schedule_callbacks = {
        button.callback_data
        for row in campaign_schedule_mode_picker(
            smart_timer_mode=False,
            allow_finite_repeats=False,
        ).inline_keyboard
        for button in row
    }
    assert "cschedule:continuous" in dialogue_schedule_callbacks
    assert "cschedule:smart" not in dialogue_schedule_callbacks


def test_long_message_previews_fit_telegram_card_limit() -> None:
    long_text = "x" * 4096
    broadcast = Campaign(
        id=31,
        name="Long broadcast",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=1,
        primary_text=long_text,
        spread_seconds=0,
        repeat_count=24,
        repeat_interval_seconds=3600,
        enabled=True,
    )
    dialogue = Campaign(
        id=32,
        name="Long dialogue",
        mode=CampaignMode.DIALOGUE,
        primary_account_id=1,
        reply_account_id=2,
        target_id=1,
        primary_text=long_text,
        reply_text=long_text,
        reply_delay_seconds=0,
        spread_seconds=0,
        repeat_count=1,
        repeat_interval_seconds=0,
        enabled=True,
    )
    template = MessageTemplate(
        id=33,
        name="Long template",
        primary_text=long_text,
        reply_text=long_text,
    )
    broadcast_data = {
        "name": "Long broadcast",
        "mode": "broadcast",
        "primary_account_id": 1,
        "sender_account_ids": list(range(1, 101)),
        "target_id": 1,
        "primary_text": long_text,
        "spread_hours": 24,
        "repeat_count": 24,
        "repeat_interval_minutes": 60,
    }
    dialogue_data = {
        "name": "Long dialogue",
        "mode": "dialogue",
        "primary_account_id": 1,
        "reply_account_id": 2,
        "target_id": 1,
        "primary_text": long_text,
        "reply_text": long_text,
        "reply_delay_seconds": 10,
    }

    cards = [
        campaign_details_text(broadcast, list(range(1, 101))),
        campaign_details_text(dialogue, [1]),
        campaign_confirmation_text(broadcast_data),
        campaign_confirmation_text(dialogue_data),
        template_details_text(template),
        template_confirmation_text(
            {
                "name": "Long template",
                "primary_text": long_text,
                "reply_text": long_text,
            }
        ),
    ]

    assert all(len(card) <= 4096 for card in cards)
    assert all("…" in card for card in cards)


def test_launch_ui_groups_tasks_and_reports_progress() -> None:
    now = datetime(2026, 7, 10, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=12,
        campaign_id=4,
        campaign_name="Day send <main>",
        mode=CampaignMode.BROADCAST,
        total=4,
        queued=1,
        running=1,
        succeeded=1,
        failed=1,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now,
    )
    failed_run = CampaignRun(
        id=12,
        campaign_id=4,
        target_id=99,
        request_key="launch-task",
        status=RunStatus.FAILED,
        next_step=RunStep.PRIMARY,
        attempt_count=5,
        max_attempts=5,
        error="Cannot send <message>",
        cycle_index=1,
        created_at=now,
        scheduled_for=now,
    )
    sent_run = CampaignRun(
        id=10,
        campaign_id=4,
        target_id=100,
        request_key="launch-sent",
        status=RunStatus.SUCCEEDED,
        next_step=RunStep.PRIMARY,
        attempt_count=1,
        max_attempts=5,
        primary_message_id=777,
        cycle_index=0,
        created_at=now,
        scheduled_for=now,
        updated_at=now,
        finished_at=now,
    )
    queued_run = CampaignRun(
        id=11,
        campaign_id=4,
        target_id=101,
        request_key="launch-queued",
        status=RunStatus.QUEUED,
        next_step=RunStep.PRIMARY,
        attempt_count=0,
        max_attempts=5,
        cycle_index=1,
        created_at=now,
        scheduled_for=now,
        updated_at=now,
    )
    public_target = Target(id=100, chat_ref="@public_room", title="Public room")
    next_target = Target(id=101, chat_ref="@next_room", title="Next room")

    list_text = launches_text(Page([launch], 0, 7, 1))
    details = launch_details_text(
        launch,
        [failed_run, sent_run, queued_run],
        {100: public_target, 101: next_target},
    )

    assert "готово <b>2/4</b>" in list_text
    assert "Day send &lt;main&gt;" in list_text
    assert "чат #99" in details
    assert "Cannot send &lt;message&gt;" in details
    assert "Последние публикации" in details
    assert 'href="https://t.me/public_room/777"' in details
    assert "Что будет дальше" in details
    assert "Next room" in details
    assert "проход 2/2" in details
    details_menu = launch_details_menu(launch, 0)
    assert details_menu.inline_keyboard[0][0].text == "🔄 Обновить отчёт"
    assert details_menu.inline_keyboard[0][1].callback_data == "campaign:details:4:0"
    assert "Остановить отправку" in details_menu.inline_keyboard[1][0].text


def test_launch_with_only_pending_reactions_remains_stoppable_and_not_archivable() -> None:
    now = datetime(2026, 7, 10, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=120,
        campaign_id=40,
        campaign_name="Reaction tail",
        mode=CampaignMode.BROADCAST,
        total=1,
        queued=0,
        running=0,
        succeeded=1,
        failed=0,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now,
        reaction_queued=2,
        reaction_running=1,
    )

    assert launch.active == 0
    assert launch.pending_work == 3
    menu = launch_details_menu(launch, 0)
    labels = [button.text for row in menu.inline_keyboard for button in row]
    assert any("Остановить реакции (3)" in label for label in labels)
    assert not any("Убрать из результатов" in label for label in labels)


def test_launch_report_explains_reaction_configuration_and_outcome() -> None:
    now = datetime(2026, 7, 10, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=121,
        campaign_id=41,
        campaign_name="Reaction report",
        mode=CampaignMode.BROADCAST,
        total=1,
        queued=0,
        running=0,
        succeeded=1,
        failed=0,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now,
        sent_publications=1,
        reactions_enabled=True,
        reaction_requested_count=2,
        reaction_emoji="🔥",
        reaction_succeeded=1,
        reaction_skipped=1,
        reaction_delivered_emojis=(("👍", 1),),
        reaction_last_error="reactor has no current route",
    )
    run = CampaignRun(
        id=121,
        campaign_id=41,
        target_id=101,
        request_key="reaction-report",
        status=RunStatus.SUCCEEDED,
        cycle_index=0,
        primary_message_id=77,
        created_at=now,
        scheduled_for=now,
        updated_at=now,
    )

    report = launch_details_text(
        launch,
        [run],
        {101: Target(id=101, chat_ref="@reaction_report", title="Reaction report")},
    )

    assert "Реакции к публикациям" in report
    assert "Настроено: <b>2</b> на публикацию · реакция <b>🔥</b>" in report
    assert "подтверждено Telegram: 1" in report
    assert "⏭ пропущено <b>1</b>" in report
    assert "Фактически поставлено: 👍 × <b>1</b>" in report
    assert "использован другой emoji" in report
    assert "Последняя причина реакции" in report


def test_timer_driven_launch_ui_reports_live_posts_distribution_and_failover() -> None:
    now = datetime(2026, 7, 10, 10, 0, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=24,
        campaign_id=8,
        campaign_name="Always-on ads <live>",
        mode=CampaignMode.BROADCAST,
        total=4,
        queued=1,
        running=1,
        succeeded=1,
        failed=1,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now + timedelta(minutes=15),
        smart_timer_mode=True,
        active_until=now + timedelta(hours=24),
        no_timer_interval_seconds=15 * 60,
        auto_sender_failover=True,
        sent_publications=1,
        next_scheduled_for=now + timedelta(minutes=2),
        target_count=2,
        account_count=2,
        failover_count=1,
        account_labels=((1, "Primary <one>"), (2, "Reserve two")),
        failovers=(
            SenderFailoverView(
                run_id=24,
                target_id=102,
                failed_account_id=2,
                replacement_account_id=1,
                occurred_at=now + timedelta(minutes=1),
                scheduled_for=now + timedelta(minutes=2),
                error_code="account_unauthorized",
            ),
        ),
        attempt_failures=(
            ActionLog(
                id=31,
                run_id=23,
                account_id=2,
                target_id=102,
                kind=ActionKind.PRIMARY_MESSAGE,
                status=ActionStatus.FAILED,
                text="Advertisement",
                error="Old account lost authorization",
                attempt_number=1,
                created_at=now + timedelta(minutes=1),
            ),
        ),
        timings=(
            TargetTimingView(
                target_id=101,
                account_id=1,
                account_label="Primary <one>",
                slowmode_seconds=None,
                next_send_at=now + timedelta(minutes=15),
                last_checked_at=now,
                last_success_at=now,
                last_message_id=501,
                adaptive_interval_seconds=60,
                activity_messages_per_minute=12.5,
                adaptive_tier="turbo",
                activity_sampled_at=now,
            ),
            TargetTimingView(
                target_id=102,
                account_id=2,
                account_label="Reserve two",
                slowmode_seconds=None,
                next_send_at=None,
                last_checked_at=now,
                last_success_at=None,
                last_message_id=None,
                send_blocked=True,
                blocked_reason="account_banned_in_chat",
                blocked_at=now,
                last_error="banned",
            ),
        ),
    )
    sent_run = CampaignRun(
        id=21,
        campaign_id=8,
        target_id=101,
        sender_account_id=1,
        request_key="smart-sent",
        status=RunStatus.SUCCEEDED,
        next_step=RunStep.PRIMARY,
        attempt_count=1,
        max_attempts=5,
        primary_message_id=501,
        content_variant_id_snapshot="contact",
        content_variant_name_snapshot="Запись к Алисе",
        content_variants_snapshot_json=(
            '[{"id":"contact","contact":{"phone_number":"+79991234567","first_name":"Алиса"}}]'
        ),
        cycle_index=0,
        created_at=now,
        scheduled_for=now,
        updated_at=now,
        finished_at=now,
    )
    queued_run = CampaignRun(
        id=22,
        campaign_id=8,
        target_id=101,
        sender_account_id=1,
        request_key="smart-next",
        status=RunStatus.QUEUED,
        next_step=RunStep.PRIMARY,
        attempt_count=0,
        max_attempts=5,
        cycle_index=1,
        created_at=now,
        scheduled_for=now + timedelta(minutes=15),
        updated_at=now,
    )
    failed_run = CampaignRun(
        id=23,
        campaign_id=8,
        target_id=102,
        sender_account_id=2,
        request_key="smart-failed",
        status=RunStatus.FAILED,
        next_step=RunStep.PRIMARY,
        attempt_count=1,
        max_attempts=5,
        error="Old account lost authorization",
        cycle_index=0,
        created_at=now,
        scheduled_for=now,
        updated_at=now,
        finished_at=now,
    )
    running_run = CampaignRun(
        id=24,
        campaign_id=8,
        target_id=102,
        sender_account_id=1,
        request_key="smart-rerouted",
        status=RunStatus.RUNNING,
        next_step=RunStep.PRIMARY,
        attempt_count=1,
        max_attempts=5,
        cycle_index=0,
        created_at=now,
        scheduled_for=now + timedelta(minutes=2),
        updated_at=now,
    )
    targets = {
        101: Target(id=101, chat_ref="@first_ads", title="First ads"),
        102: Target(id=102, chat_ref="@second_ads", title="Second ads"),
    }

    list_card = launches_text(Page([launch], 0, 7, 1))
    details = launch_details_text(
        launch,
        [sent_run, queued_run, failed_run, running_run],
        targets,
    )

    assert "опубликовано <b>1</b> · в работе <b>2</b>" in list_card
    assert "Always-on ads &lt;live&gt;" in list_card
    smart_menu = launches_menu(Page([launch], 0, 7, 1))
    assert "отправлено 1" in smart_menu.inline_keyboard[0][0].text
    assert "Статус:" in details
    assert "Реально отправлено: <b>1</b>" in details
    assert "Охват направлений: <b>1/2</b>" in details
    assert "Сбоев отдельных попыток: <b>1</b>" in details
    assert "📇 Запись к Алисе" in details
    assert "Если в чате нет таймера: каждые <b>15 мин.</b>" in details
    assert "Последние публикации" in details
    assert "10.07 · 13:00 (Asia/Jerusalem)" in details
    assert 'href="https://t.me/first_ads/501"' in details
    assert "Чаты · сначала требующие внимания" in details
    assert "First ads" in details and "Second ads" in details
    chat_section = details.split("<b>Чаты · сначала требующие внимания</b>", 1)[1]
    assert chat_section.index("Second ads") < chat_section.index("First ads")
    assert "Primary &lt;one&gt;" in details and "Reserve two" in details
    assert "Таймеры по парам аккаунт × чат" in details
    assert "⛔ исключён из этого чата" in details
    assert "Автозамены · 1" in details
    assert "аккаунт больше не авторизован" in details
    assert "Последние сбои отправки" in details
    assert "Данные обновляются кнопкой ниже" in details
    assert len(details) <= 4096


def test_large_launch_report_keeps_summary_and_respects_telegram_limit() -> None:
    now = datetime(2026, 7, 10, 10, 0, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=300,
        campaign_id=9,
        campaign_name="<" * 150,
        mode=CampaignMode.BROADCAST,
        total=30,
        queued=30,
        running=0,
        succeeded=0,
        failed=0,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now + timedelta(hours=1),
        smart_timer_mode=True,
        active_until=now + timedelta(hours=24),
        no_timer_interval_seconds=300,
        sender_mode=BroadcastSenderMode.INDEPENDENT,
        next_scheduled_for=now,
        target_count=30,
        account_count=1,
        account_labels=((1, "<" * 100),),
    )
    runs = [
        CampaignRun(
            id=index,
            campaign_id=9,
            target_id=index,
            sender_account_id=1,
            request_key=f"large-report-{index:03}",
            status=RunStatus.QUEUED,
            next_step=RunStep.PRIMARY,
            attempt_count=0,
            max_attempts=5,
            cycle_index=0,
            created_at=now,
            scheduled_for=now + timedelta(minutes=index),
            updated_at=now,
            repeat_until=now + timedelta(hours=24),
            sender_mode_snapshot=BroadcastSenderMode.INDEPENDENT,
            activity_aware_snapshot=True,
            min_messages_between_posts_snapshot=10,
            activity_check_interval_seconds_snapshot=60,
            visibility_max_wait_seconds_snapshot=900,
            no_timer_interval_seconds_snapshot=300,
            content_variant_name_snapshot=("<" * 78) + str(index % 10),
        )
        for index in range(1, 31)
    ]
    targets = {
        index: Target(id=index, chat_ref=f"@long_{index}", title="<" * 150)
        for index in range(1, 31)
    }

    report = launch_details_text(launch, runs, targets)

    assert len(report) <= 4096
    assert "Статус:" in report
    assert "<b>Итог</b>" in report
    assert "Часть подробностей скрыта из‑за лимита Telegram" in report


def test_smart_launch_report_normalizes_legacy_naive_schedule_values() -> None:
    now = datetime(2026, 7, 10, 10, 0, tzinfo=UTC)
    launch = LaunchView(
        anchor_run_id=2,
        campaign_id=9,
        campaign_name="Legacy time zone values",
        mode=CampaignMode.BROADCAST,
        total=2,
        queued=2,
        running=0,
        succeeded=0,
        failed=0,
        cancelled=0,
        created_at=now,
        first_scheduled_for=now,
        last_scheduled_for=now + timedelta(minutes=5),
        smart_timer_mode=True,
        next_scheduled_for=now,
        target_count=1,
    )
    runs = [
        CampaignRun(
            id=1,
            campaign_id=9,
            target_id=1,
            request_key="legacy-naive",
            status=RunStatus.QUEUED,
            next_step=RunStep.PRIMARY,
            attempt_count=0,
            max_attempts=5,
            cycle_index=0,
            created_at=now,
            scheduled_for=now.replace(tzinfo=None),
        ),
        CampaignRun(
            id=2,
            campaign_id=9,
            target_id=1,
            request_key="utc-aware",
            status=RunStatus.QUEUED,
            next_step=RunStep.PRIMARY,
            attempt_count=0,
            max_attempts=5,
            cycle_index=0,
            created_at=now,
            scheduled_for=now + timedelta(minutes=5),
        ),
    ]

    report = launch_details_text(
        runs=runs,
        launch=launch,
        targets={1: Target(id=1, chat_ref="@chat", title="Legacy target")},
    )

    assert "Legacy time zone values" in report


def test_keyboard_theme_uses_one_navigation_language_across_modules() -> None:
    main_labels = [button.text for row in main_menu().inline_keyboard for button in row]
    direct_labels = [
        button.text for row in direct_messages_home_menu().inline_keyboard for button in row
    ]
    invite_labels = [
        button.text for row in subscriber_invites_home_menu().inline_keyboard for button in row
    ]

    assert "↻ Обновить данные" in main_labels
    assert "🎯 Чаты и списки" in main_labels
    assert "⌂ На главную" in direct_labels
    assert "⌂ На главную" in invite_labels
    assert cancel_menu().inline_keyboard[0][0].text == "✕ Отменить ввод"
    assert direct_blacklist_menu().inline_keyboard[-1][0].text == "← К личным сообщениям"
    invite_blacklist = direct_blacklist_menu(
        add_callback="inv:blacklist:add",
        back_callback="inv:home",
    )
    assert invite_blacklist.inline_keyboard[0][0].callback_data == "inv:blacklist:add"
