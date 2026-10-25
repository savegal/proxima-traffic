"""Reproduce reported panel defects and verify claims against real middleware/SQL."""

import asyncio
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram import Bot, Router
from aiogram.exceptions import TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.methods import EditMessageText
from aiogram.types import CallbackQuery, Chat, Message, Update, User
from alembic import command
from alembic.config import Config

from proxima_traffic.bot import app as bot_app
from proxima_traffic.bot import handlers
from proxima_traffic.bot.app import create_dispatcher
from proxima_traffic.bot.channel_comment_ui import (
    _comment_preview,
    channel_comment_details_text,
    channel_comment_reactions_menu,
    channel_comment_reactions_text,
)
from proxima_traffic.bot.middleware import DraftMediaCleanupMiddleware
from proxima_traffic.bot.repository import Page
from proxima_traffic.bot.states import (
    ChannelCommentCreateState,
    OrderDraftState,
    ParticipantReactionCreateState,
    ParticipantReactionEditState,
)
from proxima_traffic.bot.storage import DatabaseFsmStorage
from proxima_traffic.channel_comments import ChannelCommentRuleView
from proxima_traffic.config import Settings
from proxima_traffic.maintenance import cleanup_draft_media
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignMode,
    ChannelCommentMode,
    ChannelCommentRule,
    utc_now,
)
from proxima_traffic.services import ConflictError


def callback(data="order:save", id_="regression"):
    return CallbackQuery(
        id=id_,
        data=data,
        from_user=User(id=7, is_bot=False, first_name="Admin"),
        chat_instance="test",
        message=Message(
            message_id=11, date=utc_now(), chat=Chat(id=7, type="private"), text="Panel"
        ),
    )


def comment_view(**values):
    fields = dict(
        id=1,
        name="Rule",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=1,
        primary_text="Text",
        channel_ref="@example_channel",
        reaction_account_ids_json="[1,2]",
        reactions_enabled=True,
        reaction_count=1,
        reaction_emoji="👍",
        reaction_dialogue_step=1,
        initial_delay_seconds=0,
        enabled=False,
        mention_cooldown_days=30,
    )
    fields.update(values)
    return ChannelCommentRuleView(
        rule=ChannelCommentRule(**fields), queued=0, running=0, succeeded=0, failed=0, skipped=0
    )


@pytest.mark.parametrize("cluster", ["❤️", "👍🏽", "👨‍👩‍👧‍👦", "🇺🇦", "e\u0301", "1️⃣"])
def test_comment_preview_keeps_complete_emoji_and_combining_sequences(cluster):
    units = len(cluster.encode("utf-16-le")) // 2
    value = "A" + cluster + "XYZ"
    for partial in range(units):
        assert _comment_preview(value, 2 + partial) == "A…"
    assert _comment_preview(value, 2 + units) == "A" + cluster + "…"


@pytest.mark.parametrize("raw", [None, "[]", "null", "{}", "not-json"])
def test_corrupt_comment_chain_has_a_recoverable_reaction_screen(raw):
    view = comment_view(dialogue_steps_json=raw)
    assert "повреждены" in channel_comment_reactions_text(view, {1: "Sender", 2: "Reactor"})
    assert "повреждена" in channel_comment_details_text(view, {1: "Sender"})
    assert channel_comment_reactions_menu(view, page=0).inline_keyboard


@pytest.mark.parametrize("step", [0, -1, 99])
def test_invalid_reaction_step_is_not_silently_replaced_with_another_author(step):
    view = comment_view(
        dialogue_steps_json='[{"account_id":1,"text":"First"},{"account_id":2,"text":"Reply"}]',
        reaction_dialogue_step=step,
    )
    text = channel_comment_reactions_text(view, {1: "First", 2: "Second"})
    assert "повреждены" in text
    assert "Максимум по выбранному набору: <b>0</b>" in text


@pytest.mark.parametrize(
    "first,second",
    [
        ("order:save", "order:save"),
        ("order:save", "order:launch:confirm"),
        ("order:launch:confirm", "order:save"),
        ("pr:create", "pr:create"),
        ("pr:save", "pr:save"),
    ],
)
async def test_real_dispatcher_consumes_each_confirmation_once(first, second, monkeypatch):
    router = Router()
    router.callback_query.handlers = list(handlers.router.callback_query.handlers)
    monkeypatch.setattr(bot_app, "router", router)
    storage = MemoryStorage()
    dispatcher = create_dispatcher(frozenset({7}), storage=storage)
    bot = Bot("123456:regression-test-token")
    state = FSMContext(storage, StorageKey(bot_id=bot.id, chat_id=7, user_id=7))
    calls = []

    async def create(*args, **kwargs):
        calls.append((args, kwargs))
        await asyncio.sleep(0.02)
        return SimpleNamespace(id=1)

    async def fail_preflight(*args, **kwargs):
        raise ConflictError("Test preflight is intentionally unavailable")

    service = SimpleNamespace(create_rule=create, update_rule=create)
    if first.startswith("order:"):
        await state.set_state(OrderDraftState.action)
        await state.set_data({"order_draft_active": True})
    else:
        await state.set_state(
            ParticipantReactionCreateState.confirm
            if first == "pr:create"
            else ParticipantReactionEditState.confirm
        )
        await state.set_data(
            dict(
                pr_name="Rule",
                pr_target_list_id=1,
                pr_account_ids=[1],
                pr_emoji="👍",
                pr_numerator=1,
                pr_denominator=10,
                pr_edit_rule_id=1,
            )
        )
    monkeypatch.setattr(handlers, "_create_campaign_from_order_draft", create)
    monkeypatch.setattr(handlers, "_inspect_campaign_with_visible_progress", fail_preflight)
    monkeypatch.setattr(handlers, "_show_participant_reaction_details", AsyncMock())
    monkeypatch.setattr(handlers, "campaign_details_text", lambda *args: "Saved")
    monkeypatch.setattr(handlers, "campaign_details_menu", lambda *args: None)
    monkeypatch.setattr(handlers, "_edit_callback", AsyncMock())
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())
    try:
        await asyncio.gather(
            *(
                dispatcher.feed_update(
                    bot,
                    Update(update_id=index, callback_query=callback(data, str(index))),
                    repository=SimpleNamespace(campaign_sender_ids=AsyncMock(return_value=[1])),
                    participant_reactions=service,
                    preflight_service=object(),
                    settings=Settings(),
                )
                for index, data in enumerate((first, second), 1)
            )
        )
        assert len(calls) == 1
        assert await state.get_state() is None
    finally:
        await dispatcher.fsm.close()
        await bot.session.close()


@pytest.mark.parametrize("kind", ["message", "callback"])
async def test_non_admin_is_rejected_before_any_fsm_read(kind, monkeypatch):
    storage = MemoryStorage()
    read = AsyncMock(wraps=storage.get_state)
    monkeypatch.setattr(storage, "get_state", read)
    monkeypatch.setattr(bot_app, "router", Router())
    dispatcher = create_dispatcher(frozenset({8}), storage=storage)
    bot = Bot("123456:regression-test-token")
    source = callback()
    update = Update(
        update_id=1,
        **(
            {"callback_query": source}
            if kind == "callback"
            else {"message": source.message.model_copy(update={"from_user": source.from_user})}
        ),
    )
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())
    monkeypatch.setattr(Message, "answer", AsyncMock())
    try:
        await dispatcher.feed_update(bot, update)
        read.assert_not_awaited()
        assert not dispatcher.fsm.events_isolation._locks
    finally:
        await dispatcher.fsm.close()
        await bot.session.close()


@pytest.mark.parametrize("transition", ["cancel", "replace", "save", "another-draft"])
async def test_draft_cleanup_preserves_saved_and_shared_media(app, tmp_path, transition):
    root = tmp_path / "media"
    root.mkdir()
    old, new = root / "old.jpg", root / "new.jpg"
    old.write_bytes(b"old")
    new.write_bytes(b"new")
    settings = app.state.settings.model_copy(update={"media_storage_path": str(root)})
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=1, chat_id=7, user_id=7))
    await state.set_state(OrderDraftState.text)
    await state.set_data({"comment_steps": [{"media_path": str(old)}]})
    if transition == "save":
        from proxima_traffic.models import Target

        async with app.state.database.session_factory() as session:
            account, target = Account(label="Media account"), Target(title="Chat", chat_ref="@chat")
            session.add_all([account, target])
            await session.flush()
            session.add(
                Campaign(
                    name="Saved media",
                    mode=CampaignMode.BROADCAST,
                    primary_account_id=account.id,
                    target_id=target.id,
                    primary_text="Text",
                    primary_media_path=str(old),
                    primary_media_kind="photo",
                )
            )
            await session.commit()
    elif transition == "another-draft":
        other = FSMContext(
            DatabaseFsmStorage(app.state.database.session_factory),
            StorageKey(bot_id=1, chat_id=8, user_id=8),
        )
        await other.set_data({"primary_media_path": str(old)})

    async def handler(event, data):
        if transition == "replace":
            await state.set_data({"primary_media_path": str(new)})
        else:
            await state.clear()
        return "completed"

    result = await DraftMediaCleanupMiddleware()(
        handler,
        callback(),
        {
            "state": state,
            "raw_state": OrderDraftState.text.state,
            "settings": settings,
        },
    )
    assert result == "completed"
    assert old.exists() == (transition in {"save", "another-draft"})
    assert new.exists()


async def test_cleanup_failure_does_not_hide_handler_result_or_remove_files(tmp_path):
    path = tmp_path / "draft.jpg"
    path.write_bytes(b"media")
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=1, chat_id=7, user_id=7))
    await state.set_data({"primary_media_path": str(path)})

    async def handler(event, data):
        await state.clear()
        return "saved"

    settings = SimpleNamespace(
        media_storage_path=str(tmp_path),
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'missing.db'}",
    )
    assert (
        await DraftMediaCleanupMiddleware()(
            handler,
            callback(),
            {
                "state": state,
                "raw_state": "draft",
                "settings": settings,
            },
        )
        == "saved"
    )
    assert path.exists()


def test_draft_cleanup_cannot_delete_files_outside_media_root(tmp_path):
    root = tmp_path / "media"
    root.mkdir()
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"external")
    assert cleanup_draft_media("unused", root, {str(outside)}) == []
    assert outside.exists()


async def test_edit_mode_delay_button_edits_the_edit_delay(monkeypatch):
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=1, chat_id=7, user_id=7))
    await state.set_data({"comment_mode": ChannelCommentMode.MESSAGE_EDIT.value})
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.edit_channel_comment_review(callback("comments:review:delay"), state)
    assert await state.get_state() == ChannelCommentCreateState.message_edit_delay.state
    assert "Задержка редактирования" in edit.call_args.args[1]


async def test_bulk_names_survive_a_progress_screen_api_failure(monkeypatch):
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=1, chat_id=7, user_id=7))
    await state.set_data(
        {
            "profile_name_assignments": [
                {
                    "account_id": 1,
                    "first_name": "Name",
                    "last_name": "",
                    "locale": "ru",
                    "style": "realistic",
                    "label": "Account",
                }
            ]
        }
    )
    error = TelegramForbiddenError(method=EditMessageText(text="progress"), message="Forbidden")
    edit = AsyncMock(side_effect=[error, None])
    apply = AsyncMock(return_value=SimpleNamespace(updated=[], failures=[]))
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    await handlers.apply_generated_account_names(
        callback(),
        state,
        SimpleNamespace(
            list_accounts=AsyncMock(return_value=Page(items=[], total=0, page=0, page_size=7))
        ),
        SimpleNamespace(bulk_update_profile_names=apply),
        None,
    )
    apply.assert_awaited_once()
    assert await state.get_data() == {}


def test_sqlite_autogenerate_renders_and_executes_batch_alter(tmp_path, schema_template):
    project = Path(__file__).resolve().parents[1]
    script_path = tmp_path / "migrations"
    shutil.copytree(
        project / "migrations", script_path, ignore=shutil.ignore_patterns("__pycache__")
    )
    database = tmp_path / "migration.db"
    shutil.copy2(schema_template, database)
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE accounts ADD COLUMN obsolete_probe TEXT")
    config = Config(str(project / "alembic.ini"))
    config.set_main_option("script_location", str(script_path))
    config.attributes["database_url"] = f"sqlite+aiosqlite:///{database.as_posix()}"
    revision = command.revision(config, message="Remove obsolete test column", autogenerate=True)
    source = Path(revision.path).read_text(encoding="utf-8")
    assert "batch_alter_table('accounts'" in source
    assert "drop_column('obsolete_probe')" in source
    command.upgrade(config, "head")
    with sqlite3.connect(database) as connection:
        assert "obsolete_probe" not in {
            row[1] for row in connection.execute("PRAGMA table_info(accounts)")
        }


@pytest.mark.parametrize("screen", ["comment-details", "comment-reactions", "participants"])
async def test_paused_logged_out_accounts_keep_their_labels(app, monkeypatch, screen):
    from proxima_traffic.bot.repository import AdminRepository
    from proxima_traffic.models import AccountStatus, ParticipantReactionRule
    from proxima_traffic.participant_reactions import ParticipantReactionRuleView

    async with app.state.database.session_factory() as session:
        account = Account(label="Paused & saved", status=AccountStatus.PAUSED, authorized=False)
        session.add(account)
        await session.commit()
        aid = account.id
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    repository = AdminRepository(app.state.database.session_factory)
    settings = app.state.settings.model_copy(update={"dry_run": False})
    if screen == "participants":
        view = ParticipantReactionRuleView(
            rule=ParticipantReactionRule(
                id=1,
                name="Rule",
                account_ids_json=f"[{aid}]",
                emoji="👍",
                sample_numerator=1,
                sample_denominator=10,
                enabled=False,
            ),
            target_list_name="Chats",
            target_count=0,
            queued=0,
            running=0,
            succeeded=0,
            failed=0,
            skipped=0,
        )
        show = handlers._show_participant_reaction_details
    else:
        view = comment_view(
            mode=ChannelCommentMode.SINGLE,
            primary_account_id=aid,
            reaction_account_ids_json=f"[{aid}]",
        )
        show = (
            handlers._show_channel_comment_details
            if screen == "comment-details"
            else handlers._show_channel_comment_reactions
        )
    await show(
        callback(),
        SimpleNamespace(get_rule_view=AsyncMock(return_value=view)),
        repository,
        settings,
        rule_id=1,
        page=0,
    )
    assert "Paused &amp; saved" in edit.call_args.args[1]


@pytest.mark.parametrize(
    "handler_name,text",
    [
        ("save_order_name", "x" * 151),
        ("save_custom_order_mention_cooldown", "0"),
        ("proxy_add_url", "not-a-proxy"),
        ("account_label", "x" * 101),
        ("receive_account_group_name", "x" * 101),
    ],
)
async def test_validation_errors_edit_the_panel_and_keep_cancel(handler_name, text, monkeypatch):
    import inspect

    bot = Bot("123456:regression-test-token")
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=bot.id, chat_id=7, user_id=7))
    await state.set_state("validation:input")
    await state.set_data({"origin_chat_id": 7, "origin_message_id": 11})
    message = callback().message.model_copy(update={"text": text}).as_(bot)
    edit = AsyncMock(return_value=callback().message)
    send = AsyncMock()
    monkeypatch.setattr(Bot, "edit_message_text", edit)
    monkeypatch.setattr(Message, "answer", send)
    handler = getattr(handlers, handler_name)
    values = dict(message=message, state=state, bot=bot, settings=Settings(), repository=object())
    try:
        await handler(**{key: values[key] for key in inspect.signature(handler).parameters})
        send.assert_not_awaited()
        edit.assert_awaited_once()
        assert edit.call_args.kwargs["message_id"] == 11
        markup = edit.call_args.kwargs["reply_markup"]
        assert any(
            button.callback_data == "fsm:cancel" for row in markup.inline_keyboard for button in row
        )
        assert await state.get_state() == "validation:input"
    finally:
        await bot.session.close()


async def _direct_launch(app, states):
    from proxima_traffic.direct_messages import DirectMessageService
    from proxima_traffic.models import (
        DirectCampaign,
        DirectLaunch,
        DirectLaunchStatus,
        DirectMessageTask,
        Subscriber,
        SubscriberList,
        SubscriberSegment,
    )

    async with app.state.database.session_factory() as session:
        audience = SubscriberList(name="Audience")
        account = Account(label="Direct sender", ownership_confirmed=True)
        subscribers = [Subscriber(telegram_user_id=900 + index) for index in range(len(states))]
        session.add_all([audience, account, *subscribers])
        await session.flush()
        campaign = DirectCampaign(
            name="Campaign", subscriber_list_id=audience.id, primary_text="Text"
        )
        session.add(campaign)
        await session.flush()
        launch = DirectLaunch(
            campaign_id=campaign.id,
            subscriber_list_id=audience.id,
            status=DirectLaunchStatus.FAILED,
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=len(states),
            contact_cooldown_days_snapshot=0,
            account_ids_snapshot_json=f"[{account.id}]",
            primary_text_snapshot="Text",
            consent_confirmed_at=utc_now(),
            total_tasks=len(states),
        )
        session.add(launch)
        await session.flush()
        for subscriber, (status, code) in zip(
            subscribers,
            states,
            strict=True,
        ):
            session.add(
                DirectMessageTask(
                    launch_id=launch.id, subscriber_id=subscriber.id, status=status, error_code=code
                )
            )
        await session.commit()
        launch_id = launch.id
    service = DirectMessageService(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="regression",
    )
    return service, launch_id


async def test_cancelled_uncertainty_is_not_subtracted_from_failed_counts(app):
    from proxima_traffic.bot.direct_message_ui import direct_messages_home_text
    from proxima_traffic.models import DirectMessageTaskStatus

    service, launch_id = await _direct_launch(
        app,
        [
            (DirectMessageTaskStatus.FAILED, "delivery_unconfirmed"),
            (DirectMessageTaskStatus.FAILED, "proxy_unavailable"),
            (DirectMessageTaskStatus.CANCELLED, "cancelled_outcome_unknown"),
        ],
    )
    dashboard = await service.dashboard()
    assert (dashboard.failed_tasks, dashboard.uncertain_tasks) == (2, 1)
    assert "ошибок: <b>1</b>" in direct_messages_home_text(dashboard)
    view = await service.get_launch_view(launch_id)
    assert (view.failed, view.uncertain, view.cancelled) == (2, 1, 1)
    views, _ = await service.list_launches(0, 10)
    assert (views[0].failed, views[0].uncertain, views[0].cancelled) == (2, 1, 1)


@pytest.mark.parametrize("screen", ["dashboard", "details"])
async def test_direct_failure_counts_are_a_single_snapshot_during_worker_updates(app, screen):
    from sqlalchemy import event

    from proxima_traffic.maintenance import sqlite_path_from_url
    from proxima_traffic.models import DirectMessageTaskStatus

    service, launch_id = await _direct_launch(app, [(DirectMessageTaskStatus.RUNNING, None)])
    path = sqlite_path_from_url(app.state.settings.database_url)
    changed = False

    def finish_task(connection, cursor, statement, parameters, context, executemany):
        nonlocal changed
        if not changed and "group by direct_message_tasks.status" in statement.lower():
            changed = True
            with sqlite3.connect(path) as writer:
                writer.execute(
                    "UPDATE direct_message_tasks SET status='FAILED', "
                    "error_code='delivery_unconfirmed' WHERE launch_id=?",
                    (launch_id,),
                )

    engine = app.state.database.engine.sync_engine
    event.listen(engine, "after_cursor_execute", finish_task)
    try:
        if screen == "dashboard":
            view = await service.dashboard()
            failed, uncertain = view.failed_tasks, view.uncertain_tasks
        else:
            view = await service.get_launch_view(launch_id)
            failed, uncertain = view.failed, view.uncertain
        assert changed
        assert 0 <= uncertain <= failed
    finally:
        event.remove(engine, "after_cursor_execute", finish_task)
