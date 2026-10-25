"""Behavioural regressions for panel responsiveness, recovery and honest reports."""

import asyncio
from datetime import timedelta
from html import unescape
from html.parser import HTMLParser
from unittest.mock import AsyncMock

import pytest
from aiogram import Bot, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Chat, Message, Update, User
from sqlalchemy import event

from proxima_traffic.bot import app as bot_app
from proxima_traffic.bot import handlers
from proxima_traffic.bot.app import create_dispatcher
from proxima_traffic.bot.middleware import (
    CallbackTimingMiddleware,
    ControlEventIsolation,
    acknowledge_callback,
)
from proxima_traffic.bot.panel import answer_panel_input, input_panel_origin
from proxima_traffic.bot.renderers import dashboard_text, launch_details_text
from proxima_traffic.bot.repository import AdminRepository, DashboardStats
from proxima_traffic.bot.states import DirectCampaignCreateState
from proxima_traffic.bot.storage import DatabaseFsmStorage
from proxima_traffic.bot.ui import fit_panel_text
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.models import (
    Account,
    ActionKind,
    ActionLog,
    ActionStatus,
    Campaign,
    CampaignMode,
    CampaignRun,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    RunStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteCampaignAccount,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberSegment,
    Target,
    TargetList,
    TargetListItem,
    utc_now,
)
from proxima_traffic.notifications import RunNotifier
from proxima_traffic.subscriber_invites import SubscriberInviteService


def _message(message_id: int = 11) -> Message:
    return Message(
        message_id=message_id,
        date=utc_now(),
        chat=Chat(id=7, type="private"),
        from_user=User(id=7, is_bot=False, first_name="Admin"),
        text="test",
    )


def _callback(id_: str = "quality", data: str = "menu:home") -> CallbackQuery:
    return CallbackQuery(
        id=id_,
        from_user=User(id=7, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data=data,
        message=_message(),
    )


class _VisibleHtml(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = ""
        self.tags: list[str] = []

    def handle_data(self, data):
        self.text += data

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)

    def handle_endtag(self, tag):
        assert self.tags.pop() == tag


def test_long_panel_text_keeps_entities_balanced_html_and_unicode_limits():
    text = '<b>Итог</b>\n<a href="https://example.com">' + "🔥 &amp; данные " * 600 + "</a>"
    rendered = fit_panel_text(text)
    parser = _VisibleHtml()
    parser.feed(rendered)
    assert len(parser.text.encode("utf-16-le")) // 2 <= 4096
    assert not parser.tags
    assert "Часть подробностей скрыта" in parser.text
    assert "&amp;" in rendered
    assert fit_panel_text(rendered) == rendered


def test_html_source_length_does_not_hide_short_visible_text():
    text = "<b>" + "&amp;" * 1000 + "</b>"
    assert fit_panel_text(text) == text


async def test_slow_callback_is_acknowledged_before_work_finishes(monkeypatch):
    answer = AsyncMock()
    monkeypatch.setattr(CallbackQuery, "answer", answer)
    middleware = CallbackTimingMiddleware(frozenset({7}))
    events: list[str] = []

    async def work(callback, data):
        await asyncio.sleep(0.3)
        assert answer.await_count == 1
        await acknowledge_callback(callback)
        assert answer.await_count == 1
        await acknowledge_callback(callback, "Готово")
        events.append("done")

    await middleware(work, _callback(), {})
    assert answer.await_count == 2
    assert events == ["done"]


async def test_fast_callback_does_not_leave_an_acknowledgement_task(monkeypatch):
    answer = AsyncMock()
    monkeypatch.setattr(CallbackQuery, "answer", answer)

    async def work(callback, data):
        await acknowledge_callback(callback, "Готово")

    await CallbackTimingMiddleware(frozenset({7}))(work, _callback("fast"), {})
    await asyncio.sleep(0.25)
    assert answer.await_count == 1


async def test_stop_controls_do_not_wait_for_a_busy_wizard():
    isolation = ControlEventIsolation(frozenset({7}))
    middleware = CallbackTimingMiddleware()
    key = StorageKey(bot_id=123456, chat_id=7, user_id=7)
    entered = asyncio.Event()
    release = asyncio.Event()
    stopped = asyncio.Event()

    async def wizard(callback, data):
        async with isolation.lock(key):
            entered.set()
            await release.wait()

    async def stop(callback, data):
        async with isolation.lock(key):
            stopped.set()

    task = asyncio.create_task(middleware(wizard, _callback("busy", "dm:new:account:1"), {}))
    await entered.wait()
    try:
        await asyncio.wait_for(
            middleware(stop, _callback("stop", "queue:stopall:confirm"), {}),
            timeout=0.5,
        )
        assert stopped.is_set()
    finally:
        release.set()
        await task
        await isolation.close()


async def test_dispatcher_serializes_wizard_input_and_rejects_stale_step_buttons(app, monkeypatch):
    storage = DatabaseFsmStorage(app.state.database.session_factory)
    # Reuse the real registered handlers/filters without reattaching the production singleton.
    router = Router()
    router.callback_query.handlers = list(handlers.router.callback_query.handlers)
    monkeypatch.setattr(bot_app, "router", router)
    dispatcher = create_dispatcher(frozenset({7}), storage=storage)
    bot = Bot("123456:test-control-panel-token")
    key = StorageKey(bot_id=123456, chat_id=7, user_id=7)
    state = FSMContext(storage, key)
    await state.set_state(DirectCampaignCreateState.accounts)
    await state.set_data({"direct_account_ids": []})
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())

    async def show(*args):
        await asyncio.sleep(0.02)

    monkeypatch.setattr(handlers, "_show_direct_account_picker", show)
    try:
        await asyncio.gather(
            *(
                dispatcher.feed_update(
                    bot,
                    Update(
                        update_id=id_, callback_query=_callback(str(id_), f"dm:new:account:{id_}")
                    ),
                    repository=object(),
                    settings=object(),
                )
                for id_ in (1, 2)
            )
        )
        assert (await state.get_data())["direct_account_ids"] == [1, 2]
        await state.set_state(DirectCampaignCreateState.content)
        before = await state.get_data()
        await dispatcher.feed_update(
            bot,
            Update(update_id=3, callback_query=_callback("old", "dm:new:account:3")),
            repository=object(),
            settings=object(),
        )
        assert await state.get_data() == before
        assert CallbackQuery.answer.call_args.kwargs["show_alert"] is True
        assert list(dispatcher.update.outer_middleware).index(dispatcher.fsm) > 2
    finally:
        await dispatcher.fsm.close()
        await bot.session.close()


async def test_final_wizard_reply_updates_original_panel_after_state_clear(monkeypatch):
    bot = Bot("123456:test-control-panel-token")
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=123456, chat_id=7, user_id=7))
    await state.clear()
    edit = AsyncMock(return_value=_message(11))
    send = AsyncMock(return_value=_message(99))
    monkeypatch.setattr(Bot, "edit_message_text", edit)
    monkeypatch.setattr(Message, "answer", send)
    token = input_panel_origin.set((7, 11))
    try:
        await answer_panel_input(_message(20).as_(bot), state, "<b>Сохранено</b>")
        assert edit.call_args.kwargs["message_id"] == 11
        send.assert_not_awaited()
        assert await state.get_data() == {}
    finally:
        input_panel_origin.reset(token)
        await bot.session.close()


@pytest.mark.parametrize(
    "error, recover",
    [
        ("message to edit not found", True),
        ("can't parse entities", False),
    ],
)
async def test_wizard_recovers_only_uneditable_screens(monkeypatch, error, recover):
    bot = Bot("123456:test-control-panel-token")
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=123456, chat_id=7, user_id=7))
    await state.set_state(DirectCampaignCreateState.content)
    await state.set_data({"origin_chat_id": 7, "origin_message_id": 11})
    monkeypatch.setattr(
        Bot,
        "edit_message_text",
        AsyncMock(
            side_effect=TelegramBadRequest(
                method=None,
                message=error,
            )
        ),
    )
    send = AsyncMock(return_value=_message(99))
    monkeypatch.setattr(Message, "answer", send)
    token = input_panel_origin.set(None)
    try:
        if recover:
            await answer_panel_input(_message(20).as_(bot), state, "Сохранено")
            assert (await state.get_data())["origin_message_id"] == 99
            assert await state.get_state() == DirectCampaignCreateState.content.state
        else:
            with pytest.raises(TelegramBadRequest):
                await answer_panel_input(_message(20).as_(bot), state, "Сохранено")
            send.assert_not_awaited()
    finally:
        input_panel_origin.reset(token)
        await bot.session.close()


async def test_invitation_history_uses_fixed_queries_and_exact_per_launch_counts(app):
    factory = app.state.database.session_factory
    async with factory() as session:
        account = Account(label="Admin", ownership_confirmed=True)
        audience = SubscriberList(name="Audience")
        session.add_all([account, audience])
        await session.flush()
        campaign = SubscriberInviteCampaign(
            name="Invites",
            subscriber_list_id=audience.id,
            destination_ref="@owned_channel",
            destination_key="@owned_channel",
            recipient_limit=4,
        )
        session.add(campaign)
        await session.flush()
        session.add(SubscriberInviteCampaignAccount(campaign_id=campaign.id, account_id=account.id))
        for index in range(7):
            launch = SubscriberInviteLaunch(
                campaign_id=campaign.id,
                subscriber_list_id=audience.id,
                destination_ref_snapshot="@owned_channel",
                destination_key_snapshot="@owned_channel",
                account_ids_snapshot_json=f"[{account.id}]",
                segment_snapshot=SubscriberSegment.ALL,
                recipient_limit_snapshot=4,
                contact_cooldown_days_snapshot=0,
                consent_confirmed_at=utc_now(),
                total_tasks=4,
                status=DirectLaunchStatus.PARTIAL,
                simulated_snapshot=False,
            )
            session.add(launch)
            await session.flush()
            for offset, status in enumerate(
                (
                    DirectMessageTaskStatus.SUCCEEDED,
                    DirectMessageTaskStatus.SUCCEEDED,
                    DirectMessageTaskStatus.FAILED,
                    DirectMessageTaskStatus.QUEUED,
                )
            ):
                subscriber = Subscriber(telegram_user_id=1000 + index * 10 + offset)
                session.add(subscriber)
                await session.flush()
                session.add(
                    SubscriberInviteTask(
                        launch_id=launch.id,
                        subscriber_id=subscriber.id,
                        sender_account_id=account.id,
                        status=status,
                        already_participant=offset == 1,
                        error_code="invite_delivery_unconfirmed" if offset == 2 else None,
                        error="Result unknown" if offset == 2 else None,
                    )
                )
        await session.commit()
    service = SubscriberInviteService(
        factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="quality",
    )
    statements: list[str] = []

    def capture(conn, cursor, statement, params, context, many):
        statements.append(statement)

    event.listen(app.state.database.engine.sync_engine, "before_cursor_execute", capture)
    try:
        views, total = await service.list_launches(0, 7)
    finally:
        event.remove(app.state.database.engine.sync_engine, "before_cursor_execute", capture)
    assert total == len(views) == 7
    assert len(statements) == 6
    for view in views:
        assert (
            view.invited,
            view.already_participant,
            view.failed,
            view.uncertain,
            view.queued,
        ) == (
            1,
            1,
            1,
            1,
            1,
        )
        assert view.selected_admin_count == view.ready_admin_count == 1
        assert view.last_error_recipient is not None
        assert view.issues == ((DirectMessageTaskStatus.FAILED, "invite_delivery_unconfirmed", 1),)
        assert view.next_scheduled_for is not None


async def test_dashboard_counts_real_simulated_unknown_and_unique_targets_separately(app):
    factory = app.state.database.session_factory
    async with factory() as session:
        account = Account(label="Sender", ownership_confirmed=True)
        target = Target(chat_ref="@owned_chat", title="Chat", approved_by_operator=True)
        audience = TargetList(name="Audience")
        session.add_all([account, target, audience])
        await session.flush()
        session.add(TargetListItem(target_list_id=audience.id, target_id=target.id, enabled=True))
        campaign = Campaign(
            name="Campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="text",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="quality",
            status=RunStatus.SUCCEEDED,
            scheduled_for=utc_now(),
        )
        session.add(run)
        await session.flush()
        for mode, old in ((False, False), (True, False), (None, False), (False, True)):
            session.add(
                ActionLog(
                    run_id=run.id,
                    account_id=account.id,
                    target_id=target.id,
                    kind=ActionKind.PRIMARY_MESSAGE,
                    status=ActionStatus.SUCCEEDED,
                    external_message_id=123,
                    simulated=mode,
                    text="text",
                    created_at=utc_now() - timedelta(hours=25 if old else 1),
                )
            )
        await session.commit()
    repository = AdminRepository(factory)
    stats = await repository.dashboard()
    assert (
        stats.sent_messages_24h,
        stats.simulated_messages_24h,
        stats.unknown_mode_messages_24h,
    ) == (1, 1, 1)
    assert stats.available_targets_total == stats.available_targets_enabled == 1
    text = dashboard_text(stats, dry_run=False, require_proxy=False)
    assert "За последние 24 часа" in text
    assert "Симуляций: <b>1</b>" in text
    launch, runs = await repository.get_launch(run.id)
    report = launch_details_text(launch, runs, {target.id: target})
    assert "Отправлено и подтверждено: <b>2</b>" in report
    assert "Симуляций: <b>1</b>" in report
    assert "без сохранённого режима: <b>1</b>" in report
    assert unescape(report).count('href="https://t.me/owned_chat/123"') == 2


def test_disabled_worker_does_not_look_ready_even_with_available_resources():
    stats = DashboardStats(1, 1, 1, 1, 0, 0, 0, 2, 0, 0, 0)
    text = dashboard_text(stats, dry_run=True, worker_enabled=False)
    assert "Очередь выключена" in text
    assert "Всё готово к отправке" not in text


async def test_simulated_send_is_recorded_and_not_checked_as_a_live_delivery(app):
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="Simulator",
        telegram_user_id=123,
        ownership_confirmed=True,
    )
    target = await repository.create_target(chat_ref="@simulated", title="Simulation")
    await repository.set_target_approval(target.id, True)
    campaign = await repository.create_campaign(
        name="Simulation",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="text",
    )
    await app.state.run_queue.enqueue_campaign(campaign.id, "simulation-quality")
    result = await app.state.queue_worker.process_next()
    assert result is not None and result.status == RunStatus.SUCCEEDED
    launch, _ = await repository.get_launch(result.id)
    assert len(launch.publications) == 1
    assert launch.publications[0].simulated is True
    notifier = RunNotifier(app.state.database.session_factory, AsyncMock())
    notice = await notifier._render(result)
    assert "Симуляций: <b>1</b>" in notice
    assert "Отправлено и подтверждено: <b>0</b>" in notice
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, launch.publications[0].id)
        action.created_at = utc_now() - timedelta(hours=1)
        await session.commit()
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
    )
    assert await monitor._claim_due_action() is None
