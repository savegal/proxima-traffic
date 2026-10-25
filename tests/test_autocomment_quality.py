from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from html.parser import HTMLParser
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from telethon import errors, types

from proxima_traffic.bot.channel_comment_ui import (
    channel_comment_confirmation_menu,
    channel_comment_draft_text,
    channel_comment_runs_text,
)
from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.config import Settings
from proxima_traffic.content import render_telegram_html
from proxima_traffic.gateways import (
    ChannelCommentDelivery,
    ChannelCommentPreflight,
    ChannelPost,
    ChannelPostScan,
    DryRunGateway,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    Account,
    ChannelCommentMessage,
    ChannelCommentMode,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    ReactionTask,
    ReactionTaskStatus,
    ensure_utc,
    utc_now,
)


class CommentGateway:
    supports_channel_comment_dispatch_hook = True

    def __init__(self):
        self.preparing = asyncio.Event()
        self.dispatched = asyncio.Event()
        self.release = asyncio.Event()
        self.pause_before_rpc = False
        self.pause_after_rpc = False
        self.error = None
        self.calls = []
        self.scans = 0

    async def preflight_channel_comments(self, **kwargs):
        return ChannelCommentPreflight("Channel", 1, 10)

    async def scan_channel_posts(self, **kwargs):
        self.scans += 1
        return ChannelPostScan("Channel", ())

    async def send_channel_comment(self, **kwargs):
        self.preparing.set()
        if self.pause_before_rpc:
            await self.release.wait()
        await kwargs["before_send"]()
        self.calls.append(kwargs)
        self.dispatched.set()
        if self.pause_after_rpc:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return ChannelCommentDelivery(101, 10, -1000000000042, 7)

    async def edit_channel_comment(self, **kwargs):
        return replace(await self.send_channel_comment(**kwargs), message_id=kwargs["message_id"])


async def _ready(app, gateway=None, *, mode=ChannelCommentMode.SINGLE):
    gateway = gateway or CommentGateway()
    async with app.state.database.session_factory() as session:
        account = Account(label="Comment sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        account_id = account.id
    service = ChannelCommentService(
        app.state.database.session_factory,
        gateway,
        app.state.campaign_runner,
        app.state.settings,
    )
    edit = (
        dict(message_edit_initial_text="Initial", message_edit_delay_seconds=300)
        if mode == ChannelCommentMode.MESSAGE_EDIT
        else {}
    )
    rule = await service.create_rule(
        name="Comment quality",
        channel_ref="@quality_channel",
        mode=mode,
        primary_account_id=account_id,
        primary_text="Final",
        initial_delay_seconds=0,
        **edit,
    )
    await service._store_scan(rule.id, ChannelPostScan("Channel", (ChannelPost(100, utc_now()),)))
    return service, rule, account_id, gateway


@pytest.mark.parametrize("operation", ["pause", "delete"])
async def test_stop_during_preparation_prevents_rpc(app, operation):
    service, rule, _, gateway = await _ready(app)
    gateway.pause_before_rpc = True
    worker = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.preparing.wait(), timeout=10)
    if operation == "pause":
        await service.set_rule_enabled(rule.id, False)
    else:
        await service.delete_rule(rule.id)
    gateway.release.set()
    assert await worker
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        assert run.status == ChannelCommentRunStatus.CANCELLED
        assert run.sent_steps == 0 and run.dispatch_started_at is None
        assert await session.scalar(select(ChannelCommentMessage.id)) is None
    assert gateway.calls == []


async def test_stop_after_rpc_keeps_confirmed_receipt_and_does_not_queue_reactions(app):
    service, rule, _, gateway = await _ready(app)
    gateway.pause_after_rpc = True
    worker = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.dispatched.wait(), timeout=10)
    await service.set_rule_enabled(rule.id, False)
    gateway.release.set()
    assert await worker
    view = await service.get_rule_view(rule.id)
    assert view.delivery.confirmed == view.cancelled == 1
    async with app.state.database.session_factory() as session:
        assert await session.scalar(select(ReactionTask.id)) is None


@pytest.mark.parametrize("cancel_requested", [False, True])
async def test_crashed_dispatched_comment_is_unknown_and_never_sent_twice(app, cancel_requested):
    service, rule, account_id, gateway = await _ready(app)
    gateway.pause_after_rpc = True
    worker = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.dispatched.wait(), timeout=10)
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        run.lease_until = utc_now() - timedelta(seconds=1)
        run.cancel_requested = cancel_requested
        await session.commit()
    assert await service.process_next() is False
    assert await service.process_next() is False
    view = await service.get_rule_view(rule.id)
    assert view.uncertain == 1 and view.failed == view.cancelled == 0
    assert view.delivery.confirmed == 0 and len(gateway.calls) == 1
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        run = await session.scalar(select(ChannelCommentRun))
        assert run.error_code == "delivery_unconfirmed" and account.sent_today == 1


async def test_expired_lease_before_dispatch_can_be_retried(app):
    service, rule, _, gateway = await _ready(app)
    run_id, _ = await service._claim_next()
    async with app.state.database.session_factory() as session:
        run = await session.get(ChannelCommentRun, run_id)
        run.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await service.process_next()
    assert (await service.recent_runs(rule.id))[0].status == ChannelCommentRunStatus.SUCCEEDED
    assert len(gateway.calls) == 1


async def test_simulated_comment_is_neither_confirmed_nor_monitored_or_counted_in_quota(app):
    service, rule, account_id, _ = await _ready(app, DryRunGateway())
    assert await service.process_next()
    view = await service.get_rule_view(rule.id)
    assert view.delivery.confirmed == 0 and view.delivery.simulated == 1
    assert view.simulated_finished == 1 and view.succeeded == 0
    async with app.state.database.session_factory() as session:
        message = await session.scalar(select(ChannelCommentMessage))
        account = await session.get(Account, account_id)
        assert message.status == "simulated" and message.next_check_at is None
        assert account.sent_today == 0 and account.next_send_at is None
    text = channel_comment_runs_text(
        await service.recent_runs(rule.id),
        channel_ref=rule.channel_ref,
        delivery_summaries=await service.recent_run_delivery_summaries([message.run_id]),
    )
    assert "Тест: без отправки" in text and "Подтверждено отправлено" not in text
    assert "Открыть последний комментарий" not in text


async def test_album_across_scan_pages_creates_one_run_and_advances_full_cursor(app):
    service, rule, _, _ = await _ready(app)
    now = utc_now()
    await service._store_scan(
        rule.id,
        ChannelPostScan(
            "Channel",
            (ChannelPost(200, now, 888),),
            202,
            888,
        ),
    )
    await service._store_scan(
        rule.id,
        ChannelPostScan(
            "Channel",
            (ChannelPost(203, now, 888), ChannelPost(204, now)),
            205,
            None,
        ),
    )
    runs = await service.recent_runs(rule.id)
    assert {run.source_post_id for run in runs} == {100, 200, 204}
    assert (await service.get_rule_view(rule.id)).rule.last_seen_post_id == 205


async def test_collection_flood_stops_send_and_scan_until_account_cooldown(app):
    service, rule, account_id, gateway = await _ready(app)
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        run.hidden_mentions_enabled_snapshot = True
        await session.commit()
    gateway.collect_channel_comment_mentionable_user_ids = AsyncMock(
        side_effect=TemporaryGatewayError("Flood", code="flood_wait", retry_after=900),
    )
    start = utc_now()
    assert await service.process_next()
    await service.scan_rule(rule.id)
    run = (await service.recent_runs(rule.id))[0]
    assert run.status == ChannelCommentRunStatus.QUEUED and run.error_code == "flood_wait"
    assert ensure_utc(run.scheduled_for) >= start + timedelta(seconds=900)
    assert run.dispatch_started_at is None and run.attempt_count == 0
    assert gateway.calls == [] and gateway.scans == 0
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account.flood_hits == 1


async def test_edit_receives_original_discussion_identity(app):
    service, rule, _, gateway = await _ready(app, mode=ChannelCommentMode.MESSAGE_EDIT)
    assert await service.process_next()
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await service.process_next()
    assert gateway.calls[-1]["expected_discussion_peer_id"] == -1000000000042
    assert gateway.calls[-1]["expected_thread_root_message_id"] == 10
    view = await service.get_rule_view(rule.id)
    assert view.succeeded == 1 and view.delivery.confirmed == 1


@pytest.mark.parametrize("peer,root", [(-1000000000043, 10), (-1000000000042, 11)])
def test_changed_discussion_is_refused(peer, root):
    with pytest.raises(PermanentGatewayError) as raised:
        TelethonGateway._ensure_comment_thread(
            types.InputPeerChannel(42, 7),
            10,
            peer,
            root,
        )
    assert raised.value.code == "comment_thread_changed"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("MSG_ID_INVALID", "comments_unavailable"),
        ("TOPIC_ID_INVALID", "comments_unavailable"),
        ("ALLOW_PAYMENT_REQUIRED_10", "comment_paid_message_required"),
        ("PRIVACY_PREMIUM_REQUIRED", "comment_sender_requires_premium"),
    ],
)
def test_comment_rpc_errors_are_specific(raw, expected):
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(_env_file=None)
    mapped = gateway._map_rpc_error(errors.RPCError(None, raw, 400), context="channel_comment")
    assert mapped.code == expected and not mapped.retryable


class PanelParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.visible = []

    def handle_starttag(self, tag, attrs):
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack.pop() == tag

    def handle_data(self, data):
        self.visible.append(data)


@pytest.mark.parametrize("mode", list(ChannelCommentMode))
def test_large_draft_fits_telegram_and_retains_valid_html(mode):
    text = "&<🚀" * 1000
    data = dict(
        comment_name="Name",
        channel_refs=["@quality_channel"],
        comment_mode=mode.value,
        primary_account_id=1,
        primary_text=text,
        message_edit_initial_text=text,
        comment_steps=[
            dict(account_id=i + 1, text=text, delay_seconds=0, reply_to_previous=i > 0)
            for i in range(20)
        ],
    )
    rendered = channel_comment_draft_text(data, {i: "Account" for i in range(1, 21)})
    parser = PanelParser()
    parser.feed(rendered)
    assert parser.stack == []
    assert len("".join(parser.visible).encode("utf-16-le")) // 2 < 4096
    callbacks = {
        b.callback_data for row in channel_comment_confirmation_menu().inline_keyboard for b in row
    }
    assert {"comments:review:channels", "comments:review:delay"} <= callbacks


@pytest.mark.parametrize(
    "value,expected",
    [
        ("@abc", "@abc"),
        ("https://t.me/s/Quality_Channel/10", "@quality_channel"),
        ("https://t.me/joinchat/MiXeD_token", "https://t.me/+MiXeD_token"),
    ],
)
def test_channel_links_normalize_without_damaging_invite_tokens(value, expected):
    assert ChannelCommentService._normalize_channel_ref(value) == expected


def test_unicode_channel_username_is_rejected():
    from proxima_traffic.services import ConflictError

    with pytest.raises(ConflictError):
        ChannelCommentService._normalize_channel_ref("@канал")


@asynccontextmanager
async def _no_account_lock(account_id):
    yield


async def test_preflight_uses_an_open_post_after_a_closed_latest_post():
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(_env_file=None)
    gateway._account_operation = _no_account_lock
    channel = SimpleNamespace(broadcast=True, title="News")
    discussion_entity = types.InputPeerChannel(42, 7)

    class Client:
        async def iter_messages(self, entity, **kwargs):
            yield SimpleNamespace(id=20, action=None)
            yield SimpleNamespace(id=19, action=None)

    gateway._ensure_client = AsyncMock(return_value=Client())
    gateway._resolve_channel_comment_channel = AsyncMock(return_value=channel)
    gateway._discussion_for_post = AsyncMock(side_effect=[errors.MsgIdInvalidError(None), object()])
    gateway._discussion_entity = AsyncMock(return_value=(discussion_entity, 190))
    gateway._ensure_discussion_membership = AsyncMock()
    gateway._ensure_can_send = lambda entity: None
    result = await gateway.preflight_channel_comments(
        account=Account(id=1, label="Sender"),
        channel_ref="@quality_channel",
    )
    assert result.channel_post_id == 19 and result.discussion_root_message_id == 190


@pytest.mark.parametrize("media", [False, True])
async def test_dispatch_callback_runs_after_preparation_and_can_stop_actual_rpc(media):
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(_env_file=None)
    gateway._delivery_receipt = AsyncMock(return_value=(None, None))
    gateway._prepare_uploaded_media = AsyncMock(return_value=types.InputMediaEmpty())
    client = AsyncMock()

    async def stop():
        if media:
            gateway._prepare_uploaded_media.assert_awaited_once()
        raise PermanentGatewayError("Stopped", code="cancelled_before_send")

    with pytest.raises(PermanentGatewayError) as raised:
        if media:
            await gateway._send_media(
                client,
                types.InputPeerChannel(42, 7),
                "Text",
                10,
                "comment:test",
                html_text=None,
                media_path="media.jpg",
                media_kind="photo",
                before_send=stop,
            )
        else:
            await gateway._send(
                client,
                types.InputPeerChannel(42, 7),
                "Text",
                10,
                "comment:test",
                before_send=stop,
            )
    assert raised.value.code == "cancelled_before_send"
    client.assert_not_awaited()


async def test_slow_scanner_does_not_block_ready_comment_processing():
    service = object.__new__(ChannelCommentService)
    service._stop_event = asyncio.Event()
    service._settings = SimpleNamespace(channel_comments_poll_seconds=0.01)
    scanning = asyncio.Event()
    scan_cancelled = asyncio.Event()

    async def scan():
        scanning.set()
        try:
            await asyncio.Event().wait()
        finally:
            scan_cancelled.set()

    async def process():
        await asyncio.wait_for(scanning.wait(), timeout=1)
        service.stop()
        return True

    service.scan_due_rules = scan
    service.process_next = process
    await asyncio.wait_for(service.run_forever(), timeout=2)
    assert scan_cancelled.is_set() and not service.running


@pytest.mark.parametrize("missing", [False, True])
async def test_comment_reaction_tracks_original_discussion_and_skips_missing_source(app, missing):
    from proxima_traffic.reactions import ReactionWorker

    service, rule, _, gateway = await _ready(app)
    async with app.state.database.session_factory() as session:
        reactor = Account(label="Reactor", ownership_confirmed=True)
        session.add(reactor)
        await session.commit()
    await service.update_rule_reactions(
        rule.id,
        enabled=True,
        count=1,
        emoji="👍",
        account_ids=[reactor.id],
    )
    gateway.send_channel_comment_reaction = AsyncMock(return_value="👍")
    assert await service.process_next()
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        if missing:
            message = await session.scalar(select(ChannelCommentMessage))
            message.status = "missing"
        await session.commit()
    worker = ReactionWorker(
        app.state.database.session_factory,
        gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="comment-quality-reactions",
    )
    assert await worker.process_next()
    if missing:
        gateway.send_channel_comment_reaction.assert_not_awaited()
        async with app.state.database.session_factory() as session:
            task = await session.get(ReactionTask, task.id)
            assert (
                task.status == ReactionTaskStatus.SKIPPED and task.error_code == "comment_missing"
            )
    else:
        call = gateway.send_channel_comment_reaction.await_args.kwargs
        assert call["expected_discussion_peer_id"] == -1000000000042
        assert call["expected_thread_root_message_id"] == 10


async def test_review_channel_edit_normalizes_duplicates_and_preserves_content(app, monkeypatch):
    from proxima_traffic.bot import handlers
    from proxima_traffic.bot.states import ChannelCommentCreateState

    class State:
        def __init__(self):
            self.data = dict(
                comment_edit_channels=True,
                comment_mode="single",
                comment_name="Draft",
                primary_account_id=1,
                primary_text="Saved content",
                initial_delay_seconds=30,
            )
            self.current = ChannelCommentCreateState.channel

        async def get_data(self):
            return self.data.copy()

        async def update_data(self, **values):
            self.data.update(values)
            return self.data.copy()

        async def set_state(self, value):
            self.current = value

    answer = AsyncMock()
    monkeypatch.setattr(handlers, "answer_panel_input", answer)
    monkeypatch.setattr(handlers, "_comment_accounts", AsyncMock(return_value=([], {1: "Sender"})))
    state = State()
    message = SimpleNamespace(
        text="@Quality_Channel\nhttps://t.me/quality_channel/10\n@second_channel",
        document=None,
    )
    await handlers.channel_comment_channel(message, state, None, app.state.settings, None)
    assert state.current == ChannelCommentCreateState.confirm
    assert state.data["channel_refs"] == ["@quality_channel", "@second_channel"]
    assert (
        state.data["primary_text"] == "Saved content" and state.data["initial_delay_seconds"] == 30
    )
    assert state.data["comment_edit_channels"] is False
    assert answer.await_args.kwargs["reply_markup"] == channel_comment_confirmation_menu()


async def test_hidden_mentions_produce_the_exact_canonical_text_expected_by_live_gateway(app):
    service, rule, _, gateway = await _ready(app)
    gateway.collect_channel_comment_mentionable_user_ids = AsyncMock(
        return_value=tuple(range(50001, 50011)),
    )
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        run.hidden_mentions_enabled_snapshot = True
        await session.commit()
    assert await service.process_next()
    payload = gateway.calls[0]
    assert payload["text"] == render_telegram_html(payload["html_text"])
    assert payload["html_text"].count("tg://user?id=") >= 5
    live = object.__new__(TelethonGateway)
    live._account_operation = _no_account_lock
    live._ensure_client = AsyncMock(
        side_effect=TemporaryGatewayError("Prepared", code="probe_reached")
    )
    with pytest.raises(TemporaryGatewayError) as raised:
        await live.send_channel_comment(
            account=Account(id=1, label="Sender"),
            channel_ref="@quality_channel",
            channel_post_id=100,
            text=payload["text"],
            html_text=payload["html_text"],
            idempotency_key="comment:canonical",
        )
    assert raised.value.code == "probe_reached"


@pytest.mark.parametrize("length,expected_count", [(1020, 0), (1018, 5)])
async def test_mentions_respect_media_caption_capacity(app, length, expected_count):
    service, rule, account_id, gateway = await _ready(app)
    gateway.collect_channel_comment_mentionable_user_ids = AsyncMock(
        return_value=tuple(range(50001, 50006)),
    )
    run_id, token = await service._claim_next()
    async with app.state.database.session_factory() as session:
        run = await session.get(ChannelCommentRun, run_id)
        run.hidden_mentions_enabled_snapshot = True
        account = await session.get(Account, account_id)
        await session.commit()
    selected = await service._resolve_channel_comment_hidden_mentions(
        run,
        rule,
        account,
        text="x" * length,
        selection_key="caption",
        message_limit=1024,
    )
    assert len(selected) == expected_count


async def test_comment_dispatch_migration_marks_legacy_inflight_outcome_as_unknown(
    app,
    monkeypatch,
):
    from alembic import command
    from alembic.config import Config

    from proxima_traffic.config import get_settings
    from proxima_traffic.models import ChannelCommentRule

    service, rule, _, _ = await _ready(app)
    run_id, _ = await service._claim_next()
    monkeypatch.setenv("PROXIMA_DATABASE_URL", app.state.settings.database_url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        await asyncio.to_thread(command.downgrade, config, "0075_invite_verified_destination")
        await asyncio.to_thread(command.upgrade, config, "head")
        async with app.state.database.session_factory() as session:
            run = await session.get(ChannelCommentRun, run_id)
            stored_rule = await session.get(ChannelCommentRule, rule.id)
            assert run.status == ChannelCommentRunStatus.RUNNING
            assert run.dispatch_started_at is not None and not run.simulated
            assert stored_rule.primary_text == "Final" and run.sent_steps == 0
            run.lease_until = utc_now() - timedelta(seconds=1)
            await session.commit()
        assert await service.process_next() is False
        assert (await service.recent_runs(rule.id))[0].error_code == "delivery_unconfirmed"
    finally:
        get_settings.cache_clear()


async def test_manual_preflight_during_cooldown_does_not_create_a_new_flood_hit(app):
    from proxima_traffic.services import ConflictError

    service, rule, account_id, gateway = await _ready(app)
    gateway.preflight_channel_comments = AsyncMock()
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        account.cooldown_until = utc_now() + timedelta(seconds=900)
        account.flood_hits = 1
        await session.commit()
    with pytest.raises(ConflictError) as raised:
        await service.preflight_rule(rule.id)
    assert raised.value.code == "preflight_account_cooldown"
    gateway.preflight_channel_comments.assert_not_awaited()
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account.flood_hits == 1


@pytest.mark.parametrize("initial_simulated", [False, True])
async def test_started_edit_cannot_switch_between_simulation_and_real_execution(
    app, initial_simulated
):
    gateway = DryRunGateway() if initial_simulated else CommentGateway()
    service, rule, _, _ = await _ready(app, gateway, mode=ChannelCommentMode.MESSAGE_EDIT)
    assert await service.process_next()
    service._gateway = CommentGateway() if initial_simulated else DryRunGateway()
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await service.process_next()
    async with app.state.database.session_factory() as session:
        message = await session.scalar(select(ChannelCommentMessage))
        run = await session.scalar(select(ChannelCommentRun))
        assert message.status == ("simulated" if initial_simulated else "pending")
        assert run.sent_steps == 1 and run.error_code == "comment_execution_mode_changed"
        assert run.status == ChannelCommentRunStatus.CANCELLED


@pytest.mark.parametrize("channels", [["@other_channel"], ["@other_channel", "@another_channel"]])
async def test_cancelled_creation_does_not_arm_channels_after_preflight(app, channels):
    from proxima_traffic.models import ChannelCommentRule
    from proxima_traffic.services import ConflictError

    service, original, account_id, gateway = await _ready(app)
    cancelled = False

    async def preflight(**kwargs):
        nonlocal cancelled
        cancelled = True
        return ChannelCommentPreflight("Channel", 1, 10)

    async def check():
        if cancelled:
            raise ConflictError("Creation cancelled")

    gateway.preflight_channel_comments = AsyncMock(side_effect=preflight)
    with pytest.raises(ConflictError, match="Creation cancelled"):
        await service.create_rules(
            name="Cancelled batch",
            channel_refs=channels,
            mode=ChannelCommentMode.SINGLE,
            primary_account_id=account_id,
            primary_text="Text",
            initial_delay_seconds=0,
            before_create=check,
        )
    assert gateway.preflight_channel_comments.await_count == 1
    async with app.state.database.session_factory() as session:
        assert list(await session.scalars(select(ChannelCommentRule.id))) == [original.id]
