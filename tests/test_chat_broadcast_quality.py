"""Chat delivery invariants; all RPCs and databases are private test doubles."""

import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from proxima_traffic.bot import handlers
from proxima_traffic.bot.keyboards import campaign_run_confirmation, launch_details_menu
from proxima_traffic.bot.renderers import _action_delivery_label, _launch_delivery_lines
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.gateways import PermanentGatewayError, SentMessage, TemporaryGatewayError
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ActionLog,
    ActionStatus,
    Campaign,
    CampaignMode,
    CampaignRun,
    ReactionTask,
    RunStatus,
    Target,
    TargetList,
    TargetListItem,
    ensure_utc,
    utc_now,
)
from proxima_traffic.notifications import RunNotifier


class ControlledGateway:
    supports_campaign_dispatch_hook = True
    simulated = False

    def __init__(self, *, block_at=None, error=None, message_ids=(501,)):
        self.block_at = block_at
        self.error = error
        self.message_ids = message_ids
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self.checked_ids = []
        self.missing_ids = set()
        self.read_error = None

    async def send_content(self, *, before_send, **kwargs):
        if self.block_at == "prepare":
            self.started.set()
            await self.release.wait()
        await before_send()
        self.calls += 1
        if self.block_at == "dispatch":
            self.started.set()
            await self.release.wait()
        if self.error:
            raise self.error
        return SentMessage(
            message_id=max(self.message_ids),
            message_ids=self.message_ids,
            simulated=self.simulated,
        )

    async def verify_sent_message(self, *, message_id, **kwargs):
        self.checked_ids.append(message_id)
        if self.read_error:
            raise self.read_error
        if message_id in self.missing_ids:
            raise PermanentGatewayError("missing", code="post_deleted_by_moderation")


async def prepare(app, gateway, *, smart=False):
    runner = app.state.campaign_runner
    runner._gateway = gateway
    app.state.run_queue._delivery_mode = gateway.simulated
    app.state.delayed_delivery_monitor._gateway = gateway
    app.state.delayed_delivery_monitor._settings.delayed_delivery_verify_batch_size = 1
    async with app.state.database.session_factory() as session:
        account = Account(label="Sender", ownership_confirmed=True)
        target = Target(chat_ref="@test_chat", title="Test chat", approved_by_operator=True)
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Controlled broadcast",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="A useful update",
            smart_timer_mode=smart,
            repeat_forever=smart,
        )
        session.add(campaign)
        await session.commit()
        campaign_id = campaign.id
    runs = await app.state.run_queue.enqueue_campaign(campaign_id, "controlled-broadcast")
    assert len(runs) == 1
    return runs[0]


async def due_action(app):
    async with app.state.database.session_factory() as session:
        action = await session.scalar(select(ActionLog))
        assert action is not None
        action.created_at = utc_now() - timedelta(hours=1)
        action.delayed_delivery_retry_at = utc_now() - timedelta(seconds=1)
        await session.commit()
        return action.id


@pytest.mark.parametrize("block_at", ["prepare", "dispatch"])
async def test_stop_respects_rpc_boundary_and_retains_confirmed_receipt(app, block_at):
    gateway = ControlledGateway(block_at=block_at)
    run = await prepare(app, gateway, smart=True)
    task = asyncio.create_task(app.state.queue_worker.process_next())
    await asyncio.wait_for(gateway.started.wait(), 10)
    await app.state.run_queue.cancel_launch(run.id)
    gateway.release.set()
    result = await task
    assert result.status == RunStatus.CANCELLED
    assert gateway.calls == (1 if block_at == "dispatch" else 0)
    async with app.state.database.session_factory() as session:
        actions = list(await session.scalars(select(ActionLog)))
        assert len(actions) == (1 if block_at == "dispatch" else 0)
        assert await session.scalar(select(func.count(ReactionTask.id))) == 0
        assert await session.scalar(select(func.count(CampaignRun.id))) == 1
        if actions:
            assert actions[0].status == ActionStatus.SUCCEEDED
            assert actions[0].external_message_id == 501


@pytest.mark.parametrize("dispatched", [False, True])
async def test_expired_worker_lease_retries_only_before_dispatch(app, dispatched):
    gateway = ControlledGateway()
    run = await prepare(app, gateway)
    claimed = await app.state.run_queue.claim_next("crashed-worker")
    assert claimed is not None
    if dispatched:
        await app.state.campaign_runner._mark_campaign_dispatch(
            claimed,
            claimed.sender_account_id,
            claimed.target_id,
            app.state.campaign_runner._campaign_step_key(claimed),
        )
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run.id)
        stored.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await app.state.run_queue.recover_expired_leases() == 1
    stored = await app.state.run_queue.get_run(run.id)
    assert stored.status == (RunStatus.FAILED if dispatched else RunStatus.QUEUED)
    if dispatched:
        assert stored.error_code == "delivery_unconfirmed"
        assert await app.state.run_queue.retry_failed_launch(run.id) == 0
        assert await app.state.run_queue.recover_expired_leases() == 0
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, stored.sender_account_id)
            assert account.sent_today == 1
            actions = list(await session.scalars(select(ActionLog)))
            assert len(actions) == 1
            assert actions[0].error_code == "delivery_unconfirmed"
            assert actions[0].external_message_id is None
        stats = await AdminRepository(app.state.database.session_factory).dashboard()
        assert stats.uncertain_messages_24h == 1
        assert stats.sent_messages_24h == 0
    assert gateway.calls == 0


@pytest.mark.parametrize("dispatched", [False, True])
async def test_timeout_knows_whether_rpc_began(app, dispatched):
    run = await prepare(app, ControlledGateway())
    claimed = await app.state.run_queue.claim_next("timeout-worker")
    if dispatched:
        await app.state.campaign_runner._mark_campaign_dispatch(
            claimed,
            claimed.sender_account_id,
            claimed.target_id,
            app.state.campaign_runner._campaign_step_key(claimed),
        )
    result = await app.state.run_queue.mark_step_error(
        run.id,
        "timeout",
        lease_token=claimed.lease_token,
        error_code="step_timeout",
        permanent=True,
    )
    assert result.error_code == ("delivery_unconfirmed" if dispatched else "preparation_timeout")
    assert result.status == (RunStatus.FAILED if dispatched else RunStatus.QUEUED)


@pytest.mark.parametrize(
    "code, retryable",
    [
        ("network_error", True),
        ("delivery_unconfirmed", False),
        ("post_deleted_by_moderation", False),
        ("album_partial_missing", False),
        ("cancelled_outcome_unknown", False),
        ("step_timeout", False),
    ],
)
async def test_manual_retry_and_button_share_same_safe_filter(app, code, retryable):
    run = await prepare(app, ControlledGateway())
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run.id)
        stored.status = RunStatus.FAILED
        stored.error_code = code
        await session.commit()
    repository = AdminRepository(app.state.database.session_factory)
    launch, _ = await repository.get_launch(run.id)
    assert launch.retryable_failed == int(retryable)
    buttons = [button for row in launch_details_menu(launch, 0).inline_keyboard for button in row]
    assert any("retry" in (button.callback_data or "") for button in buttons) is retryable
    assert await app.state.run_queue.retry_failed_launch(run.id) == int(retryable)


@pytest.mark.parametrize("missing_ids", [{501, 502}, {501}, set()])
async def test_late_album_observation_preserves_send_count_and_checks_every_element(
    app, missing_ids
):
    gateway = ControlledGateway(message_ids=(501, 502))
    gateway.missing_ids = missing_ids
    run = await prepare(app, gateway)
    assert (await app.state.queue_worker.process_next()).status == RunStatus.SUCCEEDED
    action_id = await due_action(app)
    recovery = AsyncMock(return_value=False)
    app.state.campaign_runner.recover_delayed_membership_gate = recovery
    assert await app.state.delayed_delivery_monitor.check_due_once() == 1
    assert gateway.checked_ids == [501, 502]
    assert recovery.await_count == (1 if len(missing_ids) == 2 else 0)
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        expected = (
            "missing" if len(missing_ids) == 2 else "partial_missing" if missing_ids else "present"
        )
        assert action.delayed_delivery_status == expected
        assert json.loads(action.external_message_ids_json) == [501, 502]
        assert "ошибка попытки" not in _action_delivery_label(action)
    repository = AdminRepository(app.state.database.session_factory)
    launch, _ = await repository.get_launch(run.id)
    assert launch.sent_publications == 1
    assert "подтверждено: <b>1</b>" in "\n".join(_launch_delivery_lines(launch))
    assert "физических сообщений: <b>2</b>" in "\n".join(_launch_delivery_lines(launch))
    stats = await repository.dashboard()
    assert stats.sent_messages_24h == 1
    assert stats.failed_messages_24h == 0
    async with app.state.database.session_factory() as session:
        summary, target_count = await RunNotifier._delivery_summary(
            session, ActionLog.run_id == run.id
        )
        assert "подтверждено: <b>1</b>" in "\n".join(summary)
        assert target_count == 1
    assert gateway.calls == 1


@pytest.mark.parametrize("generic", [False, True])
async def test_unreadable_delivery_is_bounded_and_not_treated_as_deleted(app, generic):
    gateway = ControlledGateway()
    run = await prepare(app, gateway)
    await app.state.queue_worker.process_next()
    action_id = await due_action(app)
    gateway.read_error = (
        RuntimeError("broken reader")
        if generic
        else TemporaryGatewayError("network down", code="network_error")
    )
    for attempt in range(3):
        if generic:
            with pytest.raises(RuntimeError):
                await app.state.delayed_delivery_monitor.check_due_once()
        else:
            await app.state.delayed_delivery_monitor.check_due_once()
        async with app.state.database.session_factory() as session:
            action = await session.get(ActionLog, action_id)
            assert action.status == ActionStatus.SUCCEEDED
            assert action.delayed_delivery_status == "unavailable"
            assert action.delayed_delivery_attempt_count == attempt + 1
            action.delayed_delivery_retry_at = utc_now() - timedelta(seconds=1)
            await session.commit()
    assert await app.state.delayed_delivery_monitor.check_due_once() == 0
    assert len(gateway.checked_ids) == 3
    assert (await app.state.run_queue.get_run(run.id)).status == RunStatus.SUCCEEDED


async def test_monitor_respects_account_cooldown_without_spending_read_budget(app):
    gateway = ControlledGateway()
    run = await prepare(app, gateway)
    await app.state.queue_worker.process_next()
    action_id = await due_action(app)
    until = utc_now() + timedelta(seconds=900)
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        account.cooldown_until = until
        await session.commit()
    await app.state.delayed_delivery_monitor.check_due_once()
    assert gateway.checked_ids == []
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        assert action.delayed_delivery_attempt_count == 0
        assert ensure_utc(action.delayed_delivery_retry_at) >= until


async def test_legacy_unknown_mode_is_not_used_to_quarantine_chat(app):
    gateway = ControlledGateway()
    run = await prepare(app, gateway)
    await app.state.queue_worker.process_next()
    action_id = await due_action(app)
    gateway.missing_ids = {501}
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        action.simulated = None
        await session.commit()
    assert await app.state.delayed_delivery_monitor.check_due_once() == 0
    assert gateway.checked_ids == []
    async with app.state.database.session_factory() as session:
        target = await session.get(Target, run.target_id)
        assert target.quality_status != "bad"


@pytest.mark.parametrize("retry_after", [2, 1200])
async def test_floodwait_uses_at_least_500_seconds_and_honors_longer_server_delay(app, retry_after):
    gateway = ControlledGateway(
        error=TemporaryGatewayError("flood", code="flood_wait", retry_after=retry_after)
    )
    await prepare(app, gateway)
    before = utc_now()
    result = await app.state.queue_worker.process_next()
    assert result.status == RunStatus.QUEUED
    assert ensure_utc(result.scheduled_for) >= before + timedelta(seconds=max(500, retry_after))
    assert result.attempt_count == 0
    assert result.dispatch_started_at is None


async def test_floodwait_marker_is_kept_until_pause_and_defer_are_persisted(app, monkeypatch):
    gateway = ControlledGateway(
        error=TemporaryGatewayError(
            "flood",
            code="flood_wait",
            retry_after=2,
        )
    )
    run = await prepare(app, gateway)
    runner = app.state.campaign_runner
    original = runner._defer_run
    observed = False

    async def defer(session, current, resume_at, *, reason):
        nonlocal observed
        async with app.state.database.session_factory() as inspection:
            stored = await inspection.get(CampaignRun, run.id)
            account = await inspection.get(Account, run.sender_account_id)
            assert stored.dispatch_started_at is not None
            assert ensure_utc(account.cooldown_until) > utc_now()
            observed = True
        return await original(session, current, resume_at, reason=reason)

    monkeypatch.setattr(runner, "_defer_run", defer)
    result = await app.state.queue_worker.process_next()
    assert observed
    assert result.status == RunStatus.QUEUED and result.dispatch_started_at is None


async def test_simulation_does_not_consume_real_quota_or_target_quality(app):
    gateway = ControlledGateway()
    gateway.simulated = True
    run = await prepare(app, gateway)
    await app.state.queue_worker.process_next()
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        target = await session.get(Target, run.target_id)
        assert account.sent_today == 0
        assert target.quality_successes == 0


async def test_quota_recovery_keeps_longer_existing_telegram_pause(app):
    run = await prepare(app, ControlledGateway())
    runner = app.state.campaign_runner
    settings = app.state.settings
    settings.account_micro_break_every = 1
    settings.account_micro_break_min_seconds = 300
    settings.account_micro_break_max_seconds = 300
    settings.account_pace_min_seconds = 60
    settings.account_pace_max_seconds = 60
    next_send = utc_now() + timedelta(seconds=900)
    cooldown = utc_now() + timedelta(seconds=3600)
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        account.next_send_at = next_send
        account.cooldown_until = cooldown
        account.cooldown_reason = "flood_wait"
        await session.commit()
    assert await runner.record_external_account_send(
        run.sender_account_id, operation_key="same-rpc"
    )
    assert not await runner.record_external_account_send(
        run.sender_account_id, operation_key="same-rpc"
    )
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        assert account.sent_today == 1
        assert ensure_utc(account.cooldown_until) == cooldown
        assert account.cooldown_reason == "flood_wait"
        assert ensure_utc(account.next_send_at) == next_send


@pytest.mark.parametrize("saved_mode", [True, None])
async def test_dry_or_legacy_launch_cannot_become_live_after_restart(app, saved_mode):
    gateway = ControlledGateway()
    gateway.simulated = True
    run = await prepare(app, gateway)
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run.id)
        stored.simulated_snapshot = saved_mode
        await session.commit()
    gateway.simulated = False
    result = await app.state.queue_worker.process_next()
    assert result.status == RunStatus.CANCELLED
    assert gateway.calls == 0
    assert result.simulated_snapshot is saved_mode


async def test_disabled_sender_during_preparation_never_sends(app):
    gateway = ControlledGateway(block_at="prepare")
    run = await prepare(app, gateway)
    task = asyncio.create_task(app.state.queue_worker.process_next())
    await asyncio.wait_for(gateway.started.wait(), 10)
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        account.status = AccountStatus.DISABLED
        await session.commit()
    gateway.release.set()
    result = await task
    assert result.status == RunStatus.FAILED
    assert gateway.calls == 0


async def test_account_paused_during_upload_defers_before_send(app):
    gateway = ControlledGateway(block_at="prepare")
    run = await prepare(app, gateway)
    task = asyncio.create_task(app.state.queue_worker.process_next())
    await asyncio.wait_for(gateway.started.wait(), 10)
    until = utc_now() + timedelta(seconds=900)
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        account.cooldown_until = until
        await session.commit()
    gateway.release.set()
    result = await task
    assert result.status == RunStatus.QUEUED
    assert result.error_code == "account_pacing"
    assert result.attempt_count == 0
    assert ensure_utc(result.scheduled_for) >= until
    assert gateway.calls == 0


async def test_unexpected_error_after_dispatch_is_one_unknown_outcome_and_one_quota_charge(app):
    run = await prepare(app, ControlledGateway(error=RuntimeError("unexpected transport failure")))
    result = await app.state.queue_worker.process_next()
    assert result.status == RunStatus.FAILED
    assert result.error_code == "delivery_unconfirmed"
    async with app.state.database.session_factory() as session:
        actions = list(await session.scalars(select(ActionLog)))
        account = await session.get(Account, run.sender_account_id)
        assert len(actions) == 1
        assert actions[0].error_code == "delivery_unconfirmed"
        assert account.sent_today == 1
    assert await app.state.run_queue.retry_failed_launch(run.id) == 0


async def test_preparation_error_is_definite_failure_with_zero_send_quota(app):
    gateway = ControlledGateway()
    gateway.send_content = AsyncMock(side_effect=RuntimeError("broken preparation"))
    run = await prepare(app, gateway)
    result = await app.state.queue_worker.process_next()
    assert result.status == RunStatus.FAILED and result.error_code == "preparation_failed"
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, run.sender_account_id)
        assert account.sent_today == 0
    stats = await AdminRepository(app.state.database.session_factory).dashboard()
    assert stats.uncertain_messages_24h == 0
    assert stats.failed_messages_24h == 1
    assert await app.state.run_queue.retry_failed_launch(run.id) == 1


@pytest.mark.parametrize("code", ["post_deleted_by_moderation", "album_partial_missing"])
async def test_crash_after_persisting_removal_never_replays_album_or_charges_quota_twice(app, code):
    gateway = ControlledGateway(
        error=PermanentGatewayError(
            "Confirmed receipt, missing elements",
            code=code,
            external_message_id=502,
            external_message_ids=(501, 502),
        )
    )
    run = await prepare(app, gateway)
    claimed = await app.state.run_queue.claim_next("interrupted-outcome-worker")
    with pytest.raises(PermanentGatewayError):
        await app.state.campaign_runner.process_step(run.id, claimed.lease_token)
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run.id)
        assert stored.dispatch_started_at is not None
        stored.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await app.state.run_queue.recover_expired_leases() == 1
    stored = await app.state.run_queue.get_run(run.id)
    assert stored.status == RunStatus.FAILED and stored.error_code == code
    assert await app.state.run_queue.retry_failed_launch(run.id) == 0
    async with app.state.database.session_factory() as session:
        assert await session.scalar(select(func.count(ActionLog.id))) == 1
        account = await session.get(Account, run.sender_account_id)
        assert account.sent_today == 1
    stats = await AdminRepository(app.state.database.session_factory).dashboard()
    assert stats.sent_messages_24h == 1
    assert stats.uncertain_messages_24h == 0


async def test_repeated_confirm_taps_create_one_launch_and_new_review_can_start_another(
    app, monkeypatch
):
    run = await prepare(app, ControlledGateway())
    repository = AdminRepository(app.state.database.session_factory)
    monkeypatch.setattr(handlers, "_edit_callback", AsyncMock())
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    preflight = SimpleNamespace(cached_campaign_summary=AsyncMock(return_value=None))

    async def confirm(token):
        keyboard = campaign_run_confirmation(run.campaign_id, 0, launch_token=token)
        data = keyboard.inline_keyboard[0][0].callback_data
        assert len(data.encode("utf-8")) <= 64
        await handlers.enqueue_campaign(
            SimpleNamespace(data=data),
            repository,
            preflight,
            app.state.run_queue,
            app.state.settings,
        )

    await asyncio.gather(confirm("aabbccddeeff"), confirm("aabbccddeeff"))
    await confirm("123456789abc")
    async with app.state.database.session_factory() as session:
        keys = list(await session.scalars(select(CampaignRun.launch_key)))
        assert keys.count(f"botchat:{run.campaign_id}:aabbccddeeff") == 1
        assert keys.count(f"botchat:{run.campaign_id}:123456789abc") == 1


async def test_source_list_edit_preserves_already_selected_order_audience(app):
    gateway = ControlledGateway(block_at="prepare")
    run = await prepare(app, gateway)
    async with app.state.database.session_factory() as session:
        audience = TargetList(name="Selected chats")
        session.add(audience)
        await session.flush()
        item = TargetListItem(target_list_id=audience.id, target_id=run.target_id)
        session.add(item)
        campaign = await session.get(Campaign, run.campaign_id)
        campaign.target_id = None
        campaign.target_list_id = audience.id
        await session.commit()
        item_id = item.id
    task = asyncio.create_task(app.state.queue_worker.process_next())
    await asyncio.wait_for(gateway.started.wait(), 10)
    async with app.state.database.session_factory() as session:
        item = await session.get(TargetListItem, item_id)
        item.enabled = False
        await session.commit()
    gateway.release.set()
    assert (await task).status == RunStatus.SUCCEEDED
    assert gateway.calls == 1


async def test_stale_preflight_progress_does_not_replace_current_panel(monkeypatch):
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())

    async def inspect(campaign_id, *, on_progress):
        await on_progress(0, 2, 0, 1)
        await on_progress(2, 2, 1, 1)
        return "completed"

    result = await handlers._inspect_campaign_with_visible_progress(
        object(),
        SimpleNamespace(inspect_campaign=inspect),
        123,
        is_current=AsyncMock(return_value=False),
    )
    assert result == "completed"
    edit.assert_not_awaited()


async def test_cancelled_preflight_does_not_clear_new_draft(monkeypatch):
    data = {"order_draft_active": True, "order_selection_key": "original"}
    state = SimpleNamespace()
    state.get_data = AsyncMock(side_effect=lambda: dict(data))
    state.update_data = AsyncMock(side_effect=lambda **kwargs: data.update(kwargs))
    state.clear = AsyncMock()
    campaign = SimpleNamespace(id=101)
    monkeypatch.setattr(
        handlers, "_create_campaign_from_order_draft", AsyncMock(return_value=campaign)
    )

    async def inspect(*args, **kwargs):
        data.clear()
        data.update(order_draft_active=True, order_selection_key="new")
        raise RuntimeError("old preflight failed")

    monkeypatch.setattr(handlers, "_inspect_campaign_with_visible_progress", inspect)
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    repository = SimpleNamespace(campaign_sender_ids=AsyncMock(return_value=[1]))
    callback = SimpleNamespace(from_user=SimpleNamespace(id=998821))
    await handlers.launch_order_draft(callback, state, repository, object(), object())
    state.clear.assert_not_awaited()
    edit.assert_not_awaited()
    assert data["order_selection_key"] == "new"


@pytest.mark.parametrize("text", ["oops", "-1", "169", "1.5"])
async def test_invalid_active_duration_cannot_create_indefinite_launch(text):
    message = SimpleNamespace(text=text, answer=AsyncMock())
    queue = SimpleNamespace(update_active_launch=AsyncMock())
    state = SimpleNamespace(get_data=AsyncMock(), clear=AsyncMock())
    await handlers.save_active_launch_duration(message, state, object(), object(), queue)
    queue.update_active_launch.assert_not_awaited()
    state.clear.assert_not_awaited()
    message.answer.assert_awaited_once()
