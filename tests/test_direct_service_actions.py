"""Private-chat actions: immutable account assignment, pacing and honest outcomes."""

import asyncio
import json
import shutil
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy import delete, event, func, inspect, select, update
from telethon import errors, functions, types

from proxima_traffic.bot import handlers
from proxima_traffic.bot.direct_message_ui import (
    direct_campaign_confirmation_menu,
    direct_launch_details_text,
    direct_launches_menu,
    direct_launches_text,
    direct_messages_home_menu,
    direct_segment_menu,
)
from proxima_traffic.bot.recipient_report_ui import recipient_report_text
from proxima_traffic.bot.states import DirectCampaignCreateState
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import (
    ChatActivity,
    DirectRecipient,
    DirectServiceActionResult,
    DryRunGateway,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ActionKind,
    ActionLog,
    ActionStatus,
    Campaign,
    CampaignMode,
    CampaignRun,
    DirectCampaign,
    DirectCampaignAccount,
    DirectLaunch,
    DirectMessageTask,
    DirectMessageTaskStatus,
    RunStatus,
    Subscriber,
    SubscriberAccountPeer,
    SubscriberSegment,
    Target,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError


@pytest.fixture
async def service_context(tmp_path, schema_template, monkeypatch):
    database_path = tmp_path / "actions.db"
    shutil.copy2(schema_template, database_path)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{database_path}",
        admin_api_key="test",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
    )
    monkeypatch.setattr("proxima_traffic.direct_messages.service_action_delay_seconds", lambda: 60)
    database = Database(settings.database_url)
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory, gateway, runner, settings, worker_id="service-actions-test"
    )
    source = tmp_path / "people.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(
                {
                    "telegram_id": 991001 + i,
                    "username": f"action_person_{i}" if i != 2 else None,
                    "is_premium": i % 2 == 0,
                }
            )
            for i in range(5)
        ),
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source, list_name="Actions", source_filename=source.name
    )
    async with database.session_factory() as session:
        accounts = [Account(label=f"Sender {i}", ownership_confirmed=True) for i in range(3)]
        session.add_all(accounts)
        await session.commit()
    try:
        yield SimpleNamespace(
            db=database,
            service=service,
            gateway=gateway,
            settings=settings,
            accounts=accounts,
            list_id=imported.subscriber_list.id,
        )
    finally:
        await database.dispose()


async def _campaign(ctx, *, limit=1, segment=SubscriberSegment.ALL, **kwargs):
    return await ctx.service.create_campaign(
        name="Action",
        subscriber_list_id=ctx.list_id,
        segment=segment,
        recipient_limit=limit,
        contact_cooldown_days=0,
        account_ids=[a.id for a in ctx.accounts[:2]],
        operation_kind="auto_delete",
        auto_delete_seconds=86400,
        **kwargs,
    )


async def _make_due(ctx, launch_id):
    async with ctx.db.session_factory() as session:
        await session.execute(
            update(DirectMessageTask)
            .where(
                DirectMessageTask.launch_id == launch_id,
                DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
            )
            .values(scheduled_for=utc_now() - timedelta(seconds=1))
        )
        await session.commit()


@pytest.mark.parametrize(
    "latest_message_id,wait_reason",
    [
        (100, "chat_idle_last_managed_post"),
        (101, "visibility_wait"),
    ],
)
async def test_waiting_broadcast_does_not_starve_service_actions(
    service_context, monkeypatch, latest_message_id, wait_reason
):
    ctx = service_context
    ctx.settings.account_pace_min_seconds = 60
    ctx.settings.account_pace_max_seconds = 600
    sender_id = ctx.accounts[0].id
    launch = await ctx.service.launch_campaign((await _campaign(ctx)).id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    expired_interval = utc_now() - timedelta(seconds=1)
    monkeypatch.setattr(
        ctx.gateway,
        "inspect_activity",
        AsyncMock(
            return_value=ChatActivity(latest_message_id=latest_message_id, checked_at=utc_now())
        ),
    )
    async with ctx.db.session_factory() as session:
        target = Target(chat_ref="@idle_chat", title="Idle chat", approved_by_operator=True)
        session.add(target)
        await session.flush()
        campaign = Campaign(
            name="Waiting broadcast",
            mode=CampaignMode.BROADCAST,
            primary_account_id=sender_id,
            target_id=target.id,
            primary_text="Post",
            activity_aware=True,
            min_messages_between_posts=3,
            visibility_max_wait_seconds=0,
        )
        session.add(campaign)
        await session.flush()
        previous = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=sender_id,
            request_key="previous-post",
            status=RunStatus.SUCCEEDED,
            primary_message_id=100,
        )
        pending = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=sender_id,
            request_key="waiting-post",
            simulated_snapshot=True,
            status=RunStatus.RUNNING,
            lease_token="waiting-lease",
            repeat_until=utc_now() + timedelta(hours=1),
        )
        session.add_all([previous, pending])
        await session.flush()
        session.add(
            ActionLog(
                run_id=previous.id,
                account_id=sender_id,
                target_id=target.id,
                kind=ActionKind.PRIMARY_MESSAGE,
                status=ActionStatus.SUCCEEDED,
                external_message_id=100,
                text="Previous post",
                simulated=True,
            )
        )
        account = await session.get(Account, sender_id)
        account.next_send_at = expired_interval
        await session.commit()

    deferred = await ctx.service._runner.process_step(pending.id, "waiting-lease")
    assert deferred.status == RunStatus.QUEUED
    assert deferred.error_code == wait_reason
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, sender_id)
        assert ensure_utc(account.next_send_at) == expired_interval
        assert account.sent_today == 0
        assert account.send_reservation_token is None

    completed = await ctx.service.process_next()
    assert completed.status == DirectMessageTaskStatus.SUCCEEDED
    assert completed.sender_account_id == sender_id
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, sender_id)
        assert account.sent_today == 1
        assert ensure_utc(account.next_send_at) >= utc_now() + timedelta(seconds=59)


async def test_service_launch_is_username_only_and_keeps_selected_accounts_and_snapshot(
    service_context,
):
    ctx = service_context
    campaign = await _campaign(ctx, limit=5)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    assert launch.operation_kind_snapshot == "auto_delete"
    assert launch.auto_delete_seconds_snapshot == 86400
    assert launch.primary_text_snapshot == ""
    assert json.loads(launch.account_ids_snapshot_json) == [a.id for a in ctx.accounts[:2]]
    async with ctx.db.session_factory() as session:
        tasks = list(
            await session.scalars(
                select(DirectMessageTask)
                .where(DirectMessageTask.launch_id == launch.id)
                .order_by(DirectMessageTask.id)
            )
        )
        assert len(tasks) == 4
        assert [t.sender_account_id for t in tasks] == [ctx.accounts[i % 2].id for i in range(4)]
        for i, task in enumerate(tasks):
            assert (
                ensure_utc(task.scheduled_for) - ensure_utc(launch.consent_confirmed_at)
            ).total_seconds() == 60 * (1 + i // 2)
        saved = await session.get(DirectCampaign, campaign.id)
        saved.operation_kind = "protect_content"
        saved.auto_delete_seconds = None
        await session.commit()
    await _make_due(ctx, launch.id)
    called = []

    async def perform(**kwargs):
        called.append(
            (kwargs["account"].id, kwargs["operation_kind"], kwargs["auto_delete_seconds"])
        )
        await kwargs["before_action"]()
        return DirectServiceActionResult(kwargs["recipient"].telegram_user_id, changed=True)

    ctx.gateway.perform_direct_service_action = perform
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.SUCCEEDED
    assert called == [(ctx.accounts[0].id, "auto_delete", 86400)]
    async with ctx.db.session_factory() as session:
        sender = await session.get(Account, ctx.accounts[0].id)
        assert ensure_utc(sender.next_send_at) > utc_now() + timedelta(seconds=58)


async def test_service_rejects_text_and_nonpremium_segment(service_context):
    with pytest.raises(ConflictError, match="без текста"):
        await _campaign(service_context, text="Hello")
    with pytest.raises(ConflictError, match="всех получателей"):
        await _campaign(service_context, segment=SubscriberSegment.NON_PREMIUM)


async def test_selected_inactive_account_is_not_silently_removed_on_launch(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, ctx.accounts[1].id)
        account.status = AccountStatus.DISABLED
        await session.commit()
    with pytest.raises(ConflictError, match="состав аккаунтов"):
        await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)


async def test_deleted_selected_account_remains_in_saved_selection_and_blocks_launch(
    service_context,
):
    ctx = service_context
    campaign = await _campaign(ctx)
    async with ctx.db.session_factory() as session:
        await session.execute(
            delete(DirectCampaignAccount).where(
                DirectCampaignAccount.account_id == ctx.accounts[1].id
            )
        )
        await session.execute(delete(Account).where(Account.id == ctx.accounts[1].id))
        await session.commit()
    view = await ctx.service.get_campaign_view(campaign.id)
    assert view.account_ids == tuple(a.id for a in ctx.accounts[:2])
    assert "удалён" in view.account_labels[1]
    with pytest.raises(ConflictError, match="состав аккаунтов"):
        await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)


async def test_service_success_does_not_increment_messages_or_claim_notification(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    result = await ctx.service.process_next()
    assert result.status == DirectMessageTaskStatus.SUCCEEDED
    assert result.external_message_id is None
    async with ctx.db.session_factory() as session:
        recipient = await session.get(Subscriber, result.subscriber_id)
        assert recipient.message_count == 0
        assert recipient.last_contacted_at is not None
    dashboard = await ctx.service.dashboard()
    assert dashboard.succeeded_tasks == dashboard.simulated_succeeded_tasks == 0
    assert dashboard.service_simulated_tasks == 1
    view = await ctx.service.get_launch_view(launch.id)
    live_text = direct_launch_details_text(replace(view, simulated=False))
    assert "Настройка подтверждена" in live_text
    assert "не подтверждено: <b>1</b>" in live_text
    assert "Отправлено" not in live_text


async def test_blacklist_before_mutation_prevents_action_and_does_not_replace_recipient(
    service_context,
):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)

    async def perform(**kwargs):
        await ctx.service.add_blacklist_entries([kwargs["recipient"].telegram_user_id])
        await kwargs["before_action"]()
        raise AssertionError("blacklisted person must not receive an action")

    ctx.gateway.perform_direct_service_action = perform
    result = await ctx.service.process_next()
    assert result.status == DirectMessageTaskStatus.SKIPPED
    assert result.error_code == "recipient_blacklisted"
    async with ctx.db.session_factory() as session:
        assert await session.scalar(select(func.count(DirectMessageTask.id))) == 1


async def test_already_set_is_skipped_without_success_or_fake_message_id(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)

    async def perform(**kwargs):
        return DirectServiceActionResult(kwargs["recipient"].telegram_user_id, changed=False)

    ctx.gateway.perform_direct_service_action = perform
    result = await ctx.service.process_next()
    assert result.status == DirectMessageTaskStatus.SKIPPED
    assert result.error_code == "service_already_set"
    assert result.external_message_id is None
    view = await ctx.service.get_launch_view(launch.id)
    assert view.succeeded == view.service_notifications == view.replacement_tasks == 0


@pytest.mark.parametrize("server_wait", [20, 900])
async def test_floodwait_preserves_sender_and_at_least_500_seconds(service_context, server_wait):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    calls = []

    async def perform(**kwargs):
        calls.append(kwargs["account"].id)
        await kwargs["before_action"]()
        raise TemporaryGatewayError("FloodWait", code="flood_wait", retry_after=server_wait)

    ctx.gateway.perform_direct_service_action = perform
    before = utc_now()
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.QUEUED
    assert task.sender_account_id == ctx.accounts[0].id
    assert (ensure_utc(task.scheduled_for) - before).total_seconds() >= max(500, server_wait)
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, ctx.accounts[0].id)
        assert (ensure_utc(account.cooldown_until) - before).total_seconds() >= max(
            500, server_wait
        )
    await _make_due(ctx, launch.id)
    deferred = await ctx.service.process_next()
    assert deferred.sender_account_id == task.sender_account_id
    assert calls == [ctx.accounts[0].id]


@pytest.mark.parametrize("attempt", [0, 1])
async def test_expired_service_mutation_is_never_replayed(service_context, attempt):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    task = await ctx.service._claim_next()
    key = f"direct-message:{task.id}" + (f":retry:{attempt}" if attempt else "")
    await ctx.service._runner.record_external_account_send(
        task.sender_account_id, operation_key=key
    )
    async with ctx.db.session_factory() as session:
        stored = await session.get(DirectMessageTask, task.id)
        stored.send_started_at = utc_now() - timedelta(minutes=10)
        stored.attempt_count = attempt
        stored.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    ctx.gateway.perform_direct_service_action = AsyncMock()
    assert await ctx.service.process_next() is None
    ctx.gateway.perform_direct_service_action.assert_not_awaited()
    view = await ctx.service.get_launch_view(launch.id)
    assert view.uncertain == 1
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, task.sender_account_id)
        assert account.sent_today == 1


async def test_service_success_does_not_clear_message_privacy_route_block(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    async with ctx.db.session_factory() as session:
        task = await session.scalar(
            select(DirectMessageTask).where(DirectMessageTask.launch_id == launch.id)
        )
        route = SubscriberAccountPeer(
            subscriber_id=task.subscriber_id,
            account_id=task.sender_account_id,
            send_blocked=True,
            blocked_reason="recipient_requires_premium",
            last_error="Premium only",
        )
        session.add(route)
        await session.commit()
    await _make_due(ctx, launch.id)
    assert (await ctx.service.process_next()).status == DirectMessageTaskStatus.SUCCEEDED
    async with ctx.db.session_factory() as session:
        saved = await session.get(SubscriberAccountPeer, route.id)
        assert saved.send_blocked and saved.blocked_reason == "recipient_requires_premium"
        assert saved.last_error == "Premium only"


async def test_service_attempt_keeps_account_reservation_through_slow_rpc(service_context):
    ctx = service_context
    ctx.settings.worker_heartbeat_seconds = 1
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    competing_runner = CampaignRunner(ctx.db.session_factory, ctx.gateway, settings=ctx.settings)

    async def perform(**kwargs):
        await kwargs["before_action"]()
        account_id = kwargs["account"].id
        async with ctx.db.session_factory() as session:
            account = await session.get(Account, account_id)
            token = account.send_reservation_token
            assert token is not None, "attempt accounting must not release a pending RPC"
            # Even an expired pacing timestamp cannot bypass the active operation's lease.
            account.next_send_at = utc_now() - timedelta(seconds=1)
            await session.commit()
        competitor_token, resume_at = await competing_runner._claim_account_send_slot(account_id)
        assert competitor_token is None and resume_at > utc_now()
        await asyncio.sleep(1.1)
        async with ctx.db.session_factory() as session:
            account = await session.get(Account, account_id)
            assert account.send_reservation_token == token
            assert ensure_utc(account.send_reservation_until) > utc_now()
            account.next_send_at = utc_now() + timedelta(seconds=60)
            await session.commit()
        return DirectServiceActionResult(kwargs["recipient"].telegram_user_id, changed=True)

    ctx.gateway.perform_direct_service_action = perform
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.SUCCEEDED
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, task.sender_account_id)
        assert account.send_reservation_token is None
        assert account.sent_today == 1


async def test_username_mismatch_before_dispatch_is_a_known_skip(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    ctx.gateway.perform_direct_service_action = AsyncMock(
        side_effect=PermanentGatewayError(
            "username принадлежит другому человеку", code="recipient_identity_mismatch"
        )
    )
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.SKIPPED
    assert task.send_started_at is None
    view = await ctx.service.get_launch_view(launch.id)
    assert view.skipped == 1 and view.uncertain == view.failed == 0
    async with ctx.db.session_factory() as session:
        subscriber = await session.get(Subscriber, task.subscriber_id)
        assert subscriber.direct_quarantine_code == "recipient_identity_mismatch"
        assert subscriber.last_delivery_uncertain_at is None


async def test_confirmed_setting_with_unverified_notification_retains_success_and_flood_pause(
    service_context,
):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)

    async def perform(**kwargs):
        await kwargs["before_action"]()
        return DirectServiceActionResult(
            kwargs["recipient"].telegram_user_id,
            changed=True,
            verification_error=TemporaryGatewayError(
                "receipt flood", code="flood_wait", retry_after=20
            ),
        )

    ctx.gateway.perform_direct_service_action = perform
    before = utc_now()
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.SUCCEEDED
    assert task.result_code == "service_change_confirmed"
    assert task.error_code == "service_notification_unconfirmed"
    assert task.external_message_id is None
    view = await ctx.service.get_launch_view(launch.id)
    assert view.succeeded == 1 and view.service_notifications == view.uncertain == 0
    async with ctx.db.session_factory() as session:
        account = await session.get(Account, task.sender_account_id)
        assert (ensure_utc(account.cooldown_until) - before).total_seconds() >= 500
        assert account.flood_hits == 1
        assert account.send_reservation_token is None
    assert await ctx.service.process_next() is None


async def test_live_mode_service_receipt_is_separate_from_message_successes(service_context):
    ctx = service_context
    ctx.settings.dry_run = False
    async with ctx.db.session_factory() as session:
        for account in await session.scalars(select(Account)):
            account.authorized = True
            account.session_encrypted = "unit-test-session"
            account.api_id = 12345
            account.api_hash_encrypted = "unit-test-api-hash"
        await session.commit()
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)

    async def perform(**kwargs):
        await kwargs["before_action"]()
        return DirectServiceActionResult(
            kwargs["recipient"].telegram_user_id,
            changed=True,
            service_message_id=55,
        )

    ctx.gateway.perform_direct_service_action = perform
    task = await ctx.service.process_next()
    assert task.status == DirectMessageTaskStatus.SUCCEEDED and task.external_message_id == 55
    view = await ctx.service.get_launch_view(launch.id)
    assert view.simulated is False and view.service_notifications == view.succeeded == 1
    dashboard = await ctx.service.dashboard()
    assert dashboard.succeeded_tasks == dashboard.simulated_succeeded_tasks == 0
    assert dashboard.service_succeeded_tasks == 1 and dashboard.service_simulated_tasks == 0


@pytest.mark.parametrize("kind", ["message", "auto_delete", "protect_content"])
async def test_live_launch_history_renders_without_a_database_session(service_context, kind):
    ctx = service_context
    campaign = await ctx.service.create_campaign(
        name="Live history",
        subscriber_list_id=ctx.list_id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[ctx.accounts[0].id],
        operation_kind=kind,
        auto_delete_seconds=86400 if kind == "auto_delete" else None,
        text="Prepared text" if kind == "message" else "",
    )
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    async with ctx.db.session_factory() as session:
        saved = await session.get(DirectLaunch, launch.id)
        saved.simulated_snapshot = False
        await session.commit()
    queries = []

    def capture_query(connection, cursor, statement, parameters, context, executemany):
        queries.append(statement)

    event.listen(ctx.db.engine.sync_engine, "before_cursor_execute", capture_query)
    try:
        views, total = await ctx.service.list_launches(0, 7)
        assert "operation_kind_snapshot" not in inspect(views[0].launch).unloaded
        assert "primary_text_snapshot" in inspect(views[0].launch).unloaded
        text = direct_launches_text(views, total, 0, 7)
        menu = direct_launches_menu(views, total, 0, 7)
    finally:
        event.remove(ctx.db.engine.sync_engine, "before_cursor_execute", capture_query)
    assert len(queries) == 3
    assert "Запуски ЛС" in text
    assert "Настройка подтверждена" in text if kind != "message" else "Отправлено" in text
    assert f"dm:launchview:{launch.id}:0" in str(menu)


async def test_launch_history_callback_opens_with_live_records(service_context, monkeypatch):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    async with ctx.db.session_factory() as session:
        saved = await session.get(DirectLaunch, launch.id)
        saved.simulated_snapshot = False
        await session.commit()
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.open_direct_launches(SimpleNamespace(data="dm:launches:0"), ctx.service)
    edit.assert_awaited_once()
    assert "Запуски ЛС" in edit.call_args.args[1]
    assert f"dm:launchview:{launch.id}:0" in str(edit.call_args.args[2])


def _live_gateway(
    monkeypatch,
    *,
    old_ttl=0,
    new_ttl=86400,
    premium=True,
    sender_premium=None,
    protected=False,
    failure=None,
    readback_failure=None,
    service_message=True,
):
    gateway = TelethonGateway(None, Settings(admin_api_key="test", account_require_proxy=False))
    profile = types.User(id=1001, access_hash=22, username="target", premium=premium)
    me = types.User(id=101, premium=premium if sender_premium is None else sender_premium)
    message = types.MessageService(
        id=55,
        peer_id=types.PeerUser(1001),
        from_id=types.PeerUser(101),
        date=utc_now(),
        action=types.MessageActionSetMessagesTTL(86400),
        out=True,
    )
    state = {"writes": [], "full_reads": 0}

    async def invoke(request):
        if isinstance(request, functions.users.GetFullUserRequest):
            state["full_reads"] += 1
            if state["full_reads"] == 2 and readback_failure:
                raise readback_failure
            return SimpleNamespace(
                full_user=SimpleNamespace(
                    id=1001,
                    ttl_period=old_ttl if state["full_reads"] == 1 else new_ttl,
                    blocked=False,
                    noforwards_my_enabled=protected
                    or any(
                        isinstance(request, functions.messages.ToggleNoForwardsRequest)
                        for request in state["writes"]
                    ),
                    noforwards_peer_enabled=False,
                )
            )
        state["writes"].append(request)
        if failure:
            raise failure
        if isinstance(request, functions.messages.ToggleNoForwardsRequest):
            message.action = types.MessageActionNoForwardsToggle(False, True)
        return SimpleNamespace(
            updates=[types.UpdateNewMessage(message, 1, 1)] if service_message else []
        )

    client = SimpleNamespace(
        __call__=invoke,
        get_entity=AsyncMock(return_value=profile),
        get_me=AsyncMock(return_value=me),
        get_messages=AsyncMock(return_value=message),
    )

    class Client:
        get_entity = client.get_entity
        get_me = client.get_me
        get_messages = client.get_messages

        async def __call__(self, request):
            return await invoke(request)

    @asynccontextmanager
    async def account_operation(account_id):
        yield

    monkeypatch.setattr(gateway, "_account_operation", account_operation)
    monkeypatch.setattr(gateway, "_ensure_client", AsyncMock(return_value=Client()))
    monkeypatch.setattr(gateway, "_ensure_profile_not_blacklisted", AsyncMock())
    return gateway, client, state


async def _live_action(gateway, **kwargs):
    return await gateway.perform_direct_service_action(
        account=Account(id=1),
        recipient=DirectRecipient(1001, "target", 999),
        operation_kind=kwargs.pop("operation_kind", "auto_delete"),
        auto_delete_seconds=kwargs.pop("auto_delete_seconds", 86400),
        premium_only=kwargs.pop("premium_only", False),
        before_action=kwargs.pop("before_action", AsyncMock()),
        **kwargs,
    )


async def test_live_service_receipt_requires_readback_and_service_message(monkeypatch):
    gateway, client, state = _live_gateway(monkeypatch)
    result = await _live_action(gateway)
    assert result.changed and result.service_message_id == 55
    assert isinstance(state["writes"][0], functions.messages.SetHistoryTTLRequest)
    client.get_entity.assert_awaited_once_with("target")
    gateway, _, _ = _live_gateway(monkeypatch, service_message=False)
    assert (await _live_action(gateway)).service_message_id is None


async def test_live_unchanged_setting_does_not_mutate_or_claim_a_notification(monkeypatch):
    gateway, _, state = _live_gateway(monkeypatch, old_ttl=86400)
    boundary = AsyncMock()
    result = await _live_action(gateway, before_action=boundary)
    assert not result.changed
    assert not state["writes"]
    boundary.assert_not_awaited()


async def test_live_protection_uses_sender_premium_and_validates_service_action(monkeypatch):
    gateway, _, state = _live_gateway(monkeypatch, premium=False, sender_premium=True)
    result = await _live_action(gateway, operation_kind="protect_content", auto_delete_seconds=None)
    assert result.changed and result.service_message_id == 55
    assert isinstance(state["writes"][0], functions.messages.ToggleNoForwardsRequest)
    assert state["writes"][0].enabled is True
    gateway, _, state = _live_gateway(monkeypatch, protected=True)
    assert not (
        await _live_action(gateway, operation_kind="protect_content", auto_delete_seconds=None)
    ).changed
    assert not state["writes"]


async def test_service_action_cancelled_before_rpc_has_no_side_effect(monkeypatch):
    gateway, _, state = _live_gateway(monkeypatch)
    boundary = AsyncMock(side_effect=PermanentGatewayError("cancelled", code="launch_cancelled"))
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway, before_action=boundary)
    assert captured.value.code == "launch_cancelled"
    assert not state["writes"]


async def test_service_username_identity_is_checked_before_setting_change(monkeypatch):
    gateway, client, state = _live_gateway(monkeypatch)
    client.get_entity.return_value = types.User(id=9999, username="target")
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway)
    assert captured.value.code == "recipient_identity_mismatch"
    assert not state["writes"]


async def test_setting_change_does_not_claim_service_message_for_wrong_peer(monkeypatch):
    gateway, client, _ = _live_gateway(monkeypatch)
    client.get_messages.return_value = types.MessageService(
        id=55,
        peer_id=types.PeerUser(9999),
        from_id=types.PeerUser(101),
        date=utc_now(),
        action=types.MessageActionSetMessagesTTL(86400),
    )
    result = await _live_action(gateway)
    assert result.changed and result.service_message_id is None


@pytest.mark.parametrize(
    "failure,code",
    [
        (errors.FloodWaitError(None, 100), "flood_wait"),
        (ConnectionError("receipt unavailable"), "network_error"),
    ],
)
async def test_service_receipt_failure_does_not_lose_confirmed_setting(monkeypatch, failure, code):
    gateway, client, _ = _live_gateway(monkeypatch)
    client.get_messages.side_effect = failure
    result = await _live_action(gateway)
    assert result.changed and result.service_message_id is None
    assert result.verification_error.code == code
    client.get_messages.assert_awaited_once()


@pytest.mark.parametrize(
    "failure", [ConnectionError("lost response"), errors.ServerError(None, "INTERNAL", 500)]
)
async def test_live_lost_response_is_terminal_unknown(monkeypatch, failure):
    gateway, _, state = _live_gateway(monkeypatch, failure=failure)
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway)
    assert captured.value.code == "delivery_unconfirmed"
    assert len(state["writes"]) == 1


async def test_live_failed_readback_is_unknown_even_if_rpc_returned_updates(monkeypatch):
    gateway, _, _ = _live_gateway(monkeypatch, new_ttl=0)
    with pytest.raises(PermanentGatewayError, match="не подтвердилось"):
        await _live_action(gateway)
    gateway, _, _ = _live_gateway(monkeypatch, readback_failure=errors.FloodWaitError(None, 100))
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway)
    assert captured.value.code == "delivery_unconfirmed"
    assert captured.value.__cause__.code == "flood_wait"


async def test_live_filters_target_premium_and_requires_sender_premium_for_protection(monkeypatch):
    gateway, _, state = _live_gateway(monkeypatch, premium=False)
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway, premium_only=True)
    assert captured.value.code == "service_recipient_not_premium"
    with pytest.raises(PermanentGatewayError) as captured:
        await _live_action(gateway, operation_kind="protect_content", auto_delete_seconds=None)
    assert captured.value.code == "service_sender_requires_premium"
    assert not state["writes"]


async def test_wizard_creates_service_campaign_without_content_step(service_context, monkeypatch):
    ctx = service_context
    state = FSMContext(MemoryStorage(), StorageKey(bot_id=1, chat_id=2, user_id=2))
    await state.set_state(DirectCampaignCreateState.accounts)
    await state.update_data(
        direct_list_id=ctx.list_id,
        direct_segment="premium",
        direct_recipient_limit=1,
        direct_contact_cooldown_days=30,
        direct_account_ids=[ctx.accounts[0].id],
        direct_operation_kind="auto_delete",
        direct_auto_delete_seconds=604800,
    )
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_remember_origin", AsyncMock())
    await handlers.finish_direct_account_selection(SimpleNamespace(), state, ctx.service)
    assert await state.get_state() is None
    data = await state.get_data()
    campaign = (await ctx.service.get_campaign_view(data["direct_launch_campaign_id"])).campaign
    assert campaign.primary_text == "" and campaign.auto_delete_seconds == 604800
    assert "dm:preview:" not in str(edit.call_args)
    assert "Автозамены аккаунтов нет" in edit.call_args.args[1]


def test_service_ui_exposes_action_and_hides_message_preview_and_nonpremium_filter():
    assert "dm:service:new" in str(direct_messages_home_menu())
    assert "dm:preview:" not in str(
        direct_campaign_confirmation_menu(1, "token", service_action=True)
    )
    assert "non_premium" not in str(direct_segment_menu(1, service_action=True))


async def test_service_results_are_labelled_as_settings_in_recipient_history(service_context):
    ctx = service_context
    campaign = await _campaign(ctx)
    launch = await ctx.service.launch_campaign(campaign.id, consent_confirmed=True)
    await _make_due(ctx, launch.id)
    await ctx.service.process_next()
    async with ctx.db.session_factory() as session:
        stored = await session.get(DirectLaunch, launch.id)
        stored.simulated_snapshot = False
        await session.commit()
    report = await ctx.service.list_recipients(launch_id=launch.id)
    assert report.total == 1
    text = recipient_report_text(report)
    assert "настройка подтверждена" in text
    assert "ЛС отправлено" not in text
