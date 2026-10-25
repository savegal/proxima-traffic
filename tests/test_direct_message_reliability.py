import asyncio
import json
import shutil
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, func, select
from telethon import errors, types

from proxima_traffic.account_state import apply_flood_pause
from proxima_traffic.bot.direct_message_ui import direct_launch_details_text
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import DirectRecipient, DryRunGateway, PermanentGatewayError
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    Account,
    AccountStatus,
    DirectCampaign,
    DirectCampaignAccount,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError


@pytest.fixture
async def dm(tmp_path, schema_template):
    path = tmp_path / "reliability.db"
    shutil.copy2(schema_template, path)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{path}",
        admin_api_key="test",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_typing_enabled=False,
    )
    database = Database(settings.database_url)
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory, gateway, runner, settings, worker_id="qa"
    )
    source = tmp_path / "people.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(
                {
                    "telegram_id": 980001 + i,
                    "username": f"qa_member_{i}",
                    "is_premium": False,
                }
            )
            for i in range(4)
        ),
        encoding="utf-8",
    )
    imported = await service.import_subscribers(source, list_name="QA", source_filename=source.name)
    async with database.session_factory() as session:
        account = Account(label="QA sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
    campaign = await service.create_campaign(
        name="QA",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[account.id],
        text="Test",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    try:
        yield SimpleNamespace(
            db=database,
            service=service,
            gateway=gateway,
            settings=settings,
            campaign=campaign,
            account=account,
        )
    finally:
        await database.dispose()


@pytest.mark.parametrize("operation_kind", ["message", "auto_delete"])
async def test_profile_flood_keeps_existing_direct_queue_until_cooldown(
    dm, monkeypatch, operation_kind
):
    async with dm.db.session_factory() as session:
        campaign = await session.get(DirectCampaign, dm.campaign.id)
        campaign.recipient_limit = 2
        campaign.operation_kind = operation_kind
        campaign.auto_delete_seconds = 86400 if operation_kind == "auto_delete" else None
        if operation_kind == "auto_delete":
            campaign.service_account_ids_json = json.dumps([dm.account.id])
        await session.commit()
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    rpc_name = (
        "send_direct_content" if operation_kind == "message" else "perform_direct_service_action"
    )
    rpc = AsyncMock(wraps=getattr(dm.gateway, rpc_name))
    monkeypatch.setattr(dm.gateway, rpc_name, rpc)
    async with dm.db.session_factory() as session:
        if operation_kind == "auto_delete":
            # Make its initial random delay due without waiting in real time.
            first = await session.scalar(
                select(DirectMessageTask)
                .where(DirectMessageTask.launch_id == launch.id)
                .order_by(DirectMessageTask.id)
                .limit(1)
            )
            first.scheduled_for = utc_now()
        account = await session.get(Account, dm.account.id)
        apply_flood_pause(account, 900)
        deadline = ensure_utc(account.cooldown_until)
        await session.commit()
    task = await dm.service.process_next()
    assert task.status == DirectMessageTaskStatus.QUEUED
    assert task.error_code == "flood_wait"
    assert task.send_started_at is None
    assert task.attempt_count == 0
    assert ensure_utc(task.scheduled_for) >= deadline
    rpc.assert_not_awaited()
    view = await dm.service.get_launch_view(launch.id)
    assert view.queued == 2
    assert view.failed == 0
    assert view.launch.status != DirectLaunchStatus.FAILED
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        account.cooldown_until = utc_now() - timedelta(seconds=1)
        stored = await session.get(DirectMessageTask, task.id)
        stored.scheduled_for = utc_now()
        await session.commit()
    resumed = await dm.service.process_next()
    assert resumed.status == DirectMessageTaskStatus.SUCCEEDED
    rpc.assert_awaited_once()


async def test_simultaneous_confirmation_creates_one_launch_and_one_recipient(dm):
    launches = await asyncio.gather(
        *(
            dm.service.launch_campaign(
                dm.campaign.id, consent_confirmed=True, request_key="same-confirmation"
            )
            for _ in range(3)
        )
    )
    assert len({launch.id for launch in launches}) == 1
    async with dm.db.session_factory() as session:
        assert await session.scalar(select(func.count(DirectLaunch.id))) == 1
        assert await session.scalar(select(func.count(DirectMessageTask.id))) == 1
    with pytest.raises(ConflictError, match="активный запуск"):
        await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)


async def test_sender_refusal_stops_remaining_recipients_in_the_same_launch(dm, monkeypatch):
    async with dm.db.session_factory() as session:
        campaign = await session.get(DirectCampaign, dm.campaign.id)
        campaign.recipient_limit = 3
        await session.commit()
    rejected = AsyncMock(
        side_effect=PermanentGatewayError("Telegram refused this sender", code="account_restricted")
    )
    monkeypatch.setattr(dm.gateway, "send_direct_content", rejected)
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    for _ in range(4):
        await dm.service.process_next()
    rejected.assert_awaited_once()
    view = await dm.service.get_launch_view(launch.id)
    assert view.sender_states[0].reason == "account_restricted"
    assert view.running == view.queued == view.uncertain == 0
    async with dm.db.session_factory() as session:
        tasks = list(
            await session.scalars(
                select(DirectMessageTask).where(DirectMessageTask.launch_id == launch.id)
            )
        )
        assert sum(t.send_started_at is not None for t in tasks) == 1
        assert len(tasks) == 3


async def test_healthy_selected_sender_continues_after_another_sender_is_refused(dm, monkeypatch):
    async with dm.db.session_factory() as session:
        backup = Account(label="Selected healthy sender", ownership_confirmed=True)
        session.add(backup)
        await session.flush()
        session.add(DirectCampaignAccount(campaign_id=dm.campaign.id, account_id=backup.id))
        campaign = await session.get(DirectCampaign, dm.campaign.id)
        campaign.recipient_limit = 3
        await session.commit()
    original = dm.gateway.send_direct_content
    used_senders = []

    async def deliver(**kwargs):
        used_senders.append(kwargs["account"].id)
        if kwargs["account"].id == dm.account.id:
            raise PermanentGatewayError("sender refused", code="account_restricted")
        return await original(**kwargs)

    monkeypatch.setattr(dm.gateway, "send_direct_content", deliver)
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    for _ in range(4):
        await dm.service.process_next()
    assert used_senders == [dm.account.id, backup.id, backup.id]
    view = await dm.service.get_launch_view(launch.id)
    assert view.succeeded == 2
    assert view.failed == 1
    assert view.replacement_tasks == 0


async def test_sender_paused_while_waiting_for_slot_is_not_used(dm, monkeypatch):
    original = dm.service._runner.run_account_send_operation
    deliver = AsyncMock(wraps=dm.gateway.send_direct_content)
    monkeypatch.setattr(dm.gateway, "send_direct_content", deliver)

    async def pause_before_slot(account_id, operation):
        async with dm.db.session_factory() as session:
            account = await session.get(Account, account_id)
            account.status = AccountStatus.PAUSED
            await session.commit()
        return await original(account_id, operation)

    monkeypatch.setattr(dm.service._runner, "run_account_send_operation", pause_before_slot)
    await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service.process_next()
    deliver.assert_not_awaited()
    assert task.error_code == "sender_unavailable"
    assert task.send_started_at is None


async def test_launch_details_queries_do_not_grow_with_sender_count(dm):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    queries = []

    def collect(_conn, _cursor, statement, _params, _context, _executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            queries.append(statement)

    event.listen(dm.db.engine.sync_engine, "before_cursor_execute", collect)
    try:
        await dm.service.get_launch_view(launch.id)
        small_count = len(queries)
        async with dm.db.session_factory() as session:
            accounts = [
                Account(label=f"Extra sender {i}", ownership_confirmed=True) for i in range(20)
            ]
            session.add_all(accounts)
            await session.flush()
            stored = await session.get(DirectLaunch, launch.id)
            stored.account_ids_snapshot_json = json.dumps(
                [dm.account.id] + [a.id for a in accounts]
            )
            await session.commit()
        queries.clear()
        view = await dm.service.get_launch_view(launch.id)
        assert len(view.sender_states) == 21
        assert len(queries) == small_count
    finally:
        event.remove(dm.db.engine.sync_engine, "before_cursor_execute", collect)


async def test_expired_dispatch_is_never_replayed_and_keeps_uncertainty_cooldown(dm):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service._claim_next()
    async with dm.db.session_factory() as session:
        stored = await session.get(DirectMessageTask, task.id)
        stored.send_started_at = utc_now() - timedelta(minutes=5)
        stored.sender_account_id = dm.account.id
        stored.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    assert await dm.service.process_next() is None
    async with dm.db.session_factory() as session:
        stored = await session.get(DirectMessageTask, task.id)
        subscriber = await session.get(Subscriber, task.subscriber_id)
        assert stored.error_code == "delivery_outcome_unknown"
        assert stored.status == DirectMessageTaskStatus.FAILED
        assert subscriber.reserved_direct_launch_id is None
        assert subscriber.last_delivery_uncertain_at is not None
        sender = await session.get(Account, dm.account.id)
        assert sender.sent_today == 1
    assert (await dm.service.get_launch_view(launch.id)).launch.status == DirectLaunchStatus.FAILED


async def test_expired_preparation_can_resume_but_stale_worker_cannot_finish(dm):
    await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    old = await dm.service._claim_next()
    async with dm.db.session_factory() as session:
        stored = await session.get(DirectMessageTask, old.id)
        stored.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    new = await dm.service._claim_next()
    assert new.id == old.id
    assert new.lease_token != old.lease_token
    with pytest.raises(ConflictError):
        await dm.service._finish_task(
            old.id,
            old.lease_token,
            status=DirectMessageTaskStatus.FAILED,
            error_code="old_worker",
            error="must not overwrite",
        )
    result = await dm.service._process_claimed(new.id, new.lease_token)
    assert result.status == DirectMessageTaskStatus.SUCCEEDED


async def test_cancel_during_definite_failure_does_not_strand_queued_task(dm):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service._claim_next()
    await dm.service.cancel_launch(launch.id)
    result = await dm.service._reschedule_task(
        task.id,
        task.lease_token,
        scheduled_for=utc_now(),
        error_code="network_error",
        error="definite failure before send",
        increment_attempt=True,
    )
    assert result.status == DirectMessageTaskStatus.CANCELLED
    assert (
        await dm.service.get_launch_view(launch.id)
    ).launch.status == DirectLaunchStatus.CANCELLED
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, task.subscriber_id)
        assert subscriber.reserved_direct_launch_id is None


async def test_timeout_before_dispatch_retries_without_recipient_uncertainty(dm, monkeypatch):
    await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    monkeypatch.setattr(
        dm.service, "_refresh_live_proxy_routes_for_accounts", AsyncMock(side_effect=TimeoutError)
    )
    result = await dm.service.process_next()
    assert result.status == DirectMessageTaskStatus.QUEUED
    assert result.attempt_count == 1
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, result.subscriber_id)
        assert subscriber.last_delivery_uncertain_at is None


async def test_unexpected_failure_after_dispatch_blocks_retry_and_replacement(dm, monkeypatch):
    await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    monkeypatch.setattr(
        dm.gateway, "send_direct_content", AsyncMock(side_effect=RuntimeError("oops"))
    )
    result = await dm.service.process_next()
    assert result.status == DirectMessageTaskStatus.FAILED
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, result.subscriber_id)
        assert subscriber.last_delivery_uncertain_at is not None
        assert await session.scalar(select(func.count(DirectMessageTask.id))) == 1


async def test_unconfirmed_direct_send_counts_as_unknown_and_reserves_sender_quota(
    dm, monkeypatch
):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    monkeypatch.setattr(
        dm.gateway,
        "send_direct_content",
        AsyncMock(side_effect=PermanentGatewayError("reply lost", code="delivery_unconfirmed")),
    )

    result = await dm.service.process_next()
    view = await dm.service.get_launch_view(launch.id)
    assert result.status == DirectMessageTaskStatus.FAILED
    assert view.succeeded == 0
    assert view.uncertain == 1
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        assert account.sent_today == 1


async def test_direct_launch_cannot_switch_from_simulation_to_live(dm, monkeypatch):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    send = AsyncMock()
    monkeypatch.setattr(dm.gateway, "send_direct_content", send)
    dm.settings.dry_run = False

    result = await dm.service.process_next()

    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "launch_mode_changed"
    assert (await dm.service.get_launch_view(launch.id)).simulated is True
    send.assert_not_awaited()


async def test_restriction_is_preserved_when_task_waits_for_account(dm, monkeypatch):
    dm.settings.account_restriction_cooldown_hours = 24
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    mapped = TelethonGateway(dm.db.session_factory, dm.settings)._map_rpc_error(
        errors.PeerFloodError(None), context="direct_message"
    )
    monkeypatch.setattr(dm.gateway, "send_direct_content", AsyncMock(side_effect=mapped))
    first = await dm.service.process_next()
    assert first.error_code == "account_restricted"
    async with dm.db.session_factory() as session:
        task = await session.get(DirectMessageTask, first.id)
        task.scheduled_for = utc_now()
        account = await session.get(Account, dm.account.id)
        cooldown_until = account.cooldown_until
        account.spambot_status = "free"
        account.spambot_checked_at = utc_now()
        await session.commit()
    second = await dm.service.process_next()
    assert second.error_code == "account_restricted"
    assert second.attempt_count == first.attempt_count
    view = await dm.service.get_launch_view(launch.id)
    assert view.sender_states[0].reason == "account_restricted"
    assert view.sender_states[0].cooldown_reason == "account_restricted"
    assert view.sender_states[0].spambot_status == "free"
    assert view.issues == ((DirectMessageTaskStatus.QUEUED, "account_restricted", 1),)
    assert "PeerFloodError" in str(view.recent_failures)
    text = direct_launch_details_text(view)
    assert "PeerFloodError" in text
    assert "штатная пауза" not in text
    assert "Все выбранные отправители на паузе" in text
    assert "(ожидает)" in text
    assert "следующая проверка, а не подтверждённый срок" in text
    assert "не снимает защитную паузу" in text
    assert "остальные доступные отправители продолжают" not in text
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        assert account.cooldown_until == cooldown_until
    # A second available sender must not be described as blocked by Telegram.
    mixed = replace(
        view,
        sender_states=view.sender_states + (
            replace(
                view.sender_states[0], account_id=999, label="Ready sender",
                reason=None, resume_at=None, cooldown_reason=None,
                spambot_status=None, spambot_checked_at=None,
            ),
        ),
    )
    assert "Все выбранные отправители на паузе" not in direct_launch_details_text(mixed)
    assert "доступные отправители могут продолжать" in direct_launch_details_text(mixed)


async def test_refusal_without_expiry_finishes_task_without_a_guessed_wait(dm, monkeypatch):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    mapped = TelethonGateway(dm.db.session_factory, dm.settings)._map_rpc_error(
        errors.PeerFloodError(None), context="direct_message"
    )
    send = AsyncMock(side_effect=mapped)
    monkeypatch.setattr(dm.gateway, "send_direct_content", send)
    result = await dm.service.process_next()
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "account_restricted"
    assert await dm.service.process_next() is None
    send.assert_awaited_once()
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        subscriber = await session.get(Subscriber, result.subscriber_id)
        assert account.cooldown_until is None
        assert account.cooldown_reason is None
        assert account.sent_today == 0
        assert subscriber.reserved_direct_launch_id is None
        assert subscriber.direct_quarantine_code is None
        assert subscriber.last_delivery_uncertain_at is None
    view = await dm.service.get_launch_view(launch.id)
    assert view.active == 0
    assert view.uncertain == 0


async def test_unknown_refusal_does_not_clear_an_existing_telegram_wait(dm):
    resume_at = utc_now() + timedelta(hours=2)
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        account.cooldown_until = resume_at
        account.cooldown_reason = "flood_wait"
        await session.commit()
    await dm.service._runner.handle_external_account_failure(
        dm.account.id, PermanentGatewayError("PeerFloodError", code="account_restricted")
    )
    async with dm.db.session_factory() as session:
        account = await session.get(Account, dm.account.id)
        assert account.cooldown_until.replace(tzinfo=resume_at.tzinfo) == resume_at
        assert account.cooldown_reason == "flood_wait"


async def test_launch_wide_failure_remains_failed_after_refresh(dm):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service._claim_next()
    await dm.service._fail_launch(task.id, task.lease_token, "content_invalid", "invalid text")
    await dm.service._refresh_launch(launch.id)
    assert (await dm.service.get_launch_view(launch.id)).launch.status == DirectLaunchStatus.FAILED


@pytest.mark.parametrize(
    "rpc,code",
    [
        (errors.PeerFloodError(None), "account_restricted"),
        (errors.FrozenMethodInvalidError(None), "account_restricted"),
        (errors.UserDeactivatedBanError(None), "account_dead"),
        (errors.AuthKeyUnregisteredError(None), "account_dead"),
        (errors.VoiceMessagesForbiddenError(None), "recipient_content_forbidden"),
        (errors.ChatSendGifsForbiddenError(None), "recipient_content_forbidden"),
        (errors.UserPrivacyRestrictedError(None), "recipient_privacy_restricted"),
        (errors.UserIsBlockedError(None), "recipient_route_blocked"),
        (errors.PremiumAccountRequiredError(None), "recipient_requires_premium"),
        (errors.UserIdInvalidError(None), "recipient_unresolvable"),
        (errors.MessageTooLongError(None), "content_invalid"),
        (errors.EntityBoundsInvalidError(None), "content_invalid"),
        (errors.ForbiddenError(None, "SOME_NEW_TELEGRAM_ERROR", 403), "telegram_forbidden"),
        (errors.RPCError(None, "FROZEN_METHOD_INVALID", 403), "account_restricted"),
        (errors.RPCError(None, "USER_PRIVACY_RESTRICTED", 403), "recipient_privacy_restricted"),
        (
            errors.RPCError(None, "ALLOW_PAYMENT_REQUIRED_100", 403),
            "recipient_paid_message_required",
        ),
        (errors.RPCError(None, "FLOOD_WAIT_90000", 420), "flood_wait"),
    ],
)
def test_direct_rpc_classification_keeps_exact_protocol_reason(rpc, code):
    settings = Settings(admin_api_key="qa", dry_run=True)
    gateway = TelethonGateway(None, settings)
    mapped = gateway._map_rpc_error(rpc, context="direct_message")
    assert mapped.code == code
    assert type(rpc).__name__ in mapped.safe_message
    if code == "flood_wait":
        assert mapped.retry_after >= 90000


async def test_lost_network_response_after_send_is_not_retryable(dm, monkeypatch):
    gateway = TelethonGateway(dm.db.session_factory, dm.settings)
    monkeypatch.setattr(gateway, "_delivery_receipt", AsyncMock(return_value=(utc_now(), None)))
    client = AsyncMock(side_effect=ConnectionError("connection reset"))
    with pytest.raises(PermanentGatewayError) as captured:
        await gateway._send(
            client,
            types.InputPeerUser(123, 456),
            "Hi",
            None,
            "stable-key",
            error_context="direct_message",
        )
    assert captured.value.code == "delivery_unconfirmed"
    assert client.await_count == 1


async def test_duplicate_random_id_does_not_guess_direct_delivery_from_matching_text(
    dm, monkeypatch
):
    gateway = TelethonGateway(dm.db.session_factory, dm.settings)
    monkeypatch.setattr(gateway, "_delivery_receipt", AsyncMock(return_value=(utc_now(), None)))
    client = AsyncMock(side_effect=errors.RandomIdDuplicateError(None))

    with pytest.raises(PermanentGatewayError) as captured:
        await gateway._send(
            client,
            types.InputPeerUser(123, 456),
            "Hi",
            None,
            "stable-duplicate-key",
            error_context="direct_message",
        )

    assert captured.value.code == "delivery_unconfirmed"
    client.iter_messages.assert_not_called()
    assert client.await_count == 1


async def test_stale_hash_uses_current_username_but_never_a_different_user():
    client = SimpleNamespace(
        get_entity=AsyncMock(
            side_effect=[
                errors.PeerIdInvalidError(None),
                types.User(id=1001, access_hash=22),
            ]
        )
    )
    entity, profile = await TelethonGateway._resolve_direct_recipient(
        client, DirectRecipient(1001, "current_username", 11)
    )
    assert profile.id == 1001
    assert entity.access_hash == 22
    client.get_entity.side_effect = [
        errors.PeerIdInvalidError(None),
        types.User(id=9999, access_hash=33),
    ]
    with pytest.raises(PermanentGatewayError) as captured:
        await TelethonGateway._resolve_direct_recipient(
            client, DirectRecipient(1001, "reassigned_username", 11)
        )
    assert captured.value.code == "recipient_identity_mismatch"
