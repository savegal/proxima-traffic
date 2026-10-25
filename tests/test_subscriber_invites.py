from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update

from proxima_traffic.account_state import apply_flood_pause
from proxima_traffic.bot.subscriber_invite_ui import invite_launch_text
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import (
    DryRunGateway,
    PermanentGatewayError,
    SubscriberInviteDelivery,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteAccountRoute,
    SubscriberInviteLaunchAccountState,
    SubscriberInviteTask,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError
from proxima_traffic.subscriber_invites import (
    SubscriberInviteService,
    normalize_invite_destination_ref,
)


def _settings(tmp_path: Path, name: str) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / name}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_typing_enabled=False,
        worker_retry_base_seconds=1,
        worker_retry_max_seconds=2,
    )


class UnresolvableRouteGateway(DryRunGateway):
    def __init__(self, blocked_account_id: int) -> None:
        super().__init__()
        self.blocked_account_id = blocked_account_id
        self.sender_ids: list[int] = []

    async def invite_subscriber(self, **kwargs):
        account = kwargs["account"]
        self.sender_ids.append(int(account.id))
        if int(account.id) == self.blocked_account_id:
            raise PermanentGatewayError(
                "recipient inaccessible", code="invite_recipient_unresolvable"
            )
        return await super().invite_subscriber(**kwargs)


class UncertainInviteGateway(DryRunGateway):
    async def invite_subscriber(self, **kwargs):
        del kwargs
        raise TemporaryGatewayError("network reply lost", code="network_error")


@pytest.mark.parametrize("healthy_backup", [False, True])
async def test_refused_invite_sender_is_not_used_for_remaining_people(tmp_path, healthy_backup):
    class RefusedSenderGateway(DryRunGateway):
        def __init__(self):
            super().__init__()
            self.sender_ids = []

        async def invite_subscriber(self, **kwargs):
            self.sender_ids.append(kwargs["account"].id)
            if kwargs["account"].id == primary_id:
                raise PermanentGatewayError("refused", code="account_restricted")
            return await super().invite_subscriber(**kwargs)

    gateway = RefusedSenderGateway()
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-refused-remaining.db", gateway, records=3
    )
    try:
        async with database.session_factory() as session:
            primary = Account(label="primary", ownership_confirmed=True)
            backup = Account(label="backup", ownership_confirmed=True)
            session.add_all([primary, backup])
            await session.commit()
            primary_id, backup_id = primary.id, backup.id
        campaign = await service.create_campaign(
            name="Refusal handling",
            subscriber_list_id=list_id,
            destination_ref="@owned_channel",
            segment=SubscriberSegment.ALL,
            recipient_limit=3,
            contact_cooldown_days=0,
            account_ids=[primary_id, backup_id] if healthy_backup else [primary_id],
        )
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        for _ in range(6):
            if await service.process_next() is None:
                break
        assert gateway.sender_ids == (
            [primary_id, backup_id, backup_id] if healthy_backup else [primary_id]
        )
        view = await service.get_launch_view(launch.id)
        assert view.queued == 0
        assert view.invited == (2 if healthy_backup else 0)
    finally:
        await database.dispose()


@pytest.mark.parametrize("readback_flood", [False, True])
async def test_invite_result_and_account_pause_are_saved_before_releasing_sender(
    tmp_path, monkeypatch, readback_flood
):
    class ReceiptGateway(DryRunGateway):
        async def invite_subscriber(self, **kwargs):
            if readback_flood:
                raise PermanentGatewayError(
                    "readback failed", code="invite_delivery_unconfirmed"
                ) from TemporaryGatewayError(
                    "verification flood", retry_after=900, code="flood_wait"
                )
            return await super().invite_subscriber(**kwargs)

    database, service, list_id = await _service_with_import(
        tmp_path, "invite-result-reservation.db", ReceiptGateway(), records=1
    )
    try:
        async with database.session_factory() as session:
            account = Account(label="admin", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            account_id = account.id
        campaign = await service.create_campaign(
            name="Result handling",
            subscriber_list_id=list_id,
            destination_ref="@owned_channel",
            segment=SubscriberSegment.ALL,
            recipient_limit=1,
            contact_cooldown_days=0,
            account_ids=[account_id],
        )
        await service.launch_campaign(campaign.id, consent_confirmed=True)
        method_name = "_finish_task" if readback_flood else "_record_success"
        original = getattr(service, method_name)
        checked = False

        async def check_reservation(*args, **kwargs):
            nonlocal checked
            async with database.session_factory() as session:
                stored = await session.get(Account, account_id)
                assert stored.send_reservation_token is not None
                assert stored.sent_today == 1
                if readback_flood:
                    assert stored.flood_hits == 1
                    assert ensure_utc(stored.cooldown_until) >= utc_now() + timedelta(seconds=899)
            checked = True
            return await original(*args, **kwargs)

        monkeypatch.setattr(service, method_name, check_reservation)
        task = await service.process_next()
        assert checked
        assert task.status == (
            DirectMessageTaskStatus.FAILED if readback_flood else DirectMessageTaskStatus.SUCCEEDED
        )
        if readback_flood:
            assert task.error_code == "invite_delivery_unconfirmed"
        assert await service.process_next() is None
    finally:
        await database.dispose()


async def test_paused_invite_sender_is_rechecked_after_waiting_for_account_slot(
    tmp_path, monkeypatch
):
    class CountingGateway(DryRunGateway):
        calls = 0

        async def invite_subscriber(self, **kwargs):
            self.calls += 1
            return await super().invite_subscriber(**kwargs)

    gateway = CountingGateway()
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-paused-boundary.db", gateway, records=1
    )
    try:
        async with database.session_factory() as session:
            account = Account(label="admin", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            account_id = account.id
        campaign = await service.create_campaign(
            name="Pause boundary",
            subscriber_list_id=list_id,
            destination_ref="@owned_channel",
            segment=SubscriberSegment.ALL,
            recipient_limit=1,
            contact_cooldown_days=0,
            account_ids=[account_id],
        )
        await service.launch_campaign(campaign.id, consent_confirmed=True)
        original = service._runner.run_account_send_operation

        async def pause_before_slot(sender_id, operation):
            async with database.session_factory() as session:
                stored = await session.get(Account, sender_id)
                stored.status = AccountStatus.PAUSED
                await session.commit()
            return await original(sender_id, operation)

        monkeypatch.setattr(service._runner, "run_account_send_operation", pause_before_slot)
        task = await service.process_next()
        assert gateway.calls == 0
        assert task.delivery_attempted_at is None
        assert task.status != DirectMessageTaskStatus.SUCCEEDED
    finally:
        await database.dispose()


class AlreadyParticipantGateway(DryRunGateway):
    async def invite_subscriber(self, **kwargs):
        recipient = kwargs["recipient"]
        return SubscriberInviteDelivery(
            telegram_user_id=recipient.telegram_user_id,
            username=recipient.username,
            is_premium=None,
            access_hash=recipient.access_hash,
            already_participant=True,
        )


class PreRpcInviteGateway(DryRunGateway):
    def __init__(self, code: str) -> None:
        super().__init__()
        self.code = code

    async def invite_subscriber(self, **kwargs):
        del kwargs
        raise TemporaryGatewayError("invite could not start", code=self.code, retry_after=1)


@pytest.mark.asyncio
async def test_restricted_invite_is_terminal_without_a_guessed_account_pause(tmp_path):
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-no-guessed-pause.db", PreRpcInviteGateway("account_restricted"),
        records=1,
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
    campaign = await service.create_campaign(
        name="Refused invite", subscriber_list_id=list_id, destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL, recipient_limit=1, contact_cooldown_days=0,
        account_ids=[account.id],
    )
    await service.launch_campaign(campaign.id, consent_confirmed=True)
    task = await service.process_next()
    assert task.status == DirectMessageTaskStatus.FAILED
    assert task.error_code == "account_restricted"
    assert await service.process_next() is None
    async with database.session_factory() as session:
        account = await session.get(Account, account.id)
        subscriber = await session.get(Subscriber, task.subscriber_id)
        assert account.cooldown_until is None
        assert account.sent_today == 0
        assert subscriber.reserved_invite_launch_id is None
        assert subscriber.last_invited_at is None
    await database.dispose()


class FatalAccountInviteGateway(DryRunGateway):
    def __init__(self, blocked_account_id: int) -> None:
        super().__init__()
        self.blocked_account_id = blocked_account_id
        self.sender_ids: list[int] = []

    async def invite_subscriber(self, **kwargs):
        account = kwargs["account"]
        self.sender_ids.append(int(account.id))
        if int(account.id) == self.blocked_account_id:
            raise PermanentGatewayError("session was revoked", code="account_dead")
        return await super().invite_subscriber(**kwargs)


class DestinationAccessGateway(DryRunGateway):
    def __init__(self, blocked_account_id: int, code: str = "invite_destination_rights") -> None:
        super().__init__()
        self.blocked_account_id = blocked_account_id
        self.code = code
        self.sender_ids: list[int] = []

    async def invite_subscriber(self, **kwargs):
        account = kwargs["account"]
        self.sender_ids.append(int(account.id))
        if int(account.id) == self.blocked_account_id:
            raise PermanentGatewayError("destination access was revoked", code=self.code)
        return await super().invite_subscriber(**kwargs)


class PreflightFailureGateway(DryRunGateway):
    async def preflight_subscriber_invite(self, **kwargs):
        del kwargs
        raise PermanentGatewayError(
            "destination access was revoked", code="invite_destination_rights"
        )


async def _service_with_import(
    tmp_path: Path,
    name: str,
    gateway: DryRunGateway,
    *,
    records: int = 2,
) -> tuple[Database, SubscriberInviteService, int]:
    settings = _settings(tmp_path, name)
    database = Database(settings.database_url)
    await database.create_schema()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    direct = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / f"{name}.jsonl"
    source.write_text(
        "\n".join(
            f'{{"telegram_id":"{700_000 + index}","username":"invite_{index}",'
            f'"is_premium":{str(index % 2 == 0).lower()}}}'
            for index in range(1, records + 1)
        )
        + "\n",
        encoding="utf-8",
    )
    imported = await direct.import_subscribers(
        source,
        list_name=f"List {name}",
        source_filename=source.name,
    )
    service = SubscriberInviteService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-invite",
    )
    return database, service, int(imported.subscriber_list.id)


@pytest.mark.asyncio
async def test_profile_flood_defers_existing_invite_queue_until_deadline(tmp_path, monkeypatch):
    database, service, list_id = await _service_with_import(
        tmp_path,
        "profile-flood-invites.db",
        DryRunGateway(),
        records=2,
    )
    try:
        async with database.session_factory() as session:
            account = Account(label="Invite sender", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            aid = account.id
        campaign = await service.create_campaign(
            name="Timed pause",
            subscriber_list_id=list_id,
            destination_ref="@owned_channel",
            segment=SubscriberSegment.ALL,
            recipient_limit=2,
            contact_cooldown_days=0,
            account_ids=[aid],
        )
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        rpc = AsyncMock(wraps=service._gateway.invite_subscriber)
        monkeypatch.setattr(service._gateway, "invite_subscriber", rpc)
        async with database.session_factory() as session:
            account = await session.get(Account, aid)
            apply_flood_pause(account, 900)
            deadline = ensure_utc(account.cooldown_until)
            await session.commit()
        task = await service.process_next()
        assert task.status == DirectMessageTaskStatus.QUEUED
        assert task.delivery_attempted_at is None
        assert task.attempt_count == 0
        assert ensure_utc(task.scheduled_for) >= deadline
        rpc.assert_not_awaited()
        view = await service.get_launch_view(launch.id)
        assert view.queued == 2
        assert view.failed == 0
        assert view.launch.status != DirectLaunchStatus.FAILED
        async with database.session_factory() as session:
            account = await session.get(Account, aid)
            account.cooldown_until = utc_now() - timedelta(seconds=1)
            stored = await session.get(SubscriberInviteTask, task.id)
            stored.scheduled_for = utc_now()
            await session.commit()
        resumed = await service.process_next()
        assert resumed.status == DirectMessageTaskStatus.SUCCEEDED
        rpc.assert_awaited_once()
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_admin_setup_grants_selected_accounts(tmp_path: Path) -> None:
    database, service, _ = await _service_with_import(
        tmp_path, "invite-admin-setup.db", DryRunGateway()
    )
    async with database.session_factory() as session:
        manager = Account(label="main admin", ownership_confirmed=True)
        worker = Account(label="worker", ownership_confirmed=True)
        session.add_all([manager, worker])
        await session.commit()
        await session.refresh(manager)
        await session.refresh(worker)
    progress: list[tuple[int, int, int, int]] = []

    async def report(checked: int, total: int, granted: int, failed: int) -> None:
        progress.append((checked, total, granted, failed))

    result = await service.prepare_invite_admins(
        manager_id=int(manager.id),
        account_ids=[int(worker.id)],
        destination_ref="@owned_channel",
        on_progress=report,
    )

    assert result.manager_label == "main admin"
    assert result.granted_labels == ("worker",)
    assert result.failed == ()
    assert progress == [(0, 1, 0, 0), (1, 1, 1, 0)]


@pytest.mark.asyncio
async def test_subscriber_invite_launch_records_success_and_prevents_repeat(tmp_path: Path) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-success.db", DryRunGateway()
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite owned channel",
        subscriber_list_id=list_id,
        destination_ref="https://t.me/owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=2,
        contact_cooldown_days=90,
        account_ids=[int(account.id)],
    )
    progress: list[tuple[int, int, int, int]] = []

    async def report(checked: int, total: int, ready: int, unavailable: int) -> None:
        progress.append((checked, total, ready, unavailable))

    launch = await service.launch_campaign(
        int(campaign.id), consent_confirmed=True, on_preflight_progress=report
    )
    assert progress[0] == (0, 1, 0, 0)
    assert progress[-1] == (1, 1, 1, 0)

    while await service.process_next() is not None:
        pass

    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.SUCCEEDED
    assert view.invited == 2
    assert view.simulated is True
    async with database.session_factory() as session:
        recipients = list(await session.scalars(select(Subscriber).order_by(Subscriber.id)))
        assert all(item.reserved_invite_launch_id is None for item in recipients)
        assert all(item.last_invite_attempt_at is not None for item in recipients)
        assert [item.invite_count for item in recipients] == [1, 1]

    with pytest.raises(Exception, match="нет доступных"):
        await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_uses_another_admin_after_identity_resolution_failure(
    tmp_path: Path,
) -> None:
    gateway = UnresolvableRouteGateway(blocked_account_id=1)
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-reroute.db", gateway, records=1
    )
    async with database.session_factory() as session:
        first = Account(label="first", ownership_confirmed=True)
        second = Account(label="second", ownership_confirmed=True)
        session.add_all([first, second])
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
    assert first.id == 1
    campaign = await service.create_campaign(
        name="Invite reroute",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(first.id), int(second.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    first_result = await service.process_next()
    assert first_result is not None
    assert first_result.status == DirectMessageTaskStatus.QUEUED
    async with database.session_factory() as session:
        await session.execute(
            update(SubscriberInviteTask)
            .where(SubscriberInviteTask.launch_id == launch.id)
            .values(scheduled_for=utc_now())
        )
        await session.commit()
    second_result = await service.process_next()
    assert second_result is not None
    assert second_result.status == DirectMessageTaskStatus.SUCCEEDED
    assert gateway.sender_ids == [int(first.id), int(second.id)]
    async with database.session_factory() as session:
        route = await session.scalar(
            select(SubscriberInviteAccountRoute).where(
                SubscriberInviteAccountRoute.account_id == first.id
            )
        )
        assert route is not None and route.invite_blocked is True
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_never_retries_an_uncertain_telegram_outcome(
    tmp_path: Path,
) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-uncertain.db", UncertainInviteGateway(), records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite uncertain",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "invite_delivery_unconfirmed"
    view = await service.get_launch_view(int(launch.id))
    assert view.invited == 0
    assert view.uncertain == 1
    async with database.session_factory() as session:
        sender = await session.get(Account, int(account.id))
        assert sender.sent_today == 1
    assert await service.process_next() is None
    with pytest.raises(ConflictError, match="нет доступных"):
        await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    await database.dispose()


@pytest.mark.asyncio
async def test_existing_participant_is_not_counted_as_new_invite_or_sender_send(
    tmp_path: Path,
) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-existing-member.db", AlreadyParticipantGateway(), records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Existing participant",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    result = await service.process_next()

    assert result.status == DirectMessageTaskStatus.SUCCEEDED
    assert result.already_participant is True
    view = await service.get_launch_view(int(launch.id))
    assert view.invited == 0
    assert view.already_participant == 1
    assert view.last_error_code is None
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
        sender = await session.get(Account, int(account.id))
        assert subscriber.invite_count == 0
        assert subscriber.last_invited_at is None
        assert sender.sent_today == 0
    await database.dispose()


@pytest.mark.asyncio
async def test_interrupted_invite_before_rpc_is_retried_without_unknown_result(
    tmp_path: Path,
) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-pre-rpc-interruption.db", DryRunGateway(), records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite pre-RPC interruption",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[int(account.id)],
    )
    await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    task = await service._claim_next()
    assert task is not None
    recovered = await service._handle_interrupted_task(task.id, task.lease_token, "step_timeout")
    assert recovered.status == DirectMessageTaskStatus.QUEUED
    assert recovered.error_code == "step_timeout"
    async with database.session_factory() as session:
        sender = await session.get(Account, int(account.id))
        assert sender.sent_today == 0
    await database.dispose()


@pytest.mark.asyncio
async def test_invite_launch_cannot_switch_from_simulation_to_live(tmp_path: Path) -> None:
    gateway = DryRunGateway()
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-mode-snapshot.db", gateway, records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite mode snapshot",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    service._settings.dry_run = False

    result = await service.process_next()

    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "launch_mode_changed"
    assert (await service.get_launch_view(int(launch.id))).simulated is True
    assert gateway._next_message_id == 10_000
    await database.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["proxy_unavailable", "account_operation_busy"])
async def test_pre_rpc_invite_failures_do_not_become_delivery_unconfirmed(
    tmp_path: Path,
    code: str,
) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path,
        f"invite-pre-rpc-{code}.db",
        PreRpcInviteGateway(code),
        records=1,
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name=f"Invite pre-RPC {code}",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    first = await service.process_next()
    assert first is not None
    assert first.status == DirectMessageTaskStatus.QUEUED
    assert first.error_code == code
    assert first.attempt_count == 1
    assert first.delivery_attempted_at is None
    retry_view = await service.get_launch_view(int(launch.id))
    assert retry_view.next_scheduled_for is not None
    assert (DirectMessageTaskStatus.QUEUED, code, 1) in retry_view.issues
    retry_text = invite_launch_text(retry_view)
    assert "Следующая проверка очереди" in retry_text
    assert "Основные причины" in retry_text
    async with database.session_factory() as session:
        task = await session.scalar(
            select(SubscriberInviteTask).where(SubscriberInviteTask.launch_id == launch.id)
        )
        assert task is not None
        task.scheduled_for = utc_now()
        task.max_attempts = 2
        await session.commit()

    second = await service.process_next()
    assert second is not None
    assert second.status == DirectMessageTaskStatus.SKIPPED
    assert second.error_code == code
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
    assert subscriber is not None
    assert subscriber.reserved_invite_launch_id is None
    assert subscriber.last_invited_target_ref is None
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_fails_over_after_fatal_account_error(tmp_path: Path) -> None:
    gateway = FatalAccountInviteGateway(blocked_account_id=1)
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-fatal-account-failover.db", gateway, records=1
    )
    async with database.session_factory() as session:
        first = Account(label="revoked", ownership_confirmed=True)
        second = Account(label="healthy", ownership_confirmed=True)
        session.add_all([first, second])
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
    assert first.id == 1
    campaign = await service.create_campaign(
        name="Invite fatal account failover",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(first.id), int(second.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    first_result = await service.process_next()
    assert first_result is not None
    assert first_result.status == DirectMessageTaskStatus.QUEUED
    assert first_result.error_code == "account_dead"
    assert first_result.attempt_count == 0
    assert first_result.delivery_attempted_at is None
    async with database.session_factory() as session:
        state = await session.scalar(
            select(SubscriberInviteLaunchAccountState).where(
                SubscriberInviteLaunchAccountState.launch_id == launch.id,
                SubscriberInviteLaunchAccountState.account_id == first.id,
            )
        )
        task = await session.scalar(
            select(SubscriberInviteTask).where(SubscriberInviteTask.launch_id == launch.id)
        )
        assert state is not None and state.send_blocked is True
        assert state.blocked_reason == "account_dead"
        assert task is not None
        task.scheduled_for = utc_now()
        await session.commit()

    second_result = await service.process_next()
    assert second_result is not None
    assert second_result.status == DirectMessageTaskStatus.SUCCEEDED
    assert gateway.sender_ids == [int(first.id), int(second.id)]
    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.SUCCEEDED
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_marks_expired_started_request_as_unknown(
    tmp_path: Path,
) -> None:
    gateway = DryRunGateway()
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-crash-recovery.db", gateway, records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite crash recovery",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    now = utc_now()
    async with database.session_factory() as session:
        task = await session.scalar(
            select(SubscriberInviteTask).where(SubscriberInviteTask.launch_id == launch.id)
        )
        assert task is not None
        task.status = DirectMessageTaskStatus.RUNNING
        task.lease_owner = "crashed-worker"
        task.lease_token = "stale-token"
        task.lease_until = now - timedelta(seconds=1)
        task.delivery_attempted_at = now
        task.sender_account_id = int(account.id)
        await session.commit()

    assert await service.process_next() is None
    assert gateway._next_message_id == 10_000
    async with database.session_factory() as session:
        task = await session.scalar(
            select(SubscriberInviteTask).where(SubscriberInviteTask.launch_id == launch.id)
        )
        subscriber = await session.scalar(select(Subscriber))
        assert task is not None
        assert task.status == DirectMessageTaskStatus.FAILED
        assert task.error_code == "invite_delivery_unconfirmed"
        assert subscriber is not None
        assert subscriber.reserved_invite_launch_id is None
        assert subscriber.last_invited_at is None
        assert subscriber.last_invite_attempt_at is not None
        sender = await session.get(Account, int(account.id))
        assert sender.sent_today == 1
    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.FAILED
    await database.dispose()


@pytest.mark.asyncio
async def test_cancelling_invite_launch_keeps_running_recipient_reserved(tmp_path: Path) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-cancel-reservation.db", DryRunGateway(), records=2
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite cancel reservation",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=2,
        contact_cooldown_days=30,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    running_task = await service._claim_next()

    assert running_task is not None
    assert await service.cancel_launch(int(launch.id)) == 1

    async with database.session_factory() as session:
        tasks = list(
            await session.scalars(
                select(SubscriberInviteTask)
                .where(SubscriberInviteTask.launch_id == launch.id)
                .order_by(SubscriberInviteTask.id)
            )
        )
        active = next(task for task in tasks if task.id == running_task.id)
        cancelled = next(task for task in tasks if task.id != running_task.id)
        active_subscriber = await session.get(Subscriber, active.subscriber_id)
        cancelled_subscriber = await session.get(Subscriber, cancelled.subscriber_id)

    assert active.status == DirectMessageTaskStatus.RUNNING
    assert active_subscriber is not None
    assert active_subscriber.reserved_invite_launch_id == launch.id
    assert cancelled.status == DirectMessageTaskStatus.CANCELLED
    assert cancelled_subscriber is not None
    assert cancelled_subscriber.reserved_invite_launch_id is None

    async with database.session_factory() as session:
        expired = await session.get(SubscriberInviteTask, running_task.id)
        assert expired is not None
        expired.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await service.process_next() is None
    async with database.session_factory() as session:
        recovered = await session.get(SubscriberInviteTask, running_task.id)
        recovered_subscriber = await session.get(Subscriber, running_task.subscriber_id)
    assert recovered is not None
    assert recovered.status == DirectMessageTaskStatus.CANCELLED
    assert recovered.error_code == "invite_launch_cancelled"
    assert recovered_subscriber is not None
    assert recovered_subscriber.reserved_invite_launch_id is None
    recovered_launch = await service.get_launch_view(int(launch.id))
    assert recovered_launch.launch.status == DirectLaunchStatus.CANCELLED
    await database.dispose()


@pytest.mark.asyncio
async def test_global_stop_list_cancels_queued_subscriber_invites(tmp_path: Path) -> None:
    gateway = DryRunGateway()
    settings = _settings(tmp_path, "invite-stop-list.db")
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-stop-list.db", gateway, records=1
    )
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    direct_messages = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct-stop-list",
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite global stop list",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    result = await direct_messages.add_blacklist_entries([700_001])
    await service.refresh_launches(result.affected_invite_launch_ids)

    assert result.cancelled_tasks == 1
    assert result.cancelled_invite_tasks == 1
    assert result.affected_invite_launch_ids == (int(launch.id),)
    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.FAILED
    assert view.skipped == 1
    async with database.session_factory() as session:
        task = await session.scalar(select(SubscriberInviteTask))
        subscriber = await session.scalar(select(Subscriber))
    assert task is not None
    assert task.status == DirectMessageTaskStatus.SKIPPED
    assert subscriber is not None
    assert subscriber.reserved_invite_launch_id is None
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_fails_over_when_one_admin_loses_destination_rights(
    tmp_path: Path,
) -> None:
    gateway = DestinationAccessGateway(blocked_account_id=1)
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-destination-failover.db", gateway, records=1
    )
    async with database.session_factory() as session:
        first = Account(label="first", ownership_confirmed=True)
        second = Account(label="second", ownership_confirmed=True)
        session.add_all([first, second])
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
    campaign = await service.create_campaign(
        name="Invite destination failover",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(first.id), int(second.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    first_result = await service.process_next()
    assert first_result is not None
    assert first_result.status == DirectMessageTaskStatus.QUEUED
    async with database.session_factory() as session:
        state = await session.scalar(
            select(SubscriberInviteLaunchAccountState).where(
                SubscriberInviteLaunchAccountState.launch_id == launch.id,
                SubscriberInviteLaunchAccountState.account_id == first.id,
            )
        )
        assert state is not None and state.send_blocked is True
        await session.execute(
            update(SubscriberInviteTask)
            .where(SubscriberInviteTask.launch_id == launch.id)
            .values(scheduled_for=utc_now())
        )
        await session.commit()
    second_result = await service.process_next()
    assert second_result is not None
    assert second_result.status == DirectMessageTaskStatus.SUCCEEDED
    assert gateway.sender_ids == [int(first.id), int(second.id)]
    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.SUCCEEDED
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_preserves_destination_failure_reason_when_all_admins_fail(
    tmp_path: Path,
) -> None:
    gateway = DestinationAccessGateway(blocked_account_id=1)
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-destination-failure.db", gateway, records=1
    )
    async with database.session_factory() as session:
        account = Account(label="admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite destination failure",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(account.id)],
    )
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    result = await service.process_next()
    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.FAILED
    assert view.last_error_code == "invite_destination_rights"
    assert view.last_error == "destination access was revoked"
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_preflight_reports_each_unavailable_admin(tmp_path: Path) -> None:
    database, service, list_id = await _service_with_import(
        tmp_path, "invite-preflight-diagnostics.db", PreflightFailureGateway(), records=1
    )
    async with database.session_factory() as session:
        account = Account(label="unavailable admin", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Invite preflight diagnostics",
        subscriber_list_id=list_id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[int(account.id)],
    )

    with pytest.raises(ConflictError) as exc_info:
        await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    assert "unavailable admin" in str(exc_info.value)
    assert "права" in str(exc_info.value)
    await database.dispose()


def test_subscriber_invite_destination_normalization() -> None:
    assert normalize_invite_destination_ref("@rare") == ("@rare", "@rare")
    assert normalize_invite_destination_ref("https://t.me/Rare/42") == ("@Rare", "@rare")
    assert normalize_invite_destination_ref("https://t.me/Owned_Channel") == (
        "@Owned_Channel",
        "@owned_channel",
    )
    assert normalize_invite_destination_ref("https://t.me/+A1_b2") == (
        "https://t.me/+A1_b2",
        "https://t.me/+A1_b2",
    )
    normalized_public = normalize_invite_destination_ref(
        "https://www.telegram.me/s/Owned_Channel/42?single"
    )
    assert normalized_public == (
        "@Owned_Channel",
        "@owned_channel",
    )
    assert normalize_invite_destination_ref("https://telegram.me/joinchat/A1_b2#details") == (
        "https://t.me/+A1_b2",
        "https://t.me/+A1_b2",
    )
    with pytest.raises(ValueError):
        normalize_invite_destination_ref("https://example.com/not-a-chat")
