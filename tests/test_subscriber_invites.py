from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, update

from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import DryRunGateway, PermanentGatewayError, TemporaryGatewayError
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteAccountRoute,
    SubscriberInviteLaunchAccountState,
    SubscriberInviteTask,
    SubscriberSegment,
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


class PrivacyRouteGateway(DryRunGateway):
    def __init__(self, blocked_account_id: int) -> None:
        super().__init__()
        self.blocked_account_id = blocked_account_id
        self.sender_ids: list[int] = []

    async def invite_subscriber(self, **kwargs):
        account = kwargs["account"]
        self.sender_ids.append(int(account.id))
        if int(account.id) == self.blocked_account_id:
            raise PermanentGatewayError("recipient privacy", code="invite_recipient_privacy")
        return await super().invite_subscriber(**kwargs)


class UncertainInviteGateway(DryRunGateway):
    async def invite_subscriber(self, **kwargs):
        del kwargs
        raise TemporaryGatewayError("network reply lost", code="network_error")


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
    launch = await service.launch_campaign(int(campaign.id), consent_confirmed=True)

    while await service.process_next() is not None:
        pass

    view = await service.get_launch_view(int(launch.id))
    assert view.launch.status == DirectLaunchStatus.SUCCEEDED
    assert view.invited == 2
    async with database.session_factory() as session:
        recipients = list(await session.scalars(select(Subscriber).order_by(Subscriber.id)))
        assert all(item.reserved_invite_launch_id is None for item in recipients)
        assert all(item.last_invite_attempt_at is not None for item in recipients)
        assert [item.invite_count for item in recipients] == [1, 1]

    with pytest.raises(Exception, match="нет доступных"):
        await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    await database.dispose()


@pytest.mark.asyncio
async def test_subscriber_invite_uses_another_admin_after_privacy_route_failure(
    tmp_path: Path,
) -> None:
    gateway = PrivacyRouteGateway(blocked_account_id=1)
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
    await service.launch_campaign(int(campaign.id), consent_confirmed=True)
    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "invite_delivery_unconfirmed"
    assert await service.process_next() is None
    with pytest.raises(ConflictError, match="нет доступных"):
        await service.launch_campaign(int(campaign.id), consent_confirmed=True)
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
    assert normalize_invite_destination_ref("https://t.me/Owned_Channel") == (
        "@Owned_Channel",
        "@owned_channel",
    )
    assert normalize_invite_destination_ref("https://t.me/+A1_b2") == (
        "https://t.me/+A1_b2",
        "https://t.me/+a1_b2",
    )
    with pytest.raises(ValueError):
        normalize_invite_destination_ref("https://example.com/not-a-chat")
