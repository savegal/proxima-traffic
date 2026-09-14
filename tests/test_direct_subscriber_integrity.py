from pathlib import Path

import pytest

from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import DryRunGateway
from proxima_traffic.models import (
    Account,
    Subscriber,
    SubscriberInviteAccountRoute,
    SubscriberInviteCampaign,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError
from proxima_traffic.subscriber_import import SubscriberRecord


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
    )


@pytest.mark.asyncio
async def test_reimport_with_username_recovers_only_invite_unresolvable_route(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "invite-route-recovery.db")
    database = Database(settings.database_url)
    await database.create_schema()
    now = utc_now()

    async with database.session_factory() as session:
        subscriber_list = SubscriberList(
            name="Recovery",
            status=SubscriberListStatus.READY,
        )
        subscriber = Subscriber(
            telegram_user_id=910_001,
            username=None,
            is_premium=None,
        )
        unresolved_account = Account(label="unresolved-sender", ownership_confirmed=True)
        privacy_account = Account(label="privacy-sender", ownership_confirmed=True)
        session.add_all([subscriber_list, subscriber, unresolved_account, privacy_account])
        await session.flush()
        unresolved_route = SubscriberInviteAccountRoute(
            subscriber_id=int(subscriber.id),
            account_id=int(unresolved_account.id),
            access_hash=None,
            invite_blocked=True,
            blocked_reason="invite_recipient_unresolvable",
            last_error="old unresolved route",
            updated_at=now,
        )
        privacy_route = SubscriberInviteAccountRoute(
            subscriber_id=int(subscriber.id),
            account_id=int(privacy_account.id),
            access_hash=None,
            invite_blocked=True,
            blocked_reason="invite_recipient_privacy",
            last_error="privacy restriction",
            updated_at=now,
        )
        session.add_all([unresolved_route, privacy_route])
        await session.commit()
        list_id = int(subscriber_list.id)
        subscriber_id = int(subscriber.id)
        unresolved_route_id = int(unresolved_route.id)
        privacy_route_id = int(privacy_route.id)

    async with database.session_factory() as session:
        await DirectMessageService._import_batch(
            session,
            list_id=list_id,
            records=[
                SubscriberRecord(
                    telegram_user_id=910_001,
                    username="restored_user",
                    is_premium=None,
                )
            ],
            imported_at=utc_now(),
        )
        await session.commit()

    async with database.session_factory() as session:
        unresolved_route = await session.get(SubscriberInviteAccountRoute, unresolved_route_id)
        privacy_route = await session.get(SubscriberInviteAccountRoute, privacy_route_id)
        subscriber = await session.get(Subscriber, subscriber_id)
        assert unresolved_route is not None
        assert unresolved_route.invite_blocked is False
        assert unresolved_route.blocked_reason is None
        assert unresolved_route.last_error is None
        assert privacy_route is not None
        assert privacy_route.invite_blocked is True
        assert privacy_route.blocked_reason == "invite_recipient_privacy"
        assert privacy_route.last_error == "privacy restriction"
        assert subscriber is not None
        assert subscriber.username == "restored_user"

    await database.dispose()


@pytest.mark.asyncio
async def test_delete_subscriber_list_reports_invite_campaign_dependency(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "list-delete-invite-dependency.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )

    async with database.session_factory() as session:
        subscriber_list = SubscriberList(
            name="Invite source",
            status=SubscriberListStatus.READY,
        )
        session.add(subscriber_list)
        await session.flush()
        campaign = SubscriberInviteCampaign(
            name="Invite campaign",
            subscriber_list_id=int(subscriber_list.id),
            destination_ref="@owned_destination",
            destination_key="username:owned_destination",
            destination_title="Owned destination",
            segment=SubscriberSegment.ALL,
            recipient_limit=10,
            contact_cooldown_days=90,
            enabled=True,
        )
        session.add(campaign)
        await session.commit()
        list_id = int(subscriber_list.id)

    with pytest.raises(ConflictError, match="кампаниями приглашений"):
        await service.delete_subscriber_list(list_id)

    async with database.session_factory() as session:
        assert await session.get(SubscriberList, list_id) is not None

    await database.dispose()
