import asyncio
import json

import pytest
from fastapi import FastAPI

from proxima_traffic.accounts import AccountService, _ConvertedSession
from proxima_traffic.bot.repository import (
    PREMIUM_ACCOUNT_GROUP_KEY,
    AccountListFilter,
    AdminRepository,
)
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    Campaign,
    CampaignMode,
    CampaignRun,
    ChannelCommentRule,
    ChannelCommentRun,
    DirectCampaign,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    ParticipantReactionRule,
    ParticipantReactionTask,
    RunStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
    TargetList,
)
from proxima_traffic.services import ConflictError


async def _account(
    repository: AdminRepository,
    label: str,
    telegram_user_id: int,
    *,
    premium: bool | None = None,
    ready: bool = False,
    status: AccountStatus = AccountStatus.ACTIVE,
) -> Account:
    account = await repository.create_account(
        label=label,
        telegram_user_id=telegram_user_id,
        ownership_confirmed=True,
    )
    async with repository._session_factory() as session:  # type: ignore[attr-defined]
        stored = await session.get(Account, account.id)
        assert stored is not None
        stored.status = status
        stored.is_premium = premium
        if ready:
            stored.session_source = AccountSessionSource.SESSION_STRING
            stored.session_encrypted = "encrypted"
            stored.api_id = 1
            stored.api_hash_encrypted = "encrypted"
            stored.authorized = True
        await session.commit()
        await session.refresh(stored)
        return stored


async def test_account_groups_are_many_to_many_and_filtering_is_database_paged(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    premium = await _account(repository, "premium", 7001, premium=True, ready=True)
    ordinary = await _account(repository, "ordinary", 7002, premium=False)
    await _account(repository, "unknown", 7003, premium=None)
    await _account(
        repository,
        "paused",
        7004,
        premium=False,
        status=AccountStatus.PAUSED,
    )
    group = await repository.create_account_group("Reactors", "Only approved reactors")
    second_group = await repository.create_account_group("Premium pool")
    members = await repository.set_account_group_members(group.id, {premium.id, ordinary.id})
    await repository.set_account_group_members(second_group.id, {premium.id})

    assert members == {premium.id, ordinary.id}
    assert await repository.list_account_group_member_ids(group.id) == {premium.id, ordinary.id}
    assert await repository.list_account_membership_group_ids(premium.id) == {
        group.id,
        second_group.id,
    }

    views = await repository.list_account_group_views()
    reactor_view = next(item for item in views if item.id == group.id)
    assert (
        reactor_view.account_count,
        reactor_view.ready_count,
        reactor_view.premium_count,
    ) == (2, 1, 1)

    only_premium = await repository.list_accounts(
        0,
        1,
        account_filter=AccountListFilter.PREMIUM,
    )
    assert only_premium.total == 1
    assert [item.id for item in only_premium.items] == [premium.id]

    only_group = await repository.list_accounts(
        0,
        1,
        account_filter=AccountListFilter.ALL,
        group_id=group.id,
    )
    assert only_group.total == 2
    assert [item.id for item in only_group.items] == [ordinary.id]
    assert only_group.page == 0 and only_group.pages == 2

    outside_groups = await repository.list_accounts(
        0,
        20,
        account_filter=AccountListFilter.ALL,
        group_id=0,
    )
    assert {item.label for item in outside_groups.items} == {"unknown", "paused"}
    assert (await repository.list_accounts(0, 20, account_filter="ready")).total == 1
    assert (await repository.list_accounts(0, 20, account_filter="needs_attention")).total == 3
    assert (await repository.list_accounts(0, 20, account_filter="no_session")).total == 3
    assert (await repository.list_accounts(0, 20, account_filter="profile_incomplete")).total == 1

    await repository.delete_account_group(group.id)
    assert await repository.list_account_membership_group_ids(ordinary.id) == set()
    assert await repository.list_account_membership_group_ids(premium.id) == {second_group.id}


async def test_safe_account_removal_preserves_history_but_rejects_live_work(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    service = AccountService(app.state.database.session_factory, app.state.settings)
    account = await _account(repository, "remove-safe", 7101, ready=True)
    target = await repository.create_target(chat_ref="@account_remove", title="Account removal")
    campaign = await repository.create_campaign(
        name="Account removal history",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Post",
    )

    async with app.state.database.session_factory() as session:
        session.add(
            CampaignRun(
                campaign_id=campaign.id,
                target_id=target.id,
                sender_account_id=account.id,
                request_key="account-removal-history",
                status=RunStatus.SUCCEEDED,
            )
        )
        await session.commit()

    result = await service.remove_account_safely(account.id)
    assert result.deleted is False and result.preserved_for_history is True
    stored = await service.get_account(account.id)
    assert stored.status is AccountStatus.DISABLED
    assert stored.session_encrypted is None and stored.authorized is False

    live = await _account(repository, "remove-live", 7102, ready=True)
    async with app.state.database.session_factory() as session:
        session.add(
            CampaignRun(
                campaign_id=campaign.id,
                target_id=target.id,
                sender_account_id=live.id,
                request_key="account-removal-live",
                status=RunStatus.QUEUED,
            )
        )
        await session.commit()

    plan = await service.account_removal_plan(live.id)
    assert plan.can_remove is False and plan.active_work == ("рассылки: 1",)
    with pytest.raises(ConflictError, match="активных задачах"):
        await service.remove_account_safely(live.id)
    with pytest.raises(ConflictError, match="активных задачах"):
        await service.clear_session(live.id)
    assert (await service.get_account(live.id)).authorized is True


async def test_account_removal_plan_covers_sender_snapshots_and_secondary_rules(
    app: FastAPI,
) -> None:
    """Queued work must keep every configured sender account intact."""

    repository = AdminRepository(app.state.database.session_factory)
    service = AccountService(app.state.database.session_factory, app.state.settings)
    account = await _account(repository, "protected sender", 7151, ready=True)
    other_account = await _account(repository, "other sender", 7152, ready=True)
    target = await repository.create_target(chat_ref="@protected_sender", title="Protected")
    async with app.state.database.session_factory() as session:
        subscriber_list = SubscriberList(
            name="Protected recipients", status=SubscriberListStatus.READY
        )
        subscriber = Subscriber(telegram_user_id=8151)
        invite_subscriber = Subscriber(telegram_user_id=8152)
        target_list = TargetList(name="Protected target list")
        session.add_all([subscriber_list, subscriber, invite_subscriber, target_list])
        await session.flush()

        direct_campaign = DirectCampaign(
            name="Protected direct campaign",
            subscriber_list_id=subscriber_list.id,
            primary_text="Message",
        )
        invite_campaign = SubscriberInviteCampaign(
            name="Protected invite campaign",
            subscriber_list_id=subscriber_list.id,
            destination_ref="@owned_channel",
            destination_key="owned_channel",
        )
        session.add_all([direct_campaign, invite_campaign])
        await session.flush()
        direct_launch = DirectLaunch(
            campaign_id=direct_campaign.id,
            subscriber_list_id=subscriber_list.id,
            status=DirectLaunchStatus.QUEUED,
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=1,
            contact_cooldown_days_snapshot=30,
            account_ids_snapshot_json=f"[{account.id}]",
            primary_text_snapshot="Message",
            consent_confirmed_at=account.created_at,
        )
        invite_launch = SubscriberInviteLaunch(
            campaign_id=invite_campaign.id,
            subscriber_list_id=subscriber_list.id,
            status=DirectLaunchStatus.QUEUED,
            destination_ref_snapshot="@owned_channel",
            destination_key_snapshot="owned_channel",
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=1,
            contact_cooldown_days_snapshot=30,
            account_ids_snapshot_json=f"[{account.id}]",
            consent_confirmed_at=account.created_at,
        )
        comment_rule = ChannelCommentRule(
            name="Protected comment rule",
            channel_ref="@protected_comments",
            primary_account_id=other_account.id,
            primary_text="First comment",
            dialogue_steps_json=json.dumps([{"account_id": account.id, "text": "follow up"}]),
        )
        comment_reaction_rule = ChannelCommentRule(
            name="Protected comment reactions",
            channel_ref="@protected_reactions",
            primary_account_id=other_account.id,
            primary_text="First comment",
            reaction_account_ids_json=f"[{account.id}]",
        )
        participant_rule = ParticipantReactionRule(
            name="Protected participant rule",
            target_list_id=target_list.id,
            account_ids_json=f"[{account.id}]",
        )
        session.add_all(
            [
                direct_launch,
                invite_launch,
                comment_rule,
                comment_reaction_rule,
                participant_rule,
            ]
        )
        await session.flush()
        dialogue_campaign = Campaign(
            name="Protected dialogue campaign",
            mode=CampaignMode.DIALOGUE,
            primary_account_id=other_account.id,
            target_id=target.id,
            primary_text="First message",
        )
        session.add(dialogue_campaign)
        await session.flush()
        session.add_all(
            [
                DirectMessageTask(launch_id=direct_launch.id, subscriber_id=subscriber.id),
                SubscriberInviteTask(
                    launch_id=invite_launch.id, subscriber_id=invite_subscriber.id
                ),
                ChannelCommentRun(rule_id=comment_rule.id, source_post_id=1),
                ChannelCommentRun(rule_id=comment_reaction_rule.id, source_post_id=2),
                ParticipantReactionTask(
                    rule_id=participant_rule.id,
                    target_id=target.id,
                    source_sender_id=99001,
                    message_id=1,
                    reactor_account_id=other_account.id,
                    emoji="👍",
                ),
                CampaignRun(
                    campaign_id=dialogue_campaign.id,
                    target_id=target.id,
                    sender_account_id=other_account.id,
                    request_key="protected-dialogue-run",
                    status=RunStatus.QUEUED,
                    dialogue_steps_snapshot_json=json.dumps(
                        [
                            {"account_id": other_account.id, "text": "First message"},
                            {"account_id": account.id, "text": "Follow up"},
                        ]
                    ),
                ),
            ]
        )
        await session.commit()

    plan = await service.account_removal_plan(account.id)
    assert plan.can_remove is False
    assert set(plan.active_work) == {
        "рассылки: 1",
        "рассылки в ЛС: 1",
        "приглашения: 1",
        "автокомментинг: 2",
        "авто-реакции в чатах: 1",
    }
    with pytest.raises(ConflictError, match="активных задачах"):
        await service.clear_session(account.id)


async def test_account_check_records_premium_only_when_telegram_supplies_it(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    service = app.state.account_service
    premium = await _account(repository, "premium-refresh", 7201, premium=None)
    ordinary = await _account(repository, "ordinary-refresh", 7202, premium=True)

    class Profile:
        id = 7201
        first_name = "Premium"
        last_name = "Account"
        phone = None
        username = "premium_account"
        photo = None
        premium = True

    await service._record_check(  # type: ignore[attr-defined]
        premium.id,
        authorized=True,
        me=Profile(),
        error=None,
        actor="test",
    )
    await service._record_check(  # type: ignore[attr-defined]
        ordinary.id,
        authorized=True,
        me=object(),
        error=None,
        actor="test",
    )
    refreshed = await service.get_account(premium.id)
    untouched = await service.get_account(ordinary.id)
    assert refreshed.is_premium is True and refreshed.premium_checked_at is not None
    assert untouched.is_premium is True
    premium_group = next(
        item
        for item in await repository.list_account_group_views()
        if item.system_key == PREMIUM_ACCOUNT_GROUP_KEY
    )
    assert premium_group.name == "⭐ Premium"
    assert await repository.list_account_group_member_ids(premium_group.id) == {
        premium.id,
        ordinary.id,
    }


async def test_system_premium_group_is_reconciled_and_cannot_be_edited(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    premium = await _account(repository, "premium-system", 7251, premium=True, ready=True)
    ordinary = await _account(repository, "ordinary-system", 7252, premium=False)

    group = await repository.ensure_system_account_groups(actor="test")
    assert group.is_system is True
    assert group.system_key == PREMIUM_ACCOUNT_GROUP_KEY
    assert await repository.list_account_group_member_ids(group.id) == {premium.id}

    with pytest.raises(ConflictError, match="обновляется по данным Telegram"):
        await repository.set_account_group_members(group.id, {ordinary.id})
    with pytest.raises(ConflictError, match="обновляется по данным Telegram"):
        await repository.update_account_group(
            group.id,
            name="not allowed",
            description="",
        )
    with pytest.raises(ConflictError, match="обновляется по данным Telegram"):
        await repository.delete_account_group(group.id)

    async with repository._session_factory() as session:  # type: ignore[attr-defined]
        stored = await session.get(Account, premium.id)
        assert stored is not None
        stored.is_premium = False
        await session.commit()
    await repository.ensure_system_account_groups(actor="test")
    assert await repository.list_account_group_member_ids(group.id) == set()


async def test_reimport_restores_preserved_account_card_and_disabled_card_cannot_toggle(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    service = app.state.account_service
    account = await _account(repository, "preserved card", 7301, ready=True)

    cleared = await service.clear_session(account.id)
    assert cleared.status is AccountStatus.DISABLED
    with pytest.raises(ConflictError, match="нет Telegram-сессии"):
        await repository.toggle_account(account.id)

    restored = await service._store_session(  # type: ignore[attr-defined]
        label="new label must not replace history",
        source=AccountSessionSource.SESSION_STRING,
        converted=_ConvertedSession(
            session_string="restored-session",
            api_id=12345,
            api_hash="restored-api-hash",
            telegram_user_id=7301,
            first_name="Restored",
            is_premium=False,
        ),
    )

    assert restored.id == account.id
    assert restored.label == "preserved card"
    assert restored.status is AccountStatus.ACTIVE
    assert restored.session_source is AccountSessionSource.SESSION_STRING
    assert restored.authorized is True and restored.is_premium is False


async def test_system_premium_group_sync_is_safe_when_called_concurrently(app: FastAPI) -> None:
    """Startup and profile refreshes may reconcile the same system group together."""

    repository = AdminRepository(app.state.database.session_factory)
    await asyncio.gather(
        repository.ensure_system_account_groups(actor="concurrent-startup-a"),
        repository.ensure_system_account_groups(actor="concurrent-startup-b"),
    )
    premium = await _account(repository, "concurrent premium", 7351, premium=True)
    await asyncio.gather(
        repository.ensure_system_account_groups(actor="concurrent-refresh-a"),
        repository.ensure_system_account_groups(actor="concurrent-refresh-b"),
    )
    groups = await repository.list_account_group_views()
    premium_group = next(group for group in groups if group.system_key == PREMIUM_ACCOUNT_GROUP_KEY)
    assert premium_group.account_count == 1
    # The account-level selector deliberately hides immutable system sections;
    # inspect the group itself to verify the synchronizer's durable membership.
    assert await repository.list_account_group_member_ids(premium_group.id) == {premium.id}
