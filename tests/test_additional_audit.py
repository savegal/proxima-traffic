"""Execution-boundary checks for the second externally supplied audit."""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select
from telethon import errors, functions, types

from proxima_traffic.blacklist_parser import ChatBlacklistParser
from proxima_traffic.gateways import GatewayError, TemporaryGatewayError
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    Account,
    AccountStatus,
    Campaign,
    CampaignAccount,
    CampaignMode,
    ChannelCommentMentionState,
    ChannelCommentMode,
    ChannelCommentRule,
    ChannelCommentRun,
    Target,
    TargetList,
    utc_now,
)
from proxima_traffic.services import ConflictError
from proxima_traffic.story_content import story_link_url


async def account_row(app, **values):
    async with app.state.database.session_factory() as session:
        account = Account(label=values.pop("label", "Reader"), ownership_confirmed=True, **values)
        session.add(account)
        await session.commit()
        return account


async def comment_rule(app):
    account = await account_row(app)
    async with app.state.database.session_factory() as session:
        rule = ChannelCommentRule(
            name="Fresh rule",
            channel_ref="@fresh",
            mode=ChannelCommentMode.SINGLE,
            primary_account_id=account.id,
            primary_text="Text",
            initial_delay_seconds=0,
        )
        session.add(rule)
        await session.commit()
        return rule


async def test_new_comment_rules_have_empty_complete_delivery_summaries(app):
    rule = await comment_rule(app)
    view = await app.state.channel_comments.get_rule_view(rule.id)
    views, total = await app.state.channel_comments.list_rules(0, 5)
    assert total == 1 and views[0].rule.id == rule.id
    for result in (view, views[0]):
        assert result.delivery.confirmed == 0
        assert result.delivery.history_incomplete is False


async def test_comment_reservations_release_under_a_legacy_sqlite_variable_limit(app):
    rule = await comment_rule(app)
    async with app.state.database.session_factory() as session:
        run = ChannelCommentRun(rule_id=rule.id, source_post_id=1)
        session.add(run)
        await session.flush()
        mention = ChannelCommentMentionState(
            rule_id=rule.id,
            telegram_user_id=9001,
            mention_count=7,
            reserved_run_id=run.id,
            reserved_until=utc_now() + timedelta(hours=1),
        )
        session.add(mention)
        await session.commit()
        mid, rid = mention.id, run.id

    def legacy_limit(connection, record):
        connection.run_async(
            lambda driver: driver._execute(
                driver._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
            )
        )

    await app.state.database.engine.dispose()
    event.listen(app.state.database.engine.sync_engine, "connect", legacy_limit)
    async with app.state.database.session_factory() as session:
        await app.state.channel_comments._release_mention_reservations(
            session, run_ids=[rid, *range(10_000, 11_050)]
        )
        await session.commit()
    async with app.state.database.session_factory() as session:
        mention = await session.get(ChannelCommentMentionState, mid)
        assert mention.reserved_run_id is None and mention.reserved_until is None
        assert mention.mention_count == 7


@pytest.mark.parametrize("expired", [False, True])
async def test_parallel_join_recording_never_loses_increments(app, expired):
    account = await account_row(
        app,
        joins_today=7,
        join_window_start=utc_now() - timedelta(hours=48 if expired else 1),
        status=AccountStatus.PAUSED,
        operator_paused=True,
    )
    await asyncio.gather(*(app.state.account_service.record_join(account.id) for _ in range(20)))
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account.id)
        assert account.joins_today == (20 if expired else 27)
        assert account.status == AccountStatus.PAUSED and account.operator_paused
    assert (
        await app.state.account_service.join_wait_seconds(
            account.id, daily_limit=account.joins_today, min_interval=0
        )
        > 0
    )


class ScanClient:
    def __init__(self, *, needs_join=False, pending=False, fail_after_join=False):
        self.needs_join = needs_join
        self.pending = pending
        self.fail_after_join = fail_after_join
        self.joined = False
        self.requests = []
        self.entity = SimpleNamespace(megagroup=True, left=True)

    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(request, functions.messages.CheckChatInviteRequest):
            return SimpleNamespace()
        if isinstance(
            request,
            (
                functions.channels.JoinChannelRequest,
                functions.messages.ImportChatInviteRequest,
            ),
        ):
            if self.pending:
                raise errors.InviteRequestSentError(None)
            self.joined = True
            return SimpleNamespace(chats=[self.entity])
        raise AssertionError(type(request).__name__)

    async def get_entity(self, entity):
        return entity

    async def iter_participants(self, entity, *, limit):
        if self.fail_after_join and self.joined:
            raise TemporaryGatewayError("route lost after joining", code="proxy_unavailable")
        if self.needs_join and not self.joined:
            raise errors.UserNotParticipantError(None)
        yield SimpleNamespace(id=9001)

    async def iter_messages(self, entity, *, limit):
        yield SimpleNamespace(sender_id=9001)


def scan_gateway(app, client, settings=None):
    gateway = TelethonGateway(app.state.database.session_factory, settings or app.state.settings)
    gateway._ensure_client = AsyncMock(return_value=client)
    gateway._resolve_entity = AsyncMock(return_value=client.entity)
    return gateway


@pytest.mark.parametrize("mode", ["members", "messages"])
async def test_public_blacklist_scan_does_not_join_readable_chats(app, mode):
    account = await account_row(app)
    client = ScanClient()
    gateway = scan_gateway(app, client)
    result = await gateway.scan_chat_blacklist_candidates(
        account=account, chat_ref="@public", mode=mode, limit=100
    )
    assert result.telegram_user_ids == (9001,) and not result.joined_chat
    assert not client.requests


async def test_public_scan_joins_only_after_membership_error_and_records_the_join(app):
    account = await account_row(app)
    client = ScanClient(needs_join=True)
    gateway = scan_gateway(app, client)
    result = await gateway.scan_chat_blacklist_candidates(
        account=account, chat_ref="@public", mode="members", limit=100
    )
    assert result.joined_chat and result.telegram_user_ids == (9001,)
    assert len(client.requests) == 1
    assert isinstance(client.requests[0], functions.channels.JoinChannelRequest)
    async with app.state.database.session_factory() as session:
        assert (await session.get(Account, account.id)).joins_today == 1


@pytest.mark.parametrize("chat_ref", ["@public", "https://t.me/+invite"])
async def test_scan_obeys_join_budget_before_any_join_request(app, chat_ref):
    account = await account_row(app, joins_today=1, join_window_start=utc_now())
    client = ScanClient(needs_join=True)
    settings = app.state.settings.model_copy(update={"account_join_daily_limit": 1})
    gateway = scan_gateway(app, client, settings)
    with pytest.raises(GatewayError) as error:
        await gateway.scan_chat_blacklist_candidates(
            account=account, chat_ref=chat_ref, mode="members", limit=100
        )
    assert error.value.code == "join_throttled"
    assert not any(
        isinstance(
            r,
            (
                functions.messages.ImportChatInviteRequest,
                functions.channels.JoinChannelRequest,
            ),
        )
        for r in client.requests
    )


async def test_an_existing_private_membership_can_be_read_with_an_exhausted_join_budget(app):
    account = await account_row(app, joins_today=1, join_window_start=utc_now())

    class ExistingMember(ScanClient):
        async def __call__(self, request):
            assert isinstance(request, functions.messages.CheckChatInviteRequest)
            return types.ChatInviteAlready(chat=self.entity)

    client = ExistingMember()
    settings = app.state.settings.model_copy(update={"account_join_daily_limit": 1})
    gateway = scan_gateway(app, client, settings)
    result = await gateway.scan_chat_blacklist_candidates(
        account=account, chat_ref="https://t.me/+invite", mode="members", limit=100
    )
    assert result.telegram_user_ids == (9001,) and not result.joined_chat


@pytest.mark.parametrize("chat_ref", ["@public", "https://t.me/+invite"])
@pytest.mark.parametrize("pending", [False, True])
async def test_scan_never_joins_the_whole_account_pool_after_a_join_or_pending_request(
    app, chat_ref, pending
):
    accounts = [await account_row(app, label=f"Reader {index}") for index in range(3)]
    client = ScanClient(needs_join=True, pending=pending, fail_after_join=not pending)
    gateway = scan_gateway(app, client)
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref=chat_ref, mode="members")
    gateway._ensure_client.assert_awaited_once_with(accounts[0].id)
    joins = [
        request
        for request in client.requests
        if isinstance(
            request,
            (
                functions.channels.JoinChannelRequest,
                functions.messages.ImportChatInviteRequest,
            ),
        )
    ]
    assert len(joins) == 1
    async with app.state.database.session_factory() as session:
        counts = list(await session.scalars(select(Account.joins_today).order_by(Account.id)))
        assert counts == [1, 0, 0]


@pytest.mark.parametrize("chat_ref", ["@public", "https://t.me/+invite"])
async def test_an_unacknowledged_join_is_not_replayed_with_another_account(app, chat_ref):
    accounts = [await account_row(app, label=f"Reader {index}") for index in range(3)]

    class LostAcknowledgement(ScanClient):
        async def __call__(self, request):
            if isinstance(request, functions.messages.CheckChatInviteRequest):
                return await super().__call__(request)
            self.requests.append(request)
            raise ConnectionError("response lost after dispatch")

    client = LostAcknowledgement(needs_join=True)
    gateway = scan_gateway(app, client)
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref=chat_ref, mode="members")
    gateway._ensure_client.assert_awaited_once_with(accounts[0].id)
    joins = [
        request
        for request in client.requests
        if isinstance(
            request,
            (functions.channels.JoinChannelRequest, functions.messages.ImportChatInviteRequest),
        )
    ]
    assert len(joins) == 1


@pytest.mark.parametrize("chat_ref", ["@public", "https://t.me/+invite"])
async def test_lease_loss_after_join_does_not_replay_the_join_with_another_account(app, chat_ref):
    accounts = [await account_row(app, label=f"Reader {index}") for index in range(3)]
    client = ScanClient(needs_join=True)
    gateway = scan_gateway(app, client)

    @asynccontextmanager
    async def lose_lease_after_operation(account_id):
        assert account_id == accounts[0].id
        yield
        raise TemporaryGatewayError("lease lost", code="account_operation_lease_lost")

    gateway._account_operation = lose_lease_after_operation
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref=chat_ref, mode="members")
    gateway._ensure_client.assert_awaited_once_with(accounts[0].id)
    assert client.joined
    async with app.state.database.session_factory() as session:
        counts = list(await session.scalars(select(Account.joins_today).order_by(Account.id)))
        assert counts == [1, 0, 0]


async def test_hidden_members_fall_back_to_history_without_joining(app):
    await account_row(app)

    class HiddenMembers(ScanClient):
        async def iter_participants(self, entity, *, limit):
            raise errors.ChatAdminRequiredError(None)
            yield  # This Telethon API returns an async iterator.

    client = HiddenMembers()
    gateway = scan_gateway(app, client)
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    result = await parser.parse(chat_ref="@public", mode="auto")
    assert result.fallback_used and result.mode == "messages"
    assert result.telegram_user_ids == (9001,) and not result.joined_chat
    assert not client.requests


@pytest.mark.parametrize("fails", [False, True])
async def test_fallback_retains_membership_side_effects_after_a_successful_join(app, fails):
    accounts = [await account_row(app, label=f"Reader {index}") for index in range(3)]

    class HiddenAfterJoin(ScanClient):
        async def iter_participants(self, entity, *, limit):
            if not self.joined:
                raise errors.UserNotParticipantError(None)
            raise errors.ChatAdminRequiredError(None)
            yield

        async def iter_messages(self, entity, *, limit):
            if fails:
                raise TemporaryGatewayError("route lost after join", code="proxy_unavailable")
            yield SimpleNamespace(sender_id=9001)

    client = HiddenAfterJoin()
    gateway = scan_gateway(app, client)
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    if fails:
        with pytest.raises(ConflictError):
            await parser.parse(chat_ref="@public", mode="auto")
    else:
        result = await parser.parse(chat_ref="@public", mode="auto")
        assert result.joined_chat and result.fallback_used
        assert result.telegram_user_ids == (9001,)
    assert gateway._ensure_client.await_count == 2
    assert all(call.args == (accounts[0].id,) for call in gateway._ensure_client.await_args_list)
    assert len(client.requests) == 1


async def test_account_paused_after_join_does_not_move_the_scan_to_another_account(app):
    accounts = [await account_row(app, label=f"Reader {index}") for index in range(2)]

    class PausedAfterJoin(ScanClient):
        async def iter_participants(self, entity, *, limit):
            if not self.joined:
                raise errors.UserNotParticipantError(None)
            async with app.state.database.session_factory() as session:
                account = await session.get(Account, accounts[0].id)
                account.status = AccountStatus.PAUSED
                account.operator_paused = True
                await session.commit()
            return
            yield

    client = PausedAfterJoin()
    gateway = scan_gateway(app, client)
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref="@public", mode="auto")
    gateway._ensure_client.assert_awaited_once_with(accounts[0].id)
    assert len(client.requests) == 1


@pytest.mark.parametrize(
    "caption,expected",
    [
        ("Канал: https://t.me/channel.", "https://t.me/channel"),
        ("[(https://example.org/path)]!", "https://example.org/path"),
        ("(https://example.org/Path_(one)).", "https://example.org/Path_(one)"),
        ("[http://label.example](https://target.example/path).", "https://target.example/path"),
        ("[Link](https://target.example/query?q=.)", "https://target.example/query?q=."),
        ('"https://example.org/path"', "https://example.org/path"),
        ("https://[::1]/path.", "https://[::1]/path"),
        ("https://example.org:bad", None),
        ("https://", None),
        ("Без ссылки", None),
        ("[a](https://one.example),[b](https://two.example)", "https://one.example"),
    ],
)
def test_story_link_targets_exclude_prose_but_preserve_explicit_urls(caption, expected):
    assert story_link_url(caption) == expected


async def test_patch_cannot_change_fields_absent_from_the_update_contract(
    app, client, admin_headers
):
    async with app.state.database.session_factory() as session:
        first, second = Account(label="First"), Account(label="Second")
        target, audience = Target(title="Chat", chat_ref="@chat"), TargetList(name="Audience")
        session.add_all([first, second, target, audience])
        await session.flush()
        campaign = Campaign(
            name="Patch contract",
            mode=CampaignMode.BROADCAST,
            primary_account_id=first.id,
            target_list_id=audience.id,
            primary_text="Text",
        )
        session.add(campaign)
        await session.flush()
        session.add(CampaignAccount(campaign_id=campaign.id, account_id=first.id))
        await session.commit()
        cid, aid, tid, lid = campaign.id, first.id, target.id, audience.id
    response = await client.patch(
        f"/api/v1/campaigns/{cid}",
        headers=admin_headers,
        json={
            "primary_account_id": second.id,
            "target_id": tid,
            "target_list_id": lid,
        },
    )
    assert response.status_code == 200
    assert response.json()["primary_account_id"] == aid
    assert response.json()["target_id"] is None and response.json()["target_list_id"] == lid
    async with app.state.database.session_factory() as session:
        pool = list(
            await session.scalars(
                select(CampaignAccount.account_id).where(CampaignAccount.campaign_id == cid)
            )
        )
        assert pool == [aid]


@pytest.mark.parametrize(
    "chat_ref",
    [
        "https://t.me/+Invite_1-2",
        "http://t.me/+Invite_1-2?source=test",
        "https://telegram.me/joinchat/Invite_1-2",
        "http://telegram.me/joinchat/Invite_1-2/#fragment",
    ],
)
def test_private_invite_aliases_share_the_same_guarded_path(chat_ref):
    assert ChatBlacklistParser._normalize_chat_ref(chat_ref) == "https://t.me/+Invite_1-2"


@pytest.mark.parametrize(
    "chat_ref", ["https://t.me/+", "https://t.me/joinchat/", "https://t.me/+x/y"]
)
def test_malformed_private_invites_are_rejected_before_any_account_operation(chat_ref):
    with pytest.raises(ConflictError):
        ChatBlacklistParser._normalize_chat_ref(chat_ref)


async def test_legacy_peer_flood_flags_trigger_a_real_spambot_recheck_without_expiry_guessing(app):
    from proxima_traffic.models import AccountSessionSource
    from proxima_traffic.spambot import SpamBotResult, SpamBotStatus
    from proxima_traffic.spambot_checks import SpamBotCheckService

    account = await account_row(
        app,
        authorized=True,
        session_source=AccountSessionSource.SESSION_STRING,
        session_encrypted="test-session",
        spambot_checked_at=utc_now(),
        spambot_status="active_restriction",
        spambot_parser_signal="peer_flood_error",
    )
    rpc = AsyncMock(
        return_value=SpamBotResult(
            SpamBotStatus.FREE, "No limits", parser_signal="official_free_reply"
        )
    )
    service = SpamBotCheckService(
        app.state.database.session_factory,
        SimpleNamespace(check_spambot_status=rpc),
        app.state.settings,
    )
    assert await service.check_due_accounts(limit=10) == 1
    rpc.assert_awaited_once()
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account.id)
        assert account.spambot_status == "free"
        assert account.spambot_parser_signal == "official_free_reply"
        assert account.cooldown_until is None
    assert await service.check_due_accounts(limit=10) == 0
