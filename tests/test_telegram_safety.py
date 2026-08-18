import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from telethon import errors, functions, types

from proxima_traffic.accounts import AccountService
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.gateways import (
    DirectRecipient,
    PermanentGatewayError,
    SentMessage,
    TemporaryGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    AccountStatus,
    Campaign,
    Target,
    TelegramDeliveryReceipt,
    utc_now,
)
from proxima_traffic.proxies import ProxyRouteUnavailable, ProxyService
from proxima_traffic.services import CampaignRunner, ConflictError


class AccountUnauthorizedGateway:
    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise PermanentGatewayError("session is no longer authorized", code="account_unauthorized")


class AccountBannedInChatGateway:
    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise PermanentGatewayError("account removed from chat", code="account_banned_in_chat")


class MissingDeliveryClient:
    def __init__(self) -> None:
        self.requests: list[object] = []

    async def __call__(self, request: object) -> object:
        self.requests.append(request)
        return types.UpdateShortSentMessage(id=777, pts=1, pts_count=1, date=None)

    async def get_messages(self, entity: object, *, ids: int) -> object | None:
        del entity, ids
        return None


class EmptyDeliveryClient(MissingDeliveryClient):
    async def get_messages(self, entity: object, *, ids: int) -> object | None:
        del entity, ids
        return type("MessageEmpty", (), {"id": 777})()


class ModerationNoticeClient(MissingDeliveryClient):
    async def get_messages(
        self,
        entity: object,
        *,
        ids: int | None = None,
        limit: int | None = None,
    ) -> object | None:
        del entity
        if ids is not None:
            return None
        assert limit is not None
        return [
            SimpleNamespace(
                id=778,
                message="Чтобы писать в чате, подпишитесь на канал и пройдите капчу.",
            )
        ]


class MembershipGateClient:
    def __init__(self, gate_message: object) -> None:
        self.gate_message = gate_message
        self.requests: list[object] = []

    async def get_messages(self, entity: object, *, limit: int) -> list[object]:
        del entity
        assert limit == 50
        return [self.gate_message]

    async def __call__(self, request: object) -> object:
        self.requests.append(request)
        if isinstance(request, functions.channels.GetParticipantRequest):
            raise errors.UserNotParticipantError(None)
        if isinstance(request, functions.channels.JoinChannelRequest):
            return True
        if isinstance(request, functions.messages.GetBotCallbackAnswerRequest):
            return SimpleNamespace(message="Access confirmed")
        raise AssertionError(f"unexpected request: {request!r}")


class DiscussionMembershipClient:
    def __init__(self) -> None:
        self.requests: list[object] = []

    async def __call__(self, request: object) -> object:
        self.requests.append(request)
        if isinstance(request, functions.channels.GetParticipantRequest):
            raise errors.UserNotParticipantError(None)
        if isinstance(request, functions.channels.JoinChannelRequest):
            return True
        raise AssertionError(f"unexpected request: {request!r}")


class DuplicateDeliveryClient:
    async def __call__(self, request: object) -> object:
        del request
        raise errors.RandomIdDuplicateError(None)

    async def iter_messages(self, entity: object, *, limit: int):
        del entity, limit
        yield type(
            "RecoveredMessage",
            (),
            {
                "id": 778,
                "message": "hello",
                "reply_to": None,
                "date": utc_now(),
                "out": True,
            },
        )()


class InvalidCommentClient:
    async def __call__(self, request: object) -> object:
        del request
        raise errors.MessageIdInvalidError(None)


class TypingClient:
    def __init__(self) -> None:
        self.requests: list[object] = []

    async def __call__(self, request: object) -> object:
        self.requests.append(request)
        return True


class ChannelPostScanClient:
    def __init__(self) -> None:
        self.arguments: dict[str, object] | None = None

    async def iter_messages(
        self,
        entity: object,
        **kwargs: object,
    ):
        self.arguments = {
            "entity": entity,
            **kwargs,
        }
        if kwargs["limit"] == 1:
            yield SimpleNamespace(id=52, date=utc_now(), action=None, sender_id=8002)
            return
        yield SimpleNamespace(id=51, date=utc_now(), action=None, sender_id=8001)
        yield SimpleNamespace(id=52, date=utc_now(), action=None, sender_id=8002)


class InMemoryAccountLeaseCoordinator:
    @asynccontextmanager
    async def hold(self, account_id: int):
        del account_id
        yield "test-account-lease"


def _gateway(settings: Settings) -> TelethonGateway:
    gateway = object.__new__(TelethonGateway)
    gateway._settings = settings
    gateway._account_leases = InMemoryAccountLeaseCoordinator()
    return gateway


def _channel_gateway(settings: Settings) -> TelethonGateway:
    gateway = _gateway(settings)
    gateway._locks = {}
    gateway._global_lock = asyncio.Lock()
    return gateway


def _settings(tmp_path, filename: str) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / filename}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_delivery_verify_delay_seconds=0,
    )


def test_telegram_rpc_errors_are_classified_by_blast_radius(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "rpc-map.db"))

    chat_ban = gateway._map_rpc_error(errors.UserBannedInChannelError(None), context="send")
    assert isinstance(chat_ban, PermanentGatewayError)
    assert chat_ban.code == "account_banned_in_chat"

    write_forbidden = gateway._map_rpc_error(
        errors.ChatSendPlainForbiddenError(None), context="send"
    )
    assert isinstance(write_forbidden, PermanentGatewayError)
    assert write_forbidden.code == "account_banned_in_chat"

    admin_required = gateway._map_rpc_error(errors.ChatAdminRequiredError(None), context="send")
    assert isinstance(admin_required, PermanentGatewayError)
    assert admin_required.code == "account_missing_admin_rights"

    peer_flood = gateway._map_rpc_error(errors.PeerFloodError(None), context="send")
    assert isinstance(peer_flood, TemporaryGatewayError)
    assert peer_flood.code == "account_restricted"
    assert peer_flood.retry_after is not None

    frozen_account = gateway._map_rpc_error(
        errors.RPCError(None, "USER_FROZEN", 403), context="send"
    )
    assert isinstance(frozen_account, TemporaryGatewayError)
    assert frozen_account.code == "account_restricted"

    slow_mode = gateway._map_rpc_error(errors.SlowModeWaitError(None, 17), context="send")
    assert isinstance(slow_mode, TemporaryGatewayError)
    assert slow_mode.code == "slow_mode"
    assert slow_mode.retry_after == 17

    gateway._settings.max_flood_wait_seconds = 10
    long_flood = gateway._map_rpc_error(errors.FloodWaitError(None, 90_000), context="send")
    assert isinstance(long_flood, TemporaryGatewayError)
    assert long_flood.code == "flood_wait"
    assert long_flood.retry_after == 108_000

    join_request = gateway._map_rpc_error(errors.InviteRequestSentError(None), context="join")
    assert isinstance(join_request, PermanentGatewayError)
    assert join_request.code == "join_request_pending"

    too_many_chats = gateway._map_rpc_error(errors.ChannelsTooMuchError(None), context="join")
    assert isinstance(too_many_chats, PermanentGatewayError)
    assert too_many_chats.code == "account_join_limit"

    normalized = PermanentGatewayError("folder expired", code="target_unavailable")
    assert gateway._map_rpc_error(normalized, context="join") is normalized

    direct_peer = gateway._map_rpc_error(errors.PeerIdInvalidError(None), context="direct_message")
    assert isinstance(direct_peer, PermanentGatewayError)
    assert direct_peer.code == "recipient_unresolvable"

    reaction_disabled = gateway._map_rpc_error(
        errors.RPCError(None, "CHAT_REACTIONS_NONE", 400),
        context="reaction",
    )
    assert isinstance(reaction_disabled, PermanentGatewayError)
    assert reaction_disabled.code == "reaction_unavailable"

    invalid_reaction = gateway._map_rpc_error(
        errors.ReactionInvalidError(None),
        context="reaction",
    )
    assert isinstance(invalid_reaction, PermanentGatewayError)
    assert invalid_reaction.code == "reaction_unavailable"

    invite_privacy = gateway._map_rpc_error(
        errors.UserPrivacyRestrictedError(None),
        context="subscriber_invite",
    )
    assert isinstance(invite_privacy, PermanentGatewayError)
    assert invite_privacy.code == "invite_recipient_privacy"

    invite_rights = gateway._map_rpc_error(
        errors.ChatAdminRequiredError(None),
        context="subscriber_invite",
    )
    assert isinstance(invite_rights, PermanentGatewayError)
    assert invite_rights.code == "invite_destination_rights"

    invite_limit = gateway._map_rpc_error(
        errors.UserChannelsTooMuchError(None),
        context="subscriber_invite",
    )
    assert isinstance(invite_limit, PermanentGatewayError)
    assert invite_limit.code == "invite_recipient_limit"

    premium_reaction = gateway._map_rpc_error(
        errors.PremiumAccountRequiredError(None),
        context="reaction",
    )
    assert isinstance(premium_reaction, PermanentGatewayError)
    assert premium_reaction.code == "reaction_unavailable"

    premium_send = gateway._map_rpc_error(
        errors.PremiumAccountRequiredError(None),
        context="send",
    )
    assert isinstance(premium_send, PermanentGatewayError)
    assert premium_send.code == "target_unavailable"

    broadcast_reaction = gateway._map_rpc_error(
        errors.BroadcastForbiddenError(None),
        context="reaction",
    )
    assert isinstance(broadcast_reaction, PermanentGatewayError)
    assert broadcast_reaction.code == "reaction_unavailable"

    broadcast_comment = gateway._map_rpc_error(
        errors.BroadcastForbiddenError(None),
        context="channel_comment",
    )
    assert isinstance(broadcast_comment, PermanentGatewayError)
    assert broadcast_comment.code == "comments_unavailable"

    broadcast_send = gateway._map_rpc_error(
        errors.BroadcastForbiddenError(None),
        context="send",
    )
    assert isinstance(broadcast_send, PermanentGatewayError)
    assert broadcast_send.code == "target_unavailable"

    premium_temporarily_unavailable = gateway._map_rpc_error(
        errors.PremiumCurrentlyUnavailableError(None),
        context="send",
    )
    assert isinstance(premium_temporarily_unavailable, TemporaryGatewayError)
    assert premium_temporarily_unavailable.code == "rpc_error"

    frozen_account = gateway._map_rpc_error(
        errors.FrozenParticipantMissingError(None),
        context="send",
    )
    assert isinstance(frozen_account, TemporaryGatewayError)
    assert frozen_account.code == "account_restricted"

    lost_after_send = gateway._map_rpc_error(errors.ChannelPrivateError(None), context="verify")
    assert isinstance(lost_after_send, PermanentGatewayError)
    assert lost_after_send.code == "account_banned_in_chat"

    docs_forbidden = gateway._map_rpc_error(
        errors.RPCError(None, "CHAT_SEND_DOCS_FORBIDDEN", 403), context="send"
    )
    assert isinstance(docs_forbidden, PermanentGatewayError)
    assert docs_forbidden.code == "content_forbidden"

    voice_forbidden = gateway._map_rpc_error(
        errors.RPCError(None, "VOICE_MESSAGES_FORBIDDEN", 403), context="send"
    )
    assert isinstance(voice_forbidden, PermanentGatewayError)
    assert voice_forbidden.code == "content_forbidden"

    gifs_forbidden = gateway._map_rpc_error(errors.ChatSendGifsForbiddenError(None), context="send")
    assert isinstance(gifs_forbidden, PermanentGatewayError)
    assert gifs_forbidden.code == "content_forbidden"

    banned_channel = gateway._map_rpc_error(errors.ChannelBannedError(None), context="send")
    assert isinstance(banned_channel, PermanentGatewayError)
    assert banned_channel.code == "target_unavailable"

    published_api_id = gateway._map_rpc_error(errors.ApiIdPublishedFloodError(None), context="send")
    assert isinstance(published_api_id, PermanentGatewayError)
    assert published_api_id.code == "account_dead"

    paid_chat = gateway._map_rpc_error(
        errors.RPCError(None, "ALLOW_PAYMENT_REQUIRED_10", 406), context="send"
    )
    assert isinstance(paid_chat, PermanentGatewayError)
    assert paid_chat.code == "target_unavailable"

    paid_direct_message = gateway._map_rpc_error(
        errors.RPCError(None, "ALLOW_PAYMENT_REQUIRED_10", 403),
        context="direct_message",
    )
    assert isinstance(paid_direct_message, PermanentGatewayError)
    assert paid_direct_message.code == "recipient_paid_message_required"

    premium_only_direct_message = gateway._map_rpc_error(
        errors.RPCError(None, "CONTACT_REQUIRE_PREMIUM", 403),
        context="direct_message",
    )
    assert isinstance(premium_only_direct_message, PermanentGatewayError)
    assert premium_only_direct_message.code == "recipient_requires_premium"

    private_direct_message = gateway._map_rpc_error(
        errors.UserPrivacyRestrictedError(None),
        context="direct_message",
    )
    assert isinstance(private_direct_message, PermanentGatewayError)
    assert private_direct_message.code == "recipient_privacy_restricted"

    unknown_direct_recipient = gateway._map_rpc_error(
        errors.PeerIdInvalidError(None),
        context="direct_message",
    )
    assert isinstance(unknown_direct_recipient, PermanentGatewayError)
    assert unknown_direct_recipient.code == "recipient_unresolvable"

    unknown_direct_forbidden = gateway._map_rpc_error(
        errors.ForbiddenError(None, "FORBIDDEN", 403), context="direct_message"
    )
    assert isinstance(unknown_direct_forbidden, PermanentGatewayError)
    assert unknown_direct_forbidden.code == "recipient_route_blocked"

    malformed_request = gateway._map_rpc_error(
        errors.RPCError(None, "MESSAGE_TOO_LONG", 400), context="send"
    )
    assert isinstance(malformed_request, PermanentGatewayError)
    assert malformed_request.code == "request_invalid"

    comments_disabled = gateway._map_rpc_error(
        errors.MessageIdInvalidError(None), context="channel_comment"
    )
    assert isinstance(comments_disabled, PermanentGatewayError)
    assert comments_disabled.code == "comments_unavailable"

    discussion_disabled = gateway._map_rpc_error(
        errors.ChatDiscussionUnallowedError(None), context="channel_comment_preflight"
    )
    assert isinstance(discussion_disabled, PermanentGatewayError)
    assert discussion_disabled.code == "comments_unavailable"

    edit_window_expired = gateway._map_rpc_error(
        errors.MessageEditTimeExpiredError(None), context="channel_comment_edit"
    )
    assert isinstance(edit_window_expired, PermanentGatewayError)
    assert edit_window_expired.code == "comment_edit_unavailable"


@pytest.mark.asyncio
async def test_direct_access_hash_resolution_still_loads_full_profile() -> None:
    class Client:
        def __init__(self) -> None:
            self.resolved: list[object] = []

        async def get_entity(self, entity: object) -> object:
            self.resolved.append(entity)
            return SimpleNamespace(
                id=99001,
                username="resolved_user",
                premium=True,
                bot=False,
                deleted=False,
                self=False,
            )

    client = Client()
    entity, profile = await TelethonGateway._resolve_direct_recipient(
        client,
        DirectRecipient(telegram_user_id=99001, username=None, access_hash=123456),
    )

    assert entity.user_id == 99001
    assert profile.username == "resolved_user"
    assert len(client.resolved) == 1


async def test_delivery_verification_detects_missing_message(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "delivery.db"))
    client = MissingDeliveryClient()

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._send(
            client,
            types.InputPeerSelf(),
            "hello",
            None,
            "stable-delivery-key",
        )

    assert raised.value.code == "post_deleted_by_moderation"
    assert raised.value.external_message_id == 777
    request = client.requests[0]
    assert isinstance(request, functions.messages.SendMessageRequest)
    assert request.random_id == gateway._message_random_id("stable-delivery-key")


async def test_duplicate_random_id_recovers_already_delivered_message(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "duplicate-delivery.db"))

    sent = await gateway._send(
        DuplicateDeliveryClient(),
        types.InputPeerSelf(),
        "hello",
        None,
        "stable-idempotency-key",
    )

    assert sent.message_id == 778


async def test_duplicate_recovery_uses_the_rendered_html_text(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "duplicate-formatted-delivery.db"))

    sent = await gateway._send(
        DuplicateDeliveryClient(),
        types.InputPeerSelf(),
        "plain fallback text",
        None,
        "stable-formatted-idempotency-key",
        html_text="<b>hello</b>",
    )

    assert sent.message_id == 778


async def test_duplicate_recovery_reuses_the_first_durable_attempt_time(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'durable-receipt.db'}")
    await database.create_schema()
    try:
        gateway = _gateway(_settings(tmp_path, "durable-receipt.db"))
        gateway._session_factory = database.session_factory
        await gateway._delivery_receipt("durable-idempotency-key")
        async with database.session_factory() as session:
            receipt = await session.scalar(select(TelegramDeliveryReceipt))
            assert receipt is not None
            receipt.first_attempt_at = utc_now() - timedelta(minutes=10)
            await session.commit()

        sent = await gateway._send(
            DuplicateDeliveryClient(),
            types.InputPeerSelf(),
            "hello",
            None,
            "durable-idempotency-key",
        )

        assert sent.message_id == 778
        async with database.session_factory() as session:
            receipt = await session.scalar(select(TelegramDeliveryReceipt))
            assert receipt is not None
            assert receipt.external_message_id == 778
    finally:
        await database.dispose()


async def test_channel_comment_send_keeps_its_error_context(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "channel-comment-context.db"))

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._send(
            InvalidCommentClient(),
            types.InputPeerSelf(),
            "comment",
            123,
            "channel-comment-context",
            error_context="channel_comment",
        )

    assert raised.value.code == "comments_unavailable"


async def test_typing_status_precedes_the_short_composition_pause(tmp_path, monkeypatch) -> None:
    gateway = _gateway(_settings(tmp_path, "typing.db"))
    client = TypingClient()
    pauses: list[float] = []

    async def capture_pause(seconds: float) -> None:
        pauses.append(seconds)

    monkeypatch.setattr("proxima_traffic.live_gateway.random.uniform", lambda _a, _b: 2.5)
    monkeypatch.setattr("proxima_traffic.live_gateway.asyncio.sleep", capture_pause)

    await gateway._show_typing_before_send(client, types.InputPeerSelf())

    assert len(client.requests) == 1
    request = client.requests[0]
    assert isinstance(request, functions.messages.SetTypingRequest)
    assert isinstance(request.action, types.SendMessageTypingAction)
    assert pauses == [2.5]


async def test_typing_status_can_be_disabled(tmp_path) -> None:
    settings = _settings(tmp_path, "typing-disabled.db")
    settings.account_typing_enabled = False
    gateway = _gateway(settings)
    client = TypingClient()

    await gateway._show_typing_before_send(client, types.InputPeerSelf())

    assert client.requests == []


async def test_channel_scan_reads_forward_from_last_seen_post(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-scan.db"))
    client = ChannelPostScanClient()
    channel = SimpleNamespace(broadcast=True, title="Новости")

    async def ensure_client(account_id: int) -> ChannelPostScanClient:
        assert account_id == 91
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        channel_ref: str,
    ) -> object:
        assert resolved_client is client
        assert account_id == 91
        assert channel_ref == "@news_channel"
        return channel

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    account = Account(id=91, label="scan-account", ownership_confirmed=True)

    scan = await gateway.scan_channel_posts(
        account=account,
        channel_ref="@news_channel",
        after_message_id=50,
        limit=30,
    )

    assert client.arguments is not None
    assert client.arguments["limit"] == 30
    assert client.arguments["min_id"] == 50
    assert client.arguments["reverse"] is True
    assert [post.message_id for post in scan.posts] == [51, 52]


async def test_channel_scan_collapses_media_album_to_one_comment_target(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-album-scan.db"))
    channel = SimpleNamespace(broadcast=True, title="Новости")

    class AlbumClient:
        async def iter_messages(self, entity: object, **kwargs: object):
            assert entity is channel
            assert kwargs == {"limit": 30, "min_id": 50, "reverse": True}
            yield SimpleNamespace(id=51, date=utc_now(), action=None, grouped_id=777)
            yield SimpleNamespace(id=52, date=utc_now(), action=None, grouped_id=777)
            yield SimpleNamespace(id=53, date=utc_now(), action=None, grouped_id=None)

    client = AlbumClient()

    async def ensure_client(account_id: int) -> AlbumClient:
        assert account_id == 92
        return client

    async def resolve_entity(*args: object) -> object:
        del args
        return channel

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]

    scan = await gateway.scan_channel_posts(
        account=Account(id=92, label="album-scan", ownership_confirmed=True),
        channel_ref="@album_channel",
        after_message_id=50,
        limit=30,
    )

    assert [post.message_id for post in scan.posts] == [51, 53]


async def test_channel_scan_bootstrap_reads_only_the_current_newest_post(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-bootstrap.db"))
    client = ChannelPostScanClient()
    channel = SimpleNamespace(broadcast=True, title="Новости")

    async def ensure_client(account_id: int) -> ChannelPostScanClient:
        assert account_id == 94
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        channel_ref: str,
    ) -> object:
        assert resolved_client is client
        assert account_id == 94
        assert channel_ref == "@bootstrap_channel"
        return channel

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    account = Account(id=94, label="bootstrap-account", ownership_confirmed=True)

    scan = await gateway.scan_channel_posts(
        account=account,
        channel_ref="@bootstrap_channel",
        after_message_id=0,
        limit=30,
        bootstrap=True,
    )

    assert client.arguments == {"entity": channel, "limit": 1}
    assert [post.message_id for post in scan.posts] == [52]


async def test_channel_scan_bootstrap_preserves_posts_after_rule_was_armed(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-armed-bootstrap.db"))
    client = ChannelPostScanClient()
    channel = SimpleNamespace(broadcast=True, title="Новости")
    armed_at = utc_now() - timedelta(minutes=2)

    async def ensure_client(account_id: int) -> ChannelPostScanClient:
        assert account_id == 97
        return client

    async def resolve_entity(*args: object) -> object:
        del args
        return channel

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    scan = await gateway.scan_channel_posts(
        account=Account(id=97, label="armed-bootstrap", ownership_confirmed=True),
        channel_ref="@armed_bootstrap",
        after_message_id=0,
        limit=30,
        bootstrap=True,
        after_date=armed_at,
    )

    assert client.arguments == {
        "entity": channel,
        "limit": 30,
        "offset_date": armed_at,
        "reverse": True,
    }
    assert [post.message_id for post in scan.posts] == [51, 52]


async def test_channel_comment_preflight_checks_discussion_and_send_rights(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "comment-preflight.db"))
    channel = SimpleNamespace(broadcast=True, title="Новости")
    discussion = object()
    discussion_entity = object()
    checked_entities: list[object] = []

    class PreflightClient:
        async def iter_messages(self, entity: object, **kwargs: object):
            assert entity is channel
            assert kwargs == {"limit": 20}
            yield SimpleNamespace(id=777, date=utc_now(), action=None)

    client = PreflightClient()

    async def ensure_client(account_id: int) -> PreflightClient:
        assert account_id == 98
        return client

    async def resolve_entity(*args: object) -> object:
        del args
        return channel

    async def discussion_for_post(*args: object) -> object:
        del args
        return discussion

    async def resolve_discussion_entity(*args: object) -> tuple[object, int]:
        assert args[-1] is discussion
        return discussion_entity, 778

    def ensure_can_send(entity: object) -> None:
        checked_entities.append(entity)

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    gateway._discussion_for_post = discussion_for_post  # type: ignore[method-assign]
    gateway._discussion_entity = resolve_discussion_entity  # type: ignore[method-assign]
    gateway._ensure_can_send = ensure_can_send  # type: ignore[method-assign]

    result = await gateway.preflight_channel_comments(
        account=Account(id=98, label="preflight", ownership_confirmed=True),
        channel_ref="@preflight_channel",
    )

    assert result.title == "Новости"
    assert result.channel_post_id == 777
    assert result.discussion_root_message_id == 778
    assert checked_entities == [discussion_entity]


async def test_channel_comment_reaction_uses_the_linked_discussion_peer(
    tmp_path,
    monkeypatch,
) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "comment-reaction.db"))
    channel = SimpleNamespace(broadcast=True)
    discussion = object()
    discussion_entity = object()

    class ReactionClient:
        def __init__(self) -> None:
            self.requests: list[object] = []

        async def __call__(self, request: object) -> object:
            self.requests.append(request)
            return True

    client = ReactionClient()

    async def ensure_client(account_id: int) -> ReactionClient:
        assert account_id == 97
        return client

    async def resolve_entity(*args: object) -> object:
        del args
        return channel

    async def discussion_for_post(*args: object) -> object:
        del args
        return discussion

    async def resolve_discussion_entity(*args: object) -> tuple[object, int]:
        assert args[-1] is discussion
        return discussion_entity, 701

    async def ensure_membership(*args: object, **kwargs: object) -> None:
        del args
        assert kwargs == {"join_if_needed": True}

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    gateway._discussion_for_post = discussion_for_post  # type: ignore[method-assign]
    gateway._discussion_entity = resolve_discussion_entity  # type: ignore[method-assign]
    gateway._ensure_discussion_membership = ensure_membership  # type: ignore[method-assign]
    monkeypatch.setattr("telethon.utils.get_input_peer", lambda entity: ("peer", entity))

    await gateway.send_channel_comment_reaction(
        account=Account(id=97, label="comment-reactor", ownership_confirmed=True),
        channel_ref="@reaction_channel",
        channel_post_id=700,
        message_id=702,
        emoji="🔥",
    )

    assert len(client.requests) == 1
    request = client.requests[0]
    assert isinstance(request, functions.messages.SendReactionRequest)
    assert request.peer == ("peer", discussion_entity)
    assert request.msg_id == 702
    assert request.reaction == [types.ReactionEmoji(emoticon="🔥")]


async def test_participant_scan_bootstrap_reads_only_the_current_newest_message(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "participant-bootstrap.db"))
    client = ChannelPostScanClient()
    target_entity = SimpleNamespace(megagroup=True)

    async def ensure_client(account_id: int) -> ChannelPostScanClient:
        assert account_id == 95
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        chat_ref: str,
    ) -> object:
        assert resolved_client is client
        assert account_id == 95
        assert chat_ref == "@participant_bootstrap"
        return target_entity

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    account = Account(id=95, label="participant-bootstrap", ownership_confirmed=True)
    target = Target(id=96, chat_ref="@participant_bootstrap", title="Participant bootstrap")

    scan = await gateway.scan_participant_messages(
        account=account,
        target=target,
        after_message_id=0,
        limit=100,
        bootstrap=True,
    )

    assert client.arguments == {"entity": target_entity, "limit": 1}
    assert [(item.message_id, item.sender_id) for item in scan.messages] == [(52, 8002)]


async def test_channel_comment_anchors_first_and_follow_up_replies(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-anchor.db"))
    client = object()
    channel = SimpleNamespace(broadcast=True)
    discussion_entity = SimpleNamespace(megagroup=True, broadcast=False)
    sent_calls: list[dict[str, object]] = []

    async def ensure_client(account_id: int) -> object:
        assert account_id == 92
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        channel_ref: str,
    ) -> object:
        assert resolved_client is client
        assert account_id == 92
        assert channel_ref == "@comments_channel"
        return channel

    async def discussion_for_post(
        resolved_client: object,
        resolved_channel: object,
        post_id: int,
    ) -> object:
        assert resolved_client is client
        assert resolved_channel is channel
        assert post_id == 70
        return object()

    async def discussion_for_entity(
        resolved_client: object,
        discussion: object,
    ) -> tuple[object, int]:
        assert resolved_client is client
        assert discussion is not None
        return discussion_entity, 700

    async def send(
        resolved_client: object,
        entity: object,
        text: str,
        reply_to_message_id: int | None,
        idempotency_key: str,
        **kwargs: object,
    ) -> SentMessage:
        sent_calls.append(
            {
                "client": resolved_client,
                "entity": entity,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
                "idempotency_key": idempotency_key,
                **kwargs,
            }
        )
        return SentMessage(message_id=800 + len(sent_calls))

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    gateway._discussion_for_post = discussion_for_post  # type: ignore[method-assign]
    gateway._discussion_entity = discussion_for_entity  # type: ignore[method-assign]
    gateway._send = send  # type: ignore[method-assign]
    account = Account(id=92, label="comment-account", ownership_confirmed=True)

    first = await gateway.send_channel_comment(
        account=account,
        channel_ref="@comments_channel",
        channel_post_id=70,
        text="Первый комментарий",
        idempotency_key="comment:1",
    )
    reply = await gateway.send_channel_comment(
        account=account,
        channel_ref="@comments_channel",
        channel_post_id=70,
        text="Ответ",
        idempotency_key="comment:2",
        reply_to_message_id=first.message_id,
    )

    assert first.thread_root_message_id == 700
    assert reply.thread_root_message_id == 700
    assert sent_calls[0]["reply_to_message_id"] == 700
    assert sent_calls[1]["reply_to_message_id"] == first.message_id
    assert sent_calls[0]["error_context"] == "channel_comment"


async def test_channel_comment_joins_linked_discussion_under_normal_limits(tmp_path) -> None:
    settings = _settings(tmp_path, "channel-discussion-join.db").model_copy(
        update={"account_auto_join": True}
    )
    gateway = _channel_gateway(settings)
    client = DiscussionMembershipClient()
    limit_checks: list[int] = []
    recorded_joins: list[int] = []

    async def enforce_join_limit(account_id: int) -> None:
        limit_checks.append(account_id)

    async def record_join(account_id: int) -> None:
        recorded_joins.append(account_id)

    gateway._enforce_join_limit = enforce_join_limit  # type: ignore[method-assign]
    gateway._account_service = SimpleNamespace(record_join=record_join)
    discussion = types.Channel(
        id=700,
        title="Linked discussion",
        photo=types.ChatPhotoEmpty(),
        date=utc_now(),
        broadcast=False,
        megagroup=True,
        access_hash=701,
    )

    joined = await gateway._ensure_discussion_membership(
        client,
        92,
        discussion,
        join_if_needed=True,
    )

    assert joined is True
    assert limit_checks == [92]
    assert recorded_joins == [92]
    assert [type(request) for request in client.requests] == [
        functions.channels.GetParticipantRequest,
        functions.channels.JoinChannelRequest,
    ]


async def test_channel_comment_preflight_never_joins_discussion(tmp_path) -> None:
    settings = _settings(tmp_path, "channel-discussion-preflight.db").model_copy(
        update={"account_auto_join": True}
    )
    gateway = _channel_gateway(settings)
    client = DiscussionMembershipClient()
    discussion = types.Channel(
        id=702,
        title="Linked discussion",
        photo=types.ChatPhotoEmpty(),
        date=utc_now(),
        broadcast=False,
        megagroup=True,
        access_hash=703,
    )

    joined = await gateway._ensure_discussion_membership(
        client,
        92,
        discussion,
        join_if_needed=False,
    )

    assert joined is False
    assert [type(request) for request in client.requests] == [
        functions.channels.GetParticipantRequest
    ]


async def test_channel_comment_rejects_guest_when_auto_join_is_disabled(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "channel-discussion-disabled.db"))
    client = DiscussionMembershipClient()
    discussion = types.Channel(
        id=704,
        title="Linked discussion",
        photo=types.ChatPhotoEmpty(),
        date=utc_now(),
        broadcast=False,
        megagroup=True,
        access_hash=705,
    )

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._ensure_discussion_membership(
            client,
            92,
            discussion,
            join_if_needed=True,
        )

    assert raised.value.code == "target_not_joined"
    assert [type(request) for request in client.requests] == [
        functions.channels.GetParticipantRequest
    ]


async def test_delivery_verification_rejects_message_empty(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "delivery-empty.db"))

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._send(
            EmptyDeliveryClient(),
            types.InputPeerSelf(),
            "hello",
            321,
            "stable-reply-key",
        )

    assert raised.value.code == "post_deleted_by_moderation"


async def test_delivery_verification_preserves_moderation_notice(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "delivery-notice.db"))

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._send(
            ModerationNoticeClient(),
            types.InputPeerSelf(),
            "hello",
            None,
            "stable-notice-key",
        )

    assert raised.value.code == "post_deleted_by_moderation"
    assert "подпишитесь" in raised.value.safe_message


async def test_membership_gate_joins_button_channels_and_clicks_verify_once(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "membership-gate.db"))
    gate_message = SimpleNamespace(
        id=778,
        message="Чтобы писать в чате, подпишитесь на канал и нажмите Проверить.",
        sender=SimpleNamespace(bot=True),
        reply_markup=types.ReplyInlineMarkup(
            rows=[
                types.KeyboardButtonRow(
                    buttons=[
                        types.KeyboardButtonUrl("Канал", "https://t.me/required_news"),
                        types.KeyboardButtonCallback("Проверить", b"membership-check"),
                    ]
                )
            ]
        ),
    )
    client = MembershipGateClient(gate_message)
    target_entity = types.InputPeerSelf()
    channel_entity = types.Channel(
        123,
        "Required news",
        types.ChatPhotoEmpty(),
        None,
        access_hash=456,
        username="required_news",
    )
    joined_accounts: list[int] = []

    async def ensure_client(account_id: int) -> MembershipGateClient:
        assert account_id == 93
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        chat_ref: str,
    ) -> object:
        assert resolved_client is client
        assert account_id == 93
        return target_entity if chat_ref == "@target_chat" else channel_entity

    async def enforce_join_limit(account_id: int) -> None:
        assert account_id == 93

    async def record_join(account_id: int) -> None:
        joined_accounts.append(account_id)

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_entity = resolve_entity  # type: ignore[method-assign]
    gateway._enforce_join_limit = enforce_join_limit  # type: ignore[method-assign]
    gateway._account_service = SimpleNamespace(record_join=record_join)
    result = await gateway.recover_membership_gate(
        account=Account(id=93, label="gate-account", ownership_confirmed=True),
        target=Target(id=94, chat_ref="@target_chat", title="Gate target"),
        deleted_message_id=777,
    )

    assert result.detected is True
    assert result.recovered is True
    assert result.joined_channel_refs == ("@required_news",)
    assert result.verification_callback_count == 1
    assert joined_accounts == [93]
    assert any(
        isinstance(request, functions.channels.JoinChannelRequest) for request in client.requests
    )
    assert any(
        isinstance(request, functions.messages.GetBotCallbackAnswerRequest)
        for request in client.requests
    )


async def test_membership_gate_ignores_non_bot_and_mismatched_bot_replies(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "membership-gate-source.db"))
    markup = types.ReplyInlineMarkup(
        rows=[
            types.KeyboardButtonRow(
                buttons=[types.KeyboardButtonUrl("Канал", "https://t.me/required_news")]
            )
        ]
    )
    messages = [
        SimpleNamespace(
            id=778,
            message="Чтобы писать в чате, подпишитесь на канал.",
            sender=SimpleNamespace(bot=False),
            reply_markup=markup,
        ),
        SimpleNamespace(
            id=779,
            message="Чтобы писать в чате, подпишитесь на канал.",
            sender=SimpleNamespace(bot=True),
            reply_to=SimpleNamespace(reply_to_msg_id=123),
            reply_markup=markup,
        ),
    ]

    message, gate = await gateway._find_membership_gate(  # noqa: SLF001 - safety policy test
        object(),
        types.InputPeerSelf(),
        messages,
        after_message_id=777,
    )

    assert message is None
    assert gate is None


async def test_membership_gate_ignores_a_bot_that_is_not_a_chat_admin(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "membership-gate-non-admin.db"))
    entity = types.Channel(
        551,
        "Target chat",
        types.ChatPhotoEmpty(),
        None,
        megagroup=True,
        access_hash=552,
    )
    message = SimpleNamespace(
        id=778,
        message="Чтобы писать в чате, подпишитесь на канал.",
        sender=types.User(553, bot=True, access_hash=554),
        reply_markup=types.ReplyInlineMarkup(
            rows=[
                types.KeyboardButtonRow(
                    buttons=[types.KeyboardButtonUrl("Канал", "https://t.me/required_news")]
                )
            ]
        ),
    )

    class NonAdminClient:
        async def __call__(self, request: object) -> object:
            assert isinstance(request, functions.channels.GetParticipantRequest)
            return SimpleNamespace(participant=types.ChannelParticipant(553, None))

    found_message, gate = await gateway._find_membership_gate(  # noqa: SLF001 - policy test
        NonAdminClient(),
        entity,
        [message],
        after_message_id=777,
    )

    assert found_message is None
    assert gate is None


async def test_delayed_verification_prioritizes_removed_post_over_later_chat_ban(tmp_path) -> None:
    gateway = _channel_gateway(_settings(tmp_path, "delayed-deletion-wins.db"))
    client = ModerationNoticeClient()
    entity = types.InputPeerSelf()
    permission_checked = False

    async def ensure_client(account_id: int) -> ModerationNoticeClient:
        assert account_id == 93
        return client

    async def resolve_entity(
        resolved_client: object,
        account_id: int,
        target: Target,
    ) -> object:
        assert resolved_client is client
        assert account_id == 93
        assert target.chat_ref == "@delayed_delete"
        return entity

    def ensure_can_send(resolved_entity: object) -> None:
        nonlocal permission_checked
        assert resolved_entity is entity
        permission_checked = True
        raise PermanentGatewayError(
            "account was removed from the chat",
            code="account_banned_in_chat",
        )

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    gateway._resolve_preflight_entity = resolve_entity  # type: ignore[method-assign]
    gateway._ensure_can_send = ensure_can_send  # type: ignore[method-assign]
    account = Account(id=93, label="delayed-delete", ownership_confirmed=True)
    target = Target(id=94, chat_ref="@delayed_delete", title="Delayed delete")

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway.verify_sent_message(
            account=account,
            target=target,
            message_id=777,
        )

    assert raised.value.code == "post_deleted_by_moderation"
    assert permission_checked is False


def test_message_random_id_is_stable_and_key_specific(tmp_path) -> None:
    gateway = _gateway(_settings(tmp_path, "random-id.db"))

    first = gateway._message_random_id("launch:target:primary")
    assert first == gateway._message_random_id("launch:target:primary")
    assert first != gateway._message_random_id("launch:target:reply")


async def test_required_proxy_outage_is_retryable(tmp_path) -> None:
    class MissingProxyService:
        async def require_account_route(self, account_id: int) -> object:
            assert account_id == 12
            raise ProxyRouteUnavailable("proxy is being rotated")

    gateway = _gateway(_settings(tmp_path, "proxy-outage.db"))
    gateway._settings.account_require_proxy = True
    gateway._proxy_service = MissingProxyService()

    with pytest.raises(TemporaryGatewayError) as raised:
        await gateway._resolve_proxy(12)

    assert raised.value.code == "proxy_unavailable"
    assert raised.value.retry_after == 60


async def test_cached_client_rechecks_its_proxy_before_reuse_when_due(tmp_path) -> None:
    class CachedClient:
        def is_connected(self) -> bool:
            return True

    class RouteCheckService:
        def __init__(self) -> None:
            self.calls: list[tuple[int, float]] = []

        async def account_route_check_due(self, account_id: int, *, max_age_seconds: float) -> bool:
            self.calls.append((account_id, max_age_seconds))
            return True

    gateway = _gateway(_settings(tmp_path, "cached-proxy-check.db"))
    gateway._clients = {12: CachedClient()}
    gateway._client_generations = {12: 4}
    route_service = RouteCheckService()
    gateway._proxy_service = route_service
    recovered: list[int] = []

    async def account_generation(account_id: int) -> int:
        assert account_id == 12
        return 4

    async def resolve_proxy(account_id: int) -> object:
        recovered.append(account_id)
        return object()

    gateway._account_connection_generation = account_generation  # type: ignore[method-assign]
    gateway._resolve_proxy = resolve_proxy  # type: ignore[method-assign]

    client = await gateway._ensure_client(12)

    assert client is gateway._clients[12]
    assert recovered == [12]
    assert route_service.calls == [(12, gateway._settings.proxy_health_recheck_seconds)]


async def test_account_operations_share_gateway_client_and_lock(tmp_path) -> None:
    settings = _settings(tmp_path, "shared-account-client.db")
    database = Database(settings.database_url)
    await database.create_schema()
    async with database.session_factory() as session:
        session.add(Account(id=7, label="shared-account", ownership_confirmed=True))
        await session.commit()
    proxy_service = ProxyService(database.session_factory, settings)
    account_service = AccountService(database.session_factory, settings, proxy_service)
    gateway = TelethonGateway(
        database.session_factory,
        settings,
        account_service=account_service,
        proxy_service=proxy_service,
    )
    shared_client = SimpleNamespace()

    async def get_me():
        return SimpleNamespace(id=7001)

    shared_client.get_me = get_me

    async def ensure_client(account_id: int):
        assert account_id == 7
        return shared_client

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    observed_clients: list[object] = []

    async def first_operation(client: object) -> str:
        observed_clients.append(client)
        first_started.set()
        await release_first.wait()
        return "first"

    async def second_operation(client: object) -> str:
        observed_clients.append(client)
        second_started.set()
        return "second"

    first_task = asyncio.create_task(
        account_service._run_account_operation(7, first_operation)  # type: ignore[attr-defined]
    )
    await first_started.wait()
    second_task = asyncio.create_task(gateway.run_account_operation(7, second_operation))
    await asyncio.sleep(0)
    assert second_started.is_set() is False
    release_first.set()
    first_result, second_result = await asyncio.gather(first_task, second_task)

    assert first_result[1] == "first"
    assert second_result[1] == "second"
    assert observed_clients == [shared_client, shared_client]
    await database.dispose()


async def test_spambot_check_disconnects_its_cached_account_client(tmp_path) -> None:
    class Conversation:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def send_message(self, text: str) -> None:
            assert text == "/start"

        async def get_response(self):
            return SimpleNamespace(
                raw_text="Good news! No limits are currently applied to your account.",
                reply_markup=None,
                entities=None,
            )

    class Client:
        def __init__(self) -> None:
            self.disconnected = False

        def conversation(self, *args: object, **kwargs: object) -> Conversation:
            del args, kwargs
            return Conversation()

        async def disconnect(self) -> None:
            self.disconnected = True

    settings = _settings(tmp_path, "spambot-disconnect.db")
    database = Database(settings.database_url)
    await database.create_schema()
    async with database.session_factory() as session:
        account = Account(label="spambot-disconnect", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        account_id = int(account.id)
    gateway = TelethonGateway(database.session_factory, settings)
    client = Client()
    gateway._clients[account_id] = client

    async def ensure_client(checked_account_id: int) -> Client:
        assert checked_account_id == account_id
        return client

    gateway._ensure_client = ensure_client  # type: ignore[method-assign]

    result = await gateway.check_spambot_status(account=account)

    assert result.status.value == "free"
    assert client.disconnected is True
    assert account_id not in gateway._clients
    await database.dispose()


async def test_gateway_reconnects_when_another_process_changes_the_proxy_route(
    tmp_path,
) -> None:
    class ConnectedClient:
        def __init__(self) -> None:
            self.disconnected = False

        def is_connected(self) -> bool:
            return not self.disconnected

        async def disconnect(self) -> None:
            self.disconnected = True

    settings = _settings(tmp_path, "proxy-generation.db")
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="proxy-generation", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        gateway = TelethonGateway(database.session_factory, settings)
        old_client = ConnectedClient()
        new_client = ConnectedClient()
        gateway._clients[account.id] = old_client
        gateway._client_generations[account.id] = 0

        async def build_client(account_id: int) -> ConnectedClient:
            assert account_id == account.id
            return new_client

        gateway._build_client = build_client  # type: ignore[method-assign]
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            stored.connection_generation = 1
            await session.commit()

        resolved = await gateway._ensure_client(account.id)

        assert resolved is new_client
        assert old_client.disconnected is True
        assert gateway._client_generations[account.id] == 1
    finally:
        await database.dispose()


async def test_client_connect_rpc_failure_is_normalized_and_client_is_closed(
    tmp_path, monkeypatch
) -> None:
    import telethon

    class AccountServiceStub:
        @staticmethod
        async def load_session(account_id: int):
            assert account_id == 77
            return SimpleNamespace(session_string="", api_id=12345, api_hash="hash")

    class FailingClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.disconnected = False

        async def connect(self) -> None:
            raise errors.AuthKeyUnregisteredError(None)

        async def disconnect(self) -> None:
            self.disconnected = True

    created: list[FailingClient] = []

    def client_factory(*args: object, **kwargs: object) -> FailingClient:
        client = FailingClient(*args, **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(telethon, "TelegramClient", client_factory)
    gateway = _gateway(_settings(tmp_path, "connect-normalized.db"))
    gateway._account_service = AccountServiceStub()

    async def no_proxy(account_id: int):
        assert account_id == 77
        return None

    gateway._resolve_proxy = no_proxy  # type: ignore[method-assign]

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._build_client(77)

    assert raised.value.code == "account_dead"
    assert created and created[0].disconnected is True


async def test_corrupted_stored_session_is_normalized_as_unauthorized(tmp_path) -> None:
    from proxima_traffic.accounts import AccountImportError

    class AccountServiceStub:
        @staticmethod
        async def load_session(account_id: int):
            assert account_id == 78
            raise AccountImportError("cipher key mismatch")

    gateway = _gateway(_settings(tmp_path, "corrupted-session.db"))
    gateway._account_service = AccountServiceStub()

    with pytest.raises(PermanentGatewayError) as raised:
        await gateway._build_client(78)

    assert raised.value.code == "account_unauthorized"
    assert "импортируйте её заново" in str(raised.value)


async def test_album_uploads_media_before_send_multi_media(tmp_path, monkeypatch) -> None:
    from telethon import utils

    gateway = _gateway(_settings(tmp_path, "album-upload.db"))

    class AlbumClient:
        def __init__(self) -> None:
            self.upload_requests: list[object] = []
            self.send_request = None
            self.uploaded_results: list[object] = []

        async def upload_file(self, path: str):
            return types.InputFile(
                id=len(self.uploaded_results) + 1,
                parts=1,
                name=path,
                md5_checksum="",
            )

        async def __call__(self, request: object):
            if isinstance(request, functions.messages.UploadMediaRequest):
                assert isinstance(request.media, types.InputMediaUploadedPhoto)
                result = object()
                self.upload_requests.append(request)
                self.uploaded_results.append(result)
                return result
            assert isinstance(request, functions.messages.SendMultiMediaRequest)
            self.send_request = request
            return SimpleNamespace(
                updates=[
                    types.UpdateMessageID(id=8000 + index, random_id=item.random_id)
                    for index, item in enumerate(request.multi_media)
                ]
            )

        async def get_messages(self, entity: object, *, ids: int):
            del entity
            return SimpleNamespace(id=ids)

    client = AlbumClient()

    def finalize_media(uploaded_result: object):
        index = client.uploaded_results.index(uploaded_result) + 1
        return types.InputMediaPhoto(
            id=types.InputPhoto(id=index, access_hash=index, file_reference=b"ref")
        )

    monkeypatch.setattr(utils, "get_input_media", finalize_media)
    sent = await gateway._send_album(
        client,
        types.InputPeerSelf(),
        "album caption",
        None,
        "album-idempotency",
        html_text=None,
        media_items=[("one.jpg", "photo"), ("two.jpg", "photo")],
    )

    assert len(client.upload_requests) == 2
    assert isinstance(client.send_request, functions.messages.SendMultiMediaRequest)
    assert all(
        isinstance(item.media, types.InputMediaPhoto) for item in client.send_request.multi_media
    )
    assert sent.message_id == 8001


async def test_account_unauthorized_error_auto_disables_account(tmp_path) -> None:
    app = create_app(
        settings=_settings(tmp_path, "account-disabled.db"),
        gateway=AccountUnauthorizedGateway(),
    )
    await app.state.database.create_schema()
    campaign_id, account_id = await _create_campaign_with_authorized_account(app)

    await app.state.run_queue.enqueue(campaign_id, "account-unauthorized-001")
    result = await app.state.queue_worker.process_next()
    assert result is not None
    assert result.status.value == "failed"
    assert result.error_code == "account_unauthorized"

    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        assert account.status == AccountStatus.DISABLED
        assert account.authorized is False
        assert account.last_error is not None

    await app.state.database.dispose()


async def test_chat_ban_is_saved_on_target_without_disabling_account(tmp_path) -> None:
    app = create_app(
        settings=_settings(tmp_path, "chat-ban.db"),
        gateway=AccountBannedInChatGateway(),
    )
    await app.state.database.create_schema()
    campaign_id, account_id = await _create_campaign_with_authorized_account(app)

    await app.state.run_queue.enqueue(campaign_id, "chat-ban-001")
    result = await app.state.queue_worker.process_next()
    assert result is not None
    assert result.status.value == "failed"
    assert result.error_code == "account_banned_in_chat"

    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        target = await session.scalar(select(Target).where(Target.chat_ref == "@safety_target"))
        assert account is not None
        assert account.status == AccountStatus.ACTIVE
        assert target is not None
        assert target.last_error_code == "account_banned_in_chat"
        assert target.last_error_account_id == account_id
        assert target.last_checked_at is not None

    await app.state.database.dispose()


def test_live_sender_requires_complete_imported_session() -> None:
    account = Account(
        label="incomplete-session",
        status=AccountStatus.ACTIVE,
        ownership_confirmed=True,
        authorized=True,
        session_encrypted="encrypted-session",
    )
    with pytest.raises(ConflictError, match="API credentials"):
        CampaignRunner._validate_sender(account, require_live_session=True)


async def _create_campaign_with_authorized_account(app) -> tuple[int, int]:
    async with app.state.database.session_factory() as session:
        primary = Account(
            label="safety-primary",
            telegram_user_id=1701,
            ownership_confirmed=True,
            authorized=True,
        )
        reply = Account(
            label="safety-reply",
            telegram_user_id=1702,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@safety_target",
            title="Safety target",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all([primary, reply, target])
        await session.flush()
        campaign = Campaign(
            name="safety-campaign",
            primary_account_id=primary.id,
            reply_account_id=reply.id,
            target_id=target.id,
            primary_text="First",
            reply_text="Reply",
            reply_delay_seconds=0,
        )
        session.add(campaign)
        await session.commit()
        return campaign.id, primary.id
