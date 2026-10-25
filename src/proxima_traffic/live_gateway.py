"""Live Telegram gateway that sends via imported personal accounts (Telethon).

Every account is pinned to its assigned proxy slot (``account:<id>``). When
``account_require_proxy`` is enabled the gateway is *fail-closed*: it refuses to
send over a direct connection if the proxy route is missing or unhealthy, which
protects the account's real IP.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import re
import socket
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import insert, select
from sqlalchemy import update as sql_update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.account_leases import AccountOperationLeaseCoordinator
from proxima_traffic.accounts import (
    AccountImportError,
    AccountService,
    TelethonTransport,
    build_telethon_transport,
)
from proxima_traffic.config import Settings
from proxima_traffic.content import (
    ContentError,
    StoredContact,
    parse_stored_contact,
    render_telegram_html,
    resolve_media_path,
)
from proxima_traffic.gateways import (
    ORDINARY_REACTION_EMOJI,
    ChannelCommentDelivery,
    ChannelCommentPreflight,
    ChannelPost,
    ChannelPostScan,
    ChatActivity,
    ChatBlacklistScan,
    DirectMessageDelivery,
    DirectRecipient,
    DirectRecipientCheck,
    DirectServiceActionResult,
    GatewayError,
    MembershipGateRecovery,
    MessageGateway,
    ParticipantMessage,
    ParticipantMessageScan,
    PermanentGatewayError,
    SentMessage,
    StoryDelivery,
    StoryViewResult,
    SubscriberInviteDelivery,
    SubscriberInviteDestination,
    TargetReadiness,
    TemporaryGatewayError,
    reaction_candidates,
)
from proxima_traffic.membership_gate import InlineButton, parse_membership_gate
from proxima_traffic.models import (
    Account,
    MembershipStatus,
    MentionBlacklistEntry,
    MentionBlacklistUsernameEntry,
    Subscriber,
    Target,
    TargetAccountState,
    TelegramDeliveryReceipt,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import ProxyRouteUnavailable, ProxyService
from proxima_traffic.spambot import SpamBotResult, parse_spambot_response

logger = logging.getLogger(__name__)


class TelethonGateway:
    """Sends real messages using encrypted, proxy-routed account sessions."""

    supports_channel_comment_dispatch_hook = True
    supports_campaign_dispatch_hook = True
    simulated = False

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        account_service: AccountService | None = None,
        proxy_service: ProxyService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._proxy_service = proxy_service or ProxyService(session_factory, settings)
        self._account_service = account_service or AccountService(
            session_factory, settings, self._proxy_service
        )
        self._account_service.bind_client_coordinator(self)
        self._clients: dict[int, object] = {}
        self._client_generations: dict[int, int] = {}
        self._entity_cache: dict[tuple[int, str], object] = {}
        self._joined: set[tuple[int, str]] = set()
        self._locks: dict[int, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        self._account_leases = AccountOperationLeaseCoordinator(
            session_factory,
            owner_id=f"{socket.gethostname()[:40]}:{os.getpid()}:{uuid4().hex}",
            lease_seconds=max(
                float(settings.worker_lease_seconds),
                float(settings.worker_step_timeout_seconds)
                + float(settings.worker_heartbeat_seconds),
            ),
            heartbeat_seconds=float(settings.worker_heartbeat_seconds),
        )

    async def check_spambot_status(self, *, account: Account) -> SpamBotResult:
        """Ask the official @SpamBot for this account's current restriction state.

        This runs under the same per-account lock as sending, so a shared
        Telethon session never receives concurrent requests.  Inline buttons
        are intentionally not clicked: the status is read from the bot's text
        reply only and no appeal/workflow action is automated.
        """
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            client: object | None = None
            cached_before_check = account.id in self._clients
            received_response = False
            try:
                client = await self._ensure_client(account.id)
                timeout = max(1.0, float(getattr(self._settings, "spambot_timeout_seconds", 15.0)))
                async with client.conversation(  # type: ignore[attr-defined]
                    "@SpamBot",
                    timeout=timeout,
                    exclusive=False,
                ) as conversation:
                    await conversation.send_message("/start")
                    response = await conversation.get_response()
                received_response = True
            except TimeoutError as exc:
                raise TemporaryGatewayError(
                    "@SpamBot did not answer before the timeout",
                    retry_after=60,
                    code="spambot_unavailable",
                ) from exc
            except Exception as exc:  # noqa: BLE001 - normalize RPC failures for the worker
                mapped = self._map_rpc_error(exc, context="spambot")
                if isinstance(mapped, Exception):
                    raise mapped from exc
                raise
            finally:
                if client is not None and (not cached_before_check or not received_response):
                    await self._disconnect_cached_client_locked(account.id, client)
            raw_text = (
                getattr(response, "raw_text", None) or getattr(response, "message", None) or ""
            )
            button_rows = tuple(
                tuple(
                    str(getattr(button, "text", "") or "")[:200]
                    for button in (getattr(row, "buttons", None) or ())
                )
                for row in (getattr(getattr(response, "reply_markup", None), "rows", None) or ())
            )
            format_entities = tuple(
                (
                    type(entity).__name__,
                    int(getattr(entity, "offset", 0) or 0),
                    int(getattr(entity, "length", 0) or 0),
                )
                for entity in (getattr(response, "entities", None) or ())
            )
            return parse_spambot_response(
                str(raw_text),
                button_rows=button_rows,
                format_entities=format_entities,
            )

    async def _disconnect_cached_client_locked(self, account_id: int, client: object) -> None:
        """Discard one client while the caller already owns its account lock."""
        async with self._global_lock:
            if self._clients.get(account_id) is client:
                self._clients.pop(account_id, None)
                self._client_generations.pop(account_id, None)
                self._entity_cache = {
                    key: value for key, value in self._entity_cache.items() if key[0] != account_id
                }
                self._joined = {key for key in self._joined if key[0] != account_id}
        try:
            await client.disconnect()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - connection cleanup must not hide the check result
            logger.debug("@SpamBot client disconnect failed", exc_info=True)

    async def invalidate_target_membership(self, *, account_id: int, target: Target) -> bool:
        """Forget one cached membership observation after Telegram contradicts it.

        This performs no Telegram action.  The next normal delivery will
        resolve the destination again and, when configured, use the existing
        rate-limited auto-join path.  Returning whether the cache was present
        lets the runner limit this recovery to one stale-cache retry.
        """

        cache_key = (account_id, target.chat_ref)
        async with self._global_lock:
            had_cached_membership = cache_key in self._joined
            self._entity_cache.pop(cache_key, None)
            self._joined.discard(cache_key)
        return had_cached_membership

    async def inspect_target(
        self,
        *,
        account: Account,
        target: Target,
        media_kinds: frozenset[str] = frozenset(),
    ) -> TargetReadiness:
        """Inspect membership, write access and timing without joining or sending."""
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        checked_at = utc_now()
        telegram_title: str | None = None
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_preflight_entity(
                    client,
                    account.id,
                    target,
                )
                candidate_title = str(getattr(entity, "title", "") or "").strip()
                telegram_title = candidate_title[:150] or None
                self._ensure_can_send(entity, media_kinds=media_kinds)
                try:
                    slowmode_seconds = await self._respect_chat_slow_mode(
                        client,
                        account.id,
                        target.id,
                        entity,
                        known_timer_checked=True,
                    )
                except TemporaryGatewayError as exc:
                    if exc.code != "slow_mode":
                        raise
                    slowmode_seconds, next_send_at = await self._known_target_timing(
                        account.id,
                        target.id,
                    )
                else:
                    _, next_send_at = await self._known_target_timing(
                        account.id,
                        target.id,
                    )
                self._entity_cache[(account.id, target.chat_ref)] = entity
                self._joined.add((account.id, target.chat_ref))
                return TargetReadiness(
                    membership_status=MembershipStatus.MEMBER,
                    can_send=True,
                    checked_at=checked_at,
                    slowmode_seconds=slowmode_seconds,
                    next_send_at=next_send_at,
                    media_kinds=media_kinds,
                    telegram_title=telegram_title,
                )
            except GatewayError as exc:
                code = str(getattr(exc, "code", None) or "gateway_error")
                status = {
                    "target_not_joined": MembershipStatus.NOT_MEMBER,
                    "join_request_pending": MembershipStatus.JOIN_PENDING,
                    "account_banned_in_chat": MembershipStatus.BANNED,
                    "target_unavailable": MembershipStatus.UNAVAILABLE,
                    "account_join_limit": MembershipStatus.UNAVAILABLE,
                }.get(code, MembershipStatus.UNKNOWN)
                return TargetReadiness(
                    membership_status=status,
                    can_send=False,
                    checked_at=checked_at,
                    media_kinds=media_kinds,
                    error_code=code,
                    error=str(getattr(exc, "safe_message", None) or exc)[:500],
                    telegram_title=telegram_title,
                )

    async def prepare_broadcast_target(self, *, account: Account, target: Target) -> bool:
        """Prepare opted-in membership before the campaign's read-only activity check."""
        if not self._settings.account_auto_join:
            return False
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            entity, joined = await self._join_and_resolve(
                client, account.id, target, report_join=True
            )
            self._ensure_can_send(entity)
            return joined

    async def inspect_activity(
        self,
        *,
        account: Account,
        target: Target,
        last_message_id: int,
    ) -> ChatActivity:
        """Read real messages newer than the anchor without marking the chat read."""
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            entity = await self._resolve_preflight_entity(client, account.id, target)
            self._ensure_can_send(entity)
            try:
                recent = await client.get_messages(  # type: ignore[attr-defined]
                    entity,
                    limit=100,
                    min_id=last_message_id,
                )
                anchor = await client.get_messages(  # type: ignore[attr-defined]
                    entity,
                    ids=last_message_id,
                )
            except Exception as exc:  # noqa: BLE001
                mapped = self._map_rpc_error(exc, context="activity")
                raise mapped from exc
            messages = list(recent) if recent is not None else []
            if not self._message_is_present(anchor, last_message_id):
                raise self._deleted_post_error(
                    last_message_id,
                    notice=self._moderation_notice(messages, after_message_id=last_message_id),
                )
            message_ids = tuple(
                sorted(
                    int(message.id)
                    for message in messages
                    if (
                        getattr(message, "id", None) is not None
                        and int(message.id) > last_message_id
                        and getattr(message, "action", None) is None
                    )
                )
            )
            latest_id = max(message_ids, default=last_message_id)
            return ChatActivity(
                latest_message_id=latest_id,
                checked_at=utc_now(),
                observed_message_ids=message_ids,
            )

    async def verify_sent_message(
        self,
        *,
        account: Account,
        target: Target,
        message_id: int,
    ) -> None:
        """Confirm one prior delivery without doing an expensive activity scan."""
        if account.id is None or target.id is None or message_id <= 0:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_preflight_entity(client, account.id, target)
                fetched = await self._fetch_message_for_verification(client, entity, message_id)
                if not self._message_is_present(fetched, message_id):
                    notice: str | None = None
                    try:
                        recent = await asyncio.wait_for(
                            client.get_messages(entity, limit=12),
                            timeout=float(self._settings.account_delivery_verify_timeout_seconds),
                        )
                        notice = self._moderation_notice(
                            list(recent) if recent is not None else [],
                            after_message_id=message_id,
                        )
                    except Exception:  # noqa: BLE001 - absence is already confirmed
                        pass
                    raise self._deleted_post_error(message_id, notice=notice)
                # Check permission after the read, so a deletion still wins if
                # moderation also blocks the sender shortly afterwards.
                self._ensure_can_send(entity)
            except GatewayError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalize durable monitor errors
                raise self._map_rpc_error(exc, context="verify") from exc

    async def verify_channel_comment(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        message_id: int,
        discussion_peer_id: int | None,
        discussion_access_hash: int | None,
    ) -> None:
        """Read the original discussion; never join, recover a gate or send again."""
        from telethon import types, utils

        del channel_ref, channel_post_id
        if account.id is None or message_id <= 0 or discussion_peer_id is None:
            raise PermanentGatewayError(
                "Не сохранён адрес исходного обсуждения; отсутствие сообщения не подтверждено",
                code="comment_verification_unavailable",
            )
        raw_id, peer_type = utils.resolve_id(discussion_peer_id)
        if peer_type is types.PeerChannel and discussion_access_hash is not None:
            peer = types.InputPeerChannel(raw_id, discussion_access_hash)
        elif peer_type is types.PeerChat:
            peer = types.InputPeerChat(raw_id)
        else:
            raise PermanentGatewayError(
                "Не сохранены данные доступа к исходному обсуждению",
                code="comment_verification_unavailable",
            )
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await client.get_entity(peer)
                if isinstance(
                    entity, (types.ChannelForbidden, types.ChatForbidden, types.ChatEmpty)
                ):
                    raise PermanentGatewayError(
                        "Исходное обсуждение недоступно для проверки",
                        code="comment_verification_unavailable",
                    )
                if utils.get_peer_id(entity) != discussion_peer_id:
                    raise PermanentGatewayError(
                        "Telegram вернул другое обсуждение; результат проверки неизвестен",
                        code="comment_verification_unavailable",
                    )
                for attempt in range(2):
                    # Do not use the immediate-delivery helper: it can normalize
                    # an inconclusive RPC response into None, which is not proof
                    # that a previously confirmed comment has disappeared.
                    fetched = await asyncio.wait_for(
                        client.get_messages(entity, ids=message_id),
                        timeout=float(self._settings.account_delivery_verify_timeout_seconds),
                    )
                    if self._message_is_present(fetched, message_id):
                        return
                    if fetched is not None and not isinstance(fetched, types.MessageEmpty):
                        raise TemporaryGatewayError(
                            "Telegram не вернул однозначный результат проверки комментария",
                            code="delivery_unconfirmed",
                        )
                    if attempt == 0:
                        await asyncio.sleep(
                            float(self._settings.account_delivery_verify_confirm_delay_seconds)
                        )
                raise PermanentGatewayError(
                    "Отправленный комментарий отсутствует при двух чтениях доступного обсуждения; "
                    "причина удаления неизвестна",
                    code="comment_missing",
                )
            except GatewayError:
                raise
            except Exception as exc:  # noqa: BLE001 - preserve unavailable versus missing
                raise self._map_rpc_error(exc, context="verify") from exc

    @staticmethod
    def _channel_comment_delivery(
        message_id: int, root_message_id: int, discussion_entity: object
    ) -> ChannelCommentDelivery:
        from telethon import utils

        peer = utils.get_input_peer(discussion_entity)
        return ChannelCommentDelivery(
            message_id=message_id,
            thread_root_message_id=root_message_id,
            discussion_peer_id=utils.get_peer_id(peer),
            discussion_access_hash=getattr(peer, "access_hash", None),
        )

    async def recover_membership_gate(
        self,
        *,
        account: Account,
        target: Target,
        deleted_message_id: int,
    ) -> MembershipGateRecovery:
        """Satisfy one explicit subscription-only gate after a removed post.

        This deliberately accepts only Telegram channel links found in the
        moderator bot's inline keyboard.  Web apps, login links, captchas and
        arbitrary callbacks are never opened or clicked.
        """

        if not self._settings.membership_gate_recovery_enabled:
            return MembershipGateRecovery(detected=False, recovered=False)
        if account.id is None or target.id is None or deleted_message_id <= 0:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_entity(client, account.id, target.chat_ref)
                return await self._recover_membership_gate_for_entity(
                    client,
                    account.id,
                    entity,
                    deleted_message_id,
                )
            except Exception as exc:  # noqa: BLE001 - preserve original post-deletion context
                mapped = self._map_rpc_error(exc, context="membership_gate")
                return MembershipGateRecovery(
                    detected=False,
                    recovered=False,
                    error=self._safe_gateway_error(mapped),
                )

    async def recover_channel_comment_membership_gate(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        deleted_message_id: int,
    ) -> MembershipGateRecovery:
        """Recover a subscription gate in the discussion group of a channel post."""

        if not self._settings.membership_gate_recovery_enabled:
            return MembershipGateRecovery(detected=False, recovered=False)
        if account.id is None or channel_post_id <= 0 or deleted_message_id <= 0:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                channel = await self._resolve_channel_comment_channel(
                    client,
                    account.id,
                    channel_ref,
                    request_join=True,
                )
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "comments can only be attached to channel posts",
                        code="channel_required",
                    )
                discussion = await self._discussion_for_post(client, channel, channel_post_id)
                discussion_entity, _ = await self._discussion_entity(client, discussion)
                return await self._recover_membership_gate_for_entity(
                    client,
                    account.id,
                    discussion_entity,
                    deleted_message_id,
                )
            except Exception as exc:  # noqa: BLE001 - preserve the deleted-comment context
                mapped = self._map_rpc_error(exc, context="channel_comment_membership_gate")
                return MembershipGateRecovery(
                    detected=False,
                    recovered=False,
                    error=self._safe_gateway_error(mapped),
                )

    async def scan_channel_posts(
        self,
        *,
        account: Account,
        channel_ref: str,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
        after_date: datetime | None = None,
    ) -> ChannelPostScan:
        """Read a manually selected channel without subscribing to update streams.

        A new rule reads forward from its durable arming timestamp. Once its
        cursor is established, reads move forward from that cursor so a busy
        channel does not lose posts created before the first successful scan.
        """
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                # Setup is the only comment operation allowed to submit a
                # private-channel join request.  Polling merely observes its
                # approval, preventing a repeated request every scan interval.
                channel = await self._resolve_channel_comment_channel(
                    client,
                    account.id,
                    channel_ref,
                    request_join=False,
                )
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "нужен канал с постами, а не группа",
                        code="channel_required",
                    )
                # On bootstrap, the arming timestamp avoids the old archive.
                # Subsequent scans read forward from the saved cursor.
                scan_kwargs: dict[str, object] = {}
                if bootstrap and after_date is not None:
                    # The rule's armed timestamp is a safe lower bound. Reading
                    # forward from it preserves every post published before the
                    # first successful scan without walking the channel archive.
                    scan_kwargs.update(
                        limit=max(1, min(limit, 100)),
                        offset_date=after_date,
                        reverse=True,
                    )
                elif bootstrap:
                    # Backwards-compatible fallback for callers that do not
                    # possess a durable arming timestamp.
                    scan_kwargs["limit"] = 1
                else:
                    scan_kwargs.update(
                        limit=max(1, min(limit, 100)),
                        min_id=max(0, after_message_id),
                        reverse=True,
                    )
                messages = [
                    message
                    async for message in client.iter_messages(  # type: ignore[attr-defined]
                        channel,
                        **scan_kwargs,
                    )
                ]
            except Exception as exc:  # noqa: BLE001 - normalized for durable retries
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="channel_scan") from exc
            posts: list[ChannelPost] = []
            seen_albums: set[int] = set()
            for message in messages:
                message_id = getattr(message, "id", None)
                if (
                    message_id is None
                    or int(message_id) <= after_message_id
                    or getattr(message, "action", None) is not None
                ):
                    continue
                grouped_id = getattr(message, "grouped_id", None)
                if isinstance(grouped_id, int) and grouped_id > 0:
                    # Telegram exposes every item in a media album as a separate
                    # message.  An album has one comment thread, anchored to its
                    # first item when the forward scan is ordered ascending.
                    # Processing its remaining media IDs created duplicate report
                    # rows that could not have their own discussion thread.
                    if grouped_id in seen_albums:
                        continue
                    seen_albums.add(grouped_id)
                posts.append(
                    ChannelPost(
                        message_id=int(message_id),
                        published_at=self._as_utc(message.date),
                        grouped_id=grouped_id,
                    )
                )
            return ChannelPostScan(
                title=str(getattr(channel, "title", "") or "") or None,
                posts=tuple(posts),
                last_seen_message_id=max(
                    (int(message.id) for message in messages if getattr(message, "id", None)),
                    default=after_message_id,
                ),
                last_seen_grouped_id=getattr(
                    max(
                        messages,
                        key=lambda message: int(getattr(message, "id", 0) or 0),
                        default=None,
                    ),
                    "grouped_id",
                    None,
                ),
            )

    async def preflight_channel_comments(
        self,
        *,
        account: Account,
        channel_ref: str,
    ) -> ChannelCommentPreflight:
        """Validate the channel, linked discussion and sender rights without posting."""

        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                channel = await self._resolve_channel_comment_channel(
                    client,
                    account.id,
                    channel_ref,
                    request_join=True,
                )
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "нужен канал с постами, а не группа",
                        code="channel_required",
                    )
                newest_post = None
                discussion_entity = None
                root_message_id = 0
                async for message in client.iter_messages(channel, limit=20):
                    if (
                        getattr(message, "id", None) is None
                        or getattr(message, "action", None) is not None
                    ):
                        continue
                    newest_post = message
                    try:
                        discussion = await self._discussion_for_post(
                            client, channel, int(message.id)
                        )
                        discussion_entity, root_message_id = await self._discussion_entity(
                            client, discussion
                        )
                    except Exception as exc:  # noqa: BLE001 - some individual posts have closed comments
                        mapped = (
                            exc
                            if isinstance(exc, GatewayError)
                            else self._map_rpc_error(exc, context="channel_comment_preflight")
                        )
                        if mapped.code == "comments_unavailable":
                            continue
                        if mapped is exc:
                            raise
                        raise mapped from exc
                    break
                if newest_post is None:
                    return ChannelCommentPreflight(
                        title=str(getattr(channel, "title", "") or "") or None,
                        channel_post_id=0,
                        discussion_root_message_id=0,
                    )
                if discussion_entity is None:
                    raise PermanentGatewayError(
                        "У последних 20 публикаций нет доступной ветки комментариев",
                        code="comments_unavailable",
                    )
                await self._ensure_discussion_membership(
                    client,
                    account.id,
                    discussion_entity,
                    join_if_needed=False,
                )
                self._ensure_can_send(discussion_entity)
            except Exception as exc:  # noqa: BLE001 - normalized for the control bot
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="channel_comment_preflight") from exc
            return ChannelCommentPreflight(
                title=str(getattr(channel, "title", "") or "") or None,
                channel_post_id=int(newest_post.id),
                discussion_root_message_id=root_message_id,
            )

    async def send_channel_comment(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        text: str,
        idempotency_key: str,
        reply_to_message_id: int | None = None,
        html_text: str | None = None,
        media_path: str | None = None,
        media_kind: str | None = None,
        media_name: str | None = None,
        before_send: Callable[[], Awaitable[None]] | None = None,
        expected_discussion_peer_id: int | None = None,
        expected_thread_root_message_id: int | None = None,
    ) -> ChannelCommentDelivery:
        """Post into the discussion thread of one channel post using Telegram's public RPC."""
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        del media_name
        media_without_caption = media_path is not None
        try:
            rendered_text = (
                render_telegram_html(html_text) if html_text is not None else text.strip()
            )
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="comment_invalid") from exc
        if rendered_text != text.strip():
            raise PermanentGatewayError(
                "форматированный текст комментария не совпадает с исходным",
                code="comment_invalid",
            )
        if media_path is not None and media_kind not in {
            "photo",
            "video",
            "document",
            "voice",
            "video_note",
        }:
            raise PermanentGatewayError("unsupported comment media kind", code="comment_invalid")
        if media_kind == "video_note" and (text or html_text):
            raise PermanentGatewayError(
                "video notes cannot contain a caption", code="comment_invalid"
            )
        maximum_length = 1024 if media_path is not None else 4096
        if channel_post_id <= 0 or (
            not 1 <= len(rendered_text) <= maximum_length
            and not (media_without_caption and not rendered_text)
        ):
            raise PermanentGatewayError(
                f"Комментарий должен содержать от 1 до {maximum_length} символов",
                code="comment_invalid",
            )
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                channel = await self._resolve_entity(client, account.id, channel_ref)
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "комментарии можно привязать только к постам канала",
                        code="channel_required",
                    )
                discussion = await self._discussion_for_post(client, channel, channel_post_id)
                discussion_entity, root_message_id = await self._discussion_entity(
                    client,
                    discussion,
                )
                self._ensure_comment_thread(
                    discussion_entity,
                    root_message_id,
                    expected_discussion_peer_id,
                    expected_thread_root_message_id,
                )
                await self._ensure_discussion_membership(
                    client,
                    account.id,
                    discussion_entity,
                    join_if_needed=True,
                )
                self._ensure_can_send(
                    discussion_entity,
                    media_kinds=frozenset({media_kind}) if media_kind else frozenset(),
                )
                await self._show_typing_before_send(client, discussion_entity, text=rendered_text)
                if media_path is None:
                    sent = await self._send(
                        client,
                        discussion_entity,
                        text.strip(),
                        reply_to_message_id or root_message_id,
                        idempotency_key,
                        html_text=html_text,
                        error_context="channel_comment",
                        before_send=before_send,
                    )
                else:
                    try:
                        resolved_media = str(resolve_media_path(self._settings, media_path))
                    except ContentError as exc:
                        raise PermanentGatewayError(str(exc), code="media_unavailable") from exc
                    sent = await self._send_media(
                        client,
                        discussion_entity,
                        text.strip(),
                        reply_to_message_id or root_message_id,
                        idempotency_key,
                        html_text=html_text,
                        media_path=resolved_media,
                        media_kind=str(media_kind),
                        error_context="channel_comment",
                        before_send=before_send,
                    )
            except Exception as exc:  # noqa: BLE001 - normalized for durable retries
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="channel_comment") from exc
            return self._channel_comment_delivery(
                sent.message_id, root_message_id, discussion_entity
            )

    async def collect_mentionable_user_ids(
        self,
        *,
        account: Account,
        target: Target,
        limit: int,
    ) -> tuple[int, ...]:
        """Read eligible members of the exact destination used for a post."""

        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                if self._settings.account_auto_join:
                    entity = await self._join_and_resolve(client, account.id, target)
                elif target.folder_slug and target.telegram_peer_id is not None:
                    entity, _ = await self._resolve_folder_target(
                        client,
                        account.id,
                        target,
                        allow_join=False,
                    )
                else:
                    entity = await self._resolve_entity(client, account.id, target.chat_ref)
                return await self._collect_mentionable_user_ids_from_entity(
                    client,
                    entity,
                    exclude_user_id=account.telegram_user_id,
                    limit=limit,
                )
            except Exception as exc:  # noqa: BLE001 - caller may safely send without mentions
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="participant_collect") from exc

    async def collect_channel_comment_mentionable_user_ids(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        limit: int,
    ) -> tuple[int, ...]:
        """Read members of the discussion group behind one channel post."""

        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if channel_post_id <= 0:
            raise PermanentGatewayError("channel post identifier is invalid")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                channel = await self._resolve_entity(client, account.id, channel_ref)
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "comments can only be attached to channel posts",
                        code="channel_required",
                    )
                discussion = await self._discussion_for_post(client, channel, channel_post_id)
                discussion_entity, _ = await self._discussion_entity(client, discussion)
                await self._ensure_discussion_membership(
                    client,
                    account.id,
                    discussion_entity,
                    join_if_needed=True,
                )
                return await self._collect_mentionable_user_ids_from_entity(
                    client,
                    discussion_entity,
                    exclude_user_id=account.telegram_user_id,
                    limit=limit,
                )
            except Exception as exc:  # noqa: BLE001 - caller may safely send without mentions
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="comment_participant_collect") from exc

    async def edit_channel_comment(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        message_id: int,
        text: str,
        idempotency_key: str,
        html_text: str | None = None,
        before_send: Callable[[], Awaitable[None]] | None = None,
        expected_discussion_peer_id: int | None = None,
        expected_thread_root_message_id: int | None = None,
    ) -> ChannelCommentDelivery:
        """Edit one previously sent comment in the matching discussion thread."""
        from telethon import errors, functions, utils  # noqa: PLC0415

        del idempotency_key
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        try:
            rendered_text = render_telegram_html(html_text) if html_text is not None else text
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="comment_edit_invalid") from exc
        if channel_post_id <= 0 or message_id <= 0 or not 1 <= len(text.strip()) <= 4096:
            raise PermanentGatewayError(
                "текст или идентификатор редактируемого комментария некорректен",
                code="comment_edit_invalid",
            )
        if rendered_text != text.strip():
            raise PermanentGatewayError(
                "форматированный текст комментария не совпадает с исходным",
                code="comment_edit_invalid",
            )
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            try:
                channel = await self._resolve_entity(client, account.id, channel_ref)
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "редактировать можно только комментарий к посту канала",
                        code="channel_required",
                    )
                discussion = await self._discussion_for_post(client, channel, channel_post_id)
                discussion_entity, root_message_id = await self._discussion_entity(
                    client,
                    discussion,
                )
                self._ensure_comment_thread(
                    discussion_entity,
                    root_message_id,
                    expected_discussion_peer_id,
                    expected_thread_root_message_id,
                )
                try:
                    message_text = text.strip()
                    entities = None
                    if html_text is not None:
                        message_text, entities = await self._parse_html_message(client, html_text)
                    if before_send is not None:
                        await before_send()
                    await client(
                        functions.messages.EditMessageRequest(
                            peer=utils.get_input_peer(discussion_entity),
                            id=message_id,
                            message=message_text,
                            entities=entities,
                            no_webpage=True,
                        )
                    )
                except errors.MessageNotModifiedError:
                    # The previous RPC may have completed just before a worker
                    # crash. Telegram reports the retry as unchanged; that is
                    # the desired durable end state.
                    pass
                await self._verify_delivery(client, discussion_entity, message_id)
            except Exception as exc:  # noqa: BLE001 - normalized for durable retries
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="channel_comment_edit") from exc
            return self._channel_comment_delivery(message_id, root_message_id, discussion_entity)

    async def send_message(
        self,
        *,
        account: Account,
        target: Target,
        text: str,
        idempotency_key: str,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        return await self.send_content(
            account=account,
            target=target,
            text=text,
            idempotency_key=idempotency_key,
            reply_to_message_id=reply_to_message_id,
        )

    async def send_reaction(
        self,
        *,
        account: Account,
        target: Target,
        message_id: int,
        emoji: str,
    ) -> str:
        """Apply an ordinary reaction, using a safe fallback only when Telegram rejects it."""
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                # Reactions are Telegram actions, not local markers. Let a
                # configured auto-join use the same guarded membership path as
                # the publication; otherwise fresh accounts were excluded from
                # every reaction before they had a chance to enter the chat.
                entity = (
                    await self._join_and_resolve(client, account.id, target)
                    if self._settings.account_auto_join
                    else await self._resolve_entity(client, account.id, target.chat_ref)
                )
                return await self._send_reaction_with_fallback(client, entity, message_id, emoji)
            except Exception as exc:  # noqa: BLE001 - map RPC errors for the durable worker
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="reaction") from exc

    async def send_channel_comment_reaction(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        message_id: int,
        emoji: str,
        expected_discussion_peer_id: int | None = None,
        expected_thread_root_message_id: int | None = None,
    ) -> str:
        """React to a comment through the discussion peer linked to its channel post."""

        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if channel_post_id <= 0 or message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                channel = await self._resolve_entity(client, account.id, channel_ref)
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "reactions can only be attached to channel comments",
                        code="channel_required",
                    )
                discussion = await self._discussion_for_post(client, channel, channel_post_id)
                discussion_entity, root_message_id = await self._discussion_entity(
                    client, discussion
                )
                self._ensure_comment_thread(
                    discussion_entity,
                    root_message_id,
                    expected_discussion_peer_id,
                    expected_thread_root_message_id,
                )
                await self._ensure_discussion_membership(
                    client,
                    account.id,
                    discussion_entity,
                    join_if_needed=True,
                )
                return await self._send_reaction_with_fallback(
                    client,
                    discussion_entity,
                    message_id,
                    emoji,
                )
            except Exception as exc:  # noqa: BLE001 - durable worker classifies the RPC
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="reaction") from exc

    async def scan_participant_messages(
        self,
        *,
        account: Account,
        target: Target,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
    ) -> ParticipantMessageScan:
        """Read a bootstrap cursor or the next page from an approved chat."""
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_entity(client, account.id, target.chat_ref)
                scan_kwargs: dict[str, int | bool] = {
                    "limit": 1 if bootstrap else max(1, min(limit, 100))
                }
                if not bootstrap:
                    scan_kwargs.update(
                        min_id=max(0, after_message_id),
                        reverse=True,
                    )
                messages = [
                    message
                    async for message in client.iter_messages(  # type: ignore[attr-defined]
                        entity,
                        **scan_kwargs,
                    )
                ]
            except Exception as exc:  # noqa: BLE001 - mapped for the durable scanner
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="participant_reaction_scan") from exc
        return ParticipantMessageScan(
            messages=tuple(
                ParticipantMessage(
                    message_id=int(message.id),
                    sender_id=(int(message.sender_id) if message.sender_id is not None else None),
                    sent_at=ensure_utc(getattr(message, "date", None)),
                    is_bot=bool(getattr(getattr(message, "sender", None), "bot", False)),
                    is_outgoing=bool(getattr(message, "out", False)),
                )
                for message in messages
                if (
                    getattr(message, "id", None) is not None
                    and int(message.id) > after_message_id
                    and getattr(message, "action", None) is None
                )
            )
        )

    async def scan_chat_blacklist_candidates(
        self,
        *,
        account: Account,
        chat_ref: str,
        mode: str,
        limit: int,
    ) -> ChatBlacklistScan:
        """Read first; request membership only when needed and under join limits."""

        from telethon import errors  # noqa: PLC0415

        if account.id is None or mode not in {"members", "messages"}:
            raise PermanentGatewayError("chat blacklist scan context is invalid")
        bounded_limit = max(1, min(int(limit), 1_000_000))
        joined_chat = False
        join_attempted = False
        try:
            async with self._account_operation(account.id):
                client = await self._ensure_client(account.id)
                entity, joined_chat = await self._resolve_blacklist_scan_entity(
                    client, account.id, chat_ref
                )
                try:
                    collected, scanned_records = await self._collect_blacklist_scan(
                        client, entity, mode, bounded_limit
                    )
                except (errors.UserNotParticipantError, errors.ChannelPrivateError) as exc:
                    public_channel = bool(
                        getattr(entity, "broadcast", False) or getattr(entity, "megagroup", False)
                    )
                    if joined_chat or not public_channel or (
                        isinstance(exc, errors.ChannelPrivateError)
                        and not getattr(entity, "left", False)
                    ):
                        raise
                    join_attempted = True
                    joined_chat = await self._join_blacklist_scan_channel(
                        client, account.id, entity
                    )
                    collected, scanned_records = await self._collect_blacklist_scan(
                        client, entity, mode, bounded_limit
                    )
        except Exception as exc:  # noqa: BLE001 - include lease loss after a successful join
            mapped = (
                exc
                if isinstance(exc, GatewayError)
                else self._map_rpc_error(exc, context="participant_reaction_scan")
            )
            if joined_chat or join_attempted:
                mapped.join_attempted = True
            if joined_chat:
                mapped.joined_chat = True
            if mapped is exc:
                raise
            raise mapped from exc
        return ChatBlacklistScan(
            telegram_user_ids=tuple(collected),
            mode=mode,
            reached_limit=scanned_records >= bounded_limit,
            joined_chat=joined_chat,
            scan_limit=bounded_limit,
        )

    @staticmethod
    async def _collect_blacklist_scan(
        client: object, entity: object, mode: str, limit: int
    ) -> tuple[list[int], int]:
        collected: list[int] = []
        seen: set[int] = set()
        scanned = 0
        iterator = (
            client.iter_participants(entity, limit=limit)
            if mode == "members"
            else client.iter_messages(entity, limit=limit)
        )
        async for item in iterator:
            scanned += 1
            if mode == "members":
                user_id, user = getattr(item, "id", None), item
            else:
                if getattr(item, "out", False) or getattr(item, "action", None) is not None:
                    continue
                user_id, user = getattr(item, "sender_id", None), getattr(item, "sender", None)
            if not isinstance(user_id, int) or user_id <= 0:
                continue
            if any(getattr(user, flag, False) for flag in ("bot", "deleted", "self", "is_self")):
                continue
            if user_id not in seen:
                seen.add(user_id)
                collected.append(user_id)
        return collected, scanned

    async def _join_blacklist_scan_channel(
        self, client: object, account_id: int, entity: object
    ) -> bool:
        from telethon import errors, functions  # noqa: PLC0415

        await self._enforce_join_limit(account_id)
        try:
            await client(functions.channels.JoinChannelRequest(channel=entity))
        except errors.UserAlreadyParticipantError:
            return False
        except errors.InviteRequestSentError as exc:
            await self._account_service.record_join(account_id)
            mapped = PermanentGatewayError(
                "заявка на вступление отправлена; дождитесь одобрения",
                code="join_request_pending",
            )
            mapped.join_attempted = True
            raise mapped from exc
        except Exception as exc:
            mapped = (
                exc
                if isinstance(exc, GatewayError)
                else self._map_rpc_error(exc, context="resolve")
            )
            mapped.join_attempted = True
            if mapped is exc:
                raise
            raise mapped from exc
        await self._account_service.record_join(account_id)
        return True

    async def _resolve_blacklist_scan_entity(
        self, client: object, account_id: int, chat_ref: str
    ) -> tuple[object, bool]:
        """Resolve a chat and, with operator authorization, join it before read-only scan."""

        from telethon import errors, functions, types  # noqa: PLC0415

        normalized = chat_ref.strip()
        private_prefixes = ("https://t.me/+", "https://t.me/joinchat/")
        if normalized.casefold().startswith(private_prefixes):
            invite_hash = normalized.rsplit("/", 1)[-1].lstrip("+")
            join_attempted = False
            joined_chat = False
            try:
                invite = await client(  # type: ignore[operator]
                    functions.messages.CheckChatInviteRequest(invite_hash)
                )
                if isinstance(invite, types.ChatInviteAlready) and getattr(invite, "chat", None):
                    return invite.chat, False
                await self._enforce_join_limit(account_id)
                join_attempted = True
                updates = await client(  # type: ignore[operator]
                    functions.messages.ImportChatInviteRequest(invite_hash)
                )
                joined_chat = True
                await self._account_service.record_join(account_id)
                chats = list(getattr(updates, "chats", None) or [])
                if not chats:
                    raise PermanentGatewayError(
                        "Telegram не вернул чат после вступления по приватной ссылке",
                        code="target_unavailable",
                    )
                return await client.get_entity(chats[0]), True  # type: ignore[attr-defined]
            except errors.InviteRequestSentError as exc:
                await self._account_service.record_join(account_id)
                mapped = PermanentGatewayError(
                    "заявка на вступление отправлена; дождитесь одобрения и запустите "
                    "парсинг снова",
                    code="join_request_pending",
                )
                mapped.join_attempted = True
                raise mapped from exc
            except Exception as exc:  # noqa: BLE001 - preserve durable Telegram errors
                mapped = (
                    exc
                    if isinstance(exc, GatewayError)
                    else self._map_rpc_error(exc, context="resolve")
                )
                if join_attempted:
                    mapped.join_attempted = True
                if joined_chat:
                    mapped.joined_chat = True
                if mapped is exc:
                    raise
                raise mapped from exc
        entity = await self._resolve_entity(client, account_id, normalized)
        return entity, False

    async def send_content(
        self,
        *,
        account: Account,
        target: Target,
        text: str,
        idempotency_key: str,
        reply_to_message_id: int | None = None,
        html_text: str | None = None,
        media_path: str | None = None,
        media_kind: str | None = None,
        media_name: str | None = None,
        media_items: list[dict[str, object]] | None = None,
        contact: dict[str, str] | None = None,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> SentMessage:
        del media_name
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        stored_contact: StoredContact | None = None
        if contact is not None:
            try:
                stored_contact = parse_stored_contact(contact)
            except ContentError as exc:
                raise PermanentGatewayError(str(exc), code="contact_invalid") from exc
            if text or html_text or media_path is not None or media_items:
                raise PermanentGatewayError(
                    "contact must be sent without text or media",
                    code="contact_invalid",
                )
        raw_media_items = list(media_items or [])
        if not raw_media_items and media_path is not None:
            raw_media_items = [{"path": media_path, "kind": media_kind}]
        if len(raw_media_items) > 10:
            raise PermanentGatewayError("media album contains more than 10 items")
        single_media_kind = (
            str(raw_media_items[0].get("kind") or "") if len(raw_media_items) == 1 else None
        )
        maximum_text = 1024 if raw_media_items else 4096
        rendered_text = ""
        if stored_contact is None:
            try:
                rendered_text = render_telegram_html(html_text) if html_text is not None else text
            except ContentError as exc:
                raise PermanentGatewayError(str(exc), code="content_invalid") from exc
            media_without_caption = single_media_kind is not None
            if (not rendered_text and not media_without_caption) or len(
                rendered_text
            ) > maximum_text:
                raise PermanentGatewayError("message text must contain 1 to 4096 characters")
            if single_media_kind == "video_note" and (text or html_text):
                raise PermanentGatewayError(
                    "video notes cannot contain a caption", code="content_invalid"
                )
        resolved_media_items: list[tuple[str, str]] = []
        for item in raw_media_items:
            item_path = str(item.get("path") or "")
            item_kind = str(item.get("kind") or "")
            if item_kind not in {"photo", "video", "document", "voice", "video_note"}:
                raise PermanentGatewayError("unsupported media kind")
            try:
                resolved = resolve_media_path(self._settings, item_path)
            except ContentError as exc:
                raise PermanentGatewayError(str(exc), code="media_unavailable") from exc
            resolved_media_items.append((str(resolved), item_kind))
        if len(resolved_media_items) > 1 and any(
            kind in {"document", "voice", "video_note"} for _, kind in resolved_media_items
        ):
            raise PermanentGatewayError(
                "documents, voices and video notes can only be sent as a single file"
            )

        async with self._account_operation(account.id):
            await self._respect_known_chat_timer(account.id, target.id)
            client = await self._ensure_client(account.id)
            if self._settings.account_auto_join:
                entity = await self._join_and_resolve(client, account.id, target)
            elif target.folder_slug and target.telegram_peer_id is not None:
                entity, _ = await self._resolve_folder_target(
                    client,
                    account.id,
                    target,
                    allow_join=False,
                )
            else:
                entity = await self._resolve_entity(client, account.id, target.chat_ref)
            required_media_kinds = (
                frozenset({"contact"})
                if stored_contact is not None
                else frozenset(kind for _, kind in resolved_media_items)
            )
            self._ensure_can_send(entity, media_kinds=required_media_kinds)
            slowmode_seconds = await self._respect_chat_slow_mode(
                client,
                account.id,
                target.id,
                entity,
                known_timer_checked=True,
            )
            await self._show_typing_before_send(client, entity, text=rendered_text)
            try:
                if stored_contact is not None:
                    sent = await self._send_contact(
                        client,
                        entity,
                        stored_contact,
                        reply_to_message_id,
                        idempotency_key,
                        before_send=before_send,
                    )
                elif not resolved_media_items:
                    sent = await self._send(
                        client,
                        entity,
                        text,
                        reply_to_message_id,
                        idempotency_key,
                        html_text=html_text,
                        before_send=before_send,
                    )
                elif len(resolved_media_items) == 1:
                    resolved_media, resolved_kind = resolved_media_items[0]
                    sent = await self._send_media(
                        client,
                        entity,
                        text,
                        reply_to_message_id,
                        idempotency_key,
                        html_text=html_text,
                        media_path=resolved_media,
                        media_kind=resolved_kind,
                        before_send=before_send,
                    )
                else:
                    sent = await self._send_album(
                        client,
                        entity,
                        text,
                        reply_to_message_id,
                        idempotency_key,
                        html_text=html_text,
                        media_items=resolved_media_items,
                        before_send=before_send,
                    )
            except TemporaryGatewayError as exc:
                if exc.code == "slow_mode":
                    await self._record_runtime_chat_timer(
                        account.id,
                        target.id,
                        int(exc.retry_after or 60),
                    )
                raise
            await self._record_chat_delivery(
                account.id,
                target.id,
                entity,
                sent.message_id,
                slowmode_seconds,
            )
            return sent

    async def perform_direct_service_action(
        self,
        *,
        account: Account,
        recipient: DirectRecipient,
        operation_kind: str,
        auto_delete_seconds: int | None,
        premium_only: bool,
        before_action: Callable[[], Awaitable[None]],
    ) -> DirectServiceActionResult:
        """Change a private-chat setting and read it back; never send ordinary content."""
        from telethon import errors, functions, types  # noqa: PLC0415

        if account.id is None or not recipient.username:
            raise PermanentGatewayError(
                "для сервисного действия нужен актуальный @username",
                code="service_username_required",
            )
        if operation_kind not in {"auto_delete", "protect_content"} or (
            operation_kind == "auto_delete"
            and auto_delete_seconds not in {0, 86400, 604800, 2592000}
        ):
            raise PermanentGatewayError("неизвестное сервисное действие", code="service_invalid")
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            dispatched = False
            request_accepted = False
            try:
                # Ignore cached access hashes: this feature explicitly resolves by username.
                entity, profile = await self._resolve_direct_recipient(
                    client, DirectRecipient(recipient.telegram_user_id, recipient.username)
                )
                await self._ensure_profile_not_blacklisted(profile)
                if premium_only and not bool(getattr(profile, "premium", False)):
                    raise PermanentGatewayError(
                        "у получателя больше нет Telegram Premium",
                        code="service_recipient_not_premium",
                    )
                if bool(getattr(profile, "support", False)):
                    raise PermanentGatewayError(
                        "сервисные аккаунты Telegram не поддерживаются",
                        code="recipient_unavailable",
                    )
                full = (await client(functions.users.GetFullUserRequest(id=entity))).full_user
                if int(getattr(full, "id", 0)) != recipient.telegram_user_id:
                    raise PermanentGatewayError(
                        "Telegram вернул настройки другого пользователя",
                        code="recipient_identity_mismatch",
                    )
                if bool(getattr(full, "blocked", False)):
                    raise PermanentGatewayError(
                        "пользователь заблокирован этим аккаунтом",
                        code="recipient_route_blocked",
                    )
                me = await client.get_me()
                if me is None:
                    raise PermanentGatewayError(
                        "сессия аккаунта не авторизована", code="account_unauthorized"
                    )
                if operation_kind == "protect_content":
                    if not all(
                        hasattr(full, name)
                        for name in ("noforwards_my_enabled", "noforwards_peer_enabled")
                    ):
                        raise PermanentGatewayError(
                            "клиент Telegram не поддерживает защиту личных диалогов",
                            code="service_unsupported",
                        )
                    if not bool(getattr(me, "premium", False)):
                        raise PermanentGatewayError(
                            "для запрета копирования в личном диалоге нужен Premium у аккаунта",
                            code="service_sender_requires_premium",
                        )
                    already_set = bool(full.noforwards_my_enabled or full.noforwards_peer_enabled)
                    request = functions.messages.ToggleNoForwardsRequest(peer=entity, enabled=True)
                else:
                    already_set = (getattr(full, "ttl_period", None) or 0) == auto_delete_seconds
                    request = functions.messages.SetHistoryTTLRequest(
                        peer=entity, period=int(auto_delete_seconds)
                    )
                if already_set:
                    return DirectServiceActionResult(
                        recipient.telegram_user_id,
                        changed=False,
                        username=getattr(profile, "username", None),
                        is_premium=bool(getattr(profile, "premium", False)),
                    )
                await self._ensure_profile_not_blacklisted(profile)
                await before_action()
                dispatched = True
                updates = await client(request)
                request_accepted = True
                checked = (await client(functions.users.GetFullUserRequest(id=entity))).full_user
                confirmed = int(getattr(checked, "id", 0)) == recipient.telegram_user_id and (
                    bool(
                        getattr(checked, "noforwards_my_enabled", False)
                        or getattr(checked, "noforwards_peer_enabled", False)
                    )
                    if operation_kind == "protect_content"
                    else (getattr(checked, "ttl_period", None) or 0) == auto_delete_seconds
                )
                if not confirmed:
                    raise PermanentGatewayError(
                        "Telegram принял запрос, но изменение настройки не подтвердилось; "
                        "повтор запрещён",
                        code="delivery_unconfirmed",
                    )
                message_id = None
                verification_error = None
                candidates = list(getattr(updates, "updates", ()) or ())
                if getattr(updates, "update", None) is not None:
                    candidates.append(updates.update)
                for update in candidates:
                    message = getattr(update, "message", None)
                    if not isinstance(message, types.MessageService):
                        continue
                    if getattr(message.peer_id, "user_id", None) != recipient.telegram_user_id:
                        continue
                    if getattr(message.from_id, "user_id", None) != int(me.id):
                        continue
                    action = message.action
                    matches = (
                        isinstance(action, types.MessageActionSetMessagesTTL)
                        and action.period == auto_delete_seconds
                        and not action.auto_setting_from
                        if operation_kind == "auto_delete"
                        else isinstance(action, types.MessageActionNoForwardsToggle)
                        and action.new_value is True
                        and action.prev_value is False
                    )
                    if matches and message.id > 0:
                        # A returned update alone is not a delivery receipt.
                        try:
                            saved = await client.get_messages(entity, ids=message.id)
                        except Exception as exc:  # noqa: BLE001 - the setting is already verified
                            mapped = self._map_rpc_error(exc, context="direct_message")
                            verification_error = (
                                mapped
                                if isinstance(mapped, GatewayError)
                                else PermanentGatewayError(
                                    "проверка сервисного сообщения не завершена",
                                    code="service_notification_unconfirmed",
                                )
                            )
                            break
                        if (
                            isinstance(saved, types.MessageService)
                            and saved.id == message.id
                            and getattr(saved.peer_id, "user_id", None)
                            == recipient.telegram_user_id
                            and getattr(saved.from_id, "user_id", None) == int(me.id)
                            and saved.action == action
                        ):
                            message_id = int(message.id)
                        break
            except Exception as exc:  # noqa: BLE001 - distinguish refusals from ambiguous effects
                if isinstance(exc, GatewayError):
                    raise
                if request_accepted:
                    mapped = self._map_rpc_error(exc, context="direct_message")
                    raise PermanentGatewayError(
                        "настройка запрошена, но проверка результата не завершилась; "
                        "повтор запрещён",
                        code="delivery_unconfirmed",
                    ) from mapped
                if isinstance(exc, errors.ChatNotModifiedError):
                    return DirectServiceActionResult(recipient.telegram_user_id, changed=False)
                if dispatched and (
                    not isinstance(exc, errors.RPCError) or int(exc.code or 0) >= 500
                ):
                    raise PermanentGatewayError(
                        "запрос выполнен, но его результат неизвестен; повтор запрещён",
                        code="delivery_unconfirmed",
                    ) from exc
                if isinstance(exc, errors.ChatAdminRequiredError):
                    raise PermanentGatewayError(
                        "Telegram не разрешил менять эту настройку диалога",
                        code="service_forbidden",
                    ) from exc
                if operation_kind == "protect_content" and (
                    isinstance(exc, errors.PremiumAccountRequiredError)
                    or getattr(exc, "message", "") == "PREMIUM_ACCOUNT_REQUIRED"
                ):
                    raise PermanentGatewayError(
                        "Telegram требует Premium у выполняющего аккаунта",
                        code="service_sender_requires_premium",
                    ) from exc
                if isinstance(exc, errors.TtlPeriodInvalidError):
                    raise PermanentGatewayError(
                        "Telegram не поддерживает выбранный период автоудаления",
                        code="service_invalid",
                    ) from exc
                raise self._map_rpc_error(exc, context="direct_message") from exc
        return DirectServiceActionResult(
            recipient.telegram_user_id,
            changed=True,
            service_message_id=message_id,
            username=getattr(profile, "username", None),
            is_premium=bool(getattr(profile, "premium", False)),
            access_hash=getattr(profile, "access_hash", None),
            verification_error=verification_error,
        )

    async def send_direct_content(
        self,
        *,
        account: Account,
        recipient: DirectRecipient,
        text: str,
        idempotency_key: str,
        html_text: str | None = None,
        media_path: str | None = None,
        media_kind: str | None = None,
        media_name: str | None = None,
    ) -> DirectMessageDelivery:
        """Send one consent-confirmed post to one exact Telegram user."""

        del media_name
        if account.id is None or recipient.telegram_user_id <= 0:
            raise PermanentGatewayError(
                "не хватает идентификатора аккаунта или получателя",
                code="recipient_unresolvable",
            )
        maximum_text = 1024 if media_path is not None else 4096
        try:
            rendered_text = render_telegram_html(html_text) if html_text is not None else text
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="content_invalid") from exc
        media_without_caption = media_path is not None
        if (not 1 <= len(rendered_text) <= maximum_text) and not (
            media_without_caption and not rendered_text
        ):
            raise PermanentGatewayError(
                f"текст личного сообщения должен содержать от 1 до {maximum_text} символов",
                code="content_invalid",
            )
        resolved_media_path: str | None = None
        if media_kind == "video_note" and (text or html_text):
            raise PermanentGatewayError(
                "video notes cannot contain a caption", code="content_invalid"
            )
        if media_path is not None:
            if media_kind not in {"photo", "video", "document", "voice", "video_note"}:
                raise PermanentGatewayError("тип медиа не поддерживается", code="content_invalid")
            try:
                resolved_media_path = str(resolve_media_path(self._settings, media_path))
            except ContentError as exc:
                raise PermanentGatewayError(str(exc), code="media_unavailable") from exc

        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            sending = False
            try:
                entity, profile = await self._resolve_direct_recipient(client, recipient)
                await self._ensure_profile_not_blacklisted(profile)
                await self._show_typing_before_send(client, entity, text=rendered_text)
                sending = True
                if resolved_media_path is None:
                    sent = await self._send(
                        client,
                        entity,
                        text,
                        None,
                        idempotency_key,
                        html_text=html_text,
                        error_context="direct_message",
                    )
                else:
                    sent = await self._send_media(
                        client,
                        entity,
                        text,
                        None,
                        idempotency_key,
                        html_text=html_text,
                        media_path=resolved_media_path,
                        media_kind=str(media_kind),
                        error_context="direct_message",
                    )
                await self._verify_direct_delivery(
                    client,
                    entity,
                    sent.message_id,
                    expected_text=rendered_text,
                    expect_media=resolved_media_path is not None,
                )
            except Exception as exc:  # noqa: BLE001 - one durable error vocabulary
                if isinstance(exc, GatewayError):
                    raise
                if sending:
                    raise PermanentGatewayError(
                        "отправка началась, но её результат не удалось сохранить; повтор запрещён",
                        code="delivery_unconfirmed",
                    ) from exc
                raise self._map_rpc_error(exc, context="direct_message") from exc

        access_hash = getattr(entity, "access_hash", None)
        if access_hash is None:
            access_hash = getattr(profile, "access_hash", None)
        resolved_username = getattr(profile, "username", None) or recipient.username
        premium = getattr(profile, "premium", None)
        return DirectMessageDelivery(
            message_id=sent.message_id,
            telegram_user_id=recipient.telegram_user_id,
            username=(str(resolved_username).casefold() if resolved_username else None),
            is_premium=(bool(premium) if premium is not None else None),
            access_hash=(int(access_hash) if access_hash is not None else None),
        )

    async def check_direct_recipient(
        self, *, account: Account, recipient: DirectRecipient
    ) -> DirectRecipientCheck:
        """Fetch identity and payment evidence without messages or contact changes."""

        from telethon import functions, types, utils  # noqa: PLC0415

        if account.id is None:
            raise PermanentGatewayError("аккаунт не сохранён", code="sender_unavailable")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                # Prefer the current username to detect its reassignment even
                # when a cached hash still reaches the original user.
                lookup = DirectRecipient(
                    recipient.telegram_user_id,
                    recipient.username,
                    None if recipient.username else recipient.access_hash,
                )
                _, profile = await self._resolve_direct_recipient(client, lookup)
                result = await client(
                    functions.users.GetFullUserRequest(utils.get_input_user(profile))
                )
                full = getattr(result, "full_user", None)
                users = getattr(result, "users", ())
                fresh_user = next(
                    (
                        user for user in users
                        if getattr(user, "id", None) == recipient.telegram_user_id
                    ),
                    None,
                )
                if (
                    not isinstance(full, types.UserFull)
                    or full.id != recipient.telegram_user_id
                    or not isinstance(fresh_user, types.User)
                    or fresh_user.min
                ):
                    raise PermanentGatewayError(
                        "Telegram не подтвердил полный свежий профиль получателя",
                        code="recipient_check_unconfirmed",
                    )
                available = not any(
                    (fresh_user.deleted, fresh_user.bot, fresh_user.is_self, fresh_user.restricted)
                )
                # Missing optional price on a complete current UserFull is zero.
                # Check both global price and sender-specific peer settings:
                # an exemption for one account is not proof of a removed price.
                settings = full.settings
                paid: bool | None = None
                if isinstance(settings, types.PeerSettings):
                    paid = any(
                        int(value or 0) > 0
                        for value in (
                            full.send_paid_messages_stars,
                            fresh_user.send_paid_messages_stars,
                            settings.charge_paid_message_stars,
                        )
                    )
                return DirectRecipientCheck(
                    telegram_user_id=full.id,
                    checked_at=utc_now(),
                    available=available,
                    paid_message_required=paid,
                )
            except Exception as exc:  # noqa: BLE001 - keep the shared RPC vocabulary
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="direct_message") from exc

    async def publish_story(
        self,
        *,
        account: Account,
        media_path: str,
        media_kind: str,
        caption: str,
        tagged_subscribers: list[Subscriber],
        idempotency_key: str,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> StoryDelivery:
        """Publish one public Telegram story tagging the specified subscribers in the caption."""

        from telethon import functions, types  # noqa: PLC0415

        if account.id is None:
            raise PermanentGatewayError(
                "аккаунт не сохранён в базе данных",
                code="account_unresolvable",
            )
        resolved_media_path: str
        try:
            story_media = resolve_media_path(self._settings, media_path)
            if not 0 < story_media.stat().st_size <= 30 * 1024 * 1024:
                raise ContentError("Медиа истории должно быть не больше 30 МБ и не пустым")
            resolved_media_path = str(story_media)
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="media_unavailable") from exc

        if media_kind not in {"photo", "video"}:
            raise PermanentGatewayError(
                "истории поддерживают только photo или video",
                code="content_invalid",
            )

        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            dispatched = False
            try:
                me = await client.get_me()
                if me is None:
                    raise PermanentGatewayError(
                        "Сессия не авторизована", code="account_unauthorized"
                    )
                is_premium = bool(getattr(me, "premium", False))
                max_caption_length = 2048 if is_premium else 200

                try:
                    can_send = await client(
                        functions.stories.CanSendStoryRequest(peer=types.InputPeerSelf())
                    )
                    count_remains = getattr(can_send, "count_remains", None)
                    if count_remains is not None and count_remains <= 0:
                        raise TemporaryGatewayError(
                            "исчерпан дневной лимит историй на аккаунте",
                            code="story_daily_limit_reached",
                            retry_after=43200,
                        )
                except GatewayError:
                    raise
                except Exception as exc:
                    mapped = self._map_rpc_error(exc, context="can_send_story")
                    if isinstance(mapped, GatewayError):
                        raise mapped from exc

                from proxima_traffic.story_content import story_caption, story_link_url

                verified = []
                usernames = []
                for subscriber in tagged_subscribers:
                    try:
                        _, profile = await self._resolve_direct_recipient(
                            client, DirectRecipient(
                                telegram_user_id=subscriber.telegram_user_id,
                                username=subscriber.username,
                            ),
                        )
                        await self._ensure_profile_not_blacklisted(profile)
                    except PermanentGatewayError as exc:
                        if exc.code in {
                            "recipient_unresolvable", "recipient_unavailable",
                            "recipient_identity_mismatch", "recipient_blacklisted",
                        }:
                            continue
                        raise
                    username = getattr(profile, "username", None)
                    if username:
                        verified.append(subscriber.id)
                        usernames.append(username)
                if not verified:
                    raise PermanentGatewayError("Нет доступных получателей для отметки",
                                                code="recipient_excluded")
                try:
                    message_text, entities = story_caption(
                        caption, usernames,
                        max_caption_length,
                    )
                except ValueError as exc:
                    raise PermanentGatewayError(str(exc), code="content_invalid") from exc

                # Detect URL to attach interactive link sticker area if present
                media_areas = None
                found_url = story_link_url(caption or "")
                if found_url:
                    media_areas = [
                        types.MediaAreaUrl(
                            coordinates=types.MediaAreaCoordinates(
                                x=50.0,
                                y=75.0,
                                w=60.0,
                                h=10.0,
                                rotation=0.0,
                            ),
                            url=found_url,
                        )
                    ]

                uploaded_media = await self._prepare_story_media(
                    client, resolved_media_path, media_kind,
                )

                random_id = self._message_random_id(idempotency_key)
                if before_send is not None:
                    await before_send()
                dispatched = True
                result = await client(
                    functions.stories.SendStoryRequest(
                        peer=types.InputPeerSelf(),
                        media=uploaded_media,
                        privacy_rules=[types.InputPrivacyValueAllowAll()],
                        pinned=False,
                        caption=message_text,
                        entities=entities if entities else None,
                        media_areas=media_areas,
                        period=86400,
                        random_id=random_id,
                    )
                )

                story_id = 0
                updates = getattr(result, "updates", [])
                for upd in updates:
                    if isinstance(upd, types.UpdateStoryID) and upd.random_id == random_id:
                        story_id = upd.id
                        break
                    if (isinstance(upd, types.UpdateStory)
                            and isinstance(upd.peer, types.PeerUser)
                            and upd.peer.user_id == me.id):
                        story_id = upd.story.id
                if story_id <= 0:
                    raise PermanentGatewayError(
                        "Telegram не вернул ID опубликованной истории; повтор отключён",
                        code="delivery_unconfirmed",
                    )
                username = getattr(me, "username", None)
                story_url = f"https://t.me/{username}/s/{story_id}" if username else None

                return StoryDelivery(
                    story_id=story_id,
                    story_url=story_url,
                    tagged_count=len(verified),
                    simulated=False,
                    tagged_subscriber_ids=tuple(verified),
                )
            except Exception as exc:  # noqa: BLE001
                if isinstance(exc, GatewayError):
                    raise
                mapped = self._map_rpc_error(exc, context="send_story")
                if dispatched and getattr(mapped, "code", None) in {"network_error", "rpc_error"}:
                    raise PermanentGatewayError(
                        f"ответ Telegram потерян после публикации истории: {mapped}",
                        code="delivery_unconfirmed",
                    ) from exc
                raise mapped from exc

    async def view_peer_stories(
        self,
        *,
        account: Account,
        recipient: DirectRecipient,
        send_reaction: bool = True,
        reaction_emoji: str = "🔥",
        excluded_story_ids: tuple[int, ...] = (),
        max_stories: int | None = None,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> StoryViewResult:
        """Count confirmed views and preserve them if a later reaction fails."""
        import random

        from telethon import functions, types

        from proxima_traffic.story_content import STORY_REACTIONS, normalize_story_reactions

        if account.id is None:
            raise PermanentGatewayError("Аккаунт не сохранён", code="account_unresolvable")
        try:
            choices = normalize_story_reactions(reaction_emoji) if send_reaction else "🔥"
        except ValueError as exc:
            raise PermanentGatewayError(str(exc), code="content_invalid") from exc
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            viewed = ()
            views_dispatched = False
            found = 0
            reaction_sent = False
            chosen_emoji = None
            reacted_id = None
            try:
                entity, _profile = await self._resolve_direct_recipient(
                    client, recipient, for_invite=False
                )
                await self._ensure_profile_not_blacklisted(_profile)
                input_peer = await client.get_input_entity(entity)
                response = await client(functions.stories.GetPeerStoriesRequest(peer=input_peer))
                raw = getattr(getattr(response, "stories", None), "stories", [])
                active = [
                    item for item in raw
                    if isinstance(item, types.StoryItem)
                    and item.id > 0
                    and (not item.expire_date or ensure_utc(item.expire_date) > utc_now())
                    and not isinstance(item.media, types.MessageMediaEmpty)
                ]
                found = len(active)
                selected = sorted({item.id for item in active if item.id not in excluded_story_ids})
                selected = selected[:max_stories] if max_stories is not None else selected
                if not selected:
                    return StoryViewResult(
                        telegram_user_id=recipient.telegram_user_id, stories_found=found,
                        stories_viewed=0, has_active_stories=bool(found),
                    )
                if before_send is not None:
                    await before_send()
                views_dispatched = True
                confirmed = await client(functions.stories.IncrementStoryViewsRequest(
                    peer=input_peer, id=selected,
                ))
                if confirmed is not True:
                    raise TemporaryGatewayError(
                        "Telegram не подтвердил просмотры", code="views_unconfirmed"
                    )
                viewed = tuple(selected)
                if before_send is not None:
                    await before_send()
                await client(functions.stories.ReadStoriesRequest(
                    peer=input_peer, max_id=max(selected)
                ))
                if send_reaction:
                    if before_send is not None:
                        await before_send()
                    chosen_emoji = random.choice(
                        STORY_REACTIONS if choices == "random" else choices.split(",")
                    )
                    reacted_id = max(selected)
                    await client(functions.stories.SendReactionRequest(
                        peer=input_peer, story_id=reacted_id,
                        reaction=types.ReactionEmoji(emoticon=chosen_emoji), add_to_recent=True,
                    ))
                    reaction_sent = True
                return StoryViewResult(
                    telegram_user_id=recipient.telegram_user_id, stories_found=found,
                    stories_viewed=len(viewed), reaction_sent=reaction_sent,
                    reaction_emoji=chosen_emoji if reaction_sent else None,
                    has_active_stories=bool(found), viewed_story_ids=viewed,
                    reacted_story_id=reacted_id if reaction_sent else None,
                )
            except Exception as exc:
                mapped = exc if isinstance(exc, GatewayError) else self._map_rpc_error(
                    exc, context="view_peer_stories",
                )
                if viewed:
                    return StoryViewResult(
                        telegram_user_id=recipient.telegram_user_id, stories_found=found,
                        stories_viewed=len(viewed), has_active_stories=True,
                        viewed_story_ids=viewed, error=mapped,
                    )
                if views_dispatched and mapped.code in {"network_error", "rpc_error"}:
                    raise PermanentGatewayError(
                        "Ответ Telegram после запроса просмотров потерян; повтор отключён",
                        code="delivery_unconfirmed",
                    ) from exc
                raise mapped from exc

    async def preflight_subscriber_invite(
        self,
        *,
        account: Account,
        destination_ref: str,
    ) -> SubscriberInviteDestination:
        """Verify only an owned channel/supergroup and invite permission.

        The method does not join the destination, modify contacts, or send an
        invitation. Every sender must already be a permitted administrator.
        """

        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                destination = await self._resolve_subscriber_invite_destination(
                    client, account.id, destination_ref
                )
                await self._ensure_can_invite_subscribers(client, destination)
            except Exception as exc:  # noqa: BLE001 - preserve one durable vocabulary
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="subscriber_invite") from exc
        return SubscriberInviteDestination(
            title=str(getattr(destination, "title", "") or "") or None,
            telegram_chat_id=int(destination.id),
        )

    async def invite_subscriber(
        self,
        *,
        account: Account,
        destination_ref: str,
        recipient: DirectRecipient,
        expected_destination_id: int | None = None,
        require_premium: bool | None = None,
        before_invite: Callable[[], Awaitable[None]] | None = None,
    ) -> SubscriberInviteDelivery:
        """Invite one consent-confirmed user without changing their contacts.

        Telegram remains authoritative: privacy, mutual-contact and channel
        limits are mapped into a recipient-specific durable result rather than
        bypassed or retried through contact manipulation.
        """

        if account.id is None or recipient.telegram_user_id <= 0:
            raise PermanentGatewayError(
                "missing account or recipient identifier",
                code="invite_recipient_unresolvable",
            )
        from telethon import errors, functions, types, utils  # noqa: PLC0415

        async with self._account_operation(account.id):
            dispatched = False
            try:
                client = await self._ensure_client(account.id)
                destination = await self._resolve_subscriber_invite_destination(
                    client, account.id, destination_ref
                )
                if (
                    expected_destination_id is not None
                    and destination.id != expected_destination_id
                ):
                    raise PermanentGatewayError(
                        "адрес цели теперь принадлежит другому каналу или группе",
                        code="invite_destination_changed",
                    )
                await self._ensure_can_invite_subscribers(client, destination)
                _, profile = await self._resolve_direct_recipient(
                    client, recipient, for_invite=True
                )
                await self._ensure_profile_not_blacklisted(profile)
                if require_premium is not None and bool(profile.premium) != require_premium:
                    raise PermanentGatewayError(
                        "текущий статус Premium человека не соответствует выбранному сегменту",
                        code="invite_recipient_segment_changed",
                    )
                if await self._subscriber_is_participant(client, destination, profile):
                    return self._subscriber_invite_delivery(
                        recipient, profile, already_participant=True
                    )
                # Recheck cancellation/blacklist and persist the attempt at the
                # actual non-idempotent RPC boundary, after all read-only work.
                await self._ensure_profile_not_blacklisted(profile)
                if before_invite is not None:
                    await before_invite()
                try:
                    dispatched = True
                    result = await client(
                        functions.channels.InviteToChannelRequest(
                            channel=utils.get_input_channel(destination),
                            users=[utils.get_input_user(profile)],
                        )
                    )
                except errors.UserAlreadyParticipantError:
                    return self._subscriber_invite_delivery(
                        recipient,
                        profile,
                        already_participant=True,
                    )
                if not isinstance(result, types.messages.InvitedUsers):
                    raise PermanentGatewayError(
                        "Telegram вернул неизвестный ответ на приглашение; повтор запрещён",
                        code="invite_delivery_unconfirmed",
                    )
                if result.missing_invitees:
                    if len(result.missing_invitees) != 1 or (
                        result.missing_invitees[0].user_id != recipient.telegram_user_id
                    ):
                        raise PermanentGatewayError(
                            "ответ Telegram не соответствует получателю; повтор запрещён",
                            code="invite_delivery_unconfirmed",
                        )
                    missing = result.missing_invitees[0]
                    code = (
                        "invite_sender_requires_premium"
                        if missing.premium_would_allow_invite
                        else "invite_recipient_privacy_premium_pm"
                        if missing.premium_required_for_pm
                        else "invite_recipient_privacy"
                    )
                    raise PermanentGatewayError(
                        "Telegram явно сообщил, что человек не добавлен", code=code
                    )
                await self._verify_subscriber_invite(client, destination, profile)
            except Exception as exc:  # noqa: BLE001 - normalize Telegram outcomes
                mapped = (
                    exc
                    if isinstance(exc, GatewayError)
                    else self._map_rpc_error(exc, context="subscriber_invite")
                )
                if (
                    not dispatched
                    and isinstance(mapped, TemporaryGatewayError)
                    and mapped.code in {"network_error", "rpc_error"}
                ):
                    raise TemporaryGatewayError(
                        "не завершена проверка перед приглашением; действие ещё не отправлено",
                        code="invite_preflight_unavailable",
                        retry_after=mapped.retry_after,
                    ) from mapped
                if isinstance(exc, GatewayError):
                    raise
                raise mapped from exc
        return self._subscriber_invite_delivery(recipient, profile)

    async def grant_subscriber_invite_admin(
        self,
        *,
        manager: Account,
        account: Account,
        destination_ref: str,
    ) -> None:
        """Give a selected owned account only the right needed for invitations.

        Telegram still requires the manager to have ``Add new admins`` and to
        be permitted to promote the target account.  Existing admin rights are
        preserved; this method only adds ``Invite users``.
        """

        if manager.id is None or account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if int(manager.id) == int(account.id):
            await self.preflight_subscriber_invite(account=manager, destination_ref=destination_ref)
            return
        from copy import deepcopy  # noqa: PLC0415

        from telethon import errors, functions, types, utils  # noqa: PLC0415

        async with self._account_operation(manager.id):
            dispatched = False
            try:
                client = await self._ensure_client(manager.id)
                destination = await self._resolve_subscriber_invite_destination(
                    client, manager.id, destination_ref
                )
                await self._ensure_can_manage_invite_admins(client, destination)
                target = await self._resolve_owned_admin_target(client, account)
                try:
                    permissions = await client.get_permissions(destination, target)
                except errors.UserNotParticipantError:
                    permissions = None
                if bool(getattr(permissions, "creator", False)) or bool(
                    getattr(permissions, "is_creator", False)
                ):
                    return
                participant = getattr(permissions, "participant", permissions)
                rights = getattr(participant, "admin_rights", None)
                if rights is not None and rights.invite_users:
                    return
                if rights is None:
                    rights = types.ChatAdminRights(invite_users=True)
                else:
                    rights = deepcopy(rights)
                    rights.invite_users = True
                dispatched = True
                await client(
                    functions.channels.EditAdminRequest(
                        channel=utils.get_input_channel(destination),
                        user_id=utils.get_input_user(target),
                        admin_rights=rights,
                        rank=str(getattr(participant, "rank", "") or ""),
                    )
                )
                try:
                    checked = await client.get_permissions(destination, target)
                except Exception as exc:
                    raise PermanentGatewayError(
                        "запрос прав принят, но итоговые права не подтверждены",
                        code="invite_admin_update_unconfirmed",
                    ) from self._map_rpc_error(exc, context="subscriber_invite_admin")
                checked_participant = getattr(checked, "participant", checked)
                checked_rights = getattr(checked_participant, "admin_rights", None)
                if not isinstance(checked_rights, types.ChatAdminRights) or (
                    bytes(checked_rights) != bytes(rights)
                ):
                    raise PermanentGatewayError(
                        "Telegram не подтвердил выданные права администратора",
                        code="invite_admin_update_unconfirmed",
                    )
            except Exception as exc:  # noqa: BLE001 - normalize Telegram outcomes
                if isinstance(exc, GatewayError):
                    raise
                mapped = self._map_rpc_error(exc, context="subscriber_invite_admin")
                if (
                    dispatched
                    and isinstance(mapped, TemporaryGatewayError)
                    and mapped.code in {"network_error", "rpc_error"}
                ):
                    raise PermanentGatewayError(
                        "запрос прав отправлен, но ответ Telegram не получен",
                        code="invite_admin_update_unconfirmed",
                    ) from mapped
                raise mapped from exc

    async def _ensure_can_manage_invite_admins(self, client: object, destination: object) -> None:
        """Require the exact Telegram permission needed to promote other admins."""

        try:
            permissions = await client.get_permissions(destination, "me")  # type: ignore[attr-defined]
        except Exception as exc:
            raise self._map_rpc_error(exc, context="subscriber_invite_admin") from exc
        if bool(getattr(permissions, "creator", False)) or bool(
            getattr(permissions, "is_creator", False)
        ):
            return
        rights = getattr(getattr(permissions, "participant", permissions), "admin_rights", None)
        if rights is not None and bool(getattr(rights, "add_admins", False)):
            return
        raise PermanentGatewayError(
            "главному аккаунту нужно право «Добавлять администраторов / Add new admins»",
            code="invite_admin_manager_rights",
        )

    @staticmethod
    async def _resolve_owned_admin_target(client: object, account: Account) -> object:
        """Resolve by current username first; a bare numeric id lacks an access hash."""

        from telethon import errors, types  # noqa: PLC0415

        candidates: list[object] = []
        if not account.telegram_user_id or account.telegram_user_id <= 0:
            raise PermanentGatewayError(
                "сначала проверьте сессию аккаунта для получения его Telegram ID",
                code="invite_admin_target_unresolvable",
            )
        if account.username:
            candidates.append(f"@{account.username.lstrip('@')}")
        if account.telegram_user_id:
            candidates.append(int(account.telegram_user_id))
        for candidate in candidates:
            try:
                profile = await client.get_entity(candidate)  # type: ignore[attr-defined]
            except (
                ValueError,
                errors.UsernameInvalidError,
                errors.UsernameNotOccupiedError,
                errors.PeerIdInvalidError,
                errors.UserIdInvalidError,
            ):
                continue
            if (
                isinstance(profile, types.User)
                and profile.id == account.telegram_user_id
                and not profile.bot
                and not profile.deleted
            ):
                return profile
        raise PermanentGatewayError(
            "Telegram не смог найти этот аккаунт для назначения: нужен актуальный @username "
            "или уже доступный главному аккаунту профиль",
            code="invite_admin_target_unresolvable",
        )

    @staticmethod
    def _subscriber_invite_delivery(
        recipient: DirectRecipient,
        profile: object,
        *,
        already_participant: bool = False,
    ) -> SubscriberInviteDelivery:
        from telethon import types  # noqa: PLC0415

        access_hash = getattr(profile, "access_hash", None)
        username = getattr(profile, "username", None) or recipient.username
        premium = getattr(profile, "premium", None)
        if isinstance(profile, types.User) and not profile.min:
            # On a complete User the absent Premium flag means false.
            premium = bool(profile.premium)
        return SubscriberInviteDelivery(
            telegram_user_id=recipient.telegram_user_id,
            username=(str(username).casefold() if username else None),
            is_premium=(bool(premium) if premium is not None else None),
            access_hash=(int(access_hash) if access_hash is not None else recipient.access_hash),
            already_participant=already_participant,
        )

    async def _verify_subscriber_invite(
        self, client: object, destination: object, profile: object
    ) -> None:
        """Count an invitation only after Telegram shows the user as a participant."""
        from telethon import errors, functions, utils  # noqa: PLC0415

        for delay in (0.0, 0.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                result = await asyncio.wait_for(
                    client(  # type: ignore[operator]
                        functions.channels.GetParticipantRequest(
                            channel=utils.get_input_channel(destination),
                            participant=utils.get_input_peer(profile),
                        )
                    ),
                    timeout=float(self._settings.account_delivery_verify_timeout_seconds),
                )
            except errors.UserNotParticipantError:
                continue
            except Exception as exc:  # noqa: BLE001 - an accepted invite must not be retried
                raise PermanentGatewayError(
                    "Telegram принял приглашение, но проверка участия не удалась",
                    code="invite_delivery_unconfirmed",
                ) from self._map_rpc_error(exc, context="subscriber_invite")
            participant = getattr(result, "participant", None)
            if self._participant_matches(participant, int(profile.id)):
                return
        raise PermanentGatewayError(
            "Telegram принял приглашение, но участие человека не подтверждено",
            code="invite_delivery_unconfirmed",
        )

    @staticmethod
    def _participant_matches(participant: object, user_id: int) -> bool:
        from telethon import types  # noqa: PLC0415

        if isinstance(participant, types.ChannelParticipantBanned):
            return (
                getattr(participant.peer, "user_id", None) == user_id
                and not participant.left
                and not participant.banned_rights.view_messages
            )
        return (
            isinstance(
                participant,
                (
                    types.ChannelParticipant,
                    types.ChannelParticipantSelf,
                    types.ChannelParticipantAdmin,
                    types.ChannelParticipantCreator,
                ),
            )
            and participant.user_id == user_id
        )

    async def _subscriber_is_participant(
        self, client: object, destination: object, profile: object
    ) -> bool:
        """Avoid counting an existing participant as a newly added person."""
        from telethon import errors, functions, types, utils  # noqa: PLC0415

        try:
            result = await client(  # type: ignore[operator]
                functions.channels.GetParticipantRequest(
                    channel=utils.get_input_channel(destination),
                    participant=utils.get_input_peer(profile),
                )
            )
        except errors.UserNotParticipantError:
            return False
        participant = getattr(result, "participant", None)
        if self._participant_matches(participant, int(profile.id)):
            return True
        if isinstance(participant, types.ChannelParticipantLeft) and (
            getattr(participant.peer, "user_id", None) == profile.id
        ):
            return False
        if isinstance(participant, types.ChannelParticipantBanned) and (
            getattr(participant.peer, "user_id", None) == profile.id
        ):
            raise PermanentGatewayError(
                "человек исключён из этой цели; сервис не снимает запрет",
                code="invite_recipient_kicked",
            )
        raise TemporaryGatewayError(
            "Telegram не подтвердил текущий статус участия; приглашение ещё не отправлено",
            code="invite_preflight_unavailable",
        )

    async def _ensure_can_invite_subscribers(self, client: object, destination: object) -> None:
        """Reject non-channel targets and missing admin invitation rights early."""

        if bool(getattr(destination, "monoforum", False)) or not (
            bool(getattr(destination, "broadcast", False))
            or bool(getattr(destination, "megagroup", False))
        ):
            raise PermanentGatewayError(
                "invitation destination must be a channel or supergroup",
                code="invite_destination_invalid",
            )
        try:
            permissions = await client.get_permissions(destination, "me")  # type: ignore[attr-defined]
        except Exception as exc:
            raise self._map_rpc_error(exc, context="subscriber_invite") from exc
        rights = getattr(getattr(permissions, "participant", permissions), "admin_rights", None)
        if bool(getattr(permissions, "creator", False)) or bool(
            getattr(permissions, "is_creator", False)
        ):
            return
        if rights is not None and bool(getattr(rights, "invite_users", False)):
            return
        raise PermanentGatewayError(
            "the account is not an administrator with invite-member permission",
            code="invite_destination_rights",
        )

    async def _resolve_subscriber_invite_destination(
        self,
        client: object,
        account_id: int,
        destination_ref: str,
    ) -> object:
        """Resolve a public target or an already-accessible private target.

        A private invite link is inspected only with ``CheckChatInvite``.  If
        Telegram does not return ``ChatInviteAlready``, the account is not a
        current member/admin and this workflow must not join it automatically.
        """

        from telethon import errors, functions, types  # noqa: PLC0415

        normalized = destination_ref.strip()
        private_prefixes = ("https://t.me/+", "https://t.me/joinchat/")
        if normalized.casefold().startswith(private_prefixes):
            invite_hash = normalized.rsplit("/", 1)[-1]
            if invite_hash.startswith("+"):
                invite_hash = invite_hash[1:]
            try:
                invite = await client(  # type: ignore[operator]
                    functions.messages.CheckChatInviteRequest(invite_hash)
                )
            except Exception as exc:  # noqa: BLE001 - one invitation vocabulary
                if isinstance(
                    exc,
                    (
                        errors.InviteHashEmptyError,
                        errors.InviteHashExpiredError,
                        errors.InviteHashInvalidError,
                        errors.InviteRevokedMissingError,
                    ),
                ) or getattr(exc, "message", "") in {
                    "INVITE_HASH_EMPTY",
                    "INVITE_HASH_EXPIRED",
                    "INVITE_HASH_INVALID",
                    "INVITE_REVOKED_MISSING",
                }:
                    raise PermanentGatewayError(
                        "приватная ссылка приглашения недействительна или истекла",
                        code="invite_destination_link_invalid",
                    ) from exc
                raise self._map_rpc_error(exc, context="subscriber_invite") from exc
            already_member = isinstance(invite, types.ChatInviteAlready)
            if not already_member or getattr(invite, "chat", None) is None:
                raise PermanentGatewayError(
                    "аккаунт не состоит в закрытой цели по этой invite-ссылке",
                    code="invite_destination_unavailable",
                )
            return invite.chat
        try:
            return await self._resolve_entity(client, account_id, normalized)
        except (
            ValueError,
            errors.UsernameInvalidError,
            errors.UsernameNotOccupiedError,
            errors.ChannelInvalidError,
            errors.ChatInvalidError,
        ) as exc:
            raise PermanentGatewayError(
                "адрес канала или группы недействителен", code="invite_destination_invalid"
            ) from exc

    @staticmethod
    async def _resolve_direct_recipient(
        client: object,
        recipient: DirectRecipient,
        *,
        for_invite: bool = False,
    ) -> tuple[object, object]:
        from telethon import errors, types  # noqa: PLC0415

        try:
            if recipient.access_hash is not None:
                entity: object = types.InputPeerUser(
                    user_id=recipient.telegram_user_id,
                    access_hash=recipient.access_hash,
                )
                # InputPeerUser itself has no bot/deleted/self/profile flags.
                # Fetch the full user so cached access hashes keep the same
                # identity and safety checks as a username resolution.
                try:
                    profile: object = await client.get_entity(entity)  # type: ignore[attr-defined]
                except (ValueError, errors.PeerIdInvalidError, errors.UserIdInvalidError):
                    if not recipient.username:
                        raise ValueError("cached recipient is unavailable") from None
                    # Refresh an obsolete account-scoped hash through the
                    # current username; the exact ID check below still applies.
                    profile = await client.get_entity(recipient.username)  # type: ignore[attr-defined]
                    entity = profile
            elif recipient.username:
                profile = await client.get_entity(recipient.username)  # type: ignore[attr-defined]
                entity = profile
            else:
                entity = await client.get_input_entity(  # type: ignore[attr-defined]
                    recipient.telegram_user_id
                )
                profile = await client.get_entity(entity)  # type: ignore[attr-defined]
        except ValueError as exc:
            raise PermanentGatewayError(
                "аккаунт не знает этого пользователя: нужен актуальный username или общий чат",
                code=("invite_recipient_unresolvable" if for_invite else "recipient_unresolvable"),
            ) from exc

        resolved_id = (
            getattr(profile, "id", None)
            or getattr(entity, "id", None)
            or getattr(entity, "user_id", None)
        )
        if resolved_id is None or int(resolved_id) != recipient.telegram_user_id:
            raise PermanentGatewayError(
                "username теперь принадлежит другому пользователю; отправка отменена",
                code="recipient_identity_mismatch",
            )
        if bool(getattr(profile, "bot", False)) or bool(getattr(profile, "deleted", False)):
            raise PermanentGatewayError(
                "получатель удалён или является ботом",
                code=("invite_recipient_unavailable" if for_invite else "recipient_unavailable"),
            )
        if bool(getattr(profile, "self", False)) or bool(getattr(profile, "is_self", False)):
            raise PermanentGatewayError(
                "аккаунт не отправляет массовое сообщение самому себе",
                code=("invite_recipient_unavailable" if for_invite else "recipient_unavailable"),
            )
        return entity, profile

    async def close(self) -> None:
        async with self._global_lock:
            account_ids = list(self._clients)
        for account_id in account_ids:
            await self.close_account(account_id)
        async with self._global_lock:
            self._entity_cache.clear()
            self._joined.clear()

    async def close_account(self, account_id: int) -> None:
        """Drop one cached Telegram client so the next send reconnects through the proxy."""
        lock = await self._lock_for(account_id)
        async with lock:
            async with self._global_lock:
                client = self._clients.pop(account_id, None)
                self._client_generations.pop(account_id, None)
                self._entity_cache = {
                    key: value for key, value in self._entity_cache.items() if key[0] != account_id
                }
                self._joined = {key for key in self._joined if key[0] != account_id}
            if client is not None:
                try:
                    await client.disconnect()  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - best-effort reconnect hygiene
                    logger.debug("gateway account client disconnect failed", exc_info=True)

    async def run_account_operation(
        self,
        account_id: int,
        operation: Callable[[object], Awaitable[object]],
    ) -> tuple[object, object]:
        """Run profile/check/folder work on the same client and lock used for sends."""
        async with self._account_operation(account_id):
            client = await self._ensure_client(account_id)
            result = await operation(client)
            me = await client.get_me()
            if me is None:
                raise PermanentGatewayError(
                    f"Telegram не вернул профиль аккаунта #{account_id}",
                    code="account_unauthorized",
                )
            return me, result

    # ------------------------------------------------------------------ helpers
    @asynccontextmanager
    async def _account_operation(self, account_id: int):
        """Combine the local client lock with a durable cross-process lease."""

        lock = await self._lock_for(account_id)
        async with lock:
            async with self._account_leases.hold(account_id) as token:
                if token is None:
                    raise TemporaryGatewayError(
                        "аккаунт занят другим worker-процессом",
                        retry_after=5,
                        code="account_operation_busy",
                    )
                yield

    async def _lock_for(self, account_id: int) -> asyncio.Lock:
        async with self._global_lock:
            lock = self._locks.get(account_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[account_id] = lock
            return lock

    async def _ensure_client(self, account_id: int):
        generation = await self._account_connection_generation(account_id)
        client = self._clients.get(account_id)
        client_generations = getattr(self, "_client_generations", None)
        if client_generations is None:
            client_generations = self._client_generations = {}
        if (
            client is not None
            and client.is_connected()  # type: ignore[attr-defined]
            and client_generations.get(account_id) == generation
        ):
            route_check_due = getattr(self._proxy_service, "account_route_check_due", None)
            if not callable(route_check_due) or not await route_check_due(
                account_id,
                max_age_seconds=self._settings.proxy_health_recheck_seconds,
            ):
                return client
            # A client can remain marked as connected after its upstream proxy
            # has disappeared.  Recheck at a bounded interval before reusing
            # that client, so a dead route becomes a clear proxy result rather
            # than a vague Telegram network error on the next send.
            await self._resolve_proxy(account_id)
            generation = await self._account_connection_generation(account_id)
            if client_generations.get(account_id) == generation:
                return client
        if client is not None:
            try:
                await client.disconnect()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - stale clients are discarded regardless
                logger.debug("stale gateway client disconnect failed", exc_info=True)
        self._clients.pop(account_id, None)
        client_generations.pop(account_id, None)
        self._entity_cache = {
            key: value for key, value in self._entity_cache.items() if key[0] != account_id
        }
        self._joined = {key for key in self._joined if key[0] != account_id}

        for _ in range(2):
            client = await self._build_client(account_id)
            current_generation = await self._account_connection_generation(account_id)
            if current_generation == generation:
                self._clients[account_id] = client
                client_generations[account_id] = generation
                return client
            try:
                await client.disconnect()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - retry with the newly committed route
                logger.debug("changed-route client disconnect failed", exc_info=True)
            generation = current_generation
        raise TemporaryGatewayError(
            "Маршрут аккаунта изменился во время подключения",
            retry_after=5,
            code="proxy_unavailable",
        )

    async def _account_connection_generation(self, account_id: int) -> int:
        async with self._session_factory() as session:
            generation = await session.scalar(
                select(Account.connection_generation).where(Account.id == account_id)
            )
        if generation is None:
            raise PermanentGatewayError(
                f"Аккаунт #{account_id} не найден",
                code="account_dead",
            )
        return max(0, int(generation))

    async def _build_client(self, account_id: int):
        from telethon import TelegramClient  # noqa: PLC0415 - heavy lazy import
        from telethon.sessions import StringSession  # noqa: PLC0415

        try:
            decrypted = await self._account_service.load_session(account_id)
            session = StringSession(decrypted.session_string)
        except (AccountImportError, TypeError, ValueError) as exc:
            raise PermanentGatewayError(
                f"сессия аккаунта #{account_id} повреждена или недоступна; импортируйте её заново",
                code="session_material_invalid",
            ) from exc
        transport = await self._resolve_proxy(account_id)
        transport_kwargs = transport.client_kwargs() if transport is not None else {}
        client = TelegramClient(
            session,
            decrypted.api_id,
            decrypted.api_hash,
            **transport_kwargs,
            connection_retries=2,
            retry_delay=2,
            timeout=int(self._settings.account_connect_timeout_seconds),
            # Delivery workers must observe every FloodWait and RPC failure so
            # they can persist the cooldown, retry safely and honour shutdown.
            # Telethon's default short sleeps/retries bypass that durable flow.
            request_retries=0,
            flood_sleep_threshold=0,
            raise_last_call_error=True,
        )
        try:
            await asyncio.wait_for(
                client.connect(), timeout=self._settings.account_connect_timeout_seconds
            )
            authorized = await client.is_user_authorized()
        except Exception as exc:  # noqa: BLE001 - normalize connect/auth failures for workers
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001 - the original connection failure is authoritative
                logger.debug("failed Telegram client cleanup after connect error", exc_info=True)
            if isinstance(exc, GatewayError):
                raise
            raise self._map_rpc_error(exc, context="connect") from exc
        if not authorized:
            await client.disconnect()
            raise PermanentGatewayError(
                f"сессия аккаунта #{account_id} недействительна; загрузите tdata заново",
                code="account_unauthorized",
            )
        return client

    async def _resolve_proxy(self, account_id: int):
        try:
            recovery = getattr(self._proxy_service, "ensure_account_route", None)
            route = (
                await recovery(account_id, actor="telethon-gateway")
                if callable(recovery)
                else await self._proxy_service.require_account_route(account_id)
            )
        except ProxyRouteUnavailable as exc:
            if self._settings.account_require_proxy:
                raise TemporaryGatewayError(
                    "назначенный прокси не работает и в пуле нет рабочего варианта; "
                    "добавьте рабочий прокси в панели",
                    retry_after=60,
                    code="proxy_unavailable",
                ) from exc
            logger.warning(
                "account %s sending WITHOUT a proxy (account_require_proxy is off)", account_id
            )
            return TelethonTransport(proxy=None)
        return build_telethon_transport(route)

    @staticmethod
    async def _discussion_for_post(client, channel: object, channel_post_id: int):
        from telethon import functions, utils  # noqa: PLC0415

        return await client(
            functions.messages.GetDiscussionMessageRequest(
                peer=utils.get_input_peer(channel),
                msg_id=channel_post_id,
            )
        )

    @staticmethod
    def _ensure_comment_thread(
        discussion_entity: object,
        root_message_id: int,
        expected_peer_id: int | None,
        expected_root_id: int | None,
    ) -> None:
        from telethon import utils

        if (
            expected_root_id is not None
            and root_message_id != expected_root_id
            or expected_peer_id is not None
            and utils.get_peer_id(discussion_entity) != expected_peer_id
        ):
            raise PermanentGatewayError(
                "Обсуждение поста изменилось; продолжать цепочку "
                "или редактировать старый ID небезопасно",
                code="comment_thread_changed",
            )

    @staticmethod
    async def _discussion_entity(client, discussion: object) -> tuple[object, int]:
        """Resolve the auto-forwarded post that anchors the public comment thread."""
        messages = list(getattr(discussion, "messages", None) or [])
        root = next(
            (
                message
                for message in reversed(messages)
                if getattr(message, "id", None) is not None
                and getattr(message, "peer_id", None) is not None
            ),
            None,
        )
        if root is None:
            raise PermanentGatewayError(
                "Telegram не вернул ветку комментариев к этому посту",
                code="comments_unavailable",
            )
        peer = root.peer_id
        chat_id = int(
            getattr(peer, "channel_id", None)
            or getattr(peer, "chat_id", None)
            or getattr(peer, "user_id", None)
            or 0
        )
        chat = next(
            (item for item in list(getattr(discussion, "chats", None) or []) if item.id == chat_id),
            None,
        )
        if chat is None:
            raise PermanentGatewayError(
                "Telegram не вернул группу обсуждения для этого поста",
                code="comments_unavailable",
            )
        return await client.get_entity(chat), int(root.id)

    async def _collect_mentionable_user_ids_from_entity(
        self,
        client: object,
        entity: object,
        *,
        exclude_user_id: int | None,
        limit: int,
    ) -> tuple[int, ...]:
        """Collect public participant records, excluding unusable identities."""

        collected: list[int] = []
        profiles: list[object] = []
        seen: set[int] = set()
        participant_limit = max(5, min(int(limit), 200))
        async for participant in client.iter_participants(entity, limit=participant_limit):  # type: ignore[attr-defined]
            user_id = getattr(participant, "id", None)
            if not isinstance(user_id, int) or user_id <= 0:
                continue
            if user_id == exclude_user_id or user_id in seen:
                continue
            if any(
                bool(getattr(participant, attribute, False))
                for attribute in ("bot", "deleted", "self", "is_self")
            ):
                continue
            seen.add(user_id)
            collected.append(user_id)
            profiles.append(participant)
        blocked = await self._blacklisted_profile_user_ids(profiles)
        return tuple(user_id for user_id in collected if user_id not in blocked)

    async def _blacklisted_profile_user_ids(self, profiles: list[object]) -> set[int]:
        """Check current usernames and retain resolved blocked identities by ID."""

        names_by_id: dict[int, set[str]] = {}
        for profile in profiles:
            user_id = int(getattr(profile, "id", 0) or 0)
            if user_id <= 0:
                continue
            names = {str(getattr(profile, "username", "") or "").casefold()}
            names.update(
                str(getattr(alias, "username", "") or "").casefold()
                for alias in (getattr(profile, "usernames", None) or ())
                if getattr(alias, "active", True)
            )
            names_by_id[user_id] = names - {""}
        if not names_by_id:
            return set()
        profile_names = tuple({name for names in names_by_id.values() for name in names})
        async with self._session_factory() as session:
            blocked = set(
                await session.scalars(
                    select(MentionBlacklistEntry.telegram_user_id).where(
                        MentionBlacklistEntry.telegram_user_id.in_(names_by_id)
                    )
                )
            )
            banned_names: set[str] = set()
            for index in range(0, len(profile_names), 500):
                banned_names.update(
                    await session.scalars(
                        select(MentionBlacklistUsernameEntry.username).where(
                            MentionBlacklistUsernameEntry.username.in_(
                                profile_names[index : index + 500]
                            )
                        )
                    )
                )
            matched = {user_id for user_id, names in names_by_id.items() if names & banned_names}
            if matched:
                await session.execute(
                    sqlite_insert(MentionBlacklistEntry)
                    .values(
                        [
                            {
                                "telegram_user_id": user_id,
                                "source": "blacklist_username_match",
                                "created_at": utc_now(),
                            }
                            for user_id in matched
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=[MentionBlacklistEntry.telegram_user_id])
                )
                await session.commit()
            return blocked | matched

    async def _ensure_profile_not_blacklisted(self, profile: object) -> None:
        if await self._blacklisted_profile_user_ids([profile]):
            raise PermanentGatewayError(
                "получатель находится в глобальном стоп-листе", code="recipient_blacklisted"
            )

    async def filter_mentionable_user_ids(
        self, *, account: Account, user_ids: tuple[int, ...]
    ) -> tuple[int, ...]:
        """Recheck a saved mention snapshot against usernames imported since collection."""

        if not user_ids:
            return ()
        async with self._session_factory() as session:
            has_names = await session.scalar(
                select(MentionBlacklistUsernameEntry.username).limit(1)
            )
        if has_names is None:
            return user_ids
        if account.id is None:
            return ()
        profiles: list[object] = []
        async with self._account_operation(account.id):
            client = await self._ensure_client(account.id)
            for user_id in user_ids:
                try:
                    profile = await client.get_entity(user_id)  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - unresolved identities cannot be safely mentioned
                    continue
                if getattr(profile, "id", None) == user_id:
                    profiles.append(profile)
        blocked = await self._blacklisted_profile_user_ids(profiles)
        resolved = {int(getattr(profile, "id", 0)) for profile in profiles}
        return tuple(
            user_id for user_id in user_ids if user_id in resolved and user_id not in blocked
        )

    async def _resolve_entity(self, client, account_id: int, chat_ref: str):
        cache_key = (account_id, chat_ref)
        cached = self._entity_cache.get(cache_key)
        if cached is not None:
            return cached
        target_ref: object = chat_ref
        stripped = chat_ref.strip()
        if stripped.lstrip("-").isdigit():
            target_ref = int(stripped)
        try:
            entity = await client.get_entity(target_ref)
        except (ValueError, TypeError) as exc:
            raise PermanentGatewayError(
                f"цель {chat_ref!r} не найдена или недоступна",
                code="target_unavailable",
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise self._map_rpc_error(exc, context="resolve") from exc
        self._entity_cache[cache_key] = entity
        return entity

    async def _resolve_preflight_entity(
        self,
        client: object,
        account_id: int,
        target: Target,
    ) -> object:
        """Resolve only existing membership; preflight must never join a chat."""
        from telethon import errors, functions, types  # noqa: PLC0415

        if target.folder_slug and target.telegram_peer_id is not None:
            # Folder links use a separate Telegram RPC. Its failures used to
            # escape this preflight path as raw numeric RPC codes (for example
            # 420 for a frozen account), so the UI presented an account-wide
            # restriction as an opaque account-chat pair error.
            try:
                entity, _ = await self._resolve_folder_target(
                    client,
                    account_id,
                    target,
                    allow_join=False,
                )
            except GatewayError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalize folder RPC failures
                raise self._map_rpc_error(exc, context="preflight") from exc
            return entity
        if target.chat_ref.startswith("https://t.me/+"):
            invite_hash = target.chat_ref.split("+", 1)[1]
            try:
                info = await client(  # type: ignore[operator]
                    functions.messages.CheckChatInviteRequest(invite_hash)
                )
            except Exception as exc:  # noqa: BLE001
                raise self._map_rpc_error(exc, context="preflight") from exc
            if not isinstance(info, types.ChatInviteAlready):
                raise PermanentGatewayError(
                    "аккаунт не состоит в приватной группе",
                    code="target_not_joined",
                )
            entity = getattr(info, "chat", None)
            if entity is None:
                raise PermanentGatewayError(
                    "приватный чат недоступен",
                    code="target_unavailable",
                )
            return entity

        entity = await self._resolve_entity(client, account_id, target.chat_ref)
        if isinstance(entity, types.User | types.ChatForbidden | types.ChannelForbidden):
            raise PermanentGatewayError(
                "чат недоступен этому аккаунту",
                code="target_unavailable",
            )
        if isinstance(entity, types.Chat):
            if bool(getattr(entity, "left", False)) or bool(getattr(entity, "deactivated", False)):
                raise PermanentGatewayError(
                    "аккаунт не состоит в этой группе",
                    code="target_not_joined",
                )
            return entity
        try:
            await client(  # type: ignore[operator]
                functions.channels.GetParticipantRequest(entity, "me")
            )
        except errors.UserNotParticipantError as exc:
            raise PermanentGatewayError(
                "аккаунт не состоит в этой группе",
                code="target_not_joined",
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise self._map_rpc_error(exc, context="preflight") from exc
        return entity

    async def _known_target_timing(
        self,
        account_id: int,
        target_id: int,
    ) -> tuple[int | None, datetime | None]:
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                return None, None
            return state.slowmode_seconds, ensure_utc(state.next_send_at)

    @staticmethod
    def _safe_gateway_error(exc: BaseException) -> str:
        return str(getattr(exc, "safe_message", None) or "membership gate operation failed")[:500]

    @staticmethod
    def _inline_buttons(message: object) -> tuple[InlineButton, ...]:
        """Map Telethon's markup to the narrowly scoped gate parser input."""

        markup = getattr(message, "reply_markup", None)
        buttons: list[InlineButton] = []
        for row in list(getattr(markup, "rows", None) or []):
            for button in list(getattr(row, "buttons", None) or []):
                raw_data = getattr(button, "data", None)
                buttons.append(
                    InlineButton(
                        text=str(getattr(button, "text", "") or ""),
                        url=(str(getattr(button, "url", "") or "") or None),
                        callback_data=(bytes(raw_data) if isinstance(raw_data, bytes) else None),
                        is_web_app=(
                            type(button).__name__
                            in {
                                "KeyboardButtonSimpleWebView",
                                "KeyboardButtonUrlAuth",
                                "KeyboardButtonWebView",
                            }
                        ),
                    )
                )
        return tuple(buttons)

    async def _recover_membership_gate_for_entity(
        self,
        client: object,
        account_id: int,
        entity: object,
        deleted_message_id: int,
    ) -> MembershipGateRecovery:
        """Resolve one validated subscription gate in the exact delivery chat."""

        gate_detected = False
        gate_notice: str | None = None
        try:
            recent = await asyncio.wait_for(
                client.get_messages(  # type: ignore[attr-defined]
                    entity,
                    limit=int(self._settings.membership_gate_scan_limit),
                ),
                timeout=float(self._settings.account_delivery_verify_timeout_seconds),
            )
            gate_message, gate = await self._find_membership_gate(
                client,
                entity,
                list(recent) if recent is not None else [],
                after_message_id=deleted_message_id,
            )
            if gate_message is None or gate is None:
                return MembershipGateRecovery(detected=False, recovered=False)
            gate_detected = True
            gate_notice = gate.notice
            if len(gate.channel_refs) > int(self._settings.membership_gate_max_channels):
                return MembershipGateRecovery(
                    detected=True,
                    recovered=False,
                    notice=gate.notice,
                    error="subscription gate lists too many channels",
                )
            joined: list[str] = []
            for channel_ref in gate.channel_refs:
                did_join = await self._join_membership_channel(client, account_id, channel_ref)
                if did_join:
                    joined.append(channel_ref)
            callback_count = 0
            gate_message_id = int(gate_message.id)
            for callback_data in gate.verification_callbacks:
                await self._click_membership_verification(
                    client,
                    entity,
                    gate_message_id,
                    callback_data,
                )
                callback_count += 1
            return MembershipGateRecovery(
                detected=True,
                recovered=True,
                notice=gate.notice,
                joined_channel_refs=tuple(joined),
                verification_callback_count=callback_count,
            )
        except GatewayError as exc:
            return MembershipGateRecovery(
                detected=gate_detected,
                recovered=False,
                notice=gate_notice,
                error=self._safe_gateway_error(exc),
            )
        except Exception as exc:  # noqa: BLE001 - preserve the deleted post as source of truth
            mapped = self._map_rpc_error(exc, context="membership_gate")
            return MembershipGateRecovery(
                detected=gate_detected,
                recovered=False,
                notice=gate_notice,
                error=self._safe_gateway_error(mapped),
            )

    async def _find_membership_gate(
        self,
        client: object,
        entity: object,
        messages: list[object],
        *,
        after_message_id: int,
    ) -> tuple[object | None, object | None]:
        """Find the first authorised gate that can belong to the deleted post.

        A generic subscription-looking post is not enough: it must come from a
        bot.  In supergroups the bot must also be an administrator.  If the bot
        chose to reply to a message, its reply must point at the post that was
        actually removed.  This prevents an unrelated bot announcement from
        making an account join channels.
        """

        candidates: list[tuple[int, int, object, object]] = []
        for message in messages:
            raw_id = getattr(message, "id", None)
            if raw_id is None or int(raw_id) <= after_message_id:
                continue
            reply_to_id = self._reply_to_message_id(message)
            if reply_to_id is not None and reply_to_id != after_message_id:
                continue
            text = str(
                getattr(message, "raw_text", None) or getattr(message, "message", None) or ""
            )
            gate = parse_membership_gate(
                text,
                self._inline_buttons(message),
                text_links=self._message_text_links(message),
            )
            if gate is None:
                continue
            if not await self._is_moderator_bot_message(client, entity, message):
                continue
            # A direct reply to the removed post is the strongest association.
            # Some moderator bots post a standalone notice, so retain that
            # supported fallback but prefer a precise reply whenever available.
            candidates.append(
                (0 if reply_to_id == after_message_id else 1, int(raw_id), message, gate)
            )
        if not candidates:
            return None, None
        _, _, message, gate = min(candidates, key=lambda item: (item[0], item[1]))
        return message, gate

    @staticmethod
    def _message_text_links(message: object) -> tuple[str, ...]:
        """Return Telegram URLs hidden behind formatted moderator-bot text.

        Plain URLs are parsed from the message body by ``parse_membership_gate``.
        ``MessageEntityTextUrl`` keeps its destination separately, therefore it
        must be extracted before the transport object is discarded.
        """

        values: list[str] = []
        for entity in tuple(getattr(message, "entities", None) or ()):
            url = getattr(entity, "url", None)
            if isinstance(url, str) and url and url not in values:
                values.append(url)
        return tuple(values)

    @staticmethod
    def _reply_to_message_id(message: object) -> int | None:
        reply_to = getattr(message, "reply_to", None)
        raw = (
            getattr(reply_to, "reply_to_msg_id", None)
            if reply_to is not None
            else getattr(message, "reply_to_msg_id", None)
        )
        try:
            return int(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    async def _is_moderator_bot_message(
        self,
        client: object,
        entity: object,
        message: object,
    ) -> bool:
        """Accept only bot notices, plus administrator status in supergroups."""

        sender = getattr(message, "sender", None)
        if sender is None:
            get_sender = getattr(message, "get_sender", None)
            if callable(get_sender):
                try:
                    sender = await get_sender()
                except Exception:  # noqa: BLE001 - absent sender metadata is not trusted
                    return False
        if sender is None or not bool(getattr(sender, "bot", False)):
            return False

        from telethon import functions, types, utils  # noqa: PLC0415

        if isinstance(entity, types.Chat):
            # Legacy/basic groups use a different RPC.  Do not relax the
            # administrator requirement merely because they lack
            # channels.GetParticipant.
            sender_id = getattr(sender, "id", None)
            if not isinstance(sender_id, int) or sender_id <= 0:
                return False
            try:
                full = await client(  # type: ignore[operator]
                    functions.messages.GetFullChatRequest(entity.id)
                )
            except Exception:  # noqa: BLE001 - membership actions must fail closed
                return False
            participants = getattr(getattr(full, "full_chat", None), "participants", None)
            return any(
                getattr(participant, "user_id", None) == sender_id
                and isinstance(
                    participant,
                    (types.ChatParticipantAdmin, types.ChatParticipantCreator),
                )
                for participant in tuple(getattr(participants, "participants", None) or ())
            )
        if not isinstance(entity, types.Channel):
            # A non-channel peer cannot expose an administrator roster.  The
            # bot identity is still required, but the gate remains anchored to
            # the exact deleted post and its Telegram-only channel links.
            return True
        try:
            result = await client(  # type: ignore[operator]
                functions.channels.GetParticipantRequest(entity, utils.get_input_peer(sender))
            )
        except Exception:  # noqa: BLE001 - do not act without admin proof
            return False
        participant = getattr(result, "participant", None)
        return isinstance(
            participant,
            (types.ChannelParticipantAdmin, types.ChannelParticipantCreator),
        )

    async def _join_membership_channel(
        self,
        client: object,
        account_id: int,
        channel_ref: str,
    ) -> bool:
        """Join one validated button target while observing normal join limits."""

        from telethon import errors, functions, types  # noqa: PLC0415

        if channel_ref.startswith("https://t.me/+") or channel_ref.startswith(
            "https://t.me/joinchat/"
        ):
            _, joined = await self._join_private_invite(client, account_id, channel_ref)
            if joined:
                await self._account_service.record_join(account_id)
            return joined
        entity = await self._resolve_entity(client, account_id, channel_ref)
        if not isinstance(entity, types.Channel):
            raise PermanentGatewayError(
                "subscription button does not lead to a Telegram channel",
                code="membership_gate_invalid_channel",
            )
        try:
            await client(functions.channels.GetParticipantRequest(entity, "me"))  # type: ignore[operator]
            return False
        except errors.UserNotParticipantError:
            await self._enforce_join_limit(account_id)
            try:
                await client(functions.channels.JoinChannelRequest(entity))  # type: ignore[operator]
            except errors.UserAlreadyParticipantError:
                return False
            await self._account_service.record_join(account_id)
            return True

    async def _ensure_discussion_membership(
        self,
        client: object,
        account_id: int,
        discussion_entity: object,
        *,
        join_if_needed: bool,
    ) -> bool:
        """Ensure that comments can be posted to a linked discussion group.

        Telegram resolves a linked discussion even when the sender is only a
        guest. Sending from that state fails with ``CHAT_GUEST_SEND_FORBIDDEN``.
        A normal delivery may join the group under the configured account join
        limits; preflight only verifies that automatic joining is enabled.
        """

        from telethon import errors, functions, types  # noqa: PLC0415

        if not isinstance(discussion_entity, types.Channel):
            return False
        try:
            await client(  # type: ignore[operator]
                functions.channels.GetParticipantRequest(discussion_entity, "me")
            )
            return False
        except errors.UserNotParticipantError:
            if not self._settings.account_auto_join:
                raise PermanentGatewayError(
                    "аккаунт не состоит в группе обсуждения, а автовступление выключено",
                    code="target_not_joined",
                ) from None
            if not join_if_needed:
                return False

        await self._enforce_join_limit(account_id)
        try:
            await client(  # type: ignore[operator]
                functions.channels.JoinChannelRequest(discussion_entity)
            )
        except errors.UserAlreadyParticipantError:
            return False
        await self._account_service.record_join(account_id)
        return True

    async def _click_membership_verification(
        self,
        client: object,
        entity: object,
        message_id: int,
        callback_data: bytes,
    ) -> None:
        """Click only a positively identified ``verify/check/done`` callback."""

        from telethon import functions, utils  # noqa: PLC0415

        answer = await client(  # type: ignore[operator]
            functions.messages.GetBotCallbackAnswerRequest(
                peer=utils.get_input_peer(entity),
                msg_id=message_id,
                data=callback_data,
            )
        )
        answer_text = str(getattr(answer, "message", "") or "").casefold()
        if any(
            marker in answer_text
            for marker in (
                "не подпис",
                "подпиш",
                "не вступ",
                "не приєдна",
                "підпиш",
                "не выполн",
                "не викон",
                "нет доступа",
                "немає доступу",
                "access denied",
                "not subscribed",
                "not a member",
                "subscribe first",
                "must join",
                "ошиб",
                "error",
            )
        ):
            raise PermanentGatewayError(
                "membership verification callback did not confirm access",
                code="membership_gate_verification_failed",
            )

    async def _join_and_resolve(
        self, client, account_id: int, target: Target, *, report_join: bool = False
    ):
        """Ensure the account is a participant, joining public chats/invites first."""
        chat_ref = target.chat_ref
        cache_key = (account_id, chat_ref)
        cached = self._entity_cache.get(cache_key)
        if cached is not None and cache_key in self._joined:
            return (cached, False) if report_join else cached

        from telethon import errors, functions, types  # noqa: PLC0415 - heavy lazy import

        try:
            if target.folder_slug and target.telegram_peer_id is not None:
                entity, joined = await self._resolve_folder_target(
                    client,
                    account_id,
                    target,
                    allow_join=True,
                )
            elif chat_ref.startswith("https://t.me/+"):
                entity, joined = await self._join_private_invite(client, account_id, chat_ref)
            else:
                entity = await self._resolve_entity(client, account_id, chat_ref)
                self._ensure_can_send(entity)
                if isinstance(entity, types.User):
                    raise PermanentGatewayError(
                        "адрес ведёт на пользователя, а не на группу Telegram",
                        code="target_unavailable",
                    )
                if isinstance(entity, types.ChatForbidden | types.ChannelForbidden):
                    raise PermanentGatewayError(
                        "чат недоступен этому аккаунту",
                        code="target_unavailable",
                    )
                if isinstance(entity, types.Chat):
                    if bool(getattr(entity, "left", False)) or bool(
                        getattr(entity, "deactivated", False)
                    ):
                        raise PermanentGatewayError(
                            "аккаунт не состоит в этой группе; нужна новая invite-ссылка",
                            code="target_not_joined",
                        )
                    joined = False
                else:
                    try:
                        await client(functions.channels.GetParticipantRequest(entity, "me"))
                        joined = False
                    except errors.UserNotParticipantError:
                        await self._enforce_join_limit(account_id)
                        try:
                            await client(functions.channels.JoinChannelRequest(entity))
                            joined = True
                        except errors.UserAlreadyParticipantError:
                            joined = False
        except errors.FloodWaitError as exc:
            raise self._map_rpc_error(exc, context="join") from exc
        except (ValueError, TypeError) as exc:
            raise PermanentGatewayError(f"цель {chat_ref!r} не найдена или недоступна") from exc
        except Exception as exc:  # noqa: BLE001 - normalise into safe gateway errors
            raise self._map_rpc_error(exc, context="join") from exc

        if joined:
            await self._account_service.record_join(account_id)
        self._entity_cache[cache_key] = entity
        self._joined.add(cache_key)
        return (entity, joined) if report_join else entity

    async def _resolve_folder_target(
        self,
        client,
        account_id: int,
        target: Target,
        *,
        allow_join: bool,
    ) -> tuple[object, bool]:
        from telethon import errors, functions, utils  # noqa: PLC0415
        from telethon.tl.types import (  # noqa: PLC0415
            ChannelForbidden,
            ChatForbidden,
            InputChatlistDialogFilter,
        )

        slug = target.folder_slug or ""
        target_peer_id = int(target.telegram_peer_id or 0)
        if target.chat_ref.startswith("@"):
            try:
                public_entity = await self._resolve_entity(client, account_id, target.chat_ref)
                self._ensure_can_send(public_entity)
                await client(functions.channels.GetParticipantRequest(public_entity, "me"))
                return public_entity, False
            except errors.UserNotParticipantError:
                # A public group can be joined directly and does not need an
                # imported-folder slot.  The previous folder-only request was
                # the source of CHATLISTS_TOO_MUCH/BAD_REQUEST on real launches.
                if not allow_join:
                    raise PermanentGatewayError(
                        "аккаунт не состоит в этой публичной группе",
                        code="target_not_joined",
                    ) from None
                await self._enforce_join_limit(account_id)
                try:
                    await client(functions.channels.JoinChannelRequest(public_entity))
                except errors.UserAlreadyParticipantError:
                    return public_entity, False
                return public_entity, True
            except PermanentGatewayError as exc:
                # A folder may be the only route when the public address cannot
                # be resolved by this account.  Do not hide a real permission
                # problem in an already resolved destination.
                if getattr(exc, "code", None) != "target_unavailable":
                    raise

        try:
            invite = await client(functions.chatlists.CheckChatlistInviteRequest(slug=slug))
        except Exception as exc:
            rpc_message = str(getattr(exc, "message", "") or "").upper()
            if rpc_message in {"INVITE_SLUG_EMPTY", "INVITE_SLUG_EXPIRED"}:
                existing = await self._find_joined_dialog(client, target_peer_id)
                if existing is not None:
                    self._ensure_can_send(existing)
                    return existing, False
            raise
        chats = list(getattr(invite, "chats", None) or [])
        chat_by_id = {utils.get_peer_id(chat): chat for chat in chats}
        chat = chat_by_id.get(target_peer_id)
        if chat is None or isinstance(chat, ChannelForbidden | ChatForbidden):
            raise PermanentGatewayError(
                "чат больше не доступен через папку Telegram",
                code="target_unavailable",
            )
        self._ensure_can_send(chat)
        input_peer = utils.get_input_peer(chat)
        peers = list(getattr(invite, "peers", None) or [])
        missing_peers = list(getattr(invite, "missing_peers", None) or [])
        already_peers = list(getattr(invite, "already_peers", None) or [])
        peer_ids = {utils.get_peer_id(peer) for peer in peers}
        missing_ids = {utils.get_peer_id(peer) for peer in missing_peers}
        already_ids = {utils.get_peer_id(peer) for peer in already_peers}
        if target_peer_id in already_ids or getattr(chat, "left", None) is False:
            return chat, False
        if not allow_join:
            raise PermanentGatewayError(
                "аккаунт не состоит в группе из папки, а авто-вступление выключено",
                code="target_not_joined",
            )
        await self._enforce_join_limit(account_id)
        if target_peer_id in missing_ids:
            filter_id = getattr(invite, "filter_id", None)
            if filter_id is None:
                raise PermanentGatewayError(
                    "Telegram не вернул идентификатор импортированной папки",
                    code="target_unavailable",
                )
            updates = await client(
                functions.chatlists.JoinChatlistUpdatesRequest(
                    chatlist=InputChatlistDialogFilter(filter_id=int(filter_id)),
                    peers=[input_peer],
                )
            )
        elif target_peer_id in peer_ids:
            updates = await client(
                functions.chatlists.JoinChatlistInviteRequest(
                    slug=slug,
                    peers=[input_peer],
                )
            )
        else:
            raise PermanentGatewayError(
                "чат удалён из общей папки Telegram",
                code="target_unavailable",
            )
        joined_chats = list(getattr(updates, "chats", None) or [])
        joined = next(
            (item for item in joined_chats if utils.get_peer_id(item) == target_peer_id),
            chat,
        )
        return joined, True

    @staticmethod
    async def _find_joined_dialog(client: object, target_peer_id: int) -> object | None:
        from telethon import utils  # noqa: PLC0415

        async for dialog in client.iter_dialogs():  # type: ignore[attr-defined]
            entity = getattr(dialog, "entity", None)
            if entity is None:
                continue
            try:
                peer_id = utils.get_peer_id(entity)
            except (TypeError, ValueError):
                continue
            if peer_id == target_peer_id:
                return entity
        return None

    async def _join_private_invite(
        self,
        client,
        account_id: int,
        chat_ref: str,
    ) -> tuple[object, bool]:
        from telethon import functions, types  # noqa: PLC0415 - heavy lazy import

        invite_hash = chat_ref.split("+", 1)[1]
        info = await client(functions.messages.CheckChatInviteRequest(invite_hash))
        if isinstance(info, types.ChatInviteAlready):
            chat = getattr(info, "chat", None)
            if chat is None:
                raise PermanentGatewayError("приватный чат недоступен") from None
            return chat, False
        await self._enforce_join_limit(account_id)
        updates = await client(functions.messages.ImportChatInviteRequest(invite_hash))
        chats = getattr(updates, "chats", None) or []
        if not chats:
            raise TemporaryGatewayError("Telegram не вернул чат после вступления")
        return chats[0], True

    async def _resolve_channel_comment_channel(
        self,
        client: object,
        account_id: int,
        channel_ref: str,
        *,
        request_join: bool,
    ) -> object:
        """Resolve a configured comment channel without hidden join side effects.

        A private link is operator-provided configuration.  We may submit its
        join request during setup, but after Telegram responds with a pending
        request the monitor only checks for approval.  Once approved Telegram
        returns ``ChatInviteAlready`` and normal scanning resumes.
        """
        if not channel_ref.startswith("https://t.me/+"):
            return await self._resolve_entity(client, account_id, channel_ref)
        from telethon import functions, types  # noqa: PLC0415

        invite_hash = channel_ref.removeprefix("https://t.me/+")
        info = await client(functions.messages.CheckChatInviteRequest(invite_hash))  # type: ignore[operator]
        if isinstance(info, types.ChatInviteAlready):
            channel = getattr(info, "chat", None)
            if channel is None:
                raise PermanentGatewayError("приватный канал недоступен", code="target_unavailable")
            return channel
        if not request_join:
            raise PermanentGatewayError(
                "заявка на вступление отправлена и ожидает одобрения",
                code="join_request_pending",
            )
        channel, _ = await self._join_private_invite(client, account_id, channel_ref)
        return channel

    async def _enforce_join_limit(self, account_id: int) -> None:
        import math  # noqa: PLC0415

        wait = await self._account_service.join_wait_seconds(
            account_id,
            daily_limit=self._settings.account_join_daily_limit,
            min_interval=self._settings.account_join_min_interval_seconds,
        )
        if wait > 0:
            raise TemporaryGatewayError(
                "join throttled", retry_after=math.ceil(wait), code="join_throttled"
            )

    async def _respect_chat_slow_mode(
        self,
        client: object,
        account_id: int,
        target_id: int,
        entity: object,
        *,
        known_timer_checked: bool = False,
    ) -> int | None:
        if not known_timer_checked:
            await self._respect_known_chat_timer(account_id, target_id)

        if not bool(getattr(entity, "megagroup", False)):
            return None
        if (
            bool(getattr(entity, "creator", False))
            or getattr(entity, "admin_rights", None) is not None
        ):
            return None

        from telethon import functions  # noqa: PLC0415

        try:
            result = await client(  # type: ignore[operator]
                functions.channels.GetFullChannelRequest(entity)
            )
        except Exception as exc:  # noqa: BLE001
            raise self._map_rpc_error(exc, context="slow_mode_check") from exc
        full_chat = getattr(result, "full_chat", None)
        raw_seconds = getattr(full_chat, "slowmode_seconds", None)
        slowmode_seconds = int(raw_seconds) if raw_seconds else None
        raw_next = getattr(full_chat, "slowmode_next_send_date", None)
        if isinstance(raw_next, datetime):
            next_send_at = self._as_utc(raw_next)
        else:
            next_send_at = datetime.fromtimestamp(int(raw_next), tz=UTC) if raw_next else None
        await self._record_slow_mode_check(
            account_id,
            target_id,
            slowmode_seconds=slowmode_seconds,
            next_send_at=next_send_at,
        )
        now = utc_now()
        if next_send_at is not None and next_send_at > now:
            raise TemporaryGatewayError(
                "в чате действует медленный режим",
                retry_after=max(1, math.ceil((next_send_at - now).total_seconds())),
                code="slow_mode",
            )
        return slowmode_seconds

    async def _respect_known_chat_timer(
        self,
        account_id: int,
        target_id: int,
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is not None and state.send_blocked:
                reason = state.blocked_reason or "account_banned_in_chat"
                # Keep the exact durable reason. Only USER_BANNED_IN_CHANNEL
                # is a confirmed ban; rights and write restrictions must not
                # turn into a false "banned in chat" report on later retries.
                code = (
                    reason
                    if reason
                    in {
                        "account_banned_in_chat",
                        "account_chat_access_forbidden",
                        "account_chat_access_lost",
                        "account_missing_post_rights",
                        "account_missing_admin_rights",
                        "account_send_forbidden",
                        "account_join_limit",
                        "target_unavailable",
                    }
                    else "account_chat_access_forbidden"
                )
                raise PermanentGatewayError(
                    f"эта пара аккаунт–чат заблокирована: {reason}",
                    code=code,
                )
            known_next_send = (
                self._as_utc(state.next_send_at)
                if state is not None and state.next_send_at is not None
                else None
            )
        if known_next_send is not None and known_next_send > now:
            raise TemporaryGatewayError(
                "таймер этого чата ещё не истёк",
                retry_after=max(
                    1,
                    math.ceil((known_next_send - now).total_seconds()),
                ),
                code="chat_timer",
            )

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    async def _record_slow_mode_check(
        self,
        account_id: int,
        target_id: int,
        *,
        slowmode_seconds: int | None,
        next_send_at: datetime | None,
    ) -> None:
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                state = TargetAccountState(
                    account_id=account_id,
                    target_id=target_id,
                )
                session.add(state)
            state.slowmode_seconds = slowmode_seconds
            state.next_send_at = next_send_at
            state.last_checked_at = utc_now()
            await session.commit()

    async def _record_chat_delivery(
        self,
        account_id: int,
        target_id: int,
        entity: object,
        message_id: int,
        slowmode_seconds: int | None,
    ) -> None:
        from telethon import utils  # noqa: PLC0415

        now = utc_now()
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                state = TargetAccountState(
                    account_id=account_id,
                    target_id=target_id,
                )
                session.add(state)
            state.slowmode_seconds = slowmode_seconds
            effective_cooldown = max(
                int(slowmode_seconds or 0),
                int(target.manual_cooldown_seconds if target is not None else 0),
            )
            state.next_send_at = (
                now + timedelta(seconds=effective_cooldown) if effective_cooldown else None
            )
            state.last_checked_at = now
            state.last_success_at = now
            state.last_message_id = message_id
            state.last_error = None
            if target is not None and target.telegram_peer_id is None:
                try:
                    target.telegram_peer_id = utils.get_peer_id(entity)
                except (TypeError, ValueError):
                    pass
            await session.commit()

    async def _record_runtime_chat_timer(
        self,
        account_id: int,
        target_id: int,
        retry_after: int,
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                state = TargetAccountState(
                    account_id=account_id,
                    target_id=target_id,
                )
                session.add(state)
            state.slowmode_seconds = max(
                int(state.slowmode_seconds or 0),
                max(1, retry_after),
            )
            detected_next = now + timedelta(seconds=max(1, retry_after))
            current_next = (
                self._as_utc(state.next_send_at) if state.next_send_at is not None else None
            )
            state.next_send_at = max(current_next, detected_next) if current_next else detected_next
            state.last_checked_at = now
            await session.commit()

    async def _show_typing_before_send(
        self,
        client: object,
        entity: object,
        *,
        text: str = "",
    ) -> None:
        """Show Telegram's native typing status for a bounded text-length pause.

        Telegram exposes a typing event, not UI-click events.  The pause is kept
        inside the account lock so a later send from the same account cannot
        overtake the message that is currently being prepared.  It is based on
        the final visible text, not its markup, and awaits normally so worker
        cancellation interrupts it immediately.
        """
        if not self._settings.account_typing_enabled:
            return
        minimum = self._settings.account_typing_min_seconds
        maximum = self._settings.account_typing_max_seconds
        if maximum <= 0:
            return
        characters_per_second = self._settings.account_typing_characters_per_second
        visible_characters = len(text.strip())
        duration = min(
            maximum,
            max(minimum, visible_characters / characters_per_second),
        )
        if duration <= 0:
            return

        from telethon import functions, types, utils  # noqa: PLC0415

        remaining = duration
        while remaining > 0:
            try:
                await client(  # type: ignore[operator]
                    functions.messages.SetTypingRequest(
                        peer=utils.get_input_peer(entity),
                        action=types.SendMessageTypingAction(),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - typing must not hide real send errors
                mapped = self._map_rpc_error(exc, context="typing")
                if getattr(mapped, "code", None) in {
                    "account_dead",
                    "account_restricted",
                    "flood_wait",
                    "slow_mode",
                }:
                    raise mapped from exc
                logger.debug(
                    "typing status was unavailable; proceeding with the message",
                    exc_info=True,
                )
                return
            pause = min(4.0, remaining)
            await asyncio.sleep(pause)
            remaining -= pause

    @staticmethod
    async def _parse_html_message(client: object, html_text: str) -> tuple[str, list[object]]:
        """Use Telethon's full parser so tg:// user links become real mention entities."""

        from telethon.extensions import html  # noqa: PLC0415

        parser = getattr(client, "_parse_message_text", None)
        if callable(parser):
            message, entities = await parser(html_text, "html")
            return str(message), list(entities)
        # Lightweight test doubles may not expose Telethon's private parser.
        message, entities = html.parse(html_text)
        return str(message), list(entities)

    async def _send(
        self,
        client,
        entity,
        text: str,
        reply_to_message_id: int | None,
        idempotency_key: str,
        *,
        html_text: str | None = None,
        error_context: str = "send",
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> SentMessage:
        from telethon import functions, types, utils  # noqa: PLC0415

        random_id = self._message_random_id(idempotency_key)
        message_text = text
        entities = None
        if html_text:
            message_text, entities = await self._parse_html_message(client, html_text)
        request = functions.messages.SendMessageRequest(
            peer=utils.get_input_peer(entity),
            message=message_text,
            random_id=random_id,
            reply_to=(
                types.InputReplyToMessage(reply_to_msg_id=reply_to_message_id)
                if reply_to_message_id is not None
                else None
            ),
            entities=entities,
        )
        _, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            if error_context != "direct_message":
                await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id, simulated=False)
        if before_send is not None:
            await before_send()
        try:
            result = await client(request)
        except Exception as exc:  # noqa: BLE001 - normalise into safe gateway errors
            mapped = self._map_rpc_error(exc, context=error_context)
            if getattr(mapped, "code", None) in {
                "network_error",
                "rpc_error",
            }:
                raise PermanentGatewayError(
                    f"ответ Telegram потерян после отправки запроса: {mapped}",
                    code="delivery_unconfirmed",
                ) from exc
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                raise PermanentGatewayError(
                    "Telegram принял повторный random_id, но ID сообщения не подтверждён; "
                    "совпадение текста в истории не доказывает отправку",
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise PermanentGatewayError(
                "Telegram не вернул идентификатор сообщения", code="delivery_unconfirmed"
            )
        await self._remember_delivery_message_id(idempotency_key, message_id)
        if error_context != "direct_message":
            await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id, simulated=False)

    async def _send_contact(
        self,
        client: object,
        entity: object,
        contact: StoredContact,
        reply_to_message_id: int | None,
        idempotency_key: str,
        *,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> SentMessage:
        from telethon import functions, types, utils  # noqa: PLC0415

        random_id = self._message_random_id(idempotency_key)
        request = functions.messages.SendMediaRequest(
            peer=utils.get_input_peer(entity),
            media=types.InputMediaContact(
                phone_number=contact.phone_number,
                first_name=contact.first_name,
                last_name=contact.last_name,
                vcard=contact.vcard,
            ),
            message="",
            random_id=random_id,
            reply_to=(
                types.InputReplyToMessage(reply_to_msg_id=reply_to_message_id)
                if reply_to_message_id is not None
                else None
            ),
        )
        _, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id, simulated=False)
        if before_send is not None:
            await before_send()
        try:
            result = await client(request)  # type: ignore[operator]
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_rpc_error(exc, context="send_contact")
            if getattr(mapped, "code", None) in {"network_error", "rpc_error"}:
                raise PermanentGatewayError(
                    "ответ Telegram потерян после отправки контакта", code="delivery_unconfirmed"
                ) from mapped
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                raise PermanentGatewayError(
                    "Telegram уже принял контакт, но его ID не подтверждён; повтор запрещён",
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise PermanentGatewayError(
                "Telegram не вернул идентификатор контакта", code="delivery_unconfirmed"
            )
        await self._remember_delivery_message_id(idempotency_key, message_id)
        await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id, simulated=False)

    async def _send_media(
        self,
        client,
        entity: object,
        text: str,
        reply_to_message_id: int | None,
        idempotency_key: str,
        *,
        html_text: str | None,
        media_path: str,
        media_kind: str,
        error_context: str = "send_media",
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> SentMessage:
        from telethon import functions, types, utils  # noqa: PLC0415

        message_text = text
        entities = None
        if html_text:
            message_text, entities = await self._parse_html_message(client, html_text)
        _, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            if error_context != "direct_message":
                await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id, simulated=False)
        dispatched = False
        try:
            media = await self._prepare_uploaded_media(
                client,
                media_path,
                media_kind,
            )
            random_id = self._message_random_id(idempotency_key)
            if before_send is not None:
                await before_send()
            dispatched = True
            result = await client(
                functions.messages.SendMediaRequest(
                    peer=utils.get_input_peer(entity),
                    media=media,
                    message=message_text,
                    random_id=random_id,
                    reply_to=(
                        types.InputReplyToMessage(reply_to_msg_id=reply_to_message_id)
                        if reply_to_message_id is not None
                        else None
                    ),
                    entities=entities,
                )
            )
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_rpc_error(exc, context=error_context)
            if dispatched and getattr(mapped, "code", None) in {
                "network_error",
                "rpc_error",
            }:
                raise PermanentGatewayError(
                    f"ответ Telegram потерян после отправки вложения: {mapped}",
                    code="delivery_unconfirmed",
                ) from exc
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                raise PermanentGatewayError(
                    "Telegram уже принял медиа, но его ID не подтверждён; повтор запрещён",
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise PermanentGatewayError(
                "Telegram не вернул идентификатор медиа-сообщения", code="delivery_unconfirmed"
            )
        await self._remember_delivery_message_id(idempotency_key, message_id)
        if error_context != "direct_message":
            await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id, simulated=False)

    async def _send_album(
        self,
        client,
        entity: object,
        text: str,
        reply_to_message_id: int | None,
        idempotency_key: str,
        *,
        html_text: str | None,
        media_items: list[tuple[str, str]],
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> SentMessage:
        from telethon import functions, types, utils  # noqa: PLC0415

        caption = text
        caption_entities = None
        if html_text:
            caption, caption_entities = await self._parse_html_message(client, html_text)
        single_media: list[object] = []
        random_ids: list[int] = []
        sent_after, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id, simulated=False)
        dispatched = False
        try:
            input_peer = utils.get_input_peer(entity)
            for index, (media_path, media_kind) in enumerate(media_items):
                uploaded_media = await self._prepare_uploaded_media(
                    client,
                    media_path,
                    media_kind,
                )
                uploaded_result = await client(
                    functions.messages.UploadMediaRequest(
                        peer=input_peer,
                        media=uploaded_media,
                    )
                )
                media = utils.get_input_media(uploaded_result)
                random_id = self._message_random_id(f"{idempotency_key}:album:{index}")
                random_ids.append(random_id)
                single_media.append(
                    types.InputSingleMedia(
                        media=media,
                        message=caption if index == 0 else "",
                        random_id=random_id,
                        entities=caption_entities if index == 0 else None,
                    )
                )
            if before_send is not None:
                await before_send()
            dispatched = True
            result = await client(
                functions.messages.SendMultiMediaRequest(
                    peer=input_peer,
                    multi_media=single_media,
                    reply_to=(
                        types.InputReplyToMessage(reply_to_msg_id=reply_to_message_id)
                        if reply_to_message_id is not None
                        else None
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_rpc_error(exc, context="send_album")
            if dispatched and getattr(mapped, "code", None) in {"network_error", "rpc_error"}:
                raise PermanentGatewayError(
                    "ответ Telegram потерян после отправки альбома", code="delivery_unconfirmed"
                ) from mapped
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                # A history entry cannot prove ownership of an album: captions
                # are not unique and a grouped message does not expose this
                # request's random IDs.  Associating a lookalike album would
                # silently attach reactions and reports to another post.
                # Preserve the uncertain outcome rather than guessing.
                raise PermanentGatewayError(
                    "Telegram мог принять альбом, но его состав нельзя безопасно подтвердить",
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_ids = self._sent_message_ids(result, set(random_ids))
        if len(message_ids) != len(random_ids):
            raise PermanentGatewayError(
                "Telegram не подтвердил все элементы альбома; повтор запрещён",
                code="delivery_unconfirmed",
                external_message_id=max(message_ids) if message_ids else None,
            )
        missing_ids = []
        for message_id in message_ids:
            try:
                await self._verify_delivery(client, entity, message_id)
            except GatewayError as exc:
                if exc.code == "post_deleted_by_moderation":
                    missing_ids.append(message_id)
                else:
                    raise PermanentGatewayError(
                        "Telegram принял альбом, но проверка его состава недоступна; "
                        "повтор запрещён",
                        code="delivery_unconfirmed",
                        external_message_ids=tuple(message_ids),
                    ) from exc
        if missing_ids:
            raise PermanentGatewayError(
                f"Из {len(message_ids)} отправленных элементов отсутствуют {len(missing_ids)}; "
                "причина удаления неизвестна",
                code="album_partial_missing"
                if len(missing_ids) < len(message_ids)
                else "post_deleted_by_moderation",
                external_message_id=max(message_ids),
                external_message_ids=tuple(message_ids),
            )
        last_message_id = max(message_ids)
        await self._remember_delivery_message_id(idempotency_key, last_message_id)
        return SentMessage(
            message_id=last_message_id, simulated=False, message_ids=tuple(message_ids)
        )

    async def _prepare_story_media(self, client, media_path: str, media_kind: str):
        from io import BytesIO
        from pathlib import Path

        from telethon import types

        from proxima_traffic.story_content import prepare_story_photo, story_video_attributes

        try:
            if media_kind == "video":
                attributes, mime_type = await asyncio.to_thread(story_video_attributes, media_path)
            else:
                prepared = await asyncio.to_thread(
                    lambda: prepare_story_photo(Path(media_path).read_bytes()),
                )
        except (ContentError, OSError) as exc:
            raise PermanentGatewayError(
                "Не удалось подготовить медиа истории: " + str(exc),
                code="content_invalid",
            ) from exc
        if media_kind == "video":
            uploaded = await client.upload_file(media_path)
            return types.InputMediaUploadedDocument(
                file=uploaded,
                mime_type=mime_type,
                attributes=attributes,
            )
        with BytesIO(prepared) as source:
            source.name = "story.jpg"
            return await self._prepare_uploaded_media(client, source, media_kind)

    @staticmethod
    async def _prepare_uploaded_media(client, media_path, media_kind: str):
        """Build InputMedia through Telethon's public upload API."""
        from telethon import types, utils  # noqa: PLC0415

        uploaded = await client.upload_file(media_path)
        if media_kind == "photo":
            return types.InputMediaUploadedPhoto(file=uploaded)
        attributes, mime_type = utils.get_attributes(
            media_path,
            force_document=media_kind in {"document", "voice", "video_note"},
            voice_note=media_kind == "voice",
            video_note=media_kind == "video_note",
            supports_streaming=media_kind in {"video", "video_note"},
        )
        return types.InputMediaUploadedDocument(
            file=uploaded,
            mime_type=mime_type,
            attributes=attributes,
            force_file=media_kind in {"document", "voice", "video_note"},
        )

    async def _delivery_receipt(self, idempotency_key: str) -> tuple[datetime, int | None]:
        """Return the first durable attempt and any already known Telegram message ID."""

        session_factory = getattr(self, "_session_factory", None)
        if session_factory is None:
            return utc_now(), None
        receipt_key = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        first_attempt_at = utc_now()
        async with session_factory() as session:
            await session.execute(
                insert(TelegramDeliveryReceipt)
                .values(
                    idempotency_key=receipt_key,
                    first_attempt_at=first_attempt_at,
                    created_at=first_attempt_at,
                    updated_at=first_attempt_at,
                )
                .prefix_with("OR IGNORE")
            )
            await session.commit()
            receipt = await session.scalar(
                select(TelegramDeliveryReceipt).where(
                    TelegramDeliveryReceipt.idempotency_key == receipt_key
                )
            )
        if receipt is None:
            raise TemporaryGatewayError(
                "Не удалось закрепить идемпотентную попытку доставки",
                retry_after=30,
                code="delivery_unconfirmed",
            )
        started_at = ensure_utc(receipt.first_attempt_at) or first_attempt_at
        message_id = (
            int(receipt.external_message_id) if receipt.external_message_id is not None else None
        )
        return started_at, message_id

    async def _remember_delivery_message_id(
        self,
        idempotency_key: str,
        message_id: int,
    ) -> None:
        session_factory = getattr(self, "_session_factory", None)
        if session_factory is None:
            return
        receipt_key = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        async with session_factory() as session:
            await session.execute(
                sql_update(TelegramDeliveryReceipt)
                .where(TelegramDeliveryReceipt.idempotency_key == receipt_key)
                .values(external_message_id=message_id, updated_at=utc_now())
            )
            await session.commit()

    @staticmethod
    def _message_random_id(idempotency_key: str) -> int:
        digest = hashlib.blake2b(
            idempotency_key.encode("utf-8"),
            digest_size=8,
            person=b"proxima",
        ).digest()
        return int.from_bytes(digest, byteorder="big", signed=True) or 1

    @staticmethod
    def _sent_message_id(result: object, random_id: int) -> int | None:
        direct_id = getattr(result, "id", None)
        if direct_id is not None:
            return int(direct_id)
        updates = list(getattr(result, "updates", None) or [])
        for update in updates:
            if (
                type(update).__name__ == "UpdateMessageID"
                and getattr(update, "random_id", None) == random_id
            ):
                return int(update.id)
        # Other message updates can share this envelope without belonging to
        # the outgoing request. Only its random_id establishes that connection.
        return None

    @staticmethod
    def _sent_message_ids(result: object, random_ids: set[int]) -> list[int]:
        found: set[int] = set()
        updates = list(getattr(result, "updates", None) or [])
        for update in updates:
            if (
                type(update).__name__ == "UpdateMessageID"
                and getattr(update, "random_id", None) in random_ids
            ):
                found.add(int(update.id))
        return sorted(found)

    @staticmethod
    def _ensure_can_send(
        entity: object,
        *,
        media_kinds: frozenset[str] = frozenset(),
    ) -> None:
        is_supergroup = bool(getattr(entity, "megagroup", False))
        is_broadcast = bool(getattr(entity, "broadcast", False)) and not is_supergroup
        creator = bool(getattr(entity, "creator", False))
        admin_rights = getattr(entity, "admin_rights", None)
        if is_broadcast and not (
            creator or (admin_rights is not None and getattr(admin_rights, "post_messages", False))
        ):
            raise PermanentGatewayError(
                "аккаунт не может публиковать сообщения в этом канале",
                code="account_missing_post_rights",
            )
        if creator or admin_rights is not None:
            return
        banned = getattr(entity, "banned_rights", None)
        default_banned = getattr(entity, "default_banned_rights", None)
        if banned is not None and (
            getattr(banned, "send_messages", False)
            or getattr(banned, "send_plain", False)
            or getattr(banned, "view_messages", False)
        ):
            raise PermanentGatewayError(
                "этому аккаунту запрещена отправка сообщений в чате",
                code="account_send_forbidden",
            )
        if default_banned is not None and (
            getattr(default_banned, "send_messages", False)
            or getattr(default_banned, "send_plain", False)
            or getattr(default_banned, "view_messages", False)
        ):
            raise PermanentGatewayError(
                "в чате глобально запрещена отправка обычных сообщений",
                code="target_unavailable",
            )
        if not media_kinds:
            return
        media_flags = {"send_media"}
        if "photo" in media_kinds:
            media_flags.add("send_photos")
        if "video" in media_kinds:
            media_flags.add("send_videos")
        if "document" in media_kinds:
            media_flags.add("send_docs")
        if "voice" in media_kinds:
            media_flags.add("send_voices")
        if "video_note" in media_kinds:
            media_flags.add("send_roundvideos")
        if banned is not None and any(bool(getattr(banned, flag, False)) for flag in media_flags):
            raise PermanentGatewayError(
                "этому аккаунту запрещено отправлять выбранный тип медиа в чат",
                code="content_forbidden",
            )
        if default_banned is not None and any(
            bool(getattr(default_banned, flag, False)) for flag in media_flags
        ):
            raise PermanentGatewayError(
                "в чате запрещён выбранный тип медиа",
                code="content_forbidden",
            )

    async def _verify_delivery(self, client, entity, message_id: int) -> None:
        """Confirm that Telegram still exposes the message after the send response."""
        if not self._settings.account_delivery_verify_enabled:
            return
        delay = float(self._settings.account_delivery_verify_delay_seconds)
        if delay > 0:
            await asyncio.sleep(delay)

        async def read_message():
            try:
                result = await self._fetch_message_for_verification(client, entity, message_id)
            except Exception as exc:  # noqa: BLE001 - the send response already contained an ID
                raise PermanentGatewayError(
                    "Telegram принял публикацию, но её наличие нельзя проверить; повтор запрещён",
                    code="delivery_unconfirmed",
                    external_message_id=message_id,
                ) from exc
            if result is not None and not self._message_is_present(result, message_id):
                if (
                    type(result).__name__ != "MessageEmpty"
                    or getattr(result, "id", None) != message_id
                ):
                    raise PermanentGatewayError(
                        "Telegram вернул неоднозначный результат чтения публикации",
                        code="delivery_unconfirmed",
                        external_message_id=message_id,
                    )
            return result

        fetched = await read_message()
        if self._message_is_present(fetched, message_id):
            return

        # A second probe separates a real moderation deletion from a short-lived
        # read-after-write delay. Only two confirmed misses quarantine the chat.
        confirm_delay = float(self._settings.account_delivery_verify_confirm_delay_seconds)
        if confirm_delay > 0:
            await asyncio.sleep(confirm_delay)
        fetched = await read_message()
        if self._message_is_present(fetched, message_id):
            return
        notice: str | None = None
        try:
            recent = await asyncio.wait_for(
                client.get_messages(entity, limit=12),
                timeout=float(self._settings.account_delivery_verify_timeout_seconds),
            )
            notice = self._moderation_notice(
                list(recent) if recent is not None else [],
                after_message_id=message_id,
            )
        except Exception:  # noqa: BLE001 - the deletion is already confirmed
            notice = None
        raise self._deleted_post_error(message_id, notice=notice)

    async def _verify_direct_delivery(
        self,
        client,
        entity,
        message_id: int,
        *,
        expected_text: str | None = None,
        expect_media: bool = False,
    ) -> None:
        """Confirm an outgoing private message; an unreadable result stays unknown."""
        if message_id <= 0:
            raise PermanentGatewayError(
                "Telegram не вернул корректный ID личного сообщения",
                code="delivery_unconfirmed",
            )
        delays = (
            float(self._settings.account_delivery_verify_delay_seconds),
            float(self._settings.account_delivery_verify_confirm_delay_seconds),
        )
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                message = await self._fetch_message_for_verification(client, entity, message_id)
            except Exception as exc:  # noqa: BLE001 - request may already have been accepted
                raise PermanentGatewayError(
                    "Telegram принял отправку, но проверка личного сообщения не удалась",
                    code="delivery_unconfirmed",
                ) from exc
            if (
                self._message_is_present(message, message_id)
                and getattr(message, "out", False)
                and (expected_text is None or getattr(message, "message", None) == expected_text)
                and (not expect_media or getattr(message, "media", None) is not None)
            ):
                return
        raise PermanentGatewayError(
            "Telegram ответил на отправку, но исходящее сообщение не найдено в диалоге",
            code="delivery_unconfirmed",
        )

    async def _send_reaction_with_fallback(
        self,
        client,
        entity,
        message_id: int,
        preferred_emoji: str,
    ) -> str:
        """Try the configured emoji, then ordinary fallbacks rejected only by Telegram.

        A fallback is deliberately limited to the four emoji exposed in the
        control panel. Network failures, flood waits, missing access and an
        unconfirmed delivery are never masked by switching emoji.
        """
        last_unavailable: GatewayError | None = None
        for emoji in reaction_candidates(preferred_emoji):
            try:
                await self._send_and_verify_reaction(client, entity, message_id, emoji)
                return emoji
            except Exception as exc:  # noqa: BLE001 - normalize only this reaction attempt
                mapped = (
                    exc
                    if isinstance(exc, GatewayError)
                    else self._map_rpc_error(
                        exc,
                        context="reaction",
                    )
                )
                if getattr(mapped, "code", None) != "reaction_unavailable":
                    if mapped is exc:
                        raise
                    raise mapped from exc
                last_unavailable = mapped
        if last_unavailable is not None:
            raise last_unavailable
        raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")

    async def _send_and_verify_reaction(self, client, entity, message_id: int, emoji: str) -> None:
        """Set a reaction and require Telegram to show it for the reacting account.

        ``SendReactionRequest`` returning ``Updates`` only means that Telegram
        accepted the RPC. It did not previously prove that the selected reaction
        became visible on the message, which let a task be marked successful
        without an operator-observable result. Repeating an identical reaction
        is idempotent for one account, so an unconfirmed result can safely be
        retried by the durable reaction worker.
        """
        from telethon import functions, types, utils  # noqa: PLC0415

        await client(
            functions.messages.SendReactionRequest(
                peer=utils.get_input_peer(entity),
                msg_id=message_id,
                reaction=[types.ReactionEmoji(emoticon=emoji)],
                big=False,
                add_to_recent=False,
            )
        )
        await self._verify_reaction_delivery(client, entity, message_id, emoji)

    async def _verify_reaction_delivery(self, client, entity, message_id: int, emoji: str) -> None:
        """Confirm that this account's requested reaction is visible in Telegram."""
        if not self._settings.account_delivery_verify_enabled:
            return
        delays = (
            float(self._settings.account_delivery_verify_delay_seconds),
            float(self._settings.account_delivery_verify_confirm_delay_seconds),
        )
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            fetched = await self._fetch_message_for_verification(client, entity, message_id)
            if self._reaction_is_selected(fetched, message_id, emoji):
                return
        raise TemporaryGatewayError(
            "Telegram принял реакцию, но не подтвердил её отображение на сообщении",
            retry_after=max(10, int(self._settings.worker_retry_base_seconds)),
            code="reaction_delivery_unconfirmed",
        )

    async def _fetch_message_for_verification(self, client, entity, message_id: int):
        try:
            return await asyncio.wait_for(
                client.get_messages(entity, ids=message_id),
                timeout=float(self._settings.account_delivery_verify_timeout_seconds),
            )
        except Exception as exc:  # noqa: BLE001 - normalise into safe gateway errors
            mapped = self._map_rpc_error(exc, context="verify")
            raise mapped from exc

    @staticmethod
    def _message_is_present(message: object | None, message_id: int) -> bool:
        return bool(
            message is not None
            and getattr(message, "id", None) == message_id
            and type(message).__name__ != "MessageEmpty"
        )

    @classmethod
    def _reaction_is_selected(cls, message: object | None, message_id: int, emoji: str) -> bool:
        """Return whether the current account selected ``emoji`` on this message."""
        if not cls._message_is_present(message, message_id):
            return False
        reactions = getattr(message, "reactions", None)
        for result in getattr(reactions, "results", None) or ():
            reaction = getattr(result, "reaction", None)
            if (
                getattr(reaction, "emoticon", None) == emoji
                and getattr(result, "chosen_order", None) is not None
            ):
                return True
        return False

    @staticmethod
    def _moderation_notice(messages: list[object], *, after_message_id: int) -> str | None:
        markers = (
            "чтобы писать",
            "для того чтобы",
            "для отправки сообщений",
            "подпиш",
            "вступ",
            "присоед",
            "пройдите провер",
            "пройти провер",
            "подтверд",
            "необходимо",
            "капч",
            "правила чата",
            "щоб писати",
            "підпиш",
            "приєднай",
            "to send messages",
            "before sending",
            "must subscribe",
            "must join",
            "join the channel",
            "verify yourself",
            "verification required",
            "captcha",
            "read the rules",
        )
        candidates: list[tuple[int, str]] = []
        for message in messages:
            raw_id = getattr(message, "id", None)
            if raw_id is None or int(raw_id) <= after_message_id:
                continue
            text = " ".join(str(getattr(message, "message", "") or "").split())
            if not text or not any(marker in text.casefold() for marker in markers):
                continue
            candidates.append((int(raw_id), text[:300]))
        if not candidates:
            return None
        return min(candidates, key=lambda item: item[0])[1]

    @staticmethod
    def _deleted_post_error(message_id: int, *, notice: str | None) -> PermanentGatewayError:
        message = "публикация отсутствует после двух проверок; причина удаления не подтверждена"
        if notice:
            message += f"; ответ чата: {notice}"
        return PermanentGatewayError(
            message,
            code="post_deleted_by_moderation",
            external_message_id=message_id,
        )

    def _map_rpc_error(self, exc: Exception, *, context: str) -> Exception:
        mapped = self._classify_rpc_error(exc, context=context)
        if isinstance(mapped, GatewayError) and mapped is not exc:
            # Only retain protocol identifiers, never request objects/arguments
            # (they can contain message text, phone numbers or proxy credentials).
            rpc_name = type(exc).__name__
            rpc_message = str(getattr(exc, "message", "") or "")
            if not re.fullmatch(r"[A-Z0-9_ ]{1,120}", rpc_message):
                rpc_message = ""
            detail = rpc_name + (f" / {rpc_message}" if rpc_message else "")
            mapped.safe_message = f"{mapped.safe_message} [Telegram: {detail}]"[:500]
            mapped.args = (mapped.safe_message,)
        return mapped

    def _classify_rpc_error(self, exc: Exception, *, context: str) -> Exception:
        import math  # noqa: PLC0415

        from telethon import errors  # noqa: PLC0415

        if isinstance(exc, GatewayError):
            return exc
        rpc_message = str(getattr(exc, "message", "") or "").upper()
        rpc_code = int(getattr(exc, "code", 0) or 0)
        if isinstance(exc, errors.RPCError) and rpc_message in {
            "FORBIDDEN",
            "BAD_REQUEST",
            "UNAUTHORIZED",
            "FLOOD",
            "INTERNAL",
        }:
            # Typed Telethon exceptions often expose only "FORBIDDEN" as
            # message. Their class still carries the precise Telegram error.
            typed_name = type(exc).__name__.removesuffix("Error")
            if typed_name not in {"Forbidden", "BadRequest", "Unauthorized", "Flood", "Server"}:
                rpc_message = re.sub(r"(?<!^)(?=[A-Z])", "_", typed_name).upper()
        if isinstance(exc, errors.SlowModeWaitError):
            return TemporaryGatewayError(
                f"slow mode {exc.seconds}s", retry_after=int(exc.seconds), code="slow_mode"
            )
        flood_seconds = getattr(exc, "seconds", None)
        raw_flood = re.fullmatch(r"FLOOD_(?:PREMIUM_)?WAIT_(\d+)", rpc_message)
        if flood_seconds is None and raw_flood:
            flood_seconds = int(raw_flood.group(1))
        if (
            isinstance(exc, errors.FloodWaitError)
            or (isinstance(exc, errors.FloodError) and flood_seconds is not None)
            or raw_flood
        ):
            buffered = math.ceil(int(flood_seconds) * self._settings.flood_buffer_multiplier)
            # A server-mandated FLOOD_WAIT must never be shortened by a local
            # scheduling cap. The buffer may only make it longer.
            wait = max(int(flood_seconds), buffered, self._settings.flood_min_pause_seconds)
            return TemporaryGatewayError(
                f"flood wait {flood_seconds}s", retry_after=wait, code="flood_wait"
            )
        comment_context = context in {
            "channel_comment",
            "channel_comment_edit",
            "channel_comment_preflight",
        }
        comments_unavailable = (
            errors.BroadcastForbiddenError,
            errors.MessageIdInvalidError,
            errors.MsgIdInvalidError,
            errors.ChatDiscussionUnallowedError,
            errors.ChannelForumMissingError,
            errors.TopicDeletedError,
        )
        if comment_context and (
            isinstance(exc, comments_unavailable)
            or rpc_message
            in {"MSG_ID_INVALID", "MESSAGE_ID_INVALID", "TOPIC_ID_INVALID", "TOPIC_DELETED"}
        ):
            return PermanentGatewayError(
                "комментарии к этому посту отключены или пост удалён",
                code="comments_unavailable",
            )
        if comment_context and (
            rpc_message == "ALLOW_PAYMENT_REQUIRED"
            or rpc_message.startswith("ALLOW_PAYMENT_REQUIRED_")
        ):
            return PermanentGatewayError(
                "Обсуждение принимает платные сообщения; Stars автоматически не списываются",
                code="comment_paid_message_required",
            )
        if comment_context and rpc_message in {
            "PREMIUM_ACCOUNT_REQUIRED",
            "PRIVACY_PREMIUM_REQUIRED",
        }:
            return PermanentGatewayError(
                "Telegram требует Premium у аккаунта для этого комментария",
                code="comment_sender_requires_premium",
            )
        comment_edit_unavailable = (
            errors.MessageAuthorRequiredError,
            errors.MessageEditTimeExpiredError,
            errors.MessageEmptyError,
        )
        if context == "channel_comment_edit" and (
            isinstance(exc, comment_edit_unavailable)
            or rpc_message
            in {
                "MESSAGE_AUTHOR_REQUIRED",
                "MESSAGE_EDIT_TIME_EXPIRED",
                "MESSAGE_EMPTY",
            }
        ):
            return PermanentGatewayError(
                "Telegram больше не позволяет отредактировать этот комментарий",
                code="comment_edit_unavailable",
            )
        if context == "reaction" and isinstance(exc, errors.MessageIdInvalidError):
            return PermanentGatewayError(
                "сообщение уже удалено или реакции для него недоступны",
                code="reaction_unavailable",
            )
        if context in {"verify", "activity"} and isinstance(exc, errors.MessageIdInvalidError):
            return TemporaryGatewayError(
                f"{context}: message disappeared after send", code="delivery_unconfirmed"
            )
        if context in {"send_story", "can_send_story"}:
            story_flood = re.fullmatch(r"STORY_SEND_FLOOD_(?:WEEKLY|MONTHLY)_(\d+)", rpc_message)
            if story_flood or rpc_message in {
                "STORY_SEND_FLOOD_WEEKLY", "STORY_SEND_FLOOD_MONTHLY"
            }:
                wait = (
                    int(story_flood.group(1)) if story_flood
                    else int(getattr(exc, "seconds", 43200))
                )
                return TemporaryGatewayError(
                    "Telegram ограничил публикацию историй", retry_after=max(1, wait),
                    code="story_daily_limit_reached",
                )

            if rpc_message in {"STORIES_TOO_MUCH", "STORY_SEND_FLOOD"}:
                return TemporaryGatewayError(
                    "достигнут дневной лимит историй на аккаунте",
                    retry_after=43200,
                    code="story_daily_limit_reached",
                )
            if rpc_message in {"PREMIUM_ACCOUNT_REQUIRED", "STORY_FEATURE_PREMIUM_REQUIRED"}:
                return PermanentGatewayError(
                    "публикация историй требует Telegram Premium на аккаунте",
                    code="premium_required",
                )
            if rpc_message in {"MEDIA_CAPTION_TOO_LONG", "STORY_CAPTION_TOO_LONG"}:
                return PermanentGatewayError(
                    "подпись истории превышает допустимый лимит символов",
                    code="caption_too_long",
                )
            if rpc_message in {"STORY_PERIOD_INVALID"}:
                return PermanentGatewayError(
                    "недопустимый период публикации истории",
                    code="story_period_invalid",
                )
        if context == "view_peer_stories":
            if rpc_message in {"USER_PRIVACY_RESTRICTED", "PRIVACY_RESTRICTED"}:
                return PermanentGatewayError(
                    "пользователь ограничил просмотр историй настройками приватности",
                    code="privacy_restricted",
                )
            if rpc_message in {"PEER_ID_INVALID", "USER_ID_INVALID", "INPUT_USER_DEACTIVATED"}:
                return PermanentGatewayError(
                    "получатель недоступен или аккаунт деактивирован",
                    code="recipient_unavailable",
                )
        if isinstance(exc, errors.RandomIdDuplicateError):
            return PermanentGatewayError(
                "Telegram уже принял сообщение с ключом этой задачи",
                code="delivery_unconfirmed",
            )
        if context == "join" and rpc_message == "CHATLISTS_TOO_MUCH":
            return PermanentGatewayError(
                "аккаунт достиг лимита папок Telegram; "
                "для публичных чатов используйте прямую ссылку",
                code="account_chatlist_limit",
            )
        if context == "join" and rpc_message == "FILTER_INCLUDE_EMPTY":
            return PermanentGatewayError(
                "папка Telegram больше не содержит выбранный чат",
                code="target_unavailable",
            )
        reaction_unavailable = (
            errors.BroadcastForbiddenError,
            errors.ReactionEmptyError,
            errors.ReactionInvalidError,
            errors.ReactionsTooManyError,
            # Telethon 1.x exposes this RPC as a typed 403 whose ``message``
            # attribute is only ``FORBIDDEN``.  Match the class explicitly so
            # it cannot fall through to the generic ForbiddenError branch and
            # falsely quarantine the account-chat pair as banned.
            errors.PremiumAccountRequiredError,
        )
        if context == "reaction" and (
            isinstance(exc, reaction_unavailable)
            or rpc_message
            in {
                "CHAT_REACTIONS_NONE",
                "REACTION_EMPTY",
                "REACTION_INVALID",
                "REACTIONS_TOO_MANY",
                "PREMIUM_ACCOUNT_REQUIRED",
            }
        ):
            return PermanentGatewayError(f"reaction: {rpc_message}", code="reaction_unavailable")
        # These refusals supply no retry duration. Do not fabricate a 24-hour
        # Telegram wait, or mistake a frozen invite sender for recipient privacy.
        if isinstance(
            exc,
            (
                errors.FrozenMethodInvalidError,
                errors.FrozenParticipantMissingError,
                errors.PeerFloodError,
                errors.UserRestrictedError,
            ),
        ) or rpc_message in {
            "EDIT_MESSAGE_TEMP_RESTRICTED",
            "FROZEN_METHOD_INVALID",
            "FROZEN_PARTICIPANT_MISSING",
            "PEER_FLOOD",
            "USER_FROZEN",
            "USER_RESTRICTED",
        }:
            message = "Telegram отказал в отправке с аккаунта; срок ограничения не указан"
            hours = self._settings.account_restriction_cooldown_hours
            if hours > 0:
                return TemporaryGatewayError(
                    message,
                    code="account_restricted",
                    retry_after=max(1, int(hours * 3600)),
                )
            return PermanentGatewayError(message, code="account_restricted")
        invite_context = context == "subscriber_invite"
        if isinstance(exc, errors.PremiumCurrentlyUnavailableError):
            return TemporaryGatewayError(
                "Premium временно недоступен", retry_after=300, code="rpc_error"
            )
        if context in {"subscriber_invite", "subscriber_invite_admin"} and (
            isinstance(
                exc,
                (
                    errors.AuthKeyError,
                    errors.UnauthorizedError,
                    errors.UserDeactivatedError,
                    errors.UserDeactivatedBanError,
                    errors.PhoneNumberBannedError,
                ),
            )
            or rpc_message
            in {
                "AUTH_KEY_DUPLICATED",
                "AUTH_KEY_INVALID",
                "AUTH_KEY_UNREGISTERED",
                "SESSION_EXPIRED",
                "SESSION_REVOKED",
                "USER_DEACTIVATED",
                "USER_DEACTIVATED_BAN",
                "PHONE_NUMBER_BANNED",
            }
        ):
            return PermanentGatewayError(
                "сессия или аккаунт больше недоступны", code="account_dead"
            )
        if context == "subscriber_invite_admin":
            admin_codes = {
                "ADMINS_TOO_MUCH": "invite_admin_limit",
                "FRESH_CHANGE_ADMINS_FORBIDDEN": "invite_admin_fresh_restriction",
                "CHAT_ADMIN_INVITE_REQUIRED": "invite_admin_manager_rights",
                "CHAT_ADMIN_REQUIRED": "invite_admin_manager_rights",
                "RIGHT_FORBIDDEN": "invite_admin_manager_rights",
                "USER_CREATOR": "invite_admin_manager_rights",
            }
            if rpc_message in admin_codes:
                return PermanentGatewayError(
                    "Telegram отказал в изменении прав администратора",
                    code=admin_codes[rpc_message],
                )
        if invite_context and rpc_message in {
            "CONTACT_REQUIRE_PREMIUM",
            "PREMIUM_ACCOUNT_REQUIRED",
            "PRIVACY_PREMIUM_REQUIRED",
        }:
            return PermanentGatewayError(
                "для приглашения этого человека Telegram требует Premium у отправителя",
                code="invite_sender_requires_premium",
            )
        if invite_context and rpc_message == "USER_NOT_MUTUAL_CONTACT":
            return PermanentGatewayError(
                "Telegram разрешает приглашать человека только взаимным контактам",
                code="invite_recipient_contact_required",
            )
        if invite_context and rpc_message == "USER_KICKED":
            return PermanentGatewayError(
                "человек исключён из выбранной цели", code="invite_recipient_kicked"
            )
        if invite_context and rpc_message == "USER_BANNED_IN_CHANNEL":
            return PermanentGatewayError(
                "приглашающему аккаунту запрещены действия в этой цели",
                code="invite_destination_rights",
            )
        if invite_context and rpc_message in {"CHANNEL_MONOFORUM_UNSUPPORTED", "CHAT_INVALID"}:
            return PermanentGatewayError(
                "эта цель не поддерживает приглашение участников", code="invite_destination_invalid"
            )
        if invite_context and rpc_message in {
            "CHAT_MEMBER_ADD_FAILED",
            "USER_BOT",
            "BOTS_TOO_MUCH",
            "BOT_GROUPS_BLOCKED",
            "MSG_ID_INVALID",
        }:
            return PermanentGatewayError(
                "Telegram явно отклонил добавление участника", code="invite_request_rejected"
            )
        if invite_context and rpc_message in {"CHAT_ADMIN_REQUIRED", "CHAT_WRITE_FORBIDDEN"}:
            return PermanentGatewayError(
                "аккаунту не выданы права на приглашение", code="invite_destination_rights"
            )
        if invite_context and rpc_message in {"CHANNEL_PRIVATE", "CHANNEL_INVALID"}:
            return PermanentGatewayError(
                "цель недоступна этому аккаунту", code="invite_destination_unavailable"
            )
        if invite_context and rpc_message in {
            "USER_PRIVACY_RESTRICTED",
            "USER_BLOCKED",
            "USER_IS_BLOCKED",
            "YOU_BLOCKED_USER",
        }:
            return PermanentGatewayError(
                "настройки человека не разрешают приглашение", code="invite_recipient_privacy"
            )
        if invite_context and rpc_message in {
            "INPUT_USER_DEACTIVATED",
            "USER_ID_INVALID",
            "USERNAME_INVALID",
            "USERNAME_NOT_OCCUPIED",
        }:
            return PermanentGatewayError(
                "получатель недоступен по сохранённым данным", code="invite_recipient_unavailable"
            )
        if invite_context and rpc_message == "PEER_ID_INVALID":
            return PermanentGatewayError(
                "аккаунт не может разрешить профиль получателя",
                code="invite_recipient_unresolvable",
            )
        if invite_context and isinstance(
            exc,
            (
                errors.ChatAdminRequiredError,
                errors.ChatWriteForbiddenError,
            ),
        ):
            return PermanentGatewayError(
                "аккаунту не выданы права администратора на приглашение участников",
                code="invite_destination_rights",
            )
        if invite_context and isinstance(
            exc, (errors.ChannelPrivateError, errors.ChannelInvalidError)
        ):
            return PermanentGatewayError(
                "аккаунт не имеет доступа к выбранному каналу или группе",
                code="invite_destination_unavailable",
            )
        if invite_context and (
            isinstance(exc, errors.UsersTooMuchError) or rpc_message == "USERS_TOO_MUCH"
        ):
            return PermanentGatewayError(
                "в выбранном канале или группе достигнут лимит участников",
                code="invite_destination_full",
            )
        if invite_context and isinstance(
            exc,
            (
                errors.UserNotMutualContactError,
                errors.UserPrivacyRestrictedError,
                errors.UserBlockedError,
                errors.UserIsBlockedError,
            ),
        ):
            return PermanentGatewayError(
                "настройки получателя не позволяют пригласить его этим аккаунтом",
                code="invite_recipient_privacy",
            )
        if invite_context and (
            isinstance(exc, errors.UserChannelsTooMuchError)
            or rpc_message == "USER_CHANNELS_TOO_MUCH"
        ):
            return PermanentGatewayError(
                "у получателя достигнут лимит каналов и групп Telegram",
                code="invite_recipient_limit",
            )
        if invite_context and isinstance(
            exc,
            (
                errors.UserKickedError,
                errors.InputUserDeactivatedError,
                errors.UserIdInvalidError,
                errors.UsernameInvalidError,
                errors.UsernameNotOccupiedError,
            ),
        ):
            return PermanentGatewayError(
                "получателя нельзя пригласить по сохранённым данным",
                code="invite_recipient_unavailable",
            )
        if invite_context and isinstance(exc, errors.PeerIdInvalidError):
            return PermanentGatewayError(
                "этот аккаунт не может безопасно разрешить получателя",
                code="invite_recipient_unresolvable",
            )
        if invite_context and isinstance(exc, errors.ForbiddenError):
            return PermanentGatewayError(
                "Telegram запретил приглашение; точная причина не распознана",
                code="invite_request_rejected",
            )
        if invite_context and isinstance(exc, errors.RPCError) and 400 <= rpc_code < 500:
            return PermanentGatewayError(
                "Telegram явно отказал в запросе; точная причина не распознана",
                code="invite_request_rejected",
            )
        direct_context = context == "direct_message"
        if direct_context and (
            isinstance(exc, (errors.UserDeactivatedBanError, errors.UserDeactivatedError))
            or rpc_message
            in {
                "AUTH_KEY_DUPLICATED",
                "AUTH_KEY_INVALID",
                "AUTH_KEY_UNREGISTERED",
                "SESSION_EXPIRED",
                "SESSION_REVOKED",
                "USER_DEACTIVATED",
                "USER_DEACTIVATED_BAN",
                "PHONE_NUMBER_BANNED",
            }
        ):
            return PermanentGatewayError("сессия отправителя недействительна", code="account_dead")
        if direct_context and (
            isinstance(exc, errors.VoiceMessagesForbiddenError)
            or rpc_message == "VOICE_MESSAGES_FORBIDDEN"
        ):
            return PermanentGatewayError(
                "получатель запретил голосовые сообщения", code="recipient_content_forbidden"
            )
        if direct_context and (
            rpc_message.startswith("CHAT_SEND_") and rpc_message.endswith("_FORBIDDEN")
        ):
            return PermanentGatewayError(
                "получатель не принимает этот тип сообщения", code="recipient_content_forbidden"
            )
        if direct_context and (
            isinstance(
                exc,
                (
                    errors.MessageTooLongError,
                    errors.MessageEmptyError,
                    errors.EntityBoundsInvalidError,
                    errors.MediaInvalidError,
                ),
            )
            or rpc_message
            in {
                "MESSAGE_TOO_LONG",
                "MESSAGE_EMPTY",
                "ENTITY_BOUNDS_INVALID",
                "ENTITIES_TOO_LONG",
                "MEDIA_INVALID",
                "MEDIA_EMPTY",
                "PHOTO_INVALID_DIMENSIONS",
                "BUTTON_DATA_INVALID",
                "REPLY_MARKUP_INVALID",
                "CAPTION_TOO_LONG",
            }
        ):
            return PermanentGatewayError(
                "Telegram отклонил текст, форматирование или вложение", code="content_invalid"
            )
        if direct_context and (
            isinstance(exc, (errors.UserIdInvalidError, errors.PeerIdInvalidError))
            or rpc_message in {"USER_ID_INVALID", "PEER_ID_INVALID"}
        ):
            return PermanentGatewayError(
                "аккаунт не может найти получателя по сохранённому ID",
                code="recipient_unresolvable",
            )
        if direct_context and (
            rpc_message == "ALLOW_PAYMENT_REQUIRED"
            or rpc_message.startswith("ALLOW_PAYMENT_REQUIRED_")
        ):
            return PermanentGatewayError(
                "получатель принимает только платные сообщения; Stars автоматически не списываются",
                code="recipient_paid_message_required",
            )
        if direct_context and (
            isinstance(exc, errors.PremiumAccountRequiredError)
            or rpc_message
            in {
                "CONTACT_REQUIRE_PREMIUM",
                "PREMIUM_ACCOUNT_REQUIRED",
                "PRIVACY_PREMIUM_REQUIRED",
            }
        ):
            return PermanentGatewayError(
                "получатель разрешает новые сообщения только Premium-аккаунтам",
                code="recipient_requires_premium",
            )
        if direct_context and rpc_message in {"USER_NOT_MUTUAL_CONTACT", "USER_PRIVACY_RESTRICTED"}:
            return PermanentGatewayError(
                "настройки приватности получателя запрещают новое личное сообщение",
                code="recipient_privacy_restricted",
            )
        if direct_context and isinstance(
            exc,
            (
                errors.UserNotMutualContactError,
                errors.UserPrivacyRestrictedError,
            ),
        ):
            return PermanentGatewayError(
                "настройки приватности получателя запрещают новое личное сообщение",
                code="recipient_privacy_restricted",
            )
        if (
            direct_context
            and isinstance(
                exc,
                (
                    errors.UserBlockedError,
                    errors.UserIsBlockedError,
                    errors.YouBlockedUserError,
                ),
            )
            or direct_context
            and rpc_message in {"USER_BLOCKED", "USER_IS_BLOCKED", "YOU_BLOCKED_USER"}
        ):
            return PermanentGatewayError(
                "этот аккаунт не может написать получателю",
                code="recipient_route_blocked",
            )
        if (
            direct_context
            and isinstance(
                exc,
                (
                    errors.InputUserDeactivatedError,
                    errors.UsernameInvalidError,
                    errors.UsernameNotOccupiedError,
                ),
            )
            or direct_context
            and rpc_message
            in {"INPUT_USER_DEACTIVATED", "USERNAME_INVALID", "USERNAME_NOT_OCCUPIED"}
        ):
            return PermanentGatewayError(
                "получатель больше не доступен по сохранённым данным",
                code="recipient_unavailable",
            )
        if direct_context and isinstance(exc, errors.PeerIdInvalidError):
            return PermanentGatewayError(
                "этот аккаунт не может разрешить получателя по сохранённым данным",
                code="recipient_unresolvable",
            )
        if direct_context and isinstance(exc, errors.ForbiddenError):
            # Unknown 403 errors for a private dialog are scoped to this
            # sender-recipient route.  A different selected account can still
            # be mutual/Premium/allowed by the recipient's privacy settings.
            return PermanentGatewayError(
                "Telegram запретил запрос; точная причина не распознана",
                code="telegram_forbidden",
            )
        if context == "verify" and isinstance(exc, errors.ChannelPrivateError):
            return PermanentGatewayError(
                "аккаунт потерял доступ к чату после отправки",
                code="account_chat_access_lost",
            )
        if (
            isinstance(exc, errors.UserBannedInChannelError)
            or rpc_message == "USER_BANNED_IN_CHANNEL"
        ):
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_banned_in_chat"
            )
        if isinstance(exc, errors.UserNotParticipantError) or rpc_message == "USER_NOT_PARTICIPANT":
            return PermanentGatewayError(
                "аккаунт ещё не состоит в этом чате",
                code="target_not_joined",
            )
        if isinstance(exc, errors.InviteRequestSentError):
            # A request-to-join is not a rejected destination.  Telegram has
            # accepted the request, but the administrator still has to admit
            # the account.  Keep the campaign task alive and let its next
            # guarded attempt re-check the invite instead of turning this
            # into a permanent account/chat block.
            return TemporaryGatewayError(
                "заявка на вступление отправлена и ожидает одобрения",
                retry_after=int(self._settings.membership_gate_retry_delay_seconds),
                code="join_request_pending",
            )
        if isinstance(exc, (errors.ChannelsTooMuchError, errors.UserChannelsTooMuchError)):
            return PermanentGatewayError(
                "аккаунт достиг лимита групп и каналов Telegram",
                code="account_join_limit",
            )
        if isinstance(exc, errors.PremiumCurrentlyUnavailableError):
            # This class inherits AuthKeyError in Telethon 1.x even though it
            # describes temporary Premium availability, not a dead session.
            return TemporaryGatewayError(
                f"{context}: Premium temporarily unavailable",
                retry_after=300,
                code="rpc_error",
            )
        # Fatal account-level failures: the session/account is dead, disable it.
        account_dead = (
            errors.ApiIdPublishedFloodError,
            errors.AuthKeyError,
            errors.AuthKeyDuplicatedError,
            errors.AuthKeyInvalidError,
            errors.AuthKeyUnregisteredError,
            errors.SessionRevokedError,
            errors.SessionExpiredError,
            errors.UnauthorizedError,
            errors.UserDeactivatedError,
            errors.UserDeactivatedBanError,
            errors.PhoneNumberBannedError,
        )
        if isinstance(exc, account_dead):
            return PermanentGatewayError(f"{context}: {type(exc).__name__}", code="account_dead")
        if rpc_message in {
            "AUTH_KEY_DUPLICATED",
            "AUTH_KEY_INVALID",
            "AUTH_KEY_PERM_EMPTY",
            "AUTH_KEY_UNREGISTERED",
            "AUTH_KEY_UNSYNCHRONIZED",
            "SESSION_EXPIRED",
            "SESSION_REVOKED",
            "USER_DEACTIVATED",
            "USER_DEACTIVATED_BAN",
            "PHONE_NUMBER_BANNED",
        }:
            return PermanentGatewayError(f"{context}: {rpc_message}", code="account_dead")
        premium_emoji_errors = (errors.EmojiInvalidError, errors.EmoticonInvalidError)
        if isinstance(exc, premium_emoji_errors) or rpc_message in {
            "EMOJI_INVALID",
            "EMOTICON_INVALID",
        }:
            return PermanentGatewayError(
                f"{context}: Telegram не принял Premium Emoji",
                code="premium_emoji_unavailable",
            )
        content_forbidden = (
            errors.ChatSendGameForbiddenError,
            errors.ChatSendGifsForbiddenError,
            errors.ChatSendInlineForbiddenError,
            errors.ChatSendMediaForbiddenError,
            errors.ChatSendPhotosForbiddenError,
            errors.ChatSendPollForbiddenError,
            errors.ChatSendStickersForbiddenError,
            errors.ChatSendVideosForbiddenError,
            errors.ChatSendVoicesForbiddenError,
            errors.SlowModeMultiMsgsDisabledError,
            errors.VoiceMessagesForbiddenError,
        )
        content_forbidden_names = {
            "CHAT_SEND_AUDIOS_FORBIDDEN",
            "CHAT_SEND_DOCS_FORBIDDEN",
            "CHAT_SEND_GAME_FORBIDDEN",
            "CHAT_SEND_GIFS_FORBIDDEN",
            "CHAT_SEND_INLINE_FORBIDDEN",
            "CHAT_SEND_MEDIA_FORBIDDEN",
            "CHAT_SEND_PHOTOS_FORBIDDEN",
            "CHAT_SEND_POLL_FORBIDDEN",
            "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN",
            "CHAT_SEND_STICKERS_FORBIDDEN",
            "CHAT_SEND_VIDEOS_FORBIDDEN",
            "CHAT_SEND_VOICES_FORBIDDEN",
            "CHAT_SEND_WEBPAGE_FORBIDDEN",
            "SLOWMODE_MULTI_MSGS_DISABLED",
            "VOICE_MESSAGES_FORBIDDEN",
        }
        if isinstance(exc, content_forbidden) or rpc_message in content_forbidden_names:
            return PermanentGatewayError(
                f"{context}: выбранный тип медиа запрещён в чате",
                code="content_forbidden",
            )
        # Telegram emits several access denials that historically all appeared
        # as ``account_banned_in_chat``. They should fail over to another
        # sender, but none of them proves a moderator ban.
        account_send_forbidden = (
            errors.ChatWriteForbiddenError,
            errors.ChatGuestSendForbiddenError,
            errors.ChatSendPlainForbiddenError,
            errors.ChatRestrictedError,
        )
        account_send_forbidden_names = {
            "CHAT_GUEST_SEND_FORBIDDEN",
            "CHAT_RESTRICTED",
            "CHAT_SEND_PLAIN_FORBIDDEN",
            "CHAT_WRITE_FORBIDDEN",
        }
        if isinstance(exc, account_send_forbidden) or rpc_message in account_send_forbidden_names:
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_send_forbidden"
            )
        account_access_forbidden = (
            errors.ChatForbiddenError,
            errors.ChannelPrivateError,
            errors.UserBlockedError,
            errors.UserIsBlockedError,
            errors.YouBlockedUserError,
        )
        account_access_forbidden_names = {
            "CHANNEL_PRIVATE",
            "CHAT_FORBIDDEN",
            "USER_IS_BLOCKED",
            "YOU_BLOCKED_USER",
        }
        if (
            isinstance(exc, account_access_forbidden)
            or rpc_message in account_access_forbidden_names
        ):
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_chat_access_forbidden"
            )
        if isinstance(exc, errors.ChatAdminRequiredError) or rpc_message == "CHAT_ADMIN_REQUIRED":
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}",
                code="account_missing_admin_rights",
            )
        # Globally invalid targets are not fixed by changing an account.
        target_permanent = (
            errors.BotChannelsNaError,
            errors.BroadcastForbiddenError,
            errors.ChannelBannedError,
            errors.ChatIdInvalidError,
            errors.ChatIdEmptyError,
            errors.ChatInvalidError,
            errors.ChannelInvalidError,
            errors.ChannelPublicGroupNaError,
            errors.NeedChatInvalidError,
            errors.PeerIdInvalidError,
            errors.PublicChannelMissingError,
            errors.UsernameInvalidError,
            errors.UsernameNotOccupiedError,
            errors.InputUserDeactivatedError,
            errors.InviteHashExpiredError,
            errors.InviteHashEmptyError,
            errors.InviteHashInvalidError,
            errors.InviteRevokedMissingError,
        )
        if isinstance(exc, target_permanent):
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="target_unavailable"
            )
        if (
            isinstance(exc, errors.PremiumAccountRequiredError)
            or rpc_message
            in {
                "ALLOW_PAYMENT_REQUIRED",
                "PREMIUM_ACCOUNT_REQUIRED",
                "PRIVACY_PREMIUM_REQUIRED",
            }
            or rpc_message.startswith("ALLOW_PAYMENT_REQUIRED_")
        ):
            return PermanentGatewayError(
                f"{context}: чат требует платное или Premium-сообщение",
                code="target_unavailable",
            )
        if rpc_message in {"INVITE_SLUG_EMPTY", "INVITE_SLUG_EXPIRED"}:
            return PermanentGatewayError(f"{context}: {rpc_message}", code="target_unavailable")
        if isinstance(exc, errors.ForbiddenError):
            # An unknown 403 is safer to scope to this account-chat pair. A
            # replacement account may still have access. Telegram did not
            # confirm a moderator ban, so preserve that fact in reports and
            # target-quality scoring.
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_chat_access_forbidden"
            )
        if isinstance(exc, errors.RPCError):
            if rpc_code in {400, 404}:
                return PermanentGatewayError(
                    f"{context}: {rpc_message or type(exc).__name__}",
                    code="request_invalid",
                )
            return TemporaryGatewayError(f"{context}: {type(exc).__name__}", code="rpc_error")
        if isinstance(exc, TimeoutError | OSError | ConnectionError):
            return TemporaryGatewayError(f"{context}: сетевая ошибка", code="network_error")
        if direct_context:
            return PermanentGatewayError(
                f"внутренняя ошибка: {type(exc).__name__}", code="unhandled_execution_error"
            )
        return TemporaryGatewayError(f"{context}: {type(exc).__name__}", code="rpc_error")


def build_gateway(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    account_service: AccountService | None = None,
    proxy_service: ProxyService | None = None,
) -> MessageGateway:
    """Return the dry-run gateway or a live Telethon gateway based on settings."""
    if settings.dry_run:
        from proxima_traffic.gateways import DryRunGateway  # noqa: PLC0415

        return DryRunGateway()
    return TelethonGateway(
        session_factory,
        settings,
        account_service=account_service,
        proxy_service=proxy_service,
    )
