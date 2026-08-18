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
import random
import socket
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import insert, select
from sqlalchemy import update as sql_update
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
    DirectMessageDelivery,
    DirectRecipient,
    GatewayError,
    MembershipGateRecovery,
    MessageGateway,
    ParticipantMessage,
    ParticipantMessageScan,
    PermanentGatewayError,
    SentMessage,
    SubscriberInviteDelivery,
    SubscriberInviteDestination,
    TargetReadiness,
    TemporaryGatewayError,
)
from proxima_traffic.membership_gate import InlineButton, parse_membership_gate
from proxima_traffic.models import (
    Account,
    MembershipStatus,
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
                if client is not None:
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
        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_preflight_entity(
                    client,
                    account.id,
                    target,
                )
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
                )

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
                if getattr(mapped, "code", None) == "delivery_unconfirmed":
                    anchor = None
                else:
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
                channel = await self._resolve_entity(client, account.id, channel_ref)
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
                channel = await self._resolve_entity(client, account.id, channel_ref)
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
                    )
                )
            return ChannelPostScan(
                title=str(getattr(channel, "title", "") or "") or None,
                posts=tuple(posts),
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
                channel = await self._resolve_entity(client, account.id, channel_ref)
                if not bool(getattr(channel, "broadcast", False)):
                    raise PermanentGatewayError(
                        "нужен канал с постами, а не группа",
                        code="channel_required",
                    )
                newest_post = None
                async for message in client.iter_messages(channel, limit=20):
                    if (
                        getattr(message, "id", None) is not None
                        and getattr(message, "action", None) is None
                    ):
                        newest_post = message
                        break
                if newest_post is None:
                    raise PermanentGatewayError(
                        "в канале пока нет публикаций для проверки комментариев",
                        code="channel_has_no_posts",
                    )
                discussion = await self._discussion_for_post(
                    client,
                    channel,
                    int(newest_post.id),
                )
                discussion_entity, root_message_id = await self._discussion_entity(
                    client,
                    discussion,
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
    ) -> ChannelCommentDelivery:
        """Post into the discussion thread of one channel post using Telegram's public RPC."""
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        del media_name
        media_without_caption = media_path is not None
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
        if channel_post_id <= 0 or (
            not 1 <= len(text.strip()) <= 4096 and not (media_without_caption and not text.strip())
        ):
            raise PermanentGatewayError(
                "комментарий не должен быть пустым",
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
                await self._show_typing_before_send(client, discussion_entity)
                if media_path is None:
                    sent = await self._send(
                        client,
                        discussion_entity,
                        text.strip(),
                        reply_to_message_id or root_message_id,
                        idempotency_key,
                        html_text=html_text,
                        error_context="channel_comment",
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
                    )
            except Exception as exc:  # noqa: BLE001 - normalized for durable retries
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="channel_comment") from exc
            return ChannelCommentDelivery(
                message_id=sent.message_id,
                thread_root_message_id=root_message_id,
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
    ) -> ChannelCommentDelivery:
        """Edit one previously sent comment in the matching discussion thread."""
        from telethon import errors, functions, utils  # noqa: PLC0415

        del idempotency_key
        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if channel_post_id <= 0 or message_id <= 0 or not 1 <= len(text.strip()) <= 4096:
            raise PermanentGatewayError(
                "текст или идентификатор редактируемого комментария некорректен",
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
                try:
                    await client(
                        functions.messages.EditMessageRequest(
                            peer=utils.get_input_peer(discussion_entity),
                            id=message_id,
                            message=text.strip(),
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
            return ChannelCommentDelivery(
                message_id=message_id,
                thread_root_message_id=root_message_id,
            )

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
    ) -> None:
        """Apply one ordinary (never paid) reaction under the shared account lock."""
        if account.id is None or target.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        from telethon import functions, types, utils  # noqa: PLC0415

        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                entity = await self._resolve_entity(client, account.id, target.chat_ref)
                await client(
                    functions.messages.SendReactionRequest(
                        peer=utils.get_input_peer(entity),
                        msg_id=message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji)],
                        big=False,
                        add_to_recent=False,
                    )
                )
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
    ) -> None:
        """React to a comment through the discussion peer linked to its channel post."""

        if account.id is None:
            raise PermanentGatewayError("live context is missing persisted identifiers")
        if channel_post_id <= 0 or message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        from telethon import functions, types, utils  # noqa: PLC0415

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
                discussion_entity, _ = await self._discussion_entity(client, discussion)
                await self._ensure_discussion_membership(
                    client,
                    account.id,
                    discussion_entity,
                    join_if_needed=True,
                )
                await client(
                    functions.messages.SendReactionRequest(
                        peer=utils.get_input_peer(discussion_entity),
                        msg_id=message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji)],
                        big=False,
                        add_to_recent=False,
                    )
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
            await self._show_typing_before_send(client, entity)
            try:
                if stored_contact is not None:
                    sent = await self._send_contact(
                        client,
                        entity,
                        stored_contact,
                        reply_to_message_id,
                        idempotency_key,
                    )
                elif not resolved_media_items:
                    sent = await self._send(
                        client,
                        entity,
                        text,
                        reply_to_message_id,
                        idempotency_key,
                        html_text=html_text,
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
            try:
                entity, profile = await self._resolve_direct_recipient(client, recipient)
                await self._show_typing_before_send(client, entity)
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
            except Exception as exc:  # noqa: BLE001 - one durable error vocabulary
                if isinstance(exc, GatewayError):
                    raise
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
            title=str(getattr(destination, "title", "") or "") or None
        )

    async def invite_subscriber(
        self,
        *,
        account: Account,
        destination_ref: str,
        recipient: DirectRecipient,
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
        from telethon import errors, functions, utils  # noqa: PLC0415

        async with self._account_operation(account.id):
            try:
                client = await self._ensure_client(account.id)
                destination = await self._resolve_subscriber_invite_destination(
                    client, account.id, destination_ref
                )
                await self._ensure_can_invite_subscribers(client, destination)
                _, profile = await self._resolve_direct_recipient(client, recipient)
                try:
                    await client(
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
            except Exception as exc:  # noqa: BLE001 - normalize Telegram outcomes
                if isinstance(exc, GatewayError):
                    raise
                raise self._map_rpc_error(exc, context="subscriber_invite") from exc
        return self._subscriber_invite_delivery(recipient, profile)

    @staticmethod
    def _subscriber_invite_delivery(
        recipient: DirectRecipient,
        profile: object,
        *,
        already_participant: bool = False,
    ) -> SubscriberInviteDelivery:
        access_hash = getattr(profile, "access_hash", None)
        username = getattr(profile, "username", None) or recipient.username
        premium = getattr(profile, "premium", None)
        return SubscriberInviteDelivery(
            telegram_user_id=recipient.telegram_user_id,
            username=(str(username).casefold() if username else None),
            is_premium=(bool(premium) if premium is not None else None),
            access_hash=(int(access_hash) if access_hash is not None else recipient.access_hash),
            already_participant=already_participant,
        )

    @staticmethod
    async def _ensure_can_invite_subscribers(client: object, destination: object) -> None:
        """Reject non-channel targets and missing admin invitation rights early."""

        if not (
            bool(getattr(destination, "broadcast", False))
            or bool(getattr(destination, "megagroup", False))
        ):
            raise PermanentGatewayError(
                "invitation destination must be a channel or supergroup",
                code="invite_destination_invalid",
            )
        try:
            permissions = await client.get_permissions(destination, "me")  # type: ignore[attr-defined]
        except Exception:
            # The subsequent invite RPC remains an authoritative permission
            # check; do not reject a valid admin because a client build cannot
            # expose the convenience permission object.
            return
        rights = getattr(permissions, "admin_rights", None)
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

        from telethon import functions, types  # noqa: PLC0415

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
                raise self._map_rpc_error(exc, context="subscriber_invite") from exc
            already_member = isinstance(invite, types.ChatInviteAlready)
            if not already_member or getattr(invite, "chat", None) is None:
                raise PermanentGatewayError(
                    "аккаунт не состоит в закрытой цели по этой invite-ссылке",
                    code="invite_destination_unavailable",
                )
            return invite.chat
        return await self._resolve_entity(client, account_id, normalized)

    @staticmethod
    async def _resolve_direct_recipient(
        client: object,
        recipient: DirectRecipient,
    ) -> tuple[object, object]:
        from telethon import types  # noqa: PLC0415

        try:
            if recipient.access_hash is not None:
                entity: object = types.InputPeerUser(
                    user_id=recipient.telegram_user_id,
                    access_hash=recipient.access_hash,
                )
                # InputPeerUser itself has no bot/deleted/self/profile flags.
                # Fetch the full user so cached access hashes keep the same
                # identity and safety checks as a username resolution.
                profile: object = await client.get_entity(entity)  # type: ignore[attr-defined]
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
                code="recipient_unresolvable",
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
                code="recipient_unavailable",
            )
        if bool(getattr(profile, "self", False)):
            raise PermanentGatewayError(
                "аккаунт не отправляет массовое сообщение самому себе",
                code="recipient_unavailable",
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
                code="account_unauthorized",
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
            flood_sleep_threshold=self._settings.account_flood_sleep_threshold,
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

    @staticmethod
    async def _collect_mentionable_user_ids_from_entity(
        client: object,
        entity: object,
        *,
        exclude_user_id: int | None,
        limit: int,
    ) -> tuple[int, ...]:
        """Collect public participant records, excluding unusable identities."""

        collected: list[int] = []
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
        return tuple(collected)

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
            raise PermanentGatewayError(f"цель {chat_ref!r} не найдена или недоступна") from exc
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
            entity, _ = await self._resolve_folder_target(
                client,
                account_id,
                target,
                allow_join=False,
            )
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
                error=self._safe_gateway_error(exc),
            )
        except Exception as exc:  # noqa: BLE001 - preserve the deleted post as source of truth
            mapped = self._map_rpc_error(exc, context="membership_gate")
            return MembershipGateRecovery(
                detected=gate_detected,
                recovered=False,
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

        candidates: list[tuple[int, object, object]] = []
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
            gate = parse_membership_gate(text, self._inline_buttons(message))
            if gate is None:
                continue
            if not await self._is_moderator_bot_message(client, entity, message):
                continue
            candidates.append((int(raw_id), message, gate))
        if not candidates:
            return None, None
        _, message, gate = min(candidates, key=lambda item: item[0])
        return message, gate

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

        if not isinstance(entity, types.Channel):
            # Basic groups do not support channels.GetParticipant; a Telegram
            # bot sender is still a substantially stronger signal than text.
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
                "not subscribed",
                "subscribe first",
                "ошиб",
                "error",
            )
        ):
            raise PermanentGatewayError(
                "membership verification callback did not confirm access",
                code="membership_gate_verification_failed",
            )

    async def _join_and_resolve(self, client, account_id: int, target: Target):
        """Ensure the account is a participant, joining public chats/invites first."""
        chat_ref = target.chat_ref
        cache_key = (account_id, chat_ref)
        cached = self._entity_cache.get(cache_key)
        if cached is not None and cache_key in self._joined:
            return cached

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
        return entity

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
                pass
            except PermanentGatewayError:
                pass

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
                raise PermanentGatewayError(
                    f"эта пара аккаунт–чат заблокирована: {reason}",
                    code="account_banned_in_chat",
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

    async def _show_typing_before_send(self, client: object, entity: object) -> None:
        """Show Telegram's native typing status during a short composition pause.

        Telegram exposes a typing event, not UI-click events.  The pause is kept
        inside the account lock so a later send from the same account cannot
        overtake the message that is currently being prepared.
        """
        if not self._settings.account_typing_enabled:
            return
        minimum = self._settings.account_typing_min_seconds
        maximum = self._settings.account_typing_max_seconds
        if maximum <= 0:
            return
        duration = random.uniform(minimum, maximum)
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
        sent_after, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id)
        try:
            result = await client(request)
        except Exception as exc:  # noqa: BLE001 - normalise into safe gateway errors
            mapped = self._map_rpc_error(exc, context=error_context)
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                recovered_id = await self._recover_duplicate_delivery(
                    client,
                    entity,
                    message_text,
                    reply_to_message_id,
                    sent_after=sent_after,
                )
                if recovered_id is not None:
                    await self._remember_delivery_message_id(idempotency_key, recovered_id)
                    return SentMessage(message_id=recovered_id)
                raise TemporaryGatewayError(
                    "Telegram уже принял сообщение, но его ID пока не найден",
                    retry_after=30,
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise TemporaryGatewayError("Telegram не вернул идентификатор сообщения")
        await self._remember_delivery_message_id(idempotency_key, message_id)
        await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id)

    async def _send_contact(
        self,
        client: object,
        entity: object,
        contact: StoredContact,
        reply_to_message_id: int | None,
        idempotency_key: str,
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
        sent_after, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id)
        try:
            result = await client(request)  # type: ignore[operator]
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_rpc_error(exc, context="send_contact")
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                recovered_id = await self._recover_duplicate_contact_delivery(
                    client,
                    entity,
                    contact.phone_number,
                    reply_to_message_id,
                    sent_after=sent_after,
                )
                if recovered_id is not None:
                    await self._remember_delivery_message_id(idempotency_key, recovered_id)
                    return SentMessage(message_id=recovered_id)
                raise TemporaryGatewayError(
                    "Telegram уже принял контакт, но его ID пока не найден",
                    retry_after=30,
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise TemporaryGatewayError("Telegram не вернул идентификатор контакта")
        await self._remember_delivery_message_id(idempotency_key, message_id)
        await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id)

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
    ) -> SentMessage:
        from telethon import functions, types, utils  # noqa: PLC0415

        message_text = text
        entities = None
        if html_text:
            message_text, entities = await self._parse_html_message(client, html_text)
        sent_after, known_message_id = await self._delivery_receipt(idempotency_key)
        if known_message_id is not None:
            await self._verify_delivery(client, entity, known_message_id)
            return SentMessage(message_id=known_message_id)
        try:
            media = await self._prepare_uploaded_media(
                client,
                media_path,
                media_kind,
            )
            random_id = self._message_random_id(idempotency_key)
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
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                if media_kind and not message_text:
                    raise TemporaryGatewayError(
                        "Telegram мог принять медиа без подписи, но не вернул его ID; "
                        "повтор отложен, чтобы не создать дубль",
                        retry_after=30,
                        code="delivery_unconfirmed",
                    ) from exc
                recovered_id = await self._recover_duplicate_delivery(
                    client,
                    entity,
                    message_text,
                    reply_to_message_id,
                    sent_after=sent_after,
                )
                if recovered_id is not None:
                    await self._remember_delivery_message_id(idempotency_key, recovered_id)
                    return SentMessage(message_id=recovered_id)
                raise TemporaryGatewayError(
                    "Telegram уже принял медиа, но его ID пока не найден",
                    retry_after=30,
                    code="delivery_unconfirmed",
                ) from exc
            raise mapped from exc
        message_id = self._sent_message_id(result, random_id)
        if message_id is None:
            raise TemporaryGatewayError("Telegram не вернул идентификатор медиа-сообщения")
        await self._remember_delivery_message_id(idempotency_key, message_id)
        await self._verify_delivery(client, entity, message_id)
        return SentMessage(message_id=message_id)

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
            return SentMessage(message_id=known_message_id)
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
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                recovered_id = await self._recover_duplicate_delivery(
                    client,
                    entity,
                    caption,
                    reply_to_message_id,
                    sent_after=sent_after,
                )
                if recovered_id is not None:
                    await self._remember_delivery_message_id(idempotency_key, recovered_id)
                    return SentMessage(message_id=recovered_id)
            raise mapped from exc
        message_ids = self._sent_message_ids(result, set(random_ids))
        if not message_ids:
            raise TemporaryGatewayError("Telegram не вернул идентификаторы альбома")
        for message_id in message_ids:
            await self._verify_delivery(client, entity, message_id)
        last_message_id = max(message_ids)
        await self._remember_delivery_message_id(idempotency_key, last_message_id)
        return SentMessage(message_id=last_message_id)

    @staticmethod
    async def _prepare_uploaded_media(client, media_path: str, media_kind: str):
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

    async def _recover_duplicate_delivery(
        self,
        client,
        entity: object,
        text: str,
        reply_to_message_id: int | None,
        *,
        sent_after: datetime,
    ) -> int | None:
        """Recover a Telegram-accepted idempotent send after a lost response."""
        try:
            async for message in client.iter_messages(entity, limit=50):
                if not bool(getattr(message, "out", False)):
                    continue
                message_date = getattr(message, "date", None)
                if isinstance(message_date, datetime):
                    if self._as_utc(message_date) < sent_after - timedelta(minutes=2):
                        break
                else:
                    continue
                if str(getattr(message, "message", "") or "") != text:
                    continue
                reply = getattr(message, "reply_to", None)
                actual_reply_id = getattr(reply, "reply_to_msg_id", None)
                if actual_reply_id != reply_to_message_id:
                    continue
                message_id = getattr(message, "id", None)
                if message_id is not None:
                    return int(message_id)
        except Exception:  # noqa: BLE001 - keep the original send failure authoritative
            return None
        return None

    async def _recover_duplicate_contact_delivery(
        self,
        client: object,
        entity: object,
        phone_number: str,
        reply_to_message_id: int | None,
        *,
        sent_after: datetime,
    ) -> int | None:
        try:
            async for message in client.iter_messages(entity, limit=50):  # type: ignore[attr-defined]
                if not bool(getattr(message, "out", False)):
                    continue
                message_date = getattr(message, "date", None)
                if isinstance(message_date, datetime):
                    if self._as_utc(message_date) < sent_after - timedelta(minutes=2):
                        break
                else:
                    continue
                media = getattr(message, "media", None)
                if str(getattr(media, "phone_number", "") or "") != phone_number:
                    continue
                reply = getattr(message, "reply_to", None)
                if getattr(reply, "reply_to_msg_id", None) != reply_to_message_id:
                    continue
                message_id = getattr(message, "id", None)
                if isinstance(message_id, int) and message_id > 0:
                    return message_id
        except Exception:  # noqa: BLE001 - best-effort duplicate recovery
            logger.debug("contact duplicate delivery recovery failed", exc_info=True)
        return None

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
        for update in updates:
            message = getattr(update, "message", None)
            if message is not None and getattr(message, "id", None) is not None:
                return int(message.id)
        return None

    @staticmethod
    def _sent_message_ids(result: object, random_ids: set[int]) -> list[int]:
        found: set[int] = set()
        direct_id = getattr(result, "id", None)
        if direct_id is not None:
            found.add(int(direct_id))
        updates = list(getattr(result, "updates", None) or [])
        for update in updates:
            if (
                type(update).__name__ == "UpdateMessageID"
                and getattr(update, "random_id", None) in random_ids
            ):
                found.add(int(update.id))
            message = getattr(update, "message", None)
            if message is not None and getattr(message, "id", None) is not None:
                found.add(int(message.id))
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
                code="account_banned_in_chat",
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
                code="account_banned_in_chat",
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
        fetched = await self._fetch_message_for_verification(client, entity, message_id)
        if self._message_is_present(fetched, message_id):
            return

        # A second probe separates a real moderation deletion from a short-lived
        # read-after-write delay. Only two confirmed misses quarantine the chat.
        confirm_delay = float(self._settings.account_delivery_verify_confirm_delay_seconds)
        if confirm_delay > 0:
            await asyncio.sleep(confirm_delay)
        fetched = await self._fetch_message_for_verification(client, entity, message_id)
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

    async def _fetch_message_for_verification(self, client, entity, message_id: int):
        try:
            return await asyncio.wait_for(
                client.get_messages(entity, ids=message_id),
                timeout=float(self._settings.account_delivery_verify_timeout_seconds),
            )
        except Exception as exc:  # noqa: BLE001 - normalise into safe gateway errors
            mapped = self._map_rpc_error(exc, context="verify")
            if getattr(mapped, "code", None) == "delivery_unconfirmed":
                return None
            raise mapped from exc

    @staticmethod
    def _message_is_present(message: object | None, message_id: int) -> bool:
        return bool(
            message is not None
            and getattr(message, "id", None) == message_id
            and type(message).__name__ != "MessageEmpty"
        )

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
        message = "публикация удалена модерацией сразу после отправки"
        if notice:
            message += f"; ответ чата: {notice}"
        return PermanentGatewayError(
            message,
            code="post_deleted_by_moderation",
            external_message_id=message_id,
        )

    def _map_rpc_error(self, exc: Exception, *, context: str) -> Exception:
        import math  # noqa: PLC0415

        from telethon import errors  # noqa: PLC0415

        if isinstance(exc, GatewayError):
            return exc
        rpc_message = str(getattr(exc, "message", "") or "").upper()
        rpc_code = int(getattr(exc, "code", 0) or 0)
        if isinstance(exc, errors.SlowModeWaitError):
            return TemporaryGatewayError(
                f"slow mode {exc.seconds}s", retry_after=int(exc.seconds), code="slow_mode"
            )
        flood_seconds = getattr(exc, "seconds", None)
        if isinstance(exc, errors.FloodWaitError) or (
            isinstance(exc, errors.FloodError) and flood_seconds is not None
        ):
            buffered = math.ceil(int(flood_seconds) * self._settings.flood_buffer_multiplier)
            # A server-mandated FLOOD_WAIT must never be shortened by a local
            # scheduling cap. The buffer may only make it longer.
            wait = max(int(flood_seconds), buffered)
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
            errors.ChatDiscussionUnallowedError,
            errors.ChannelForumMissingError,
            errors.TopicDeletedError,
        )
        if comment_context and isinstance(exc, comments_unavailable):
            return PermanentGatewayError(
                "комментарии к этому посту отключены или пост удалён",
                code="comments_unavailable",
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
        if isinstance(exc, errors.RandomIdDuplicateError):
            return PermanentGatewayError(
                "Telegram уже принял сообщение с ключом этой задачи",
                code="delivery_unconfirmed",
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
        invite_context = context == "subscriber_invite"
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
        if invite_context and isinstance(exc, errors.UsersTooMuchError):
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
        if invite_context and isinstance(exc, errors.UserChannelsTooMuchError):
            return PermanentGatewayError(
                "у получателя достигнут лимит каналов и групп Telegram",
                code="invite_recipient_limit",
            )
        if invite_context and isinstance(
            exc,
            (
                errors.UserKickedError,
                errors.UserBannedInChannelError,
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
                "Telegram запретил этому аккаунту приглашение данного получателя",
                code="invite_recipient_privacy",
            )
        direct_context = context == "direct_message"
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
        if direct_context and isinstance(
            exc,
            (
                errors.UserBlockedError,
                errors.UserIsBlockedError,
                errors.YouBlockedUserError,
            ),
        ):
            return PermanentGatewayError(
                "этот аккаунт не может написать получателю",
                code="recipient_route_blocked",
            )
        if direct_context and isinstance(
            exc,
            (
                errors.InputUserDeactivatedError,
                errors.UsernameInvalidError,
                errors.UsernameNotOccupiedError,
            ),
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
                "этот аккаунт не может написать получателю",
                code="recipient_route_blocked",
            )
        if context == "verify" and isinstance(exc, errors.ChannelPrivateError):
            return PermanentGatewayError(
                "аккаунт потерял доступ к чату после отправки",
                code="account_banned_in_chat",
            )
        # Account-level restrictions surface across many chats at once (usually a
        # @SpamBot restriction). Treat them as recoverable but cool the account down.
        restriction_wait = int(self._settings.account_restriction_cooldown_hours * 3600)
        if isinstance(
            exc,
            (
                errors.FrozenMethodInvalidError,
                errors.FrozenParticipantMissingError,
                errors.PeerFloodError,
                errors.UserRestrictedError,
            ),
        ):
            return TemporaryGatewayError(
                f"{context}: {type(exc).__name__}",
                retry_after=restriction_wait,
                code="account_restricted",
            )
        if isinstance(exc, errors.UserBannedInChannelError):
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_banned_in_chat"
            )
        if isinstance(exc, errors.InviteRequestSentError):
            return PermanentGatewayError(
                "заявка на вступление отправлена и ожидает одобрения",
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
        if rpc_message in {
            "EDIT_MESSAGE_TEMP_RESTRICTED",
            "FROZEN_PARTICIPANT_MISSING",
            "PEER_FLOOD",
            "USER_FROZEN",
            "USER_RESTRICTED",
        }:
            return TemporaryGatewayError(
                f"{context}: {rpc_message}",
                retry_after=restriction_wait,
                code="account_restricted",
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
        # Account-specific chat failures should switch to another selected sender.
        account_target_blocked = (
            errors.ChatWriteForbiddenError,
            errors.ChatGuestSendForbiddenError,
            errors.ChatSendPlainForbiddenError,
            errors.ChatForbiddenError,
            errors.ChatRestrictedError,
            errors.ChannelPrivateError,
            errors.UserNotParticipantError,
            errors.UserBlockedError,
            errors.UserIsBlockedError,
            errors.YouBlockedUserError,
        )
        account_target_blocked_names = {
            "CHANNEL_PRIVATE",
            "CHAT_FORBIDDEN",
            "CHAT_GUEST_SEND_FORBIDDEN",
            "CHAT_RESTRICTED",
            "CHAT_SEND_PLAIN_FORBIDDEN",
            "CHAT_WRITE_FORBIDDEN",
            "USER_BANNED_IN_CHANNEL",
            "USER_IS_BLOCKED",
            "USER_NOT_PARTICIPANT",
            "YOU_BLOCKED_USER",
        }
        if isinstance(exc, account_target_blocked) or rpc_message in account_target_blocked_names:
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_banned_in_chat"
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
            # replacement account may still have access; two blocked routes will
            # quarantine the chat through the normal quality score.
            return PermanentGatewayError(
                f"{context}: {type(exc).__name__}", code="account_banned_in_chat"
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
