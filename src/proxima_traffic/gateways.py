from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from proxima_traffic.content import ContentError, parse_stored_contact, render_telegram_html
from proxima_traffic.models import Account, MembershipStatus, Subscriber, Target
from proxima_traffic.spambot import SpamBotResult, SpamBotStatus

REACTION_FALLBACK_EMOJIS = ("👍", "🔥", "❤️", "⚡")
ORDINARY_REACTION_EMOJI = frozenset(REACTION_FALLBACK_EMOJIS)


def reaction_candidates(preferred_emoji: str) -> tuple[str, ...]:
    """Return the configured emoji first, followed by safe ordinary fallbacks."""
    if preferred_emoji not in ORDINARY_REACTION_EMOJI:
        return ()
    return (preferred_emoji,) + tuple(
        emoji for emoji in REACTION_FALLBACK_EMOJIS if emoji != preferred_emoji
    )


@dataclass(frozen=True, slots=True)
class SentMessage:
    message_id: int
    simulated: bool | None = None
    message_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class DirectRecipient:
    telegram_user_id: int
    username: str | None = None
    access_hash: int | None = None


@dataclass(frozen=True, slots=True)
class DirectMessageDelivery:
    message_id: int
    telegram_user_id: int
    username: str | None
    is_premium: bool | None
    access_hash: int | None


@dataclass(frozen=True, slots=True)
class StoryDelivery:
    story_id: int
    story_url: str | None
    tagged_count: int
    simulated: bool = False
    tagged_subscriber_ids: tuple[int, ...] | None = None


@dataclass(frozen=True, slots=True)
class StoryViewResult:
    telegram_user_id: int
    stories_found: int
    stories_viewed: int
    reaction_sent: bool = False
    reaction_emoji: str | None = None
    has_active_stories: bool = False
    simulated: bool = False
    viewed_story_ids: tuple[int, ...] = ()
    reacted_story_id: int | None = None
    error: GatewayError | None = None


@dataclass(frozen=True, slots=True)
class DirectServiceActionResult:
    telegram_user_id: int
    changed: bool
    service_message_id: int | None = None
    username: str | None = None
    is_premium: bool | None = None
    access_hash: int | None = None
    verification_error: GatewayError | None = None


@dataclass(frozen=True, slots=True)
class DirectRecipientCheck:
    """Fresh, read-only evidence; unknown payment requirements cannot release quarantine."""

    telegram_user_id: int
    checked_at: datetime
    available: bool
    paid_message_required: bool | None = None


@dataclass(frozen=True, slots=True)
class SubscriberInviteDestination:
    """An owned channel or supergroup verified for invitations by one account."""

    title: str | None
    telegram_chat_id: int | None = None


@dataclass(frozen=True, slots=True)
class SubscriberInviteDelivery:
    telegram_user_id: int
    username: str | None
    is_premium: bool | None
    access_hash: int | None
    already_participant: bool = False


@dataclass(frozen=True, slots=True)
class ChatBlacklistScan:
    """Read-only Telegram IDs collected from one chat."""

    telegram_user_ids: tuple[int, ...]
    mode: str
    reached_limit: bool = False
    joined_chat: bool = False
    scan_limit: int = 100_000


@dataclass(frozen=True, slots=True)
class MembershipGateRecovery:
    """Result of one safe attempt to satisfy a channel-subscription gate."""

    detected: bool
    recovered: bool
    notice: str | None = None
    joined_channel_refs: tuple[str, ...] = ()
    verification_callback_count: int = 0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class TargetReadiness:
    membership_status: MembershipStatus
    can_send: bool
    checked_at: datetime
    slowmode_seconds: int | None = None
    next_send_at: datetime | None = None
    media_kinds: frozenset[str] = frozenset()
    error_code: str | None = None
    error: str | None = None
    # A title is accepted only when Telegram returned an actual chat entity.
    # It lets the control panel use Telegram's name instead of an operator
    # alias without requiring a second input step during target creation.
    telegram_title: str | None = None


@dataclass(frozen=True, slots=True)
class ChatActivity:
    latest_message_id: int
    checked_at: datetime
    observed_message_ids: tuple[int, ...] | None = None

    def messages_after(self, message_id: int) -> int:
        if self.observed_message_ids is not None:
            return sum(item > message_id for item in self.observed_message_ids)
        return max(0, self.latest_message_id - message_id)


@dataclass(frozen=True, slots=True)
class ChannelPost:
    message_id: int
    published_at: datetime
    grouped_id: int | None = None


@dataclass(frozen=True, slots=True)
class ChannelPostScan:
    title: str | None
    posts: tuple[ChannelPost, ...]
    last_seen_message_id: int | None = None
    last_seen_grouped_id: int | None = None


@dataclass(frozen=True, slots=True)
class ChannelCommentPreflight:
    title: str | None
    channel_post_id: int
    discussion_root_message_id: int


@dataclass(frozen=True, slots=True)
class ChannelCommentDelivery:
    message_id: int
    thread_root_message_id: int
    discussion_peer_id: int | None = None
    discussion_access_hash: int | None = None
    simulated: bool = False


@dataclass(frozen=True, slots=True)
class ParticipantMessage:
    message_id: int
    sender_id: int | None
    sent_at: datetime | None = None
    is_bot: bool = False
    is_outgoing: bool = False


@dataclass(frozen=True, slots=True)
class ParticipantMessageScan:
    messages: tuple[ParticipantMessage, ...]


class GatewayError(Exception):
    """Safe execution error that can be persisted without leaking credentials."""

    code = "gateway_error"
    retryable = True

    def __init__(
        self,
        message: str,
        *,
        retry_after: int | None = None,
        code: str | None = None,
        external_message_id: int | None = None,
        external_message_ids: tuple[int, ...] = (),
    ) -> None:
        super().__init__(message)
        self.safe_message = message
        self.retry_after = retry_after
        self.external_message_id = external_message_id
        self.external_message_ids = external_message_ids
        if code is not None:
            self.code = code


class TemporaryGatewayError(GatewayError):
    code = "gateway_temporary"


class PermanentGatewayError(GatewayError):
    code = "gateway_permanent"
    retryable = False


class MessageGateway(Protocol):
    async def perform_direct_service_action(
        self,
        *,
        account: Account,
        recipient: DirectRecipient,
        operation_kind: str,
        auto_delete_seconds: int | None,
        premium_only: bool,
        before_action: Callable[[], Awaitable[None]],
    ) -> DirectServiceActionResult: ...

    async def check_spambot_status(self, *, account: Account) -> SpamBotResult: ...

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
    ) -> SentMessage: ...

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
    ) -> DirectMessageDelivery: ...

    async def check_direct_recipient(
        self, *, account: Account, recipient: DirectRecipient
    ) -> DirectRecipientCheck: ...

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
    ) -> StoryDelivery: ...

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
    ) -> StoryViewResult: ...

    async def preflight_subscriber_invite(
        self,
        *,
        account: Account,
        destination_ref: str,
    ) -> SubscriberInviteDestination: ...

    async def grant_subscriber_invite_admin(
        self,
        *,
        manager: Account,
        account: Account,
        destination_ref: str,
    ) -> None: ...

    async def scan_chat_blacklist_candidates(
        self,
        *,
        account: Account,
        chat_ref: str,
        mode: str,
        limit: int,
    ) -> ChatBlacklistScan: ...

    async def invite_subscriber(
        self,
        *,
        account: Account,
        destination_ref: str,
        recipient: DirectRecipient,
        expected_destination_id: int | None = None,
        require_premium: bool | None = None,
        before_invite: Callable[[], Awaitable[None]] | None = None,
    ) -> SubscriberInviteDelivery: ...

    async def inspect_activity(
        self,
        *,
        account: Account,
        target: Target,
        last_message_id: int,
    ) -> ChatActivity: ...

    async def verify_sent_message(
        self,
        *,
        account: Account,
        target: Target,
        message_id: int,
    ) -> None: ...

    async def verify_channel_comment(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        message_id: int,
        discussion_peer_id: int | None,
        discussion_access_hash: int | None,
    ) -> None: ...

    async def recover_membership_gate(
        self,
        *,
        account: Account,
        target: Target,
        deleted_message_id: int,
    ) -> MembershipGateRecovery: ...

    async def inspect_target(
        self,
        *,
        account: Account,
        target: Target,
        media_kinds: frozenset[str] = frozenset(),
    ) -> TargetReadiness: ...

    async def send_message(
        self,
        *,
        account: Account,
        target: Target,
        text: str,
        idempotency_key: str,
        reply_to_message_id: int | None = None,
    ) -> SentMessage: ...

    async def send_reaction(
        self, *, account: Account, target: Target, message_id: int, emoji: str
    ) -> str: ...

    async def collect_mentionable_user_ids(
        self,
        *,
        account: Account,
        target: Target,
        limit: int,
    ) -> tuple[int, ...]: ...


class ChannelCommentGateway(Protocol):
    async def verify_channel_comment(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        message_id: int,
        discussion_peer_id: int | None,
        discussion_access_hash: int | None,
    ) -> None: ...

    async def preflight_channel_comments(
        self,
        *,
        account: Account,
        channel_ref: str,
    ) -> ChannelCommentPreflight: ...

    async def scan_channel_posts(
        self,
        *,
        account: Account,
        channel_ref: str,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
        after_date: datetime | None = None,
    ) -> ChannelPostScan: ...

    async def recover_channel_comment_membership_gate(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        deleted_message_id: int,
    ) -> MembershipGateRecovery: ...

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
    ) -> ChannelCommentDelivery: ...

    async def collect_channel_comment_mentionable_user_ids(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        limit: int,
    ) -> tuple[int, ...]: ...

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
    ) -> ChannelCommentDelivery: ...

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
    ) -> str: ...


class ParticipantReactionGateway(Protocol):
    async def scan_participant_messages(
        self,
        *,
        account: Account,
        target: Target,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
    ) -> ParticipantMessageScan: ...


class DryRunGateway:
    """In-memory gateway that never connects to Telegram."""

    supports_channel_comment_dispatch_hook = True
    supports_campaign_dispatch_hook = True
    simulated = True

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
        del premium_only
        if account.id is None or not recipient.username:
            raise PermanentGatewayError(
                "для сервисного действия нужен @username", code="service_username_required"
            )
        if operation_kind not in {"auto_delete", "protect_content"} or (
            operation_kind == "auto_delete"
            and auto_delete_seconds not in {0, 86400, 604800, 2592000}
        ):
            raise PermanentGatewayError("неизвестное сервисное действие", code="service_invalid")
        await before_action()
        return DirectServiceActionResult(
            recipient.telegram_user_id, changed=True, username=recipient.username
        )

    async def check_direct_recipient(
        self, *, account: Account, recipient: DirectRecipient
    ) -> DirectRecipientCheck:
        raise PermanentGatewayError(
            "Симуляция не подтверждает свежие данные Telegram; карантин сохранён",
            code="recipient_check_unconfirmed",
        )

    def __init__(self) -> None:
        self._next_message_id = 10_000
        self._lock = asyncio.Lock()

    async def check_spambot_status(self, *, account: Account) -> SpamBotResult:
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return SpamBotResult(
            status=SpamBotStatus.FREE,
            raw_text="dry-run: @SpamBot was not contacted",
            parser_signal="dry_run",
        )

    async def inspect_target(
        self,
        *,
        account: Account,
        target: Target,
        media_kinds: frozenset[str] = frozenset(),
    ) -> TargetReadiness:
        from proxima_traffic.models import utc_now

        if account.id is None or target.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
            media_kinds=media_kinds,
        )

    async def inspect_activity(
        self,
        *,
        account: Account,
        target: Target,
        last_message_id: int,
    ) -> ChatActivity:
        from proxima_traffic.models import utc_now

        if account.id is None or target.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return ChatActivity(
            latest_message_id=last_message_id + 10_000,
            checked_at=utc_now(),
        )

    async def verify_sent_message(
        self,
        *,
        account: Account,
        target: Target,
        message_id: int,
    ) -> None:
        if account.id is None or target.id is None or message_id <= 0:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")

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
        del discussion_peer_id, discussion_access_hash
        if account.id is None or not channel_ref or channel_post_id <= 0 or message_id <= 0:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")

    async def recover_membership_gate(
        self,
        *,
        account: Account,
        target: Target,
        deleted_message_id: int,
    ) -> MembershipGateRecovery:
        del account, target, deleted_message_id
        return MembershipGateRecovery(detected=False, recovered=False)

    async def preflight_channel_comments(
        self,
        *,
        account: Account,
        channel_ref: str,
    ) -> ChannelCommentPreflight:
        del channel_ref
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return ChannelCommentPreflight(
            title="Dry-run channel",
            channel_post_id=1,
            discussion_root_message_id=1,
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
        del channel_ref, after_message_id, limit, bootstrap, after_date
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return ChannelPostScan(title=None, posts=())

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
        del channel_ref, html_text, idempotency_key, reply_to_message_id, media_name
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        media_without_caption = media_path is not None
        if media_path is not None and media_kind not in {
            "photo",
            "video",
            "document",
            "voice",
            "video_note",
        }:
            raise PermanentGatewayError("unsupported media kind", code="comment_invalid")
        if media_kind == "video_note" and text:
            raise PermanentGatewayError(
                "video notes cannot contain a caption", code="comment_invalid"
            )
        if channel_post_id <= 0 or (
            not 1 <= len(text.strip()) <= 4096 and not (media_without_caption and not text.strip())
        ):
            raise PermanentGatewayError("channel comment is invalid", code="comment_invalid")
        if before_send is not None:
            await before_send()
        async with self._lock:
            self._next_message_id += 1
            return ChannelCommentDelivery(
                message_id=self._next_message_id,
                thread_root_message_id=channel_post_id,
                simulated=True,
            )

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
        del channel_ref, html_text, idempotency_key
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if channel_post_id <= 0 or message_id <= 0 or not 1 <= len(text.strip()) <= 4096:
            raise PermanentGatewayError(
                "channel comment edit is invalid",
                code="comment_edit_invalid",
            )
        if before_send is not None:
            await before_send()
        return ChannelCommentDelivery(
            message_id=message_id,
            thread_root_message_id=channel_post_id,
            simulated=True,
        )

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
        del channel_ref
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if channel_post_id <= 0 or message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        return emoji

    async def collect_mentionable_user_ids(
        self,
        *,
        account: Account,
        target: Target,
        limit: int,
    ) -> tuple[int, ...]:
        del target
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return tuple(70_000 + index for index in range(1, max(5, min(limit, 10)) + 1))

    async def collect_channel_comment_mentionable_user_ids(
        self,
        *,
        account: Account,
        channel_ref: str,
        channel_post_id: int,
        limit: int,
    ) -> tuple[int, ...]:
        del channel_ref, channel_post_id
        if account.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return tuple(80_000 + index for index in range(1, max(5, min(limit, 10)) + 1))

    async def send_message(
        self,
        *,
        account: Account,
        target: Target,
        text: str,
        idempotency_key: str,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        del reply_to_message_id, idempotency_key
        if account.id is None or target.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if not text or len(text) > 4096:
            raise PermanentGatewayError("message text must contain 1 to 4096 characters")
        async with self._lock:
            self._next_message_id += 1
            return SentMessage(message_id=self._next_message_id, simulated=True)

    async def send_reaction(
        self, *, account: Account, target: Target, message_id: int, emoji: str
    ) -> str:
        if account.id is None or target.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if message_id <= 0 or emoji not in ORDINARY_REACTION_EMOJI:
            raise PermanentGatewayError("reaction is invalid", code="reaction_invalid")
        return emoji

    async def scan_participant_messages(
        self,
        *,
        account: Account,
        target: Target,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
    ) -> ParticipantMessageScan:
        del after_message_id, limit, bootstrap
        if account.id is None or target.id is None:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        return ParticipantMessageScan(messages=())

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
        if before_send is not None:
            await before_send()
        if contact is not None:
            if text or html_text or media_path is not None or media_items:
                raise PermanentGatewayError("contact must be sent without text or media")
            try:
                parse_stored_contact(contact)
            except ContentError as exc:
                raise PermanentGatewayError(str(exc), code="contact_invalid") from exc
            del idempotency_key, reply_to_message_id
            if account.id is None or target.id is None:
                raise PermanentGatewayError("dry-run context is missing persisted identifiers")
            async with self._lock:
                self._next_message_id += 1
                return SentMessage(message_id=self._next_message_id, simulated=True)
        if media_items:
            if len(media_items) > 10:
                raise PermanentGatewayError("media album contains too many items")
            media_path = str(media_items[0].get("path") or "")
            media_kind = str(media_items[0].get("kind") or "")
        if media_path is not None and media_kind not in {
            "photo",
            "video",
            "document",
            "voice",
            "video_note",
        }:
            raise PermanentGatewayError("unsupported media kind")
        try:
            rendered_text = render_telegram_html(html_text) if html_text is not None else text
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="content_invalid") from exc
        media_without_caption = media_path is not None
        if media_kind == "video_note" and (text or html_text):
            raise PermanentGatewayError(
                "video notes cannot contain a caption", code="content_invalid"
            )
        if not rendered_text and media_without_caption:
            del idempotency_key, reply_to_message_id
            if account.id is None or target.id is None:
                raise PermanentGatewayError("dry-run context is missing persisted identifiers")
            async with self._lock:
                self._next_message_id += 1
                return SentMessage(message_id=self._next_message_id, simulated=True)
        return await self.send_message(
            account=account,
            target=target,
            text=rendered_text,
            idempotency_key=idempotency_key,
            reply_to_message_id=reply_to_message_id,
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
        del idempotency_key, media_name
        if account.id is None or recipient.telegram_user_id <= 0:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if recipient.username is None and recipient.access_hash is None:
            raise PermanentGatewayError(
                "получатель недоступен по одному ID для этого аккаунта",
                code="recipient_unresolvable",
            )
        if media_path is not None and media_kind not in {
            "photo",
            "video",
            "document",
            "voice",
            "video_note",
        }:
            raise PermanentGatewayError("unsupported media kind", code="content_invalid")
        try:
            rendered_text = render_telegram_html(html_text) if html_text is not None else text
        except ContentError as exc:
            raise PermanentGatewayError(str(exc), code="content_invalid") from exc
        maximum = 1024 if media_path is not None else 4096
        media_without_caption = media_path is not None
        if media_kind == "video_note" and (text or html_text):
            raise PermanentGatewayError(
                "video notes cannot contain a caption", code="content_invalid"
            )
        if (not 1 <= len(rendered_text) <= maximum) and not (
            media_without_caption and not rendered_text
        ):
            raise PermanentGatewayError(
                "message text has an invalid length",
                code="content_invalid",
            )
        async with self._lock:
            self._next_message_id += 1
            return DirectMessageDelivery(
                message_id=self._next_message_id,
                telegram_user_id=recipient.telegram_user_id,
                username=recipient.username,
                is_premium=None,
                access_hash=recipient.access_hash,
            )

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
        if before_send is not None:
            await before_send()
        del media_path, media_kind, caption, idempotency_key
        story_id = 990000 + (account.id or 1)
        story_url = f"https://t.me/c/{account.id}/story/{story_id}"
        return StoryDelivery(
            story_id=story_id,
            story_url=story_url,
            tagged_count=len(tagged_subscribers),
            simulated=True,
        )

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
        if before_send is not None:
            await before_send()
        del account
        from proxima_traffic.story_content import normalize_story_reactions

        choices = normalize_story_reactions(reaction_emoji)
        emoji = "🔥" if choices == "random" else choices.split(",")[0]
        ids = tuple(i for i in (1, 2) if i not in excluded_story_ids)[:max_stories]
        return StoryViewResult(
            telegram_user_id=recipient.telegram_user_id,
            stories_found=2,
            stories_viewed=len(ids),
            reaction_sent=send_reaction and bool(ids),
            reaction_emoji=emoji if send_reaction and ids else None,
            has_active_stories=True,
            simulated=True,
            viewed_story_ids=ids,
            reacted_story_id=max(ids) if send_reaction and ids else None,
        )

    async def preflight_subscriber_invite(
        self,
        *,
        account: Account,
        destination_ref: str,
    ) -> SubscriberInviteDestination:
        if account.id is None or not destination_ref.strip():
            raise PermanentGatewayError("dry-run context is missing destination or account")
        return SubscriberInviteDestination(title="Dry-run destination")

    async def grant_subscriber_invite_admin(
        self,
        *,
        manager: Account,
        account: Account,
        destination_ref: str,
    ) -> None:
        if manager.id is None or account.id is None or not destination_ref.strip():
            raise PermanentGatewayError("dry-run context is missing destination or account")

    async def scan_chat_blacklist_candidates(
        self,
        *,
        account: Account,
        chat_ref: str,
        mode: str,
        limit: int,
    ) -> ChatBlacklistScan:
        if account.id is None or not chat_ref.strip() or mode not in {"members", "messages"}:
            raise PermanentGatewayError("dry-run context is missing chat scanner context")
        count = max(1, min(int(limit), 10))
        return ChatBlacklistScan(
            telegram_user_ids=tuple(90_000 + index for index in range(1, count + 1)),
            mode=mode,
            reached_limit=count == int(limit),
            joined_chat=False,
            scan_limit=int(limit),
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
        del destination_ref, expected_destination_id, require_premium
        if account.id is None or recipient.telegram_user_id <= 0:
            raise PermanentGatewayError("dry-run context is missing persisted identifiers")
        if recipient.username is None and recipient.access_hash is None:
            raise PermanentGatewayError(
                "recipient cannot be safely resolved for this account",
                code="invite_recipient_unresolvable",
            )
        if before_invite is not None:
            await before_invite()
        return SubscriberInviteDelivery(
            telegram_user_id=recipient.telegram_user_id,
            username=recipient.username,
            is_premium=None,
            access_hash=recipient.access_hash,
        )
