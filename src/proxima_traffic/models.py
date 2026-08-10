from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import (
    text as sql_text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize DB datetimes so SQLite naive values compare safely with utc_now()."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class AccountStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class AccountSessionSource(StrEnum):
    NONE = "none"
    TDATA = "tdata"
    SESSION_FILE = "session_file"
    SESSION_STRING = "session_string"


class MembershipStatus(StrEnum):
    UNKNOWN = "unknown"
    MEMBER = "member"
    NOT_MEMBER = "not_member"
    JOIN_PENDING = "join_pending"
    BANNED = "banned"
    UNAVAILABLE = "unavailable"


class CampaignMode(StrEnum):
    DIALOGUE = "dialogue"
    BROADCAST = "broadcast"


class BroadcastSenderMode(StrEnum):
    """How selected accounts cover broadcast targets."""

    DISTRIBUTED = "distributed"
    INDEPENDENT = "independent"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunStep(StrEnum):
    PRIMARY = "primary"
    REPLY = "reply"


class ChannelCommentMode(StrEnum):
    SINGLE = "single"
    DIALOGUE = "dialogue"
    MESSAGE_EDIT = "message_edit"


class ChannelCommentRunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ReactionTaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ParticipantReactionTaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class SubscriberListStatus(StrEnum):
    IMPORTING = "importing"
    READY = "ready"
    FAILED = "failed"


class SubscriberSegment(StrEnum):
    ALL = "all"
    PREMIUM = "premium"
    NON_PREMIUM = "non_premium"


class DirectLaunchStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DirectMessageTaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ActionKind(StrEnum):
    PRIMARY_MESSAGE = "primary_message"
    REPLY_MESSAGE = "reply_message"


class ActionStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ProxyProtocol(StrEnum):
    HTTP = "http"
    HTTPS = "https"
    SOCKS5 = "socks5"
    SOCKS5H = "socks5h"


class ProxyStatus(StrEnum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    QUARANTINED = "quarantined"
    DISABLED = "disabled"


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    label: Mapped[str] = mapped_column(String(100), unique=True)
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, unique=True)
    ownership_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[AccountStatus] = mapped_column(
        Enum(AccountStatus, native_enum=False), default=AccountStatus.ACTIVE
    )
    session_source: Mapped[AccountSessionSource] = mapped_column(
        Enum(AccountSessionSource, native_enum=False), default=AccountSessionSource.NONE
    )
    session_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    api_hash_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    about: Mapped[str | None] = mapped_column(String(70), nullable=True)
    has_profile_photo: Mapped[bool] = mapped_column(Boolean, default=False)
    authorized: Mapped[bool] = mapped_column(Boolean, default=False)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    connection_generation: Mapped[int] = mapped_column(Integer, default=0)
    next_send_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_reservation_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    send_reservation_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cooldown_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    spambot_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    spambot_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    spambot_parser_signal: Mapped[str | None] = mapped_column(String(100), nullable=True)
    spambot_response_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    flood_hits: Mapped[int] = mapped_column(Integer, default=0)
    flood_window_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    sent_today: Mapped[int] = mapped_column(Integer, default=0)
    day_window_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    sent_since_break: Mapped[int] = mapped_column(Integer, default=0)
    joins_today: Mapped[int] = mapped_column(Integer, default=0)
    join_window_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_join_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class MentionBlacklistEntry(Base):
    """A Telegram user who must never be mentioned or contacted directly."""

    __tablename__ = "mention_blacklist_entries"

    telegram_user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source: Mapped[str] = mapped_column(String(100), default="manual")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class AccountOperationLease(Base):
    """Cross-process fencing lease for one live Telegram account session."""

    __tablename__ = "account_operation_leases"
    __table_args__ = (Index("ix_account_operation_leases_until", "lease_until"),)

    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"), primary_key=True
    )
    owner_id: Mapped[str] = mapped_column(String(100))
    lease_token: Mapped[str] = mapped_column(String(32))
    lease_until: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class TelegramDeliveryReceipt(Base):
    """Durable transport receipt for recovering an idempotent Telegram send."""

    __tablename__ = "telegram_delivery_receipts"

    id: Mapped[int] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    first_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    external_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class AccountSendReceipt(Base):
    """Count one durable secondary Telegram action exactly once."""

    __tablename__ = "account_send_receipts"
    __table_args__ = (Index("ix_account_send_receipts_account", "account_id"),)

    operation_key_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Target(Base):
    __tablename__ = "targets"
    __table_args__ = (Index("ix_targets_standalone_id", "is_standalone", "id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    chat_ref: Mapped[str] = mapped_column(String(255), unique=True)
    title: Mapped[str] = mapped_column(String(150))
    chat_type: Mapped[str | None] = mapped_column(String(24), nullable=True)
    folder_slug: Mapped[str | None] = mapped_column(String(128), nullable=True)
    telegram_peer_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    is_standalone: Mapped[bool] = mapped_column(Boolean, default=True)
    manual_cooldown_seconds: Mapped[int] = mapped_column(Integer, default=0)
    approved_by_operator: Mapped[bool] = mapped_column(Boolean, default=False)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_error_account_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality_status: Mapped[str] = mapped_column(String(20), default="learning", index=True)
    quality_score: Mapped[int] = mapped_column(Integer, default=100)
    quality_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    quality_attempts: Mapped[int] = mapped_column(Integer, default=0)
    quality_successes: Mapped[int] = mapped_column(Integer, default=0)
    quality_failures: Mapped[int] = mapped_column(Integer, default=0)
    quality_banned_accounts: Mapped[int] = mapped_column(Integer, default=0)
    quality_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    quality_bad_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    quality_reset_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class TargetList(Base):
    __tablename__ = "target_lists"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class TargetAccountState(Base):
    """Telegram timing state that is specific to one sender in one chat."""

    __tablename__ = "target_account_states"
    __table_args__ = (
        UniqueConstraint("target_id", "account_id", name="uq_target_account_state"),
        Index("ix_target_account_next_send", "account_id", "next_send_at"),
        Index("ix_target_account_send_blocked", "target_id", "send_blocked"),
        Index(
            "ix_target_account_membership",
            "target_id",
            "membership_status",
            "can_send",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    slowmode_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    next_send_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    adaptive_interval_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    activity_messages_per_minute: Mapped[float | None] = mapped_column(Float, nullable=True)
    adaptive_tier: Mapped[str | None] = mapped_column(String(20), nullable=True)
    activity_sampled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    membership_status: Mapped[MembershipStatus] = mapped_column(
        Enum(MembershipStatus, native_enum=False, length=14),
        default=MembershipStatus.UNKNOWN,
    )
    can_send: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    membership_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    preflight_media_kinds: Mapped[str | None] = mapped_column(String(100), nullable=True)
    membership_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    send_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    blocked_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    blocked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)


class TargetListItem(Base):
    __tablename__ = "target_list_items"
    __table_args__ = (
        UniqueConstraint("target_list_id", "target_id", name="uq_target_list_item"),
        Index("ix_target_list_items_enabled", "target_list_id", "enabled"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    target_list_id: Mapped[int] = mapped_column(ForeignKey("target_lists.id", ondelete="CASCADE"))
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id", ondelete="CASCADE"))
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class MessageTemplate(Base):
    __tablename__ = "message_templates"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    primary_text: Mapped[str] = mapped_column(Text)
    reply_text: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class Campaign(Base):
    __tablename__ = "campaigns"
    __table_args__ = (
        CheckConstraint("primary_account_id != reply_account_id", name="different_accounts"),
        CheckConstraint("reply_delay_seconds >= 0", name="non_negative_reply_delay"),
        CheckConstraint(
            "(target_id IS NOT NULL AND target_list_id IS NULL) OR "
            "(target_id IS NULL AND target_list_id IS NOT NULL)",
            name="exactly_one_campaign_target",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    mode: Mapped[CampaignMode] = mapped_column(
        Enum(CampaignMode, native_enum=False), default=CampaignMode.DIALOGUE
    )
    primary_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    reply_account_id: Mapped[int | None] = mapped_column(ForeignKey("accounts.id"), nullable=True)
    target_id: Mapped[int | None] = mapped_column(ForeignKey("targets.id"), nullable=True)
    target_list_id: Mapped[int | None] = mapped_column(ForeignKey("target_lists.id"), nullable=True)
    primary_text: Mapped[str] = mapped_column(Text)
    primary_html: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_media_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_media_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    primary_media_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_media_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_variants_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_text: Mapped[str] = mapped_column(Text, default="")
    reply_delay_seconds: Mapped[int] = mapped_column(Integer, default=0)
    spread_seconds: Mapped[int] = mapped_column(Integer, default=0)
    repeat_count: Mapped[int] = mapped_column(Integer, default=1)
    repeat_interval_seconds: Mapped[int] = mapped_column(Integer, default=0)
    smart_timer_mode: Mapped[bool] = mapped_column(Boolean, default=False)
    smart_duration_seconds: Mapped[int] = mapped_column(Integer, default=0)
    smart_no_timer_interval_seconds: Mapped[int] = mapped_column(Integer, default=900)
    auto_sender_failover: Mapped[bool] = mapped_column(Boolean, default=False)
    sender_mode: Mapped[BroadcastSenderMode] = mapped_column(
        Enum(BroadcastSenderMode, native_enum=False, length=16),
        default=BroadcastSenderMode.DISTRIBUTED,
    )
    activity_aware: Mapped[bool] = mapped_column(Boolean, default=False)
    min_messages_between_posts: Mapped[int] = mapped_column(Integer, default=0)
    activity_check_interval_seconds: Mapped[int] = mapped_column(Integer, default=300)
    visibility_max_wait_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    dialogue_steps_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    reactions_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    reaction_count: Mapped[int] = mapped_column(Integer, default=0)
    reaction_emoji: Mapped[str] = mapped_column(String(16), default="👍")
    reaction_dialogue_step: Mapped[int] = mapped_column(Integer, default=1)
    hidden_mentions_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    mention_cooldown_days: Mapped[int] = mapped_column(Integer, default=30)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class CampaignAccount(Base):
    """Sender pool for broadcast campaigns (round-robin rotation)."""

    __tablename__ = "campaign_accounts"
    __table_args__ = (UniqueConstraint("campaign_id", "account_id", name="uq_campaign_account"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("campaigns.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class CampaignRun(Base):
    __tablename__ = "campaign_runs"
    __table_args__ = (
        UniqueConstraint("request_key", name="uq_campaign_runs_request_key"),
        Index("ix_campaign_runs_campaign_status", "campaign_id", "status"),
        Index("ix_campaign_runs_campaign_launch_key", "campaign_id", "launch_key"),
        Index("ix_campaign_runs_notification", "status", "notified_at"),
        Index(
            "ix_campaign_runs_notification_due",
            "status",
            "notified_at",
            "notification_next_attempt_at",
        ),
        Index("ix_campaign_runs_lease_fence", "status", "lease_owner", "lease_token"),
        Index("ix_campaign_runs_archive", "archived_at", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("campaigns.id"))
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id"))
    sender_account_id: Mapped[int | None] = mapped_column(ForeignKey("accounts.id"), nullable=True)
    cycle_index: Mapped[int] = mapped_column(Integer, default=0)
    repeat_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    request_key: Mapped[str] = mapped_column(String(64))
    launch_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus, native_enum=False), default=RunStatus.QUEUED
    )
    next_step: Mapped[RunStep] = mapped_column(
        Enum(RunStep, native_enum=False), default=RunStep.PRIMARY
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    membership_gate_retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    primary_text_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_html_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden_mentions_enabled_snapshot: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    mention_cooldown_days_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hidden_mention_user_ids_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden_mention_skip_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    hidden_mention_resolved_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    primary_media_path_snapshot: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_media_kind_snapshot: Mapped[str | None] = mapped_column(String(20), nullable=True)
    primary_media_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_media_size_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_items_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_variants_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_variant_index: Mapped[int] = mapped_column(Integer, default=0)
    content_variant_id_snapshot: Mapped[str | None] = mapped_column(String(40), nullable=True)
    content_variant_name_snapshot: Mapped[str | None] = mapped_column(String(80), nullable=True)
    reply_text_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_delay_seconds_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    no_timer_interval_seconds_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    auto_sender_failover_snapshot: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    sender_pool_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    sender_mode_snapshot: Mapped[BroadcastSenderMode | None] = mapped_column(
        Enum(BroadcastSenderMode, native_enum=False, length=16), nullable=True
    )
    activity_aware_snapshot: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    min_messages_between_posts_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    activity_check_interval_seconds_snapshot: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    visibility_max_wait_seconds_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dialogue_steps_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    dialogue_step_index: Mapped[int] = mapped_column(Integer, default=0)
    dialogue_last_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    primary_message_id: Mapped[int | None] = mapped_column(nullable=True)
    reply_message_id: Mapped[int | None] = mapped_column(nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notification_lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    notification_lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    notification_lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    notification_next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    notification_attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    notification_last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class CampaignMentionState(Base):
    """Cooldown and in-flight reservation for one campaign member in one chat."""

    __tablename__ = "campaign_mention_states"
    __table_args__ = (
        UniqueConstraint(
            "campaign_id",
            "target_id",
            "telegram_user_id",
            name="uq_campaign_mention_state_user",
        ),
        Index(
            "ix_campaign_mention_state_cooldown",
            "campaign_id",
            "target_id",
            "last_mentioned_at",
        ),
        Index(
            "ix_campaign_mention_state_reservation",
            "reserved_run_id",
            "reserved_until",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("campaigns.id", ondelete="CASCADE"))
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id", ondelete="CASCADE"))
    telegram_user_id: Mapped[int] = mapped_column(BigInteger)
    last_mentioned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    mention_count: Mapped[int] = mapped_column(Integer, default=0)
    reserved_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("campaign_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    reserved_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberList(Base):
    """One operator-owned, consent-confirmed source of direct-message recipients."""

    __tablename__ = "subscriber_lists"
    __table_args__ = (Index("ix_subscriber_lists_status", "status", "updated_at"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    source_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[SubscriberListStatus] = mapped_column(
        Enum(SubscriberListStatus, native_enum=False, length=16),
        default=SubscriberListStatus.IMPORTING,
    )
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    premium_count: Mapped[int] = mapped_column(Integer, default=0)
    non_premium_count: Mapped[int] = mapped_column(Integer, default=0)
    username_count: Mapped[int] = mapped_column(Integer, default=0)
    last_import_total: Mapped[int] = mapped_column(Integer, default=0)
    last_import_added: Mapped[int] = mapped_column(Integer, default=0)
    last_import_updated: Mapped[int] = mapped_column(Integer, default=0)
    last_import_duplicates: Mapped[int] = mapped_column(Integer, default=0)
    last_import_invalid: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_imported_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class DirectCampaign(Base):
    """Saved personal-message campaign; a launch always uses immutable snapshots."""

    __tablename__ = "direct_campaigns"
    __table_args__ = (
        CheckConstraint(
            "recipient_limit >= 1 AND recipient_limit <= 1000000",
            name="valid_direct_campaign_recipient_limit",
        ),
        CheckConstraint(
            "contact_cooldown_days >= 0 AND contact_cooldown_days <= 3650",
            name="valid_direct_campaign_cooldown",
        ),
        Index("ix_direct_campaigns_enabled", "enabled", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    subscriber_list_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_lists.id", ondelete="RESTRICT")
    )
    segment: Mapped[SubscriberSegment] = mapped_column(
        Enum(SubscriberSegment, native_enum=False, length=16),
        default=SubscriberSegment.ALL,
    )
    recipient_limit: Mapped[int] = mapped_column(Integer, default=10)
    contact_cooldown_days: Mapped[int] = mapped_column(Integer, default=30)
    primary_text: Mapped[str] = mapped_column(Text)
    primary_html: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_media_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_media_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    primary_media_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_media_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class DirectLaunch(Base):
    """One confirmed personal-message launch with immutable content and recipient rules."""

    __tablename__ = "direct_launches"
    __table_args__ = (
        Index("ix_direct_launches_status", "status", "created_at"),
        Index("ix_direct_launches_campaign", "campaign_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("direct_campaigns.id", ondelete="RESTRICT"))
    subscriber_list_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_lists.id", ondelete="RESTRICT")
    )
    status: Mapped[DirectLaunchStatus] = mapped_column(
        Enum(DirectLaunchStatus, native_enum=False, length=16),
        default=DirectLaunchStatus.QUEUED,
    )
    segment_snapshot: Mapped[SubscriberSegment] = mapped_column(
        Enum(SubscriberSegment, native_enum=False, length=16)
    )
    recipient_limit_snapshot: Mapped[int] = mapped_column(Integer)
    contact_cooldown_days_snapshot: Mapped[int] = mapped_column(Integer)
    account_ids_snapshot_json: Mapped[str] = mapped_column(Text)
    primary_text_snapshot: Mapped[str] = mapped_column(Text)
    primary_html_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_media_path_snapshot: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_media_kind_snapshot: Mapped[str | None] = mapped_column(String(20), nullable=True)
    primary_media_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_media_size_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    excluded_recent: Mapped[int] = mapped_column(Integer, default=0)
    excluded_reserved: Mapped[int] = mapped_column(Integer, default=0)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    consent_confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class Subscriber(Base):
    """A globally deduplicated Telegram recipient profile."""

    __tablename__ = "subscribers"
    __table_args__ = (
        UniqueConstraint("telegram_user_id", name="uq_subscribers_telegram_user_id"),
        Index("ix_subscribers_premium", "is_premium", "do_not_contact"),
        Index("ix_subscribers_username", "username"),
        Index("ix_subscribers_contacted", "last_contacted_at"),
        Index("ix_subscribers_delivery_uncertain", "last_delivery_uncertain_at"),
        Index("ix_subscribers_reservation", "reserved_direct_launch_id"),
        Index("ix_subscribers_invite_reservation", "reserved_invite_launch_id"),
        Index("ix_subscribers_invited", "last_invite_attempt_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger)
    username: Mapped[str | None] = mapped_column(String(32), nullable=True)
    is_premium: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    premium_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    do_not_contact: Mapped[bool] = mapped_column(Boolean, default=False)
    do_not_contact_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    last_contacted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_delivery_uncertain_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    reserved_direct_launch_id: Mapped[int | None] = mapped_column(
        ForeignKey("direct_launches.id", ondelete="SET NULL"), nullable=True
    )
    # Invitation reservations are deliberately separate from personal-message
    # reservations: an approved direct message must not block an unrelated
    # channel invitation forever, but two invite launches must never invite the
    # same person concurrently.
    reserved_invite_launch_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_invited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_invite_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_invited_target_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    invite_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberListMember(Base):
    __tablename__ = "subscriber_list_members"
    __table_args__ = (
        UniqueConstraint("subscriber_list_id", "subscriber_id", name="uq_subscriber_list_member"),
        Index("ix_subscriber_list_members_subscriber", "subscriber_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    subscriber_list_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_lists.id", ondelete="CASCADE")
    )
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id", ondelete="CASCADE"))
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class DirectCampaignAccount(Base):
    __tablename__ = "direct_campaign_accounts"
    __table_args__ = (
        UniqueConstraint("campaign_id", "account_id", name="uq_direct_campaign_account"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("direct_campaigns.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SubscriberAccountPeer(Base):
    """Account-scoped peer data; Telegram access hashes cannot cross auth sessions."""

    __tablename__ = "subscriber_account_peers"
    __table_args__ = (
        UniqueConstraint("subscriber_id", "account_id", name="uq_subscriber_account_peer"),
        Index("ix_subscriber_account_peer_block", "account_id", "send_blocked"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    send_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    blocked_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class DirectMessageTask(Base):
    """Durable, idempotent delivery of one saved post to one subscriber."""

    __tablename__ = "direct_message_tasks"
    __table_args__ = (
        UniqueConstraint("launch_id", "subscriber_id", name="uq_direct_message_recipient"),
        Index("ix_direct_message_tasks_due", "status", "scheduled_for"),
        Index("ix_direct_message_tasks_launch", "launch_id", "status"),
        Index("ix_direct_message_tasks_lease", "status", "lease_until"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    launch_id: Mapped[int] = mapped_column(ForeignKey("direct_launches.id", ondelete="CASCADE"))
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id", ondelete="RESTRICT"))
    sender_account_id: Mapped[int | None] = mapped_column(
        ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True
    )
    status: Mapped[DirectMessageTaskStatus] = mapped_column(
        Enum(DirectMessageTaskStatus, native_enum=False, length=16),
        default=DirectMessageTaskStatus.QUEUED,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberInviteCampaign(Base):
    """Saved, consent-confirmed invitation settings for one owned destination."""

    __tablename__ = "subscriber_invite_campaigns"
    __table_args__ = (
        CheckConstraint(
            "recipient_limit >= 1 AND recipient_limit <= 1000000",
            name="valid_subscriber_invite_recipient_limit",
        ),
        CheckConstraint(
            "contact_cooldown_days >= 0 AND contact_cooldown_days <= 3650",
            name="valid_subscriber_invite_cooldown",
        ),
        Index("ix_subscriber_invite_campaigns_enabled", "enabled", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    subscriber_list_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_lists.id", ondelete="RESTRICT")
    )
    destination_ref: Mapped[str] = mapped_column(String(255))
    destination_key: Mapped[str] = mapped_column(String(255))
    destination_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    segment: Mapped[SubscriberSegment] = mapped_column(
        Enum(SubscriberSegment, native_enum=False, length=16), default=SubscriberSegment.ALL
    )
    recipient_limit: Mapped[int] = mapped_column(Integer, default=10)
    contact_cooldown_days: Mapped[int] = mapped_column(Integer, default=90)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberInviteCampaignAccount(Base):
    __tablename__ = "subscriber_invite_campaign_accounts"
    __table_args__ = (
        UniqueConstraint("campaign_id", "account_id", name="uq_subscriber_invite_campaign_account"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_invite_campaigns.id", ondelete="CASCADE")
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SubscriberInviteLaunch(Base):
    """One immutable invitation batch, queued only after operator confirmation."""

    __tablename__ = "subscriber_invite_launches"
    __table_args__ = (
        Index("ix_subscriber_invite_launches_status", "status", "created_at"),
        Index("ix_subscriber_invite_launches_campaign", "campaign_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_invite_campaigns.id", ondelete="RESTRICT")
    )
    subscriber_list_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_lists.id", ondelete="RESTRICT")
    )
    status: Mapped[DirectLaunchStatus] = mapped_column(
        Enum(DirectLaunchStatus, native_enum=False, length=16), default=DirectLaunchStatus.QUEUED
    )
    destination_ref_snapshot: Mapped[str] = mapped_column(String(255))
    destination_key_snapshot: Mapped[str] = mapped_column(String(255))
    destination_title_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    segment_snapshot: Mapped[SubscriberSegment] = mapped_column(
        Enum(SubscriberSegment, native_enum=False, length=16)
    )
    recipient_limit_snapshot: Mapped[int] = mapped_column(Integer)
    contact_cooldown_days_snapshot: Mapped[int] = mapped_column(Integer)
    account_ids_snapshot_json: Mapped[str] = mapped_column(Text)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    excluded_recent: Mapped[int] = mapped_column(Integer, default=0)
    excluded_reserved: Mapped[int] = mapped_column(Integer, default=0)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    failure_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    failure: Mapped[str | None] = mapped_column(String(500), nullable=True)
    consent_confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberInviteAccountRoute(Base):
    """Account-local user access hash and a durable failed invitation route."""

    __tablename__ = "subscriber_invite_account_routes"
    __table_args__ = (
        UniqueConstraint("subscriber_id", "account_id", name="uq_subscriber_invite_account_route"),
        Index("ix_subscriber_invite_route_block", "account_id", "invite_blocked"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    invite_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    blocked_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberInviteLaunchAccountState(Base):
    """A sender that cannot invite into one particular immutable launch."""

    __tablename__ = "subscriber_invite_launch_account_states"
    __table_args__ = (
        UniqueConstraint("launch_id", "account_id", name="uq_subscriber_invite_launch_account"),
        Index("ix_subscriber_invite_launch_account_block", "launch_id", "send_blocked"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    launch_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_invite_launches.id", ondelete="CASCADE")
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    send_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    blocked_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    blocked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SubscriberInviteTask(Base):
    """One conservative, non-retried-over-network invitation attempt."""

    __tablename__ = "subscriber_invite_tasks"
    __table_args__ = (
        UniqueConstraint("launch_id", "subscriber_id", name="uq_subscriber_invite_recipient"),
        Index("ix_subscriber_invite_tasks_due", "status", "scheduled_for"),
        Index("ix_subscriber_invite_tasks_launch", "launch_id", "status"),
        Index("ix_subscriber_invite_tasks_lease", "status", "lease_until"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    launch_id: Mapped[int] = mapped_column(
        ForeignKey("subscriber_invite_launches.id", ondelete="CASCADE")
    )
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id", ondelete="RESTRICT"))
    sender_account_id: Mapped[int | None] = mapped_column(
        ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True
    )
    status: Mapped[DirectMessageTaskStatus] = mapped_column(
        Enum(DirectMessageTaskStatus, native_enum=False, length=16),
        default=DirectMessageTaskStatus.QUEUED,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivery_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    already_participant: Mapped[bool] = mapped_column(Boolean, default=False)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ActionLog(Base):
    __tablename__ = "action_logs"
    __table_args__ = (
        Index("ix_action_logs_run_created", "run_id", "created_at"),
        Index(
            "ix_action_logs_delayed_delivery",
            "status",
            "delayed_delivery_checked_at",
            "created_at",
        ),
        Index(
            "ix_action_logs_delayed_delivery_due",
            "status",
            "delayed_delivery_checked_at",
            "delayed_delivery_retry_at",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("campaign_runs.id"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id"))
    kind: Mapped[ActionKind] = mapped_column(Enum(ActionKind, native_enum=False))
    status: Mapped[ActionStatus] = mapped_column(Enum(ActionStatus, native_enum=False))
    external_message_id: Mapped[int | None] = mapped_column(nullable=True)
    reply_to_message_id: Mapped[int | None] = mapped_column(nullable=True)
    text: Mapped[str] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    attempt_number: Mapped[int] = mapped_column(Integer, default=1)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delayed_delivery_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    delayed_delivery_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    delayed_delivery_lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    delayed_delivery_lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    delayed_delivery_lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ReactionTask(Base):
    """A separate, durable reaction to a message already accepted by Telegram."""

    __tablename__ = "reaction_tasks"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "message_id",
            "reactor_account_id",
            name="uq_reaction_task_message_reactor",
        ),
        UniqueConstraint(
            "channel_comment_run_id",
            "message_id",
            "reactor_account_id",
            name="uq_comment_reaction_task_message_reactor",
        ),
        CheckConstraint(
            "(run_id IS NOT NULL AND channel_comment_run_id IS NULL "
            "AND campaign_id IS NOT NULL AND target_id IS NOT NULL) OR "
            "(run_id IS NULL AND channel_comment_run_id IS NOT NULL "
            "AND campaign_id IS NULL AND target_id IS NULL)",
            name="exactly_one_reaction_context",
        ),
        Index("ix_reaction_tasks_due", "status", "scheduled_for"),
        Index("ix_reaction_tasks_campaign", "campaign_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int | None] = mapped_column(
        ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=True
    )
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("campaign_runs.id", ondelete="CASCADE"), nullable=True
    )
    channel_comment_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("channel_comment_runs.id", ondelete="CASCADE"), nullable=True
    )
    target_id: Mapped[int | None] = mapped_column(
        ForeignKey("targets.id", ondelete="CASCADE"), nullable=True
    )
    source_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    reactor_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    message_id: Mapped[int] = mapped_column(Integer)
    emoji: Mapped[str] = mapped_column(String(16))
    status: Mapped[ReactionTaskStatus] = mapped_column(
        Enum(ReactionTaskStatus, native_enum=False, length=16), default=ReactionTaskStatus.QUEUED
    )
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (Index("ix_audit_events_created", "created_at"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    actor: Mapped[str] = mapped_column(String(100), default="admin-api")
    event_type: Mapped[str] = mapped_column(String(100))
    entity_type: Mapped[str] = mapped_column(String(100))
    entity_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    details_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class BotFsmState(Base):
    """Durable aiogram wizard state for surviving control-bot restarts."""

    __tablename__ = "bot_fsm_states"

    storage_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    state: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_json: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ChannelCommentRule(Base):
    """A manually approved channel whose future posts receive a configured comment flow."""

    __tablename__ = "channel_comment_rules"
    __table_args__ = (
        Index("ix_channel_comment_rules_scan", "enabled", "archived_at", "last_scanned_at"),
        Index(
            "uq_channel_comment_rules_active_name",
            "name",
            unique=True,
            sqlite_where=sql_text("archived_at IS NULL"),
            postgresql_where=sql_text("archived_at IS NULL"),
        ),
        Index(
            "uq_channel_comment_rules_active_channel",
            "channel_ref",
            unique=True,
            sqlite_where=sql_text("archived_at IS NULL"),
            postgresql_where=sql_text("archived_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150))
    channel_ref: Mapped[str] = mapped_column(String(255))
    channel_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mode: Mapped[ChannelCommentMode] = mapped_column(
        Enum(ChannelCommentMode, native_enum=False, length=16),
        default=ChannelCommentMode.SINGLE,
    )
    primary_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    primary_text: Mapped[str] = mapped_column(Text)
    primary_media_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_media_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    primary_media_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    primary_media_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dialogue_steps_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_edit_initial_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_edit_delay_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reactions_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    reaction_count: Mapped[int] = mapped_column(Integer, default=0)
    reaction_emoji: Mapped[str] = mapped_column(String(16), default="👍")
    reaction_dialogue_step: Mapped[int] = mapped_column(Integer, default=1)
    reaction_account_ids_json: Mapped[str] = mapped_column(Text, default="[]")
    hidden_mentions_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    mention_cooldown_days: Mapped[int] = mapped_column(Integer, default=30)
    initial_delay_seconds: Mapped[int] = mapped_column(Integer, default=0)
    approved_by_operator: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    armed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    last_seen_post_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_account_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ChannelCommentRun(Base):
    """Durable, idempotent execution of one comment flow under one channel post."""

    __tablename__ = "channel_comment_runs"
    __table_args__ = (
        UniqueConstraint("rule_id", "source_post_id", name="uq_channel_comment_rule_post"),
        Index("ix_channel_comment_runs_due", "status", "scheduled_for"),
        Index("ix_channel_comment_runs_rule", "rule_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("channel_comment_rules.id", ondelete="CASCADE"))
    source_post_id: Mapped[int] = mapped_column(Integer)
    source_post_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[ChannelCommentRunStatus] = mapped_column(
        Enum(ChannelCommentRunStatus, native_enum=False, length=16),
        default=ChannelCommentRunStatus.QUEUED,
    )
    step_index: Mapped[int] = mapped_column(Integer, default=0)
    sent_steps: Mapped[int] = mapped_column(Integer, default=0)
    hidden_mentions_enabled_snapshot: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    mention_cooldown_days_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hidden_mention_user_ids_snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    hidden_mention_skip_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    hidden_mention_resolved_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    thread_root_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_comment_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ChannelCommentMentionState(Base):
    """Cooldown and in-flight reservation for one participant under one rule."""

    __tablename__ = "channel_comment_mention_states"
    __table_args__ = (
        UniqueConstraint(
            "rule_id",
            "telegram_user_id",
            name="uq_channel_comment_mention_state_user",
        ),
        Index(
            "ix_channel_comment_mention_state_cooldown",
            "rule_id",
            "last_mentioned_at",
        ),
        Index(
            "ix_channel_comment_mention_state_reservation",
            "reserved_run_id",
            "reserved_until",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("channel_comment_rules.id", ondelete="CASCADE"))
    telegram_user_id: Mapped[int] = mapped_column(BigInteger)
    last_mentioned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    mention_count: Mapped[int] = mapped_column(Integer, default=0)
    reserved_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("channel_comment_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    reserved_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ChannelCommentReactionAccountState(Base):
    """Persistent route block for a reactor in one channel-comment rule."""

    __tablename__ = "channel_comment_reaction_account_states"
    __table_args__ = (
        UniqueConstraint(
            "rule_id",
            "account_id",
            name="uq_channel_comment_reaction_account",
        ),
        Index(
            "ix_channel_comment_reaction_blocked",
            "rule_id",
            "send_blocked",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("channel_comment_rules.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    send_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    blocked_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    blocked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ParticipantReactionRule(Base):
    """Opt-in chat-list rule for a small, sampled set of participant messages."""

    __tablename__ = "participant_reaction_rules"
    __table_args__ = (
        CheckConstraint(
            "sample_numerator >= 1 AND sample_denominator >= sample_numerator",
            name="valid_participant_reaction_sample",
        ),
        Index("ix_participant_reaction_rules_scan", "enabled", "last_scanned_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True)
    target_list_id: Mapped[int] = mapped_column(ForeignKey("target_lists.id", ondelete="CASCADE"))
    account_ids_json: Mapped[str] = mapped_column(Text, default="[]")
    emoji: Mapped[str] = mapped_column(String(16), default="👍")
    sample_numerator: Mapped[int] = mapped_column(Integer, default=1)
    sample_denominator: Mapped[int] = mapped_column(Integer, default=10)
    approved_by_operator: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ParticipantReactionTargetState(Base):
    """One cursor per rule and chat, so new chats never process their old history."""

    __tablename__ = "participant_reaction_target_states"
    __table_args__ = (
        UniqueConstraint("rule_id", "target_id", name="uq_participant_reaction_rule_target"),
        Index("ix_participant_reaction_target_state", "rule_id", "last_seen_message_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("participant_reaction_rules.id", ondelete="CASCADE")
    )
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id", ondelete="CASCADE"))
    last_seen_message_id: Mapped[int] = mapped_column(Integer, default=0)
    primed: Mapped[bool] = mapped_column(Boolean, default=False)
    last_scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ParticipantReactionTask(Base):
    """Durable one-account reaction to an observed participant message."""

    __tablename__ = "participant_reaction_tasks"
    __table_args__ = (
        UniqueConstraint(
            "rule_id", "target_id", "message_id", name="uq_participant_reaction_message"
        ),
        Index("ix_participant_reaction_tasks_due", "status", "scheduled_for"),
        Index("ix_participant_reaction_tasks_rule", "rule_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("participant_reaction_rules.id", ondelete="CASCADE")
    )
    target_id: Mapped[int] = mapped_column(ForeignKey("targets.id", ondelete="CASCADE"))
    reactor_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    message_id: Mapped[int] = mapped_column(Integer)
    source_sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    emoji: Mapped[str] = mapped_column(String(16))
    status: Mapped[ParticipantReactionTaskStatus] = mapped_column(
        Enum(ParticipantReactionTaskStatus, native_enum=False, length=16),
        default=ParticipantReactionTaskStatus.QUEUED,
    )
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ProxyGroup(Base):
    __tablename__ = "proxy_groups"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    description: Mapped[str] = mapped_column(String(500), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ProxyEndpoint(Base):
    __tablename__ = "proxy_endpoints"
    __table_args__ = (
        UniqueConstraint("fingerprint", name="uq_proxy_endpoints_fingerprint"),
        CheckConstraint("port >= 1 AND port <= 65535", name="valid_proxy_port"),
        CheckConstraint(
            "success_count >= 0 AND failure_count >= 0 AND consecutive_failures >= 0",
            name="non_negative_proxy_counters",
        ),
        Index("ix_proxy_endpoints_status_enabled", "status", "enabled"),
        Index("ix_proxy_endpoints_group", "group_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    label: Mapped[str] = mapped_column(String(100))
    group_id: Mapped[int | None] = mapped_column(
        ForeignKey("proxy_groups.id", ondelete="SET NULL"), nullable=True
    )
    protocol: Mapped[ProxyProtocol] = mapped_column(Enum(ProxyProtocol, native_enum=False))
    host: Mapped[str] = mapped_column(String(255))
    port: Mapped[int] = mapped_column(Integer)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    password_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    rotation_url_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    fingerprint: Mapped[str] = mapped_column(String(64))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[ProxyStatus] = mapped_column(
        Enum(ProxyStatus, native_enum=False), default=ProxyStatus.UNKNOWN
    )
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    observed_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ProxyAssignment(Base):
    __tablename__ = "proxy_assignments"
    __table_args__ = (UniqueConstraint("assignment_key", name="uq_proxy_assignments_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    proxy_id: Mapped[int] = mapped_column(ForeignKey("proxy_endpoints.id", ondelete="CASCADE"))
    assignment_key: Mapped[str] = mapped_column(String(150))
    purpose: Mapped[str] = mapped_column(String(250), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
