from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

from proxima_traffic.content import ContentError, validate_spintax, validate_telegram_text
from proxima_traffic.dialogues import MAX_DIALOGUE_STEPS, validate_dialogue_steps
from proxima_traffic.models import (
    AccountStatus,
    ActionKind,
    ActionStatus,
    BroadcastSenderMode,
    CampaignMode,
    ProxyStatus,
    RunStatus,
    RunStep,
)


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PageRead[PageItem](BaseModel):
    """A stable, bounded response envelope for collection endpoints."""

    items: list[PageItem]
    total: int = Field(ge=0)
    page: int = Field(ge=0)
    page_size: int = Field(ge=1, le=500)


class AccountCreate(BaseModel):
    label: str = Field(min_length=1, max_length=100)
    telegram_user_id: int | None = None
    ownership_confirmed: bool = False


class AccountRead(ORMModel):
    id: int
    label: str
    telegram_user_id: int | None
    ownership_confirmed: bool
    status: AccountStatus
    cooldown_until: datetime | None
    cooldown_reason: str | None
    spambot_checked_at: datetime | None
    spambot_status: str | None
    spambot_parser_signal: str | None
    spambot_response_json: str | None
    created_at: datetime


class AccountUpdate(BaseModel):
    status: AccountStatus | None = None
    ownership_confirmed: bool | None = None

    @model_validator(mode="after")
    def reject_explicit_nulls(self) -> "AccountUpdate":
        _reject_explicit_nulls(self)
        return self


class TargetCreate(BaseModel):
    chat_ref: str = Field(min_length=1, max_length=255)
    title: str = Field(min_length=1, max_length=150)


class TargetRead(ORMModel):
    id: int
    chat_ref: str
    title: str
    is_standalone: bool
    manual_cooldown_seconds: int
    approved_by_operator: bool
    approved_at: datetime | None
    quality_status: str
    quality_score: int
    quality_reason: str | None
    quality_attempts: int
    quality_successes: int
    quality_failures: int
    quality_banned_accounts: int
    quality_updated_at: datetime | None
    quality_bad_at: datetime | None
    created_at: datetime


class TargetCooldownUpdate(BaseModel):
    manual_cooldown_seconds: int = Field(ge=0, le=7 * 24 * 3600)


class TargetListCreate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    chat_refs: list[str] = Field(min_length=1, max_length=500)


class TargetListRead(BaseModel):
    id: int
    name: str
    item_count: int
    approved_count: int


class MessageTemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    primary_text: str = Field(min_length=1, max_length=4096)
    reply_text: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def validate_template_spintax(self) -> "MessageTemplateCreate":
        try:
            validate_spintax(self.primary_text)
            validate_spintax(self.reply_text)
        except ContentError as exc:
            raise ValueError(str(exc)) from exc
        return self


class MessageTemplateUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=150)
    primary_text: str | None = Field(default=None, min_length=1, max_length=4096)
    reply_text: str | None = Field(default=None, min_length=1, max_length=4096)
    enabled: bool | None = None

    @model_validator(mode="after")
    def reject_explicit_nulls(self) -> "MessageTemplateUpdate":
        _reject_explicit_nulls(self)
        try:
            if self.primary_text is not None:
                validate_spintax(self.primary_text)
            if self.reply_text is not None:
                validate_spintax(self.reply_text)
        except ContentError as exc:
            raise ValueError(str(exc)) from exc
        return self


class MessageTemplateRead(ORMModel):
    id: int
    name: str
    primary_text: str
    reply_text: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


class DialogueStepPayload(BaseModel):
    account_id: int = Field(gt=0)
    text: str = Field(default="", max_length=4096)
    delay_seconds: int = Field(default=0, ge=0, le=24 * 3600)
    reply_to_previous: bool = True
    media_path: str | None = Field(default=None, max_length=500)
    media_kind: str | None = Field(default=None, max_length=20)
    media_name: str | None = Field(default=None, max_length=255)
    media_size: int | None = Field(default=None, ge=0)


class CampaignCreate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    mode: CampaignMode = CampaignMode.DIALOGUE
    primary_account_id: int
    reply_account_id: int | None = None
    sender_account_ids: list[int] = Field(default_factory=list, max_length=100)
    sender_mode: BroadcastSenderMode = BroadcastSenderMode.DISTRIBUTED
    target_id: int | None = None
    target_list_id: int | None = None
    primary_text: str = Field(default="", max_length=4096)
    primary_html: str | None = Field(default=None, max_length=16_384)
    reply_text: str = Field(default="", max_length=4096)
    reply_delay_seconds: int = Field(default=0, ge=0, le=3600)
    spread_seconds: int = Field(default=0, ge=0, le=7 * 24 * 3600)
    repeat_count: int = Field(default=1, ge=1, le=100)
    repeat_interval_seconds: int = Field(default=0, ge=0, le=7 * 24 * 3600)
    smart_timer_mode: bool = False
    smart_duration_seconds: int = Field(default=0, ge=0, le=7 * 24 * 3600)
    smart_no_timer_interval_seconds: int = Field(default=900, ge=60, le=7 * 24 * 3600)
    auto_sender_failover: bool = False
    activity_aware: bool = False
    min_messages_between_posts: int = Field(default=0, ge=0, le=100_000)
    activity_check_interval_seconds: int = Field(default=300, ge=30, le=3600)
    visibility_max_wait_seconds: int = Field(default=3600, ge=60, le=7 * 24 * 3600)
    hidden_mentions_enabled: bool = False
    mention_cooldown_days: int = Field(default=30, ge=1, le=365)
    dialogue_steps: list[DialogueStepPayload] = Field(
        default_factory=list,
        max_length=MAX_DIALOGUE_STEPS,
    )

    @model_validator(mode="after")
    def validate_campaign_shape(self) -> "CampaignCreate":
        if (self.target_id is None) == (self.target_list_id is None):
            raise ValueError("exactly one of target_id or target_list_id is required")
        if not self.primary_text.strip() and not (
            self.mode == CampaignMode.DIALOGUE and self.dialogue_steps
        ):
            raise ValueError("primary_text must contain non-whitespace characters")
        self.sender_account_ids = list(dict.fromkeys(self.sender_account_ids))
        if self.mode == CampaignMode.DIALOGUE:
            self.sender_mode = BroadcastSenderMode.DISTRIBUTED
            if self.primary_html is not None:
                raise ValueError("primary_html is only supported for broadcast campaigns")
            if self.hidden_mentions_enabled:
                raise ValueError(
                    "hidden_mentions_enabled is only supported for broadcast campaigns"
                )
            if self.mention_cooldown_days != 30:
                raise ValueError("mention_cooldown_days is only supported for broadcast campaigns")
            if self.dialogue_steps:
                try:
                    normalized = validate_dialogue_steps(
                        [step.model_dump() for step in self.dialogue_steps]
                    )
                except ValueError as exc:
                    raise ValueError(str(exc)) from exc
                self.dialogue_steps = [DialogueStepPayload(**step.as_dict()) for step in normalized]
                self.primary_account_id = normalized[0].account_id
                self.reply_account_id = normalized[1].account_id
                self.primary_text = normalized[0].text
                self.reply_text = normalized[1].text
                self.reply_delay_seconds = normalized[1].delay_seconds
            if self.reply_account_id is None:
                raise ValueError("для диалога нужен аккаунт ответа")
            if self.primary_account_id == self.reply_account_id:
                raise ValueError("primary and reply accounts must be different")
            reply_is_captionless_media = bool(
                self.dialogue_steps
                and self.dialogue_steps[1].media_kind in {"voice", "video_note"}
            )
            if not self.reply_text.strip() and not reply_is_captionless_media:
                raise ValueError("для диалога нужен текст ответа")
            try:
                validate_spintax(self.primary_text)
                validate_spintax(self.reply_text)
            except ContentError as exc:
                raise ValueError(str(exc)) from exc
            if self.sender_account_ids:
                raise ValueError("sender_account_ids are only supported for broadcast campaigns")
            if self.spread_seconds != 0:
                raise ValueError("spread_seconds is only supported for broadcast campaigns")
            if (
                self.repeat_count != 1
                or self.repeat_interval_seconds != 0
                or self.auto_sender_failover
            ):
                raise ValueError("для диалога доступен один проход или умный режим")
            if self.smart_timer_mode:
                if not 60 <= self.smart_duration_seconds <= 7 * 24 * 3600:
                    raise ValueError("smart_duration_seconds must be between 1 minute and 7 days")
                if self.activity_aware and self.min_messages_between_posts < 1:
                    raise ValueError(
                        "min_messages_between_posts must be positive in activity-aware mode"
                    )
            elif self.smart_duration_seconds != 0:
                raise ValueError("smart_duration_seconds requires smart_timer_mode")
            elif self.activity_aware:
                raise ValueError("activity_aware requires smart_timer_mode")
        elif self.mode == CampaignMode.BROADCAST:
            try:
                validate_telegram_text(self.primary_text, self.primary_html)
            except ContentError as exc:
                raise ValueError(str(exc)) from exc
            if self.sender_mode == BroadcastSenderMode.INDEPENDENT:
                self.auto_sender_failover = False
            if self.dialogue_steps:
                raise ValueError("цепочку dialogue_steps можно использовать только в диалоге")
            if self.reply_account_id is not None:
                raise ValueError("broadcast campaign must not set reply_account_id")
            if self.reply_text.strip():
                raise ValueError("broadcast campaign must not set reply_text")
            if self.reply_delay_seconds != 0:
                raise ValueError("задержку ответа можно использовать только в диалоге")
            if self.smart_timer_mode:
                if self.repeat_count != 1:
                    raise ValueError("timer-driven broadcasts must have repeat_count equal to 1")
                if self.repeat_interval_seconds != 0 or self.spread_seconds != 0:
                    raise ValueError(
                        "timer-driven broadcasts use smart duration and no-timer interval fields"
                    )
                if not 60 <= self.smart_duration_seconds <= 7 * 24 * 3600:
                    raise ValueError("smart_duration_seconds must be between 1 minute and 7 days")
            elif self.repeat_count == 1 and self.repeat_interval_seconds != 0:
                raise ValueError("one-pass broadcast must have a zero repeat interval")
            elif self.smart_duration_seconds != 0:
                raise ValueError("smart_duration_seconds requires smart_timer_mode")
            if self.repeat_count > 1 and self.repeat_interval_seconds < 60:
                raise ValueError("repeat_interval_seconds must be at least 60 for repeats")
            if self.activity_aware:
                if not self.smart_timer_mode:
                    raise ValueError("activity_aware requires smart_timer_mode")
                if self.min_messages_between_posts < 1:
                    raise ValueError(
                        "min_messages_between_posts must be positive in activity-aware mode"
                    )
        return self


class CampaignRead(ORMModel):
    id: int
    name: str
    mode: CampaignMode
    primary_account_id: int
    reply_account_id: int | None
    sender_account_ids: list[int] = Field(default_factory=list)
    sender_mode: BroadcastSenderMode
    target_id: int | None
    target_list_id: int | None
    primary_text: str
    primary_html: str | None
    primary_media_kind: str | None
    primary_media_name: str | None
    primary_media_size: int | None
    reply_text: str
    reply_delay_seconds: int
    spread_seconds: int
    repeat_count: int
    repeat_interval_seconds: int
    smart_timer_mode: bool
    smart_duration_seconds: int
    smart_no_timer_interval_seconds: int
    auto_sender_failover: bool
    activity_aware: bool
    min_messages_between_posts: int
    activity_check_interval_seconds: int
    visibility_max_wait_seconds: int
    hidden_mentions_enabled: bool
    mention_cooldown_days: int
    content_variant_count: int = 1
    enabled_content_variant_count: int = 1
    dialogue_steps: list[DialogueStepPayload] = Field(default_factory=list)
    enabled: bool
    created_at: datetime


class CampaignUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=150)
    primary_text: str | None = Field(default=None, min_length=1, max_length=4096)
    primary_html: str | None = Field(default=None, min_length=1, max_length=16_384)
    reply_text: str | None = Field(default=None, min_length=1, max_length=4096)
    reply_delay_seconds: int | None = Field(default=None, ge=0, le=3600)
    spread_seconds: int | None = Field(default=None, ge=0, le=7 * 24 * 3600)
    repeat_count: int | None = Field(default=None, ge=1, le=100)
    repeat_interval_seconds: int | None = Field(default=None, ge=0, le=7 * 24 * 3600)
    smart_timer_mode: bool | None = None
    smart_duration_seconds: int | None = Field(default=None, ge=0, le=7 * 24 * 3600)
    smart_no_timer_interval_seconds: int | None = Field(default=None, ge=60, le=7 * 24 * 3600)
    auto_sender_failover: bool | None = None
    sender_mode: BroadcastSenderMode | None = None
    activity_aware: bool | None = None
    min_messages_between_posts: int | None = Field(default=None, ge=0, le=100_000)
    activity_check_interval_seconds: int | None = Field(default=None, ge=30, le=3600)
    visibility_max_wait_seconds: int | None = Field(default=None, ge=60, le=7 * 24 * 3600)
    hidden_mentions_enabled: bool | None = None
    mention_cooldown_days: int | None = Field(default=None, ge=1, le=365)
    sender_account_ids: list[int] | None = Field(default=None, max_length=100)
    dialogue_steps: list[DialogueStepPayload] | None = Field(
        default=None,
        min_length=2,
        max_length=MAX_DIALOGUE_STEPS,
    )
    enabled: bool | None = None

    @model_validator(mode="after")
    def reject_explicit_nulls(self) -> "CampaignUpdate":
        _reject_explicit_nulls(self)
        try:
            if self.primary_text is not None:
                validate_spintax(self.primary_text)
            if self.reply_text is not None:
                validate_spintax(self.reply_text)
        except ContentError as exc:
            raise ValueError(str(exc)) from exc
        if self.sender_account_ids is not None:
            self.sender_account_ids = list(dict.fromkeys(self.sender_account_ids))
        if self.dialogue_steps is not None:
            try:
                normalized = validate_dialogue_steps(
                    [step.model_dump() for step in self.dialogue_steps]
                )
            except ValueError as exc:
                raise ValueError(str(exc)) from exc
            self.dialogue_steps = [DialogueStepPayload(**step.as_dict()) for step in normalized]
        return self


class CampaignPreflightRead(BaseModel):
    campaign_id: int
    account_count: int
    target_count: int
    checked_pairs: int
    ready_pairs: int
    member_targets: int
    joinable_targets: int
    blocked_targets: int
    unavailable_targets: int
    uncovered_targets: int
    oldest_checked_at: datetime | None
    newest_checked_at: datetime | None


def _reject_explicit_nulls(payload: BaseModel) -> None:
    for field_name in payload.model_fields_set:
        if getattr(payload, field_name) is None:
            raise ValueError(f"{field_name} cannot be null")


class RunCreate(BaseModel):
    request_key: str = Field(min_length=8, max_length=64)


class ActiveLaunchUpdate(BaseModel):
    primary_text: str | None = Field(default=None, min_length=1, max_length=4096)
    no_timer_interval_seconds: int | None = Field(default=None, ge=60, le=7 * 24 * 3600)
    remaining_duration_seconds: int | None = Field(default=None, ge=60, le=7 * 24 * 3600)

    @model_validator(mode="after")
    def require_change(self) -> "ActiveLaunchUpdate":
        if (
            self.primary_text is None
            and self.no_timer_interval_seconds is None
            and self.remaining_duration_seconds is None
        ):
            raise ValueError("at least one active launch setting is required")
        if self.primary_text is not None and not self.primary_text.strip():
            raise ValueError("primary_text must contain non-whitespace characters")
        return self


class ActiveLaunchUpdateRead(BaseModel):
    anchor_run_id: int
    updated_tasks: int


class LaunchArchiveRead(BaseModel):
    anchor_run_id: int
    archived: bool
    updated_tasks: int


class RunRead(ORMModel):
    id: int
    campaign_id: int
    target_id: int
    sender_account_id: int | None
    cycle_index: int
    content_variant_index: int
    content_variant_id_snapshot: str | None
    content_variant_name_snapshot: str | None
    request_key: str
    status: RunStatus
    next_step: RunStep
    attempt_count: int
    membership_gate_retry_count: int
    max_attempts: int
    scheduled_for: datetime
    repeat_until: datetime | None
    lease_owner: str | None
    lease_until: datetime | None
    heartbeat_at: datetime | None
    cancel_requested: bool
    primary_text_snapshot: str | None
    primary_html_snapshot: str | None
    primary_media_kind_snapshot: str | None
    primary_media_name_snapshot: str | None
    primary_media_size_snapshot: int | None
    reply_text_snapshot: str | None
    reply_delay_seconds_snapshot: int | None
    no_timer_interval_seconds_snapshot: int | None
    auto_sender_failover_snapshot: bool | None
    sender_mode_snapshot: BroadcastSenderMode | None
    dialogue_step_index: int
    dialogue_last_message_id: int | None
    primary_message_id: int | None
    reply_message_id: int | None
    error: str | None
    error_code: str | None
    created_at: datetime
    started_at: datetime | None
    updated_at: datetime
    finished_at: datetime | None
    notified_at: datetime | None
    archived_at: datetime | None


class ActionLogRead(ORMModel):
    id: int
    run_id: int
    account_id: int
    target_id: int
    kind: ActionKind
    status: ActionStatus
    external_message_id: int | None
    reply_to_message_id: int | None
    text: str
    error: str | None
    attempt_number: int
    created_at: datetime


class RunDetails(RunRead):
    actions: list[ActionLogRead]


class StatusRead(BaseModel):
    dry_run: bool
    accounts: int
    approved_targets: int
    target_lists: int
    templates: int
    campaigns: int
    runs: int
    queued_runs: int
    failed_runs: int
    proxies: int
    healthy_proxies: int


class ProxyGroupCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(default="", max_length=500)


class ProxyGroupRead(ORMModel):
    id: int
    name: str
    description: str
    created_at: datetime


class ProxyCreate(BaseModel):
    url: str = Field(min_length=3, max_length=1000)
    label: str | None = Field(default=None, min_length=1, max_length=100)
    group_name: str | None = Field(default=None, min_length=1, max_length=100)
    rotation_url: str | None = Field(default=None, min_length=1, max_length=2000)


class ProxyBatchCreate(BaseModel):
    values: list[str] = Field(min_length=1, max_length=2000)
    group_name: str | None = Field(default=None, min_length=1, max_length=100)


class ProxyImportRead(BaseModel):
    added: int
    skipped: int
    invalid: int
    errors: list[str]


class ProxyRead(BaseModel):
    id: int
    label: str
    group_id: int | None
    group_name: str | None
    endpoint: str
    enabled: bool
    status: ProxyStatus
    success_count: int
    failure_count: int
    consecutive_failures: int
    latency_ms: int | None
    observed_ip: str | None
    last_error: str | None
    last_checked_at: datetime | None
    has_rotation_url: bool
    last_rotated_at: datetime | None
    assignment_key: str | None
    assignment_purpose: str | None
    assignment_keys: list[str] = Field(default_factory=list)
    assignment_count: int = 0
    created_at: datetime
    updated_at: datetime


class ProxyAssignmentCreate(BaseModel):
    assignment_key: str = Field(min_length=1, max_length=150)
    purpose: str = Field(default="", max_length=250)


class ProxyAllocationCreate(ProxyAssignmentCreate):
    group_name: str | None = Field(default=None, min_length=1, max_length=100)


class ProxyEndpointUpdate(BaseModel):
    url: str = Field(min_length=3, max_length=1000)


class ProxyRotationUpdate(BaseModel):
    rotation_url: str | None = Field(default=None, min_length=1, max_length=2000)


class ProxyMetadataUpdate(BaseModel):
    label: str = Field(min_length=1, max_length=100)
    group_name: str | None = Field(default=None, min_length=1, max_length=100)


class ProxyCheckRead(BaseModel):
    proxy_id: int
    healthy: bool
    status: ProxyStatus
    latency_ms: int | None
    observed_ip: str | None
    error: str | None


class AuditEventRead(ORMModel):
    id: int
    actor: str
    event_type: str
    entity_type: str
    entity_id: str | None
    details_json: str
    created_at: datetime


class WorkerStatusRead(BaseModel):
    worker_id: str
    running: bool
    healthy: bool
    background_workers: dict[str, bool]
    active_steps: int
    processed_steps: int
    failed_steps: int
    last_error: str | None
    last_processed_at: str | None
    queued_runs: int
    running_runs: int
    expired_leases: int
