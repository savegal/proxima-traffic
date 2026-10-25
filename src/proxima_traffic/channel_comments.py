from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.content import (
    ALLOWED_MEDIA_KINDS,
    ContentError,
    render_spintax,
    render_telegram_html,
    resolve_media_path,
    validate_telegram_text,
)
from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.dialogues import DialogueStep, parse_dialogue_steps, serialize_dialogue_steps
from proxima_traffic.failure_policy import (
    ACCOUNT_CHAT_BLOCK_CODES,
    ACCOUNT_FATAL_CODES,
    ACCOUNT_TEMPORARY_CODES,
    CHANNEL_COMMENT_AUTO_PAUSE_CODES,
    failure_code,
)
from proxima_traffic.gateways import (
    ORDINARY_REACTION_EMOJI,
    ChannelCommentDelivery,
    ChannelCommentGateway,
    ChannelCommentPreflight,
    ChannelPostScan,
    GatewayError,
    MembershipGateRecovery,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.mentions import (
    HIDDEN_MENTION_MAXIMUM,
    HIDDEN_MENTION_MINIMUM,
    append_hidden_mentions,
    hidden_mention_capacity,
    load_mention_exclusion_user_ids,
    normalize_user_ids,
    select_hidden_mentions,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ChannelCommentMentionState,
    ChannelCommentMessage,
    ChannelCommentMode,
    ChannelCommentReactionAccountState,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    ReactionTask,
    ReactionTaskStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import (
    AccountSendDeferred,
    CampaignRunner,
    ConflictError,
    NotFoundError,
    add_audit_event,
    safe_error_message,
)
from proxima_traffic.worker_supervision import WorkerIterationReporter

if TYPE_CHECKING:
    from proxima_traffic.config import Settings


logger = logging.getLogger(__name__)


AUTO_REPLACE_PRIMARY_ACCOUNT_CODES = (
    ACCOUNT_FATAL_CODES
    | ACCOUNT_CHAT_BLOCK_CODES
    | frozenset({"account_paused", "account_restricted", "sender_unavailable"})
)


class _ChannelCommentCancelled(PermanentGatewayError):
    def __init__(self) -> None:
        super().__init__("Правило остановлено до отправки", code="cancelled_before_send")


@dataclass(frozen=True, slots=True)
class ChannelCommentDeliverySummary:
    """Physical messages, partitioned by their most recent late observation."""

    confirmed: int = 0
    simulated: int = 0
    present: int = 0
    missing: int = 0
    unavailable: int = 0
    pending: int = 0
    scheduled: int = 0
    checked_at: datetime | None = None
    next_check_at: datetime | None = None
    history_incomplete: bool = False


@dataclass(frozen=True, slots=True)
class ChannelCommentRuleView:
    rule: ChannelCommentRule
    queued: int
    running: int
    succeeded: int
    failed: int
    skipped: int
    uncertain: int = 0
    cancelled: int = 0
    simulated_finished: int = 0
    mentions_resolved_posts: int = 0
    mentions_skipped_posts: int = 0
    mention_count: int = 0
    mention_skip_reasons: tuple[tuple[str, int], ...] = ()
    reaction_queued: int = 0
    reaction_running: int = 0
    reaction_succeeded: int = 0
    reaction_failed: int = 0
    reaction_skipped: int = 0
    reaction_cancelled: int = 0
    delivery: ChannelCommentDeliverySummary = ChannelCommentDeliverySummary()
    legacy_runs_with_incomplete_delivery: int = 0


@dataclass(frozen=True, slots=True)
class ChannelCommentReactionSummary:
    """Reaction task state shown beside one concrete auto-comment result."""

    queued: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    cancelled: int = 0


class ChannelCommentService:
    """Durable monitor and sender for operator-approved channel discussion threads."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: ChannelCommentGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        worker_id: str | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._worker_id = worker_id or f"comments-{uuid4().hex[:10]}"
        self._stop_event = asyncio.Event()
        self._running = False

    async def create_rule(
        self,
        *,
        name: str,
        channel_ref: str,
        mode: ChannelCommentMode,
        primary_account_id: int,
        primary_text: str,
        initial_delay_seconds: int,
        primary_html: str | None = None,
        primary_media_path: str | None = None,
        primary_media_kind: str | None = None,
        primary_media_name: str | None = None,
        primary_media_size: int | None = None,
        dialogue_steps: list[DialogueStep] | None = None,
        message_edit_initial_text: str | None = None,
        message_edit_initial_html: str | None = None,
        message_edit_delay_seconds: int | None = None,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        """Create one rule, retaining the public single-channel API."""
        rules = await self.create_rules(
            name=name,
            channel_refs=(channel_ref,),
            mode=mode,
            primary_account_id=primary_account_id,
            primary_text=primary_text,
            primary_html=primary_html,
            initial_delay_seconds=initial_delay_seconds,
            primary_media_path=primary_media_path,
            primary_media_kind=primary_media_kind,
            primary_media_name=primary_media_name,
            primary_media_size=primary_media_size,
            dialogue_steps=dialogue_steps,
            message_edit_initial_text=message_edit_initial_text,
            message_edit_initial_html=message_edit_initial_html,
            message_edit_delay_seconds=message_edit_delay_seconds,
            actor=actor,
        )
        return rules[0]

    async def create_rules(
        self,
        *,
        name: str,
        channel_refs: Sequence[str],
        mode: ChannelCommentMode,
        primary_account_id: int,
        primary_text: str,
        initial_delay_seconds: int,
        primary_html: str | None = None,
        primary_media_path: str | None = None,
        primary_media_kind: str | None = None,
        primary_media_name: str | None = None,
        primary_media_size: int | None = None,
        dialogue_steps: list[DialogueStep] | None = None,
        message_edit_initial_text: str | None = None,
        message_edit_initial_html: str | None = None,
        message_edit_delay_seconds: int | None = None,
        preflight_failures: list[tuple[str, str]] | None = None,
        before_create: Callable[[], Awaitable[None]] | None = None,
        actor: str = "control-bot",
    ) -> list[ChannelCommentRule]:
        """Create identical comment rules for several channels atomically.

        A rule deliberately remains tied to one channel, so its cursor,
        discussion availability and error history never leak into another
        channel.  This batch helper shares the configured flow while creating
        one independent durable rule per selected channel.
        """
        normalized_name = self._normalize_name(name)
        raw_values: Sequence[str] = (
            (channel_refs,) if isinstance(channel_refs, str) else channel_refs
        )
        raw_refs = [str(value).strip() for value in raw_values if str(value).strip()]
        if not raw_refs:
            raise ConflictError("укажите хотя бы один публичный канал")
        if len(raw_refs) > 25:
            raise ConflictError("за один раз можно настроить не больше 25 каналов")
        normalized_refs = [self._normalize_channel_ref(value) for value in raw_refs]
        if len(set(normalized_refs)) != len(normalized_refs):
            raise ConflictError("один и тот же канал указан несколько раз")
        normalized_names = self._batch_rule_names(normalized_name, len(normalized_refs))
        if primary_media_name is not None and len(primary_media_name) > 255:
            raise ConflictError("имя медиафайла не должно быть длиннее 255 символов")
        try:
            normalized_primary_media_size = (
                int(primary_media_size) if primary_media_size is not None else None
            )
        except (TypeError, ValueError) as exc:
            raise ConflictError("размер медиафайла указан неверно") from exc
        if normalized_primary_media_size is not None and normalized_primary_media_size < 0:
            raise ConflictError("размер медиафайла указан неверно")
        primary_media_size = normalized_primary_media_size
        has_primary_media = primary_media_path is not None
        media_without_caption = has_primary_media
        normalized_text = self._normalize_text(
            primary_text,
            primary_html,
            allow_empty=media_without_caption,
            has_media=has_primary_media,
        )
        if has_primary_media:
            if mode == ChannelCommentMode.MESSAGE_EDIT:
                raise ConflictError("режим редактирования комментария поддерживает только текст")
            if primary_media_kind not in ALLOWED_MEDIA_KINDS:
                raise ConflictError("неизвестный тип медиафайла")
            if primary_media_kind == "video_note" and (normalized_text or primary_html):
                raise ConflictError("к видеокружку нельзя добавить подпись")
            try:
                resolve_media_path(self._settings, str(primary_media_path))
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
        elif any(
            value is not None
            for value in (primary_media_kind, primary_media_name, primary_media_size)
        ):
            raise ConflictError("тип медиа указан без файла")
        normalized_delay = self._normalize_delay(initial_delay_seconds)
        serialized_steps: str | None = None
        normalized_edit_initial_text: str | None = None
        normalized_edit_initial_html: str | None = None
        normalized_edit_delay: int | None = None
        if mode == ChannelCommentMode.DIALOGUE:
            if not dialogue_steps:
                raise ConflictError("для цепочки нужны минимум два сообщения")
            steps = parse_dialogue_steps(
                serialize_dialogue_steps(dialogue_steps),
                primary_account_id=primary_account_id,
                primary_text=normalized_text,
                reply_account_id=None,
                reply_text="",
                reply_delay_seconds=0,
            )
            if len(steps) < 2:
                raise ConflictError("для цепочки нужны минимум два сообщения")
            for index, step in enumerate(steps, start=1):
                if step.media_path is None:
                    continue
                try:
                    resolve_media_path(self._settings, step.media_path)
                except ContentError as exc:
                    raise ConflictError(f"медиа шага цепочки № {index}: {exc}") from exc
            if (
                steps[0].account_id != primary_account_id
                or steps[0].text != normalized_text
                or steps[0].html_text != primary_html
                or steps[0].media_path != primary_media_path
                or steps[0].media_kind != primary_media_kind
                or steps[0].media_name != primary_media_name
                or steps[0].media_size != primary_media_size
            ):
                raise ConflictError("первый шаг цепочки должен совпадать с первым комментарием")
            serialized_steps = serialize_dialogue_steps(steps)
        elif mode == ChannelCommentMode.MESSAGE_EDIT:
            if dialogue_steps:
                raise ConflictError("режим редактирования комментария не содержит цепочку")
            if message_edit_initial_text is None:
                raise ConflictError("для режима редактирования нужен исходный комментарий")
            if message_edit_delay_seconds is None:
                raise ConflictError("для режима редактирования нужна задержка")
            normalized_edit_initial_text = self._normalize_text(
                message_edit_initial_text,
                message_edit_initial_html,
            )
            normalized_edit_initial_html = message_edit_initial_html
            normalized_edit_delay = self._normalize_message_edit_delay(message_edit_delay_seconds)
        elif dialogue_steps:
            raise ConflictError("одиночный режим не содержит цепочку")
        elif (
            message_edit_initial_text is not None
            or message_edit_initial_html is not None
            or message_edit_delay_seconds is not None
        ):
            raise ConflictError("параметры редактирования доступны только в этом режиме")

        async with self._session_factory() as session:
            account = await session.get(Account, primary_account_id)
            self._validate_account(account)
            accounts = {primary_account_id: account}
            if mode == ChannelCommentMode.DIALOGUE:
                for account_id in dict.fromkeys(step.account_id for step in steps):
                    dialogue_account = await session.get(Account, account_id)
                    self._validate_account(dialogue_account)
                    accounts[account_id] = dialogue_account

        # A paused rule is history, not a lock on the channel.  A currently
        # enabled rule does protect its own channel from a duplicate flow, but
        # it must not prevent the other channels of a batch from being armed.
        async with self._session_factory() as session:
            active_rules = list(
                await session.scalars(
                    select(ChannelCommentRule).where(
                        ChannelCommentRule.archived_at.is_(None),
                        ChannelCommentRule.enabled.is_(True),
                        or_(
                            ChannelCommentRule.channel_ref.in_(normalized_refs),
                            ChannelCommentRule.name.in_(normalized_names),
                        ),
                    )
                )
            )
        active_refs = {rule.channel_ref for rule in active_rules}
        active_names = {rule.name for rule in active_rules}
        candidate_rules: list[tuple[str, str]] = []
        failures: list[tuple[str, str]] = []
        for rule_name, channel_ref in zip(normalized_names, normalized_refs, strict=True):
            if channel_ref in active_refs or rule_name in active_names:
                failures.append(
                    (
                        channel_ref,
                        "уже отслеживается активным правилом; сначала остановите его",
                    )
                )
                continue
            candidate_rules.append((rule_name, channel_ref))

        # One Telegram preflight per channel is intentional: discussion groups
        # and sender permissions may differ even when the flow is identical.
        # A bad or private channel must not prevent every other selected
        # channel from being armed.  Keep it out of this batch and report its
        # exact reason to the operator; fail only if none can be enabled.
        # A private invite can legitimately answer ``join_request_pending``.
        # The operator explicitly selected the link, so preserve a live rule
        # instead of treating approval as a configuration failure.  The scan
        # loop will re-check membership and start on its own after approval.
        ready_rules: list[tuple[str, str, ChannelCommentPreflight | None, str | None]] = []
        for rule_name, channel_ref in candidate_rules:
            if before_create is not None:
                await before_create()
            try:
                ready_rules.append(
                    (
                        rule_name,
                        channel_ref,
                        await self._preflight_accounts(list(accounts.values()), channel_ref),
                        None,
                    )
                )
            except ConflictError as exc:
                if failure_code(exc) == "join_request_pending":
                    ready_rules.append((rule_name, channel_ref, None, str(exc)))
                else:
                    failures.append((channel_ref, str(exc)))
        if preflight_failures is not None:
            preflight_failures.extend(failures)
        if not ready_rules:
            details = "; ".join(f"{channel}: {reason}" for channel, reason in failures)
            raise ConflictError(
                "ни один канал не прошёл проверку" + (f": {details}" if details else "")
            )
        async with self._session_factory() as session:
            # Repeat the active-rule check in the write transaction: another
            # operator could have enabled or created a rule while preflight was
            # running.  Disabled predecessors are archived atomically so the
            # partial unique indexes release their channel/name before insert.
            overlapping_rules = list(
                await session.scalars(
                    select(ChannelCommentRule).where(
                        ChannelCommentRule.archived_at.is_(None),
                        or_(
                            ChannelCommentRule.channel_ref.in_(
                                [channel_ref for _, channel_ref, _, _ in ready_rules]
                            ),
                            ChannelCommentRule.name.in_(
                                [rule_name for rule_name, _, _, _ in ready_rules]
                            ),
                        ),
                    )
                )
            )
            raced_active_rules = [rule for rule in overlapping_rules if rule.enabled]
            if raced_active_rules:
                raced_refs = {rule.channel_ref for rule in raced_active_rules}
                raced_names = {rule.name for rule in raced_active_rules}
                retained_rules: list[
                    tuple[str, str, ChannelCommentPreflight | None, str | None]
                ] = []
                raced_failures: list[tuple[str, str]] = []
                for prepared_rule in ready_rules:
                    rule_name, channel_ref, _, _ = prepared_rule
                    if channel_ref in raced_refs or rule_name in raced_names:
                        raced_failures.append(
                            (
                                channel_ref,
                                (
                                    "стало активно отслеживаться во время проверки; "
                                    "повторите при необходимости"
                                ),
                            )
                        )
                    else:
                        retained_rules.append(prepared_rule)
                ready_rules = retained_rules
                if preflight_failures is not None:
                    preflight_failures.extend(raced_failures)
                if not ready_rules:
                    raise ConflictError("ни один канал не доступен для нового запуска")
                overlapping_rules = [rule for rule in overlapping_rules if not rule.enabled]

            retired_at = utc_now()
            if before_create is not None:
                await before_create()
            for predecessor in overlapping_rules:
                predecessor.enabled = False
                predecessor.archived_at = retired_at
                queued_ids = list(
                    await session.scalars(
                        select(ChannelCommentRun.id).where(
                            ChannelCommentRun.rule_id == predecessor.id,
                            ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                        )
                    )
                )
                await session.execute(
                    update(ChannelCommentRun)
                    .where(
                        ChannelCommentRun.rule_id == predecessor.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    )
                    .values(
                        status=ChannelCommentRunStatus.CANCELLED,
                        finished_at=retired_at,
                        error="правило заменено новым запуском",
                        error_code="rule_replaced",
                    )
                )
                await session.execute(
                    update(ChannelCommentRun)
                    .where(
                        ChannelCommentRun.rule_id == predecessor.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    )
                    .values(cancel_requested=True)
                )
                if queued_ids:
                    await self._release_mention_reservations(session, run_ids=queued_ids)
                await self._cancel_rule_reactions(
                    session,
                    int(predecessor.id),
                    reason="channel comment rule replaced",
                    code="rule_replaced",
                )
                add_audit_event(
                    session,
                    event_type="channel_comment_rule.replaced",
                    entity_type="channel_comment_rule",
                    entity_id=predecessor.id,
                    details={"channel_ref": predecessor.channel_ref},
                    actor=actor,
                )
            # Flush updates first: SQLite/PostgreSQL partial unique indexes
            # must see the predecessors archived before inserting replacements.
            await session.flush()
            rules = [
                ChannelCommentRule(
                    name=rule_name,
                    channel_ref=channel_ref,
                    mode=mode,
                    primary_account_id=primary_account_id,
                    primary_text=normalized_text,
                    primary_html=primary_html,
                    primary_media_path=primary_media_path,
                    primary_media_kind=primary_media_kind,
                    primary_media_name=(primary_media_name[:255] if primary_media_name else None),
                    primary_media_size=(
                        int(primary_media_size) if primary_media_size is not None else None
                    ),
                    dialogue_steps_json=serialized_steps,
                    message_edit_initial_text=normalized_edit_initial_text,
                    message_edit_initial_html=normalized_edit_initial_html,
                    message_edit_delay_seconds=normalized_edit_delay,
                    mention_cooldown_days=(self._settings.channel_comments_mention_cooldown_days),
                    initial_delay_seconds=normalized_delay,
                    approved_by_operator=True,
                    enabled=True,
                    armed_at=utc_now(),
                    channel_title=preflight.title if preflight is not None else None,
                )
                for rule_name, channel_ref, preflight, pending_error in ready_rules
            ]
            for rule, (_, _, _, pending_error) in zip(rules, ready_rules, strict=True):
                if pending_error:
                    rule.last_error = pending_error[:500]
                    rule.last_error_code = "join_request_pending"
                    rule.last_error_account_id = primary_account_id
            session.add_all(rules)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("канал или название уже добавлены") from exc
            for rule in rules:
                add_audit_event(
                    session,
                    event_type="channel_comment_rule.created",
                    entity_type="channel_comment_rule",
                    entity_id=rule.id,
                    details={
                        "channel_ref": rule.channel_ref,
                        "mode": mode.value,
                        "initial_delay_seconds": normalized_delay,
                        "message_edit_delay_seconds": normalized_edit_delay,
                        "mention_cooldown_days": rule.mention_cooldown_days,
                        "batch_size": len(rules),
                    },
                    actor=actor,
                )
            await session.commit()
            for rule in rules:
                await session.refresh(rule)
            return rules

    async def list_rules(
        self,
        page: int,
        page_size: int,
    ) -> tuple[list[ChannelCommentRuleView], int]:
        normalized_page = max(0, page)
        normalized_size = max(1, min(page_size, 50))
        async with self._session_factory() as session:
            rules = list(
                await session.scalars(
                    select(ChannelCommentRule)
                    .where(ChannelCommentRule.archived_at.is_(None))
                    .order_by(ChannelCommentRule.enabled.desc(), ChannelCommentRule.id.desc())
                    .offset(normalized_page * normalized_size)
                    .limit(normalized_size)
                )
            )
            total = int(
                await session.scalar(
                    select(func.count())
                    .select_from(ChannelCommentRule)
                    .where(ChannelCommentRule.archived_at.is_(None))
                )
                or 0
            )
            views = await self._views_for_rules(session, rules)
        return views, total

    async def get_rule_view(self, rule_id: int) -> ChannelCommentRuleView:
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            return await self._rule_view(session, rule)

    async def recent_runs(self, rule_id: int, limit: int = 8) -> list[ChannelCommentRun]:
        async with self._session_factory() as session:
            return list(
                await session.scalars(
                    select(ChannelCommentRun)
                    .where(ChannelCommentRun.rule_id == rule_id)
                    .order_by(ChannelCommentRun.id.desc())
                    .limit(max(1, min(limit, 30)))
                )
            )

    async def recent_run_delivery_summaries(
        self, run_ids: list[int]
    ) -> dict[int, ChannelCommentDeliverySummary]:
        async with self._session_factory() as session:
            return await self._delivery_summaries(session, run_ids)

    @staticmethod
    async def _delivery_summaries(
        session: AsyncSession, ids: list[int], *, by_rule: bool = False
    ) -> dict[int, ChannelCommentDeliverySummary]:
        normalized_ids = list(dict.fromkeys(value for value in ids if value > 0))
        if not normalized_ids:
            return {}
        group = ChannelCommentRun.rule_id if by_rule else ChannelCommentMessage.run_id
        result = await session.execute(
            select(
                group,
                ChannelCommentMessage.status,
                func.count(ChannelCommentMessage.id),
                func.count(ChannelCommentMessage.next_check_at),
                func.max(ChannelCommentMessage.checked_at),
                func.min(ChannelCommentMessage.next_check_at),
                func.min(ChannelCommentMessage.step_index),
            )
            .select_from(ChannelCommentMessage)
            .join(ChannelCommentRun, ChannelCommentRun.id == ChannelCommentMessage.run_id)
            .where(group.in_(normalized_ids))
            .group_by(group, ChannelCommentMessage.status)
        )
        states: dict[int, dict[str, int]] = {value: {} for value in normalized_ids}
        scheduled: dict[int, int] = dict.fromkeys(normalized_ids, 0)
        checked: dict[int, list[datetime]] = {value: [] for value in normalized_ids}
        due: dict[int, list[datetime]] = {value: [] for value in normalized_ids}
        first_steps: dict[int, list[int]] = {value: [] for value in normalized_ids}
        for value, status, count, next_count, checked_at, next_at, first_step in result:
            value = int(value)
            states[value][status] = int(count)
            scheduled[value] += int(next_count)
            first_steps[value].append(int(first_step))
            if checked_at is not None:
                checked[value].append(ensure_utc(checked_at))
            if next_at is not None:
                due[value].append(ensure_utc(next_at))
        return {
            value: ChannelCommentDeliverySummary(
                confirmed=sum(counts.values()) - counts.get("simulated", 0),
                simulated=counts.get("simulated", 0),
                present=counts.get("present", 0),
                missing=counts.get("missing", 0),
                unavailable=counts.get("unavailable", 0),
                pending=counts.get("pending", 0),
                scheduled=scheduled[value],
                checked_at=max(checked[value], default=None),
                next_check_at=min(due[value], default=None),
                history_incomplete=min(first_steps[value], default=0) > 0,
            )
            for value, counts in states.items()
        }

    async def recent_run_reaction_summaries(
        self,
        run_ids: list[int],
    ) -> dict[int, ChannelCommentReactionSummary]:
        """Return every reaction outcome for the runs currently visible in a report."""
        normalized_ids = list(dict.fromkeys(run_id for run_id in run_ids if run_id > 0))
        if not normalized_ids:
            return {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    ReactionTask.channel_comment_run_id,
                    ReactionTask.status,
                    func.count(ReactionTask.id),
                )
                .where(ReactionTask.channel_comment_run_id.in_(normalized_ids))
                .group_by(ReactionTask.channel_comment_run_id, ReactionTask.status)
            )
        counts: dict[int, dict[ReactionTaskStatus, int]] = {run_id: {} for run_id in normalized_ids}
        for run_id, status, count in result:
            if run_id is not None:
                counts[int(run_id)][status] = int(count)
        return {
            run_id: ChannelCommentReactionSummary(
                queued=states.get(ReactionTaskStatus.QUEUED, 0),
                running=states.get(ReactionTaskStatus.RUNNING, 0),
                succeeded=states.get(ReactionTaskStatus.SUCCEEDED, 0),
                failed=states.get(ReactionTaskStatus.FAILED, 0),
                skipped=states.get(ReactionTaskStatus.SKIPPED, 0),
                cancelled=states.get(ReactionTaskStatus.CANCELLED, 0),
            )
            for run_id, states in counts.items()
        }

    async def set_rule_enabled(
        self,
        rule_id: int,
        enabled: bool,
        *,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        if enabled:
            # A manually resumed rule must prove that every fixed account can
            # still reach and write to the linked discussion.  This prevents a
            # stale ban from being cleared by merely pressing "enable".
            await self.preflight_rule(rule_id)
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            rule.enabled = enabled
            rule.last_error = None
            rule.last_error_code = None
            rule.last_error_account_id = None
            if enabled:
                # Resume from this moment: posts published while paused are never backfilled.
                rule.armed_at = utc_now()
            else:
                await session.execute(
                    update(ChannelCommentRun)
                    .where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    )
                    .values(cancel_requested=True)
                )
                queued_ids = list(
                    await session.scalars(
                        select(ChannelCommentRun.id).where(
                            ChannelCommentRun.rule_id == rule.id,
                            ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                        )
                    )
                )
                await session.execute(
                    update(ChannelCommentRun)
                    .where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    )
                    .values(
                        status=ChannelCommentRunStatus.CANCELLED,
                        error_code="rule_paused",
                        finished_at=utc_now(),
                    )
                )
                if queued_ids:
                    await self._release_mention_reservations(session, run_ids=queued_ids)
                await self._cancel_rule_reactions(
                    session,
                    int(rule.id),
                    reason="channel comment rule paused",
                    code="rule_paused",
                )
            add_audit_event(
                session,
                event_type=(
                    "channel_comment_rule.enabled" if enabled else "channel_comment_rule.paused"
                ),
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={"enabled": enabled},
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def set_auto_replace_primary_account(
        self,
        rule_id: int,
        enabled: bool,
        *,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        """Opt in or out of replacing the first sender after a route failure."""
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            rule.auto_replace_primary_account = bool(enabled)
            add_audit_event(
                session,
                event_type="channel_comment_rule.auto_replace_primary_account_changed",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={"enabled": rule.auto_replace_primary_account},
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def set_rule_hidden_mentions_enabled(
        self,
        rule_id: int,
        enabled: bool,
        *,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        """Change mention behaviour for posts discovered after this update."""

        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            rule.hidden_mentions_enabled = bool(enabled)
            add_audit_event(
                session,
                event_type="channel_comment_rule.hidden_mentions_changed",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={"enabled": rule.hidden_mentions_enabled},
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def set_rule_mention_cooldown_days(
        self,
        rule_id: int,
        days: int,
        *,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        normalized_days = self._normalize_mention_cooldown_days(days)
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            previous_days = int(rule.mention_cooldown_days)
            rule.mention_cooldown_days = normalized_days
            add_audit_event(
                session,
                event_type="channel_comment_rule.mention_cooldown_changed",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={"previous_days": previous_days, "days": normalized_days},
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def update_rule_reactions(
        self,
        rule_id: int,
        *,
        enabled: bool | None = None,
        count: int | None = None,
        emoji: str | None = None,
        dialogue_step: int | None = None,
        account_ids: list[int] | None = None,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        """Update future comment reactions without probing the comment channel.

        A reaction is attempted only after its source comment was delivered.
        Requiring a live channel preflight here made a harmless settings change
        fail for an old, private or temporarily unavailable channel.
        """

        if count is not None and not 0 <= count <= 10:
            raise ConflictError("количество реакций должно быть от 0 до 10")
        if emoji is not None and emoji not in ORDINARY_REACTION_EMOJI:
            raise ConflictError("выберите реакцию из списка")
        normalized_ids: list[int] | None = None
        if account_ids is not None:
            normalized_ids = []
            for value in account_ids:
                if isinstance(value, bool):
                    continue
                try:
                    account_id = int(value)
                except (TypeError, ValueError):
                    continue
                if account_id > 0 and account_id not in normalized_ids:
                    normalized_ids.append(account_id)
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            steps = self._rule_steps(rule)
            max_step = 1 if rule.mode != ChannelCommentMode.DIALOGUE else len(steps)
            effective_step = int(dialogue_step or rule.reaction_dialogue_step)
            if not 1 <= effective_step <= max_step:
                raise ConflictError("такого комментария в цепочке нет")
            current_ids = self._reaction_account_ids(rule)
            effective_ids = normalized_ids if normalized_ids is not None else current_ids
            effective_count = int(count if count is not None else rule.reaction_count)
            effective_enabled = bool(enabled if enabled is not None else rule.reactions_enabled)
            if count == 0:
                effective_enabled = False
            elif effective_enabled and effective_count == 0:
                effective_count = 1
            source_account_id = int(steps[effective_step - 1].account_id)
            if effective_enabled and not any(
                account_id != source_account_id for account_id in effective_ids
            ):
                raise ConflictError(
                    "для реакций выберите хотя бы один аккаунт, отличный от автора комментария"
                )
            for account_id in effective_ids:
                account = await session.get(Account, account_id)
                self._validate_account(account)
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            rule.reactions_enabled = effective_enabled
            rule.reaction_count = effective_count
            rule.reaction_dialogue_step = effective_step
            if emoji is not None:
                rule.reaction_emoji = emoji
            if normalized_ids is not None:
                rule.reaction_account_ids_json = json.dumps(normalized_ids)
            if effective_ids:
                await session.execute(
                    ChannelCommentReactionAccountState.__table__.delete().where(
                        ChannelCommentReactionAccountState.rule_id == rule.id,
                        ChannelCommentReactionAccountState.account_id.in_(effective_ids),
                    )
                )
            if not effective_enabled or effective_count < 1:
                await self._cancel_rule_reactions(
                    session,
                    int(rule.id),
                    reason="comment reactions disabled by operator",
                    code="reactions_disabled",
                )
            add_audit_event(
                session,
                event_type="channel_comment_rule.reactions_changed",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={
                    "enabled": rule.reactions_enabled,
                    "count": rule.reaction_count,
                    "emoji": rule.reaction_emoji,
                    "dialogue_step": rule.reaction_dialogue_step,
                    "account_ids": self._reaction_account_ids(rule),
                },
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def replace_primary_account(
        self,
        rule_id: int,
        account_id: int,
        *,
        actor: str = "control-bot",
    ) -> ChannelCommentRule:
        """Replace the first sender without allowing old queued posts to change meaning."""
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            account = await session.get(Account, account_id)
            self._validate_account(account)
            previous_account_id = rule.primary_account_id
            if account_id == previous_account_id:
                return rule
            if rule.mode == ChannelCommentMode.MESSAGE_EDIT:
                pending_edits = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(ChannelCommentRun)
                        .where(
                            ChannelCommentRun.rule_id == rule.id,
                            (
                                (ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING)
                                | (
                                    (ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED)
                                    & (ChannelCommentRun.sent_steps > 0)
                                )
                            ),
                        )
                    )
                    or 0
                )
                if pending_edits:
                    raise ConflictError(
                        "нельзя сменить аккаунт, пока ожидается редактирование "
                        "уже отправленного комментария"
                    )
            if rule.mode == ChannelCommentMode.DIALOGUE:
                started_dialogues = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(ChannelCommentRun)
                        .where(
                            ChannelCommentRun.rule_id == rule.id,
                            (
                                (ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING)
                                | (
                                    (ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED)
                                    & (ChannelCommentRun.sent_steps > 0)
                                )
                            ),
                        )
                    )
                    or 0
                )
                if started_dialogues:
                    raise ConflictError(
                        "нельзя сменить первого аккаунта, пока выполняется цепочка комментариев"
                    )
                steps = self._rule_steps(rule)
                steps[0] = replace(steps[0], account_id=account_id)
                try:
                    validated_steps = parse_dialogue_steps(
                        serialize_dialogue_steps(steps),
                        primary_account_id=account_id,
                        primary_text=rule.primary_text,
                        reply_account_id=None,
                        reply_text="",
                        reply_delay_seconds=0,
                    )
                except ValueError as exc:
                    raise ConflictError("новый первый аккаунт конфликтует с цепочкой") from exc
                rule.dialogue_steps_json = serialize_dialogue_steps(validated_steps)
            rule.primary_account_id = account_id
            # A queued flow is a snapshot of the former account setup. Cancel it
            # rather than silently changing who will post under an old channel post.
            queued_ids = list(
                await session.scalars(
                    select(ChannelCommentRun.id).where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                        ChannelCommentRun.sent_steps == 0,
                    )
                )
            )
            await session.execute(
                update(ChannelCommentRun)
                .where(
                    ChannelCommentRun.rule_id == rule.id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.sent_steps == 0,
                )
                .values(
                    status=ChannelCommentRunStatus.CANCELLED,
                    error_code="primary_account_replaced",
                    finished_at=utc_now(),
                )
            )
            if queued_ids:
                await self._release_mention_reservations(session, run_ids=queued_ids)
            await self._cancel_rule_reactions(
                session,
                int(rule.id),
                reason="channel comment rule archived",
                code="rule_archived",
            )
            rule.armed_at = utc_now()
            rule.last_error = None
            rule.last_error_code = None
            rule.last_error_account_id = None
            add_audit_event(
                session,
                event_type="channel_comment_rule.primary_account_replaced",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={
                    "previous_account_id": previous_account_id,
                    "account_id": account_id,
                },
                actor=actor,
            )
            await session.commit()
            await session.refresh(rule)
            return rule

    async def delete_rule(self, rule_id: int, *, actor: str = "control-bot") -> None:
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            now = utc_now()
            queued_ids = list(
                await session.scalars(
                    select(ChannelCommentRun.id).where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    )
                )
            )
            running_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(ChannelCommentRun)
                    .where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    )
                )
                or 0
            )
            rule.enabled = False
            rule.archived_at = now
            await session.execute(
                update(ChannelCommentRun)
                .where(
                    ChannelCommentRun.rule_id == rule.id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                )
                .values(
                    status=ChannelCommentRunStatus.CANCELLED,
                    finished_at=now,
                    error="правило архивировано до отправки комментария",
                    error_code="rule_archived",
                )
            )
            await session.execute(
                update(ChannelCommentRun)
                .where(
                    ChannelCommentRun.rule_id == rule.id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                )
                .values(cancel_requested=True)
            )
            if queued_ids:
                await self._release_mention_reservations(session, run_ids=queued_ids)
            add_audit_event(
                session,
                event_type="channel_comment_rule.archived",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={
                    "channel_ref": rule.channel_ref,
                    "cancelled_queued": len(queued_ids),
                    "running_at_archive": running_count,
                },
                actor=actor,
            )
            await session.commit()

    async def preflight_rule(self, rule_id: int) -> ChannelCommentPreflight | None:
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                raise NotFoundError("настройка комментариев не найдена")
            steps = self._rule_steps(rule)
            account_ids = list(dict.fromkeys(step.account_id for step in steps))
            accounts: list[Account] = []
            checked_account_id = rule.primary_account_id
            try:
                for account_id in account_ids:
                    checked_account_id = account_id
                    account = await session.get(Account, account_id)
                    self._validate_account(account)
                    accounts.append(account)
            except ConflictError as exc:
                self._store_rule_error(rule, exc, account_id=checked_account_id)
                if failure_code(exc) in CHANNEL_COMMENT_AUTO_PAUSE_CODES:
                    await self._auto_pause_rule(
                        session,
                        rule,
                        exc,
                        account_id=checked_account_id,
                    )
                await session.commit()
                raise
            channel_ref = rule.channel_ref
        try:
            result = await self._preflight_accounts(accounts, channel_ref)
        except ConflictError as exc:
            async with self._session_factory() as session:
                stored = await self._active_rule(session, rule_id)
                if stored is not None:
                    self._store_rule_error(stored, exc, account_id=stored.primary_account_id)
                    if (
                        failure_code(exc) in CHANNEL_COMMENT_AUTO_PAUSE_CODES
                        and failure_code(exc) != "join_request_pending"
                    ):
                        await self._auto_pause_rule(
                            session,
                            stored,
                            exc,
                            account_id=stored.primary_account_id,
                        )
                    await session.commit()
            raise
        async with self._session_factory() as session:
            stored = await self._active_rule(session, rule_id)
            if stored is not None:
                if result is not None and result.title:
                    stored.channel_title = result.title[:255]
                stored.last_error = None
                stored.last_error_code = None
                stored.last_error_account_id = None
                await session.commit()
        return result

    async def scan_due_rules(self) -> int:
        now = utc_now()
        cutoff = now - timedelta(seconds=self._settings.channel_comments_scan_interval_seconds)
        async with self._session_factory() as session:
            rule_ids = list(
                await session.scalars(
                    select(ChannelCommentRule.id)
                    .where(
                        ChannelCommentRule.enabled.is_(True),
                        ChannelCommentRule.approved_by_operator.is_(True),
                        ChannelCommentRule.archived_at.is_(None),
                        (
                            ChannelCommentRule.last_scanned_at.is_(None)
                            | (ChannelCommentRule.last_scanned_at <= cutoff)
                        ),
                    )
                    .order_by(ChannelCommentRule.last_scanned_at, ChannelCommentRule.id)
                )
            )
        semaphore = asyncio.Semaphore(self._settings.channel_comments_scan_concurrency)

        async def scan_one(rule_id: int) -> None:
            async with semaphore:
                try:
                    await self.scan_rule(rule_id)
                except Exception:  # noqa: BLE001 - isolate one rule from all others
                    logger.exception("Channel-comment scan failed for rule %s", rule_id)

        await asyncio.gather(*(scan_one(rule_id) for rule_id in rule_ids))
        return len(rule_ids)

    async def scan_rule(self, rule_id: int) -> None:
        validation_error: ConflictError | None = None
        account: Account | None = None
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None or not rule.enabled or not rule.approved_by_operator:
                return
            account = await session.get(Account, rule.primary_account_id)
            if account is not None and (
                (cooldown := ensure_utc(account.cooldown_until)) is not None
                and cooldown > utc_now()
            ):
                return
            try:
                self._validate_account(account)
            except ConflictError as exc:
                validation_error = exc
            else:
                channel_ref = rule.channel_ref
                after_message_id = int(rule.last_seen_post_id or 0)
                bootstrap = rule.last_seen_post_id is None
                armed_at = ensure_utc(rule.armed_at)

        if validation_error is not None:
            await self._record_scan_failure(rule_id, validation_error)
            return
        assert account is not None

        try:
            async with asyncio.timeout(self._settings.channel_comments_scan_timeout_seconds):
                scan = await self._gateway.scan_channel_posts(
                    account=account,
                    channel_ref=channel_ref,
                    after_message_id=after_message_id,
                    limit=self._settings.channel_comments_scan_limit,
                    bootstrap=bootstrap,
                    after_date=armed_at if bootstrap else None,
                )
        except TimeoutError:
            await self._record_scan_failure(
                rule_id,
                TemporaryGatewayError(
                    "проверка канала не завершилась вовремя",
                    code="channel_scan_timeout",
                    retry_after=self._settings.channel_comments_scan_interval_seconds,
                ),
            )
            return
        except Exception as exc:  # noqa: BLE001 - persisted below without secret details
            await self._record_scan_failure(rule_id, exc)
            return
        await self._store_scan(rule_id, scan)

    async def process_next(self) -> bool:
        await self._recover_expired_leases()
        claimed = await self._claim_next()
        if claimed is None:
            return False
        run_id, lease_token = claimed
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat(run_id, lease_token, heartbeat_stop),
            name=f"channel-comment-heartbeat-{run_id}",
        )
        try:
            async with asyncio.timeout(self._settings.worker_step_timeout_seconds):
                await self._process_claimed(run_id, lease_token)
        except TimeoutError:
            logger.warning("Channel-comment run %s timed out", run_id)
            await self._handle_delivery_error(
                run_id,
                lease_token,
                0,
                TemporaryGatewayError(
                    "channel-comment operation timed out",
                    code="comment_timeout",
                    retry_after=self._settings.worker_retry_base_seconds,
                ),
            )
        finally:
            heartbeat_stop.set()
            await heartbeat_task
        return True

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        self._running = True
        scan_task: asyncio.Task[int] | None = None
        try:
            while not self._stop_event.is_set():
                processed = False
                try:
                    if scan_task is None or scan_task.done():
                        if scan_task is not None:
                            completed_scan = scan_task
                            scan_task = None
                            completed_scan.result()
                        scan_task = asyncio.create_task(
                            self.scan_due_rules(), name="channel-comment-scans"
                        )
                    for _ in range(10):
                        if self._stop_event.is_set():
                            break
                        if not await self.process_next():
                            break
                        processed = True
                except OperationalError as exc:
                    if is_transient_sqlite_lock(exc):
                        # A concurrent import, UI action or another durable
                        # worker briefly owns SQLite's single write slot.  This
                        # is recoverable; retry quietly instead of reporting a
                        # worker crash and leaving the interface looking broken.
                        logger.warning("Channel-comment worker waits for a busy SQLite writer")
                        await asyncio.sleep(0.5)
                    else:
                        logger.exception("Channel-comment worker iteration failed")
                        if reporter is not None:
                            await reporter(exc)
                except Exception as exc:  # noqa: BLE001 - a bad rule must not stop all rules
                    logger.exception("Channel-comment worker iteration failed")
                    if reporter is not None:
                        await reporter(exc)
                else:
                    if reporter is not None:
                        await reporter()
                if processed:
                    continue
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._settings.channel_comments_poll_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            if scan_task is not None:
                scan_task.cancel()
                with suppress(asyncio.CancelledError):
                    await scan_task
            self._running = False

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def running(self) -> bool:
        return self._running

    async def _store_scan(self, rule_id: int, scan: ChannelPostScan) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None or not rule.enabled or not rule.approved_by_operator:
                return
            if scan.title:
                rule.channel_title = scan.title[:255]
            highest_seen = int(rule.last_seen_post_id or 0)
            previous_grouped_id = rule.last_seen_grouped_id
            armed_at = ensure_utc(rule.armed_at) or now
            max_age = timedelta(seconds=self._settings.channel_comments_max_post_age_seconds)
            flow_delay_seconds = rule.initial_delay_seconds + sum(
                step.delay_seconds for step in self._rule_steps(rule)[1:]
            )
            post_ids = [post.message_id for post in scan.posts]
            existing_post_ids = set(
                await session.scalars(
                    select(ChannelCommentRun.source_post_id).where(
                        ChannelCommentRun.rule_id == rule.id,
                        ChannelCommentRun.source_post_id.in_(post_ids),
                    )
                )
            )
            for post in scan.posts:
                highest_seen = max(highest_seen, post.message_id)
                if post.grouped_id is not None and post.grouped_id == previous_grouped_id:
                    continue
                published_at = ensure_utc(post.published_at) or now
                if published_at <= armed_at:
                    continue
                if post.message_id in existing_post_ids:
                    continue
                age = now - published_at
                status = (
                    ChannelCommentRunStatus.SKIPPED
                    if age > max_age
                    else ChannelCommentRunStatus.QUEUED
                )
                run = ChannelCommentRun(
                    rule_id=rule.id,
                    source_post_id=post.message_id,
                    source_post_at=published_at,
                    status=status,
                    hidden_mentions_enabled_snapshot=rule.hidden_mentions_enabled,
                    mention_cooldown_days_snapshot=rule.mention_cooldown_days,
                    scheduled_for=max(
                        now,
                        published_at + timedelta(seconds=rule.initial_delay_seconds),
                    ),
                    expires_at=(published_at + timedelta(seconds=flow_delay_seconds) + max_age),
                    max_attempts=self._settings.worker_max_attempts,
                    error_code=(
                        "post_too_old" if status == ChannelCommentRunStatus.SKIPPED else None
                    ),
                    error=(
                        "пост старше лимита ожидания; комментарий не догоняется"
                        if status == ChannelCommentRunStatus.SKIPPED
                        else None
                    ),
                    finished_at=now if status == ChannelCommentRunStatus.SKIPPED else None,
                )
                session.add(run)
            if scan.last_seen_message_id is not None and scan.last_seen_message_id >= highest_seen:
                highest_seen = scan.last_seen_message_id
                rule.last_seen_grouped_id = scan.last_seen_grouped_id
            elif scan.posts and highest_seen > int(rule.last_seen_post_id or 0):
                rule.last_seen_grouped_id = max(
                    scan.posts, key=lambda post: post.message_id
                ).grouped_id
            rule.last_seen_post_id = highest_seen or rule.last_seen_post_id
            rule.last_scanned_at = now
            rule.last_error = None
            rule.last_error_code = None
            rule.last_error_account_id = None
            try:
                await session.commit()
            except IntegrityError:
                # Two processes may notice the same post together (for example,
                # bot and API workers during a rolling restart). The unique
                # rule/post constraint makes the duplicate harmless.
                await session.rollback()

    async def _record_scan_failure(self, rule_id: int, exc: BaseException) -> None:
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None:
                return
            account_id = int(rule.primary_account_id)
        await self._runner.handle_external_account_failure(account_id, exc)
        if await self._try_auto_replace_primary_account(
            rule_id=rule_id,
            failed_account_id=account_id,
            exc=exc,
        ):
            # The replacement passed a live channel check. Scan once with it so
            # the monitor does not wait for the next interval to resume.
            await self.scan_rule(rule_id)
            return
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None or rule.primary_account_id != account_id:
                return
            self._store_rule_error(rule, exc, account_id=account_id)
            rule.last_scanned_at = utc_now()
            if (
                failure_code(exc) in CHANNEL_COMMENT_AUTO_PAUSE_CODES
                and failure_code(exc) != "join_request_pending"
            ):
                await self._auto_pause_rule(
                    session,
                    rule,
                    exc,
                    account_id=account_id,
                )
            await session.commit()

    async def _claim_next(self) -> tuple[int, str] | None:
        now = utc_now()
        async with self._session_factory() as session:
            expired_count = await self._expire_stale_runs(session, now)
            run = await session.scalar(
                select(ChannelCommentRun)
                .where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.scheduled_for <= now,
                    ChannelCommentRun.cancel_requested.is_(False),
                )
                .order_by(ChannelCommentRun.scheduled_for, ChannelCommentRun.id)
                .limit(1)
            )
            if run is None:
                if expired_count:
                    await session.commit()
                return None
            token = uuid4().hex
            claimed = await session.execute(
                update(ChannelCommentRun)
                .execution_options(synchronize_session=False)
                .where(
                    ChannelCommentRun.id == run.id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.scheduled_for <= now,
                    ChannelCommentRun.cancel_requested.is_(False),
                )
                .values(
                    status=ChannelCommentRunStatus.RUNNING,
                    lease_owner=self._worker_id,
                    lease_token=token,
                    lease_until=now + timedelta(seconds=self._settings.worker_lease_seconds),
                    started_at=run.started_at or now,
                    attempt_count=run.attempt_count + 1,
                )
            )
            if claimed.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
            return run.id, token

    async def _heartbeat(self, run_id: int, lease_token: str, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._settings.worker_heartbeat_seconds
                )
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await session.execute(
                        update(ChannelCommentRun)
                        .where(
                            ChannelCommentRun.id == run_id,
                            ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                            ChannelCommentRun.lease_owner == self._worker_id,
                            ChannelCommentRun.lease_token == lease_token,
                        )
                        .values(
                            lease_until=utc_now()
                            + timedelta(seconds=self._settings.worker_lease_seconds)
                        )
                    )
                    await session.commit()
            except Exception:  # noqa: BLE001 - expiry recovery is safer than killing the step
                logger.exception("Channel-comment heartbeat failed for %s", run_id)
                return
            if renewed.rowcount != 1:
                logger.warning("Channel-comment run %s lost its lease", run_id)
                return

    async def _process_claimed(self, run_id: int, lease_token: str) -> None:
        try:
            context = await self._load_run_context(run_id, lease_token)
        except NotFoundError:
            # The rule may have been deleted after this worker claimed its run.
            return
        except ConflictError as exc:
            try:
                await self._finish_failed(run_id, lease_token, exc, permanent=True)
            except (ConflictError, NotFoundError):
                # A newer worker or the operator already resolved this run.
                return
            return
        if context is None:
            return
        run, rule, steps, step = context
        account_id = step.account_id

        async def before_send() -> None:
            await self._mark_comment_dispatch(run_id, lease_token, account_id)

        dispatch_kwargs = (
            {"before_send": before_send}
            if getattr(self._gateway, "supports_channel_comment_dispatch_hook", False)
            else {}
        )
        if dispatch_kwargs and run.step_index > 0:
            async with self._session_factory() as session:
                original = await session.scalar(
                    select(ChannelCommentMessage)
                    .where(ChannelCommentMessage.run_id == run.id)
                    .order_by(ChannelCommentMessage.step_index)
                    .limit(1)
                )
            dispatch_kwargs.update(
                expected_discussion_peer_id=original.discussion_peer_id
                if original is not None
                else None,
                expected_thread_root_message_id=run.thread_root_message_id,
            )
        is_message_edit = rule.mode == ChannelCommentMode.MESSAGE_EDIT and run.step_index == 1
        content_key = f"channel-comment:{run.id}:step:{run.step_index}"
        idempotency_key = content_key
        if run.membership_gate_retry_count > 0:
            idempotency_key = (
                f"{content_key}:membership-gate-retry:{run.membership_gate_retry_count}"
            )

        async def deliver() -> ChannelCommentDelivery | datetime:
            async with self._session_factory() as session:
                stored = await self._require_running(session, run_id, lease_token)
                if stored.cancel_requested:
                    raise _ChannelCommentCancelled
                expires_at = ensure_utc(stored.expires_at)
                if expires_at is not None and expires_at <= utc_now():
                    raise PermanentGatewayError(
                        "срок актуальности комментария истёк до отправки",
                        code="comment_expired",
                    )
                account = await session.get(Account, account_id)
                if account is not None and not is_message_edit:
                    resume_at = self._runner.account_resume_at(account)
                    if resume_at is not None:
                        return resume_at
                self._validate_account(account)
            try:
                text = render_spintax(step.text, seed=content_key)
                html_text = (
                    render_spintax(step.html_text, seed=content_key)
                    if step.html_text is not None
                    else None
                )
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            if is_message_edit:
                if run.last_comment_message_id is None:
                    raise ConflictError("не найден комментарий для отложенного редактирования")
                if not dispatch_kwargs:
                    await before_send()
                sent = await self._gateway.edit_channel_comment(
                    account=account,
                    channel_ref=rule.channel_ref,
                    channel_post_id=run.source_post_id,
                    message_id=run.last_comment_message_id,
                    text=text,
                    html_text=html_text,
                    idempotency_key=idempotency_key,
                    **dispatch_kwargs,
                )
            else:
                reply_to = None
                if run.step_index > 0:
                    reply_to = (
                        run.last_comment_message_id
                        if step.reply_to_previous
                        else run.thread_root_message_id
                    )
                    if reply_to is None:
                        raise ConflictError("в цепочке не найден предыдущий комментарий")
                mention_ids = ()
                if run.step_index == 0 and not (step.media_path is not None and not text):
                    mention_ids = await self._resolve_channel_comment_hidden_mentions(
                        run,
                        rule,
                        account,
                        text=text,
                        selection_key=f"{content_key}:hidden-mentions",
                        message_limit=1024 if step.media_path is not None else 4096,
                    )
                html_text = append_hidden_mentions(
                    text=text,
                    html_text=html_text,
                    user_ids=mention_ids,
                    message_limit=1024 if step.media_path is not None else 4096,
                )
                if mention_ids and html_text is not None:
                    text = render_telegram_html(html_text)
                if not dispatch_kwargs:
                    await before_send()
                sent = await self._gateway.send_channel_comment(
                    account=account,
                    channel_ref=rule.channel_ref,
                    channel_post_id=run.source_post_id,
                    text=text,
                    idempotency_key=idempotency_key,
                    reply_to_message_id=reply_to,
                    html_text=html_text,
                    media_path=step.media_path,
                    media_kind=step.media_kind,
                    media_name=step.media_name,
                    **dispatch_kwargs,
                )
                try:
                    if not sent.simulated:
                        await self._runner.record_external_account_send(
                            account_id,
                            operation_key=idempotency_key,
                        )
                except Exception as exc:  # noqa: BLE001 - the comment already reached Telegram
                    raise PermanentGatewayError(
                        "Telegram принял комментарий, но учёт действия не удалось сохранить; "
                        "повтор запрещён",
                        code="delivery_unconfirmed",
                        external_message_id=sent.message_id,
                    ) from exc
            return sent

        account_failure_applied = False

        async def guarded_deliver() -> datetime | None:
            nonlocal account_failure_applied
            try:
                result = await deliver()
                if isinstance(result, datetime):
                    return result
                try:
                    await self._record_delivery(run_id, lease_token, rule, steps, result)
                except (ConflictError, NotFoundError):
                    # A concurrent stop, delete or lease recovery owns this run.
                    return None
                except Exception as exc:  # noqa: BLE001 - Telegram already returned a receipt
                    raise PermanentGatewayError(
                        "Telegram принял комментарий, но результат не удалось сохранить; "
                        "повтор запрещён",
                        code="delivery_unconfirmed",
                        external_message_id=result.message_id,
                    ) from exc
                return None
            except GatewayError as exc:
                if isinstance(exc.__cause__, GatewayError):
                    await self._runner.handle_external_account_failure(account_id, exc.__cause__)
                await self._runner.handle_external_account_failure(account_id, exc)
                account_failure_applied = True
                if exc.code == "delivery_unconfirmed":
                    await self._runner.record_external_account_send(
                        account_id,
                        operation_key=idempotency_key,
                    )
                raise

        try:
            operation = (
                self._runner.run_account_operation
                if getattr(self._gateway, "simulated", False)
                else self._runner.run_account_send_operation
            )
            result = await operation(account_id, guarded_deliver)
        except _ChannelCommentCancelled:
            await self._finish_cancelled(run_id, lease_token, "cancelled_before_send")
            return
        except AccountSendDeferred as exc:
            await self._defer_run(
                run_id,
                lease_token,
                exc.resume_at,
                reason="account_pacing",
            )
            return
        except Exception as exc:  # noqa: BLE001 - all errors become durable run state
            if isinstance(exc, GatewayError):
                logger.warning(
                    "Channel-comment run %s was handled [%s]: %s",
                    run_id,
                    exc.code or "gateway_error",
                    safe_error_message(exc),
                )
            else:
                logger.exception("Channel-comment run %s delivery failed", run_id)
            await self._handle_delivery_error(
                run_id,
                lease_token,
                account_id,
                exc,
                account_failure_applied=account_failure_applied,
            )
            return
        if isinstance(result, datetime):
            await self._defer_run(run_id, lease_token, result, reason="account_pacing")
            return

    async def _mark_comment_dispatch(self, run_id: int, lease_token: str, account_id: int) -> None:
        """Persist the last cancellation check immediately before the Telegram mutation."""
        now = utc_now()
        active_rule_ids = select(ChannelCommentRule.id).where(
            ChannelCommentRule.enabled.is_(True),
            ChannelCommentRule.approved_by_operator.is_(True),
            ChannelCommentRule.archived_at.is_(None),
        )
        async with self._session_factory() as session:
            changed = await session.execute(
                update(ChannelCommentRun)
                .where(
                    ChannelCommentRun.id == run_id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    ChannelCommentRun.lease_owner == self._worker_id,
                    ChannelCommentRun.lease_token == lease_token,
                    ChannelCommentRun.lease_until > now,
                    ChannelCommentRun.cancel_requested.is_(False),
                    ChannelCommentRun.rule_id.in_(active_rule_ids),
                    or_(ChannelCommentRun.expires_at.is_(None), ChannelCommentRun.expires_at > now),
                )
                .values(
                    dispatch_started_at=now,
                    dispatch_account_id=account_id,
                    simulated=bool(getattr(self._gateway, "simulated", False)),
                )
                .execution_options(synchronize_session=False)
            )
            await session.commit()
            if changed.rowcount != 1:
                stored = await self._require_running(session, run_id, lease_token)
                expires_at = ensure_utc(stored.expires_at)
                if (
                    not stored.cancel_requested
                    and expires_at is not None
                    and expires_at <= utc_now()
                ):
                    raise PermanentGatewayError(
                        "Срок актуальности комментария истёк", code="comment_expired"
                    )
                raise _ChannelCommentCancelled

    async def _load_run_context(
        self,
        run_id: int,
        lease_token: str,
    ) -> tuple[ChannelCommentRun, ChannelCommentRule, list[DialogueStep], DialogueStep] | None:
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            rule = await session.get(ChannelCommentRule, run.rule_id)
            if rule is None:
                raise NotFoundError("настройка канала удалена")
            if run.cancel_requested:
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = utc_now()
                run.error = "cancel requested before comment delivery started"
                run.error_code = "cancelled_before_send"
                await self._release_mention_reservations(session, run_ids=[run.id])
                self._release_lease(run)
                await session.commit()
                return None
            if run.sent_steps > 0 and run.simulated != bool(
                getattr(self._gateway, "simulated", False)
            ):
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = utc_now()
                run.error = (
                    "Режим отправки изменился; начатая обработка не смешивает тест и Telegram"
                )
                run.error_code = "comment_execution_mode_changed"
                await self._release_mention_reservations(session, run_ids=[run.id])
                self._release_lease(run)
                await session.commit()
                return None
            expires_at = ensure_utc(run.expires_at)
            if expires_at is not None and expires_at <= utc_now():
                run.status = (
                    ChannelCommentRunStatus.FAILED
                    if run.sent_steps > 0
                    else ChannelCommentRunStatus.SKIPPED
                )
                run.finished_at = utc_now()
                run.error = "срок актуальности комментария истёк"
                run.error_code = "comment_expired"
                await self._release_mention_reservations(session, run_ids=[run.id])
                self._release_lease(run)
                await session.commit()
                return None
            elif not rule.enabled or not rule.approved_by_operator:
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = utc_now()
                run.error = "правило остановлено до отправки комментария"
                run.error_code = "rule_archived" if rule.archived_at is not None else "rule_paused"
                await self._release_mention_reservations(session, run_ids=[run.id])
                self._release_lease(run)
                await session.commit()
                return None
            steps = self._rule_steps(rule)
            if run.step_index >= len(steps):
                raise ConflictError("у комментария не осталось шагов")
            return run, rule, steps, steps[run.step_index]

    async def _resolve_channel_comment_hidden_mentions(
        self,
        run: ChannelCommentRun,
        rule: ChannelCommentRule,
        account: Account,
        *,
        text: str,
        selection_key: str,
        message_limit: int = 4096,
    ) -> tuple[int, ...]:
        # Existing queued rows from before this feature have no snapshot and
        # must not gain mentions when an operator enables the rule later.
        if run.hidden_mentions_enabled_snapshot is not True:
            return ()
        async with self._session_factory() as session:
            excluded_user_ids = await load_mention_exclusion_user_ids(session)
        maximum_count = min(
            HIDDEN_MENTION_MAXIMUM, hidden_mention_capacity(text, message_limit=message_limit)
        )
        if run.hidden_mention_user_ids_snapshot_json is not None:
            parsed = self._parse_hidden_mention_snapshot(
                run.hidden_mention_user_ids_snapshot_json,
                exclude_user_ids=excluded_user_ids,
            )
            parsed = parsed[:maximum_count]
            if len(parsed) < HIDDEN_MENTION_MINIMUM:
                parsed = ()
            checker = getattr(self._gateway, "filter_mentionable_user_ids", None)
            if checker is not None:
                try:
                    parsed = await checker(account=account, user_ids=parsed)
                    if len(parsed) < HIDDEN_MENTION_MINIMUM:
                        parsed = ()
                except Exception as exc:  # noqa: BLE001 - publish without unchecked mentions
                    if failure_code(exc) in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
                        raise
                    logger.info("Could not recheck hidden mention usernames for run %s", run.id)
                    parsed = ()
            if run.hidden_mention_resolved_count != len(parsed):
                await self._update_channel_comment_hidden_mention_snapshot(run, parsed)
            return parsed

        reason: str | None = None
        candidates: tuple[int, ...] = ()
        if maximum_count < HIDDEN_MENTION_MINIMUM:
            reason = "message_has_no_room_for_mentions"
        else:
            collector = getattr(self._gateway, "collect_channel_comment_mentionable_user_ids", None)
            if collector is None:
                reason = "gateway_does_not_support_participant_collection"
            else:
                try:
                    collected = await collector(
                        account=account,
                        channel_ref=rule.channel_ref,
                        channel_post_id=run.source_post_id,
                        limit=200,
                    )
                    candidates = normalize_user_ids(
                        collected,
                        exclude_user_ids=excluded_user_ids,
                    )
                    if len(candidates) < HIDDEN_MENTION_MINIMUM:
                        reason = "fewer_than_five_eligible_members"
                except Exception as exc:  # noqa: BLE001 - a list failure must not lose a comment
                    if failure_code(exc) in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
                        raise
                    logger.info(
                        "Hidden mentions were skipped for channel comment run %s: %s",
                        run.id,
                        safe_error_message(exc),
                    )
                    reason = "participant_collection_unavailable"
        return await self._reserve_channel_comment_hidden_mentions(
            run,
            rule,
            candidates,
            selection_key=selection_key,
            maximum_count=maximum_count,
            reason=reason,
            exclude_user_ids=excluded_user_ids,
        )

    async def _reserve_channel_comment_hidden_mentions(
        self,
        run: ChannelCommentRun,
        rule: ChannelCommentRule,
        candidates: tuple[int, ...],
        *,
        selection_key: str,
        maximum_count: int,
        reason: str | None,
        exclude_user_ids: set[int],
    ) -> tuple[int, ...]:
        for _ in range(3):
            async with self._session_factory() as session:
                stored = await self._require_running(
                    session,
                    run.id,
                    run.lease_token or "",
                )
                if stored.hidden_mention_user_ids_snapshot_json is not None:
                    parsed = self._parse_hidden_mention_snapshot(
                        stored.hidden_mention_user_ids_snapshot_json,
                        exclude_user_ids=exclude_user_ids,
                    )
                    if stored.hidden_mention_resolved_count != len(parsed):
                        stored.hidden_mention_user_ids_snapshot_json = json.dumps(list(parsed))
                        stored.hidden_mention_resolved_count = len(parsed)
                        if not parsed:
                            stored.hidden_mention_skip_reason = "excluded_user_after_resolution"
                        await session.commit()
                    return parsed

                now = utc_now()
                cooldown_days = int(
                    stored.mention_cooldown_days_snapshot
                    or rule.mention_cooldown_days
                    or self._settings.channel_comments_mention_cooldown_days
                )
                cooldown_cutoff = now - timedelta(days=cooldown_days)
                states = list(
                    await session.scalars(
                        select(ChannelCommentMentionState).where(
                            ChannelCommentMentionState.rule_id == stored.rule_id,
                            ChannelCommentMentionState.telegram_user_id.in_(candidates),
                        )
                    )
                )
                states_by_user = {int(item.telegram_user_id): item for item in states}
                eligible = tuple(
                    user_id
                    for user_id in candidates
                    if self._mention_state_is_eligible(
                        states_by_user.get(user_id),
                        run_id=stored.id,
                        now=now,
                        cooldown_cutoff=cooldown_cutoff,
                    )
                )
                selected = select_hidden_mentions(
                    eligible,
                    selection_key=selection_key,
                    maximum_count=maximum_count,
                )
                resolved_reason = reason
                if not selected and resolved_reason is None:
                    resolved_reason = (
                        "mention_cooldown_active"
                        if len(candidates) >= HIDDEN_MENTION_MINIMUM
                        else "fewer_than_five_eligible_members"
                    )

                reservation_conflict = False
                reserve_until = ensure_utc(stored.expires_at) or now + timedelta(
                    seconds=max(
                        self._settings.channel_comments_max_post_age_seconds,
                        int(self._settings.worker_step_timeout_seconds * 2),
                    )
                )
                for user_id in selected:
                    state = states_by_user.get(user_id)
                    if state is None:
                        session.add(
                            ChannelCommentMentionState(
                                rule_id=stored.rule_id,
                                telegram_user_id=user_id,
                                reserved_run_id=stored.id,
                                reserved_until=reserve_until,
                            )
                        )
                        continue
                    claimed = await session.execute(
                        update(ChannelCommentMentionState)
                        .execution_options(synchronize_session=False)
                        .where(
                            ChannelCommentMentionState.id == state.id,
                            or_(
                                ChannelCommentMentionState.last_mentioned_at.is_(None),
                                ChannelCommentMentionState.last_mentioned_at <= cooldown_cutoff,
                            ),
                            or_(
                                ChannelCommentMentionState.reserved_run_id == stored.id,
                                ChannelCommentMentionState.reserved_run_id.is_(None),
                                ChannelCommentMentionState.reserved_until.is_(None),
                                ChannelCommentMentionState.reserved_until <= now,
                            ),
                        )
                        .values(
                            reserved_run_id=stored.id,
                            reserved_until=reserve_until,
                            updated_at=now,
                        )
                    )
                    if claimed.rowcount != 1:
                        reservation_conflict = True
                        break
                if reservation_conflict:
                    await session.rollback()
                    continue
                stored.hidden_mention_user_ids_snapshot_json = json.dumps(list(selected))
                stored.hidden_mention_resolved_count = len(selected)
                stored.hidden_mention_skip_reason = resolved_reason
                add_audit_event(
                    session,
                    event_type="channel_comment.hidden_mentions_resolved",
                    entity_type="channel_comment_run",
                    entity_id=stored.id,
                    details={
                        "selected_count": len(selected),
                        "reason": resolved_reason,
                        "cooldown_days": cooldown_days,
                        "candidate_count": len(candidates),
                        "eligible_count": len(eligible),
                    },
                    actor="channel-comment-worker",
                )
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                return selected

        # Reservation contention must never suppress the actual comment.
        async with self._session_factory() as session:
            stored = await self._require_running(session, run.id, run.lease_token or "")
            stored.hidden_mention_user_ids_snapshot_json = "[]"
            stored.hidden_mention_resolved_count = 0
            stored.hidden_mention_skip_reason = "mention_reservation_conflict"
            await session.commit()
        return ()

    async def _update_channel_comment_hidden_mention_snapshot(
        self,
        run: ChannelCommentRun,
        user_ids: tuple[int, ...],
    ) -> None:
        """Normalize an older snapshot after the owned-account set changes."""

        async with self._session_factory() as session:
            stored = await self._require_running(session, run.id, run.lease_token or "")
            stored.hidden_mention_user_ids_snapshot_json = json.dumps(list(user_ids))
            stored.hidden_mention_resolved_count = len(user_ids)
            if not user_ids:
                stored.hidden_mention_skip_reason = "excluded_user_after_resolution"
            reservation_query = update(ChannelCommentMentionState).where(
                ChannelCommentMentionState.reserved_run_id == stored.id
            )
            if user_ids:
                reservation_query = reservation_query.where(
                    ChannelCommentMentionState.telegram_user_id.not_in(user_ids)
                )
            await session.execute(
                reservation_query.values(reserved_run_id=None, reserved_until=None)
            )
            await session.commit()

    @staticmethod
    def _mention_state_is_eligible(
        state: ChannelCommentMentionState | None,
        *,
        run_id: int,
        now: datetime,
        cooldown_cutoff: datetime,
    ) -> bool:
        if state is None:
            return True
        last_mentioned_at = ensure_utc(state.last_mentioned_at)
        if last_mentioned_at is not None and last_mentioned_at > cooldown_cutoff:
            return False
        reserved_until = ensure_utc(state.reserved_until)
        return bool(
            state.reserved_run_id in {None, run_id}
            or reserved_until is None
            or reserved_until <= now
        )

    @staticmethod
    def _parse_hidden_mention_snapshot(
        value: str,
        *,
        exclude_user_id: int | None = None,
        exclude_user_ids: tuple[int, ...] | set[int] = (),
    ) -> tuple[int, ...]:
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            return ()
        if not isinstance(raw, list):
            return ()
        normalized = normalize_user_ids(
            raw,
            exclude_user_id=exclude_user_id,
            exclude_user_ids=exclude_user_ids,
        )
        if not HIDDEN_MENTION_MINIMUM <= len(normalized) <= HIDDEN_MENTION_MAXIMUM:
            return ()
        return normalized

    async def _finalize_hidden_mentions(
        self,
        session: AsyncSession,
        run: ChannelCommentRun,
        *,
        delivered_at: datetime,
    ) -> None:
        raw_snapshot = run.hidden_mention_user_ids_snapshot_json
        if raw_snapshot is None:
            return
        user_ids = self._parse_hidden_mention_snapshot(raw_snapshot)
        states = list(
            await session.scalars(
                select(ChannelCommentMentionState).where(
                    ChannelCommentMentionState.rule_id == run.rule_id,
                    ChannelCommentMentionState.telegram_user_id.in_(user_ids),
                )
            )
        )
        states_by_user = {int(item.telegram_user_id): item for item in states}
        for user_id in user_ids:
            state = states_by_user.get(user_id)
            if state is None:
                session.add(
                    ChannelCommentMentionState(
                        rule_id=run.rule_id,
                        telegram_user_id=user_id,
                        last_mentioned_at=delivered_at,
                        mention_count=1,
                    )
                )
                continue
            state.last_mentioned_at = delivered_at
            state.mention_count += 1
            if state.reserved_run_id == run.id:
                state.reserved_run_id = None
                state.reserved_until = None
        await self._release_mention_reservations(session, run_ids=[run.id])
        if user_ids:
            add_audit_event(
                session,
                event_type="channel_comment.hidden_mentions_delivered",
                entity_type="channel_comment_run",
                entity_id=run.id,
                details={"selected_count": len(user_ids)},
                actor="channel-comment-worker",
            )

    async def _finalize_moderated_hidden_mentions(
        self,
        run_id: int,
        lease_token: str,
    ) -> None:
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            if run.step_index == 0:
                await self._finalize_hidden_mentions(
                    session,
                    run,
                    delivered_at=utc_now(),
                )
                await session.commit()

    async def _record_delivery(
        self,
        run_id: int,
        lease_token: str,
        rule_snapshot: ChannelCommentRule,
        steps: list[DialogueStep],
        sent: ChannelCommentDelivery,
    ) -> None:
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            rule = await session.get(ChannelCommentRule, run.rule_id)
            if rule is None:
                raise NotFoundError("настройка канала удалена")
            was_message_edit = (
                rule_snapshot.mode == ChannelCommentMode.MESSAGE_EDIT and run.step_index == 1
            )
            delivered_at = utc_now()
            if sent.message_id <= 0 or sent.thread_root_message_id <= 0:
                raise PermanentGatewayError(
                    "Нет подтверждённого ID комментария", code="delivery_unconfirmed"
                )
            source_account_id = int(steps[run.step_index].account_id)
            if not was_message_edit:
                session.add(
                    ChannelCommentMessage(
                        run_id=run.id,
                        account_id=source_account_id,
                        step_index=int(run.step_index),
                        channel_ref=rule_snapshot.channel_ref,
                        source_post_id=run.source_post_id,
                        message_id=int(sent.message_id),
                        discussion_peer_id=sent.discussion_peer_id,
                        discussion_access_hash=sent.discussion_access_hash,
                        delivered_at=delivered_at,
                        status="simulated" if sent.simulated else "pending",
                        next_check_at=None
                        if sent.simulated
                        else delivered_at
                        + timedelta(
                            seconds=int(self._settings.delayed_delivery_verify_delay_seconds)
                        ),
                    )
                )
            else:
                message = await session.scalar(
                    select(ChannelCommentMessage).where(
                        ChannelCommentMessage.run_id == run.id,
                        ChannelCommentMessage.message_id == sent.message_id,
                        ChannelCommentMessage.account_id == source_account_id,
                    )
                )
                if message is not None:
                    # One physical message: an edit re-arms observation of its
                    # latest content and invalidates any in-flight old read.
                    message.edited_at = delivered_at
                    message.status = "simulated" if sent.simulated else "pending"
                    message.check_stage = 0
                    message.checked_at = None
                    message.first_checked_at = None
                    message.first_check_status = None
                    message.attempt_count = 0
                    message.error = None
                    message.error_code = None
                    message.next_check_at = (
                        None
                        if sent.simulated
                        else delivered_at
                        + timedelta(
                            seconds=int(self._settings.delayed_delivery_verify_delay_seconds)
                        )
                    )
                    message.lease_owner = None
                    message.lease_token = None
                    message.lease_until = None
            if run.step_index == 0 and not sent.simulated:
                await self._finalize_hidden_mentions(
                    session,
                    run,
                    delivered_at=delivered_at,
                )
            elif sent.simulated:
                await self._release_mention_reservations(session, run_ids=[run.id])
            if (
                not was_message_edit
                and not sent.simulated
                and rule.enabled
                and not run.cancel_requested
            ):
                await self._schedule_comment_reactions(
                    session,
                    run,
                    rule_snapshot,
                    source_account_id=source_account_id,
                    message_id=int(sent.message_id),
                    dialogue_step=int(run.step_index) + 1,
                    now=delivered_at,
                )
            run.thread_root_message_id = run.thread_root_message_id or sent.thread_root_message_id
            run.last_comment_message_id = sent.message_id
            run.sent_steps += 1
            run.step_index += 1
            run.simulated = sent.simulated
            run.dispatch_started_at = None
            run.dispatch_account_id = None
            run.error = None
            run.error_code = None
            if run.cancel_requested:
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = delivered_at
                run.error = "comment completed while cancellation was being requested"
                run.error_code = "completed_during_cancel"
                self._release_lease(run)
            elif not rule.enabled or not rule.approved_by_operator:
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = delivered_at
                run.error = "комментарий завершился во время остановки правила"
                run.error_code = (
                    "completed_during_archive" if rule.archived_at is not None else "rule_paused"
                )
                self._release_lease(run)
            elif run.step_index < len(steps):
                run.status = ChannelCommentRunStatus.QUEUED
                run.scheduled_for = delivered_at + timedelta(
                    seconds=steps[run.step_index].delay_seconds
                )
                self._release_lease(run)
            else:
                run.status = ChannelCommentRunStatus.SUCCEEDED
                run.finished_at = delivered_at
                self._release_lease(run)
                rule.last_success_at = run.finished_at
            add_audit_event(
                session,
                event_type=(
                    "channel_comment.edited" if was_message_edit else "channel_comment.sent"
                ),
                entity_type="channel_comment_run",
                entity_id=run.id,
                details={
                    "rule_id": rule_snapshot.id,
                    "source_post_id": run.source_post_id,
                    "step": run.step_index,
                    "message_id": sent.message_id,
                    "account_id": source_account_id,
                    "discussion_peer_id": sent.discussion_peer_id,
                    "action": "edit" if was_message_edit else "send",
                },
                actor="channel-comment-worker",
            )
            await session.commit()

    async def _handle_delivery_error(
        self,
        run_id: int,
        lease_token: str,
        account_id: int,
        exc: BaseException,
        *,
        account_failure_applied: bool = False,
    ) -> None:
        code = failure_code(exc, "delivery_failed")
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            if run.dispatch_started_at is not None:
                account_id = int(run.dispatch_account_id or account_id)
                if code == "comment_timeout" or not isinstance(exc, (GatewayError, ConflictError)):
                    exc = PermanentGatewayError(
                        "Операция прервана после начала отправки; "
                        "результат Telegram неизвестен, повтор запрещён",
                        code="delivery_unconfirmed",
                    )
                    code = "delivery_unconfirmed"
                if code != "delivery_unconfirmed":
                    # An explicit Telegram rejection proves no message was accepted.
                    run.dispatch_started_at = None
                    run.dispatch_account_id = None
                    await session.commit()
        if not account_failure_applied and isinstance(exc.__cause__, GatewayError):
            await self._runner.handle_external_account_failure(account_id, exc.__cause__)
        if code == "delivery_unconfirmed":
            if not account_failure_applied and account_id > 0:
                await self._runner.record_external_account_send(
                    account_id,
                    operation_key=f"channel-comment:{run_id}:step:{run.step_index}"
                    + (
                        f":membership-gate-retry:{run.membership_gate_retry_count}"
                        if run.membership_gate_retry_count
                        else ""
                    ),
                )
            await self._finish_failed(run_id, lease_token, exc, permanent=True)
            return
        if code in {"account_restricted", "peer_flood"} and (
            self._settings.account_restriction_cooldown_hours == 0
        ):
            if not account_failure_applied:
                await self._runner.handle_external_account_failure(account_id, exc)
            await self._finish_failed(run_id, lease_token, exc, permanent=True)
            return
        if code in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES and not account_failure_applied:
            await self._runner.handle_external_account_failure(account_id, exc)
        replaced = False
        if code in AUTO_REPLACE_PRIMARY_ACCOUNT_CODES:
            replaced = await self._try_auto_replace_primary_account(
                run_id=run_id,
                lease_token=lease_token,
                failed_account_id=account_id,
                exc=exc,
            )
        if replaced:
            return
        if (
            code == "post_deleted_by_moderation"
            and getattr(exc, "external_message_id", None) is not None
        ):
            # Telegram accepted the comment before moderation removed it, so it
            # still consumes this account's workload for the day.
            await self._runner.record_external_account_send(
                account_id,
                operation_key=(
                    f"channel-comment:{run_id}:moderated:{int(exc.external_message_id)}"
                ),
            )
            await self._finalize_moderated_hidden_mentions(run_id, lease_token)
            recovery = await self._recover_membership_gate(run_id, lease_token, account_id, exc)
            if recovery is not None and recovery.detected:
                if recovery.recovered and await self._schedule_membership_gate_retry(
                    run_id,
                    lease_token,
                    account_id,
                    recovery,
                ):
                    return
                exc = PermanentGatewayError(
                    "membership gate recovery failed: "
                    f"{recovery.error or 'retry window is no longer available'}",
                    code="post_deleted_by_moderation",
                    external_message_id=getattr(exc, "external_message_id", None),
                )
        if code in {
            # This lock is acquired before a Telegram RPC. It is normal when a
            # scan, reaction or another worker owns the account briefly, so it
            # must not spend a comment-delivery attempt.
            "account_operation_busy",
            "flood_wait",
            "slow_mode",
            "proxy_unavailable",
            "account_restricted",
            "network_error",
            "rpc_error",
            "comment_timeout",
            "join_request_pending",
        }:
            retry_after = int(getattr(exc, "retry_after", None) or 60)
            resume_at = utc_now() + timedelta(seconds=max(1, retry_after))
            if account_id > 0 and code in ACCOUNT_TEMPORARY_CODES:
                async with self._session_factory() as session:
                    account = await session.get(Account, account_id)
                    cooldown = ensure_utc(account.cooldown_until) if account is not None else None
                    if cooldown is not None:
                        resume_at = max(resume_at, cooldown)
            await self._defer_run(
                run_id,
                lease_token,
                resume_at,
                reason=code,
                consume_attempt=code in {"network_error", "rpc_error", "comment_timeout"},
                error=safe_error_message(exc),
            )
            return
        await self._finish_failed(
            run_id,
            lease_token,
            exc,
            permanent=(isinstance(exc, ConflictError) or getattr(exc, "retryable", True) is False),
        )

    async def _recover_membership_gate(
        self,
        run_id: int,
        lease_token: str,
        account_id: int,
        exc: BaseException,
    ) -> MembershipGateRecovery | None:
        """Resolve an explicit gate in the actual discussion group for this comment."""

        deleted_message_id = getattr(exc, "external_message_id", None)
        recover = getattr(self._gateway, "recover_channel_comment_membership_gate", None)
        if recover is None or deleted_message_id is None:
            return None
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            rule = await session.get(ChannelCommentRule, run.rule_id)
            account = await session.get(Account, account_id)
        if rule is None or account is None:
            return None
        try:
            result = await recover(
                account=account,
                channel_ref=rule.channel_ref,
                channel_post_id=int(run.source_post_id),
                deleted_message_id=int(deleted_message_id),
            )
        except GatewayError as recovery_exc:
            return MembershipGateRecovery(
                detected=True,
                recovered=False,
                error=safe_error_message(recovery_exc),
            )
        except Exception:  # noqa: BLE001 - the deleted comment remains authoritative
            logger.exception("Membership-gate recovery failed for channel-comment run %s", run_id)
            return MembershipGateRecovery(
                detected=True,
                recovered=False,
                error="membership gate operation failed",
            )
        return result if isinstance(result, MembershipGateRecovery) else None

    async def _schedule_membership_gate_retry(
        self,
        run_id: int,
        lease_token: str,
        account_id: int,
        recovery: MembershipGateRecovery,
    ) -> bool:
        """Persist one completed gate flow and retry the exact comment step once."""

        retry_at = utc_now() + timedelta(
            seconds=int(getattr(self._settings, "membership_gate_retry_delay_seconds", 300))
        )
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            if run.membership_gate_retry_count >= 1 or run.cancel_requested:
                return False
            expires_at = ensure_utc(run.expires_at)
            if expires_at is not None and retry_at >= expires_at:
                return False
            run.membership_gate_retry_count = 1
            run.status = ChannelCommentRunStatus.QUEUED
            run.scheduled_for = retry_at
            run.error = None
            run.error_code = "membership_gate_retry"
            run.attempt_count = max(0, run.attempt_count - 1)
            self._release_lease(run)
            add_audit_event(
                session,
                event_type="channel_comment.membership_gate_retry_scheduled",
                entity_type="channel_comment_run",
                entity_id=run.id,
                details={
                    "account_id": account_id,
                    "retry_at": retry_at.isoformat(),
                    "joined_channel_refs": list(recovery.joined_channel_refs),
                    "verification_callback_count": recovery.verification_callback_count,
                },
                actor="channel-comment-worker",
            )
            await session.commit()
            return True

    async def _defer_run(
        self,
        run_id: int,
        lease_token: str,
        scheduled_for: datetime,
        *,
        reason: str,
        consume_attempt: bool = False,
        error: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            rule = await session.get(ChannelCommentRule, run.rule_id)
            now = utc_now()
            expires_at = ensure_utc(run.expires_at)
            if run.cancel_requested:
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = now
                run.error = "cancel requested before comment"
                run.error_code = "cancelled_before_send"
            elif expires_at is not None and (
                expires_at <= now or ensure_utc(scheduled_for) >= expires_at
            ):
                run.status = (
                    ChannelCommentRunStatus.FAILED
                    if run.sent_steps > 0
                    else ChannelCommentRunStatus.SKIPPED
                )
                run.finished_at = now
                run.error = "срок актуальности комментария истёк во время повторных попыток"
                run.error_code = "comment_expired"
            elif consume_attempt and run.attempt_count >= run.max_attempts:
                run.status = ChannelCommentRunStatus.FAILED
                run.finished_at = now
                run.error = (error or "лимит повторных попыток исчерпан")[:2000]
                run.error_code = reason
            else:
                run.status = ChannelCommentRunStatus.QUEUED
                run.scheduled_for = scheduled_for
                run.error = (error or self._retry_reason_text(reason))[:2000]
                run.error_code = reason
                if not consume_attempt:
                    run.attempt_count = max(0, run.attempt_count - 1)
            if run.status != ChannelCommentRunStatus.QUEUED:
                await self._release_mention_reservations(session, run_ids=[run.id])
            if rule is not None and reason != "account_pacing":
                rule.last_error = (error or self._retry_reason_text(reason))[:500]
                rule.last_error_code = reason[:100]
                steps = self._rule_steps(rule)
                rule.last_error_account_id = (
                    steps[min(max(0, run.step_index), len(steps) - 1)].account_id
                    if steps
                    else rule.primary_account_id
                )
            self._release_lease(run)
            await session.commit()

    async def _finish_cancelled(self, run_id: int, lease_token: str, code: str) -> None:
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            run.status = ChannelCommentRunStatus.CANCELLED
            run.finished_at = utc_now()
            run.error = "cancel requested before comment"
            run.error_code = code
            await self._release_mention_reservations(session, run_ids=[run.id])
            self._release_lease(run)
            await session.commit()

    async def _finish_failed(
        self,
        run_id: int,
        lease_token: str,
        exc: BaseException,
        *,
        permanent: bool,
    ) -> None:
        error = safe_error_message(exc)
        code = str(getattr(exc, "code", None) or "delivery_failed")
        async with self._session_factory() as session:
            run = await self._require_running(session, run_id, lease_token)
            rule = await session.get(ChannelCommentRule, run.rule_id)
            exhausted = run.attempt_count >= run.max_attempts
            if run.cancel_requested and code != "delivery_unconfirmed":
                run.status = ChannelCommentRunStatus.CANCELLED
                run.finished_at = utc_now()
                code = "cancelled_before_send"
            elif code in {"comments_unavailable", "post_deleted_by_moderation"} or (
                code == "comment_expired" and run.sent_steps == 0
            ):
                run.status = (
                    ChannelCommentRunStatus.FAILED
                    if run.sent_steps
                    else ChannelCommentRunStatus.SKIPPED
                )
                run.finished_at = utc_now()
            elif permanent or exhausted:
                run.status = ChannelCommentRunStatus.FAILED
                run.finished_at = utc_now()
            else:
                run.status = ChannelCommentRunStatus.QUEUED
                run.scheduled_for = utc_now() + timedelta(
                    seconds=min(300, 15 * (2 ** max(0, run.attempt_count - 1)))
                )
                expires_at = ensure_utc(run.expires_at)
                if expires_at is not None and ensure_utc(run.scheduled_for) >= expires_at:
                    run.status = (
                        ChannelCommentRunStatus.FAILED
                        if run.sent_steps > 0
                        else ChannelCommentRunStatus.SKIPPED
                    )
                    run.finished_at = utc_now()
                    code = "comment_expired"
                    error = "срок актуальности комментария истёк во время повторных попыток"
            run.error = error[:2000]
            run.error_code = code[:100]
            self._release_lease(run)
            if run.status != ChannelCommentRunStatus.QUEUED:
                await self._release_mention_reservations(session, run_ids=[run.id])
            if rule is not None:
                rule.last_error = error[:500]
                rule.last_error_code = code[:100]
                steps = self._rule_steps(rule)
                failed_account_id = (
                    steps[min(max(0, run.step_index), len(steps) - 1)].account_id
                    if steps
                    else rule.primary_account_id
                )
                rule.last_error_account_id = failed_account_id
                if code in CHANNEL_COMMENT_AUTO_PAUSE_CODES:
                    await self._auto_pause_rule(
                        session,
                        rule,
                        exc,
                        account_id=failed_account_id,
                        current_run_id=run.id,
                    )
            add_audit_event(
                session,
                event_type=(
                    "channel_comment.retry_scheduled"
                    if run.status == ChannelCommentRunStatus.QUEUED
                    else (
                        "channel_comment.skipped"
                        if run.status == ChannelCommentRunStatus.SKIPPED
                        else "channel_comment.failed"
                    )
                ),
                entity_type="channel_comment_run",
                entity_id=run.id,
                details={"rule_id": run.rule_id, "error_code": code},
                actor="channel-comment-worker",
            )
            await session.commit()

    async def _recover_expired_leases(self) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            uncertain_runs = list(
                await session.scalars(
                    select(ChannelCommentRun).where(
                        ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                        ChannelCommentRun.lease_until < now,
                        ChannelCommentRun.dispatch_started_at.is_not(None),
                        ChannelCommentRun.dispatch_account_id.is_not(None),
                    )
                )
            )
        for run in uncertain_runs:
            if run.simulated:
                continue
            await self._runner.record_external_account_send(
                int(run.dispatch_account_id),
                operation_key=f"channel-comment:{run.id}:step:{run.step_index}"
                + (
                    f":membership-gate-retry:{run.membership_gate_retry_count}"
                    if run.membership_gate_retry_count
                    else ""
                ),
            )
        async with self._session_factory() as session:
            await session.execute(
                update(ChannelCommentRun)
                .where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    ChannelCommentRun.lease_until < now,
                    ChannelCommentRun.dispatch_started_at.is_not(None),
                )
                .values(
                    status=ChannelCommentRunStatus.FAILED,
                    finished_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                    error=(
                        "Процесс прервался после начала отправки. "
                        "Результат Telegram неизвестен; повтор запрещён"
                    ),
                    error_code="delivery_unconfirmed",
                )
                .execution_options(synchronize_session=False)
            )
            await session.execute(
                update(ChannelCommentRun)
                .execution_options(synchronize_session=False)
                .where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
                    ChannelCommentRun.lease_until.is_not(None),
                    ChannelCommentRun.lease_until < now,
                )
                .values(
                    status=ChannelCommentRunStatus.QUEUED,
                    scheduled_for=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                    error_code="lease_recovered",
                )
            )
            await session.execute(
                update(ChannelCommentRun)
                .execution_options(synchronize_session=False)
                .where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.cancel_requested.is_(True),
                    or_(
                        ChannelCommentRun.error_code.is_(None),
                        ChannelCommentRun.error_code != "lease_recovered",
                    ),
                )
                .values(
                    status=ChannelCommentRunStatus.CANCELLED,
                    finished_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                    error="cancel requested before comment",
                    error_code="cancelled_before_send",
                )
            )
            await session.execute(
                update(ChannelCommentRun)
                .execution_options(synchronize_session=False)
                .where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.cancel_requested.is_(True),
                    ChannelCommentRun.error_code == "lease_recovered",
                )
                .values(
                    status=ChannelCommentRunStatus.FAILED,
                    finished_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                    error=(
                        "cancel was requested while the comment was running; "
                        "the Telegram outcome is unknown after lease expiry"
                    ),
                    error_code="cancelled_outcome_unknown",
                )
            )
            await self._expire_stale_runs(session, now)
            terminal_run_ids = list(
                await session.scalars(
                    select(ChannelCommentMentionState.reserved_run_id)
                    .join(
                        ChannelCommentRun,
                        ChannelCommentRun.id == ChannelCommentMentionState.reserved_run_id,
                    )
                    .where(
                        ChannelCommentMentionState.reserved_run_id.is_not(None),
                        ChannelCommentRun.status.in_(
                            (
                                ChannelCommentRunStatus.CANCELLED,
                                ChannelCommentRunStatus.FAILED,
                                ChannelCommentRunStatus.SKIPPED,
                            )
                        ),
                    )
                )
            )
            await self._release_mention_reservations(
                session,
                run_ids=terminal_run_ids,
            )
            await session.commit()

    async def _rule_view(
        self,
        session: AsyncSession,
        rule: ChannelCommentRule,
    ) -> ChannelCommentRuleView:
        return (await self._views_for_rules(session, [rule]))[0]

    async def _views_for_rules(
        self,
        session: AsyncSession,
        rules: list[ChannelCommentRule],
    ) -> list[ChannelCommentRuleView]:
        if not rules:
            return []
        rule_ids = [int(rule.id) for rule in rules]
        result = await session.execute(
            select(
                ChannelCommentRun.rule_id,
                ChannelCommentRun.status,
                ChannelCommentRun.error_code,
                ChannelCommentRun.simulated,
                func.count(ChannelCommentRun.id),
            )
            .where(ChannelCommentRun.rule_id.in_(rule_ids))
            .group_by(
                ChannelCommentRun.rule_id,
                ChannelCommentRun.status,
                ChannelCommentRun.error_code,
                ChannelCommentRun.simulated,
            )
        )
        counts: dict[int, dict[ChannelCommentRunStatus, int]] = {
            rule_id: {} for rule_id in rule_ids
        }
        uncertain_counts = dict.fromkeys(rule_ids, 0)
        simulated_counts = dict.fromkeys(rule_ids, 0)
        for rule_id, status, code, simulated, count in result:
            if code in {"delivery_unconfirmed", "cancelled_outcome_unknown"}:
                uncertain_counts[int(rule_id)] += int(count)
            elif simulated and status == ChannelCommentRunStatus.SUCCEEDED:
                simulated_counts[int(rule_id)] += int(count)
            else:
                counts[int(rule_id)][status] = counts[int(rule_id)].get(status, 0) + int(count)
        mention_result = await session.execute(
            select(
                ChannelCommentRun.rule_id,
                ChannelCommentRun.hidden_mention_skip_reason,
                func.count(ChannelCommentRun.id),
                func.coalesce(func.sum(ChannelCommentRun.hidden_mention_resolved_count), 0),
            )
            .where(
                ChannelCommentRun.rule_id.in_(rule_ids),
                ChannelCommentRun.hidden_mentions_enabled_snapshot.is_(True),
                ChannelCommentRun.simulated.is_(False),
                ChannelCommentRun.hidden_mention_user_ids_snapshot_json.is_not(None),
                ChannelCommentRun.sent_steps > 0,
            )
            .group_by(
                ChannelCommentRun.rule_id,
                ChannelCommentRun.hidden_mention_skip_reason,
            )
        )
        mention_stats: dict[int, dict[str | None, tuple[int, int]]] = {
            rule_id: {} for rule_id in rule_ids
        }
        for rule_id, reason, post_count, mention_count in mention_result:
            mention_stats[int(rule_id)][reason] = (int(post_count), int(mention_count))
        reaction_result = await session.execute(
            select(
                ChannelCommentRun.rule_id,
                ReactionTask.status,
                func.count(ReactionTask.id),
            )
            .join(
                ReactionTask,
                ReactionTask.channel_comment_run_id == ChannelCommentRun.id,
            )
            .where(ChannelCommentRun.rule_id.in_(rule_ids))
            .group_by(ChannelCommentRun.rule_id, ReactionTask.status)
        )
        reaction_counts: dict[int, dict[ReactionTaskStatus, int]] = {
            rule_id: {} for rule_id in rule_ids
        }
        for rule_id, status, count in reaction_result:
            reaction_counts[int(rule_id)][status] = int(count)
        delivery_summaries = await self._delivery_summaries(session, rule_ids, by_rule=True)
        legacy_result = await session.execute(
            select(ChannelCommentRun.rule_id, func.count(ChannelCommentRun.id))
            .outerjoin(
                ChannelCommentMessage,
                (ChannelCommentMessage.run_id == ChannelCommentRun.id)
                & (ChannelCommentMessage.step_index == 0),
            )
            .where(
                ChannelCommentRun.rule_id.in_(rule_ids),
                ChannelCommentRun.sent_steps > 0,
                ChannelCommentMessage.id.is_(None),
            )
            .group_by(ChannelCommentRun.rule_id)
        )
        legacy_counts = {int(rule_id): int(count) for rule_id, count in legacy_result}
        return [
            ChannelCommentRuleView(
                rule=rule,
                queued=counts[int(rule.id)].get(ChannelCommentRunStatus.QUEUED, 0),
                running=counts[int(rule.id)].get(ChannelCommentRunStatus.RUNNING, 0),
                succeeded=counts[int(rule.id)].get(ChannelCommentRunStatus.SUCCEEDED, 0),
                failed=counts[int(rule.id)].get(ChannelCommentRunStatus.FAILED, 0),
                skipped=counts[int(rule.id)].get(ChannelCommentRunStatus.SKIPPED, 0),
                cancelled=counts[int(rule.id)].get(ChannelCommentRunStatus.CANCELLED, 0),
                uncertain=uncertain_counts[int(rule.id)],
                simulated_finished=simulated_counts[int(rule.id)],
                delivery=delivery_summaries[int(rule.id)],
                legacy_runs_with_incomplete_delivery=legacy_counts.get(int(rule.id), 0),
                mentions_resolved_posts=sum(
                    post_count
                    for reason, (post_count, _) in mention_stats[int(rule.id)].items()
                    if reason is None
                ),
                mentions_skipped_posts=sum(
                    post_count
                    for reason, (post_count, _) in mention_stats[int(rule.id)].items()
                    if reason is not None
                ),
                mention_count=sum(
                    mention_count for _, mention_count in mention_stats[int(rule.id)].values()
                ),
                mention_skip_reasons=tuple(
                    sorted(
                        (
                            (reason, post_count)
                            for reason, (post_count, _) in mention_stats[int(rule.id)].items()
                            if reason is not None
                        ),
                        key=lambda item: (-item[1], item[0]),
                    )
                ),
                reaction_queued=reaction_counts[int(rule.id)].get(ReactionTaskStatus.QUEUED, 0),
                reaction_running=reaction_counts[int(rule.id)].get(ReactionTaskStatus.RUNNING, 0),
                reaction_succeeded=reaction_counts[int(rule.id)].get(
                    ReactionTaskStatus.SUCCEEDED, 0
                ),
                reaction_failed=reaction_counts[int(rule.id)].get(ReactionTaskStatus.FAILED, 0),
                reaction_skipped=reaction_counts[int(rule.id)].get(ReactionTaskStatus.SKIPPED, 0),
                reaction_cancelled=reaction_counts[int(rule.id)].get(
                    ReactionTaskStatus.CANCELLED, 0
                ),
            )
            for rule in rules
        ]

    @staticmethod
    async def _require_running(
        session: AsyncSession,
        run_id: int,
        lease_token: str,
    ) -> ChannelCommentRun:
        run = await session.get(ChannelCommentRun, run_id)
        if run is None:
            raise NotFoundError("комментарий не найден")
        if run.status != ChannelCommentRunStatus.RUNNING or run.lease_token != lease_token:
            raise ConflictError("задача комментария уже взята другим worker")
        return run

    @staticmethod
    def _release_lease(run: ChannelCommentRun) -> None:
        run.lease_owner = None
        run.lease_token = None
        run.lease_until = None

    @staticmethod
    def _store_rule_error(
        rule: ChannelCommentRule,
        exc: BaseException,
        *,
        account_id: int | None = None,
    ) -> None:
        rule.last_error = safe_error_message(exc)[:500]
        rule.last_error_code = str(getattr(exc, "code", None) or "rule_error")[:100]
        rule.last_error_account_id = account_id

    async def _auto_pause_rule(
        self,
        session: AsyncSession,
        rule: ChannelCommentRule,
        exc: BaseException,
        *,
        account_id: int | None,
        current_run_id: int | None = None,
    ) -> None:
        """Stop a fixed comment route after a permanent account/channel failure."""
        code = failure_code(exc, "rule_error")
        if code not in CHANNEL_COMMENT_AUTO_PAUSE_CODES:
            return
        now = utc_now()
        error = safe_error_message(exc)
        was_enabled = bool(rule.enabled)
        rule.enabled = False
        rule.last_error = error[:500]
        rule.last_error_code = code[:100]
        rule.last_error_account_id = account_id
        queued_ids = list(
            await session.scalars(
                select(ChannelCommentRun.id).where(
                    ChannelCommentRun.rule_id == rule.id,
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                )
            )
        )
        await session.execute(
            update(ChannelCommentRun)
            .where(
                ChannelCommentRun.rule_id == rule.id,
                ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
            )
            .values(
                status=ChannelCommentRunStatus.CANCELLED,
                finished_at=now,
                error=f"правило автоматически остановлено: {error}"[:2000],
                error_code="rule_auto_paused",
                lease_owner=None,
                lease_token=None,
                lease_until=None,
            )
        )
        running_filter = [
            ChannelCommentRun.rule_id == rule.id,
            ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING,
        ]
        if current_run_id is not None:
            running_filter.append(ChannelCommentRun.id != current_run_id)
        await session.execute(
            update(ChannelCommentRun)
            .where(*running_filter)
            .values(
                cancel_requested=True,
                error=f"правило автоматически остановлено: {error}"[:2000],
                error_code="cancel_requested",
            )
        )
        await self._release_mention_reservations(session, run_ids=queued_ids)
        await self._cancel_rule_reactions(
            session,
            int(rule.id),
            reason="channel comment rule automatically paused",
            code="rule_auto_paused",
        )
        if was_enabled:
            add_audit_event(
                session,
                event_type="channel_comment_rule.auto_paused",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={
                    "account_id": account_id,
                    "error_code": code,
                    "error": error[:500],
                },
                actor="channel-comment-worker",
            )

    @staticmethod
    async def _active_rule(
        session: AsyncSession,
        rule_id: int,
    ) -> ChannelCommentRule | None:
        return await session.scalar(
            select(ChannelCommentRule).where(
                ChannelCommentRule.id == rule_id,
                ChannelCommentRule.archived_at.is_(None),
            )
        )

    async def _preflight_accounts(
        self,
        accounts: list[Account],
        channel_ref: str,
    ) -> ChannelCommentPreflight | None:
        preflight = getattr(self._gateway, "preflight_channel_comments", None)
        if preflight is None:
            return None
        result: ChannelCommentPreflight | None = None
        for account in accounts:
            cooldown = ensure_utc(account.cooldown_until)
            if cooldown is not None and cooldown > utc_now():
                paused = ConflictError(
                    f"Аккаунт «{account.label}» ожидает окончания паузы Telegram "
                    f"до {cooldown:%d.%m %H:%M UTC}"
                )
                paused.code = "preflight_account_cooldown"  # type: ignore[attr-defined]
                raise paused
            try:
                async with asyncio.timeout(self._settings.channel_comments_scan_timeout_seconds):
                    current = await preflight(account=account, channel_ref=channel_ref)
            except TimeoutError:
                exc: BaseException = TemporaryGatewayError(
                    "проверка возможности комментирования не завершилась вовремя",
                    code="channel_preflight_timeout",
                )
            except Exception as caught:  # noqa: BLE001 - converted to a safe control error
                exc = caught
            else:
                result = result or current
                continue
            if account.id is not None:
                await self._runner.handle_external_account_failure(account.id, exc)
            wrapped = ConflictError(
                f"аккаунт «{account.label}» не прошёл проверку комментариев: "
                f"{safe_error_message(exc)}"
            )
            wrapped.code = str(  # type: ignore[attr-defined]
                getattr(exc, "code", None) or "channel_preflight_failed"
            )
            raise wrapped from exc
        return result

    async def _try_auto_replace_primary_account(
        self,
        *,
        failed_account_id: int,
        exc: BaseException,
        rule_id: int | None = None,
        run_id: int | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Move a safe, unstarted comment flow to a verified fallback sender.

        A replacement is intentionally conservative: only the first step can
        move, no other flow may be running, and a candidate is live-checked in
        the channel before it becomes the new default sender.
        """
        if failure_code(exc) not in AUTO_REPLACE_PRIMARY_ACCOUNT_CODES:
            return False
        if run_id is None and rule_id is None:
            return False
        if run_id is not None and not lease_token:
            return False

        async with self._session_factory() as session:
            run: ChannelCommentRun | None = None
            if run_id is not None:
                try:
                    run = await self._require_running(session, run_id, str(lease_token))
                except (ConflictError, NotFoundError):
                    return False
                rule = await self._active_rule(session, int(run.rule_id))
            else:
                rule = await self._active_rule(session, int(rule_id))
            if rule is None or rule.primary_account_id != failed_account_id:
                return False
            if not await self._auto_replacement_is_safe(
                session,
                rule,
                run=run,
            ):
                return False
            candidate_conditions = [
                Account.id != failed_account_id,
                Account.status == AccountStatus.ACTIVE,
                Account.ownership_confirmed.is_(True),
            ]
            if not self._settings.dry_run:
                candidate_conditions.extend(
                    [
                        Account.authorized.is_(True),
                        Account.session_encrypted.is_not(None),
                        Account.api_id.is_not(None),
                        Account.api_hash_encrypted.is_not(None),
                    ]
                )
            candidates = list(
                await session.scalars(
                    select(Account).where(*candidate_conditions).order_by(Account.id)
                )
            )
            channel_ref = rule.channel_ref

        for candidate in candidates:
            candidate_id = int(candidate.id)
            try:
                self._validate_account(candidate)
                if rule.mode == ChannelCommentMode.DIALOGUE and (
                    self._replacement_dialogue_steps(rule, candidate_id) is None
                ):
                    continue
                preflight = await self._preflight_accounts([candidate], channel_ref)
            except ConflictError:
                # A candidate that cannot comment in this channel is not a
                # fallback. Its own failure state has already been recorded by
                # the preflight helper.
                continue
            except Exception:  # noqa: BLE001 - keep the failed route contained
                logger.exception(
                    "Auto replacement preflight failed for channel-comment rule %s", rule.id
                )
                continue

            if await self._apply_auto_primary_replacement(
                rule_id=int(rule.id),
                failed_account_id=failed_account_id,
                replacement_account_id=candidate_id,
                exc=exc,
                run_id=run_id,
                lease_token=lease_token,
                channel_title=preflight.title if preflight is not None else None,
            ):
                return True
        return False

    async def _auto_replacement_is_safe(
        self,
        session: AsyncSession,
        rule: ChannelCommentRule,
        *,
        run: ChannelCommentRun | None,
    ) -> bool:
        if not rule.enabled or not rule.auto_replace_primary_account:
            return False
        if run is not None and (
            run.rule_id != rule.id
            or run.cancel_requested
            or run.step_index != 0
            or run.sent_steps != 0
        ):
            return False
        running_condition = ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING
        if run is not None:
            running_condition = running_condition & (ChannelCommentRun.id != run.id)
        another_started_run = await session.scalar(
            select(ChannelCommentRun.id)
            .where(
                ChannelCommentRun.rule_id == rule.id,
                or_(
                    running_condition,
                    (ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED)
                    & (ChannelCommentRun.sent_steps > 0),
                ),
            )
            .limit(1)
        )
        return another_started_run is None

    async def _apply_auto_primary_replacement(
        self,
        *,
        rule_id: int,
        failed_account_id: int,
        replacement_account_id: int,
        exc: BaseException,
        run_id: int | None,
        lease_token: str | None,
        channel_title: str | None,
    ) -> bool:
        async with self._session_factory() as session:
            rule = await self._active_rule(session, rule_id)
            if rule is None or rule.primary_account_id != failed_account_id:
                return False
            run: ChannelCommentRun | None = None
            if run_id is not None:
                if not lease_token:
                    return False
                try:
                    run = await self._require_running(session, run_id, lease_token)
                except (ConflictError, NotFoundError):
                    return False
            if not await self._auto_replacement_is_safe(session, rule, run=run):
                return False
            replacement = await session.get(Account, replacement_account_id)
            try:
                self._validate_account(replacement)
            except ConflictError:
                return False

            replacement_steps = self._replacement_dialogue_steps(rule, replacement_account_id)
            if rule.mode == ChannelCommentMode.DIALOGUE and replacement_steps is None:
                return False
            rule.primary_account_id = replacement_account_id
            if replacement_steps is not None:
                rule.dialogue_steps_json = replacement_steps
            if channel_title:
                rule.channel_title = channel_title[:255]
            rule.last_error = "основной аккаунт автоматически заменён после ошибки Telegram"
            rule.last_error_code = "primary_account_auto_replaced"
            rule.last_error_account_id = failed_account_id
            if run is not None:
                run.status = ChannelCommentRunStatus.QUEUED
                run.scheduled_for = utc_now()
                run.attempt_count = max(0, run.attempt_count - 1)
                run.error = (
                    "основной аккаунт автоматически заменён; комментарий поставлен в очередь"
                )
                run.error_code = "primary_account_auto_replaced"
                self._release_lease(run)
            add_audit_event(
                session,
                event_type="channel_comment_rule.primary_account_auto_replaced",
                entity_type="channel_comment_rule",
                entity_id=rule.id,
                details={
                    "previous_account_id": failed_account_id,
                    "replacement_account_id": replacement_account_id,
                    "trigger_code": failure_code(exc),
                    "run_id": run_id,
                },
                actor="channel-comment-worker",
            )
            await session.commit()
            return True

    def _replacement_dialogue_steps(
        self,
        rule: ChannelCommentRule,
        replacement_account_id: int,
    ) -> str | None:
        if rule.mode != ChannelCommentMode.DIALOGUE:
            return rule.dialogue_steps_json
        try:
            steps = self._rule_steps(rule)
            steps[0] = replace(steps[0], account_id=replacement_account_id)
            validated_steps = parse_dialogue_steps(
                serialize_dialogue_steps(steps),
                primary_account_id=replacement_account_id,
                primary_text=rule.primary_text,
                reply_account_id=None,
                reply_text="",
                reply_delay_seconds=0,
            )
        except (ConflictError, ValueError):
            return None
        return serialize_dialogue_steps(validated_steps)

    async def _schedule_comment_reactions(
        self,
        session: AsyncSession,
        run: ChannelCommentRun,
        rule: ChannelCommentRule,
        *,
        source_account_id: int,
        message_id: int,
        dialogue_step: int,
        now: datetime,
    ) -> None:
        if (
            not rule.reactions_enabled
            or rule.reaction_count < 1
            or rule.reaction_dialogue_step != dialogue_step
        ):
            return
        candidate_ids = [
            account_id
            for account_id in self._reaction_account_ids(rule)
            if account_id != source_account_id
        ]
        if not candidate_ids:
            return
        rotation = int.from_bytes(
            hashlib.sha256(f"comment-reaction:{run.id}:{message_id}".encode()).digest()[:4],
            "big",
        ) % len(candidate_ids)
        candidate_ids = candidate_ids[rotation:] + candidate_ids[:rotation]
        account_conditions = [
            Account.id.in_(candidate_ids),
            Account.status == AccountStatus.ACTIVE,
            Account.ownership_confirmed.is_(True),
        ]
        if not self._settings.dry_run:
            account_conditions.append(Account.authorized.is_(True))
        accounts = list(await session.scalars(select(Account).where(*account_conditions)))
        accounts_by_id = {int(account.id): account for account in accounts}
        blocked_ids = set(
            await session.scalars(
                select(ChannelCommentReactionAccountState.account_id).where(
                    ChannelCommentReactionAccountState.rule_id == rule.id,
                    ChannelCommentReactionAccountState.account_id.in_(candidate_ids),
                    ChannelCommentReactionAccountState.send_blocked.is_(True),
                )
            )
        )
        ordered_ids = sorted(
            candidate_ids,
            key=lambda account_id: (
                (
                    (
                        (resume_at := self._runner.account_resume_at(accounts_by_id[account_id]))
                        is not None
                        and resume_at > now
                    ),
                    candidate_ids.index(account_id),
                )
                if account_id in accounts_by_id
                else (True, candidate_ids.index(account_id))
            ),
        )
        created = 0
        for ordinal, account_id in enumerate(ordered_ids):
            if (
                account_id not in accounts_by_id
                or account_id in blocked_ids
                or created >= min(10, int(rule.reaction_count))
            ):
                continue
            digest = hashlib.sha256(
                f"comment-reaction-delay:{run.id}:{message_id}:{account_id}".encode()
            ).digest()
            scheduled_for = now + timedelta(
                seconds=12 + int.from_bytes(digest[:2], "big") % 25 + ordinal * 3
            )
            resume_at = self._runner.account_resume_at(accounts_by_id[account_id])
            if resume_at is not None and resume_at > scheduled_for:
                scheduled_for = resume_at
            session.add(
                ReactionTask(
                    campaign_id=None,
                    run_id=None,
                    channel_comment_run_id=run.id,
                    target_id=None,
                    source_account_id=source_account_id,
                    reactor_account_id=account_id,
                    message_id=message_id,
                    emoji=rule.reaction_emoji,
                    scheduled_for=scheduled_for,
                    max_attempts=min(3, max(1, self._settings.worker_max_attempts)),
                )
            )
            created += 1
        if created:
            add_audit_event(
                session,
                event_type="channel_comment.reactions_queued",
                entity_type="channel_comment_run",
                entity_id=run.id,
                details={
                    "message_id": message_id,
                    "dialogue_step": dialogue_step,
                    "count": created,
                    "emoji": rule.reaction_emoji,
                },
                actor="channel-comment-worker",
            )

    @staticmethod
    def _reaction_account_ids(rule: ChannelCommentRule) -> list[int]:
        try:
            raw = json.loads(rule.reaction_account_ids_json or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
        if not isinstance(raw, list):
            return []
        return list(
            dict.fromkeys(
                int(value)
                for value in raw
                if isinstance(value, int) and not isinstance(value, bool) and value > 0
            )
        )

    @staticmethod
    async def _cancel_rule_reactions(
        session: AsyncSession,
        rule_id: int,
        *,
        reason: str,
        code: str,
    ) -> None:
        run_ids = select(ChannelCommentRun.id).where(ChannelCommentRun.rule_id == rule_id)
        await session.execute(
            update(ReactionTask)
            .where(
                ReactionTask.channel_comment_run_id.in_(run_ids),
                ReactionTask.status == ReactionTaskStatus.QUEUED,
            )
            .values(
                status=ReactionTaskStatus.CANCELLED,
                finished_at=utc_now(),
                error=reason,
                error_code=code,
            )
        )
        await session.execute(
            update(ReactionTask)
            .where(
                ReactionTask.channel_comment_run_id.in_(run_ids),
                ReactionTask.status == ReactionTaskStatus.RUNNING,
            )
            .values(
                cancel_requested=True,
                error=reason,
                error_code="cancel_requested",
            )
        )

    async def _expire_stale_runs(self, session: AsyncSession, now: datetime) -> int:
        runs = list(
            await session.scalars(
                select(ChannelCommentRun).where(
                    ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED,
                    ChannelCommentRun.expires_at.is_not(None),
                    ChannelCommentRun.expires_at <= now,
                )
            )
        )
        if not runs:
            return 0
        for run in runs:
            run.status = (
                ChannelCommentRunStatus.FAILED
                if run.sent_steps > 0
                else ChannelCommentRunStatus.SKIPPED
            )
            run.finished_at = now
            run.error = "срок актуальности комментария истёк"
            run.error_code = "comment_expired"
            self._release_lease(run)
        await self._release_mention_reservations(
            session,
            run_ids=[run.id for run in runs],
        )
        return len(runs)

    @staticmethod
    async def _release_mention_reservations(
        session: AsyncSession,
        *,
        run_ids: list[int],
    ) -> None:
        if not run_ids:
            return
        await session.execute(
            update(ChannelCommentMentionState)
            .where(ChannelCommentMentionState.reserved_run_id.in_(run_ids))
            .values(reserved_run_id=None, reserved_until=None)
        )

    @staticmethod
    def _retry_reason_text(reason: str) -> str:
        return {
            "account_operation_busy": "аккаунт занят другой операцией; комментарий ждёт",
            "account_pacing": "отправка ждёт доступного интервала аккаунта",
            "flood_wait": "Telegram потребовал увеличить паузу перед повтором",
            "slow_mode": "в группе обсуждения действует медленный режим",
            "proxy_unavailable": "маршрут аккаунта временно недоступен",
            "account_restricted": "аккаунт временно ограничен",
            "network_error": "сетевая ошибка; назначена повторная попытка",
            "rpc_error": "Telegram RPC временно недоступен; назначен повтор",
            "delivery_unconfirmed": "результат доставки уточняется перед повтором",
            "comment_timeout": "операция комментария превысила лимит времени",
        }.get(reason, "назначена повторная попытка комментария")

    def _validate_account(self, account: Account | None) -> None:
        if account is None or not account.ownership_confirmed:
            exc = ConflictError("выбранный аккаунт отсутствует или не подтверждён")
            exc.code = "sender_unavailable"  # type: ignore[attr-defined]
            raise exc
        if account.status != AccountStatus.ACTIVE:
            temporary_reasons = {
                "account_restricted",
                "flood_escalation",
                "flood_wait",
                "peer_flood",
                "spambot_parse_unknown",
                "spambot_temp_ban",
            }
            code = (
                "account_restricted"
                if account.cooldown_reason in temporary_reasons
                else "account_dead"
                if account.status == AccountStatus.DISABLED
                else "account_paused"
            )
            exc = ConflictError("выбранный аккаунт не активен")
            exc.code = code  # type: ignore[attr-defined]
            raise exc
        if not self._settings.dry_run and (
            not account.authorized
            or not account.session_encrypted
            or account.api_id is None
            or not account.api_hash_encrypted
        ):
            exc = ConflictError("у аккаунта нет готовой Telegram-сессии")
            exc.code = "account_unauthorized"  # type: ignore[attr-defined]
            raise exc

    @staticmethod
    def _normalize_name(value: str) -> str:
        normalized = " ".join(value.split())
        if not 1 <= len(normalized) <= 150:
            raise ConflictError("название должно содержать от 1 до 150 символов")
        return normalized

    @classmethod
    def _batch_rule_names(cls, base_name: str, count: int) -> list[str]:
        """Keep rule names unique while preserving the operator's base name."""
        if count == 1:
            return [base_name]
        names: list[str] = []
        for index in range(1, count + 1):
            suffix = f" · {index}"
            prefix = base_name[: 150 - len(suffix)].rstrip()
            names.append(cls._normalize_name(f"{prefix}{suffix}"))
        return names

    @staticmethod
    def _normalize_channel_ref(value: str) -> str:
        normalized = value.strip().rstrip("/")
        lowered = normalized.casefold()
        public_prefixes = (
            "https://t.me/",
            "http://t.me/",
            "https://www.t.me/",
            "http://www.t.me/",
            "https://telegram.me/",
            "http://telegram.me/",
            "https://www.telegram.me/",
            "http://www.telegram.me/",
            "t.me/",
            "www.t.me/",
            "telegram.me/",
            "www.telegram.me/",
        )
        prefix = next((item for item in public_prefixes if lowered.startswith(item)), None)
        if prefix is not None:
            path = normalized[len(prefix) :].split("?", 1)[0].split("#", 1)[0]
            parts = [part for part in path.split("/") if part]
            if parts and parts[0].casefold() == "s":
                # Telegram's preview links use /s/<username>/<post-id>.
                parts = parts[1:]
            if (
                parts
                and parts[0].casefold()
                not in {
                    "+",
                    "c",
                    "joinchat",
                    "addlist",
                }
                and not parts[0].startswith("+")
            ):
                normalized = "@" + parts[0]
            elif parts and parts[0].casefold() == "joinchat" and len(parts) == 2:
                normalized = "https://t.me/+" + parts[1]
            elif parts and parts[0].startswith("+") and len(parts) == 1:
                normalized = "https://t.me/" + parts[0]
            else:
                raise ConflictError(
                    "укажите публичную ссылку канала или действующую приватную invite-ссылку"
                )
        if normalized.startswith("https://t.me/+"):
            invite_hash = normalized.removeprefix("https://t.me/+").strip()
            if not invite_hash or "/" in invite_hash or len(invite_hash) > 200:
                raise ConflictError("приватная invite-ссылка указана неверно")
            return "https://t.me/+" + invite_hash
        if not normalized.startswith("@") or not 2 <= len(normalized) <= 33:
            raise ConflictError("укажите публичную ссылку канала в формате @channel")
        if not normalized[1:].isascii() or not normalized[1:].replace("_", "").isalnum():
            raise ConflictError("ссылка канала содержит недопустимые символы")
        return normalized.lower()

    @staticmethod
    def _normalize_text(
        value: str,
        html_text: str | None = None,
        *,
        allow_empty: bool = False,
        has_media: bool = False,
    ) -> str:
        normalized = value.strip()
        try:
            validate_telegram_text(
                normalized,
                html_text,
                has_media=has_media,
                allow_empty=allow_empty,
            )
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        return normalized

    @staticmethod
    def _normalize_delay(value: int) -> int:
        if not 0 <= int(value) <= 24 * 3600:
            raise ConflictError("задержка должна быть от 0 до 24 часов")
        return int(value)

    @staticmethod
    def _normalize_message_edit_delay(value: int) -> int:
        seconds = int(value)
        if not 5 * 60 <= seconds <= 30 * 60:
            raise ConflictError("задержка редактирования должна быть от 5 до 30 минут")
        return seconds

    @staticmethod
    def _normalize_mention_cooldown_days(value: int) -> int:
        days = int(value)
        if not 1 <= days <= 365:
            raise ConflictError("повторная отметка: укажите от 1 до 365 дней")
        return days

    @staticmethod
    def _rule_steps(rule: ChannelCommentRule) -> list[DialogueStep]:
        if rule.mode == ChannelCommentMode.SINGLE:
            return [
                DialogueStep(
                    account_id=rule.primary_account_id,
                    text=rule.primary_text,
                    html_text=rule.primary_html,
                    media_path=rule.primary_media_path,
                    media_kind=rule.primary_media_kind,
                    media_name=rule.primary_media_name,
                    media_size=rule.primary_media_size,
                )
            ]
        if rule.mode == ChannelCommentMode.MESSAGE_EDIT:
            if not rule.message_edit_initial_text or rule.message_edit_delay_seconds is None:
                raise ConflictError("настройка редактирования комментария повреждена")
            edit_delay = int(rule.message_edit_delay_seconds)
            if not 5 * 60 <= edit_delay <= 30 * 60:
                raise ConflictError("настройка редактирования содержит неверную задержку")
            return [
                DialogueStep(
                    account_id=rule.primary_account_id,
                    text=rule.message_edit_initial_text,
                    html_text=rule.message_edit_initial_html,
                ),
                DialogueStep(
                    account_id=rule.primary_account_id,
                    text=rule.primary_text,
                    html_text=rule.primary_html,
                    delay_seconds=edit_delay,
                ),
            ]
        try:
            return parse_dialogue_steps(
                rule.dialogue_steps_json,
                primary_account_id=rule.primary_account_id,
                primary_text=rule.primary_text,
                reply_account_id=None,
                reply_text="",
                reply_delay_seconds=0,
            )
        except ValueError as exc:
            raise ConflictError("цепочка комментариев повреждена") from exc
