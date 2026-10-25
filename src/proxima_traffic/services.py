from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypeVar
from uuid import uuid4

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from proxima_traffic.content import (
    BroadcastContentVariant,
    ContentError,
    parse_content_variants,
    render_spintax,
    serialize_content_variants,
    validate_spintax,
)
from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.dialogues import (
    DialogueStep,
    parse_dialogue_steps,
    serialize_dialogue_steps,
)
from proxima_traffic.failure_policy import (
    ACCOUNT_FATAL_CODES,
    ACCOUNT_TARGET_BLOCK_CODES,
    ACCOUNT_TEMPORARY_CODES,
    CAMPAIGN_NON_RETRYABLE_CODES,
    OBSERVED_TARGET_FAILURE_CODES,
    failure_code,
)
from proxima_traffic.gateways import (
    GatewayError,
    MembershipGateRecovery,
    MessageGateway,
    PermanentGatewayError,
    SentMessage,
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
    AccountSendReceipt,
    AccountStatus,
    ActionKind,
    ActionLog,
    ActionStatus,
    AuditEvent,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMentionState,
    CampaignMode,
    CampaignRun,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    MembershipStatus,
    ParticipantReactionRule,
    ParticipantReactionTask,
    ParticipantReactionTaskStatus,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    RunStep,
    Subscriber,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    Target,
    TargetAccountState,
    TargetListItem,
    ensure_utc,
    utc_now,
)
from proxima_traffic.scheduling import AccountCapacitySlot
from proxima_traffic.worker_supervision import WorkerIterationReporter

if TYPE_CHECKING:
    from proxima_traffic.proxies import ProxyService
    from proxima_traffic.spambot_checks import SpamBotCheckService

logger = logging.getLogger(__name__)
T = TypeVar("T")

TARGET_QUALITY_BAD = "bad"
TARGET_QUALITY_LEARNING = "learning"
TARGET_QUALITY_WORKING = "working"
TARGET_QUALITY_FAILURE_CODES = frozenset(
    {
        "account_banned_in_chat",
        "post_deleted_by_moderation",
        "target_unavailable",
    }
)


class DomainError(Exception):
    pass


class NotFoundError(DomainError):
    pass


class ConflictError(DomainError):
    pass


def _launch_membership(anchor: CampaignRun) -> object:
    """Select one launch without colliding with a legacy request key."""
    if anchor.launch_key is not None:
        return CampaignRun.launch_key == anchor.launch_key
    return CampaignRun.launch_key.is_(None) & (CampaignRun.request_key == anchor.request_key)


class RunCancelled(DomainError, PermanentGatewayError):
    def __init__(self, message: str) -> None:
        PermanentGatewayError.__init__(self, message, code="cancelled_before_send")


class LeaseLostError(ConflictError):
    pass


class AccountSendDeferred(DomainError):
    """The account's atomic cross-process send slot is not available yet."""

    def __init__(self, resume_at: datetime) -> None:
        super().__init__("account send slot is not available")
        self.resume_at = resume_at


_CREDENTIAL_URL = re.compile(r"(://[^\s:/@]+:)[^\s@]+(@)")


def safe_error_message(error: BaseException) -> str:
    if isinstance(error, GatewayError):
        raw = error.safe_message
    else:
        raw = str(error) or type(error).__name__
    return _CREDENTIAL_URL.sub(r"\1***\2", raw).replace("\x00", "")[:2000]


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    worker_id: str
    running: bool
    active_steps: int
    processed_steps: int
    failed_steps: int
    last_error: str | None
    last_processed_at: str | None


@dataclass(frozen=True, slots=True)
class GlobalStopSummary:
    """A truthful result for the operator's cross-queue emergency stop."""

    campaign_runs: int = 0
    campaign_reactions: int = 0
    channel_comment_rules_paused: int = 0
    channel_comment_runs: int = 0
    participant_reaction_rules_paused: int = 0
    participant_reaction_tasks: int = 0
    direct_launches: int = 0
    direct_message_tasks: int = 0
    subscriber_invite_launches: int = 0
    subscriber_invite_tasks: int = 0

    @property
    def task_count(self) -> int:
        """Number of concrete deliveries/tasks that were cancelled or signalled."""

        return (
            self.campaign_runs
            + self.campaign_reactions
            + self.channel_comment_runs
            + self.participant_reaction_tasks
            + self.direct_message_tasks
            + self.subscriber_invite_tasks
        )

    @property
    def rule_count(self) -> int:
        return self.channel_comment_rules_paused + self.participant_reaction_rules_paused

    @property
    def launch_count(self) -> int:
        return self.direct_launches + self.subscriber_invite_launches


@dataclass(frozen=True, slots=True)
class CampaignDeliveryStopSummary:
    """Result of stopping only chat-campaign deliveries from the campaigns screen."""

    queued_runs: int = 0
    running_runs: int = 0
    queued_reactions: int = 0
    running_reactions: int = 0

    @property
    def run_count(self) -> int:
        return self.queued_runs + self.running_runs

    @property
    def reaction_count(self) -> int:
        return self.queued_reactions + self.running_reactions

    @property
    def task_count(self) -> int:
        return self.run_count + self.reaction_count


def add_audit_event(
    session: AsyncSession,
    *,
    event_type: str,
    entity_type: str,
    entity_id: int | str | None,
    details: dict[str, object] | None = None,
    actor: str = "admin-api",
) -> None:
    session.add(
        AuditEvent(
            actor=actor,
            event_type=event_type,
            entity_type=entity_type,
            entity_id=None if entity_id is None else str(entity_id),
            details_json=json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        )
    )


async def release_campaign_mention_reservations(
    session: AsyncSession,
    *,
    run_ids: list[int],
    keep_user_ids: tuple[int, ...] = (),
) -> None:
    """Release only the mention reservations owned by the supplied runs."""

    if not run_ids:
        return
    query = update(CampaignMentionState).where(CampaignMentionState.reserved_run_id.in_(run_ids))
    if keep_user_ids:
        query = query.where(CampaignMentionState.telegram_user_id.not_in(keep_user_ids))
    await session.execute(query.values(reserved_run_id=None, reserved_until=None))


async def hold_uncertain_campaign_mention_reservations(
    session: AsyncSession,
    run: CampaignRun,
    *,
    now: datetime,
) -> None:
    """Conservatively enforce cooldown when Telegram delivery outcome is unknown."""

    cooldown_days = max(1, min(365, int(run.mention_cooldown_days_snapshot or 30)))
    await session.execute(
        update(CampaignMentionState)
        .where(CampaignMentionState.reserved_run_id == run.id)
        .values(
            reserved_until=now + timedelta(days=cooldown_days),
            updated_at=now,
        )
    )


class CampaignRunner:
    """Executes exactly one durable step of a leased campaign run."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        *,
        settings: object | None = None,
        spambot_checks: SpamBotCheckService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._spambot_checks = spambot_checks
        self._account_locks: dict[int, asyncio.Lock] = {}
        self._account_locks_guard = asyncio.Lock()
        self._target_locks: dict[int, asyncio.Lock] = {}
        self._target_locks_guard = asyncio.Lock()
        self._send_reservation: ContextVar[tuple[int, str] | None] = ContextVar(
            f"proxima-send-reservation-{id(self)}",
            default=None,
        )
        self._send_attempt_started: ContextVar[bool] = ContextVar(
            f"proxima-send-attempt-{id(self)}", default=False
        )
        self._other_account_action_started: ContextVar[bool] = ContextVar(
            f"proxima-other-account-action-{id(self)}", default=False
        )

    def _cfg(self, name: str, default: float | int) -> float | int:
        if self._settings is None:
            return default
        return getattr(self._settings, name, default)

    @property
    def _pace_min_seconds(self) -> float:
        return max(0.0, float(self._cfg("account_pace_min_seconds", 60.0)))

    @property
    def _pace_max_seconds(self) -> float:
        return max(self._pace_min_seconds, float(self._cfg("account_pace_max_seconds", 600.0)))

    @property
    def _account_auto_join(self) -> bool:
        """Whether a known not-yet-member route may be retried through auto-join."""

        return bool(self._cfg("account_auto_join", False))

    async def process_step(self, run_id: int, lease_token: str) -> CampaignRun:
        run, campaign, target, sender, _ = await self._load_context(run_id, lease_token)
        if run.cancel_requested:
            raise RunCancelled("cancellation requested")

        async def deliver_current() -> CampaignRun:
            latest_run, latest_campaign, latest_target, latest_sender, _ = await self._load_context(
                run_id, lease_token
            )
            if latest_run.cancel_requested:
                raise RunCancelled("cancellation requested before sending")
            if latest_sender.id != sender.id:
                raise LeaseLostError("Отправитель изменился во время ожидания блокировки")
            if latest_campaign.mode == CampaignMode.BROADCAST:
                return await self._send_broadcast(
                    latest_run, latest_campaign, latest_sender, latest_target
                )
            return await self._send_dialogue_step(
                latest_run, latest_campaign, latest_sender, latest_target
            )

        try:
            if campaign.mode == CampaignMode.BROADCAST:
                if self._is_smart_run(run):
                    return await self._run_with_broadcast_locks(
                        int(sender.id),
                        int(target.id),
                        deliver_current,
                    )
                return await self._run_with_account_lock(
                    int(sender.id),
                    deliver_current,
                    track_send_attempt=True,
                )
            if self._is_smart_run(run):
                return await self._run_with_broadcast_locks(
                    int(sender.id),
                    int(target.id),
                    deliver_current,
                )
            return await self._run_with_account_lock(
                int(sender.id),
                deliver_current,
                track_send_attempt=True,
            )
        except AccountSendDeferred as exc:
            async with self._session_factory() as session:
                deferred = await self._defer_run(
                    session,
                    run,
                    exc.resume_at,
                    reason="account_pacing",
                )
            if deferred is None:
                raise RunCancelled("cancellation requested") from exc
            return deferred

    async def _run_with_account_lock(
        self,
        account_id: int,
        operation: Callable[[], Awaitable[T]],
        *,
        reserve_send: bool = True,
        track_send_attempt: bool = False,
    ) -> T:
        """Serialize one process's sends per account so pacing/cooldowns are rechecked."""
        async with self._account_locks_guard:
            lock = self._account_locks.get(account_id)
            if lock is None:
                lock = asyncio.Lock()
                self._account_locks[account_id] = lock
        async with lock:
            if not reserve_send:
                return await operation()
            async with self._account_send_slot(account_id, track_send_attempt=track_send_attempt):
                return await operation()

    async def run_account_operation(
        self,
        account_id: int,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        """Share campaign account serialization with other durable delivery flows."""
        return await self._run_with_account_lock(account_id, operation, reserve_send=False)

    async def run_account_send_operation(
        self,
        account_id: int,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        """Atomically reserve pacing for a non-campaign Telegram send."""

        return await self._run_with_account_lock(account_id, operation, reserve_send=True)

    @property
    def _send_reservation_seconds(self) -> float:
        step_timeout = float(self._cfg("worker_step_timeout_seconds", 180.0))
        heartbeat = float(self._cfg("worker_heartbeat_seconds", 15.0))
        return max(120.0, step_timeout + heartbeat * 2)

    @asynccontextmanager
    async def _account_send_slot(self, account_id: int, *, track_send_attempt: bool = False):
        token, resume_at = await self._claim_account_send_slot(account_id)
        if token is None:
            raise AccountSendDeferred(resume_at or utc_now() + timedelta(seconds=1))
        context_token = self._send_reservation.set((account_id, token))
        # Campaigns can finish this step by waiting for chat activity. Only an
        # outbound attempt or membership change should start another interval.
        attempt_token = self._send_attempt_started.set(not track_send_attempt)
        action_token = self._other_account_action_started.set(False)
        heartbeat_stop = asyncio.Event()
        reservation_lost = asyncio.Event()
        owner_task = asyncio.current_task()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_account_send_slot(
                account_id,
                token,
                heartbeat_stop,
                reservation_lost,
                owner_task,
            ),
            name=f"account-send-reservation-{account_id}",
        )
        try:
            yield
            if reservation_lost.is_set():
                raise LeaseLostError("account send reservation was lost")
        except asyncio.CancelledError:
            if reservation_lost.is_set():
                raise LeaseLostError("account send reservation was lost") from None
            raise
        finally:
            pace_account = (
                self._send_attempt_started.get() or self._other_account_action_started.get()
            )
            self._other_account_action_started.reset(action_token)
            self._send_attempt_started.reset(attempt_token)
            self._send_reservation.reset(context_token)
            heartbeat_stop.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._release_account_send_slot(account_id, token, pace_account=pace_account)

    def _mark_account_send_attempt(self, account_id: int) -> None:
        reservation = self._send_reservation.get()
        if reservation is not None and reservation[0] == account_id:
            self._send_attempt_started.set(True)

    async def _claim_account_send_slot(
        self,
        account_id: int,
    ) -> tuple[str | None, datetime | None]:
        token = uuid4().hex
        for _ in range(2):
            now = utc_now()
            reservation_until = now + timedelta(seconds=self._send_reservation_seconds)
            async with self._session_factory() as session:
                claimed = await session.execute(
                    update(Account)
                    .where(
                        Account.id == account_id,
                        or_(
                            Account.send_reservation_token.is_(None),
                            Account.send_reservation_until.is_(None),
                            Account.send_reservation_until <= now,
                        ),
                    )
                    .values(
                        send_reservation_token=token,
                        send_reservation_until=reservation_until,
                    )
                )
                await session.commit()
                if claimed.rowcount == 1:
                    account = await session.get(Account, account_id)
                    if account is None:
                        raise ConflictError("account is missing")
                    resume_at = self._account_resume_at(account)
                    if resume_at is None:
                        return token, None
                    await session.execute(
                        update(Account)
                        .where(
                            Account.id == account_id,
                            Account.send_reservation_token == token,
                        )
                        .values(
                            send_reservation_token=None,
                            send_reservation_until=None,
                        )
                    )
                    await session.commit()
                    return None, resume_at
                account = await session.get(Account, account_id)
                if account is None:
                    raise ConflictError("account is missing")
                active_until = ensure_utc(account.send_reservation_until)
                if active_until is not None and active_until > now:
                    account_resume_at = self._account_resume_at(account)
                    return None, max(
                        value for value in (active_until, account_resume_at) if value is not None
                    )
        return None, utc_now() + timedelta(seconds=1)

    async def _heartbeat_account_send_slot(
        self,
        account_id: int,
        token: str,
        stop_event: asyncio.Event,
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
    ) -> None:
        interval = max(1.0, float(self._cfg("worker_heartbeat_seconds", 15.0)))
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await session.execute(
                        update(Account)
                        .where(
                            Account.id == account_id,
                            Account.send_reservation_token == token,
                        )
                        .values(
                            send_reservation_until=utc_now()
                            + timedelta(seconds=self._send_reservation_seconds)
                        )
                    )
                    # The renewal must be committed while the session is still
                    # open.  Committing after the context manager exits rolls
                    # the update back on normal AsyncSession cleanup, allowing
                    # another worker to reclaim an active account mid-send.
                    await session.commit()
                if renewed.rowcount != 1:
                    self._signal_lost_reservation(lost_event, owner_task)
                    return
            except Exception:  # noqa: BLE001 - expiry permits another worker to recover
                logger.exception("Account send reservation heartbeat failed for %s", account_id)
                self._signal_lost_reservation(lost_event, owner_task)
                return

    @staticmethod
    def _signal_lost_reservation(
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
    ) -> None:
        lost_event.set()
        if owner_task is not None and not owner_task.done():
            owner_task.cancel()

    async def _release_account_send_slot(
        self, account_id: int, token: str, *, pace_account: bool = True
    ) -> None:
        now = utc_now()
        values: dict[str, object] = {
            "send_reservation_token": None,
            "send_reservation_until": None,
        }
        if pace_account and self._pacing_enabled:
            # Failed attempts also contact Telegram. Keep the next recipient
            # behind the same account-wide interval without counting a delivery.
            values["next_send_at"] = case(
                (
                    or_(Account.next_send_at.is_(None), Account.next_send_at <= now),
                    now + timedelta(seconds=self._pace_delay_seconds()),
                ),
                else_=Account.next_send_at,
            )
        async with self._session_factory() as session:
            await session.execute(
                update(Account)
                .where(
                    Account.id == account_id,
                    Account.send_reservation_token == token,
                )
                .values(**values)
            )
            await session.commit()

    def account_resume_at(self, account: Account) -> datetime | None:
        return self._account_resume_at(account)

    async def record_external_account_send(
        self,
        account_id: int,
        *,
        operation_key: str | None = None,
        keep_reservation: bool = True,
    ) -> bool:
        """Count a secondary Telegram action once under daily pacing rules."""
        async with self._session_factory() as session:
            recorded = await self._record_account_send_once(
                session,
                account_id,
                operation_key=operation_key,
                release_reservation=not keep_reservation,
            )
            await session.commit()
            return recorded

    async def _record_account_send_once(
        self,
        session: AsyncSession,
        account_id: int,
        *,
        operation_key: str | None,
        release_reservation: bool = False,
    ) -> bool:
        """Commit quota and its idempotency receipt in the caller's transaction."""
        if operation_key:
            inserted = await session.execute(
                insert(AccountSendReceipt)
                .values(
                    operation_key_hash=hashlib.sha256(operation_key.encode("utf-8")).hexdigest(),
                    account_id=account_id,
                    created_at=utc_now(),
                )
                .on_conflict_do_nothing(index_elements=[AccountSendReceipt.operation_key_hash])
            )
            if inserted.rowcount != 1:
                return False
        await self._record_account_send(
            session, account_id, release_reservation=release_reservation
        )
        return True

    async def handle_external_account_failure(self, account_id: int, exc: BaseException) -> None:
        await self._handle_account_failure(account_id, exc)

    async def record_observed_target_failure(
        self,
        account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> None:
        """Persist a target-specific problem found outside a send attempt."""
        await self._record_target_failure(account_id, target_id, exc)

    async def handle_observed_gateway_failure(
        self,
        account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> None:
        """Apply the same safety policy to read/probe failures as to sends."""
        code = failure_code(exc, "")
        if code in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
            await self._handle_account_failure(account_id, exc)
        if code in OBSERVED_TARGET_FAILURE_CODES:
            await self._record_target_failure(account_id, target_id, exc)

    async def record_observed_post_deletion(
        self,
        *,
        account_id: int,
        target_id: int,
        message_id: int,
        exc: BaseException,
    ) -> None:
        """Persist a later moderation deletion detected by a delivery monitor."""
        await self._record_detected_post_deletion(
            account_id=account_id,
            target_id=target_id,
            message_id=message_id,
            exc=exc,
        )

    async def recover_delayed_membership_gate(
        self,
        *,
        action_id: int,
        delayed_delivery_token: str,
        account_id: int,
        target_id: int,
        message_id: int,
        exc: BaseException,
    ) -> bool:
        """Recover one late moderator subscription gate and reopen its exact step.

        The delayed monitor only acts while the deleted action is still the
        newest successful step of its run.  That avoids replaying a message
        below a later dialogue reply or a completed newer publication.
        """

        async with self._session_factory() as session:
            action = await session.get(ActionLog, action_id)
            if (
                action is None
                or action.status != ActionStatus.SUCCEEDED
                or action.external_message_id != message_id
                or action.delayed_delivery_lease_token != delayed_delivery_token
            ):
                return False
            run = await session.get(CampaignRun, action.run_id)
            campaign = await session.get(Campaign, run.campaign_id) if run is not None else None
            if (
                run is None
                or campaign is None
                or not campaign.enabled
                or run.membership_gate_retry_count >= 1
                or run.cancel_requested
                or run.status in {RunStatus.RUNNING, RunStatus.CANCELLED, RunStatus.FAILED}
                or self._repeat_window_expired(run)
            ):
                return False
            newer_cycle = await session.scalar(
                select(CampaignRun.id)
                .where(
                    _launch_membership(run),
                    CampaignRun.target_id == run.target_id,
                    CampaignRun.cycle_index > run.cycle_index,
                )
                .limit(1)
            )
            if newer_cycle is not None:
                return False
            has_later_step = await session.scalar(
                select(ActionLog.id)
                .where(
                    ActionLog.run_id == action.run_id,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.id > action.id,
                )
                .limit(1)
            )
            if has_later_step is not None:
                return False

        recovery = await self._recover_membership_gate(run, account_id, target_id, exc)
        if recovery is None or not recovery.detected or not recovery.recovered:
            return False

        retry_at = utc_now() + timedelta(
            seconds=int(self._cfg("membership_gate_retry_delay_seconds", 300))
        )
        async with self._session_factory() as session:
            action = await session.get(ActionLog, action_id)
            run = await session.get(CampaignRun, action.run_id) if action is not None else None
            if (
                action is None
                or run is None
                or action.status != ActionStatus.SUCCEEDED
                or action.external_message_id != message_id
                or action.delayed_delivery_lease_token != delayed_delivery_token
                or run.membership_gate_retry_count >= 1
                or run.cancel_requested
                or run.status in {RunStatus.RUNNING, RunStatus.CANCELLED, RunStatus.FAILED}
                or self._repeat_window_expired(run)
            ):
                return False
            newer_cycle = await session.scalar(
                select(CampaignRun.id)
                .where(
                    _launch_membership(run),
                    CampaignRun.target_id == run.target_id,
                    CampaignRun.cycle_index > run.cycle_index,
                )
                .limit(1)
            )
            if newer_cycle is not None:
                return False
            has_later_step = await session.scalar(
                select(ActionLog.id)
                .where(
                    ActionLog.run_id == action.run_id,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.id > action.id,
                )
                .limit(1)
            )
            campaign = await session.get(Campaign, run.campaign_id)
            if has_later_step is not None or campaign is None or not campaign.enabled:
                return False
            if campaign.mode == CampaignMode.BROADCAST:
                if (
                    action.kind != ActionKind.PRIMARY_MESSAGE
                    or run.primary_message_id != message_id
                ):
                    return False
                run.primary_message_id = None
                run.next_step = RunStep.PRIMARY
            else:
                retry_step_index = int(run.dialogue_step_index) - 1
                if retry_step_index < 0 or run.dialogue_last_message_id != message_id:
                    return False
                run.dialogue_step_index = retry_step_index
                run.dialogue_last_message_id = action.reply_to_message_id
                run.next_step = RunStep.PRIMARY if retry_step_index == 0 else RunStep.REPLY
                if retry_step_index == 0:
                    run.primary_message_id = None
                elif retry_step_index == 1:
                    run.reply_message_id = None

            action.delayed_delivery_status = "missing"
            action.delayed_delivery_error_code = "post_deleted_by_moderation"
            action.status = ActionStatus.FAILED
            action.error_code = "membership_gate_recovered"
            action.error = (
                "subscription gate resolved after delayed delivery verification: "
                f"{recovery.notice or 'Telegram inline buttons'}"
            )[:2000]
            action.delayed_delivery_checked_at = utc_now()
            action.delayed_delivery_retry_at = None
            action.delayed_delivery_lease_owner = None
            action.delayed_delivery_lease_token = None
            action.delayed_delivery_lease_until = None
            run.membership_gate_retry_count = 1
            run.status = RunStatus.QUEUED
            run.scheduled_for = retry_at
            run.error = None
            run.error_code = "membership_gate_retry"
            run.finished_at = None
            run.attempt_count = max(0, run.attempt_count - 1)
            self._release_lease(run)
            await self.refresh_target_quality(session, target_id)
            add_audit_event(
                session,
                event_type="run.membership_gate_late_retry_scheduled",
                entity_type="campaign_run",
                entity_id=run.id,
                details={
                    "action_id": action.id,
                    "account_id": account_id,
                    "target_id": target_id,
                    "message_id": message_id,
                    "retry_at": retry_at.isoformat(),
                    "joined_channel_refs": list(recovery.joined_channel_refs),
                    "verification_callback_count": recovery.verification_callback_count,
                },
                actor="delivery-monitor",
            )
            await session.commit()
            return True

    async def _run_with_broadcast_locks(
        self,
        account_id: int,
        target_id: int,
        operation: Callable[[], Awaitable[CampaignRun]],
    ) -> CampaignRun:
        """Serialize the final activity check and send for one destination chat."""
        async with self._target_locks_guard:
            target_lock = self._target_locks.get(target_id)
            if target_lock is None:
                target_lock = asyncio.Lock()
                self._target_locks[target_id] = target_lock
        async with target_lock:
            return await self._run_with_account_lock(account_id, operation, track_send_attempt=True)

    async def _load_context(
        self, run_id: int, lease_token: str
    ) -> tuple[CampaignRun, Campaign, Target, Account, Account | None]:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise NotFoundError("run not found")
            if run.status != RunStatus.RUNNING:
                raise ConflictError("run must be leased before execution")
            if run.lease_token != lease_token:
                raise LeaseLostError("run lease fencing token is no longer current")

            campaign = await session.get(Campaign, run.campaign_id)
            if campaign is None:
                raise ConflictError("campaign is missing")
            gateway_mode = getattr(self._gateway, "simulated", None)
            if isinstance(gateway_mode, bool):
                if run.simulated_snapshot is None:
                    raise RunCancelled("Режим старой задачи не сохранён; создайте новый запуск")
                if run.simulated_snapshot != gateway_mode:
                    raise RunCancelled("Тестовый / реальный режим изменился; нужен новый запуск")
            target = await session.get(Target, run.target_id)
            self._validate_config(campaign, target)
            self._validate_run_payload(run, campaign)
            require_live = self._require_live_session()

            if campaign.mode == CampaignMode.BROADCAST:
                sender_id = run.sender_account_id or campaign.primary_account_id
                sender = await session.get(Account, sender_id)
                if sender is None:
                    raise ConflictError("аккаунт отправителя не найден")
                return run, campaign, target, sender, None

            steps = self._dialogue_steps(run, campaign)
            step_index = self._dialogue_step_index(run, len(steps))
            if step_index >= len(steps):
                raise ConflictError("в цепочке диалога нет ожидающего сообщения")
            sender = await session.get(Account, steps[step_index].account_id)
            self._validate_sender(sender, require_live_session=require_live)
            return run, campaign, target, sender, None

    async def _send_broadcast(
        self,
        run: CampaignRun,
        campaign: Campaign,
        sender: Account,
        target: Target,
    ) -> CampaignRun:
        if self._repeat_window_expired(run):
            return await self._cancel_expired_timer_run(run)
        try:
            self._validate_sender(sender, require_live_session=self._require_live_session())
        except ConflictError as exc:
            exc.code = "sender_unavailable"
            rerouted = await self._reroute_broadcast_sender(
                run,
                campaign,
                sender.id,
                target.id,
                exc,
            )
            if rerouted is not None:
                return rerouted
            raise
        content_key = f"{run.request_key}:primary"
        idempotency_key = self._transport_idempotency_key(run, content_key)
        primary_text = self._render_delivery_text(
            self._primary_text(run, campaign),
            idempotency_key=content_key,
        )
        deferred = await self._defer_if_unavailable(run, sender.id)
        if deferred is not None:
            return deferred
        try:
            await self._validate_broadcast_route(sender.id, target.id)
            prepared = await self._prepare_broadcast_target(run, sender, target)
            if prepared is not None:
                return prepared
            visibility_deferred = await self._defer_for_visibility(run, campaign, sender, target)
            if visibility_deferred is not None:
                return visibility_deferred
            sent = await self._send_broadcast_content(
                run,
                campaign,
                sender,
                target,
                idempotency_key=idempotency_key,
                content_render_key=content_key,
                text=primary_text,
            )
        except Exception as exc:
            if (
                failure_code(exc, "") == "post_deleted_by_moderation"
                and not self._send_attempt_started.get()
            ):
                # Activity inspection observed an older publication disappearing.
                # Its original action was already updated; this step never sent.
                raise
            refreshed_membership = await self._retry_stale_auto_join_membership(
                run=run,
                account_id=sender.id,
                target=target,
                exc=exc,
            )
            if refreshed_membership is not None:
                return refreshed_membership
            # A join request belongs to the chosen account.  Failing over here
            # would submit the same private-chat request from every sender,
            # while the first account is simply waiting for an administrator.
            if getattr(exc, "code", None) != "join_request_pending":
                rerouted = await self._reroute_broadcast_sender(
                    run, campaign, sender.id, target.id, exc
                )
                if rerouted is not None:
                    return rerouted
            handled = await self._on_send_failure(
                run,
                sender.id,
                target.id,
                ActionKind.PRIMARY_MESSAGE,
                primary_text,
                None,
                exc,
            )
            if handled is not None:
                return handled
            raise

        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            if sent.message_id <= 0:
                raise PermanentGatewayError(
                    "Нет подтверждённого ID сообщения", code="delivery_unconfirmed"
                )
            stored.dispatch_started_at = None
            stored.dispatch_account_id = None
            stored.dispatch_idempotency_key = None
            stored.simulated_snapshot = sent.simulated
            completed_at = utc_now()
            stored.primary_message_id = sent.message_id
            stored.status = RunStatus.CANCELLED if stored.cancel_requested else RunStatus.SUCCEEDED
            stored.error = None
            stored.error_code = None
            stored.finished_at = completed_at
            if sent.simulated is not True:
                await self._finalize_broadcast_hidden_mentions(
                    session, stored, delivered_at=completed_at
                )
            else:
                await self._release_broadcast_mention_reservations(session, run_ids=[stored.id])
            self._release_lease(stored)
            if sent.simulated is not True:
                await self._record_account_send_once(
                    session, sender.id, operation_key=self._campaign_step_key(run)
                )
            if sent.simulated is not True:
                await self._record_target_success(
                    session, target.id, account_id=sender.id, message_id=sent.message_id
                )
            await self._schedule_next_timer_repeat(
                session,
                stored,
                campaign,
                sender.id,
                target.id,
                completed_at,
            )
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=sender.id,
                    target_id=target.id,
                    kind=ActionKind.PRIMARY_MESSAGE,
                    status=ActionStatus.SUCCEEDED,
                    simulated=sent.simulated,
                    external_message_id=sent.message_id,
                    external_message_ids_json=json.dumps(
                        list(getattr(sent, "message_ids", ()) or (sent.message_id,))
                    ),
                    text=primary_text,
                    attempt_number=stored.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            await self._schedule_reactions(
                session, stored, campaign, target, sender.id, sent.message_id, dialogue_step=1
            )
            await self.refresh_target_quality(session, target.id)
            add_audit_event(
                session,
                event_type="run.succeeded",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={"message_id": sent.message_id, "mode": "broadcast"},
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    @staticmethod
    def _repeat_window_expired(run: CampaignRun) -> bool:
        repeat_until = ensure_utc(run.repeat_until)
        return repeat_until is not None and repeat_until <= utc_now()

    @staticmethod
    def _is_smart_run(run: CampaignRun) -> bool:
        """Keep an indefinite smart run distinct from an ordinary one-pass task."""

        return bool(run.repeat_forever_snapshot or ensure_utc(run.repeat_until) is not None)

    async def _cancel_expired_timer_run(self, run: CampaignRun) -> CampaignRun:
        """Finish a timer-driven run whose launch window has ended before sending."""
        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            stored.status = RunStatus.CANCELLED
            stored.error = None
            stored.error_code = "repeat_window_elapsed"
            stored.finished_at = utc_now()
            self._release_lease(stored)
            add_audit_event(
                session,
                event_type="run.timer_window_elapsed",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={
                    "repeat_until": (
                        ensure_utc(stored.repeat_until).isoformat()
                        if ensure_utc(stored.repeat_until) is not None
                        else None
                    )
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    async def _schedule_next_timer_repeat(
        self,
        session: AsyncSession,
        run: CampaignRun,
        campaign: Campaign,
        account_id: int,
        target_id: int,
        completed_at: datetime,
    ) -> None:
        """Create one next post per chat, scheduled at its first allowed time."""
        repeat_until = ensure_utc(run.repeat_until)
        repeat_forever = bool(run.repeat_forever_snapshot)
        if (repeat_until is None and not repeat_forever) or run.cancel_requested:
            return

        no_timer_interval = (
            run.no_timer_interval_seconds_snapshot
            if run.no_timer_interval_seconds_snapshot is not None
            else campaign.smart_no_timer_interval_seconds
        )
        if campaign.mode == CampaignMode.DIALOGUE:
            await self._schedule_next_dialogue_repeat(
                session,
                run,
                campaign,
                target_id,
                completed_at,
                repeat_until,
                repeat_forever,
                no_timer_interval,
            )
            return
        sender_pool = [account_id]
        if run.sender_pool_snapshot_json:
            try:
                sender_pool = list(
                    dict.fromkeys(int(item) for item in json.loads(run.sender_pool_snapshot_json))
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                sender_pool = [account_id]
        timing_rows = list(
            await session.scalars(
                select(TargetAccountState).where(
                    TargetAccountState.account_id.in_(sender_pool),
                    TargetAccountState.target_id == target_id,
                )
            )
        )
        timing_by_account = {row.account_id: row for row in timing_rows}
        candidates: list[tuple[datetime, int, int]] = []
        sticky_pool = [account_id, *(item for item in sender_pool if item != account_id)]
        for order, candidate_id in enumerate(sticky_pool):
            candidate = await session.get(Account, candidate_id)
            try:
                self._validate_sender(
                    candidate,
                    require_live_session=self._require_live_session(),
                )
            except ConflictError:
                continue
            timing = timing_by_account.get(candidate_id)
            if not self._broadcast_route_is_eligible(timing):
                continue
            known_next_send = ensure_utc(timing.next_send_at) if timing is not None else None
            activity_aware = run.activity_aware_snapshot is True or (
                run.activity_aware_snapshot is None and campaign.activity_aware
            )
            if known_next_send is not None and known_next_send > completed_at:
                candidate_time = known_next_send
            elif (
                activity_aware
                and timing is not None
                and timing.adaptive_interval_seconds is not None
            ):
                learned_interval = max(60, int(timing.adaptive_interval_seconds))
                visibility_limit = (
                    run.visibility_max_wait_seconds_snapshot
                    if run.visibility_max_wait_seconds_snapshot is not None
                    else campaign.visibility_max_wait_seconds
                )
                if visibility_limit > 0:
                    learned_interval = min(learned_interval, visibility_limit)
                candidate_time = completed_at + timedelta(seconds=learned_interval)
            elif activity_aware:
                # Do not wait for the broad fallback interval before taking the
                # first sample. A busy chat can satisfy the depth threshold in a
                # minute, and the maximum visibility wait is a hard deadline.
                first_probe_interval = min(
                    no_timer_interval,
                    (
                        run.activity_check_interval_seconds_snapshot
                        if run.activity_check_interval_seconds_snapshot is not None
                        else campaign.activity_check_interval_seconds
                    ),
                )
                visibility_limit = (
                    run.visibility_max_wait_seconds_snapshot
                    if run.visibility_max_wait_seconds_snapshot is not None
                    else campaign.visibility_max_wait_seconds
                )
                if visibility_limit > 0:
                    first_probe_interval = min(first_probe_interval, visibility_limit)
                candidate_time = completed_at + timedelta(seconds=max(30, first_probe_interval))
            else:
                candidate_time = completed_at + timedelta(seconds=no_timer_interval)
            account_resume_at = self._account_resume_at(candidate)
            if account_resume_at is not None and account_resume_at > candidate_time:
                candidate_time = account_resume_at
            candidates.append((candidate_time, order, candidate_id))
        if not candidates:
            return
        scheduled_for, _, next_account_id = min(
            candidates,
            key=lambda item: (item[0], item[1]),
        )
        if repeat_until is not None and scheduled_for >= repeat_until:
            return

        launch_key = run.launch_key
        if launch_key is None:
            launch_key = run.request_key
            run.launch_key = launch_key
        variants = self._run_content_variants(run, campaign)
        next_variant_index = (run.content_variant_index + 1) % len(variants)
        next_variant = variants[next_variant_index]
        next_media = next_variant.media[0] if next_variant.media else None
        next_run = CampaignRun(
            campaign_id=run.campaign_id,
            simulated_snapshot=run.simulated_snapshot,
            target_id=target_id,
            sender_account_id=next_account_id,
            cycle_index=run.cycle_index + 1,
            request_key=hashlib.sha256(f"{run.request_key}:next".encode()).hexdigest(),
            launch_key=launch_key,
            status=RunStatus.QUEUED,
            next_step=RunStep.PRIMARY,
            max_attempts=run.max_attempts,
            scheduled_for=scheduled_for,
            repeat_until=repeat_until,
            repeat_forever_snapshot=repeat_forever,
            primary_text_snapshot=next_variant.text,
            primary_html_snapshot=next_variant.html_text,
            hidden_mentions_enabled_snapshot=run.hidden_mentions_enabled_snapshot,
            mention_cooldown_days_snapshot=run.mention_cooldown_days_snapshot,
            primary_media_path_snapshot=(next_media.path if next_media else None),
            primary_media_kind_snapshot=(next_media.kind if next_media else None),
            primary_media_name_snapshot=(next_media.name if next_media else None),
            primary_media_size_snapshot=(next_media.size if next_media else None),
            media_items_snapshot_json=self._serialize_media_items(next_variant),
            content_variants_snapshot_json=(
                run.content_variants_snapshot_json or serialize_content_variants(variants)
            ),
            content_variant_index=next_variant_index,
            content_variant_id_snapshot=next_variant.variant_id,
            content_variant_name_snapshot=next_variant.name,
            reply_text_snapshot=run.reply_text_snapshot,
            reply_delay_seconds_snapshot=run.reply_delay_seconds_snapshot,
            no_timer_interval_seconds_snapshot=run.no_timer_interval_seconds_snapshot,
            auto_sender_failover_snapshot=run.auto_sender_failover_snapshot,
            sender_pool_snapshot_json=run.sender_pool_snapshot_json,
            sender_mode_snapshot=run.sender_mode_snapshot,
            activity_aware_snapshot=run.activity_aware_snapshot,
            min_messages_between_posts_snapshot=run.min_messages_between_posts_snapshot,
            activity_check_interval_seconds_snapshot=(run.activity_check_interval_seconds_snapshot),
            visibility_max_wait_seconds_snapshot=run.visibility_max_wait_seconds_snapshot,
        )
        session.add(next_run)
        add_audit_event(
            session,
            event_type="run.timer_repeat_scheduled",
            entity_type="campaign_run",
            entity_id=run.id,
            details={
                "next_cycle": next_run.cycle_index,
                "scheduled_for": scheduled_for.isoformat(),
                "repeat_until": repeat_until.isoformat() if repeat_until is not None else None,
                "repeat_forever": repeat_forever,
                "sender_account_id": next_account_id,
                "content_variant_id": next_variant.variant_id,
                "content_variant_name": next_variant.name,
            },
            actor="worker",
        )

    async def _schedule_next_dialogue_repeat(
        self,
        session: AsyncSession,
        run: CampaignRun,
        campaign: Campaign,
        target_id: int,
        completed_at: datetime,
        repeat_until: datetime | None,
        repeat_forever: bool,
        no_timer_interval: int,
    ) -> None:
        """Schedule the next complete dialogue chain for one destination chat."""
        steps = self._dialogue_steps(run, campaign)
        first_account_id = steps[0].account_id
        first_state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == first_account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        activity_aware = run.activity_aware_snapshot is True or (
            run.activity_aware_snapshot is None and campaign.activity_aware
        )
        known_next_send = ensure_utc(first_state.next_send_at) if first_state is not None else None
        if known_next_send is not None and known_next_send > completed_at:
            scheduled_for = known_next_send
        elif (
            activity_aware
            and first_state is not None
            and first_state.adaptive_interval_seconds is not None
        ):
            learned_interval = max(60, int(first_state.adaptive_interval_seconds))
            visibility_limit = (
                run.visibility_max_wait_seconds_snapshot
                if run.visibility_max_wait_seconds_snapshot is not None
                else campaign.visibility_max_wait_seconds
            )
            if visibility_limit > 0:
                learned_interval = min(learned_interval, visibility_limit)
            scheduled_for = completed_at + timedelta(seconds=learned_interval)
        elif activity_aware:
            first_probe_interval = min(
                no_timer_interval,
                (
                    run.activity_check_interval_seconds_snapshot
                    if run.activity_check_interval_seconds_snapshot is not None
                    else campaign.activity_check_interval_seconds
                ),
            )
            visibility_limit = (
                run.visibility_max_wait_seconds_snapshot
                if run.visibility_max_wait_seconds_snapshot is not None
                else campaign.visibility_max_wait_seconds
            )
            if visibility_limit > 0:
                first_probe_interval = min(first_probe_interval, visibility_limit)
            scheduled_for = completed_at + timedelta(seconds=max(30, first_probe_interval))
        else:
            scheduled_for = completed_at + timedelta(seconds=no_timer_interval)

        # Avoid starting a chain that is already known to stall on account pacing.
        # Every step still rechecks its own account and chat timer immediately before send.
        cumulative_delay = 0
        for index, step in enumerate(steps):
            if index > 0:
                cumulative_delay += step.delay_seconds
            account_id = step.account_id
            account = await session.get(Account, account_id)
            if account is None:
                continue
            resume_at = self._account_resume_at(account)
            pair_state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            pair_resume_at = ensure_utc(pair_state.next_send_at) if pair_state is not None else None
            for ready_at in (resume_at, pair_resume_at):
                if ready_at is None:
                    continue
                required_start = ready_at - timedelta(seconds=cumulative_delay)
                if required_start > scheduled_for:
                    scheduled_for = required_start
        if repeat_until is not None and scheduled_for >= repeat_until:
            return

        launch_key = run.launch_key or run.request_key
        run.launch_key = launch_key
        next_run = CampaignRun(
            campaign_id=run.campaign_id,
            simulated_snapshot=run.simulated_snapshot,
            target_id=target_id,
            sender_account_id=first_account_id,
            cycle_index=run.cycle_index + 1,
            request_key=hashlib.sha256(f"{run.request_key}:next".encode()).hexdigest(),
            launch_key=launch_key,
            status=RunStatus.QUEUED,
            next_step=RunStep.PRIMARY,
            max_attempts=run.max_attempts,
            scheduled_for=scheduled_for,
            repeat_until=repeat_until,
            repeat_forever_snapshot=repeat_forever,
            primary_text_snapshot=run.primary_text_snapshot,
            hidden_mentions_enabled_snapshot=run.hidden_mentions_enabled_snapshot,
            mention_cooldown_days_snapshot=run.mention_cooldown_days_snapshot,
            reply_text_snapshot=run.reply_text_snapshot,
            reply_delay_seconds_snapshot=run.reply_delay_seconds_snapshot,
            no_timer_interval_seconds_snapshot=run.no_timer_interval_seconds_snapshot,
            auto_sender_failover_snapshot=False,
            sender_pool_snapshot_json=run.sender_pool_snapshot_json,
            sender_mode_snapshot=BroadcastSenderMode.DISTRIBUTED,
            activity_aware_snapshot=run.activity_aware_snapshot,
            min_messages_between_posts_snapshot=run.min_messages_between_posts_snapshot,
            activity_check_interval_seconds_snapshot=(run.activity_check_interval_seconds_snapshot),
            visibility_max_wait_seconds_snapshot=run.visibility_max_wait_seconds_snapshot,
            dialogue_steps_snapshot_json=(
                run.dialogue_steps_snapshot_json or serialize_dialogue_steps(steps)
            ),
            dialogue_step_index=0,
        )
        session.add(next_run)
        add_audit_event(
            session,
            event_type="run.dialogue_repeat_scheduled",
            entity_type="campaign_run",
            entity_id=run.id,
            details={
                "next_cycle": next_run.cycle_index,
                "scheduled_for": scheduled_for.isoformat(),
                "repeat_until": repeat_until.isoformat() if repeat_until is not None else None,
                "repeat_forever": repeat_forever,
                "dialogue_steps": len(steps),
                "first_account_id": first_account_id,
            },
            actor="worker",
        )

    async def _reroute_broadcast_sender(
        self,
        run: CampaignRun,
        campaign: Campaign,
        failed_account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> CampaignRun | None:
        """Continue a broadcast with another selected sender after a permanent account loss."""
        code = str(getattr(exc, "code", None) or "")
        if getattr(exc, "external_message_id", None) is not None:
            # A receipt may refer to a post already accepted by Telegram.
            return None
        if (
            code in {"account_restricted", "peer_flood"}
            and float(self._cfg("account_restriction_cooldown_hours", 0.0)) == 0
        ):
            # An explicit refusal without an expiry is terminal for this task;
            # don't bounce it between senders or invent a retry time.
            return None
        auto_sender_failover = (
            run.auto_sender_failover_snapshot
            if run.auto_sender_failover_snapshot is not None
            else campaign.auto_sender_failover
        )
        failover_codes = (
            ACCOUNT_FATAL_CODES
            | ACCOUNT_TARGET_BLOCK_CODES
            | frozenset({"account_restricted", "peer_flood", "sender_unavailable"})
        )
        if not auto_sender_failover or code not in failover_codes:
            return None

        if isinstance(exc, GatewayError):
            await self._clear_campaign_dispatch(run)

        # A chat ban belongs to the pair, not to the whole account. Persist it
        # before selecting a replacement so this and future launches never pick
        # the same broken route again until an operator explicitly unblocks it.
        await self._record_target_failure(failed_account_id, target_id, exc)
        await self._handle_account_failure(failed_account_id, exc)
        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            if stored.cancel_requested:
                return None
            rated_target = await session.get(Target, target_id)
            if rated_target is None or rated_target.quality_status == TARGET_QUALITY_BAD:
                return None
            sender_pool: list[int] = []
            if run.sender_pool_snapshot_json:
                try:
                    raw_sender_pool = json.loads(run.sender_pool_snapshot_json)
                    sender_pool = [int(item) for item in raw_sender_pool]
                except (TypeError, ValueError, json.JSONDecodeError):
                    sender_pool = []
            if not sender_pool:
                sender_pool = list(
                    await session.scalars(
                        select(CampaignAccount.account_id)
                        .where(CampaignAccount.campaign_id == campaign.id)
                        .order_by(CampaignAccount.id)
                    )
                )
            candidate_rows = list(
                await session.scalars(
                    select(Account).where(
                        Account.id.in_(sender_pool),
                        Account.id != failed_account_id,
                    )
                )
            )
            candidates_by_id = {candidate.id: candidate for candidate in candidate_rows}
            candidates = [
                candidates_by_id[account_id]
                for account_id in sender_pool
                if account_id in candidates_by_id
            ]
            replacement: tuple[datetime, Account] | None = None
            for candidate in candidates:
                try:
                    self._validate_sender(
                        candidate,
                        require_live_session=self._require_live_session(),
                    )
                except ConflictError:
                    continue
                scheduled_for = self._account_resume_at(candidate) or utc_now()
                timing = await session.scalar(
                    select(TargetAccountState).where(
                        TargetAccountState.account_id == candidate.id,
                        TargetAccountState.target_id == target_id,
                    )
                )
                if not self._broadcast_route_is_eligible(timing):
                    continue
                known_next_send = ensure_utc(timing.next_send_at) if timing is not None else None
                if known_next_send is not None and known_next_send > scheduled_for:
                    scheduled_for = known_next_send
                if replacement is None or scheduled_for < replacement[0]:
                    replacement = (scheduled_for, candidate)

            if replacement is None:
                return None

            scheduled_for, candidate = replacement
            repeat_until = ensure_utc(stored.repeat_until)
            if repeat_until is not None and scheduled_for >= repeat_until:
                # A replacement that only becomes available after the active
                # window ends is not a real failover.  Do not requeue it at
                # ``repeat_until`` just for the worker to claim and cancel it
                # later: that made active launches look as though they had
                # silently stalled.
                stored.status = RunStatus.CANCELLED
                stored.error = None
                stored.error_code = "repeat_window_elapsed"
                stored.finished_at = utc_now()
                self._release_lease(stored)
                session.add(
                    ActionLog(
                        run_id=stored.id,
                        account_id=failed_account_id,
                        target_id=target_id,
                        kind=ActionKind.PRIMARY_MESSAGE,
                        status=ActionStatus.FAILED,
                        simulated=self._failure_delivery_mode(),
                        error_code=code,
                        text=self._primary_text(run, campaign),
                        error=safe_error_message(exc),
                        attempt_number=run.attempt_count,
                        lease_token=run.lease_token,
                    )
                )
                await self.refresh_target_quality(session, target_id)
                add_audit_event(
                    session,
                    event_type="run.sender_failover_outside_window",
                    entity_type="campaign_run",
                    entity_id=stored.id,
                    details={
                        "error_code": code,
                        "failed_account_id": failed_account_id,
                        "replacement_account_id": candidate.id,
                        "replacement_available_at": scheduled_for.isoformat(),
                        "repeat_until": repeat_until.isoformat(),
                    },
                    actor="worker",
                )
                await session.commit()
                await session.refresh(stored)
                return stored
            stored.sender_account_id = candidate.id
            stored.status = RunStatus.QUEUED
            stored.scheduled_for = scheduled_for
            stored.attempt_count = max(0, stored.attempt_count - 1)
            stored.error = None
            stored.error_code = None
            self._release_lease(stored)
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=failed_account_id,
                    target_id=target_id,
                    kind=ActionKind.PRIMARY_MESSAGE,
                    status=ActionStatus.FAILED,
                    simulated=self._failure_delivery_mode(),
                    error_code=code,
                    text=self._primary_text(run, campaign),
                    error=safe_error_message(exc),
                    attempt_number=run.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            await self.refresh_target_quality(session, target_id)
            add_audit_event(
                session,
                event_type="run.sender_failed_over",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={
                    "error_code": code,
                    "failed_account_id": failed_account_id,
                    "replacement_account_id": candidate.id,
                    "scheduled_for": scheduled_for.isoformat(),
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    async def _retry_stale_auto_join_membership(
        self,
        *,
        run: CampaignRun,
        account_id: int,
        target: Target,
        exc: BaseException,
    ) -> CampaignRun | None:
        """Retry one cached-membership miss through the guarded auto-join path.

        A long-running active campaign keeps resolved Telegram peers cached.
        If Telegram removes an account from a chat after a successful send,
        the cached peer can surface ``USER_NOT_PARTICIPANT`` on the next
        publication.  With auto-join enabled this is recoverable once: clear
        only that cache entry and let the normal, rate-limited join flow run on
        the retry.  A non-cached miss still follows the normal permanent route
        policy, so private/basic chats cannot spin forever.
        """

        if not self._account_auto_join or failure_code(exc, "") != "target_not_joined":
            return None
        invalidator = getattr(self._gateway, "invalidate_target_membership", None)
        if not callable(invalidator):
            return None
        try:
            had_cached_membership = bool(
                await invalidator(account_id=account_id, target=target)
            )
        except Exception:  # noqa: BLE001 - keep the original Telegram error authoritative
            logger.warning(
                "Could not invalidate cached membership for account %s target %s",
                account_id,
                target.id,
                exc_info=True,
            )
            return None
        if not had_cached_membership:
            return None
        async with self._session_factory() as session:
            return await self._defer_run(
                session,
                run,
                utc_now() + timedelta(seconds=30),
                reason="target_not_joined",
            )

    def _broadcast_route_is_eligible(self, state: TargetAccountState | None) -> bool:
        """Keep recurring and failover sends on a route preflight proved usable.

        Smart launches can live for days, so this intentionally does not impose
        the short *fresh preflight* TTL used at initial enqueue. It does however
        honour any later knowledge that a particular account cannot write to the
        particular chat, rather than spending a Telegram request on a known-bad
        pair.
        """

        if state is None:
            return True
        if state.send_blocked:
            return False
        # Older launches and timer-only records do not carry a membership
        # observation. They are not evidence of an unusable route, so retain
        # the historical behaviour for them. A real preflight result is
        # always present in LIVE mode and is handled strictly below.
        if state.membership_checked_at is None:
            return True
        if state.membership_status == MembershipStatus.MEMBER and state.can_send is True:
            return True
        return bool(
            self._account_auto_join
            and state.membership_status == MembershipStatus.NOT_MEMBER
            and state.membership_error_code == "target_not_joined"
        )

    async def _prepare_broadcast_target(
        self, run: CampaignRun, account: Account, target: Target
    ) -> CampaignRun | None:
        preparer = getattr(self._gateway, "prepare_broadcast_target", None)
        if not self._account_auto_join or not callable(preparer):
            return None
        try:
            joined = await preparer(account=account, target=target)
        except GatewayError as exc:
            if failure_code(exc, "") in {"network_error", "rpc_error", "delivery_unconfirmed"}:
                self._other_account_action_started.set(True)
            raise
        if not joined:
            return None
        self._other_account_action_started.set(True)
        now = utc_now()
        async with self._session_factory() as session:
            timing = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account.id,
                    TargetAccountState.target_id == target.id,
                )
            )
            if timing is None:
                timing = TargetAccountState(account_id=account.id, target_id=target.id)
                session.add(timing)
            timing.membership_status = MembershipStatus.MEMBER
            timing.membership_checked_at = now
            timing.membership_error_code = None
            timing.can_send = True
            timing.send_blocked = False
            timing.blocked_reason = None
            timing.last_error = None
            stored_account = await session.get(Account, account.id)
            resume_at = now + timedelta(seconds=self._pace_delay_seconds())
            if stored_account is not None:
                current = ensure_utc(stored_account.next_send_at)
                if current is not None:
                    resume_at = max(resume_at, current)
                stored_account.next_send_at = resume_at
            return await self._defer_run(session, run, resume_at, reason="join_completed")

    async def _validate_broadcast_route(self, account_id: int, target_id: int) -> None:
        """Check queued routes again before reading activity or sending a post."""
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
        if state is None or self._broadcast_route_is_eligible(state):
            return
        # Pending requests need a fresh Telegram check to discover approval.
        if state.membership_status == MembershipStatus.JOIN_PENDING:
            return
        if (
            self._account_auto_join
            and state.membership_status == MembershipStatus.NOT_MEMBER
            and state.blocked_reason in {None, "target_not_joined"}
            and state.membership_error_code in {None, "target_not_joined"}
        ):
            return
        code = state.blocked_reason or state.membership_error_code
        if state.membership_status == MembershipStatus.NOT_MEMBER and not code:
            code = "target_not_joined"
        if code not in ACCOUNT_TARGET_BLOCK_CODES | ACCOUNT_FATAL_CODES | {"target_unavailable"}:
            code = "account_send_forbidden" if state.can_send is False else "target_unavailable"
        raise PermanentGatewayError(
            state.last_error or "сохранённая проверка не подтверждает доступ отправителя к чату",
            code=code,
        )

    async def _send_primary(
        self,
        run: CampaignRun,
        campaign: Campaign,
        primary: Account,
        target: Target,
    ) -> CampaignRun:
        content_key = f"{run.request_key}:primary"
        idempotency_key = self._transport_idempotency_key(run, content_key)
        primary_text = self._render_delivery_text(
            self._primary_text(run, campaign),
            idempotency_key=content_key,
        )
        reply_delay_seconds = self._reply_delay_seconds(run, campaign)
        if run.primary_message_id is not None:
            return await self._schedule_reply(run.id, reply_delay_seconds, run.lease_token)
        deferred = await self._defer_if_unavailable(run, primary.id)
        if deferred is not None:
            return deferred
        try:
            self._mark_account_send_attempt(primary.id)
            sent = await self._gateway.send_message(
                account=primary,
                target=target,
                text=primary_text,
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            handled = await self._on_send_failure(
                run,
                primary.id,
                target.id,
                ActionKind.PRIMARY_MESSAGE,
                primary_text,
                None,
                exc,
            )
            if handled is not None:
                return handled
            raise

        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            stored.primary_message_id = sent.message_id
            if stored.cancel_requested:
                stored.status = RunStatus.CANCELLED
                stored.finished_at = utc_now()
            else:
                stored.next_step = RunStep.REPLY
                stored.status = RunStatus.QUEUED
                stored.scheduled_for = utc_now() + timedelta(seconds=reply_delay_seconds)
            self._release_lease(stored)
            await self._record_account_send(session, primary.id)
            await self._record_target_success(
                session,
                target.id,
                account_id=primary.id,
                message_id=sent.message_id,
            )
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=primary.id,
                    target_id=target.id,
                    kind=ActionKind.PRIMARY_MESSAGE,
                    status=ActionStatus.SUCCEEDED,
                    simulated=sent.simulated,
                    external_message_id=sent.message_id,
                    text=primary_text,
                    attempt_number=stored.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            await self._schedule_reactions(
                session,
                stored,
                campaign,
                target,
                primary.id,
                sent.message_id,
                dialogue_step=1,
            )
            add_audit_event(
                session,
                event_type=(
                    "run.cancelled_after_primary"
                    if stored.status == RunStatus.CANCELLED
                    else "run.primary_succeeded"
                ),
                entity_type="campaign_run",
                entity_id=stored.id,
                details={"message_id": sent.message_id},
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    async def _send_dialogue_step(
        self,
        run: CampaignRun,
        campaign: Campaign,
        sender: Account,
        target: Target,
    ) -> CampaignRun:
        steps = self._dialogue_steps(run, campaign)
        step_index = self._dialogue_step_index(run, len(steps))
        if step_index >= len(steps):
            raise ConflictError("цепочка диалога уже завершена")
        step = steps[step_index]
        if sender.id != step.account_id:
            raise ConflictError("текущий аккаунт не соответствует шагу диалога")
        if step_index == 0 and self._repeat_window_expired(run):
            return await self._cancel_expired_timer_run(run)
        deferred = await self._defer_if_unavailable(run, sender.id)
        if deferred is not None:
            return deferred
        try:
            await self._validate_broadcast_route(sender.id, target.id)
            prepared = await self._prepare_broadcast_target(run, sender, target)
        except GatewayError as exc:
            handled = await self._on_send_failure(
                run,
                sender.id,
                target.id,
                ActionKind.PRIMARY_MESSAGE if step_index == 0 else ActionKind.REPLY_MESSAGE,
                step.text,
                None,
                exc,
            )
            if handled is not None:
                return handled
            raise
        if prepared is not None:
            return prepared
        if step_index == 0:
            try:
                visibility_deferred = await self._defer_for_visibility(
                    run, campaign, sender, target
                )
            except GatewayError as exc:
                await self.handle_observed_gateway_failure(sender.id, target.id, exc)
                raise
            if visibility_deferred is not None:
                return visibility_deferred
        reply_to_message_id = (
            run.dialogue_last_message_id or run.reply_message_id or run.primary_message_id
            if step_index > 0 and step.reply_to_previous
            else None
        )
        if step_index > 0 and step.reply_to_previous and reply_to_message_id is None:
            raise ConflictError("для ответа не найдено предыдущее сообщение")
        if reply_to_message_id is not None:
            await self._ensure_reply_anchor_still_exists(
                account=sender,
                target=target,
                message_id=reply_to_message_id,
            )
        idempotency_suffix = (
            "primary"
            if step_index == 0
            else "reply"
            if step_index == 1
            else f"dialogue:{step_index}"
        )
        kind = ActionKind.PRIMARY_MESSAGE if step_index == 0 else ActionKind.REPLY_MESSAGE
        content_key = f"{run.request_key}:{idempotency_suffix}"
        idempotency_key = self._transport_idempotency_key(run, content_key)
        step_text = self._render_delivery_text(step.text, idempotency_key=content_key)
        step_html = (
            self._render_delivery_text(step.html_text, idempotency_key=content_key)
            if step.html_text is not None
            else None
        )
        try:
            content_sender = getattr(self._gateway, "send_content", None)
            if content_sender is None:
                if step.media_path is not None or step_html is not None:
                    raise ConflictError(
                        "настроенный шлюз не поддерживает форматирование или медиа в цепочках"
                    )
                await self._mark_campaign_dispatch(run, sender.id, target.id, idempotency_key)
                sent = await self._gateway.send_message(
                    account=sender,
                    target=target,
                    text=step_text,
                    idempotency_key=idempotency_key,
                    reply_to_message_id=reply_to_message_id,
                )
            else:
                dispatch_kwargs = await self._campaign_dispatch_kwargs(
                    run, sender.id, target.id, idempotency_key
                )
                sent = await content_sender(
                    account=sender,
                    target=target,
                    text=step_text,
                    idempotency_key=idempotency_key,
                    reply_to_message_id=reply_to_message_id,
                    html_text=step_html,
                    media_path=step.media_path,
                    media_kind=step.media_kind,
                    media_name=step.media_name,
                    **dispatch_kwargs,
                )
        except Exception as exc:
            handled = await self._on_send_failure(
                run,
                sender.id,
                target.id,
                kind,
                step_text,
                reply_to_message_id,
                exc,
            )
            if handled is not None:
                return handled
            raise

        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            if sent.message_id <= 0:
                raise PermanentGatewayError(
                    "Нет подтверждённого ID сообщения", code="delivery_unconfirmed"
                )
            stored.dispatch_started_at = None
            stored.dispatch_account_id = None
            stored.dispatch_idempotency_key = None
            stored.simulated_snapshot = sent.simulated
            completed_at = utc_now()
            stored.dialogue_last_message_id = sent.message_id
            if step_index == 0:
                stored.primary_message_id = sent.message_id
            elif step_index == 1:
                stored.reply_message_id = sent.message_id
            next_index = step_index + 1
            stored.dialogue_step_index = next_index
            stored.error = None
            stored.error_code = None
            if stored.cancel_requested:
                stored.status = RunStatus.CANCELLED
                stored.finished_at = completed_at
            elif next_index < len(steps):
                stored.next_step = RunStep.REPLY
                stored.status = RunStatus.QUEUED
                stored.scheduled_for = completed_at + timedelta(
                    seconds=steps[next_index].delay_seconds
                )
                # Attempts are per message. A long successful chain must not use
                # up the retry budget of later messages.
                stored.attempt_count = 0
            else:
                stored.status = RunStatus.SUCCEEDED
                stored.finished_at = completed_at
            self._release_lease(stored)
            if sent.simulated is not True:
                await self._record_account_send_once(
                    session, sender.id, operation_key=self._campaign_step_key(run)
                )
            if sent.simulated is not True:
                await self._record_target_success(
                    session, target.id, account_id=sender.id, message_id=sent.message_id
                )
            if stored.status == RunStatus.SUCCEEDED:
                await self._schedule_next_timer_repeat(
                    session,
                    stored,
                    campaign,
                    sender.id,
                    target.id,
                    completed_at,
                )
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=sender.id,
                    target_id=target.id,
                    kind=kind,
                    status=ActionStatus.SUCCEEDED,
                    simulated=sent.simulated,
                    external_message_id=sent.message_id,
                    external_message_ids_json=json.dumps(
                        list(getattr(sent, "message_ids", ()) or (sent.message_id,))
                    ),
                    reply_to_message_id=reply_to_message_id,
                    text=step_text,
                    attempt_number=run.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            await self._schedule_reactions(
                session,
                stored,
                campaign,
                target,
                sender.id,
                sent.message_id,
                dialogue_step=step_index + 1,
            )
            add_audit_event(
                session,
                event_type=(
                    "run.succeeded"
                    if stored.status == RunStatus.SUCCEEDED
                    else "run.dialogue_message_succeeded"
                ),
                entity_type="campaign_run",
                entity_id=stored.id,
                details={
                    "dialogue_step": step_index + 1,
                    "dialogue_steps": len(steps),
                    "account_id": sender.id,
                    "message_id": sent.message_id,
                    "reply_to_message_id": reply_to_message_id,
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    async def _ensure_reply_anchor_still_exists(
        self,
        *,
        account: Account,
        target: Target,
        message_id: int,
    ) -> None:
        """Never continue a dialogue below a post that moderation has removed."""
        inspector = getattr(self._gateway, "inspect_activity", None)
        if inspector is None:
            return
        try:
            await inspector(
                account=account,
                target=target,
                last_message_id=message_id,
            )
        except GatewayError as exc:
            if getattr(exc, "code", None) == "post_deleted_by_moderation":
                await self._record_detected_post_deletion(
                    account_id=int(account.id),
                    target_id=int(target.id),
                    message_id=message_id,
                    exc=exc,
                )
                raise
            await self.handle_observed_gateway_failure(
                int(account.id),
                int(target.id),
                exc,
            )
            # A temporary activity read problem is not grounds to fail a valid
            # dialogue; Telegram will still validate the reply relationship.
            return

    async def _send_reply(
        self,
        run: CampaignRun,
        campaign: Campaign,
        reply: Account,
        target: Target,
    ) -> CampaignRun:
        if run.primary_message_id is None:
            raise ConflictError("для ответа не найдено исходное сообщение")
        content_key = f"{run.request_key}:reply"
        idempotency_key = self._transport_idempotency_key(run, content_key)
        reply_text = self._render_delivery_text(
            self._reply_text(run, campaign),
            idempotency_key=content_key,
        )
        deferred = await self._defer_if_unavailable(run, reply.id)
        if deferred is not None:
            return deferred
        await self._ensure_reply_anchor_still_exists(
            account=reply,
            target=target,
            message_id=run.primary_message_id,
        )
        try:
            self._mark_account_send_attempt(reply.id)
            sent = await self._gateway.send_message(
                account=reply,
                target=target,
                text=reply_text,
                idempotency_key=idempotency_key,
                reply_to_message_id=run.primary_message_id,
            )
        except Exception as exc:
            handled = await self._on_send_failure(
                run,
                reply.id,
                target.id,
                ActionKind.REPLY_MESSAGE,
                reply_text,
                run.primary_message_id,
                exc,
            )
            if handled is not None:
                return handled
            raise

        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            stored.reply_message_id = sent.message_id
            stored.status = RunStatus.SUCCEEDED
            stored.error = None
            stored.error_code = None
            stored.finished_at = utc_now()
            self._release_lease(stored)
            await self._record_account_send(session, reply.id)
            await self._record_target_success(
                session,
                target.id,
                account_id=reply.id,
                message_id=sent.message_id,
            )
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=reply.id,
                    target_id=target.id,
                    kind=ActionKind.REPLY_MESSAGE,
                    status=ActionStatus.SUCCEEDED,
                    simulated=sent.simulated,
                    external_message_id=sent.message_id,
                    reply_to_message_id=run.primary_message_id,
                    text=reply_text,
                    attempt_number=stored.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            add_audit_event(
                session,
                event_type="run.succeeded",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={"reply_message_id": sent.message_id},
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    def _throttle_resume_at(self, exc: BaseException) -> datetime | None:
        """Return a resume time for soft throttles that should defer without burning attempts."""
        code = getattr(exc, "code", None)
        if code not in {
            "account_pacing",
            "chat_timer",
            "join_throttled",
            "join_request_pending",
            "slow_mode",
            "flood_wait",
            "proxy_unavailable",
        }:
            return None
        retry_after = int(getattr(exc, "retry_after", None) or 60)
        return utc_now() + timedelta(seconds=retry_after)

    async def _record_join_request_pending(
        self,
        account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> None:
        """Persist a submitted private-chat request without poisoning the route.

        Approval is an external asynchronous action.  It must be visible in
        the target/launch reports, yet it is neither a ban nor a failed
        delivery and therefore must not set ``send_blocked`` or lower target
        quality.
        """
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                return
            now = utc_now()
            error = safe_error_message(exc)[:500]
            target.last_checked_at = now
            target.last_error_code = "join_request_pending"
            target.last_error = error
            target.last_error_account_id = account_id
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                state = TargetAccountState(account_id=account_id, target_id=target_id)
                session.add(state)
            state.membership_status = MembershipStatus.JOIN_PENDING
            state.can_send = False
            state.membership_checked_at = now
            state.membership_error_code = "join_request_pending"
            state.last_checked_at = now
            state.last_error = error
            # A previous stale result must not prevent the approval re-check.
            state.send_blocked = False
            state.blocked_reason = None
            state.blocked_at = None
            add_audit_event(
                session,
                event_type="target_account.join_request_pending",
                entity_type="target_account_state",
                entity_id=f"{target_id}:{account_id}",
                details={"target_id": target_id, "account_id": account_id},
                actor="worker",
            )
            await session.commit()

    async def _on_send_failure(
        self,
        run: CampaignRun,
        account_id: int,
        target_id: int,
        kind: ActionKind,
        text: str,
        reply_to_message_id: int | None,
        exc: BaseException,
    ) -> CampaignRun | None:
        """Handle a failed send. Returns a deferred run for soft throttles, else None."""
        if isinstance(exc, RunCancelled):
            raise exc
        resume_at = self._throttle_resume_at(exc)
        if (
            isinstance(exc, GatewayError)
            and resume_at is None
            and failure_code(exc) != "delivery_unconfirmed"
            and getattr(exc, "external_message_id", None) is None
            and failure_code(exc) not in {"post_deleted_by_moderation", "album_partial_missing"}
        ):
            await self._clear_campaign_dispatch(run)
        if resume_at is not None:
            reason = str(getattr(exc, "code", None) or "throttle")
            if reason == "join_request_pending":
                await self._record_join_request_pending(account_id, target_id, exc)
            if getattr(exc, "code", None) == "flood_wait":
                await self._handle_account_failure(account_id, exc)
            async with self._session_factory() as session:
                account = await session.get(Account, account_id)
                cooldown = ensure_utc(account.cooldown_until) if account is not None else None
                if cooldown is not None:
                    resume_at = max(resume_at, cooldown)
                deferred = await self._defer_run(session, run, resume_at, reason=reason)
            if deferred is None:
                raise RunCancelled("cancellation requested")
            return deferred
        if (
            getattr(exc, "code", None) == "post_deleted_by_moderation"
            and run.membership_gate_retry_count == 0
        ):
            recovery = await self._recover_membership_gate(run, account_id, target_id, exc)
            if recovery is not None and recovery.detected:
                if recovery.recovered:
                    deferred = await self._schedule_membership_gate_retry(
                        run=run,
                        account_id=account_id,
                        target_id=target_id,
                        kind=kind,
                        text=text,
                        reply_to_message_id=reply_to_message_id,
                        deleted_message_id=getattr(exc, "external_message_id", None),
                        deleted_message_ids=getattr(exc, "external_message_ids", ()),
                        recovery=recovery,
                    )
                    if deferred is not None:
                        return deferred
                else:
                    # The post really was deleted; retain its terminal code so
                    # the existing target-quality quarantine moves this chat to
                    # "bad" instead of retrying an uncertain gate indefinitely.
                    exc = PermanentGatewayError(
                        f"membership gate recovery failed: {recovery.error or 'unknown error'}",
                        code="post_deleted_by_moderation",
                        external_message_id=getattr(exc, "external_message_id", None),
                        external_message_ids=getattr(exc, "external_message_ids", ()),
                    )
        await self._record_action_failure(
            run_id=run.id,
            account_id=account_id,
            target_id=target_id,
            kind=kind,
            text=text,
            reply_to_message_id=reply_to_message_id,
            error=safe_error_message(exc),
            error_code=failure_code(
                exc,
                "delivery_unconfirmed"
                if self._send_attempt_started.get()
                else "preparation_failed",
            ),
            external_message_id=getattr(exc, "external_message_id", None),
            attempt_number=run.attempt_count,
            lease_token=run.lease_token,
            external_message_ids=getattr(exc, "external_message_ids", ()),
        )
        if self._send_attempt_started.get() and failure_code(exc, "") in {
            "post_deleted_by_moderation",
            "delivery_unconfirmed",
            "album_partial_missing",
        }:
            # Accepted and uncertain attempts consume quota, never a success count.
            await self.record_external_account_send(
                account_id,
                operation_key=self._campaign_step_key(run),
            )
        if isinstance(exc.__cause__, GatewayError):
            await self._record_target_failure(account_id, target_id, exc.__cause__)
            await self._handle_account_failure(account_id, exc.__cause__)
        await self._record_target_failure(account_id, target_id, exc)
        await self._handle_account_failure(account_id, exc)
        return None

    async def _recover_membership_gate(
        self,
        run: CampaignRun,
        account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> MembershipGateRecovery | None:
        """Ask the gateway to satisfy a narrowly recognised subscription gate."""

        deleted_message_id = getattr(exc, "external_message_id", None)
        recover = getattr(self._gateway, "recover_membership_gate", None)
        if recover is None or deleted_message_id is None:
            return None
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            target = await session.get(Target, target_id)
        if account is None or target is None:
            return None
        try:
            result = await recover(
                account=account,
                target=target,
                deleted_message_id=int(deleted_message_id),
            )
        except GatewayError as recovery_exc:
            return MembershipGateRecovery(
                detected=True,
                recovered=False,
                error=safe_error_message(recovery_exc),
            )
        except Exception:  # noqa: BLE001 - the original moderated post remains authoritative
            logger.exception("Membership-gate recovery failed for run %s", run.id)
            return MembershipGateRecovery(
                detected=True,
                recovered=False,
                error="membership gate operation failed",
            )
        return result if isinstance(result, MembershipGateRecovery) else None

    async def _schedule_membership_gate_retry(
        self,
        *,
        run: CampaignRun,
        account_id: int,
        target_id: int,
        kind: ActionKind,
        text: str,
        reply_to_message_id: int | None,
        deleted_message_id: int | None,
        recovery: MembershipGateRecovery,
        deleted_message_ids: tuple[int, ...] = (),
    ) -> CampaignRun | None:
        """Persist the completed gate flow and one five-minute retry of this step."""

        retry_at = utc_now() + timedelta(
            seconds=int(self._cfg("membership_gate_retry_delay_seconds", 300))
        )
        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            if stored.membership_gate_retry_count >= 1:
                return None
            stored.membership_gate_retry_count = 1
            session.add(
                ActionLog(
                    run_id=stored.id,
                    account_id=account_id,
                    target_id=target_id,
                    kind=kind,
                    status=ActionStatus.FAILED,
                    simulated=self._failure_delivery_mode(),
                    external_message_id=deleted_message_id,
                    external_message_ids_json=json.dumps(list(deleted_message_ids))
                    if deleted_message_ids
                    else None,
                    delayed_delivery_status="missing",
                    delayed_delivery_checked_at=utc_now(),
                    reply_to_message_id=reply_to_message_id,
                    text=text,
                    error=(
                        "subscription gate resolved: "
                        f"{recovery.notice or 'Telegram inline buttons'}"
                    ),
                    error_code="membership_gate_recovered",
                    attempt_number=run.attempt_count,
                    lease_token=run.lease_token,
                )
            )
            await self._record_account_send_once(
                session, account_id, operation_key=self._campaign_step_key(run)
            )
            stored.dispatch_started_at = None
            stored.dispatch_account_id = None
            stored.dispatch_idempotency_key = None
            if stored.cancel_requested:
                stored.status = RunStatus.CANCELLED
                stored.finished_at = utc_now()
                self._release_lease(stored)
                await session.commit()
                await session.refresh(stored)
                return stored

            repeat_until = ensure_utc(stored.repeat_until)
            dialogue_in_progress = bool(
                stored.dialogue_steps_snapshot_json is not None and stored.dialogue_step_index > 0
            )
            if repeat_until is not None and not dialogue_in_progress and retry_at > repeat_until:
                stored.status = RunStatus.CANCELLED
                stored.error_code = "membership_gate_retry_outside_window"
                stored.error = "subscription gate was resolved after the campaign window ended"
                stored.finished_at = utc_now()
                self._release_lease(stored)
                add_audit_event(
                    session,
                    event_type="run.membership_gate_retry_skipped",
                    entity_type="campaign_run",
                    entity_id=stored.id,
                    details={
                        "retry_at": retry_at.isoformat(),
                        "repeat_until": repeat_until.isoformat(),
                    },
                    actor="worker",
                )
                await session.commit()
                await session.refresh(stored)
                return stored

            stored.status = RunStatus.QUEUED
            stored.error = None
            stored.error_code = "membership_gate_retry"
            stored.scheduled_for = retry_at
            # A recovered subscription gate is a state transition, not an
            # unstable send retry; preserve the normal retry budget.
            stored.attempt_count = max(0, stored.attempt_count - 1)
            self._release_lease(stored)
            add_audit_event(
                session,
                event_type="run.membership_gate_retry_scheduled",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={
                    "account_id": account_id,
                    "target_id": target_id,
                    "retry_at": retry_at.isoformat(),
                    "joined_channel_refs": list(recovery.joined_channel_refs),
                    "verification_callback_count": recovery.verification_callback_count,
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored

    async def _schedule_reply(
        self, run_id: int, delay_seconds: int, lease_token: str | None
    ) -> CampaignRun:
        async with self._session_factory() as session:
            run = await self._require_running_run(session, run_id, lease_token)
            if run.cancel_requested:
                run.status = RunStatus.CANCELLED
                run.finished_at = utc_now()
            else:
                run.next_step = RunStep.REPLY
                run.status = RunStatus.QUEUED
                run.scheduled_for = utc_now() + timedelta(seconds=delay_seconds)
            self._release_lease(run)
            await session.commit()
            await session.refresh(run)
            return run

    @property
    def _pacing_enabled(self) -> bool:
        return self._pace_max_seconds > 0

    def _pace_delay_seconds(self) -> float:
        if self._pace_max_seconds <= self._pace_min_seconds:
            return self._pace_max_seconds
        return random.uniform(self._pace_min_seconds, self._pace_max_seconds)

    async def _record_account_send(
        self, session: AsyncSession, account_id: int, *, release_reservation: bool = False
    ) -> None:
        """Update per-account pacing, daily counters and micro-breaks after a delivery."""
        account = await session.get(Account, account_id)
        if account is None:
            return
        reservation = self._send_reservation.get()
        if reservation is not None and reservation[0] == account_id:
            if account.send_reservation_token != reservation[1]:
                raise LeaseLostError("account send reservation is no longer current")
        now = utc_now()

        day_start = ensure_utc(account.day_window_start)
        if day_start is None or (now - day_start) >= timedelta(hours=24):
            account.day_window_start = now
            account.sent_today = 0
            day_start = now
        account.sent_today += 1
        account.sent_since_break += 1

        next_send_at: datetime | None = None
        cooldown_until: datetime | None = None
        cooldown_reason: str | None = None

        if self._pacing_enabled:
            next_send_at = now + timedelta(seconds=self._pace_delay_seconds())

        micro_every = int(self._cfg("account_micro_break_every", 0))
        if micro_every > 0 and account.sent_since_break >= micro_every:
            micro_min = float(self._cfg("account_micro_break_min_seconds", 300.0))
            micro_max = max(micro_min, float(self._cfg("account_micro_break_max_seconds", 900.0)))
            pause = random.uniform(micro_min, micro_max)
            cooldown_until = now + timedelta(seconds=pause)
            cooldown_reason = "micro_break"
            account.sent_since_break = 0

        daily_limit = int(self._cfg("account_daily_limit", 200))
        if daily_limit > 0 and account.sent_today >= daily_limit:
            day_end = (ensure_utc(account.day_window_start) or now) + timedelta(hours=24)
            if cooldown_until is None or day_end > cooldown_until:
                cooldown_until = day_end
                cooldown_reason = "daily_limit"

        if next_send_at is not None:
            current_next = ensure_utc(account.next_send_at)
            if current_next is None or next_send_at > current_next:
                account.next_send_at = next_send_at
        if cooldown_until is not None:
            current_cooldown = ensure_utc(account.cooldown_until)
            if current_cooldown is None or cooldown_until > current_cooldown:
                account.cooldown_until = cooldown_until
                account.cooldown_reason = cooldown_reason
        if release_reservation and reservation is not None and reservation[0] == account_id:
            account.send_reservation_token = None
            account.send_reservation_until = None

    @staticmethod
    async def _record_target_success(
        session: AsyncSession,
        target_id: int,
        *,
        account_id: int,
        message_id: int,
    ) -> None:
        target = await session.get(Target, target_id)
        if target is None:
            return
        now = utc_now()
        target.last_checked_at = now
        target.last_success_at = target.last_checked_at
        target.last_error_code = None
        target.last_error = None
        target.last_error_account_id = None
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        if state is None:
            state = TargetAccountState(account_id=account_id, target_id=target_id)
            session.add(state)
        state.last_checked_at = now
        state.last_success_at = now
        state.last_message_id = message_id
        state.last_error = None
        # A successful delivery is stronger and newer evidence than the
        # read-only preflight.  In particular, turn a previously joinable
        # route into a confirmed member route after the live auto-join.
        state.membership_status = MembershipStatus.MEMBER
        state.can_send = True
        state.membership_checked_at = now
        state.membership_error_code = None
        state.send_blocked = False
        state.blocked_reason = None
        state.blocked_at = None

    async def _record_target_failure(
        self,
        account_id: int,
        target_id: int,
        exc: BaseException,
    ) -> None:
        code = failure_code(exc, "")
        if code not in OBSERVED_TARGET_FAILURE_CODES:
            return
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                return
            now = utc_now()
            error = safe_error_message(exc)[:500]
            target.last_checked_at = now
            target.last_error_code = code
            target.last_error = error
            target.last_error_account_id = account_id
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
            state.last_checked_at = now
            state.last_error = error
            if code in ACCOUNT_TARGET_BLOCK_CODES:
                newly_blocked = not state.send_blocked
                state.send_blocked = True
                state.blocked_reason = code
                state.blocked_at = state.blocked_at or now
                state.next_send_at = None
                if newly_blocked:
                    add_audit_event(
                        session,
                        event_type="target_account.permanently_blocked",
                        entity_type="target_account_state",
                        entity_id=f"{target_id}:{account_id}",
                        details={
                            "target_id": target_id,
                            "account_id": account_id,
                            "reason": code,
                            "error": error,
                        },
                        actor="worker",
                    )
            await self.refresh_target_quality(session, target_id)
            await session.commit()

    @staticmethod
    async def refresh_target_quality(session: AsyncSession, target_id: int) -> None:
        """Recalculate a transparent target score from confirmed target failures."""
        target = await session.get(Target, target_id)
        if target is None:
            return
        log_filters = [
            ActionLog.target_id == target_id,
            ActionLog.simulated.is_not(True),
            ActionLog.kind == ActionKind.PRIMARY_MESSAGE,
        ]
        reset_at = ensure_utc(target.quality_reset_at)
        if reset_at is not None:
            log_filters.append(ActionLog.created_at >= reset_at)
        successes = int(
            await session.scalar(
                select(func.count(ActionLog.id)).where(
                    *log_filters,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                )
            )
            or 0
        )
        failures = int(
            await session.scalar(
                select(func.count(ActionLog.id)).where(
                    *log_filters,
                    ActionLog.status == ActionStatus.FAILED,
                    ActionLog.error_code.in_(TARGET_QUALITY_FAILURE_CODES),
                )
            )
            or 0
        )
        moderation_deletions = int(
            await session.scalar(
                select(func.count(ActionLog.id)).where(
                    ActionLog.target_id == target_id,
                    ActionLog.simulated.is_not(True),
                    ActionLog.status == ActionStatus.FAILED,
                    ActionLog.error_code == "post_deleted_by_moderation",
                )
            )
            or 0
        )
        unavailable_failures = int(
            await session.scalar(
                select(func.count(ActionLog.id)).where(
                    *log_filters,
                    ActionLog.status == ActionStatus.FAILED,
                    ActionLog.error_code == "target_unavailable",
                )
            )
            or 0
        )
        banned_filters = [
            TargetAccountState.target_id == target_id,
            TargetAccountState.send_blocked.is_(True),
            TargetAccountState.blocked_reason == "account_banned_in_chat",
        ]
        if reset_at is not None:
            banned_filters.append(TargetAccountState.blocked_at >= reset_at)
        banned_accounts = int(
            await session.scalar(select(func.count(TargetAccountState.id)).where(*banned_filters))
            or 0
        )
        attempts = successes + failures
        delivery_score = round(successes * 100 / attempts) if attempts else 100
        score = max(0, delivery_score - min(70, banned_accounts * 35))

        reasons: list[str] = []
        if moderation_deletions:
            score = 0
            detail = target.last_error or "публикация исчезла после подтверждённой отправки"
            reasons.append(f"публикации исчезают после отправки: {detail}")
        if unavailable_failures:
            score = 0
            reasons.append("Telegram подтвердил, что чат недоступен или не принимает сообщения")
        if banned_accounts >= 2:
            reasons.append(f"бан отправки получили {banned_accounts} аккаунта")
        if successes == 0 and failures >= 3 and not moderation_deletions:
            reasons.append(f"0 успешных из {attempts} подтверждённых попыток")
        elif attempts >= 5 and successes * 5 <= attempts:
            reasons.append(f"успешно {successes} из {attempts} попыток ({delivery_score}%)")

        previous_status = target.quality_status
        now = utc_now()
        target.quality_attempts = attempts
        target.quality_successes = successes
        target.quality_failures = failures
        target.quality_banned_accounts = banned_accounts
        target.quality_score = score
        target.quality_updated_at = now
        if reasons:
            target.quality_status = TARGET_QUALITY_BAD
            target.quality_reason = "; ".join(reasons)[:500]
            target.quality_bad_at = target.quality_bad_at or now
            await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.target_id == target_id,
                    CampaignRun.status == RunStatus.QUEUED,
                )
                .values(
                    status=RunStatus.CANCELLED,
                    error_code="target_low_quality",
                    error=target.quality_reason,
                    finished_at=now,
                    cancel_requested=True,
                )
            )
            if previous_status != TARGET_QUALITY_BAD:
                add_audit_event(
                    session,
                    event_type="target.quality_auto_paused",
                    entity_type="target",
                    entity_id=target_id,
                    details={
                        "score": score,
                        "attempts": attempts,
                        "successes": successes,
                        "failures": failures,
                        "banned_accounts": banned_accounts,
                        "reason": target.quality_reason,
                    },
                    actor="worker",
                )
        elif previous_status == TARGET_QUALITY_BAD:
            # A bad target stays quarantined until the operator explicitly
            # restores it. A later preflight must not silently undo the decision.
            target.quality_status = TARGET_QUALITY_BAD
            target.quality_reason = target.quality_reason or "требуется ручная проверка"
            target.quality_bad_at = target.quality_bad_at or now
        else:
            target.quality_status = (
                TARGET_QUALITY_WORKING
                if attempts >= 3 and successes * 100 >= attempts * 60
                else TARGET_QUALITY_LEARNING
            )
            target.quality_reason = None
            target.quality_bad_at = None

    @staticmethod
    async def refresh_target_quality_many(
        session: AsyncSession,
        target_ids: set[int],
    ) -> None:
        """Refresh many target scores with grouped queries instead of per-target N+1 reads."""
        if not target_ids:
            return
        ids = sorted(target_ids)
        targets = list(await session.scalars(select(Target).where(Target.id.in_(ids))))
        by_id = {int(target.id): target for target in targets}
        primary_counts = await session.execute(
            select(
                ActionLog.target_id,
                ActionLog.status,
                ActionLog.error_code,
                func.count(ActionLog.id),
            )
            .join(Target, Target.id == ActionLog.target_id)
            .where(
                ActionLog.target_id.in_(ids),
                ActionLog.kind == ActionKind.PRIMARY_MESSAGE,
                or_(
                    Target.quality_reset_at.is_(None),
                    ActionLog.created_at >= Target.quality_reset_at,
                ),
            )
            .group_by(ActionLog.target_id, ActionLog.status, ActionLog.error_code)
        )
        successes: dict[int, int] = {}
        failures: dict[int, int] = {}
        unavailable: dict[int, int] = {}
        for target_id, status, error_code, count in primary_counts:
            target_id = int(target_id)
            count = int(count)
            if status == ActionStatus.SUCCEEDED:
                successes[target_id] = successes.get(target_id, 0) + count
            elif status == ActionStatus.FAILED and error_code in TARGET_QUALITY_FAILURE_CODES:
                failures[target_id] = failures.get(target_id, 0) + count
                if error_code == "target_unavailable":
                    unavailable[target_id] = unavailable.get(target_id, 0) + count
        moderation_rows = await session.execute(
            select(ActionLog.target_id, func.count(ActionLog.id))
            .where(
                ActionLog.target_id.in_(ids),
                ActionLog.status == ActionStatus.FAILED,
                ActionLog.error_code == "post_deleted_by_moderation",
            )
            .group_by(ActionLog.target_id)
        )
        moderation = {int(target_id): int(count) for target_id, count in moderation_rows}
        banned_rows = await session.execute(
            select(TargetAccountState.target_id, func.count(TargetAccountState.id))
            .join(Target, Target.id == TargetAccountState.target_id)
            .where(
                TargetAccountState.target_id.in_(ids),
                TargetAccountState.send_blocked.is_(True),
                TargetAccountState.blocked_reason == "account_banned_in_chat",
                or_(
                    Target.quality_reset_at.is_(None),
                    TargetAccountState.blocked_at >= Target.quality_reset_at,
                ),
            )
            .group_by(TargetAccountState.target_id)
        )
        banned = {int(target_id): int(count) for target_id, count in banned_rows}
        now = utc_now()
        for target_id, target in by_id.items():
            success_count = successes.get(target_id, 0)
            failure_count = failures.get(target_id, 0)
            moderation_count = moderation.get(target_id, 0)
            unavailable_count = unavailable.get(target_id, 0)
            banned_count = banned.get(target_id, 0)
            attempts = success_count + failure_count
            delivery_score = round(success_count * 100 / attempts) if attempts else 100
            score = max(0, delivery_score - min(70, banned_count * 35))
            reasons: list[str] = []
            if moderation_count:
                score = 0
                detail = target.last_error or "публикация исчезла после отправки"
                reasons.append(f"публикации исчезают после отправки: {detail}")
            if unavailable_count:
                score = 0
                reasons.append("Telegram подтвердил глобальную недоступность чата")
            if banned_count >= 2:
                reasons.append(f"бан отправки получили {banned_count} аккаунта")
            if success_count == 0 and failure_count >= 3 and not moderation_count:
                reasons.append(f"0 успешных из {attempts} подтверждённых попыток")
            elif attempts >= 5 and success_count * 5 <= attempts:
                reasons.append(f"успешно {success_count} из {attempts} попыток ({delivery_score}%)")
            previous_status = target.quality_status
            target.quality_attempts = attempts
            target.quality_successes = success_count
            target.quality_failures = failure_count
            target.quality_banned_accounts = banned_count
            target.quality_score = score
            target.quality_updated_at = now
            if reasons:
                target.quality_status = TARGET_QUALITY_BAD
                target.quality_reason = "; ".join(reasons)[:500]
                target.quality_bad_at = target.quality_bad_at or now
                await session.execute(
                    update(CampaignRun)
                    .where(
                        CampaignRun.target_id == target_id,
                        CampaignRun.status == RunStatus.QUEUED,
                    )
                    .values(
                        status=RunStatus.CANCELLED,
                        error_code="target_low_quality",
                        error=target.quality_reason,
                        finished_at=now,
                        cancel_requested=True,
                    )
                )
                if previous_status != TARGET_QUALITY_BAD:
                    add_audit_event(
                        session,
                        event_type="target.quality_auto_paused",
                        entity_type="target",
                        entity_id=target_id,
                        details={
                            "score": score,
                            "attempts": attempts,
                            "successes": success_count,
                            "failures": failure_count,
                            "banned_accounts": banned_count,
                            "reason": target.quality_reason,
                        },
                        actor="worker",
                    )
            elif previous_status == TARGET_QUALITY_BAD:
                target.quality_status = TARGET_QUALITY_BAD
                target.quality_reason = target.quality_reason or "требуется ручная проверка"
                target.quality_bad_at = target.quality_bad_at or now
            else:
                target.quality_status = (
                    TARGET_QUALITY_WORKING
                    if attempts >= 3 and success_count * 100 >= attempts * 60
                    else TARGET_QUALITY_LEARNING
                )
                target.quality_reason = None
                target.quality_bad_at = None

    def _account_resume_at(self, account: Account) -> datetime | None:
        """Latest future timestamp gating this account's next send, if any."""
        now = utc_now()
        daily_limit = int(self._cfg("account_daily_limit", 200))
        day_start = ensure_utc(account.day_window_start)
        used_today = (
            0
            if day_start is None or now - day_start >= timedelta(hours=24)
            else max(0, int(account.sent_today or 0))
        )
        cooldown_until = ensure_utc(account.cooldown_until)
        if (
            account.cooldown_reason == "daily_limit"
            and daily_limit > 0
            and used_today < daily_limit
        ):
            cooldown_until = None
        candidates = [
            value
            for value in (ensure_utc(account.next_send_at), cooldown_until)
            if value is not None and value > now
        ]
        if daily_limit > 0 and used_today >= daily_limit and day_start is not None:
            candidates.append(day_start + timedelta(hours=24))
        return max(candidates) if candidates else None

    @staticmethod
    def _primary_text(run: CampaignRun, campaign: Campaign) -> str:
        if run.primary_text_snapshot is not None:
            return run.primary_text_snapshot
        return campaign.primary_text

    @staticmethod
    def _primary_content_value(
        run: CampaignRun,
        campaign: Campaign,
        snapshot_name: str,
        campaign_name: str,
    ) -> object | None:
        snapshot = getattr(run, snapshot_name)
        if run.primary_text_snapshot is not None:
            return snapshot
        return snapshot if snapshot is not None else getattr(campaign, campaign_name)

    @staticmethod
    def _serialize_media_items(variant: BroadcastContentVariant) -> str:
        return json.dumps(
            [item.as_dict() for item in variant.media],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _campaign_content_variants(campaign: Campaign) -> list[BroadcastContentVariant]:
        try:
            variants = parse_content_variants(
                campaign.content_variants_json,
                fallback_text=campaign.primary_text,
                fallback_html=campaign.primary_html,
                fallback_media_path=campaign.primary_media_path,
                fallback_media_kind=campaign.primary_media_kind,
                fallback_media_name=campaign.primary_media_name,
                fallback_media_size=campaign.primary_media_size,
            )
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        enabled = [variant for variant in variants if variant.enabled]
        if not enabled:
            raise ConflictError("campaign has no enabled content variants")
        return enabled

    @classmethod
    def _run_content_variants(
        cls,
        run: CampaignRun,
        campaign: Campaign,
    ) -> list[BroadcastContentVariant]:
        try:
            variants = parse_content_variants(
                run.content_variants_snapshot_json,
                fallback_text=cls._primary_text(run, campaign),
                fallback_html=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_html_snapshot",
                    "primary_html",
                ),
                fallback_media_path=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_path_snapshot",
                    "primary_media_path",
                ),
                fallback_media_kind=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_kind_snapshot",
                    "primary_media_kind",
                ),
                fallback_media_name=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_name_snapshot",
                    "primary_media_name",
                ),
                fallback_media_size=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_size_snapshot",
                    "primary_media_size",
                ),
            )
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        return [variant for variant in variants if variant.enabled]

    @classmethod
    def _run_content_variant(
        cls,
        run: CampaignRun,
        campaign: Campaign,
    ) -> BroadcastContentVariant:
        variants = cls._run_content_variants(run, campaign)
        if run.content_variant_id_snapshot is not None:
            for variant in variants:
                if variant.variant_id == run.content_variant_id_snapshot:
                    return variant
        return variants[run.content_variant_index % len(variants)]

    async def _send_broadcast_content(
        self,
        run: CampaignRun,
        campaign: Campaign,
        account: Account,
        target: Target,
        *,
        idempotency_key: str,
        content_render_key: str,
        text: str,
    ) -> SentMessage:
        sender = getattr(self._gateway, "send_content", None)
        current_variant = self._run_content_variant(run, campaign)
        contact = current_variant.contact.as_dict() if current_variant.contact else None
        media_items = None
        if run.media_items_snapshot_json is not None:
            try:
                raw_items = json.loads(run.media_items_snapshot_json)
                media_items = raw_items if isinstance(raw_items, list) else None
            except json.JSONDecodeError as exc:
                raise ConflictError("media album snapshot is damaged") from exc
        if sender is None:
            has_rich_content = (
                contact is not None
                or any(
                    self._primary_content_value(run, campaign, snapshot_name, campaign_name)
                    is not None
                    for snapshot_name, campaign_name in (
                        ("primary_html_snapshot", "primary_html"),
                        ("primary_media_path_snapshot", "primary_media_path"),
                    )
                )
                or bool(media_items)
            )
            if has_rich_content:
                raise ConflictError("configured gateway does not support formatted media content")
            await self._mark_campaign_dispatch(run, account.id, target.id, idempotency_key)
            return await self._gateway.send_message(
                account=account,
                target=target,
                text=text,
                idempotency_key=idempotency_key,
            )
        raw_html_text = self._primary_content_value(
            run,
            campaign,
            "primary_html_snapshot",
            "primary_html",
        )
        html_text = (
            self._render_delivery_text(str(raw_html_text), idempotency_key=content_render_key)
            if raw_html_text is not None
            else None
        )
        message_limit = 1024 if media_items or current_variant.media else 4096
        mention_ids = await self._resolve_broadcast_hidden_mentions(
            run,
            account,
            target,
            selection_key=f"{run.request_key}:hidden-mentions",
            text=text,
            message_limit=message_limit,
            eligible=contact is None,
        )
        html_text = append_hidden_mentions(
            text=text,
            html_text=html_text,
            user_ids=mention_ids,
            message_limit=message_limit,
        )
        dispatch_kwargs = await self._campaign_dispatch_kwargs(
            run, account.id, target.id, idempotency_key
        )
        return await sender(
            account=account,
            target=target,
            text=text,
            idempotency_key=idempotency_key,
            html_text=html_text,
            media_path=self._primary_content_value(
                run,
                campaign,
                "primary_media_path_snapshot",
                "primary_media_path",
            ),
            media_kind=self._primary_content_value(
                run,
                campaign,
                "primary_media_kind_snapshot",
                "primary_media_kind",
            ),
            media_name=self._primary_content_value(
                run,
                campaign,
                "primary_media_name_snapshot",
                "primary_media_name",
            ),
            media_items=media_items,
            contact=contact,
            **dispatch_kwargs,
        )

    async def _campaign_dispatch_kwargs(
        self,
        run: CampaignRun,
        account_id: int,
        target_id: int,
        key: str,
    ) -> dict:
        async def before_send() -> None:
            await self._mark_campaign_dispatch(run, account_id, target_id, key)

        if getattr(self._gateway, "supports_campaign_dispatch_hook", False):
            return {"before_send": before_send}
        await before_send()
        return {}

    @classmethod
    def _campaign_step_key(cls, run: CampaignRun) -> str:
        index = max(int(run.dialogue_step_index or 0), 1 if run.next_step == RunStep.REPLY else 0)
        suffix = "primary" if index == 0 else "reply" if index == 1 else f"dialogue:{index}"
        return cls._transport_idempotency_key(run, f"{run.request_key}:{suffix}")

    async def _mark_campaign_dispatch(
        self,
        run: CampaignRun,
        account_id: int,
        target_id: int,
        key: str,
    ) -> None:
        """Persist the last cancellation check immediately before Telegram's send RPC."""
        now = utc_now()
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            try:
                self._validate_sender(account, require_live_session=self._require_live_session())
            except ConflictError as exc:
                raise PermanentGatewayError(str(exc), code="sender_unavailable") from exc
            resume_at = self._account_resume_at(account)
            if resume_at is not None and resume_at > now:
                raise TemporaryGatewayError(
                    "Аккаунт получил паузу до начала отправки",
                    code="account_pacing",
                    retry_after=max(1, int((resume_at - now).total_seconds()) + 1),
                )
            campaign = await session.get(Campaign, run.campaign_id)
            if campaign is None or not campaign.enabled:
                raise RunCancelled("Рассылка выключена до отправки")
            target = await session.get(Target, target_id)
            try:
                self._validate_config(campaign, target)
            except ConflictError as exc:
                raise RunCancelled(str(exc)) from exc
            # The run retains its selected chat. Editing a source list controls
            # future launches; explicit stop and target quarantine fence this run.
            marked = await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run.id,
                    CampaignRun.status == RunStatus.RUNNING,
                    CampaignRun.lease_token == run.lease_token,
                    CampaignRun.lease_until > now,
                    CampaignRun.cancel_requested.is_(False),
                    CampaignRun.campaign_id.in_(
                        select(Campaign.id).where(Campaign.enabled.is_(True))
                    ),
                )
                .values(
                    dispatch_started_at=now,
                    dispatch_account_id=account_id,
                    dispatch_idempotency_key=key,
                )
                .execution_options(synchronize_session=False)
            )
            await session.commit()
            if marked.rowcount != 1:
                current = await session.get(CampaignRun, run.id)
                if current is None or current.lease_token != run.lease_token:
                    raise LeaseLostError("Право выполнения задачи потеряно до отправки")
                raise RunCancelled("Рассылка остановлена до отправки сообщения")
        if getattr(self._gateway, "simulated", None) is not True:
            self._mark_account_send_attempt(account_id)

    async def _clear_campaign_dispatch(self, run: CampaignRun) -> None:
        async with self._session_factory() as session:
            await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run.id,
                    CampaignRun.lease_token == run.lease_token,
                    CampaignRun.status == RunStatus.RUNNING,
                )
                .values(
                    dispatch_started_at=None,
                    dispatch_account_id=None,
                    dispatch_idempotency_key=None,
                )
            )
            await session.commit()

    async def _resolve_broadcast_hidden_mentions(
        self,
        run: CampaignRun,
        account: Account,
        target: Target,
        *,
        selection_key: str,
        text: str,
        message_limit: int,
        eligible: bool,
    ) -> tuple[int, ...]:
        """Persist one sampled member set before the outbound post is attempted."""

        # The setting is immutable for a queued task.  ``None`` only appears
        # on legacy rows created before this feature and must remain disabled,
        # rather than inheriting a campaign setting changed later.
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
            parsed = parsed[:maximum_count] if eligible else ()
            if len(parsed) < HIDDEN_MENTION_MINIMUM:
                parsed = ()
            checker = getattr(self._gateway, "filter_mentionable_user_ids", None)
            if checker is not None:
                try:
                    parsed = await checker(account=account, user_ids=parsed)
                    if len(parsed) < HIDDEN_MENTION_MINIMUM:
                        parsed = ()
                except Exception as exc:  # noqa: BLE001 - send the post without unchecked mentions
                    if failure_code(exc) in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
                        raise
                    logger.info("Could not recheck hidden mention usernames for run %s", run.id)
                    parsed = ()
            if run.hidden_mention_resolved_count != len(parsed):
                await self._update_broadcast_hidden_mention_snapshot(run, parsed)
            return parsed

        reason: str | None = None
        candidates: tuple[int, ...] = ()
        if not eligible:
            reason = "content_does_not_support_mentions"
        elif maximum_count < HIDDEN_MENTION_MINIMUM:
            reason = "message_has_no_room_for_mentions"
        else:
            collector = getattr(self._gateway, "collect_mentionable_user_ids", None)
            if collector is None:
                reason = "gateway_does_not_support_participant_collection"
            else:
                try:
                    collected = await collector(
                        account=account,
                        target=target,
                        limit=200,
                    )
                    candidates = normalize_user_ids(
                        collected,
                        exclude_user_ids=excluded_user_ids,
                    )
                    if len(candidates) < HIDDEN_MENTION_MINIMUM:
                        reason = "fewer_than_five_eligible_members"
                except Exception as exc:  # noqa: BLE001 - a member list must not block delivery
                    if failure_code(exc) in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
                        raise
                    logger.info(
                        "Hidden mentions were skipped for campaign run %s: %s",
                        run.id,
                        safe_error_message(exc),
                    )
                    reason = "participant_collection_unavailable"
        return await self._reserve_broadcast_hidden_mentions(
            run,
            candidates,
            selection_key=selection_key,
            maximum_count=maximum_count,
            reason=reason,
            exclude_user_ids=excluded_user_ids,
        )

    async def _reserve_broadcast_hidden_mentions(
        self,
        run: CampaignRun,
        candidates: tuple[int, ...],
        *,
        selection_key: str,
        maximum_count: int,
        reason: str | None,
        exclude_user_ids: set[int],
    ) -> tuple[int, ...]:
        """Atomically reserve a cooldown-safe member set for one destination."""

        for _ in range(3):
            async with self._session_factory() as session:
                stored = await self._require_running_run(session, run.id, run.lease_token)
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
                        await self._release_broadcast_mention_reservations(
                            session,
                            run_ids=[stored.id],
                            keep_user_ids=parsed,
                        )
                        await session.commit()
                    return parsed

                now = utc_now()
                cooldown_days = max(
                    1,
                    min(
                        365,
                        int(
                            stored.mention_cooldown_days_snapshot
                            or self._cfg("broadcast_mention_cooldown_days", 30)
                        ),
                    ),
                )
                cooldown_cutoff = now - timedelta(days=cooldown_days)
                states = list(
                    await session.scalars(
                        select(CampaignMentionState).where(
                            CampaignMentionState.campaign_id == stored.campaign_id,
                            CampaignMentionState.target_id == stored.target_id,
                            CampaignMentionState.telegram_user_id.in_(candidates),
                        )
                    )
                )
                states_by_user = {int(item.telegram_user_id): item for item in states}
                eligible = tuple(
                    user_id
                    for user_id in candidates
                    if self._broadcast_mention_state_is_eligible(
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

                repeat_until = ensure_utc(stored.repeat_until)
                reserve_until = now + timedelta(days=7)
                if repeat_until is not None and repeat_until > reserve_until:
                    reserve_until = repeat_until
                reservation_conflict = False
                for user_id in selected:
                    state = states_by_user.get(user_id)
                    if state is None:
                        session.add(
                            CampaignMentionState(
                                campaign_id=stored.campaign_id,
                                target_id=stored.target_id,
                                telegram_user_id=user_id,
                                reserved_run_id=stored.id,
                                reserved_until=reserve_until,
                            )
                        )
                        continue
                    claimed = await session.execute(
                        update(CampaignMentionState)
                        .execution_options(synchronize_session=False)
                        .where(
                            CampaignMentionState.id == state.id,
                            or_(
                                CampaignMentionState.last_mentioned_at.is_(None),
                                CampaignMentionState.last_mentioned_at <= cooldown_cutoff,
                            ),
                            or_(
                                CampaignMentionState.reserved_run_id == stored.id,
                                CampaignMentionState.reserved_run_id.is_(None),
                                CampaignMentionState.reserved_until.is_(None),
                                CampaignMentionState.reserved_until <= now,
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
                    event_type="campaign.hidden_mentions_resolved",
                    entity_type="campaign_run",
                    entity_id=stored.id,
                    details={
                        "selected_count": len(selected),
                        "reason": resolved_reason,
                        "cooldown_days": cooldown_days,
                        "candidate_count": len(candidates),
                        "eligible_count": len(eligible),
                    },
                    actor="worker",
                )
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                return selected

        # Reservation contention must never suppress the publication itself.
        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            stored.hidden_mention_user_ids_snapshot_json = "[]"
            stored.hidden_mention_resolved_count = 0
            stored.hidden_mention_skip_reason = "mention_reservation_conflict"
            await session.commit()
        return ()

    async def _update_broadcast_hidden_mention_snapshot(
        self,
        run: CampaignRun,
        user_ids: tuple[int, ...],
    ) -> None:
        """Normalize an older snapshot after sender failover or account discovery."""

        async with self._session_factory() as session:
            stored = await self._require_running_run(session, run.id, run.lease_token)
            stored.hidden_mention_user_ids_snapshot_json = json.dumps(list(user_ids))
            stored.hidden_mention_resolved_count = len(user_ids)
            if not user_ids:
                stored.hidden_mention_skip_reason = "excluded_user_after_resolution"
            await self._release_broadcast_mention_reservations(
                session,
                run_ids=[stored.id],
                keep_user_ids=user_ids,
            )
            await session.commit()

    @staticmethod
    def _broadcast_mention_state_is_eligible(
        state: CampaignMentionState | None,
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
        if state.reserved_run_id == run_id:
            return True
        reserved_until = ensure_utc(state.reserved_until)
        return bool(
            state.reserved_run_id is None or reserved_until is None or reserved_until <= now
        )

    @staticmethod
    async def _release_broadcast_mention_reservations(
        session: AsyncSession,
        *,
        run_ids: list[int],
        keep_user_ids: tuple[int, ...] = (),
    ) -> None:
        await release_campaign_mention_reservations(
            session,
            run_ids=run_ids,
            keep_user_ids=keep_user_ids,
        )

    async def _finalize_broadcast_hidden_mentions(
        self,
        session: AsyncSession,
        run: CampaignRun,
        *,
        delivered_at: datetime,
    ) -> None:
        raw_snapshot = run.hidden_mention_user_ids_snapshot_json
        if raw_snapshot is None:
            return
        user_ids = self._parse_hidden_mention_snapshot(raw_snapshot)
        states = list(
            await session.scalars(
                select(CampaignMentionState).where(
                    CampaignMentionState.campaign_id == run.campaign_id,
                    CampaignMentionState.target_id == run.target_id,
                    CampaignMentionState.telegram_user_id.in_(user_ids),
                )
            )
        )
        states_by_user = {int(item.telegram_user_id): item for item in states}
        for user_id in user_ids:
            state = states_by_user.get(user_id)
            if state is None:
                session.add(
                    CampaignMentionState(
                        campaign_id=run.campaign_id,
                        target_id=run.target_id,
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
        await self._release_broadcast_mention_reservations(session, run_ids=[run.id])
        if user_ids:
            add_audit_event(
                session,
                event_type="campaign.hidden_mentions_delivered",
                entity_type="campaign_run",
                entity_id=run.id,
                details={"selected_count": len(user_ids)},
                actor="worker",
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

    @staticmethod
    def _transport_idempotency_key(run: CampaignRun, content_key: str) -> str:
        """Keep content stable while making a confirmed-deleted retry a new send."""

        if run.membership_gate_retry_count <= 0:
            return content_key
        return f"{content_key}:membership-gate-retry:{run.membership_gate_retry_count}"

    @staticmethod
    def _render_delivery_text(value: str, *, idempotency_key: str) -> str:
        try:
            return render_spintax(value, seed=idempotency_key)
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc

    @staticmethod
    def _reply_text(run: CampaignRun, campaign: Campaign) -> str:
        if run.reply_text_snapshot is not None:
            return run.reply_text_snapshot
        return campaign.reply_text

    @staticmethod
    def _reply_delay_seconds(run: CampaignRun, campaign: Campaign) -> int:
        snapshot = run.reply_delay_seconds_snapshot
        return int(snapshot if snapshot is not None else campaign.reply_delay_seconds)

    @classmethod
    def _dialogue_steps(
        cls,
        run: CampaignRun,
        campaign: Campaign,
    ) -> list[DialogueStep]:
        try:
            return parse_dialogue_steps(
                (
                    run.dialogue_steps_snapshot_json
                    if run.dialogue_steps_snapshot_json is not None
                    else campaign.dialogue_steps_json
                ),
                primary_account_id=campaign.primary_account_id,
                primary_text=cls._primary_text(run, campaign),
                primary_html=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_html_snapshot",
                    "primary_html",
                ),
                primary_media_path=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_path_snapshot",
                    "primary_media_path",
                ),
                primary_media_kind=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_kind_snapshot",
                    "primary_media_kind",
                ),
                primary_media_name=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_name_snapshot",
                    "primary_media_name",
                ),
                primary_media_size=cls._primary_content_value(
                    run,
                    campaign,
                    "primary_media_size_snapshot",
                    "primary_media_size",
                ),
                reply_account_id=campaign.reply_account_id,
                reply_text=cls._reply_text(run, campaign),
                reply_delay_seconds=cls._reply_delay_seconds(run, campaign),
            )
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc

    async def _schedule_reactions(
        self,
        session: AsyncSession,
        run: CampaignRun,
        campaign: Campaign,
        target: Target,
        source_account_id: int,
        message_id: int,
        *,
        dialogue_step: int,
    ) -> int:
        """Queue optional ordinary reactions without affecting source delivery."""
        if (
            run.cancel_requested
            or run.status == RunStatus.CANCELLED
            or run.simulated_snapshot is True
        ):
            return 0
        enabled = await session.scalar(select(Campaign.enabled).where(Campaign.id == campaign.id))
        if not enabled:
            return 0
        if (
            not campaign.reactions_enabled
            or campaign.reaction_count < 1
            or (
                campaign.mode == CampaignMode.DIALOGUE
                and campaign.reaction_dialogue_step != dialogue_step
            )
        ):
            return 0
        if campaign.mode == CampaignMode.DIALOGUE:
            candidates = [step.account_id for step in self._dialogue_steps(run, campaign)]
        elif run.sender_mode_snapshot == BroadcastSenderMode.INDEPENDENT:
            # An independent run snapshots its one fixed *sender*.  Reusing
            # that value for reactions leaves no reactor after the source is
            # excluded, so take the campaign's explicitly configured pool.
            candidates = list(
                await session.scalars(
                    select(CampaignAccount.account_id)
                    .where(CampaignAccount.campaign_id == campaign.id)
                    .order_by(CampaignAccount.id)
                )
            )
        elif run.sender_pool_snapshot_json:
            try:
                candidates = [int(value) for value in json.loads(run.sender_pool_snapshot_json)]
            except (TypeError, ValueError, json.JSONDecodeError):
                candidates = []
        else:
            candidates = list(
                await session.scalars(
                    select(CampaignAccount.account_id)
                    .where(CampaignAccount.campaign_id == campaign.id)
                    .order_by(CampaignAccount.id)
                )
            )
        unique_ids = list(
            dict.fromkeys(
                account_id for account_id in candidates if account_id != source_account_id
            )
        )
        if not unique_ids:
            return 0
        rotation = int.from_bytes(
            hashlib.sha256(f"reaction-pool:{run.id}:{message_id}".encode()).digest()[:4],
            "big",
        ) % len(unique_ids)
        unique_ids = unique_ids[rotation:] + unique_ids[:rotation]
        account_conditions = [
            Account.id.in_(unique_ids),
            Account.status == AccountStatus.ACTIVE,
            Account.ownership_confirmed.is_(True),
        ]
        if not self._settings.dry_run:
            account_conditions.append(Account.authorized.is_(True))
        active_accounts = list(await session.scalars(select(Account).where(*account_conditions)))
        accounts_by_id = {int(account.id): account for account in active_accounts}
        route_states = list(
            await session.scalars(
                select(TargetAccountState).where(
                    TargetAccountState.target_id == target.id,
                    TargetAccountState.account_id.in_(unique_ids),
                )
            )
        )
        now = utc_now()
        auto_joinable_reactor_ids = {
            int(state.account_id)
            for state in route_states
            if self._account_auto_join
            and state.membership_status == MembershipStatus.NOT_MEMBER
            and state.membership_error_code == "target_not_joined"
        }
        blocked_ids = {
            int(state.account_id)
            for state in route_states
            if state.send_blocked
            or (
                state.membership_checked_at is not None
                and state.membership_status
                in {
                    MembershipStatus.BANNED,
                    MembershipStatus.JOIN_PENDING,
                    MembershipStatus.UNAVAILABLE,
                }
            )
            or (
                state.membership_checked_at is not None
                and state.membership_status == MembershipStatus.NOT_MEMBER
                and int(state.account_id) not in auto_joinable_reactor_ids
            )
        }
        if not self._settings.dry_run:
            # Broadcast preflight may validate only the selected sender.  Do
            # not turn an optional reaction into a live membership probe for
            # every other account: a reactor needs its own fresh ready route.
            latest_ready_at = now - timedelta(
                seconds=max(1, int(self._settings.preflight_max_age_seconds))
            )
            ready_reactor_ids = {
                int(state.account_id)
                for state in route_states
                if state.membership_status == MembershipStatus.MEMBER
                and state.can_send is True
                and (checked_at := ensure_utc(state.membership_checked_at)) is not None
                and checked_at >= latest_ready_at
            }
            ready_reactor_ids.update(
                int(state.account_id)
                for state in route_states
                if int(state.account_id) in auto_joinable_reactor_ids
                and (checked_at := ensure_utc(state.membership_checked_at)) is not None
                and checked_at >= latest_ready_at
            )
            blocked_ids.update(set(unique_ids) - ready_reactor_ids)
        ordered_ids = sorted(
            unique_ids,
            key=lambda account_id: (
                (
                    (
                        ensure_utc(self.account_resume_at(accounts_by_id[account_id])) is not None
                        and ensure_utc(self.account_resume_at(accounts_by_id[account_id])) > now
                    ),
                    unique_ids.index(account_id),
                )
                if account_id in accounts_by_id
                else (True, unique_ids.index(account_id))
            ),
        )
        existing_reactor_ids = set(
            await session.scalars(
                select(ReactionTask.reactor_account_id).where(
                    ReactionTask.run_id == run.id,
                    ReactionTask.message_id == message_id,
                )
            )
        )
        created = 0
        for ordinal, account_id in enumerate(ordered_ids):
            if (
                account_id not in accounts_by_id
                or account_id in blocked_ids
                or account_id in existing_reactor_ids
                or created >= min(10, campaign.reaction_count)
            ):
                continue
            digest = hashlib.sha256(
                f"reaction:{run.id}:{message_id}:{account_id}".encode()
            ).digest()
            delay_seconds = 15 + int.from_bytes(digest[:2], "big") % 31
            scheduled_for = now + timedelta(seconds=delay_seconds + ordinal * 3)
            resume_at = ensure_utc(self.account_resume_at(accounts_by_id[account_id]))
            if resume_at is not None and resume_at > scheduled_for:
                scheduled_for = resume_at
            session.add(
                ReactionTask(
                    campaign_id=campaign.id,
                    run_id=run.id,
                    target_id=target.id,
                    source_account_id=source_account_id,
                    reactor_account_id=account_id,
                    message_id=message_id,
                    emoji=campaign.reaction_emoji,
                    scheduled_for=scheduled_for,
                    max_attempts=min(3, max(1, self._settings.worker_max_attempts)),
                )
            )
            created += 1
        if created:
            add_audit_event(
                session,
                event_type="reaction.queued",
                entity_type="campaign_run",
                entity_id=run.id,
                details={
                    "message_id": message_id,
                    "count": created,
                    "emoji": campaign.reaction_emoji,
                    "dialogue_step": dialogue_step,
                },
                actor="worker",
            )
        return created

    async def reconcile_missing_broadcast_reactions(self, campaign_id: int) -> int:
        """Restore optional reactions omitted by an older scheduler version.

        Only successful broadcast primaries are eligible.  The regular reaction
        scheduler is idempotent, so this can be safely used after a process
        restart and never duplicates an existing reactor/message pair.
        """

        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if (
                campaign is None
                or campaign.mode != CampaignMode.BROADCAST
                or not campaign.reactions_enabled
                or campaign.reaction_count < 1
            ):
                return 0
            runs = list(
                await session.scalars(
                    select(CampaignRun)
                    .where(
                        CampaignRun.campaign_id == campaign_id,
                        CampaignRun.status == RunStatus.SUCCEEDED,
                        CampaignRun.primary_message_id.is_not(None),
                    )
                    .order_by(CampaignRun.id)
                )
            )
            created = 0
            for run in runs:
                target = await session.get(Target, run.target_id)
                if target is None or run.primary_message_id is None:
                    continue
                source_account_id = int(run.sender_account_id or campaign.primary_account_id)
                created += await self._schedule_reactions(
                    session,
                    run,
                    campaign,
                    target,
                    source_account_id,
                    int(run.primary_message_id),
                    dialogue_step=1,
                )
            if created:
                add_audit_event(
                    session,
                    event_type="reaction.reconciled",
                    entity_type="campaign",
                    entity_id=campaign_id,
                    details={"created": created},
                    actor="recovery",
                )
                await session.commit()
            return created

    @staticmethod
    def _dialogue_step_index(run: CampaignRun, step_count: int) -> int:
        index = max(0, int(run.dialogue_step_index or 0))
        if index == 0 and run.dialogue_steps_snapshot_json is None:
            if run.reply_message_id is not None:
                return step_count
            if run.primary_message_id is not None:
                return 1
        return index

    async def _defer_if_unavailable(self, run: CampaignRun, account_id: int) -> CampaignRun | None:
        """Requeue the run without penalty while the account is paced/cooling down."""
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return None
            account_resume_at = self._account_resume_at(account)
            pair_state = (
                await session.scalar(
                    select(TargetAccountState).where(
                        TargetAccountState.account_id == account_id,
                        TargetAccountState.target_id == run.target_id,
                    )
                )
                if run.dialogue_steps_snapshot_json is not None
                else None
            )
            pair_resume_at = ensure_utc(pair_state.next_send_at) if pair_state is not None else None
            now = utc_now()
            candidates = [
                value
                for value in (account_resume_at, pair_resume_at)
                if value is not None and value > now
            ]
            resume_at = max(candidates) if candidates else None
            if resume_at is None:
                return None
            return await self._defer_run(
                session,
                run,
                resume_at,
                reason=(
                    "chat_timer"
                    if pair_resume_at is not None and resume_at == pair_resume_at
                    else account.cooldown_reason or "pacing"
                ),
            )

    async def _defer_for_visibility(
        self,
        run: CampaignRun,
        campaign: Campaign,
        account: Account,
        target: Target,
    ) -> CampaignRun | None:
        enabled = (
            run.activity_aware_snapshot
            if run.activity_aware_snapshot is not None
            else campaign.activity_aware
        )
        minimum = (
            run.min_messages_between_posts_snapshot
            if run.min_messages_between_posts_snapshot is not None
            else campaign.min_messages_between_posts
        )
        maximum_wait = (
            run.visibility_max_wait_seconds_snapshot
            if run.visibility_max_wait_seconds_snapshot is not None
            else campaign.visibility_max_wait_seconds
        )
        check_interval = (
            run.activity_check_interval_seconds_snapshot
            if run.activity_check_interval_seconds_snapshot is not None
            else campaign.activity_check_interval_seconds
        )
        strict_threshold = enabled and minimum > 0 and maximum_wait == 0
        inspector = getattr(self._gateway, "inspect_activity", None)
        if inspector is None:
            if strict_threshold:
                async with self._session_factory() as session:
                    return await self._defer_run(
                        session,
                        run,
                        utc_now() + timedelta(seconds=max(30, check_interval)),
                        reason="activity_probe_unavailable",
                    )
            return None

        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account.id,
                    TargetAccountState.target_id == target.id,
                )
            )
            latest_managed_message_id = await session.scalar(
                select(func.max(ActionLog.external_message_id)).where(
                    ActionLog.target_id == target.id,
                    ActionLog.kind == ActionKind.PRIMARY_MESSAGE,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.external_message_id.is_not(None),
                )
            )

        # This condition is global for the chat, not per account. Independent
        # senders stay independent, but they cannot stack another publication
        # while a post made by this service is still the chat's last message.
        if self._is_smart_run(run) and latest_managed_message_id is not None:
            try:
                _, organic_after_latest = await self._inspect_organic_activity(
                    inspector=inspector,
                    account=account,
                    target=target,
                    last_message_id=int(latest_managed_message_id),
                )
            except GatewayError as exc:
                if getattr(exc, "code", None) == "post_deleted_by_moderation":
                    await self._record_detected_post_deletion(
                        account_id=account.id,
                        target_id=target.id,
                        message_id=int(latest_managed_message_id),
                        exc=exc,
                    )
                    raise
                if failure_code(exc, "") in (
                    ACCOUNT_FATAL_CODES | ACCOUNT_TARGET_BLOCK_CODES | {"target_unavailable"}
                ):
                    raise
                await self.handle_observed_gateway_failure(account.id, target.id, exc)
                if strict_threshold:
                    async with self._session_factory() as session:
                        return await self._defer_run(
                            session,
                            run,
                            utc_now() + timedelta(seconds=max(30, check_interval)),
                            reason="activity_probe_unavailable",
                        )
                organic_after_latest = 0
            if organic_after_latest <= 0:
                async with self._session_factory() as session:
                    return await self._defer_run(
                        session,
                        run,
                        utc_now() + timedelta(seconds=max(30, min(60, check_interval))),
                        reason="chat_idle_last_managed_post",
                    )
            if strict_threshold and organic_after_latest < minimum:
                async with self._session_factory() as session:
                    return await self._defer_run(
                        session,
                        run,
                        utc_now() + timedelta(seconds=max(30, check_interval)),
                        reason="visibility_wait",
                    )

        if not enabled or minimum <= 0 or state is None or state.last_message_id is None:
            return None
        last_message_id = int(state.last_message_id)
        last_success_at = ensure_utc(state.last_success_at)

        now = utc_now()
        if (
            maximum_wait > 0
            and last_success_at is not None
            and now - last_success_at >= timedelta(seconds=maximum_wait)
        ):
            await self._store_activity_learning(
                account_id=account.id,
                target_id=target.id,
                messages_after=0,
                elapsed_seconds=max(60, int((now - last_success_at).total_seconds())),
                estimated_interval_seconds=maximum_wait,
                sampled_at=now,
            )
            return None
        try:
            activity, messages_after = await self._inspect_organic_activity(
                inspector=inspector,
                account=account,
                target=target,
                last_message_id=last_message_id,
            )
        except GatewayError as exc:
            if getattr(exc, "code", None) == "post_deleted_by_moderation":
                await self._record_detected_post_deletion(
                    account_id=account.id,
                    target_id=target.id,
                    message_id=last_message_id,
                    exc=exc,
                )
                raise
            if failure_code(exc, "") in (
                ACCOUNT_FATAL_CODES | ACCOUNT_TARGET_BLOCK_CODES | {"target_unavailable"}
            ):
                raise
            await self.handle_observed_gateway_failure(account.id, target.id, exc)
            if strict_threshold:
                async with self._session_factory() as session:
                    return await self._defer_run(
                        session,
                        run,
                        utc_now() + timedelta(seconds=max(30, check_interval)),
                        reason="activity_probe_unavailable",
                    )
            # The hard Telegram and operator timers remain authoritative. An
            # unavailable activity probe must not turn a valid launch into a failure.
            return None
        elapsed_seconds = max(
            60,
            int((now - last_success_at).total_seconds()) if last_success_at is not None else 60,
        )
        fallback_interval = (
            run.no_timer_interval_seconds_snapshot
            if run.no_timer_interval_seconds_snapshot is not None
            else campaign.smart_no_timer_interval_seconds
        )
        if messages_after > 0:
            estimated_interval = round(elapsed_seconds * minimum / messages_after)
        else:
            estimated_interval = maximum_wait or fallback_interval
        upper_bound = maximum_wait or max(60, fallback_interval)
        estimated_interval = min(upper_bound, max(60, estimated_interval))
        await self._store_activity_learning(
            account_id=account.id,
            target_id=target.id,
            messages_after=messages_after,
            elapsed_seconds=elapsed_seconds,
            estimated_interval_seconds=estimated_interval,
            sampled_at=activity.checked_at,
        )
        if messages_after >= minimum:
            return None

        resume_at = now + timedelta(seconds=max(30, check_interval))
        if maximum_wait > 0 and last_success_at is not None:
            resume_at = min(
                resume_at,
                last_success_at + timedelta(seconds=maximum_wait),
            )
        async with self._session_factory() as session:
            return await self._defer_run(
                session,
                run,
                resume_at,
                reason="visibility_wait",
            )

    async def _record_detected_post_deletion(
        self,
        *,
        account_id: int,
        target_id: int,
        message_id: int,
        exc: BaseException,
    ) -> None:
        """Turn a previously accepted but later deleted post into a terminal chat signal."""
        async with self._session_factory() as session:
            action = await session.scalar(
                select(ActionLog)
                .where(
                    ActionLog.target_id == target_id,
                    ActionLog.external_message_id == message_id,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                )
                .order_by(ActionLog.id.desc())
            )
            if action is None:
                return
            error = safe_error_message(exc)[:2000]
            action.delayed_delivery_status = "missing"
            action.delayed_delivery_error = error
            action.delayed_delivery_error_code = "post_deleted_by_moderation"
            action.status = ActionStatus.FAILED
            action.error_code = "post_deleted_by_moderation"
            action.error = error
            target = await session.get(Target, target_id)
            if target is not None:
                target.last_checked_at = utc_now()
                target.last_error_code = "post_deleted_by_moderation"
                target.last_error = error[:500]
                target.last_error_account_id = account_id
            await self.refresh_target_quality(session, target_id)
            add_audit_event(
                session,
                event_type="target.post_deleted_by_moderation",
                entity_type="target",
                entity_id=target_id,
                details={
                    "account_id": account_id,
                    "message_id": message_id,
                    "error": error[:500],
                },
                actor="worker",
            )
            await session.commit()

    async def _inspect_organic_activity(
        self,
        *,
        inspector: Callable[..., Awaitable[object]],
        account: Account,
        target: Target,
        last_message_id: int,
    ) -> tuple[object, int]:
        """Count real messages after an anchor, excluding this service's posts."""
        activity = await inspector(
            account=account,
            target=target,
            last_message_id=last_message_id,
        )
        raw_messages_after = int(activity.messages_after(last_message_id))
        observed_ids = getattr(activity, "observed_message_ids", None)
        filters = [
            ActionLog.target_id == target.id,
            ActionLog.status == ActionStatus.SUCCEEDED,
            ActionLog.external_message_id.is_not(None),
            ActionLog.external_message_id > last_message_id,
        ]
        if observed_ids is not None:
            normalized_ids = tuple(
                int(message_id) for message_id in observed_ids if int(message_id) > last_message_id
            )
            if not normalized_ids:
                return activity, 0
            filters.append(ActionLog.external_message_id.in_(normalized_ids))
        else:
            filters.append(ActionLog.external_message_id <= int(activity.latest_message_id))
        async with self._session_factory() as session:
            managed_messages_after = int(
                await session.scalar(select(func.count(ActionLog.id)).where(*filters)) or 0
            )
        return activity, max(0, raw_messages_after - managed_messages_after)

    async def _store_activity_learning(
        self,
        *,
        account_id: int,
        target_id: int,
        messages_after: int,
        elapsed_seconds: int,
        estimated_interval_seconds: int,
        sampled_at: datetime,
    ) -> None:
        """Smooth one observation into the independent account/chat timing lane."""
        async with self._session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == account_id,
                    TargetAccountState.target_id == target_id,
                )
            )
            if state is None:
                state = TargetAccountState(account_id=account_id, target_id=target_id)
                session.add(state)
            previous = state.adaptive_interval_seconds
            learned = int(estimated_interval_seconds)
            if previous is not None:
                learned = round(previous * 0.6 + learned * 0.4)
            state.adaptive_interval_seconds = max(60, learned)
            state.activity_messages_per_minute = round(
                messages_after * 60 / max(60, elapsed_seconds), 3
            )
            state.adaptive_tier = self._adaptive_tier(state.adaptive_interval_seconds)
            state.activity_sampled_at = sampled_at
            await session.commit()

    @staticmethod
    def _adaptive_tier(interval_seconds: int) -> str:
        if interval_seconds <= 90:
            return "turbo"
        if interval_seconds <= 300:
            return "fast"
        if interval_seconds <= 900:
            return "steady"
        return "quiet"

    async def _defer_run(
        self,
        session: AsyncSession,
        run: CampaignRun,
        resume_at: datetime,
        *,
        reason: str,
    ) -> CampaignRun | None:
        stored = await self._require_running_run(session, run.id, run.lease_token)
        stored.dispatch_started_at = None
        stored.dispatch_account_id = None
        stored.dispatch_idempotency_key = None
        if stored.cancel_requested:
            return None
        repeat_until = ensure_utc(stored.repeat_until)
        dialogue_in_progress = bool(
            stored.dialogue_steps_snapshot_json is not None and stored.dialogue_step_index > 0
        )
        if (
            repeat_until is not None
            and not dialogue_in_progress
            and resume_at >= repeat_until
        ):
            # Do not schedule a task at the exact expiry boundary.  It cannot
            # send by definition and would only be claimed later to become an
            # opaque ``repeat_window_elapsed`` cancellation.
            stored.status = RunStatus.CANCELLED
            stored.error = None
            stored.error_code = "repeat_window_elapsed"
            stored.finished_at = utc_now()
            self._release_lease(stored)
            add_audit_event(
                session,
                event_type="run.defer_outside_timer_window",
                entity_type="campaign_run",
                entity_id=stored.id,
                details={
                    "reason": reason,
                    "resume_at": resume_at.isoformat(),
                    "repeat_until": repeat_until.isoformat(),
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(stored)
            return stored
        stored.status = RunStatus.QUEUED
        stored.error_code = reason
        stored.error = None
        stored.scheduled_for = (
            resume_at
            if dialogue_in_progress
            else min(resume_at, repeat_until)
            if repeat_until is not None
            else resume_at
        )
        stored.attempt_count = max(0, stored.attempt_count - 1)
        self._release_lease(stored)
        add_audit_event(
            session,
            event_type="run.deferred",
            entity_type="campaign_run",
            entity_id=stored.id,
            details={"reason": reason, "resume_at": resume_at.isoformat()},
            actor="worker",
        )
        await session.commit()
        await session.refresh(stored)
        return stored

    async def _handle_account_failure(self, account_id: int, exc: BaseException) -> None:
        """Apply account-level safety reactions (auto-disable, cooldown, flood escalation)."""
        code = failure_code(exc, "")
        if code not in ACCOUNT_FATAL_CODES | ACCOUNT_TEMPORARY_CODES:
            return
        if code in {"peer_flood", "account_restricted"} and self._spambot_checks is not None:
            result = await self._spambot_checks.check_account(
                account_id,
                trigger="send_restriction",
                force=True,
            )
            # A positive @SpamBot restriction already persisted its own pause.
            # A "free" response does not invalidate the RPC refusal. Without
            # a supplied expiry, report that refusal rather than invent 24h.
            if result is not None and result.is_restricted:
                return
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return
            now = utc_now()
            event = "account.cooldown"
            if code in ACCOUNT_FATAL_CODES:
                account.status = AccountStatus.DISABLED
                account.authorized = False
                account.last_error = safe_error_message(exc)[:500]
                event = "account.auto_disabled"
            elif code in {"peer_flood", "account_restricted"}:
                hours = float(self._cfg("account_restriction_cooldown_hours", 0.0))
                if hours > 0:
                    resume_at = now + timedelta(hours=hours)
                    current_cooldown = ensure_utc(account.cooldown_until)
                    if current_cooldown is None or current_cooldown < resume_at:
                        account.cooldown_until = resume_at
                        account.cooldown_reason = code
                else:
                    event = "account.send_refused"
                account.last_error = safe_error_message(exc)[:500]
            elif code == "flood_wait":
                retry_after = max(
                    int(self._cfg("flood_min_pause_seconds", 500)),
                    int(getattr(exc, "retry_after", None) or 0),
                )
                resume_at = now + timedelta(seconds=retry_after)
                current_cooldown = ensure_utc(account.cooldown_until)
                if current_cooldown is None or current_cooldown < resume_at:
                    account.cooldown_until = resume_at
                    account.cooldown_reason = "flood_wait"
                account.last_error = safe_error_message(exc)[:500]
                window = timedelta(hours=float(self._cfg("flood_window_hours", 12.0)))
                if (
                    account.flood_window_start is None
                    or (now - (ensure_utc(account.flood_window_start) or now)) > window
                ):
                    account.flood_window_start = now
                    account.flood_hits = 0
                account.flood_hits += 1
                threshold = int(self._cfg("flood_escalation_threshold", 3))
                if account.flood_hits >= threshold:
                    hours = float(self._cfg("flood_cooldown_hours", 24.0))
                    escalated_until = now + timedelta(hours=hours)
                    current_cooldown = ensure_utc(account.cooldown_until)
                    if current_cooldown is None or current_cooldown < escalated_until:
                        account.cooldown_until = escalated_until
                    account.cooldown_reason = "flood_escalation"
                    account.flood_hits = 0
                    event = "account.flood_cooldown"
                else:
                    event = "account.flood_hit"
            add_audit_event(
                session,
                event_type=event,
                entity_type="account",
                entity_id=account_id,
                details={"code": code},
                actor="worker",
            )
            await session.commit()

    def _failure_delivery_mode(self) -> bool | None:
        mode = getattr(self._gateway, "simulated", None)
        return mode if isinstance(mode, bool) else None

    async def _record_action_failure(
        self,
        *,
        run_id: int,
        account_id: int,
        target_id: int,
        kind: ActionKind,
        text: str,
        reply_to_message_id: int | None,
        error: str,
        error_code: str | None,
        external_message_id: int | None,
        attempt_number: int,
        lease_token: str | None,
        external_message_ids: tuple[int, ...] = (),
    ) -> None:
        async with self._session_factory() as session:
            current = await session.get(CampaignRun, run_id)
            if (
                current is None
                or current.status != RunStatus.RUNNING
                or current.lease_token != lease_token
            ):
                return
            session.add(
                ActionLog(
                    run_id=run_id,
                    account_id=account_id,
                    target_id=target_id,
                    kind=kind,
                    status=ActionStatus.FAILED,
                    delayed_delivery_status="partial_missing"
                    if error_code == "album_partial_missing"
                    else "missing"
                    if error_code == "post_deleted_by_moderation"
                    else None,
                    delayed_delivery_checked_at=utc_now()
                    if error_code in {"post_deleted_by_moderation", "album_partial_missing"}
                    else None,
                    simulated=self._failure_delivery_mode(),
                    external_message_id=external_message_id,
                    external_message_ids_json=json.dumps(list(external_message_ids))
                    if external_message_ids
                    else None,
                    reply_to_message_id=reply_to_message_id,
                    text=text,
                    error=error[:2000],
                    error_code=(error_code or None),
                    attempt_number=attempt_number,
                    lease_token=lease_token,
                )
            )
            add_audit_event(
                session,
                event_type="run.step_failed",
                entity_type="campaign_run",
                entity_id=run_id,
                details={
                    "step": kind.value,
                    "error": error[:500],
                    "error_code": error_code,
                },
                actor="worker",
            )
            await session.commit()

    @staticmethod
    async def _require_running_run(
        session: AsyncSession, run_id: int, lease_token: str | None
    ) -> CampaignRun:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            raise NotFoundError("run not found")
        if run.status != RunStatus.RUNNING:
            raise LeaseLostError("run lease was lost")
        if lease_token is None or run.lease_token != lease_token:
            raise LeaseLostError("run lease fencing token is no longer current")
        return run

    @staticmethod
    def _release_lease(run: CampaignRun) -> None:
        run.lease_owner = None
        run.lease_token = None
        run.lease_until = None
        run.heartbeat_at = None

    @staticmethod
    def _validate_config(campaign: Campaign, target: Target | None) -> None:
        if not campaign.enabled:
            raise ConflictError("campaign is disabled")
        if target is None:
            raise ConflictError("campaign target is missing")
        if target.quality_status == TARGET_QUALITY_BAD:
            raise ConflictError("target is auto-paused by the quality rating")
        if campaign.target_id is not None and not target.approved_by_operator:
            raise ConflictError("target must be explicitly approved")
        if not 0 <= campaign.spread_seconds <= 7 * 24 * 3600:
            raise ConflictError("spread window must be between 0 and 7 days")

    @staticmethod
    def _validate_campaign_payload(campaign: Campaign) -> None:
        if campaign.mode == CampaignMode.DIALOGUE:
            try:
                parse_dialogue_steps(
                    campaign.dialogue_steps_json,
                    primary_account_id=campaign.primary_account_id,
                    primary_text=campaign.primary_text,
                    primary_html=campaign.primary_html,
                    primary_media_path=campaign.primary_media_path,
                    primary_media_kind=campaign.primary_media_kind,
                    primary_media_name=campaign.primary_media_name,
                    primary_media_size=campaign.primary_media_size,
                    reply_account_id=campaign.reply_account_id,
                    reply_text=campaign.reply_text,
                    reply_delay_seconds=campaign.reply_delay_seconds,
                )
            except ValueError as exc:
                raise ConflictError(str(exc)) from exc
            if campaign.spread_seconds != 0:
                raise ConflictError("spread window is only supported for broadcast campaigns")
            if (
                campaign.repeat_count != 1
                or campaign.repeat_interval_seconds != 0
                or campaign.auto_sender_failover
            ):
                raise ConflictError("для диалога доступен один проход или умный режим")
            if campaign.smart_timer_mode:
                if campaign.repeat_forever and campaign.smart_duration_seconds != 0:
                    raise ConflictError("repeat_forever requires zero smart duration")
                if not campaign.repeat_forever and not (
                    60 <= campaign.smart_duration_seconds <= 7 * 24 * 3600
                ):
                    raise ConflictError("smart duration must be between 1 minute and 7 days")
                if not 60 <= campaign.smart_no_timer_interval_seconds <= 7 * 24 * 3600:
                    raise ConflictError("no-timer interval must be between 1 minute and 7 days")
                if campaign.activity_aware:
                    if not 1 <= campaign.min_messages_between_posts <= 100_000:
                        raise ConflictError(
                            "activity-aware mode requires 1 to 100000 messages between chains"
                        )
                    if not 30 <= campaign.activity_check_interval_seconds <= 3600:
                        raise ConflictError(
                            "activity check interval must be between 30 and 3600 seconds"
                        )
                    if not 0 <= campaign.visibility_max_wait_seconds <= 7 * 24 * 3600:
                        raise ConflictError(
                            "visibility maximum wait must be between 0 minutes and 7 days"
                        )
            elif campaign.smart_duration_seconds != 0 or campaign.repeat_forever:
                raise ConflictError("smart duration requires timer-driven mode")
            elif campaign.activity_aware:
                raise ConflictError("адаптивность диалога доступна только в умном режиме")
        elif campaign.mode == CampaignMode.BROADCAST:
            CampaignRunner._campaign_content_variants(campaign)
            if campaign.primary_media_path is not None:
                if campaign.primary_media_kind not in {
                    "photo",
                    "video",
                    "document",
                    "voice",
                    "video_note",
                }:
                    raise ConflictError("broadcast media kind is invalid")
                if len(campaign.primary_text) > 1024:
                    raise ConflictError("media caption must contain at most 1024 characters")
            elif campaign.primary_media_kind is not None:
                raise ConflictError("broadcast media kind requires a media file")
            if campaign.reply_text.strip():
                raise ConflictError("broadcast campaign must not contain reply text")
            if campaign.reply_delay_seconds != 0:
                raise ConflictError("задержку ответа можно использовать только в диалоге")
            if not 1 <= campaign.repeat_count <= 100:
                raise ConflictError("repeat count must be between 1 and 100")
            if campaign.smart_timer_mode:
                if campaign.repeat_count != 1:
                    raise ConflictError("timer-driven broadcasts must have repeat_count equal to 1")
                if campaign.repeat_interval_seconds != 0 or campaign.spread_seconds != 0:
                    raise ConflictError("timer-driven broadcasts use dedicated timing fields")
                if campaign.repeat_forever and campaign.smart_duration_seconds != 0:
                    raise ConflictError("repeat_forever requires zero smart duration")
                if not campaign.repeat_forever and not (
                    60 <= campaign.smart_duration_seconds <= 7 * 24 * 3600
                ):
                    raise ConflictError("smart duration must be between 1 minute and 7 days")
                if not 60 <= campaign.smart_no_timer_interval_seconds <= 7 * 24 * 3600:
                    raise ConflictError("no-timer interval must be between 1 minute and 7 days")
                if campaign.activity_aware:
                    if not 1 <= campaign.min_messages_between_posts <= 100_000:
                        raise ConflictError(
                            "activity-aware mode requires 1 to 100000 messages between posts"
                        )
                    if not 30 <= campaign.activity_check_interval_seconds <= 3600:
                        raise ConflictError(
                            "activity check interval must be between 30 and 3600 seconds"
                        )
                    if not 0 <= campaign.visibility_max_wait_seconds <= 7 * 24 * 3600:
                        raise ConflictError(
                            "visibility maximum wait must be between 0 minutes and 7 days"
                        )
            elif campaign.repeat_count == 1 and campaign.repeat_interval_seconds != 0:
                raise ConflictError("one-pass broadcast must have a zero repeat interval")
            elif campaign.repeat_count > 1 and not (
                60 <= campaign.repeat_interval_seconds <= 7 * 24 * 3600
            ):
                raise ConflictError("repeat interval must be between 1 minute and 7 days")
            if not campaign.smart_timer_mode and (
                campaign.smart_duration_seconds != 0 or campaign.repeat_forever
            ):
                raise ConflictError("smart duration requires timer-driven mode")
            if campaign.activity_aware and not campaign.smart_timer_mode:
                raise ConflictError("activity-aware mode requires timer-driven broadcast")

    def _validate_run_payload(self, run: CampaignRun, campaign: Campaign) -> None:
        if campaign.mode == CampaignMode.DIALOGUE:
            self._dialogue_steps(run, campaign)
        else:
            self._run_content_variant(run, campaign)

    def _require_live_session(self) -> bool:
        return not bool(self._cfg("dry_run", True))

    @staticmethod
    def _validate_sender(
        account: Account | None,
        *,
        require_live_session: bool = False,
    ) -> None:
        if account is None:
            raise ConflictError("campaign account is missing")
        if not account.ownership_confirmed:
            raise ConflictError("account ownership must be confirmed")
        if account.status != AccountStatus.ACTIVE:
            raise ConflictError(
                "аккаунт отправителя выключен или его сессия больше не действительна"
            )
        if require_live_session:
            if not account.authorized:
                raise ConflictError(
                    "account session is not authorized; import tdata and check session"
                )
            if not account.session_encrypted:
                raise ConflictError("account has no imported session")
            if account.api_id is None or not account.api_hash_encrypted:
                raise ConflictError(
                    "account session API credentials are missing; re-import account session"
                )

    @classmethod
    def validate_campaign(
        cls,
        campaign: Campaign,
        primary: Account | None,
        reply: Account | None,
        target: Target | None,
        *,
        require_live_session: bool = False,
    ) -> None:
        cls._validate_config(campaign, target)
        cls._validate_campaign_payload(campaign)
        cls._validate_sender(primary, require_live_session=require_live_session)
        if campaign.mode == CampaignMode.DIALOGUE:
            cls._validate_sender(reply, require_live_session=require_live_session)
            if primary is not None and reply is not None and primary.id == reply.id:
                raise ConflictError("primary and reply accounts must be different")


class RunQueue:
    """Persistent queue with idempotency, leases, retries and cancellation."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        lease_seconds: int,
        max_attempts: int,
        retry_base_seconds: int,
        retry_max_seconds: int = 300,
        max_retry_after_seconds: int = 86400,
        require_live_session: bool = False,
        require_proxy_route: bool = False,
        account_auto_join: bool = False,
        account_pace_seconds: float = 0.0,
        account_daily_limit: int = 200,
        account_micro_break_every: int = 0,
        account_micro_break_seconds: float = 0.0,
        preflight_required: bool = False,
        preflight_max_age_seconds: int = 3600,
        proxy_service: ProxyService | None = None,
        campaign_id_scope: frozenset[int] | None = None,
        delivery_mode: bool | None = None,
    ) -> None:
        self._delivery_mode = delivery_mode
        self._session_factory = session_factory
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds
        self._retry_max_seconds = retry_max_seconds
        self._max_retry_after_seconds = max_retry_after_seconds
        self._require_live_session = require_live_session
        self._require_proxy_route = require_proxy_route
        self._account_auto_join = account_auto_join
        self._account_pace_seconds = max(0.0, account_pace_seconds)
        self._account_daily_limit = max(0, account_daily_limit)
        self._account_micro_break_every = max(0, account_micro_break_every)
        self._account_micro_break_seconds = max(0.0, account_micro_break_seconds)
        self._preflight_required = preflight_required
        self._preflight_max_age_seconds = max(60, preflight_max_age_seconds)
        self._proxy_service = proxy_service
        self._campaign_id_scope = (
            frozenset(campaign_id_scope) if campaign_id_scope is not None else None
        )
        self._claim_lock = asyncio.Lock()
        self._account_runner: CampaignRunner | None = None

    async def enqueue(self, campaign_id: int, request_key: str) -> CampaignRun:
        runs = await self.enqueue_campaign(campaign_id, request_key)
        return runs[0]

    async def enqueue_campaign(
        self,
        campaign_id: int,
        request_key: str,
        *,
        allow_deferred_start: bool = False,
    ) -> list[CampaignRun]:
        request_key = request_key.strip()
        if not 8 <= len(request_key) <= 64:
            raise ConflictError("request_key must contain 8 to 64 characters")
        if not re.fullmatch(r"[A-Za-z0-9._:-]+", request_key):
            raise ConflictError("request_key contains unsupported characters")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("campaign not found")
            existing_launch = list(
                await session.scalars(
                    select(CampaignRun)
                    .where(CampaignRun.launch_key == request_key)
                    .order_by(CampaignRun.id)
                )
            )
            if existing_launch:
                if any(run.campaign_id != campaign_id for run in existing_launch):
                    raise ConflictError("request_key already belongs to another campaign")
                return existing_launch
            primary = await session.get(Account, campaign.primary_account_id)
            reply = (
                await session.get(Account, campaign.reply_account_id)
                if campaign.reply_account_id is not None
                else None
            )
            dialogue_steps: list[DialogueStep] = []
            dialogue_steps_snapshot_json: str | None = None
            if campaign.mode == CampaignMode.DIALOGUE:
                try:
                    dialogue_steps = parse_dialogue_steps(
                        campaign.dialogue_steps_json,
                        primary_account_id=campaign.primary_account_id,
                        primary_text=campaign.primary_text,
                        primary_html=campaign.primary_html,
                        primary_media_path=campaign.primary_media_path,
                        primary_media_kind=campaign.primary_media_kind,
                        primary_media_name=campaign.primary_media_name,
                        primary_media_size=campaign.primary_media_size,
                        reply_account_id=campaign.reply_account_id,
                        reply_text=campaign.reply_text,
                        reply_delay_seconds=campaign.reply_delay_seconds,
                    )
                except ValueError as exc:
                    raise ConflictError(str(exc)) from exc
                dialogue_steps_snapshot_json = serialize_dialogue_steps(dialogue_steps)
                for account_id in dict.fromkeys(step.account_id for step in dialogue_steps):
                    dialogue_account = await session.get(Account, account_id)
                    CampaignRunner._validate_sender(
                        dialogue_account,
                        require_live_session=self._require_live_session,
                    )
            targets = await self._resolve_campaign_targets(session, campaign)
            if not targets:
                raise ConflictError("campaign target list has no approved targets")
            for target in targets:
                if campaign.mode == CampaignMode.BROADCAST:
                    CampaignRunner._validate_config(campaign, target)
                    CampaignRunner._validate_campaign_payload(campaign)
                else:
                    CampaignRunner.validate_campaign(
                        campaign,
                        primary,
                        reply,
                        target,
                        require_live_session=self._require_live_session,
                    )

            content_variants = (
                CampaignRunner._campaign_content_variants(campaign)
                if campaign.mode == CampaignMode.BROADCAST
                else []
            )
            content_variants_snapshot_json = (
                serialize_content_variants(content_variants) if content_variants else None
            )
            configured_sender_pool = await self._resolve_sender_pool(session, campaign)
            independent = (
                campaign.mode == CampaignMode.BROADCAST
                and campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
            )
            smart_sender_selection = campaign.mode == CampaignMode.BROADCAST
            sender_pool = await self._validate_sender_pool(
                session,
                configured_sender_pool,
                require_live_session=self._require_live_session,
                tolerate_unavailable=smart_sender_selection,
            )
            sender_pool = await self._validate_account_proxy_routes(
                session,
                campaign,
                sender_pool,
                tolerate_unavailable=smart_sender_selection,
            )
            cycles = [0] if campaign.smart_timer_mode else list(range(campaign.repeat_count))
            task_specs = [
                (target, cycle_index, account_id)
                for cycle_index in cycles
                for target in targets
                for account_id in (sender_pool if independent else [None])
            ]
            task_count = len(task_specs)
            if task_count > 5000:
                raise ConflictError(
                    "broadcast is too large; reduce chats, accounts or repeats to 5000 streams"
                )
            keys = {
                (target.id, cycle_index, forced_account_id): (
                    request_key
                    if task_count == 1
                    else hashlib.sha256(
                        f"{request_key}:{target.id}:{cycle_index}:{forced_account_id or 0}".encode()
                    ).hexdigest()
                )
                for target, cycle_index, forced_account_id in task_specs
            }
            existing_runs = list(
                await session.scalars(
                    select(CampaignRun).where(CampaignRun.request_key.in_(list(keys.values())))
                )
            )
            for existing in existing_runs:
                if existing.campaign_id != campaign_id:
                    raise ConflictError("request_key already belongs to another campaign")
            existing_by_key = {run.request_key: run for run in existing_runs}
            sender_pool_snapshot_json = json.dumps(
                list(dict.fromkeys(step.account_id for step in dialogue_steps))
                if campaign.mode == CampaignMode.DIALOGUE
                else sender_pool
            )
            now = utc_now()
            capacity_account_ids = list(
                dict.fromkeys(
                    [
                        *sender_pool,
                        *(step.account_id for step in dialogue_steps),
                    ]
                )
            )
            sender_accounts = {
                account_id: account
                for account_id in capacity_account_ids
                if (account := await session.get(Account, account_id)) is not None
            }
            capacity_slots = {
                account_id: AccountCapacitySlot.from_account(
                    account,
                    now,
                    daily_limit=self._account_daily_limit,
                )
                for account_id, account in sender_accounts.items()
            }
            ready_now_account_ids = frozenset(
                account_id
                for account_id, slot in capacity_slots.items()
                if slot.candidate_at(now, daily_limit=self._account_daily_limit) <= now
            )
            planning_now = now
            if allow_deferred_start:
                deferred_start_times = [
                    slot.candidate_at(now, daily_limit=self._account_daily_limit)
                    for account_id, slot in capacity_slots.items()
                    if account_id not in ready_now_account_ids
                ]
                if deferred_start_times:
                    planning_now = min(deferred_start_times)
            target_assignments: dict[int, int] = {}
            repeat_forever = bool(campaign.smart_timer_mode and campaign.repeat_forever)
            repeat_until = (
                planning_now + timedelta(seconds=campaign.smart_duration_seconds)
                if campaign.smart_timer_mode and not repeat_forever
                else None
            )
            timing_states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.target_id.in_([target.id for target in targets]),
                        TargetAccountState.account_id.in_(
                            list(
                                dict.fromkeys(
                                    [
                                        *sender_pool,
                                        *(step.account_id for step in dialogue_steps),
                                    ]
                                )
                            )
                        ),
                    )
                )
            )
            state_by_pair = {(state.target_id, state.account_id): state for state in timing_states}
            preflight_skips: dict[int, str] = {}
            preflight_ready_accounts: dict[int, frozenset[int]] = {}
            preflight_deferred_accounts: dict[int, frozenset[int]] = {}
            if self._preflight_required:
                (
                    preflight_skips,
                    preflight_ready_accounts,
                    preflight_deferred_accounts,
                ) = self._preflight_routing(
                    campaign,
                    targets,
                    sender_pool,
                    dialogue_steps,
                    state_by_pair,
                    now,
                    ready_now_account_ids,
                    allow_deferred_start=allow_deferred_start,
                )
                if len(preflight_skips) == len(targets):
                    preview = ", ".join(f"#{target_id}" for target_id in list(preflight_skips)[:8])
                    suffix = (
                        f" и ещё {len(preflight_skips) - 8}" if len(preflight_skips) > 8 else ""
                    )
                    raise ConflictError(
                        "Проверить чаты и аккаунты перед запуском: отправка сейчас "
                        "недоступна — нет ни одного чата с готовой "
                        "парой аккаунт–чат без защитной паузы Telegram: "
                        f"{preview}{suffix}; проверка действует "
                        f"{self._preflight_max_age_seconds // 60} мин."
                    )
            runs: list[CampaignRun] = []
            capacity_window_exceeded_count = 0
            capacity_window_target_ids: set[int] = set()
            target_indexes = {target.id: index for index, target in enumerate(targets)}
            account_indexes = {account_id: index for index, account_id in enumerate(sender_pool)}
            for target, cycle_index, forced_account_id in task_specs:
                target_index = target_indexes[target.id]
                key = keys[(target.id, cycle_index, forced_account_id)]
                run = existing_by_key.get(key)
                if run is None:
                    preflight_skip_code = preflight_skips.get(target.id)
                    # Independent broadcasting is a matrix of account/chat
                    # streams.  A failed check for one account must cancel only
                    # its own stream, not discard every other verified account
                    # for the same chat.
                    if (
                        preflight_skip_code is None
                        and independent
                        and self._preflight_required
                        and forced_account_id
                        not in preflight_ready_accounts.get(
                            target.id,
                            frozenset(),
                        )
                    ):
                        preflight_skip_code = (
                            "preflight_account_cooldown"
                            if forced_account_id
                            in preflight_deferred_accounts.get(target.id, frozenset())
                            else "preflight_independent_not_ready"
                        )
                    content_variant_index = (
                        (target_index + cycle_index + account_indexes.get(forced_account_id, 0))
                        % len(content_variants)
                        if content_variants
                        else 0
                    )
                    content_variant = (
                        content_variants[content_variant_index] if content_variants else None
                    )
                    content_media = (
                        content_variant.media[0]
                        if content_variant is not None and content_variant.media
                        else None
                    )
                    spread_delay = (
                        0
                        if campaign.smart_timer_mode
                        else self._spread_delay_seconds(
                            campaign.spread_seconds, target_index, len(targets)
                        )
                    )
                    base_scheduled_for = now + timedelta(
                        seconds=(spread_delay + cycle_index * campaign.repeat_interval_seconds)
                    )
                    sticky_account_id = None if independent else target_assignments.get(target.id)
                    if forced_account_id is not None:
                        rotated_pool = [forced_account_id]
                    elif sticky_account_id is not None:
                        rotated_pool = [
                            sticky_account_id,
                            *(item for item in sender_pool if item != sticky_account_id),
                        ]
                    else:
                        rotation = target_index % len(sender_pool)
                        rotated_pool = sender_pool[rotation:] + sender_pool[:rotation]
                    candidates: list[tuple[datetime, int, int, int]] = []
                    blocked_dialogue_step = next(
                        (
                            step
                            for step in dialogue_steps
                            if (
                                (pair_state := state_by_pair.get((target.id, step.account_id)))
                                is not None
                                and (
                                    pair_state.send_blocked
                                    or pair_state.membership_status
                                    in {
                                        MembershipStatus.BANNED,
                                        MembershipStatus.JOIN_PENDING,
                                        MembershipStatus.UNAVAILABLE,
                                    }
                                    or (
                                        pair_state.membership_status == MembershipStatus.NOT_MEMBER
                                        and not self._account_auto_join
                                    )
                                )
                            )
                        ),
                        None,
                    )
                    if preflight_skip_code is not None or blocked_dialogue_step is not None:
                        candidate_pool = []
                    elif self._preflight_required:
                        ready_accounts = preflight_ready_accounts.get(target.id, frozenset())
                        candidate_pool = [
                            account_id
                            for account_id in rotated_pool
                            if account_id in ready_accounts
                        ]
                    else:
                        candidate_pool = rotated_pool
                    for order, account_id in enumerate(candidate_pool):
                        pair_state = state_by_pair.get((target.id, account_id))
                        if pair_state is not None and pair_state.send_blocked:
                            continue
                        if pair_state is not None and pair_state.membership_checked_at is not None:
                            if pair_state.membership_status in {
                                MembershipStatus.BANNED,
                                MembershipStatus.JOIN_PENDING,
                                MembershipStatus.UNAVAILABLE,
                            }:
                                continue
                            if (
                                pair_state.membership_status == MembershipStatus.NOT_MEMBER
                                and not self._account_auto_join
                            ):
                                continue
                        candidate_time = base_scheduled_for
                        known_next_send = (
                            ensure_utc(pair_state.next_send_at) if pair_state is not None else None
                        )
                        if known_next_send is not None and known_next_send > candidate_time:
                            candidate_time = known_next_send
                        slot = capacity_slots.get(account_id)
                        if slot is not None:
                            candidate_time = slot.candidate_at(
                                candidate_time,
                                daily_limit=self._account_daily_limit,
                            )
                        if campaign.mode == CampaignMode.DIALOGUE:
                            cumulative_delay = 0
                            for step_index, step in enumerate(dialogue_steps):
                                if step_index > 0:
                                    cumulative_delay += step.delay_seconds
                                step_slot = capacity_slots.get(step.account_id)
                                if step_slot is None:
                                    continue
                                step_time = step_slot.candidate_at(
                                    candidate_time + timedelta(seconds=cumulative_delay),
                                    daily_limit=self._account_daily_limit,
                                )
                                candidate_time = max(
                                    candidate_time,
                                    step_time - timedelta(seconds=cumulative_delay),
                                )
                        membership_rank = (
                            0
                            if pair_state is not None
                            and pair_state.membership_status == MembershipStatus.MEMBER
                            and pair_state.can_send is True
                            else 1
                        )
                        candidates.append((candidate_time, membership_rank, order, account_id))
                    candidates.sort(key=lambda item: (item[1], item[0], item[2]))
                    # Keep the selected lane on a cancelled independent stream
                    # as well.  Without this, reports lost which account/chat
                    # pair failed preflight and appeared to have a mysterious
                    # "pair" error with no account attached.
                    sender_account_id = (
                        candidates[0][3]
                        if candidates
                        else forced_account_id
                        if independent
                        else None
                    )
                    scheduled_for = candidates[0][0] if candidates else base_scheduled_for
                    if sender_account_id is not None:
                        if not independent:
                            target_assignments.setdefault(target.id, sender_account_id)
                        if campaign.mode == CampaignMode.DIALOGUE:
                            cumulative_delay = 0
                            for step_index, step in enumerate(dialogue_steps):
                                if step_index > 0:
                                    cumulative_delay += step.delay_seconds
                                slot = capacity_slots.get(step.account_id)
                                if slot is not None:
                                    slot.reserve(
                                        scheduled_for + timedelta(seconds=cumulative_delay),
                                        pace_seconds=self._account_pace_seconds,
                                        micro_break_every=self._account_micro_break_every,
                                        micro_break_seconds=self._account_micro_break_seconds,
                                    )
                        else:
                            slot = capacity_slots.get(sender_account_id)
                            if slot is not None:
                                slot.reserve(
                                    scheduled_for,
                                    pace_seconds=self._account_pace_seconds,
                                    micro_break_every=self._account_micro_break_every,
                                    micro_break_seconds=self._account_micro_break_seconds,
                                )
                    capacity_window_exceeded = (
                        bool(candidates)
                        and repeat_until is not None
                        and scheduled_for >= repeat_until
                    )
                    if capacity_window_exceeded and repeat_until is not None:
                        scheduled_for = repeat_until
                        capacity_window_exceeded_count += 1
                        capacity_window_target_ids.add(int(target.id))
                    run = CampaignRun(
                        campaign_id=campaign_id,
                        simulated_snapshot=self._delivery_mode,
                        target_id=target.id,
                        request_key=key,
                        launch_key=request_key,
                        status=(
                            RunStatus.CANCELLED
                            if preflight_skip_code is not None or capacity_window_exceeded
                            else RunStatus.QUEUED
                            if candidates
                            else RunStatus.FAILED
                        ),
                        next_step=RunStep.PRIMARY,
                        max_attempts=self._max_attempts,
                        scheduled_for=scheduled_for,
                        sender_account_id=sender_account_id,
                        cycle_index=cycle_index,
                        repeat_until=repeat_until,
                        repeat_forever_snapshot=repeat_forever,
                        primary_text_snapshot=(
                            content_variant.text if content_variant else campaign.primary_text
                        ),
                        primary_html_snapshot=(
                            content_variant.html_text if content_variant else campaign.primary_html
                        ),
                        hidden_mentions_enabled_snapshot=(
                            campaign.hidden_mentions_enabled
                            if campaign.mode == CampaignMode.BROADCAST
                            else False
                        ),
                        mention_cooldown_days_snapshot=(
                            campaign.mention_cooldown_days
                            if campaign.mode == CampaignMode.BROADCAST
                            else None
                        ),
                        primary_media_path_snapshot=(content_media.path if content_media else None),
                        primary_media_kind_snapshot=(content_media.kind if content_media else None),
                        primary_media_name_snapshot=(content_media.name if content_media else None),
                        primary_media_size_snapshot=(content_media.size if content_media else None),
                        media_items_snapshot_json=(
                            CampaignRunner._serialize_media_items(content_variant)
                            if content_variant
                            else None
                        ),
                        content_variants_snapshot_json=content_variants_snapshot_json,
                        content_variant_index=content_variant_index,
                        content_variant_id_snapshot=(
                            content_variant.variant_id if content_variant else None
                        ),
                        content_variant_name_snapshot=(
                            content_variant.name if content_variant else None
                        ),
                        reply_text_snapshot=campaign.reply_text,
                        reply_delay_seconds_snapshot=campaign.reply_delay_seconds,
                        no_timer_interval_seconds_snapshot=(
                            campaign.smart_no_timer_interval_seconds
                        ),
                        auto_sender_failover_snapshot=(
                            False if independent else campaign.auto_sender_failover
                        ),
                        sender_pool_snapshot_json=(
                            json.dumps([forced_account_id])
                            if forced_account_id is not None
                            else sender_pool_snapshot_json
                        ),
                        sender_mode_snapshot=campaign.sender_mode,
                        activity_aware_snapshot=campaign.activity_aware,
                        min_messages_between_posts_snapshot=(campaign.min_messages_between_posts),
                        activity_check_interval_seconds_snapshot=(
                            campaign.activity_check_interval_seconds
                        ),
                        visibility_max_wait_seconds_snapshot=(campaign.visibility_max_wait_seconds),
                        dialogue_steps_snapshot_json=dialogue_steps_snapshot_json,
                        dialogue_step_index=0,
                        error=(
                            "чат пропущен: нет свежего готового маршрута отправителя"
                            if preflight_skip_code is not None
                            else "not enough account capacity inside the launch window"
                            if capacity_window_exceeded
                            else None
                            if candidates
                            else (
                                "a dialogue account is permanently blocked in this chat"
                                if campaign.mode == CampaignMode.DIALOGUE
                                else "all selected accounts are permanently blocked in this chat"
                            )
                        ),
                        error_code=(
                            preflight_skip_code
                            if preflight_skip_code is not None
                            else "capacity_window_exceeded"
                            if capacity_window_exceeded
                            else None
                            if candidates
                            else (
                                "dialogue_sender_blocked"
                                if campaign.mode == CampaignMode.DIALOGUE
                                else "all_senders_blocked"
                            )
                        ),
                        finished_at=(
                            None
                            if candidates
                            and preflight_skip_code is None
                            and not capacity_window_exceeded
                            else now
                        ),
                    )
                    session.add(run)
                runs.append(run)
            executable_active_runs = [
                run
                for run in runs
                if run.status == RunStatus.QUEUED
                and (
                    run.repeat_forever_snapshot
                    or run.repeat_until is None
                    or ensure_utc(run.scheduled_for) < ensure_utc(run.repeat_until)
                )
            ]
            if campaign.smart_timer_mode and not executable_active_runs:
                # The planner is the only component that sees the whole
                # account × chat matrix and its virtual account reservations.
                # Never return a seemingly successful active launch with zero
                # tasks that can actually run before the deadline.  Individual
                # route failures remain visible in the preflight review; this
                # guard stops the empty launch itself.
                if capacity_window_exceeded_count > 0:
                    reason = (
                        "все готовые маршруты освободятся только после окончания её окна; "
                        "увеличьте срок, уменьшите объём или дождитесь сброса лимита аккаунта"
                    )
                else:
                    reason = (
                        "после проверки не осталось ни одной готовой пары аккаунт–чат; "
                        "проверьте доступ, вступления и ограничения Telegram"
                    )
                raise ConflictError(f"активная рассылка не может стартовать: {reason}")
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                replays = list(
                    await session.scalars(
                        select(CampaignRun).where(CampaignRun.request_key.in_(list(keys.values())))
                    )
                )
                if len(replays) == task_count and all(
                    replay.campaign_id == campaign_id for replay in replays
                ):
                    replay_by_key = {replay.request_key: replay for replay in replays}
                    return [
                        replay_by_key[keys[(target.id, cycle_index, forced_account_id)]]
                        for target, cycle_index, forced_account_id in task_specs
                    ]
                raise ConflictError("request_key conflict") from exc

            add_audit_event(
                session,
                event_type="campaign.enqueued",
                entity_type="campaign",
                entity_id=campaign_id,
                details={
                    "request_key": request_key,
                    "run_count": len(runs),
                    "target_ids": [target.id for target in targets],
                    "ready_target_count": len(targets) - len(preflight_skips),
                    "skipped_target_count": len(preflight_skips),
                    "skipped_target_ids": sorted(preflight_skips),
                    "capacity_window_exceeded_count": capacity_window_exceeded_count,
                    "capacity_window_target_ids": sorted(capacity_window_target_ids),
                    "eligible_sender_ids": sender_pool,
                    "repeat_count": campaign.repeat_count,
                    "repeat_interval_seconds": campaign.repeat_interval_seconds,
                    "smart_timer_mode": campaign.smart_timer_mode,
                    "smart_duration_seconds": campaign.smart_duration_seconds,
                    "smart_no_timer_interval_seconds": (campaign.smart_no_timer_interval_seconds),
                    "auto_sender_failover": campaign.auto_sender_failover,
                    "activity_aware": campaign.activity_aware,
                    "min_messages_between_posts": campaign.min_messages_between_posts,
                    "repeat_until": repeat_until.isoformat() if repeat_until else None,
                    "repeat_forever": repeat_forever,
                },
            )
            await session.commit()
            for run in runs:
                await session.refresh(run)
            return runs

    def _preflight_routing(
        self,
        campaign: Campaign,
        targets: list[Target],
        sender_pool: list[int],
        dialogue_steps: list[DialogueStep],
        state_by_pair: dict[tuple[int, int], TargetAccountState],
        now: datetime,
        ready_now_account_ids: frozenset[int],
        *,
        allow_deferred_start: bool = False,
    ) -> tuple[
        dict[int, str],
        dict[int, frozenset[int]],
        dict[int, frozenset[int]],
    ]:
        fresh_after = now - timedelta(seconds=self._preflight_max_age_seconds)
        required_media_kinds = frozenset()
        required_media_kinds_by_account: dict[int, frozenset[str]] = {}
        if campaign.mode == CampaignMode.DIALOGUE:
            media_by_account: dict[int, set[str]] = {}
            for step in dialogue_steps:
                if step.media_kind is None:
                    continue
                media_by_account.setdefault(int(step.account_id), set()).add(step.media_kind)
            required_media_kinds_by_account = {
                account_id: frozenset(kinds) for account_id, kinds in media_by_account.items()
            }
        else:
            variants = CampaignRunner._campaign_content_variants(campaign)
            required_media_kinds = frozenset(
                kind
                for variant in variants
                for kind in (
                    (["contact"] if variant.contact is not None else [])
                    + [media.kind for media in variant.media]
                )
            )

        def route_is_ready(target_id: int, account_id: int) -> bool:
            state = state_by_pair.get((target_id, account_id))
            checked_at = ensure_utc(state.membership_checked_at) if state is not None else None
            checked_media_kinds = frozenset(
                item
                for item in (
                    (state.preflight_media_kinds or "") if state is not None else ""
                ).split(",")
                if item
            )
            member_route = bool(
                state is not None
                and state.membership_status == MembershipStatus.MEMBER
                and state.can_send is True
                and not state.send_blocked
            )
            auto_join_route = bool(
                state is not None
                and self._account_auto_join
                and state.membership_status == MembershipStatus.NOT_MEMBER
                and state.membership_error_code == "target_not_joined"
            )
            return bool(
                state is not None
                and checked_at is not None
                and checked_at >= fresh_after
                and (member_route or auto_join_route)
                and required_media_kinds_by_account.get(
                    account_id,
                    required_media_kinds,
                ).issubset(checked_media_kinds)
            )

        skipped: dict[int, str] = {}
        ready_accounts_by_target: dict[int, frozenset[int]] = {}
        deferred_accounts_by_target: dict[int, frozenset[int]] = {}
        dialogue_accounts = list(dict.fromkeys(step.account_id for step in dialogue_steps))
        for target in targets:
            relevant_accounts = (
                dialogue_accounts if campaign.mode == CampaignMode.DIALOGUE else sender_pool
            )
            accessible_accounts = frozenset(
                account_id
                for account_id in relevant_accounts
                if route_is_ready(target.id, account_id)
            )
            ready_accounts = (
                accessible_accounts
                if allow_deferred_start
                else frozenset(
                    account_id
                    for account_id in accessible_accounts
                    if account_id in ready_now_account_ids
                )
            )
            deferred_accounts = accessible_accounts - ready_accounts
            ready_accounts_by_target[target.id] = ready_accounts
            deferred_accounts_by_target[target.id] = deferred_accounts
            if campaign.mode == CampaignMode.DIALOGUE:
                covered = len(ready_accounts) == len(dialogue_accounts)
                error_code = "preflight_dialogue_not_ready"
            elif campaign.sender_mode == BroadcastSenderMode.INDEPENDENT:
                # A partial independent matrix remains useful: each ready
                # account/chat pair is queued, while the unavailable pair is
                # recorded as a transparent skipped stream below.
                covered = bool(ready_accounts)
                error_code = "preflight_no_ready_sender"
            else:
                covered = bool(ready_accounts)
                error_code = "preflight_no_ready_sender"
            if not covered:
                skipped[target.id] = (
                    "preflight_account_cooldown" if deferred_accounts else error_code
                )
        return skipped, ready_accounts_by_target, deferred_accounts_by_target

    async def _validate_account_proxy_routes(
        self,
        session: AsyncSession,
        campaign: Campaign,
        sender_pool: list[int],
        *,
        tolerate_unavailable: bool = False,
    ) -> list[int]:
        """Keep only proxy-ready broadcast senders, but never weaken fixed-account flows."""
        from proxima_traffic.proxies import ProxyRouteUnavailable

        if not self._require_proxy_route or not self._require_live_session:
            return sender_pool
        if self._proxy_service is None:
            raise ConflictError("proxy service is not configured")
        account_ids = list(dict.fromkeys(sender_pool))
        if campaign.mode == CampaignMode.DIALOGUE:
            try:
                steps = parse_dialogue_steps(
                    campaign.dialogue_steps_json,
                    primary_account_id=campaign.primary_account_id,
                    primary_text=campaign.primary_text,
                    primary_html=campaign.primary_html,
                    primary_media_path=campaign.primary_media_path,
                    primary_media_kind=campaign.primary_media_kind,
                    primary_media_name=campaign.primary_media_name,
                    primary_media_size=campaign.primary_media_size,
                    reply_account_id=campaign.reply_account_id,
                    reply_text=campaign.reply_text,
                    reply_delay_seconds=campaign.reply_delay_seconds,
                )
            except ValueError as exc:
                raise ConflictError(str(exc)) from exc
            account_ids = list(dict.fromkeys([*account_ids, *(step.account_id for step in steps)]))
        available: list[int] = []
        for account_id in account_ids:
            try:
                recovery = getattr(self._proxy_service, "ensure_account_route", None)
                if callable(recovery):
                    await recovery(account_id, actor="broadcast-route-recovery")
                else:
                    await self._proxy_service.require_account_route(account_id)
            except ProxyRouteUnavailable as exc:
                if tolerate_unavailable and account_id in sender_pool:
                    continue
                account = await session.get(Account, account_id)
                label = account.label if account is not None else f"#{account_id}"
                raise ConflictError(
                    f"у аккаунта «{label}» нет рабочего прокси; "
                    "автозамена проверила пул, но не нашла исправный вариант"
                ) from exc
            if account_id in sender_pool:
                available.append(account_id)
        if not available:
            raise ConflictError("campaign has no sender accounts with a healthy proxy")
        return available

    @staticmethod
    async def _validate_sender_pool(
        session: AsyncSession,
        sender_pool: list[int],
        *,
        require_live_session: bool,
        tolerate_unavailable: bool = False,
    ) -> list[int]:
        available: list[int] = []
        for account_id in sender_pool:
            account = await session.get(Account, account_id)
            try:
                CampaignRunner._validate_sender(
                    account,
                    require_live_session=require_live_session,
                )
            except ConflictError:
                if not tolerate_unavailable:
                    raise
                continue
            available.append(account_id)
        if not available:
            raise ConflictError("campaign has no available sender accounts")
        return available

    @staticmethod
    def _spread_delay_seconds(spread_seconds: int, index: int, total: int) -> int:
        """Evenly distribute run starts across the spread window (0 = all at once)."""
        if spread_seconds <= 0 or total <= 1:
            return 0
        return int(spread_seconds * index / (total - 1))

    @staticmethod
    async def _resolve_sender_pool(session: AsyncSession, campaign: Campaign) -> list[int]:
        if campaign.mode != CampaignMode.BROADCAST:
            return [campaign.primary_account_id]
        pool = list(
            await session.scalars(
                select(CampaignAccount.account_id)
                .where(CampaignAccount.campaign_id == campaign.id)
                .order_by(CampaignAccount.id)
            )
        )
        return pool or [campaign.primary_account_id]

    @staticmethod
    async def _resolve_campaign_targets(
        session: AsyncSession,
        campaign: Campaign,
    ) -> list[Target]:
        if campaign.target_id is not None:
            target = await session.get(Target, campaign.target_id)
            return [] if target is None or target.quality_status == TARGET_QUALITY_BAD else [target]
        if campaign.target_list_id is None:
            return []
        return list(
            await session.scalars(
                select(Target)
                .join(TargetListItem, TargetListItem.target_id == Target.id)
                .where(
                    TargetListItem.target_list_id == campaign.target_list_id,
                    TargetListItem.enabled.is_(True),
                    Target.quality_status != TARGET_QUALITY_BAD,
                )
                .order_by(Target.id)
            )
        )

    async def claim_next(self, worker_id: str) -> CampaignRun | None:
        async with self._claim_lock:
            await self.recover_expired_leases()
            async with self._session_factory() as session:
                now = utc_now()
                lease_token = uuid4().hex
                active_sender_run = aliased(CampaignRun)
                sender_matches = or_(
                    active_sender_run.sender_account_id == CampaignRun.sender_account_id,
                    and_(
                        active_sender_run.sender_account_id.is_(None),
                        CampaignRun.sender_account_id.is_(None),
                    ),
                )
                sender_is_idle = (
                    ~select(active_sender_run.id)
                    .where(
                        active_sender_run.status == RunStatus.RUNNING,
                        # A scoped recovery worker intentionally does not
                        # mutate runs outside its campaign scope.  Those
                        # expired leases must nevertheless stop blocking the
                        # same sender forever: a genuine running task always
                        # owns a future lease because claiming writes both
                        # fields atomically.
                        active_sender_run.lease_until > now,
                        sender_matches,
                    )
                    .correlate(CampaignRun)
                    .exists()
                )
                campaign_is_enabled = (
                    select(Campaign.id)
                    .where(
                        Campaign.id == CampaignRun.campaign_id,
                        Campaign.enabled.is_(True),
                    )
                    .correlate(CampaignRun)
                    .exists()
                )
                active_campaign_run = aliased(CampaignRun)
                campaign_is_already_served = (
                    select(active_campaign_run.id)
                    .where(
                        active_campaign_run.status == RunStatus.RUNNING,
                        active_campaign_run.lease_until > now,
                        active_campaign_run.campaign_id == CampaignRun.campaign_id,
                    )
                    .correlate(CampaignRun)
                    .exists()
                )
                candidate_filters = [
                    CampaignRun.status == RunStatus.QUEUED,
                    CampaignRun.scheduled_for <= now,
                    CampaignRun.cancel_requested.is_(False),
                    campaign_is_enabled,
                    sender_is_idle,
                ]
                if self._campaign_id_scope is not None:
                    candidate_filters.append(
                        CampaignRun.campaign_id.in_(self._campaign_id_scope)
                    )
                candidate_id = await session.scalar(
                    select(CampaignRun.id)
                    .where(*candidate_filters)
                    # A large older batch must not make every newer enabled
                    # campaign look stalled.  Prefer a due campaign that has
                    # no currently leased run; within each group retain the
                    # durable schedule order.  Sender idleness above still
                    # prevents parallel use of the same Telegram account.
                    .order_by(
                        case((campaign_is_already_served, 1), else_=0),
                        CampaignRun.scheduled_for,
                        CampaignRun.id,
                    )
                    .limit(1)
                )
                if candidate_id is None:
                    return None

                result = await session.execute(
                    update(CampaignRun)
                    .where(
                        CampaignRun.id == candidate_id,
                        CampaignRun.status == RunStatus.QUEUED,
                        campaign_is_enabled,
                        sender_is_idle,
                        *(
                            [CampaignRun.campaign_id.in_(self._campaign_id_scope)]
                            if self._campaign_id_scope is not None
                            else []
                        ),
                    )
                    .values(
                        status=RunStatus.RUNNING,
                        attempt_count=CampaignRun.attempt_count + 1,
                        lease_owner=worker_id,
                        lease_token=lease_token,
                        lease_until=now + timedelta(seconds=self._lease_seconds),
                        heartbeat_at=now,
                        started_at=func.coalesce(CampaignRun.started_at, now),
                        updated_at=now,
                    )
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount != 1:
                    await session.rollback()
                    return None
                await session.commit()
                return await session.get(CampaignRun, candidate_id)

    async def heartbeat(self, run_id: int, worker_id: str, lease_token: str) -> bool:
        async with self._session_factory() as session:
            now = utc_now()
            result = await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run_id,
                    CampaignRun.status == RunStatus.RUNNING,
                    CampaignRun.lease_owner == worker_id,
                    CampaignRun.lease_token == lease_token,
                )
                .values(
                    heartbeat_at=now,
                    lease_until=now + timedelta(seconds=self._lease_seconds),
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                await session.rollback()
                return False
            await session.commit()
            return True

    async def mark_step_error(
        self,
        run_id: int,
        error: str,
        *,
        retry_after: int | None = None,
        permanent: bool = False,
        lease_token: str | None = None,
        error_code: str | None = None,
    ) -> CampaignRun:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise NotFoundError("run not found")
            if lease_token is not None and run.lease_token != lease_token:
                raise LeaseLostError("cannot update a run owned by a newer lease")
            if run.dispatch_started_at is not None and error_code in {
                "step_timeout",
                "unhandled_execution_error",
                "cancelled",
                "lease_expired",
            }:
                error_code = "delivery_unconfirmed"
                error = (
                    "Операция прервана после начала отправки; "
                    "исход Telegram неизвестен, повтор запрещён"
                )
                permanent = True
            elif run.dispatch_started_at is None and error_code == "step_timeout":
                error_code = "preparation_timeout"
                error = "Подготовка не завершилась вовремя; запрос отправки ещё не начался"
                permanent = False
            elif run.dispatch_started_at is None and error_code == "unhandled_execution_error":
                error_code = "preparation_failed"
            if run.dispatch_started_at is not None and error_code == "delivery_unconfirmed":
                await self._record_uncertain_dispatch(session, run, error)
            if (
                run.dispatch_started_at is not None
                and error_code == "delivery_unconfirmed"
                and run.simulated_snapshot is not True
                and self._account_runner is not None
            ):
                if run.dispatch_account_id is not None and run.dispatch_idempotency_key:
                    await self._account_runner._record_account_send_once(
                        session,
                        run.dispatch_account_id,
                        operation_key=run.dispatch_idempotency_key,
                    )
            now = utc_now()
            run.error = error[:2000]
            run.error_code = (error_code or "execution_error")[:100]
            if run.error_code == "cancelled" and run.dispatch_started_at is None:
                run.cancel_requested = True
            run.lease_owner = None
            run.lease_token = None
            run.lease_until = None
            run.heartbeat_at = None
            if run.cancel_requested and run.error_code not in {
                "delivery_unconfirmed",
                "cancelled_outcome_unknown",
            }:
                run.status = RunStatus.CANCELLED
                run.finished_at = now
            elif permanent or run.attempt_count >= run.max_attempts:
                run.status = RunStatus.FAILED
                run.finished_at = now
            else:
                retry_delay = min(
                    self._retry_max_seconds,
                    self._retry_base_seconds * (2 ** max(run.attempt_count - 1, 0)),
                )
                if retry_after is not None:
                    requested_delay = max(retry_delay, retry_after)
                    if run.error_code in {"flood_wait", "slow_mode", "chat_timer"}:
                        retry_delay = requested_delay
                    else:
                        retry_delay = min(
                            self._max_retry_after_seconds,
                            requested_delay,
                        )
                run.status = RunStatus.QUEUED
                run.scheduled_for = now + timedelta(seconds=retry_delay)
            if run.error_code != "delivery_unconfirmed":
                run.dispatch_started_at = None
                run.dispatch_account_id = None
                run.dispatch_idempotency_key = None
            if run.status in {RunStatus.CANCELLED, RunStatus.FAILED}:
                if run.error_code in {
                    "delivery_unconfirmed",
                    "post_deleted_by_moderation",
                    "step_timeout",
                    "unhandled_execution_error",
                }:
                    await hold_uncertain_campaign_mention_reservations(
                        session,
                        run,
                        now=now,
                    )
                else:
                    await release_campaign_mention_reservations(
                        session,
                        run_ids=[run.id],
                    )
            add_audit_event(
                session,
                event_type=f"run.{run.status.value}",
                entity_type="campaign_run",
                entity_id=run.id,
                details={
                    "attempt": run.attempt_count,
                    "error": error[:500],
                    "error_code": run.error_code,
                },
                actor="worker",
            )
            await session.commit()
            await session.refresh(run)
            return run

    async def cancel(self, run_id: int) -> CampaignRun:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise NotFoundError("run not found")
            if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
                return run
            run.cancel_requested = True
            if run.status == RunStatus.QUEUED:
                run.status = RunStatus.CANCELLED
                run.finished_at = utc_now()
                run.lease_owner = None
                run.lease_token = None
                run.lease_until = None
                run.heartbeat_at = None
                await release_campaign_mention_reservations(
                    session,
                    run_ids=[run.id],
                )
            add_audit_event(
                session,
                event_type="run.cancel_requested",
                entity_type="campaign_run",
                entity_id=run.id,
            )
            await session.commit()
            await session.refresh(run)
            return run

    @staticmethod
    async def _record_uncertain_dispatch(
        session: AsyncSession,
        run: CampaignRun,
        error: str,
    ) -> None:
        if run.dispatch_account_id is None:
            return
        exists = await session.scalar(
            select(ActionLog.id)
            .where(
                ActionLog.run_id == run.id,
                ActionLog.lease_token == run.lease_token,
                ActionLog.error_code == "delivery_unconfirmed",
            )
            .limit(1)
        )
        if exists is not None:
            return
        session.add(
            ActionLog(
                run_id=run.id,
                account_id=run.dispatch_account_id,
                target_id=run.target_id,
                kind=ActionKind.PRIMARY_MESSAGE
                if run.next_step == RunStep.PRIMARY
                else ActionKind.REPLY_MESSAGE,
                status=ActionStatus.FAILED,
                simulated=run.simulated_snapshot,
                text="",
                error=error[:2000],
                error_code="delivery_unconfirmed",
                attempt_number=run.attempt_count,
                lease_token=run.lease_token,
            )
        )

    async def cancel_launch(self, anchor_run_id: int) -> int:
        """Cancel every pending task created by the same campaign launch."""
        async with self._session_factory() as session:
            anchor = await session.get(CampaignRun, anchor_run_id)
            if anchor is None:
                raise NotFoundError("launch not found")
            launch_key = anchor.launch_key or anchor.request_key
            launch_run_ids = list(
                await session.scalars(select(CampaignRun.id).where(_launch_membership(anchor)))
            )
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(
                        _launch_membership(anchor),
                        CampaignRun.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]),
                    )
                )
            )
            now = utc_now()
            queued_run_ids: list[int] = []
            for run in runs:
                run.cancel_requested = True
                if run.status == RunStatus.QUEUED:
                    queued_run_ids.append(run.id)
                    run.status = RunStatus.CANCELLED
                    run.finished_at = now
                    run.lease_owner = None
                    run.lease_token = None
                    run.lease_until = None
                    run.heartbeat_at = None
            await release_campaign_mention_reservations(
                session,
                run_ids=queued_run_ids,
            )
            cancelled_reactions = await session.execute(
                update(ReactionTask)
                .where(
                    ReactionTask.run_id.in_(launch_run_ids),
                    ReactionTask.status == ReactionTaskStatus.QUEUED,
                )
                .values(
                    status=ReactionTaskStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now,
                    error="launch cancel requested",
                    error_code="launch_cancelled",
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            running_reactions = await session.execute(
                update(ReactionTask)
                .where(
                    ReactionTask.run_id.in_(launch_run_ids),
                    ReactionTask.status == ReactionTaskStatus.RUNNING,
                )
                .values(
                    cancel_requested=True,
                    error="launch cancel requested while reaction is running",
                    error_code="cancel_requested",
                )
            )
            reaction_count = int(cancelled_reactions.rowcount or 0) + int(
                running_reactions.rowcount or 0
            )
            if runs or reaction_count:
                add_audit_event(
                    session,
                    event_type="launch.cancel_requested",
                    entity_type="campaign_run",
                    entity_id=anchor_run_id,
                    details={
                        "launch_key": launch_key,
                        "campaign_runs": len(runs),
                        "reaction_tasks": reaction_count,
                    },
                )
                await session.commit()
            return len(runs) + reaction_count

    async def update_active_launch(
        self,
        anchor_run_id: int,
        *,
        primary_text: str | None = None,
        no_timer_interval_seconds: int | None = None,
        min_messages_between_posts: int | None = None,
        visibility_max_wait_seconds: int | None = None,
        remaining_duration_seconds: int | None = None,
    ) -> int:
        """Update only pending work from one launch, preserving sent history."""
        if primary_text is not None:
            primary_text = primary_text.strip()
            if not 1 <= len(primary_text) <= 4096:
                raise ConflictError("message text must contain 1 to 4096 characters")
            try:
                validate_spintax(primary_text)
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
        if no_timer_interval_seconds is not None and not (
            60 <= no_timer_interval_seconds <= 7 * 24 * 3600
        ):
            raise ConflictError("no-timer interval must be between 1 minute and 7 days")
        if min_messages_between_posts is not None and not (
            1 <= min_messages_between_posts <= 100_000
        ):
            raise ConflictError("message depth must be between 1 and 100000")
        if visibility_max_wait_seconds is not None and not (
            0 <= visibility_max_wait_seconds <= 7 * 24 * 3600
        ):
            raise ConflictError("visibility wait must be between 0 minutes and 7 days")
        if remaining_duration_seconds is not None and not (
            remaining_duration_seconds == 0 or 60 <= remaining_duration_seconds <= 7 * 24 * 3600
        ):
            raise ConflictError("remaining duration must be zero or between 1 minute and 7 days")
        if (
            primary_text is None
            and no_timer_interval_seconds is None
            and min_messages_between_posts is None
            and visibility_max_wait_seconds is None
            and remaining_duration_seconds is None
        ):
            raise ConflictError("choose at least one launch setting to update")

        async with self._session_factory() as session:
            anchor = await session.get(CampaignRun, anchor_run_id)
            if anchor is None:
                raise NotFoundError("launch not found")
            campaign = await session.get(Campaign, anchor.campaign_id)
            if campaign is None:
                raise NotFoundError("campaign not found")
            if primary_text is not None and campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError(
                    "active text replacement is only supported for broadcast launches"
                )
            launch_key = anchor.launch_key or anchor.request_key
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(_launch_membership(anchor)).order_by(CampaignRun.id)
                )
            )
            active = [
                run
                for run in runs
                if run.status in {RunStatus.QUEUED, RunStatus.RUNNING} and not run.cancel_requested
            ]
            if not active:
                raise ConflictError("launch has no pending tasks")
            if primary_text is not None:
                variant_counts = {
                    len(CampaignRunner._run_content_variants(run, campaign)) for run in active
                }
                if any(count > 1 for count in variant_counts):
                    raise ConflictError(
                        "active text replacement is unavailable for a launch with variants"
                    )
            if primary_text is not None and len(primary_text) > 1024:
                has_media = any(
                    run.primary_media_path_snapshot is not None
                    or bool(run.media_items_snapshot_json not in {None, "[]"})
                    or (
                        run.primary_text_snapshot is None
                        and campaign.primary_media_path is not None
                    )
                    for run in active
                )
                if has_media:
                    raise ConflictError("media caption must contain at most 1024 characters")
            smart = any(CampaignRunner._is_smart_run(run) for run in runs)
            if (
                no_timer_interval_seconds is not None
                or min_messages_between_posts is not None
                or visibility_max_wait_seconds is not None
                or remaining_duration_seconds is not None
            ) and not smart:
                raise ConflictError(
                    "frequency and active duration can only be changed for a smart launch"
                )

            now = utc_now()
            new_repeat_forever = remaining_duration_seconds == 0
            new_repeat_until = (
                now + timedelta(seconds=remaining_duration_seconds)
                if remaining_duration_seconds is not None and remaining_duration_seconds > 0
                else None
            )
            latest_success_by_target: dict[int, datetime] = {}
            for run in runs:
                if run.status != RunStatus.SUCCEEDED or run.finished_at is None:
                    continue
                finished_at = ensure_utc(run.finished_at)
                if finished_at is None:
                    continue
                current = latest_success_by_target.get(run.target_id)
                if current is None or finished_at > current:
                    latest_success_by_target[run.target_id] = finished_at

            pair_states: dict[tuple[int, int], TargetAccountState] = {}
            pair_account_ids = {
                run.sender_account_id for run in active if run.sender_account_id is not None
            }
            if pair_account_ids:
                states = list(
                    await session.scalars(
                        select(TargetAccountState).where(
                            TargetAccountState.target_id.in_({run.target_id for run in active}),
                            TargetAccountState.account_id.in_(pair_account_ids),
                        )
                    )
                )
                pair_states = {(state.target_id, state.account_id): state for state in states}

            for run in active:
                if primary_text is not None:
                    current_variant = CampaignRunner._run_content_variant(run, campaign)
                    if current_variant.contact is not None:
                        raise ConflictError(
                            "active text replacement is unavailable for a contact post"
                        )
                    run.primary_text_snapshot = primary_text
                    run.primary_html_snapshot = None
                    if run.content_variants_snapshot_json is not None:
                        variant = CampaignRunner._run_content_variants(run, campaign)[0]
                        updated_variant = BroadcastContentVariant(
                            variant_id=variant.variant_id,
                            name=variant.name,
                            text=primary_text,
                            html_text=None,
                            media=variant.media,
                            contact=None,
                            enabled=True,
                        )
                        run.content_variants_snapshot_json = serialize_content_variants(
                            [updated_variant]
                        )
                if no_timer_interval_seconds is not None:
                    run.no_timer_interval_seconds_snapshot = no_timer_interval_seconds
                if min_messages_between_posts is not None:
                    run.min_messages_between_posts_snapshot = min_messages_between_posts
                if visibility_max_wait_seconds is not None:
                    run.visibility_max_wait_seconds_snapshot = visibility_max_wait_seconds
                if remaining_duration_seconds is not None:
                    run.repeat_until = new_repeat_until
                    run.repeat_forever_snapshot = new_repeat_forever

                if run.status != RunStatus.QUEUED or not CampaignRunner._is_smart_run(run):
                    continue
                deadline = ensure_utc(run.repeat_until)
                if no_timer_interval_seconds is not None and run.cycle_index > 0:
                    previous = latest_success_by_target.get(run.target_id)
                    if previous is not None:
                        scheduled_for = previous + timedelta(seconds=no_timer_interval_seconds)
                        if run.sender_account_id is not None:
                            state = pair_states.get((run.target_id, run.sender_account_id))
                            known_timer = (
                                ensure_utc(state.next_send_at) if state is not None else None
                            )
                            if known_timer is not None and known_timer > scheduled_for:
                                scheduled_for = known_timer
                        run.scheduled_for = max(now, scheduled_for)
                if deadline is not None and ensure_utc(run.scheduled_for) > deadline:
                    run.scheduled_for = deadline

            add_audit_event(
                session,
                event_type="launch.active_settings_updated",
                entity_type="campaign_run",
                entity_id=anchor_run_id,
                details={
                    "launch_key": launch_key,
                    "updated_tasks": len(active),
                    "primary_text_length": (
                        len(primary_text) if primary_text is not None else None
                    ),
                    "no_timer_interval_seconds": no_timer_interval_seconds,
                    "min_messages_between_posts": min_messages_between_posts,
                    "visibility_max_wait_seconds": visibility_max_wait_seconds,
                    "remaining_duration_seconds": remaining_duration_seconds,
                    "repeat_until": (
                        new_repeat_until.isoformat() if new_repeat_until is not None else None
                    ),
                    "repeat_forever": (
                        new_repeat_forever if remaining_duration_seconds is not None else None
                    ),
                },
                actor="control-bot",
            )
            await session.commit()
            return len(active)

    async def retry_failed_launch(self, anchor_run_id: int) -> int:
        """Requeue only failed tasks while preserving their Telegram idempotency keys."""
        async with self._session_factory() as session:
            anchor = await session.get(CampaignRun, anchor_run_id)
            if anchor is None:
                raise NotFoundError("launch not found")
            launch_key = anchor.launch_key or anchor.request_key
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(
                        _launch_membership(anchor),
                        CampaignRun.status == RunStatus.FAILED,
                        CampaignRun.dispatch_started_at.is_(None),
                        or_(
                            CampaignRun.error_code.is_(None),
                            CampaignRun.error_code.not_in(CAMPAIGN_NON_RETRYABLE_CODES),
                        ),
                    )
                )
            )
            now = utc_now()
            for run in runs:
                uncertain_delivery = run.error_code in {
                    "cancelled_outcome_unknown",
                    "delivery_unconfirmed",
                    "lease_expired",
                    "post_deleted_by_moderation",
                    "step_timeout",
                    "unhandled_execution_error",
                }
                if not uncertain_delivery:
                    run.hidden_mention_user_ids_snapshot_json = None
                    run.hidden_mention_resolved_count = None
                    run.hidden_mention_skip_reason = None
                run.status = RunStatus.QUEUED
                run.scheduled_for = now
                run.attempt_count = 0
                run.cancel_requested = False
                run.error = None
                run.error_code = None
                run.finished_at = None
                run.notified_at = None
                run.lease_owner = None
                run.lease_token = None
                run.lease_until = None
                run.heartbeat_at = None
            if runs:
                add_audit_event(
                    session,
                    event_type="launch.failed_requeued",
                    entity_type="campaign_run",
                    entity_id=anchor_run_id,
                    details={"launch_key": launch_key, "count": len(runs)},
                )
                await session.commit()
            return len(runs)

    async def cancel_all(self) -> int:
        """Preserve the legacy task-count API for non-UI callers.

        The control panel needs the detailed result to report paused rules and
        the different delivery queues accurately; callers that historically
        expected an integer continue to receive the number of affected tasks.
        """

        return (await self.cancel_all_with_summary()).task_count

    async def cancel_all_campaign_deliveries(self) -> CampaignDeliveryStopSummary:
        """Stop current chat publications and their campaign reactions only.

        This is deliberately narrower than the emergency global stop: it does
        not pause comment rules, participant reactions, direct messages, or
        subscriber invites.  Saved campaign scenarios remain enabled, so an
        operator can review and launch them again explicitly later.
        """

        async with self._session_factory() as session:
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(
                        CampaignRun.status.in_([RunStatus.QUEUED, RunStatus.RUNNING])
                    )
                )
            )
            now = utc_now()
            queued_run_ids: list[int] = []
            queued_runs = 0
            running_runs = 0
            for run in runs:
                run.cancel_requested = True
                if run.status == RunStatus.QUEUED:
                    queued_runs += 1
                    queued_run_ids.append(run.id)
                    run.status = RunStatus.CANCELLED
                    run.finished_at = now
                    run.lease_owner = None
                    run.lease_token = None
                    run.lease_until = None
                    run.heartbeat_at = None
                else:
                    running_runs += 1
            await release_campaign_mention_reservations(session, run_ids=queued_run_ids)

            cancelled_reactions = await session.execute(
                update(ReactionTask)
                .where(ReactionTask.status == ReactionTaskStatus.QUEUED)
                .values(
                    status=ReactionTaskStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now,
                    error="campaign delivery stop requested",
                    error_code="campaign_stop",
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            cancelled_reaction_count = int(cancelled_reactions.rowcount or 0)
            running_reactions = await session.execute(
                update(ReactionTask)
                .where(ReactionTask.status == ReactionTaskStatus.RUNNING)
                .values(
                    cancel_requested=True,
                    error="campaign delivery stop requested while reaction is running",
                    error_code="cancel_requested",
                )
            )
            running_reaction_count = int(running_reactions.rowcount or 0)
            summary = CampaignDeliveryStopSummary(
                queued_runs=queued_runs,
                running_runs=running_runs,
                queued_reactions=cancelled_reaction_count,
                running_reactions=running_reaction_count,
            )
            if summary.task_count:
                add_audit_event(
                    session,
                    event_type="campaign_delivery.cancel_all_requested",
                    entity_type="campaign_run",
                    entity_id=None,
                    details={
                        "queued_runs": summary.queued_runs,
                        "running_runs": summary.running_runs,
                        "queued_reactions": summary.queued_reactions,
                        "running_reactions": summary.running_reactions,
                    },
                )
                await session.commit()
            return summary

    async def cancel_all_with_summary(self) -> GlobalStopSummary:
        """Atomically stop every delivery queue owned by this installation.

        Queued work becomes terminal immediately. Running work keeps its lease
        so its worker can persist the truthful external outcome, but is told to
        stop before its next external step. Subscriber reservations belonging
        to cancelled work are released in the same transaction.
        """

        async with self._session_factory() as session:
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(
                        CampaignRun.status.in_([RunStatus.QUEUED, RunStatus.RUNNING])
                    )
                )
            )
            now = utc_now()
            queued_run_ids: list[int] = []
            for run in runs:
                run.cancel_requested = True
                if run.status == RunStatus.QUEUED:
                    queued_run_ids.append(run.id)
                    run.status = RunStatus.CANCELLED
                    run.finished_at = now
                    run.lease_owner = None
                    run.lease_token = None
                    run.lease_until = None
                    run.heartbeat_at = None
            await release_campaign_mention_reservations(
                session,
                run_ids=queued_run_ids,
            )
            cancelled_reactions = await session.execute(
                update(ReactionTask)
                .where(ReactionTask.status == ReactionTaskStatus.QUEUED)
                .values(
                    status=ReactionTaskStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now,
                    error="global stop requested",
                    error_code="global_stop",
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            running_reactions = await session.execute(
                update(ReactionTask)
                .where(ReactionTask.status == ReactionTaskStatus.RUNNING)
                .values(
                    cancel_requested=True,
                    error="global stop requested while reaction is running",
                    error_code="cancel_requested",
                )
            )
            paused_comment_rules = await session.execute(
                update(ChannelCommentRule)
                .where(ChannelCommentRule.enabled.is_(True))
                .values(enabled=False)
            )
            cancelled_comments = await session.execute(
                update(ChannelCommentRun)
                .where(ChannelCommentRun.status == ChannelCommentRunStatus.QUEUED)
                .values(
                    status=ChannelCommentRunStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now,
                    error="global stop requested",
                    error_code="global_stop",
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            running_comments = await session.execute(
                update(ChannelCommentRun)
                .where(ChannelCommentRun.status == ChannelCommentRunStatus.RUNNING)
                .values(
                    cancel_requested=True,
                    error="global stop requested while comment is running",
                    error_code="cancel_requested",
                )
            )
            paused_participant_rules = await session.execute(
                update(ParticipantReactionRule)
                .where(ParticipantReactionRule.enabled.is_(True))
                .values(enabled=False)
            )
            cancelled_participant_reactions = await session.execute(
                update(ParticipantReactionTask)
                .where(ParticipantReactionTask.status == ParticipantReactionTaskStatus.QUEUED)
                .values(
                    status=ParticipantReactionTaskStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now,
                    error="global stop requested",
                    error_code="global_stop",
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            running_participant_reactions = await session.execute(
                update(ParticipantReactionTask)
                .where(ParticipantReactionTask.status == ParticipantReactionTaskStatus.RUNNING)
                .values(
                    cancel_requested=True,
                    error="global stop requested while participant reaction is running",
                    error_code="cancel_requested",
                )
            )

            direct_launches = list(
                await session.scalars(
                    select(DirectLaunch).where(
                        DirectLaunch.status.in_(
                            [DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING]
                        )
                    )
                )
            )
            direct_launch_ids = [int(launch.id) for launch in direct_launches]
            direct_queued_subscriber_ids: list[int] = []
            direct_message_tasks = 0
            if direct_launch_ids:
                direct_queued_subscriber_ids = [
                    int(subscriber_id)
                    for subscriber_id in await session.scalars(
                        select(DirectMessageTask.subscriber_id).where(
                            DirectMessageTask.launch_id.in_(direct_launch_ids),
                            DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                        )
                    )
                ]
                cancelled_direct_messages = await session.execute(
                    update(DirectMessageTask)
                    .where(
                        DirectMessageTask.launch_id.in_(direct_launch_ids),
                        DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                    .values(
                        status=DirectMessageTaskStatus.CANCELLED,
                        error="global stop requested",
                        error_code="global_stop",
                        finished_at=now,
                        updated_at=now,
                        lease_owner=None,
                        lease_token=None,
                        lease_until=None,
                    )
                )
                direct_message_tasks = int(cancelled_direct_messages.rowcount or 0)
                if direct_queued_subscriber_ids:
                    await session.execute(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(direct_queued_subscriber_ids),
                            Subscriber.reserved_direct_launch_id.in_(direct_launch_ids),
                        )
                        .values(reserved_direct_launch_id=None, updated_at=now)
                    )
                running_direct_launch_ids = {
                    int(launch_id)
                    for launch_id in await session.scalars(
                        select(DirectMessageTask.launch_id)
                        .where(
                            DirectMessageTask.launch_id.in_(direct_launch_ids),
                            DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                        )
                        .distinct()
                    )
                }
                for launch in direct_launches:
                    launch.cancel_requested = True
                    launch.updated_at = now
                    if int(launch.id) not in running_direct_launch_ids:
                        launch.status = DirectLaunchStatus.CANCELLED
                        launch.finished_at = now

            invite_launches = list(
                await session.scalars(
                    select(SubscriberInviteLaunch).where(
                        SubscriberInviteLaunch.status.in_(
                            [DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING]
                        )
                    )
                )
            )
            invite_launch_ids = [int(launch.id) for launch in invite_launches]
            invite_queued_subscriber_ids: list[int] = []
            subscriber_invite_tasks = 0
            if invite_launch_ids:
                invite_queued_subscriber_ids = [
                    int(subscriber_id)
                    for subscriber_id in await session.scalars(
                        select(SubscriberInviteTask.subscriber_id).where(
                            SubscriberInviteTask.launch_id.in_(invite_launch_ids),
                            SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                        )
                    )
                ]
                cancelled_invites = await session.execute(
                    update(SubscriberInviteTask)
                    .where(
                        SubscriberInviteTask.launch_id.in_(invite_launch_ids),
                        SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                    .values(
                        status=DirectMessageTaskStatus.CANCELLED,
                        error="global stop requested",
                        error_code="global_stop",
                        finished_at=now,
                        updated_at=now,
                        lease_owner=None,
                        lease_token=None,
                        lease_until=None,
                    )
                )
                subscriber_invite_tasks = int(cancelled_invites.rowcount or 0)
                if invite_queued_subscriber_ids:
                    await session.execute(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(invite_queued_subscriber_ids),
                            Subscriber.reserved_invite_launch_id.in_(invite_launch_ids),
                        )
                        .values(reserved_invite_launch_id=None, updated_at=now)
                    )
                running_invite_launch_ids = {
                    int(launch_id)
                    for launch_id in await session.scalars(
                        select(SubscriberInviteTask.launch_id)
                        .where(
                            SubscriberInviteTask.launch_id.in_(invite_launch_ids),
                            SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                        )
                        .distinct()
                    )
                }
                for launch in invite_launches:
                    launch.cancel_requested = True
                    launch.updated_at = now
                    if int(launch.id) not in running_invite_launch_ids:
                        launch.status = DirectLaunchStatus.CANCELLED
                        launch.finished_at = now

            campaign_count = len(runs)
            reaction_count = int(cancelled_reactions.rowcount or 0) + int(
                running_reactions.rowcount or 0
            )
            comment_rule_count = int(paused_comment_rules.rowcount or 0)
            comment_count = int(cancelled_comments.rowcount or 0) + int(
                running_comments.rowcount or 0
            )
            participant_rule_count = int(paused_participant_rules.rowcount or 0)
            participant_count = int(cancelled_participant_reactions.rowcount or 0) + int(
                running_participant_reactions.rowcount or 0
            )
            summary = GlobalStopSummary(
                campaign_runs=campaign_count,
                campaign_reactions=reaction_count,
                channel_comment_rules_paused=comment_rule_count,
                channel_comment_runs=comment_count,
                participant_reaction_rules_paused=participant_rule_count,
                participant_reaction_tasks=participant_count,
                direct_launches=len(direct_launches),
                direct_message_tasks=direct_message_tasks,
                subscriber_invite_launches=len(invite_launches),
                subscriber_invite_tasks=subscriber_invite_tasks,
            )
            if summary.task_count or summary.rule_count or summary.launch_count:
                add_audit_event(
                    session,
                    event_type="queue.cancel_all_requested",
                    entity_type="queue",
                    entity_id=None,
                    details={
                        "campaign_runs": campaign_count,
                        "campaign_reactions": reaction_count,
                        "channel_comment_rules_paused": comment_rule_count,
                        "channel_comment_runs": comment_count,
                        "participant_reaction_rules_paused": participant_rule_count,
                        "participant_reaction_tasks": participant_count,
                        "direct_launches": summary.direct_launches,
                        "direct_message_tasks": summary.direct_message_tasks,
                        "subscriber_invite_launches": summary.subscriber_invite_launches,
                        "subscriber_invite_tasks": summary.subscriber_invite_tasks,
                    },
                )
                await session.commit()
            return summary

    async def recover_expired_leases(self) -> int:
        async with self._session_factory() as session:
            now = utc_now()
            expired_filters = [
                CampaignRun.status == RunStatus.RUNNING,
                CampaignRun.lease_until.is_not(None),
                CampaignRun.lease_until < now,
            ]
            if self._campaign_id_scope is not None:
                expired_filters.append(CampaignRun.campaign_id.in_(self._campaign_id_scope))
            expired = list(await session.scalars(select(CampaignRun).where(*expired_filters)))
            recovered = 0
            for run in expired:
                values: dict[str, object] = {
                    "lease_owner": None,
                    "lease_token": None,
                    "lease_until": None,
                    "heartbeat_at": None,
                    "updated_at": now,
                }
                if run.dispatch_started_at is not None:
                    accepted_failure = (
                        await session.execute(
                            select(ActionLog.error_code, ActionLog.error)
                            .where(
                                ActionLog.run_id == run.id,
                                ActionLog.lease_token == run.lease_token,
                                ActionLog.external_message_id.is_not(None),
                                ActionLog.error_code.in_(
                                    ["post_deleted_by_moderation", "album_partial_missing"]
                                ),
                            )
                            .order_by(ActionLog.id.desc())
                            .limit(1)
                        )
                    ).first()
                    values.update(
                        status=RunStatus.FAILED,
                        error_code=accepted_failure[0]
                        if accepted_failure
                        else "delivery_unconfirmed",
                        error=accepted_failure[1]
                        if accepted_failure
                        else (
                            "Процесс прервался после начала отправки; "
                            "исход Telegram неизвестен, повтор запрещён"
                        ),
                        finished_at=now,
                    )
                elif run.cancel_requested:
                    values.update(
                        status=RunStatus.FAILED,
                        error=(
                            "cancel was requested after execution started; "
                            "the Telegram outcome is unknown after lease expiry"
                        ),
                        error_code="cancelled_outcome_unknown",
                        finished_at=now,
                    )
                elif run.attempt_count >= run.max_attempts:
                    values.update(
                        status=RunStatus.FAILED,
                        error="worker lease expired",
                        error_code="lease_expired",
                        finished_at=now,
                    )
                else:
                    values.update(status=RunStatus.QUEUED, scheduled_for=now)
                result = await session.execute(
                    update(CampaignRun)
                    .where(
                        CampaignRun.id == run.id,
                        CampaignRun.status == RunStatus.RUNNING,
                        CampaignRun.lease_token == run.lease_token,
                        CampaignRun.lease_until < now,
                    )
                    .values(**values)
                    .execution_options(synchronize_session=False)
                )
                changed = int(result.rowcount or 0)
                recovered += changed
                if changed and values.get("error_code") == "delivery_unconfirmed":
                    await self._record_uncertain_dispatch(session, run, str(values["error"]))
                if (
                    changed
                    and run.dispatch_started_at is not None
                    and run.simulated_snapshot is not True
                    and self._account_runner is not None
                ):
                    if run.dispatch_account_id is not None and run.dispatch_idempotency_key:
                        await self._account_runner._record_account_send_once(
                            session,
                            run.dispatch_account_id,
                            operation_key=run.dispatch_idempotency_key,
                        )
                if changed and values.get("status") == RunStatus.FAILED:
                    await hold_uncertain_campaign_mention_reservations(
                        session,
                        run,
                        now=now,
                    )
            if recovered:
                add_audit_event(
                    session,
                    event_type="queue.leases_recovered",
                    entity_type="campaign_run",
                    entity_id=None,
                    details={"count": recovered},
                    actor="worker",
                )
                await session.commit()
            return recovered

    async def get_run(self, run_id: int) -> CampaignRun:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise NotFoundError("run not found")
            return run


class QueueWorker:
    def __init__(
        self,
        queue: RunQueue,
        runner: CampaignRunner,
        *,
        worker_id: str,
        poll_seconds: float,
        heartbeat_seconds: float = 5.0,
        step_timeout_seconds: float = 60.0,
        concurrency: int = 1,
    ) -> None:
        self._queue = queue
        queue._account_runner = runner
        self._runner = runner
        self._worker_id = worker_id
        self._poll_seconds = poll_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._step_timeout_seconds = step_timeout_seconds
        self._concurrency = concurrency
        self._stop_event = asyncio.Event()
        self._running = False
        self._active_steps = 0
        self._processed_steps = 0
        self._failed_steps = 0
        self._last_error: str | None = None
        self._last_processed_at: str | None = None

    async def process_next(self) -> CampaignRun | None:
        run = await self._queue.claim_next(self._worker_id)
        if run is None:
            return None
        if run.lease_token is None:
            raise LeaseLostError("claimed run has no fencing token")
        lease_token = run.lease_token
        heartbeat_stop = asyncio.Event()
        lease_lost = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat(
                run.id,
                lease_token,
                heartbeat_stop,
                lease_lost,
                asyncio.current_task(),
            ),
            name=f"heartbeat-{run.id}",
        )

        async def stop_heartbeat() -> None:
            """End renewal before writing this step's terminal/retry outcome.

            The outcome write intentionally clears the run lease.  Letting a
            concurrent heartbeat observe that expected change and cancel this
            task can interrupt the very write that makes the outcome durable.
            """

            heartbeat_stop.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

        self._active_steps += 1
        try:
            try:
                async with asyncio.timeout(self._step_timeout_seconds):
                    result = await self._runner.process_step(run.id, lease_token)
            except BaseException:
                await stop_heartbeat()
                if lease_lost.is_set():
                    raise LeaseLostError("campaign run lease was lost") from None
                raise
            await stop_heartbeat()
            if lease_lost.is_set():
                terminal = await self._terminal_result_after_lease_signal(run.id)
                if terminal is not None:
                    return terminal
                raise LeaseLostError("campaign run lease was lost")
            self._processed_steps += 1
            self._last_error = None
            self._last_processed_at = utc_now().isoformat()
            return result
        except TimeoutError:
            return await self._mark_error(
                run.id,
                lease_token,
                "execution step timed out",
                permanent=True,
                error_code="step_timeout",
            )
        except RunCancelled as exc:
            return await self._mark_error(
                run.id,
                lease_token,
                safe_error_message(exc),
                error_code="cancelled",
            )
        except asyncio.CancelledError:
            if not lease_lost.is_set():
                raise
            terminal = await self._terminal_result_after_lease_signal(run.id)
            if terminal is not None:
                return terminal
            logger.warning("Campaign run %s lost its lease during execution", run.id)
            return await self._queue.get_run(run.id)
        except LeaseLostError:
            terminal = await self._terminal_result_after_lease_signal(run.id)
            if terminal is not None:
                return terminal
            logger.warning("Campaign run %s lost its lease fencing token", run.id)
            return await self._queue.get_run(run.id)
        except GatewayError as exc:
            message = safe_error_message(exc)
            if exc.retryable:
                logger.warning("Campaign run %s temporary gateway error: %s", run.id, message)
            else:
                logger.error("Campaign run %s permanent gateway error: %s", run.id, message)
            return await self._mark_error(
                run.id,
                lease_token,
                message,
                retry_after=exc.retry_after,
                permanent=not exc.retryable,
                error_code=exc.code,
            )
        except (ConflictError, NotFoundError) as exc:
            logger.warning("Campaign run %s cannot continue: %s", run.id, exc)
            return await self._mark_error(
                run.id,
                lease_token,
                safe_error_message(exc),
                permanent=True,
                error_code=str(getattr(exc, "code", None) or "preflight_failed"),
            )
        except Exception as exc:
            logger.exception("Campaign run %s step failed", run.id)
            return await self._mark_error(
                run.id,
                lease_token,
                safe_error_message(exc),
                retry_after=getattr(exc, "retry_after", None),
                permanent=True,
                error_code="unhandled_execution_error",
            )
        finally:
            heartbeat_stop.set()
            if not heartbeat_task.done():
                with suppress(asyncio.CancelledError):
                    await heartbeat_task
            self._active_steps -= 1

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        self._running = True
        workers = [
            asyncio.create_task(self._worker_loop(reporter), name=f"queue-worker-{index + 1}")
            for index in range(self._concurrency)
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            self._running = False

    async def _worker_loop(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop_event.is_set():
            try:
                processed = await self.process_next()
            except asyncio.CancelledError:
                raise
            except OperationalError as exc:
                if is_transient_sqlite_lock(exc):
                    logger.warning("Campaign worker waits for a busy SQLite writer")
                    await asyncio.sleep(0.5)
                    continue
                self._failed_steps += 1
                self._last_error = "worker loop iteration failed"
                self._last_processed_at = utc_now().isoformat()
                logger.exception("Queue worker loop iteration failed")
                if reporter is not None:
                    await reporter(exc)
                processed = None
            except Exception as exc:  # noqa: BLE001 - keep the durable worker alive after infra failures
                self._failed_steps += 1
                self._last_error = "worker loop iteration failed"
                self._last_processed_at = utc_now().isoformat()
                logger.exception("Queue worker loop iteration failed")
                if reporter is not None:
                    await reporter(exc)
                processed = None
            else:
                if reporter is not None:
                    await reporter()
            if processed is not None:
                continue
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass

    async def _heartbeat(
        self,
        run_id: int,
        lease_token: str,
        stop_event: asyncio.Event,
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
    ) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._heartbeat_seconds)
                return
            except TimeoutError:
                try:
                    renewed = await self._queue.heartbeat(run_id, self._worker_id, lease_token)
                except Exception:
                    logger.exception("Heartbeat failed for campaign run %s", run_id)
                    self._signal_lost_lease(lost_event, owner_task)
                    return
                if not renewed:
                    logger.warning("Heartbeat lost for campaign run %s", run_id)
                    self._signal_lost_lease(lost_event, owner_task)
                    return

    async def _terminal_result_after_lease_signal(self, run_id: int) -> CampaignRun | None:
        """Accept an outcome already durably finalized by this worker step.

        CampaignRunner writes and clears its lease together with a terminal
        Telegram outcome.  A heartbeat scheduled in that tiny interval sees a
        missing lease and used to cancel the owner task, producing a misleading
        ``lost lease`` warning after a successful send.  A terminal durable
        result is authoritative; a still-running result remains fenced.
        """

        current = await self._queue.get_run(run_id)
        if current.status not in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
            return None
        self._processed_steps += 1
        self._last_error = None
        self._last_processed_at = utc_now().isoformat()
        logger.debug(
            "Campaign run %s finalized before its heartbeat observed lease release",
            run_id,
        )
        return current

    @staticmethod
    def _signal_lost_lease(
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
    ) -> None:
        lost_event.set()
        if owner_task is not None and not owner_task.done():
            owner_task.cancel()

    async def _mark_error(
        self,
        run_id: int,
        lease_token: str,
        error: str,
        *,
        retry_after: int | None = None,
        permanent: bool = False,
        error_code: str,
    ) -> CampaignRun:
        self._failed_steps += 1
        self._last_error = error
        self._last_processed_at = utc_now().isoformat()
        try:
            return await self._queue.mark_step_error(
                run_id,
                error,
                retry_after=retry_after,
                permanent=permanent,
                lease_token=lease_token,
                error_code=error_code,
            )
        except LeaseLostError:
            logger.warning("Could not record error for run %s because lease moved", run_id)
            return await self._queue.get_run(run_id)

    def snapshot(self) -> WorkerSnapshot:
        return WorkerSnapshot(
            worker_id=self._worker_id,
            running=self._running,
            active_steps=self._active_steps,
            processed_steps=self._processed_steps,
            failed_steps=self._failed_steps,
            last_error=self._last_error,
            last_processed_at=self._last_processed_at,
        )

    def stop(self) -> None:
        self._stop_event.set()
