"""Durable, low-priority ordinary reactions for messages sent by a campaign."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.failure_policy import (
    ACCOUNT_TARGET_BLOCK_CODES,
    ACCOUNT_WIDE_CODES,
    OBSERVED_TARGET_FAILURE_CODES,
    failure_code,
)
from proxima_traffic.gateways import (
    ChannelCommentGateway,
    MessageGateway,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    ChannelCommentMessage,
    ChannelCommentReactionAccountState,
    ChannelCommentRule,
    ChannelCommentRun,
    MembershipStatus,
    ReactionTask,
    ReactionTaskStatus,
    Target,
    TargetAccountState,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import (
    AccountSendDeferred,
    CampaignRunner,
    add_audit_event,
    safe_error_message,
)
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)


class _ReactionCancelledBeforeDelivery(Exception):
    """The task was cancelled after it was claimed but before Telegram was called."""


class ReactionWorker:
    """Executes queued reactions separately from posting and reporting workers."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway & ChannelCommentGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        worker_id: str,
        campaign_id_scope: frozenset[int] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._worker_id = worker_id
        self._campaign_id_scope = (
            frozenset(campaign_id_scope) if campaign_id_scope is not None else None
        )
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop_event.is_set():
            processed = False
            try:
                recovered = await self.recover_expired_leases()
                if recovered:
                    logger.warning("Recovered %s expired reaction leases", recovered)
                processed = await self.process_next()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - database/claim failures must not stop the worker
                logger.exception("Reaction worker iteration failed")
                if reporter is not None:
                    await reporter(exc)
            else:
                if reporter is not None:
                    await reporter()
            if processed:
                continue
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._settings.worker_poll_seconds
                )
            except TimeoutError:
                pass

    async def process_next(self) -> bool:
        claimed = await self._claim_next()
        if claimed is None:
            return False
        task_id, token = claimed
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat(task_id, token, heartbeat_stop),
            name=f"reaction-heartbeat-{task_id}",
        )
        try:
            try:
                async with asyncio.timeout(self._settings.worker_step_timeout_seconds):
                    await self._process(task_id, token)
            except TimeoutError:
                await self._handle_error(
                    task_id,
                    token,
                    0,
                    TemporaryGatewayError(
                        "reaction operation timed out",
                        code="reaction_timeout",
                        retry_after=self._settings.worker_retry_base_seconds,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - persist a terminal result for unknown effects
                logger.exception("Reaction task %s crashed", task_id)
                await self._handle_error(task_id, token, 0, exc)
        finally:
            heartbeat_stop.set()
            await heartbeat_task
        return True

    async def recover_expired_leases(self) -> int:
        """Return work left RUNNING by a process restart back to the durable queue."""
        now = utc_now()
        async with self._session_factory() as session:
            expired_filters = [
                ReactionTask.status == ReactionTaskStatus.RUNNING,
                ReactionTask.lease_until.is_not(None),
                ReactionTask.lease_until < now,
            ]
            if self._campaign_id_scope is not None:
                expired_filters.append(ReactionTask.campaign_id.in_(self._campaign_id_scope))
            expired = list(
                await session.scalars(select(ReactionTask).where(*expired_filters))
            )
            for task in expired:
                task.error = "reaction worker lease expired"
                task.error_code = "lease_expired"
                if task.cancel_requested:
                    task.status = ReactionTaskStatus.FAILED
                    task.error = (
                        "cancel was requested while the reaction was running; "
                        "the Telegram outcome is unknown after lease expiry"
                    )
                    task.error_code = "cancelled_outcome_unknown"
                    task.finished_at = now
                elif task.attempt_count >= task.max_attempts:
                    task.status = ReactionTaskStatus.FAILED
                    task.error = "maximum reaction delivery attempts exhausted"
                    task.error_code = "reaction_attempts_exhausted"
                    task.finished_at = now
                else:
                    task.status = ReactionTaskStatus.QUEUED
                    task.scheduled_for = now
                self._release(task)
            if expired:
                add_audit_event(
                    session,
                    event_type="reaction.leases_recovered",
                    entity_type="reaction_task",
                    entity_id=None,
                    details={"count": len(expired)},
                    actor="reaction-worker",
                )
                await session.commit()
            return len(expired)

    async def _claim_next(self) -> tuple[int, str] | None:
        now = utc_now()
        async with self._session_factory() as session:
            candidate_filters = [
                ReactionTask.status == ReactionTaskStatus.QUEUED,
                ReactionTask.scheduled_for <= now,
                ReactionTask.cancel_requested.is_(False),
            ]
            if self._campaign_id_scope is not None:
                candidate_filters.append(ReactionTask.campaign_id.in_(self._campaign_id_scope))
            task = await session.scalar(
                select(ReactionTask)
                .where(*candidate_filters)
                .order_by(ReactionTask.scheduled_for, ReactionTask.id)
                .limit(1)
            )
            if task is None:
                return None
            token = uuid4().hex
            result = await session.execute(
                update(ReactionTask)
                .where(
                    ReactionTask.id == task.id,
                    ReactionTask.status == ReactionTaskStatus.QUEUED,
                    ReactionTask.cancel_requested.is_(False),
                    *(
                        [ReactionTask.campaign_id.in_(self._campaign_id_scope)]
                        if self._campaign_id_scope is not None
                        else []
                    ),
                )
                .values(
                    status=ReactionTaskStatus.RUNNING,
                    lease_owner=self._worker_id,
                    lease_token=token,
                    lease_until=now + timedelta(seconds=self._settings.worker_lease_seconds),
                    started_at=task.started_at or now,
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
            return task.id, token

    async def _heartbeat(self, task_id: int, token: str, stop_event: asyncio.Event) -> None:
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
                        update(ReactionTask)
                        .where(
                            ReactionTask.id == task_id,
                            ReactionTask.status == ReactionTaskStatus.RUNNING,
                            ReactionTask.lease_owner == self._worker_id,
                            ReactionTask.lease_token == token,
                        )
                        .values(
                            lease_until=utc_now()
                            + timedelta(seconds=self._settings.worker_lease_seconds)
                        )
                    )
                    await session.commit()
            except Exception:  # noqa: BLE001 - expiry recovery is safer than killing the step
                logger.exception("Reaction-task heartbeat failed for %s", task_id)
                return
            if renewed.rowcount != 1:
                logger.warning("Reaction task %s lost its lease", task_id)
                return

    async def _process(self, task_id: int, token: str) -> None:
        terminal: tuple[ReactionTaskStatus, str, str] | None = None
        needs_failover = False
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            account = await session.get(Account, task.reactor_account_id)
            campaign: Campaign | None = None
            target: Target | None = None
            comment_run: ChannelCommentRun | None = None
            comment_rule: ChannelCommentRule | None = None
            comment_message: ChannelCommentMessage | None = None
            is_comment = task.channel_comment_run_id is not None
            if is_comment:
                comment_run = await session.get(ChannelCommentRun, task.channel_comment_run_id)
                if comment_run is not None:
                    comment_rule = await session.get(ChannelCommentRule, comment_run.rule_id)
                    comment_message = await session.scalar(
                        select(ChannelCommentMessage).where(
                            ChannelCommentMessage.run_id == comment_run.id,
                            ChannelCommentMessage.message_id == task.message_id,
                        )
                    )
            else:
                campaign = await session.get(Campaign, task.campaign_id)
                target = await session.get(Target, task.target_id)
            if task.cancel_requested:
                terminal = (
                    ReactionTaskStatus.CANCELLED,
                    "cancel requested before reaction",
                    "cancelled_before_send",
                )
            elif task.attempt_count >= task.max_attempts:
                terminal = (
                    ReactionTaskStatus.FAILED,
                    "maximum reaction delivery attempts exhausted",
                    "reaction_attempts_exhausted",
                )
            elif (
                account is None
                or (is_comment and (comment_run is None or comment_rule is None))
                or (not is_comment and (campaign is None or target is None))
            ):
                terminal = (
                    ReactionTaskStatus.SKIPPED,
                    "context is missing",
                    "reaction_context_missing",
                )
            elif is_comment and (
                comment_message is not None and comment_message.status == "missing"
            ):
                terminal = (
                    ReactionTaskStatus.SKIPPED,
                    "source comment is confirmed missing",
                    "comment_missing",
                )
            elif is_comment and (
                not comment_rule.enabled
                or not comment_rule.approved_by_operator
                or not comment_rule.reactions_enabled
            ):
                terminal = (
                    ReactionTaskStatus.CANCELLED,
                    "comment reactions disabled",
                    "reactions_disabled",
                )
            elif not is_comment and (not campaign.enabled or not campaign.reactions_enabled):
                terminal = (
                    ReactionTaskStatus.CANCELLED,
                    "reactions disabled",
                    "reactions_disabled",
                )
            elif task.source_account_id == task.reactor_account_id:
                terminal = (
                    ReactionTaskStatus.SKIPPED,
                    "source account cannot react to itself",
                    "self_reaction",
                )
            elif (
                account.status != AccountStatus.ACTIVE
                or (not self._settings.dry_run and not account.authorized)
                or not account.ownership_confirmed
            ):
                needs_failover = True
            elif is_comment:
                comment_route = await session.scalar(
                    select(ChannelCommentReactionAccountState).where(
                        ChannelCommentReactionAccountState.rule_id == comment_rule.id,
                        ChannelCommentReactionAccountState.account_id == account.id,
                    )
                )
                if comment_route is not None and comment_route.send_blocked:
                    needs_failover = True
                else:
                    resume_at = self._runner.account_resume_at(account)
            else:
                route_state = await session.scalar(
                    select(TargetAccountState).where(
                        TargetAccountState.account_id == account.id,
                        TargetAccountState.target_id == target.id,
                    )
                )
                if not self._route_is_ready(route_state):
                    needs_failover = True
                else:
                    resume_at = self._runner.account_resume_at(account)
        if terminal is not None:
            await self._finish(task_id, token, *terminal)
            return
        if needs_failover:
            assert account is not None
            if await self._failover_reactor(task_id, token, int(account.id), None):
                return
            await self._finish(
                task_id,
                token,
                ReactionTaskStatus.SKIPPED,
                "no eligible replacement reactor for this chat",
                "reactor_unavailable",
            )
            return
        assert account is not None
        if resume_at is not None and resume_at > utc_now():
            await self._defer(task_id, token, resume_at, "account_pacing")
            return

        async def deliver() -> str | None:
            await self._record_delivery_attempt(task_id, token)
            if is_comment:
                assert comment_run is not None and comment_rule is not None
                delivered_emoji = await self._gateway.send_channel_comment_reaction(
                    account=account,
                    channel_ref=comment_rule.channel_ref,
                    channel_post_id=int(comment_run.source_post_id),
                    message_id=task.message_id,
                    emoji=task.emoji,
                    **(
                        {
                            "expected_discussion_peer_id": (
                                comment_message.discussion_peer_id
                                if comment_message is not None
                                else None
                            ),
                            "expected_thread_root_message_id": comment_run.thread_root_message_id,
                        }
                        if getattr(self._gateway, "supports_channel_comment_dispatch_hook", False)
                        else {}
                    ),
                )
            else:
                assert target is not None
                delivered_emoji = await self._gateway.send_reaction(
                    account=account,
                    target=target,
                    message_id=task.message_id,
                    emoji=task.emoji,
                )
            await self._runner.record_external_account_send(
                int(account.id),
                operation_key=(
                    f"comment-reaction:{task.id}" if is_comment else f"campaign-reaction:{task.id}"
                ),
            )
            return delivered_emoji

        try:
            delivered_emoji = await self._runner.run_account_send_operation(
                account.id,
                deliver,
            )
        except AccountSendDeferred as exc:
            await self._defer(task_id, token, exc.resume_at, "account_pacing")
            return
        except _ReactionCancelledBeforeDelivery:
            await self._finish(
                task_id,
                token,
                ReactionTaskStatus.CANCELLED,
                "cancel requested before reaction",
                "cancelled_before_send",
            )
            return
        except Exception as exc:  # noqa: BLE001 - persist all gateway failures
            code = failure_code(exc, "reaction_failed")
            account_failure_recorded = False
            if code in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(int(account.id), exc)
                account_failure_recorded = True
            target_failure_recorded = False
            if not is_comment and code in OBSERVED_TARGET_FAILURE_CODES:
                assert target is not None
                await self._runner.record_observed_target_failure(
                    int(account.id),
                    int(target.id),
                    exc,
                )
                target_failure_recorded = True
            elif is_comment and code in ACCOUNT_TARGET_BLOCK_CODES:
                await self._record_comment_route_failure(task_id, int(account.id), exc)
                target_failure_recorded = True
            if code in ACCOUNT_TARGET_BLOCK_CODES | ACCOUNT_WIDE_CODES:
                if await self._failover_reactor(task_id, token, int(account.id), exc):
                    return
            await self._handle_error(
                task_id,
                token,
                int(account.id),
                exc,
                account_failure_recorded=account_failure_recorded,
                target_failure_recorded=target_failure_recorded,
            )
            return
        actual_emoji = delivered_emoji if isinstance(delivered_emoji, str) else task.emoji
        await self._finish(
            task_id,
            token,
            ReactionTaskStatus.SUCCEEDED,
            None,
            None,
            actual_emoji=actual_emoji,
        )

    async def _record_delivery_attempt(self, task_id: int, token: str) -> None:
        """Count only a real Telegram delivery attempt, never local deferrals."""
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if task.cancel_requested:
                raise _ReactionCancelledBeforeDelivery
            task.attempt_count += 1
            await session.commit()

    async def _handle_error(
        self,
        task_id: int,
        token: str,
        account_id: int,
        exc: BaseException,
        *,
        account_failure_recorded: bool = False,
        target_failure_recorded: bool = False,
    ) -> None:
        code = failure_code(exc, "reaction_failed")
        target_id = 0
        async with self._session_factory() as session:
            task = await session.get(ReactionTask, task_id)
            if task is not None:
                target_id = int(task.target_id or 0)
                if account_id <= 0:
                    account_id = int(task.reactor_account_id) if task is not None else 0
        if code in ACCOUNT_WIDE_CODES and account_id > 0 and not account_failure_recorded:
            await self._runner.handle_external_account_failure(account_id, exc)
        if (
            code in OBSERVED_TARGET_FAILURE_CODES
            and account_id > 0
            and target_id > 0
            and not target_failure_recorded
        ):
            await self._runner.record_observed_target_failure(account_id, target_id, exc)
        retryable = bool(getattr(exc, "retryable", False))
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if retryable and not task.cancel_requested and task.attempt_count < task.max_attempts:
                retry_after = getattr(exc, "retry_after", None)
                wait = max(15, int(retry_after or self._settings.worker_retry_base_seconds))
                task.status = ReactionTaskStatus.QUEUED
                task.scheduled_for = utc_now() + timedelta(seconds=wait)
                task.error = safe_error_message(exc)
                task.error_code = code
                self._release(task)
                await session.commit()
                return
        skipped_codes = ACCOUNT_TARGET_BLOCK_CODES | frozenset(
            {
                "reaction_invalid",
                "reaction_unavailable",
                "target_unavailable",
                "request_invalid",
            }
        )
        status = ReactionTaskStatus.SKIPPED if code in skipped_codes else ReactionTaskStatus.FAILED
        await self._finish(task_id, token, status, safe_error_message(exc), code)

    async def _failover_reactor(
        self,
        task_id: int,
        token: str,
        failed_account_id: int,
        exc: BaseException | None,
    ) -> bool:
        """Move a reaction to another unused, reachable account from its configured pool."""

        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if task.cancel_requested:
                return False
            is_comment = task.channel_comment_run_id is not None
            comment_rule: ChannelCommentRule | None = None
            if is_comment:
                comment_run = await session.get(ChannelCommentRun, task.channel_comment_run_id)
                if comment_run is None:
                    return False
                comment_rule = await session.get(ChannelCommentRule, comment_run.rule_id)
                if (
                    comment_rule is None
                    or not comment_rule.enabled
                    or not comment_rule.reactions_enabled
                ):
                    return False
                try:
                    raw_ids = json.loads(comment_rule.reaction_account_ids_json or "[]")
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw_ids = []
                candidate_ids = [
                    int(value)
                    for value in raw_ids
                    if isinstance(value, int) and not isinstance(value, bool) and value > 0
                ]
                same_message = ReactionTask.channel_comment_run_id == task.channel_comment_run_id
            else:
                campaign = await session.get(Campaign, task.campaign_id)
                run = await session.get(CampaignRun, task.run_id)
                if (
                    campaign is None
                    or run is None
                    or not campaign.enabled
                    or not campaign.reactions_enabled
                ):
                    return False
                if campaign.mode == CampaignMode.DIALOGUE:
                    candidate_ids = [
                        int(step.account_id) for step in self._runner._dialogue_steps(run, campaign)
                    ]
                elif run.sender_mode_snapshot == BroadcastSenderMode.INDEPENDENT:
                    candidate_ids = list(
                        await session.scalars(
                            select(CampaignAccount.account_id)
                            .where(CampaignAccount.campaign_id == campaign.id)
                            .order_by(CampaignAccount.id)
                        )
                    )
                elif run.sender_pool_snapshot_json:
                    try:
                        candidate_ids = [
                            int(value) for value in json.loads(run.sender_pool_snapshot_json)
                        ]
                    except (TypeError, ValueError, json.JSONDecodeError):
                        candidate_ids = []
                else:
                    candidate_ids = list(
                        await session.scalars(
                            select(CampaignAccount.account_id)
                            .where(CampaignAccount.campaign_id == campaign.id)
                            .order_by(CampaignAccount.id)
                        )
                    )
                same_message = ReactionTask.run_id == task.run_id
            already_assigned = set(
                await session.scalars(
                    select(ReactionTask.reactor_account_id).where(
                        same_message,
                        ReactionTask.message_id == task.message_id,
                        ReactionTask.id != task.id,
                    )
                )
            )
            candidate_ids = list(
                dict.fromkeys(
                    int(account_id)
                    for account_id in candidate_ids
                    if int(account_id)
                    not in {
                        int(task.source_account_id),
                        failed_account_id,
                    }
                    and int(account_id) not in already_assigned
                )
            )
            if not candidate_ids:
                return False
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(candidate_ids)))
            )
            accounts_by_id = {int(account.id): account for account in accounts}
            if is_comment:
                assert comment_rule is not None
                blocked_ids = set(
                    await session.scalars(
                        select(ChannelCommentReactionAccountState.account_id).where(
                            ChannelCommentReactionAccountState.rule_id == comment_rule.id,
                            ChannelCommentReactionAccountState.account_id.in_(candidate_ids),
                            ChannelCommentReactionAccountState.send_blocked.is_(True),
                        )
                    )
                )
                states_by_account: dict[int, TargetAccountState] = {}
            else:
                states = list(
                    await session.scalars(
                        select(TargetAccountState).where(
                            TargetAccountState.target_id == task.target_id,
                            TargetAccountState.account_id.in_(candidate_ids),
                        )
                    )
                )
                states_by_account = {int(state.account_id): state for state in states}
                blocked_ids = set()
            eligible = [
                accounts_by_id[account_id]
                for account_id in candidate_ids
                if account_id in accounts_by_id
                and self._account_is_active(accounts_by_id[account_id])
                and account_id not in blocked_ids
                # Comment reactors are live-preflighted when the operator
                # enables or changes their pool. They have no Target row—the
                # destination is the discussion peer behind this channel post.
                # Applying the ordinary campaign TargetAccountState gate here
                # therefore made every live failover impossible.
                and (is_comment or self._route_is_ready(states_by_account.get(account_id)))
            ]
            if not eligible:
                return False
            now = utc_now()
            eligible.sort(
                key=lambda account: (
                    (
                        (resume_at := self._runner.account_resume_at(account)) is not None
                        and resume_at > now
                    ),
                    candidate_ids.index(int(account.id)),
                )
            )
            replacement = eligible[0]
            resume_at = self._runner.account_resume_at(replacement)
            task.reactor_account_id = int(replacement.id)
            task.status = ReactionTaskStatus.QUEUED
            task.scheduled_for = max(now, resume_at) if resume_at is not None else now
            task.error = safe_error_message(exc) if exc is not None else None
            task.error_code = failure_code(exc, "reactor_failover")
            self._release(task)
            add_audit_event(
                session,
                event_type="reaction.reactor_failed_over",
                entity_type="reaction_task",
                entity_id=task.id,
                details={
                    "failed_account_id": failed_account_id,
                    "replacement_account_id": int(replacement.id),
                    "target_id": int(task.target_id or 0),
                    "channel_comment_run_id": task.channel_comment_run_id,
                    "message_id": int(task.message_id),
                },
                actor="reaction-worker",
            )
            await session.commit()
            return True

    async def _record_comment_route_failure(
        self,
        task_id: int,
        account_id: int,
        exc: BaseException,
    ) -> None:
        code = failure_code(exc, "account_banned_in_chat")
        async with self._session_factory() as session:
            task = await session.get(ReactionTask, task_id)
            if task is None or task.channel_comment_run_id is None:
                return
            run = await session.get(ChannelCommentRun, task.channel_comment_run_id)
            if run is None:
                return
            state = await session.scalar(
                select(ChannelCommentReactionAccountState).where(
                    ChannelCommentReactionAccountState.rule_id == run.rule_id,
                    ChannelCommentReactionAccountState.account_id == account_id,
                )
            )
            if state is None:
                state = ChannelCommentReactionAccountState(
                    rule_id=run.rule_id,
                    account_id=account_id,
                )
                session.add(state)
            state.send_blocked = True
            state.blocked_reason = code
            state.blocked_at = utc_now()
            state.last_error = safe_error_message(exc)[:500]
            add_audit_event(
                session,
                event_type="channel_comment.reactor_blocked",
                entity_type="channel_comment_rule",
                entity_id=run.rule_id,
                details={
                    "account_id": account_id,
                    "channel_comment_run_id": int(run.id),
                    "error_code": code,
                },
                actor="reaction-worker",
            )
            await session.commit()

    def _account_is_active(self, account: Account) -> bool:
        return bool(
            account.status == AccountStatus.ACTIVE
            and (account.authorized or self._settings.dry_run)
            and account.ownership_confirmed
        )

    def _route_is_ready(self, state: TargetAccountState | None) -> bool:
        """Require a recent usable route, including a configured auto-join route."""

        if self._settings.dry_run:
            return state is None or not state.send_blocked
        if state is None or state.send_blocked:
            return False
        checked_at = ensure_utc(state.membership_checked_at)
        if checked_at is None:
            return False
        maximum_age = timedelta(
            seconds=max(1, int(self._settings.preflight_max_age_seconds))
        )
        if checked_at < utc_now() - maximum_age:
            return False
        if state.membership_status == MembershipStatus.MEMBER:
            return state.can_send is True
        return bool(
            self._settings.account_auto_join
            and state.membership_status == MembershipStatus.NOT_MEMBER
            and state.membership_error_code == "target_not_joined"
        )

    async def _defer(self, task_id: int, token: str, scheduled_for, reason: str) -> None:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if task.cancel_requested:
                task.status = ReactionTaskStatus.CANCELLED
                task.finished_at = utc_now()
                task.error = "cancel requested before reaction"
                task.error_code = "cancelled_before_send"
            else:
                task.status = ReactionTaskStatus.QUEUED
                task.scheduled_for = scheduled_for
                task.error = reason
                task.error_code = reason
            self._release(task)
            await session.commit()

    async def _finish(
        self,
        task_id: int,
        token: str,
        status: ReactionTaskStatus,
        error: str | None,
        code: str | None,
        *,
        actual_emoji: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            requested_emoji = task.emoji
            if task.cancel_requested:
                if status == ReactionTaskStatus.SUCCEEDED:
                    error = "reaction completed while cancellation was being requested"
                    code = "completed_during_cancel"
                else:
                    status = ReactionTaskStatus.CANCELLED
                    error = error or "cancel requested before reaction completed"
                    code = "cancelled_before_send"
            if status == ReactionTaskStatus.SUCCEEDED and actual_emoji is not None:
                task.emoji = actual_emoji
            task.status = status
            task.error = error
            task.error_code = code
            task.finished_at = utc_now()
            self._release(task)
            add_audit_event(
                session,
                event_type=f"reaction.{status.value}",
                entity_type="reaction_task",
                entity_id=task.id,
                details={
                    "campaign_id": task.campaign_id,
                    "channel_comment_run_id": task.channel_comment_run_id,
                    "message_id": task.message_id,
                    "emoji": task.emoji,
                    "requested_emoji": requested_emoji,
                    "fallback_used": task.emoji != requested_emoji,
                    "error_code": code,
                },
                actor="reaction-worker",
            )
            await session.commit()

    @staticmethod
    async def _require_running(session: AsyncSession, task_id: int, token: str) -> ReactionTask:
        task = await session.get(ReactionTask, task_id)
        if task is None or task.status != ReactionTaskStatus.RUNNING or task.lease_token != token:
            raise RuntimeError("reaction task lease was lost")
        return task

    @staticmethod
    def _release(task: ReactionTask) -> None:
        task.lease_owner = None
        task.lease_token = None
        task.lease_until = None
