# ruff: noqa: E501
"""Opt-in, sampled reactions to new messages in operator-approved chats.

The feature deliberately reacts with one selected account at most once per sampled
participant message.  It never backfills chat history and never reacts to a known
service account's own message.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.failure_policy import (
    ACCOUNT_TARGET_BLOCK_CODES,
    ACCOUNT_WIDE_CODES,
    OBSERVED_TARGET_FAILURE_CODES,
    failure_code,
)
from proxima_traffic.gateways import (
    ORDINARY_REACTION_EMOJI,
    MessageGateway,
    ParticipantReactionGateway,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    MembershipStatus,
    ParticipantReactionRule,
    ParticipantReactionTargetState,
    ParticipantReactionTask,
    ParticipantReactionTaskStatus,
    Target,
    TargetAccountState,
    TargetList,
    TargetListItem,
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

logger = logging.getLogger(__name__)
_ALLOWED_EMOJI = ORDINARY_REACTION_EMOJI


@dataclass(frozen=True, slots=True)
class ParticipantReactionRuleView:
    rule: ParticipantReactionRule
    target_list_name: str
    target_count: int
    queued: int
    running: int
    succeeded: int
    failed: int
    skipped: int


class ParticipantReactionService:
    """Scan selected chat lists and deliver small, durable reaction tasks."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway & ParticipantReactionGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        worker_id: str | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._worker_id = worker_id or f"participant-reactions-{uuid4().hex[:10]}"
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def create_rule(
        self,
        *,
        name: str,
        target_list_id: int,
        account_ids: list[int],
        emoji: str,
        sample_numerator: int,
        sample_denominator: int,
        actor: str = "control-bot",
    ) -> ParticipantReactionRule:
        normalized_name = " ".join(name.split())
        if not 1 <= len(normalized_name) <= 150:
            raise ConflictError("название правила должно содержать от 1 до 150 символов")
        self._validate_sample(sample_numerator, sample_denominator)
        if emoji not in _ALLOWED_EMOJI:
            raise ConflictError("выберите одну из доступных реакций")
        normalized_accounts = self._normalize_account_ids(account_ids)
        if not normalized_accounts:
            raise ConflictError("выберите хотя бы один аккаунт для реакций")
        async with self._session_factory() as session:
            target_list = await session.get(TargetList, target_list_id)
            if target_list is None:
                raise NotFoundError("база чатов не найдена")
            count = await self._approved_target_count(session, target_list_id)
            if count == 0:
                raise ConflictError("в базе нет включённых разрешённых чатов")
            for account_id in normalized_accounts:
                self._validate_account(await session.get(Account, account_id))
            rule = ParticipantReactionRule(
                name=normalized_name,
                target_list_id=target_list_id,
                account_ids_json=json.dumps(normalized_accounts),
                emoji=emoji,
                sample_numerator=sample_numerator,
                sample_denominator=sample_denominator,
                approved_by_operator=True,
                enabled=True,
            )
            session.add(rule)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("правило с таким названием уже есть") from exc
            add_audit_event(
                session,
                event_type="participant_reaction_rule.created",
                entity_type="participant_reaction_rule",
                entity_id=rule.id,
                details={
                    "target_list_id": target_list_id,
                    "account_ids": normalized_accounts,
                    "emoji": emoji,
                    "sample": f"{sample_numerator}/{sample_denominator}",
                },
                actor=actor,
            )
            await session.commit()
            return rule

    async def list_rules(
        self, page: int, page_size: int
    ) -> tuple[list[ParticipantReactionRuleView], int]:
        normalized_page = max(0, page)
        normalized_size = max(1, min(page_size, 50))
        async with self._session_factory() as session:
            total = int(
                await session.scalar(select(func.count()).select_from(ParticipantReactionRule)) or 0
            )
            rules = list(
                await session.scalars(
                    select(ParticipantReactionRule)
                    .order_by(
                        ParticipantReactionRule.enabled.desc(), ParticipantReactionRule.id.desc()
                    )
                    .offset(normalized_page * normalized_size)
                    .limit(normalized_size)
                )
            )
            return await self._views_for_rules(session, rules), total

    async def get_rule_view(self, rule_id: int) -> ParticipantReactionRuleView:
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            if rule is None:
                raise NotFoundError("правило реакций не найдено")
            return await self._view(session, rule)

    async def set_rule_enabled(
        self, rule_id: int, enabled: bool, *, actor: str = "control-bot"
    ) -> None:
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            if rule is None:
                raise NotFoundError("правило реакций не найдено")
            rule.enabled = enabled
            if not enabled:
                await self._cancel_pending_tasks(
                    session,
                    rule.id,
                    "rule paused",
                    code="rule_paused",
                )
            add_audit_event(
                session,
                event_type="participant_reaction_rule.enabled"
                if enabled
                else "participant_reaction_rule.paused",
                entity_type="participant_reaction_rule",
                entity_id=rule.id,
                details={"enabled": enabled},
                actor=actor,
            )
            await session.commit()

    async def update_rule(
        self,
        rule_id: int,
        *,
        target_list_id: int,
        account_ids: list[int],
        emoji: str,
        sample_numerator: int,
        sample_denominator: int,
        actor: str = "control-bot",
    ) -> ParticipantReactionRule:
        """Replace settings without allowing old queued work to leak through."""
        self._validate_sample(sample_numerator, sample_denominator)
        if emoji not in _ALLOWED_EMOJI:
            raise ConflictError("выберите одну из доступных реакций")
        normalized_accounts = self._normalize_account_ids(account_ids)
        if not normalized_accounts:
            raise ConflictError("выберите хотя бы один аккаунт для реакций")
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            if rule is None:
                raise NotFoundError("правило реакций не найдено")
            if await session.get(TargetList, target_list_id) is None:
                raise NotFoundError("база чатов не найдена")
            if await self._approved_target_count(session, target_list_id) == 0:
                raise ConflictError("в базе нет включённых разрешённых чатов")
            for account_id in normalized_accounts:
                self._validate_account(await session.get(Account, account_id))
            target_list_changed = rule.target_list_id != target_list_id
            rule.target_list_id = target_list_id
            rule.account_ids_json = json.dumps(normalized_accounts)
            rule.emoji = emoji
            rule.sample_numerator = sample_numerator
            rule.sample_denominator = sample_denominator
            rule.last_error = None
            rule.last_error_code = None
            await self._cancel_pending_tasks(session, rule.id, "rule configuration changed")
            if target_list_changed:
                await session.execute(
                    ParticipantReactionTargetState.__table__.delete().where(
                        ParticipantReactionTargetState.rule_id == rule.id
                    )
                )
                rule.last_scanned_at = None
            add_audit_event(
                session,
                event_type="participant_reaction_rule.updated",
                entity_type="participant_reaction_rule",
                entity_id=rule.id,
                details={
                    "target_list_id": target_list_id,
                    "account_ids": normalized_accounts,
                    "emoji": emoji,
                    "sample": f"{sample_numerator}/{sample_denominator}",
                },
                actor=actor,
            )
            await session.commit()
            return rule

    async def delete_rule(self, rule_id: int, *, actor: str = "control-bot") -> None:
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            if rule is None:
                raise NotFoundError("правило реакций не найдено")
            add_audit_event(
                session,
                event_type="participant_reaction_rule.deleted",
                entity_type="participant_reaction_rule",
                entity_id=rule.id,
                details={"name": rule.name},
                actor=actor,
            )
            await session.delete(rule)
            await session.commit()

    async def recent_tasks(self, rule_id: int, limit: int = 8) -> list[ParticipantReactionTask]:
        async with self._session_factory() as session:
            return list(
                await session.scalars(
                    select(ParticipantReactionTask)
                    .where(ParticipantReactionTask.rule_id == rule_id)
                    .order_by(ParticipantReactionTask.id.desc())
                    .limit(max(1, min(limit, 30)))
                )
            )

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop_event.is_set():
            processed = False
            try:
                await self.recover_expired_leases()
                await self.scan_due_rules()
                processed = await self.process_next()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - scanning or claiming must not stop the worker
                logger.exception("Participant-reaction worker iteration failed")
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
                    timeout=self._settings.participant_reactions_poll_seconds,
                )
            except TimeoutError:
                pass

    async def scan_due_rules(self) -> int:
        cutoff = utc_now() - timedelta(
            seconds=self._settings.participant_reactions_scan_interval_seconds
        )
        async with self._session_factory() as session:
            ids = list(
                await session.scalars(
                    select(ParticipantReactionRule.id)
                    .where(
                        ParticipantReactionRule.enabled.is_(True),
                        ParticipantReactionRule.approved_by_operator.is_(True),
                        (ParticipantReactionRule.last_scanned_at.is_(None))
                        | (ParticipantReactionRule.last_scanned_at <= cutoff),
                    )
                    .order_by(ParticipantReactionRule.last_scanned_at, ParticipantReactionRule.id)
                )
            )
        for rule_id in ids:
            if self._stop_event.is_set():
                break
            await self.scan_rule(rule_id)
        return len(ids)

    async def scan_rule(self, rule_id: int) -> None:
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            if rule is None or not rule.enabled or not rule.approved_by_operator:
                return
            accounts = await self._accounts_for_rule(session, rule)
            targets = await self._targets_for_rule(session, rule)
            rule.last_scanned_at = utc_now()
            if not accounts:
                rule.last_error = "no eligible reactor account"
                rule.last_error_code = "reactor_unavailable"
                await session.commit()
                return
            if not targets:
                rule.last_error = "no eligible chats in list"
                rule.last_error_code = "target_list_empty"
                await session.commit()
                return
            own_sender_ids = set(
                await session.scalars(
                    select(Account.telegram_user_id).where(
                        Account.ownership_confirmed.is_(True),
                        Account.telegram_user_id.is_not(None),
                    )
                )
            )
            await session.commit()
        failed_targets = 0
        for target in targets:
            if self._stop_event.is_set():
                return
            async with self._session_factory() as session:
                refreshed_accounts = list(
                    await session.scalars(
                        select(Account).where(
                            Account.id.in_([int(account.id) for account in accounts])
                        )
                    )
                )
                eligible_accounts = await self._eligible_accounts_for_target(
                    session,
                    refreshed_accounts,
                    int(target.id),
                    require_send_capacity=False,
                )
            if not eligible_accounts:
                await self._store_scan_error(
                    rule_id,
                    int(target.id),
                    ConflictError("no eligible account for this chat"),
                )
                failed_targets += 1
                continue
            if not await self._scan_target(
                rule_id,
                eligible_accounts,
                own_sender_ids,
                target,
            ):
                failed_targets += 1
        if failed_targets == 0:
            async with self._session_factory() as session:
                stored_rule = await session.get(ParticipantReactionRule, rule_id)
                if stored_rule is not None:
                    stored_rule.last_error = None
                    stored_rule.last_error_code = None
                    await session.commit()

    async def _scan_target(
        self,
        rule_id: int,
        accounts: list[Account],
        own_sender_ids: set[int],
        target: Target,
    ) -> bool:
        async with self._session_factory() as session:
            state = await session.scalar(
                select(ParticipantReactionTargetState).where(
                    ParticipantReactionTargetState.rule_id == rule_id,
                    ParticipantReactionTargetState.target_id == target.id,
                )
            )
            if state is None:
                state = ParticipantReactionTargetState(rule_id=rule_id, target_id=target.id)
                session.add(state)
                await session.flush()
            checkpoint = state.last_seen_message_id
            bootstrap = not state.primed
            # The scanner runs outside this transaction; persist a newly allocated
            # cursor row first so the following result can always advance it.
            await session.commit()
        last_error: BaseException | None = None
        scan = None
        for scanner in accounts:
            try:
                scan = await self._gateway.scan_participant_messages(
                    account=scanner,
                    target=target,
                    after_message_id=checkpoint,
                    limit=self._settings.participant_reactions_scan_limit,
                    bootstrap=bootstrap,
                )
            except Exception as exc:  # noqa: BLE001 - try another eligible route
                last_error = exc
                code = failure_code(exc, "participant_scan_failed")
                if code in ACCOUNT_WIDE_CODES:
                    await self._runner.handle_external_account_failure(int(scanner.id), exc)
                if code in OBSERVED_TARGET_FAILURE_CODES:
                    await self._runner.record_observed_target_failure(
                        int(scanner.id),
                        int(target.id),
                        exc,
                    )
                continue
            break
        if scan is None:
            await self._store_scan_error(
                rule_id,
                target.id,
                last_error or RuntimeError("participant scan failed"),
            )
            return False
        await self._persist_scan(rule_id, target.id, accounts, own_sender_ids, scan.messages)
        return True

    async def _persist_scan(
        self,
        rule_id: int,
        target_id: int,
        accounts: list[Account],
        own_sender_ids: set[int],
        messages,
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            state = await session.scalar(
                select(ParticipantReactionTargetState).where(
                    ParticipantReactionTargetState.rule_id == rule_id,
                    ParticipantReactionTargetState.target_id == target_id,
                )
            )
            if rule is None or state is None or not rule.enabled:
                return
            newest_id = max(
                (item.message_id for item in messages), default=state.last_seen_message_id
            )
            # First sighting only establishes the cursor: old chat history is never engaged.
            if not state.primed:
                state.primed = True
                state.last_seen_message_id = newest_id
                state.last_scanned_at = now
                state.last_error = None
                state.last_error_code = None
                await session.commit()
                return
            recent_sender_ids: set[int] = set()
            sender_cooldown = int(self._settings.participant_reactions_sender_cooldown_seconds)
            if sender_cooldown > 0:
                recent_sender_ids = set(
                    await session.scalars(
                        select(ParticipantReactionTask.source_sender_id).where(
                            ParticipantReactionTask.rule_id == rule.id,
                            ParticipantReactionTask.target_id == target_id,
                            ParticipantReactionTask.source_sender_id.is_not(None),
                            ParticipantReactionTask.created_at
                            >= now - timedelta(seconds=sender_cooldown),
                        )
                    )
                )
            ready_accounts = [
                account
                for account in accounts
                if (resume_at := self._runner.account_resume_at(account)) is None
                or resume_at <= now
            ]
            reactor_pool = ready_accounts or accounts
            for message in messages:
                if message.message_id <= state.last_seen_message_id:
                    continue
                if (
                    message.sender_id is None
                    or message.sender_id <= 0
                    or message.sender_id in own_sender_ids
                    or message.sender_id in recent_sender_ids
                    or message.is_bot
                    or message.is_outgoing
                ):
                    continue
                sent_at = ensure_utc(message.sent_at)
                if sent_at is not None and sent_at < now - timedelta(
                    seconds=self._settings.participant_reactions_max_message_age_seconds
                ):
                    continue
                if not self._sample(rule, target_id, message.message_id):
                    continue
                reactor = self._pick_reactor(
                    rule,
                    target_id,
                    message.message_id,
                    reactor_pool,
                )
                session.add(
                    ParticipantReactionTask(
                        rule_id=rule.id,
                        target_id=target_id,
                        reactor_account_id=reactor.id,
                        message_id=message.message_id,
                        source_sender_id=message.sender_id,
                        emoji=rule.emoji,
                        scheduled_for=now
                        + timedelta(seconds=self._delay(rule, target_id, message.message_id)),
                    )
                )
                recent_sender_ids.add(int(message.sender_id))
            state.last_seen_message_id = newest_id
            state.last_scanned_at = now
            state.last_error = None
            state.last_error_code = None
            try:
                await session.commit()
            except IntegrityError:
                # Another scanner/process may have safely inserted the same unique task.
                await session.rollback()

    async def process_next(self) -> bool:
        claimed = await self._claim_next()
        if claimed is None:
            return False
        task_id, token = claimed
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat(task_id, token, heartbeat_stop),
            name=f"participant-reaction-heartbeat-{task_id}",
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
                        "participant reaction operation timed out",
                        code="participant_reaction_timeout",
                        retry_after=self._settings.worker_retry_base_seconds,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - preserve an explicit result for unknown effects
                logger.exception("Participant reaction task %s crashed", task_id)
                await self._handle_error(task_id, token, 0, exc)
        finally:
            heartbeat_stop.set()
            await heartbeat_task
        return True

    async def _claim_next(self) -> tuple[int, str] | None:
        now = utc_now()
        async with self._session_factory() as session:
            task = await session.scalar(
                select(ParticipantReactionTask)
                .where(
                    ParticipantReactionTask.status == ParticipantReactionTaskStatus.QUEUED,
                    ParticipantReactionTask.scheduled_for <= now,
                    ParticipantReactionTask.cancel_requested.is_(False),
                )
                .order_by(ParticipantReactionTask.scheduled_for, ParticipantReactionTask.id)
                .limit(1)
            )
            if task is None:
                return None
            token = uuid4().hex
            result = await session.execute(
                update(ParticipantReactionTask)
                .where(
                    ParticipantReactionTask.id == task.id,
                    ParticipantReactionTask.status == ParticipantReactionTaskStatus.QUEUED,
                    ParticipantReactionTask.cancel_requested.is_(False),
                )
                .values(
                    status=ParticipantReactionTaskStatus.RUNNING,
                    lease_owner=self._worker_id,
                    lease_token=token,
                    lease_until=now + timedelta(seconds=self._settings.worker_lease_seconds),
                    started_at=task.started_at or now,
                    attempt_count=task.attempt_count + 1,
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
                        update(ParticipantReactionTask)
                        .where(
                            ParticipantReactionTask.id == task_id,
                            ParticipantReactionTask.status == ParticipantReactionTaskStatus.RUNNING,
                            ParticipantReactionTask.lease_owner == self._worker_id,
                            ParticipantReactionTask.lease_token == token,
                        )
                        .values(
                            lease_until=utc_now()
                            + timedelta(seconds=self._settings.worker_lease_seconds)
                        )
                    )
                    await session.commit()
            except Exception:  # noqa: BLE001 - expiry recovery is safer than killing the step
                logger.exception("Participant-reaction heartbeat failed for %s", task_id)
                return
            if renewed.rowcount != 1:
                logger.warning("Participant reaction task %s lost its lease", task_id)
                return

    async def _process(self, task_id: int, token: str) -> None:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            rule = await session.get(ParticipantReactionRule, task.rule_id)
            account = await session.get(Account, task.reactor_account_id)
            target = await session.get(Target, task.target_id)
            if task.cancel_requested:
                terminal = (
                    ParticipantReactionTaskStatus.CANCELLED,
                    "cancel requested before reaction",
                    "cancelled_before_send",
                )
            elif rule is None or account is None or target is None:
                terminal = (
                    ParticipantReactionTaskStatus.SKIPPED,
                    "context is missing",
                    "context_missing",
                )
            elif not rule.enabled or not rule.approved_by_operator:
                terminal = (ParticipantReactionTaskStatus.CANCELLED, "rule paused", "rule_paused")
            elif target.quality_status == "bad":
                terminal = (
                    ParticipantReactionTaskStatus.SKIPPED,
                    "chat is unavailable",
                    "target_unavailable",
                )
            elif not await self._target_is_currently_selected(session, rule, target.id):
                terminal = (
                    ParticipantReactionTaskStatus.CANCELLED,
                    "chat was removed from the selected list",
                    "target_removed_from_list",
                )
            elif not self._is_valid_account(account):
                terminal = (
                    ParticipantReactionTaskStatus.SKIPPED,
                    "reactor unavailable",
                    "reactor_unavailable",
                )
            elif not await self._account_is_eligible_for_target(
                session,
                int(account.id),
                int(target.id),
            ):
                terminal = None
                resume_at = None
                needs_failover = True
            else:
                terminal = None
                resume_at = self._runner.account_resume_at(account)
                needs_failover = False
        if terminal is not None:
            await self._finish(task_id, token, *terminal)
            return
        if needs_failover:
            if await self._failover_reactor(task_id, token, int(account.id), None):
                return
            await self._finish(
                task_id,
                token,
                ParticipantReactionTaskStatus.SKIPPED,
                "no eligible reactor for this chat",
                "reactor_unavailable",
            )
            return
        assert account is not None and target is not None
        if resume_at is not None and resume_at > utc_now():
            await self._defer(task_id, token, resume_at, "account_pacing")
            return

        async def deliver() -> None:
            await self._gateway.send_reaction(
                account=account, target=target, message_id=task.message_id, emoji=task.emoji
            )
            await self._runner.record_external_account_send(
                int(account.id),
                operation_key=f"participant-reaction:{task.id}",
            )

        try:
            await self._runner.run_account_send_operation(
                int(account.id),
                deliver,
            )
        except AccountSendDeferred as exc:
            await self._defer(task_id, token, exc.resume_at, "account_pacing")
            return
        except Exception as exc:  # noqa: BLE001
            code = failure_code(exc, "participant_reaction_failed")
            account_failure_recorded = False
            if code in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(int(account.id), exc)
                account_failure_recorded = True
            if code in OBSERVED_TARGET_FAILURE_CODES:
                await self._runner.record_observed_target_failure(
                    int(account.id),
                    int(target.id),
                    exc,
                )
            if code in ACCOUNT_TARGET_BLOCK_CODES | ACCOUNT_WIDE_CODES:
                if await self._failover_reactor(task_id, token, int(account.id), exc):
                    return
            await self._handle_error(
                task_id,
                token,
                int(account.id),
                exc,
                account_failure_recorded=account_failure_recorded,
            )
            return
        await self._finish(task_id, token, ParticipantReactionTaskStatus.SUCCEEDED, None, None)

    async def _handle_error(
        self,
        task_id: int,
        token: str,
        account_id: int,
        exc: BaseException,
        *,
        account_failure_recorded: bool = False,
    ) -> None:
        code = failure_code(exc, "participant_reaction_failed")
        if account_id <= 0:
            async with self._session_factory() as session:
                task = await session.get(ParticipantReactionTask, task_id)
                account_id = int(task.reactor_account_id) if task is not None else 0
        if code in ACCOUNT_WIDE_CODES and account_id > 0 and not account_failure_recorded:
            await self._runner.handle_external_account_failure(account_id, exc)
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if (
                bool(getattr(exc, "retryable", False))
                and not task.cancel_requested
                and task.attempt_count < task.max_attempts
            ):
                wait = max(
                    15,
                    int(
                        getattr(exc, "retry_after", None)
                        or self._settings.worker_retry_base_seconds
                    ),
                )
                task.status = ParticipantReactionTaskStatus.QUEUED
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
        status = (
            ParticipantReactionTaskStatus.SKIPPED
            if code in skipped_codes
            else ParticipantReactionTaskStatus.FAILED
        )
        await self._finish(task_id, token, status, safe_error_message(exc), code)

    async def _defer(self, task_id: int, token: str, scheduled_for, reason: str) -> None:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if task.cancel_requested:
                task.status = ParticipantReactionTaskStatus.CANCELLED
                task.finished_at = utc_now()
                task.error = "cancel requested before reaction"
                task.error_code = "cancelled_before_send"
            else:
                task.status = ParticipantReactionTaskStatus.QUEUED
                task.scheduled_for = scheduled_for
                task.error = reason
                task.error_code = reason
            self._release(task)
            await session.commit()

    async def _finish(
        self, task_id: int, token: str, status, error: str | None, code: str | None
    ) -> None:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            if task.cancel_requested:
                if status == ParticipantReactionTaskStatus.SUCCEEDED:
                    error = "reaction completed while cancellation was being requested"
                    code = "completed_during_cancel"
                else:
                    status = ParticipantReactionTaskStatus.CANCELLED
                    error = error or "cancel requested before reaction completed"
                    code = "cancelled_before_send"
            task.status = status
            task.error = error
            task.error_code = code
            task.finished_at = utc_now()
            self._release(task)
            add_audit_event(
                session,
                event_type=f"participant_reaction.{status.value}",
                entity_type="participant_reaction_task",
                entity_id=task.id,
                details={
                    "rule_id": task.rule_id,
                    "target_id": task.target_id,
                    "message_id": task.message_id,
                    "error_code": code,
                },
                actor="participant-reaction-worker",
            )
            await session.commit()

    async def recover_expired_leases(self) -> int:
        now = utc_now()
        async with self._session_factory() as session:
            tasks = list(
                await session.scalars(
                    select(ParticipantReactionTask).where(
                        ParticipantReactionTask.status == ParticipantReactionTaskStatus.RUNNING,
                        ParticipantReactionTask.lease_until.is_not(None),
                        ParticipantReactionTask.lease_until < now,
                    )
                )
            )
            for task in tasks:
                task.error = "participant reaction worker lease expired"
                task.error_code = "lease_expired"
                if task.cancel_requested:
                    task.status = ParticipantReactionTaskStatus.FAILED
                    task.error = (
                        "cancel was requested while the reaction was running; "
                        "the Telegram outcome is unknown after lease expiry"
                    )
                    task.error_code = "cancelled_outcome_unknown"
                    task.finished_at = now
                elif task.attempt_count >= task.max_attempts:
                    task.status = ParticipantReactionTaskStatus.FAILED
                    task.finished_at = now
                else:
                    task.status = ParticipantReactionTaskStatus.QUEUED
                    task.scheduled_for = now
                self._release(task)
            if tasks:
                await session.commit()
            return len(tasks)

    async def _store_scan_error(self, rule_id: int, target_id: int, exc: BaseException) -> None:
        async with self._session_factory() as session:
            rule = await session.get(ParticipantReactionRule, rule_id)
            state = await session.scalar(
                select(ParticipantReactionTargetState).where(
                    ParticipantReactionTargetState.rule_id == rule_id,
                    ParticipantReactionTargetState.target_id == target_id,
                )
            )
            if rule is not None:
                rule.last_error = safe_error_message(exc)
                rule.last_error_code = str(getattr(exc, "code", None) or "scan_failed")
            if state is not None:
                state.last_error = safe_error_message(exc)
                state.last_error_code = str(getattr(exc, "code", None) or "scan_failed")
                state.last_scanned_at = utc_now()
            await session.commit()

    async def _accounts_for_rule(
        self, session: AsyncSession, rule: ParticipantReactionRule
    ) -> list[Account]:
        ids = self._account_ids(rule)
        if not ids:
            return []
        accounts = list(await session.scalars(select(Account).where(Account.id.in_(ids))))
        by_id = {int(account.id): account for account in accounts}
        return [
            by_id[item] for item in ids if item in by_id and self._is_valid_account(by_id[item])
        ]

    async def _eligible_accounts_for_target(
        self,
        session: AsyncSession,
        accounts: list[Account],
        target_id: int,
        *,
        exclude_account_id: int | None = None,
        require_send_capacity: bool = True,
    ) -> list[Account]:
        account_ids = [int(account.id) for account in accounts]
        if not account_ids:
            return []
        states = list(
            await session.scalars(
                select(TargetAccountState).where(
                    TargetAccountState.target_id == target_id,
                    TargetAccountState.account_id.in_(account_ids),
                )
            )
        )
        by_account = {int(state.account_id): state for state in states}
        eligible: list[Account] = []
        for account in accounts:
            account_id = int(account.id)
            if account_id == exclude_account_id or not ParticipantReactionService._is_valid_account(
                account
            ):
                continue
            resume_at = self._runner.account_resume_at(account)
            if require_send_capacity and resume_at is not None and resume_at > utc_now():
                continue
            state = by_account.get(account_id)
            if state is None:
                eligible.append(account)
                continue
            if state.send_blocked:
                continue
            if state.membership_checked_at is not None and (
                state.can_send is False
                or state.membership_status
                in {
                    MembershipStatus.BANNED,
                    MembershipStatus.NOT_MEMBER,
                    MembershipStatus.UNAVAILABLE,
                    MembershipStatus.JOIN_PENDING,
                }
            ):
                continue
            eligible.append(account)
        return eligible

    @staticmethod
    async def _account_is_eligible_for_target(
        session: AsyncSession,
        account_id: int,
        target_id: int,
    ) -> bool:
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        if state is None:
            return True
        if state.send_blocked:
            return False
        return not (
            state.membership_checked_at is not None
            and (
                state.can_send is False
                or state.membership_status
                in {
                    MembershipStatus.BANNED,
                    MembershipStatus.NOT_MEMBER,
                    MembershipStatus.UNAVAILABLE,
                    MembershipStatus.JOIN_PENDING,
                }
            )
        )

    async def _failover_reactor(
        self,
        task_id: int,
        token: str,
        failed_account_id: int,
        exc: BaseException | None,
    ) -> bool:
        async with self._session_factory() as session:
            task = await self._require_running(session, task_id, token)
            rule = await session.get(ParticipantReactionRule, task.rule_id)
            if (
                task.cancel_requested
                or task.attempt_count >= task.max_attempts
                or rule is None
                or not rule.enabled
            ):
                return False
            accounts = await self._accounts_for_rule(session, rule)
            eligible = await self._eligible_accounts_for_target(
                session,
                accounts,
                int(task.target_id),
                exclude_account_id=failed_account_id,
                require_send_capacity=False,
            )
            if not eligible:
                return False
            replacement = self._pick_reactor(
                rule,
                int(task.target_id),
                int(task.message_id),
                eligible,
            )
            task.reactor_account_id = int(replacement.id)
            task.status = ParticipantReactionTaskStatus.QUEUED
            now = utc_now()
            resume_at = self._runner.account_resume_at(replacement)
            task.scheduled_for = max(now, resume_at) if resume_at is not None else now
            task.error = safe_error_message(exc) if exc is not None else None
            task.error_code = (
                str(getattr(exc, "code", None) or "reactor_failover")
                if exc is not None
                else "reactor_failover"
            )
            self._release(task)
            add_audit_event(
                session,
                event_type="participant_reaction.reactor_failed_over",
                entity_type="participant_reaction_task",
                entity_id=task.id,
                details={
                    "failed_account_id": failed_account_id,
                    "replacement_account_id": int(replacement.id),
                    "target_id": int(task.target_id),
                },
                actor="participant-reaction-worker",
            )
            await session.commit()
            return True

    async def _targets_for_rule(
        self, session: AsyncSession, rule: ParticipantReactionRule
    ) -> list[Target]:
        return list(
            await session.scalars(
                select(Target)
                .join(TargetListItem, TargetListItem.target_id == Target.id)
                .where(
                    TargetListItem.target_list_id == rule.target_list_id,
                    TargetListItem.enabled.is_(True),
                    Target.quality_status != "bad",
                )
                .order_by(Target.id)
            )
        )

    async def _view(
        self, session: AsyncSession, rule: ParticipantReactionRule
    ) -> ParticipantReactionRuleView:
        return (await self._views_for_rules(session, [rule]))[0]

    async def _views_for_rules(
        self,
        session: AsyncSession,
        rules: list[ParticipantReactionRule],
    ) -> list[ParticipantReactionRuleView]:
        if not rules:
            return []
        rule_ids = [int(rule.id) for rule in rules]
        list_ids = list({int(rule.target_list_id) for rule in rules})
        status_result = await session.execute(
            select(
                ParticipantReactionTask.rule_id,
                ParticipantReactionTask.status,
                func.count(ParticipantReactionTask.id),
            )
            .where(ParticipantReactionTask.rule_id.in_(rule_ids))
            .group_by(ParticipantReactionTask.rule_id, ParticipantReactionTask.status)
        )
        status_counts: dict[int, dict[ParticipantReactionTaskStatus, int]] = {
            rule_id: {} for rule_id in rule_ids
        }
        for rule_id, status, count in status_result:
            status_counts[int(rule_id)][status] = int(count)
        list_names = dict(
            await session.execute(
                select(TargetList.id, TargetList.name).where(TargetList.id.in_(list_ids))
            )
        )
        target_result = await session.execute(
            select(TargetListItem.target_list_id, func.count(TargetListItem.id))
            .join(Target, Target.id == TargetListItem.target_id)
            .where(
                TargetListItem.target_list_id.in_(list_ids),
                TargetListItem.enabled.is_(True),
                Target.quality_status != "bad",
            )
            .group_by(TargetListItem.target_list_id)
        )
        target_counts = {int(list_id): int(count) for list_id, count in target_result}
        return [
            ParticipantReactionRuleView(
                rule=rule,
                target_list_name=list_names.get(int(rule.target_list_id), "удалённая база"),
                target_count=target_counts.get(int(rule.target_list_id), 0),
                queued=status_counts[int(rule.id)].get(ParticipantReactionTaskStatus.QUEUED, 0),
                running=status_counts[int(rule.id)].get(ParticipantReactionTaskStatus.RUNNING, 0),
                succeeded=status_counts[int(rule.id)].get(
                    ParticipantReactionTaskStatus.SUCCEEDED, 0
                ),
                failed=status_counts[int(rule.id)].get(ParticipantReactionTaskStatus.FAILED, 0),
                skipped=status_counts[int(rule.id)].get(ParticipantReactionTaskStatus.SKIPPED, 0),
            )
            for rule in rules
        ]

    @staticmethod
    async def _approved_target_count(session: AsyncSession, target_list_id: int) -> int:
        return int(
            await session.scalar(
                select(func.count())
                .select_from(TargetListItem)
                .join(Target, Target.id == TargetListItem.target_id)
                .where(
                    TargetListItem.target_list_id == target_list_id,
                    TargetListItem.enabled.is_(True),
                    Target.quality_status != "bad",
                )
            )
            or 0
        )

    @staticmethod
    def _validate_account(account: Account | None) -> None:
        if account is None or not ParticipantReactionService._is_valid_account(account):
            raise ConflictError("выбранный аккаунт не готов к работе")

    @staticmethod
    def _is_valid_account(account: Account) -> bool:
        return bool(
            account.status == AccountStatus.ACTIVE
            and account.authorized
            and account.ownership_confirmed
        )

    @staticmethod
    def _validate_sample(numerator: int, denominator: int) -> None:
        if not 1 <= numerator <= denominator <= 100:
            raise ConflictError("доля реакций должна быть от 1/100 до 100/100")

    @staticmethod
    def _normalize_account_ids(account_ids: list[int]) -> list[int]:
        return list(
            dict.fromkeys(
                int(value) for value in account_ids if isinstance(value, int) and value > 0
            )
        )

    @staticmethod
    def _account_ids(rule: ParticipantReactionRule) -> list[int]:
        try:
            raw = json.loads(rule.account_ids_json)
        except (TypeError, ValueError):
            return []
        return ParticipantReactionService._normalize_account_ids(
            [value for value in raw if isinstance(value, int)]
        )

    @staticmethod
    async def _target_is_currently_selected(
        session: AsyncSession, rule: ParticipantReactionRule, target_id: int
    ) -> bool:
        return bool(
            await session.scalar(
                select(TargetListItem.id).where(
                    TargetListItem.target_list_id == rule.target_list_id,
                    TargetListItem.target_id == target_id,
                    TargetListItem.enabled.is_(True),
                )
            )
        )

    @staticmethod
    async def _cancel_pending_tasks(
        session: AsyncSession,
        rule_id: int,
        reason: str,
        *,
        code: str = "rule_reconfigured",
    ) -> None:
        await session.execute(
            update(ParticipantReactionTask)
            .where(
                ParticipantReactionTask.rule_id == rule_id,
                ParticipantReactionTask.status == ParticipantReactionTaskStatus.QUEUED,
            )
            .values(
                status=ParticipantReactionTaskStatus.CANCELLED,
                error=reason,
                error_code=code,
                finished_at=utc_now(),
            )
        )
        await session.execute(
            update(ParticipantReactionTask)
            .where(
                ParticipantReactionTask.rule_id == rule_id,
                ParticipantReactionTask.status == ParticipantReactionTaskStatus.RUNNING,
            )
            .values(
                cancel_requested=True,
                error=reason,
                error_code="cancel_requested",
            )
        )

    @staticmethod
    def _digest(rule: ParticipantReactionRule, target_id: int, message_id: int) -> int:
        value = f"{rule.id}:{target_id}:{message_id}".encode()
        return int.from_bytes(hashlib.sha256(value).digest()[:8], "big")

    def _sample(self, rule: ParticipantReactionRule, target_id: int, message_id: int) -> bool:
        return (
            self._digest(rule, target_id, message_id) % rule.sample_denominator
            < rule.sample_numerator
        )

    def _pick_reactor(
        self,
        rule: ParticipantReactionRule,
        target_id: int,
        message_id: int,
        accounts: list[Account],
    ) -> Account:
        return accounts[self._digest(rule, target_id, message_id) % len(accounts)]

    def _delay(self, rule: ParticipantReactionRule, target_id: int, message_id: int) -> int:
        return 8 + self._digest(rule, target_id, message_id) % 23

    @staticmethod
    async def _require_running(
        session: AsyncSession, task_id: int, token: str
    ) -> ParticipantReactionTask:
        task = await session.get(ParticipantReactionTask, task_id)
        if (
            task is None
            or task.status != ParticipantReactionTaskStatus.RUNNING
            or task.lease_token != token
        ):
            raise RuntimeError("participant reaction task lease was lost")
        return task

    @staticmethod
    def _release(task: ParticipantReactionTask) -> None:
        task.lease_owner = None
        task.lease_token = None
        task.lease_until = None
