"""Durable execution shared by story publication and viewing queues."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert

from proxima_traffic.account_state import session_fingerprint
from proxima_traffic.blacklist_import import subscriber_blacklisted_condition
from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.gateways import (
    DirectRecipient,
    GatewayError,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    StoryCampaignStatus,
    StoryTaggingAccount,
    StoryTaggingCampaign,
    StoryTaggingMentionState,
    StoryTaggingTask,
    StoryTaskStatus,
    StoryViewerAccount,
    StoryViewerCampaign,
    StoryViewerCampaignStatus,
    StoryViewerRecord,
    StoryViewerTask,
    StoryViewerTaskStatus,
    Subscriber,
    SubscriberListMember,
    ensure_utc,
    utc_now,
)

logger = logging.getLogger(__name__)
CONTENTION_CODES = frozenset({"account_busy", "account_operation_busy", "account_pacing"})
ACCOUNT_CODES = frozenset(
    {
        "account_dead",
        "account_unauthorized",
        "account_unresolvable",
        "account_identity_mismatch",
        "account_restricted",
        "session_unavailable",
        "premium_required",
    }
)
WAIT_CODES = frozenset({"flood_wait", "story_daily_limit_reached", "account_restricted"})


def release_story_task(task) -> None:
    task.lease_owner = task.lease_token = task.lease_until = None


async def lock_story_queue(session) -> None:
    # SQLite is the project's supported store. Lock BEFORE selecting candidates:
    # ORM assignment followed by commit is not an atomic lease acquisition.
    await session.execute(text("BEGIN IMMEDIATE"))


async def story_accounts(session, account_ids):
    query = select(Account).where(
        Account.status == AccountStatus.ACTIVE, Account.operator_paused.is_(False)
    )
    if account_ids is not None:
        if not account_ids:
            raise ValueError("Выберите хотя бы один активный аккаунт")
        query = query.where(Account.id.in_(set(account_ids)))
    accounts = list((await session.scalars(query.order_by(Account.id))).all())
    if not accounts or (account_ids is not None and len(accounts) != len(set(account_ids))):
        raise ValueError("Выбранные аккаунты недоступны или приостановлены")
    return accounts


def validate_story_name(name: str) -> str:
    name = name.strip()
    if not 2 <= len(name) <= 100 or any(ord(c) < 32 for c in name):
        raise ValueError("Название должно содержать 2–100 символов без переводов строк")
    return name


class StoryWorker:
    """Short serialized claims, renewable leases and fenced outcome writes."""

    def __init__(self, session_factory, gateway, settings, *, viewing=False, concurrency=1):
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._viewing = viewing
        self._concurrency = max(1, min(8, int(concurrency)))
        self._worker_id = f"stories-{uuid4().hex[:12]}"
        self._stop_event = asyncio.Event()
        self._tasks = []
        self.Task = StoryViewerTask if viewing else StoryTaggingTask
        self.Campaign = StoryViewerCampaign if viewing else StoryTaggingCampaign
        self.Link = StoryViewerAccount if viewing else StoryTaggingAccount
        self.Status = StoryViewerTaskStatus if viewing else StoryTaskStatus
        self.CampaignStatus = StoryViewerCampaignStatus if viewing else StoryCampaignStatus

    def start(self):
        if not self._settings.worker_enabled:
            return
        if self.is_stopped():
            self._stop_event.clear()
            self._tasks = [asyncio.create_task(self._run_loop()) for _ in range(self._concurrency)]

    def stop(self):
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()

    async def wait_stopped(self):
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def is_stopped(self):
        return not self._tasks or all(task.done() for task in self._tasks)

    async def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                if not await self._process_next_task():
                    await asyncio.sleep(3)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Story queue iteration failed")
                await asyncio.sleep(3)

    async def _complete(self, session):
        pending = (
            select(self.Task.id)
            .where(
                self.Task.campaign_id == self.Campaign.id,
                self.Task.status.in_([self.Status.QUEUED, self.Status.RUNNING]),
            )
            .exists()
        )
        await session.execute(
            update(self.Campaign)
            .where(
                self.Campaign.status == self.CampaignStatus.ACTIVE,
                ~pending,
            )
            .values(status=self.CampaignStatus.COMPLETED)
        )

    async def _claim(self):
        if not self._settings.worker_enabled:
            return None
        now = utc_now()
        async with self._session_factory() as session:
            await lock_story_queue(session)
            # An expired execution may already have reached Telegram. Never
            # automatically publish or react again after losing that evidence.
            await session.execute(
                update(self.Task)
                .where(
                    self.Task.status == self.Status.RUNNING,
                    or_(self.Task.lease_until.is_(None), self.Task.lease_until <= now),
                )
                .values(
                    status=self.Status.FAILED,
                    error_code="delivery_unconfirmed",
                    error="Процесс прерван; результат Telegram неизвестен. Автоповтор отключён.",
                    finished_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
            )
            await self._complete(session)
            candidates = (
                await session.execute(
                    select(self.Task, self.Campaign)
                    .join(
                        self.Campaign,
                        self.Campaign.id == self.Task.campaign_id,
                    )
                    .where(
                        self.Task.status == self.Status.QUEUED,
                        self.Task.scheduled_for <= now,
                        self.Campaign.status == self.CampaignStatus.ACTIVE,
                        self.Campaign.enabled.is_(True),
                    )
                    .order_by(self.Task.scheduled_for, self.Task.id)
                    .limit(32)
                )
            ).all()
            deferred = set()
            for task, campaign in candidates:
                if campaign.id in deferred:
                    continue
                if campaign.simulated != self._settings.dry_run:
                    campaign.status = self.CampaignStatus.PAUSED
                    task.error_code = "execution_mode_changed"
                    task.error = "Режим работы изменён; создайте кампанию в текущем режиме"
                    deferred.add(campaign.id)
                    continue
                links = (
                    await session.execute(
                        select(self.Link, Account)
                        .join(
                            Account,
                            Account.id == self.Link.account_id,
                        )
                        .where(
                            self.Link.campaign_id == campaign.id,
                            self.Link.enabled.is_(True),
                            Account.status == AccountStatus.ACTIVE,
                            Account.operator_paused.is_(False),
                        )
                        .order_by(self.Link.id)
                    )
                ).all()
                if not links:
                    campaign.status = self.CampaignStatus.PAUSED
                    task.error_code = "no_available_accounts"
                    task.error = "Нет активных аккаунтов. Включите аккаунт и возобновите кампанию."
                    deferred.add(campaign.id)
                    continue
                eligible = []
                ready_times = []
                for link, account in links:
                    last = ensure_utc(link.last_view_at if self._viewing else link.last_story_at)
                    if last and last.date() < now.date():
                        if self._viewing:
                            link.views_today = link.reactions_today = 0
                        else:
                            link.stories_today = 0
                    count = link.views_today if self._viewing else link.stories_today
                    # Limits are per sender across campaigns, including earlier
                    # campaigns that the operator has paused.
                    counter = self.Link.views_today if self._viewing else self.Link.stories_today
                    last_column = (
                        self.Link.last_view_at if self._viewing else self.Link.last_story_at
                    )
                    daily = await session.scalar(
                        select(func.coalesce(func.sum(counter), 0))
                        .join(
                            self.Campaign,
                            self.Campaign.id == self.Link.campaign_id,
                        )
                        .where(
                            self.Link.account_id == account.id,
                            self.Campaign.simulated == campaign.simulated,
                            last_column >= now.replace(hour=0, minute=0, second=0, microsecond=0),
                        )
                    )
                    ready_at = now
                    if max(count, daily or 0) >= campaign.daily_limit_per_account:
                        ready_at = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        ready_at += timedelta(days=1)
                    cooldown = ensure_utc(account.cooldown_until)
                    next_action = ensure_utc(
                        await session.scalar(
                            select(
                                func.max(self.Link.next_action_at),
                            )
                            .join(self.Campaign, self.Campaign.id == self.Link.campaign_id)
                            .where(
                                self.Link.account_id == account.id,
                                self.Campaign.simulated == campaign.simulated,
                            )
                        )
                    )
                    ready_at = max(ready_at, cooldown or now, next_action or now)
                    ready_times.append(ready_at)
                    if ready_at > now:
                        continue
                    busy = False
                    for table, status in (
                        (StoryTaggingTask, StoryTaskStatus),
                        (StoryViewerTask, StoryViewerTaskStatus),
                    ):
                        if await session.scalar(
                            select(table.id)
                            .where(
                                table.account_id == account.id,
                                table.status == status.RUNNING,
                            )
                            .limit(1)
                        ):
                            busy = True
                            break
                    if not busy:
                        eligible.append((count, link.id, link, account, int(daily or 0)))
                if not eligible:
                    next_check = max(min(ready_times), now + timedelta(seconds=10))
                    # A long Telegram wait belongs to the sender/campaign, not
                    # thousands of repeated ten-second recipient retries.
                    await session.execute(
                        update(self.Task)
                        .where(
                            self.Task.campaign_id == campaign.id,
                            self.Task.status == self.Status.QUEUED,
                            self.Task.scheduled_for <= now,
                        )
                        .values(scheduled_for=next_check)
                        .execution_options(synchronize_session=False)
                    )
                    deferred.add(campaign.id)
                    continue
                _, _, link, account, daily = min(eligible, key=lambda row: row[:2])
                task.status = self.Status.RUNNING
                task.lease_owner = self._worker_id
                task.lease_token = uuid4().hex
                task.lease_until = now + timedelta(seconds=240)
                task.account_id = account.id
                task.started_at = now
                task.finished_at = None
                task.attempt_count += 1
                await session.commit()
                return task, campaign, link, account, daily
            await session.commit()
        return None

    async def _heartbeat(self, task_id, token):
        while True:
            await asyncio.sleep(30)
            async with self._session_factory() as session:
                result = await session.execute(
                    update(self.Task)
                    .where(
                        self.Task.id == task_id,
                        self.Task.lease_token == token,
                        self.Task.status == self.Status.RUNNING,
                    )
                    .values(lease_until=utc_now() + timedelta(seconds=240))
                )
                await session.commit()
                if not result.rowcount:
                    return

    async def _audience(self, session, campaign, ids):
        return list(
            (
                await session.scalars(
                    select(Subscriber)
                    .join(
                        SubscriberListMember,
                        SubscriberListMember.subscriber_id == Subscriber.id,
                    )
                    .where(
                        Subscriber.id.in_(ids),
                        SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
                        Subscriber.do_not_contact.is_(False),
                        ~subscriber_blacklisted_condition(),
                    )
                    .order_by(Subscriber.id)
                )
            ).all()
        )

    async def _execute(self, task, campaign, link, account, daily):
        if not self._settings.worker_enabled:
            raise TemporaryGatewayError("Фоновые отправки выключены", code="dispatch_cancelled")
        async with self._session_factory() as session:
            ids = (
                [task.subscriber_id]
                if self._viewing
                else json.loads(task.tagged_subscriber_ids_json)
            )
            subscribers = await self._audience(session, campaign, ids)
            # Recheck pause/delete/account toggles immediately before dispatch.
            current = await session.get(self.Campaign, campaign.id)
            current_task = await session.get(self.Task, task.id)
            sender = await session.get(Account, account.id)
            assignment = await session.get(self.Link, link.id)
            if (
                not current_task
                or current_task.status != self.Status.RUNNING
                or current_task.lease_token != task.lease_token
                or ensure_utc(current_task.lease_until) is None
                or ensure_utc(current_task.lease_until) <= utc_now()
                or not current
                or current.status != self.CampaignStatus.ACTIVE
                or not current.enabled
                or not assignment
                or not assignment.enabled
                or not sender
                or sender.status != AccountStatus.ACTIVE
                or sender.operator_paused
            ):
                raise TemporaryGatewayError("Запуск отменён до отправки", code="dispatch_cancelled")
            cooldown = ensure_utc(sender.cooldown_until)
            if cooldown and cooldown > utc_now():
                raise TemporaryGatewayError(
                    "Аккаунт на паузе",
                    code="account_pacing",
                    retry_after=max(1, int((cooldown - utc_now()).total_seconds())),
                )
            if not subscribers:
                raise PermanentGatewayError(
                    "Получатель исключён из базы или стоп-листом", code="recipient_excluded"
                )
            if self._viewing:
                sub = subscribers[0]
                excluded = tuple(
                    (
                        await session.scalars(
                            select(StoryViewerRecord.story_id).where(
                                StoryViewerRecord.account_id == account.id,
                                StoryViewerRecord.subscriber_id == sub.id,
                            )
                        )
                    ).all()
                )
                recipient = DirectRecipient(
                    telegram_user_id=sub.telegram_user_id, username=sub.username
                )
            else:
                subscribers = [s for s in subscribers if s.username]
                if not subscribers:
                    raise PermanentGatewayError(
                        "Нет пользователей с username", code="recipient_excluded"
                    )

        async def before_send():
            await self._before_dispatch(task, campaign, link, account, [s.id for s in subscribers])

        if self._viewing:
            return await self._gateway.view_peer_stories(
                account=account,
                recipient=recipient,
                send_reaction=campaign.send_reaction,
                reaction_emoji=campaign.reaction_emoji,
                excluded_story_ids=excluded,
                max_stories=max(1, campaign.daily_limit_per_account - daily),
                before_send=before_send,
            ), subscribers
        return await self._gateway.publish_story(
            account=account,
            media_path=campaign.media_path,
            media_kind=campaign.media_kind,
            caption=campaign.caption,
            tagged_subscribers=subscribers,
            idempotency_key=f"story:{campaign.created_at.isoformat()}:{task.id}",
            before_send=before_send,
        ), subscribers

    async def _before_dispatch(self, claimed, campaign, link, account, subscriber_ids):
        """Recheck ownership and operator changes after uploads/resolution, before each RPC."""
        async with self._session_factory() as session:
            await lock_story_queue(session)
            task = await session.get(self.Task, claimed.id)
            current = await session.get(self.Campaign, campaign.id)
            assignment = await session.get(self.Link, link.id)
            sender = await session.get(Account, account.id)
            now = utc_now()
            if (
                not task
                or task.status != self.Status.RUNNING
                or task.lease_token != claimed.lease_token
                or task.account_id != account.id
                or ensure_utc(task.lease_until) is None
                or ensure_utc(task.lease_until) <= now
                or not current
                or current.status != self.CampaignStatus.ACTIVE
                or not current.enabled
                or not assignment
                or not assignment.enabled
                or not sender
                or sender.status != AccountStatus.ACTIVE
                or sender.operator_paused
                or session_fingerprint(sender) != session_fingerprint(account)
            ):
                raise TemporaryGatewayError("Запуск отменён до отправки", code="dispatch_cancelled")
            cooldown = ensure_utc(sender.cooldown_until)
            if cooldown and cooldown > now:
                raise TemporaryGatewayError(
                    "Аккаунт на паузе",
                    code="account_pacing",
                    retry_after=max(1, int((cooldown - now).total_seconds())),
                )
            audience = await self._audience(session, campaign, subscriber_ids)
            if {s.id for s in audience} != set(subscriber_ids):
                raise PermanentGatewayError(
                    "Получатель исключён перед отправкой", code="recipient_excluded"
                )

    async def _process_next_task(self):
        claimed = await self._claim()
        if claimed is None:
            return False
        task, campaign, link, account, daily = claimed
        heartbeat = asyncio.create_task(self._heartbeat(task.id, task.lease_token))
        result = None
        subscribers = []
        error = None
        cancelled = False
        try:
            async with asyncio.timeout(180):
                result, subscribers = await self._execute(task, campaign, link, account, daily)
        except asyncio.CancelledError:
            cancelled = True
            error = PermanentGatewayError(
                "Выполнение прервано; результат неизвестен", code="delivery_unconfirmed"
            )
        except GatewayError as exc:
            error = (
                PermanentGatewayError(
                    "Право выполнения потеряно; результат Telegram неизвестен",
                    code="delivery_unconfirmed",
                )
                if exc.code == "account_operation_lease_lost"
                else exc
            )
        except Exception:
            logger.exception("Story task %s execution failed", task.id)
            error = PermanentGatewayError(
                "Не удалось подтвердить результат выполнения", code="delivery_unconfirmed"
            )
        try:
            # Retry only the DB write, never the Telegram operation.
            for attempt in range(4):
                try:
                    await self._finish(task, campaign, link, account, result, subscribers, error)
                    break
                except Exception as exc:
                    if not is_transient_sqlite_lock(exc) or attempt == 3:
                        raise
                    await asyncio.sleep(0.1 * (attempt + 1))
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if cancelled:
            raise asyncio.CancelledError
        return True

    async def _finish(self, claimed, campaign, link, account, result, subscribers, error):
        now = utc_now()
        async with self._session_factory() as session:
            await lock_story_queue(session)
            task = await session.get(self.Task, claimed.id)
            if (
                not task
                or task.lease_token != claimed.lease_token
                or task.status != self.Status.RUNNING
            ):
                return
            assignment = await session.get(self.Link, link.id)
            sender = await session.get(Account, account.id)
            if result is not None:
                if result.simulated != campaign.simulated:
                    error = PermanentGatewayError(
                        "Режим результата не совпадает с кампанией", code="execution_mode_changed"
                    )
                elif self._viewing:
                    task.stories_found = result.stories_found
                    task.stories_viewed = result.stories_viewed
                    task.reaction_sent = result.reaction_sent
                    task.reaction_emoji = result.reaction_emoji
                    task.status = (
                        self.Status.SUCCEEDED if result.stories_viewed else self.Status.SKIPPED
                    )
                    error = result.error
                    if assignment:
                        assignment.last_view_at = now
                        assignment.views_today += result.stories_viewed
                        assignment.reactions_today += int(result.reaction_sent)
                    for story_id in result.viewed_story_ids:
                        await session.execute(
                            insert(StoryViewerRecord)
                            .values(
                                campaign_id=campaign.id,
                                account_id=account.id,
                                subscriber_id=claimed.subscriber_id,
                                story_id=story_id,
                                reacted=result.reaction_sent
                                and story_id == result.reacted_story_id,
                                emoji=result.reaction_emoji
                                if story_id == result.reacted_story_id
                                else None,
                                viewed_at=now,
                            )
                            .on_conflict_do_nothing()
                        )
                    if error:
                        task.status = self.Status.FAILED
                elif result.story_id > 0 and (
                    (
                        result.tagged_subscriber_ids is None
                        and result.tagged_count == len(subscribers)
                    )
                    or (
                        result.tagged_subscriber_ids is not None
                        and result.tagged_count == len(set(result.tagged_subscriber_ids))
                        and result.tagged_count > 0
                        and set(result.tagged_subscriber_ids) <= {s.id for s in subscribers}
                    )
                ):
                    task.status = self.Status.SUCCEEDED
                    task.story_id, task.story_url = result.story_id, result.story_url
                    if assignment:
                        assignment.last_story_at = now
                        assignment.stories_today += 1
                    for sub in subscribers:
                        if (
                            result.tagged_subscriber_ids is not None
                            and sub.id not in result.tagged_subscriber_ids
                        ):
                            continue
                        await session.execute(
                            insert(StoryTaggingMentionState)
                            .values(
                                campaign_id=campaign.id,
                                subscriber_id=sub.id,
                                story_id=result.story_id,
                                tagged_at=now,
                            )
                            .on_conflict_do_nothing()
                        )
                else:
                    error = PermanentGatewayError(
                        "Telegram не подтвердил историю и все отметки", code="delivery_unconfirmed"
                    )
            if assignment:
                delay = (
                    random.uniform(campaign.delay_min_seconds, campaign.delay_max_seconds)
                    if (self._viewing)
                    else campaign.cooldown_seconds
                )
                assignment.next_action_at = now + timedelta(seconds=delay)
            if error:
                task.error_code = error.code
                task.error = error.safe_message[:1000]
                # Partial success is persisted, and must never be retried on a
                # different account. The failure remains visible to the operator.
                partial = result is not None
                if sender and error.code in WAIT_CODES:
                    until = now + timedelta(seconds=max(1, error.retry_after or 500))
                    if not sender.cooldown_until or ensure_utc(sender.cooldown_until) < until:
                        sender.cooldown_until = until
                        sender.cooldown_reason = error.code
                if sender and error.code in ACCOUNT_CODES:
                    if error.code == "premium_required":
                        if assignment:
                            assignment.enabled = False
                    elif error.code != "account_restricted" or not error.retryable:
                        sender.status = AccountStatus.PAUSED
                        if error.code in {"account_dead", "account_unauthorized"}:
                            sender.authorized = False
                        sender.cooldown_reason = error.code
                        sender.last_error = error.safe_message[:1000]
                rotate = error.code in ACCOUNT_CODES or error.code in WAIT_CODES
                if rotate and not partial:
                    task.attempt_count = max(0, task.attempt_count - 1)
                retry = (
                    not partial
                    and (error.retryable or rotate)
                    and error.code != "delivery_unconfirmed"
                )
                if error.code == "dispatch_cancelled":
                    task.attempt_count = max(0, task.attempt_count - 1)
                elif error.code in CONTENTION_CODES:
                    task.attempt_count = max(0, task.attempt_count - 1)
                elif retry and task.attempt_count >= task.max_attempts and not rotate:
                    retry = False
                if retry:
                    task.status = self.Status.QUEUED
                    task.scheduled_for = now + timedelta(
                        seconds=(0 if rotate else max(1, error.retry_after or 15))
                    )
                else:
                    task.status = (
                        self.Status.SKIPPED
                        if error.code
                        in {
                            "recipient_excluded",
                            "privacy_restricted",
                            "recipient_unavailable",
                            "recipient_unresolvable",
                        }
                        else self.Status.FAILED
                    )
            else:
                task.error = task.error_code = None
                if (
                    not self._viewing
                    and result is not None
                    and result.tagged_count < len(subscribers)
                ):
                    task.error_code = "recipients_excluded"
                    missing = len(subscribers) - result.tagged_count
                    task.error = f"Не отмечено недоступных получателей: {missing}"
            if task.status != self.Status.QUEUED:
                task.finished_at = now
            release_story_task(task)
            await session.flush()
            await self._complete(session)
            await session.commit()
