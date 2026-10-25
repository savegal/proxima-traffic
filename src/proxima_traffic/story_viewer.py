"""Story viewer service and background execution worker.

Watches active stories of subscribers and optionally sends reactions/likes,
driving profile visits and engagement with human-like randomized delays and account rotation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.blacklist_import import subscriber_blacklisted_condition
from proxima_traffic.config import Settings
from proxima_traffic.gateways import (
    MessageGateway,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    StoryViewerAccount,
    StoryViewerCampaign,
    StoryViewerCampaignStatus,
    StoryViewerTask,
    StoryViewerTaskStatus,
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.story_content import normalize_story_reactions
from proxima_traffic.story_runtime import (
    StoryWorker,
    lock_story_queue,
    story_accounts,
    validate_story_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StoryViewerDashboard:
    campaign_count: int
    active_campaign_count: int
    total_tasks: int
    succeeded_tasks: int
    skipped_tasks: int
    failed_tasks: int
    total_stories_viewed: int
    total_reactions_sent: int
    simulated: bool = False


@dataclass(frozen=True, slots=True)
class StoryViewerAccountView:
    account_id: int
    label: str | None
    phone: str
    views_today: int
    reactions_today: int
    last_view_at: datetime | None
    enabled: bool
    is_paused: bool
    cooldown_until: datetime | None


@dataclass(frozen=True, slots=True)
class StoryViewerCampaignView:
    id: int
    name: str
    subscriber_list_id: int
    subscriber_list_name: str
    send_reaction: bool
    reaction_emoji: str
    delay_min_seconds: int
    delay_max_seconds: int
    daily_limit_per_account: int
    max_targets: int
    status: StoryViewerCampaignStatus
    enabled: bool
    created_at: datetime
    total_tasks: int
    succeeded_tasks: int
    skipped_tasks: int
    failed_tasks: int
    stories_viewed: int
    reactions_sent: int
    accounts: list[StoryViewerAccountView] = field(default_factory=list)
    simulated: bool | None = True
    last_error: str | None = None
    next_run_at: datetime | None = None


class StoryViewerService:
    """Manages mass story viewing campaigns, target generation, and background worker."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._worker: StoryViewerWorker | None = None

    def start(self) -> None:
        """Start the background worker if not running."""
        if self._worker is None or self._worker.is_stopped():
            self._worker = StoryViewerWorker(
                session_factory=self._session_factory,
                gateway=self._gateway,
                settings=self._settings,
            )
            self._worker.start()

    def stop(self) -> None:
        """Stop the background worker."""
        if self._worker is not None:
            self._worker.stop()

    async def wait_stopped(self) -> None:
        if self._worker is not None:
            await self._worker.wait_stopped()

    async def _last_error(self, session, campaign_id):
        return await session.scalar(
            select(StoryViewerTask.error)
            .where(
                StoryViewerTask.campaign_id == campaign_id,
                StoryViewerTask.error.is_not(None),
            )
            .order_by(StoryViewerTask.id.desc())
            .limit(1)
        )

    async def _next_run_at(self, session, campaign_id):
        return await session.scalar(
            select(func.min(StoryViewerTask.scheduled_for)).where(
                StoryViewerTask.campaign_id == campaign_id,
                StoryViewerTask.status == self._queued_status(),
            )
        )

    @staticmethod
    def _queued_status():
        return StoryViewerTaskStatus.QUEUED

    async def get_dashboard(self) -> StoryViewerDashboard:
        """Calculate high-level dashboard metrics for story viewing."""
        async with self._session_factory() as session:
            campaign_count = (await session.scalar(select(func.count(StoryViewerCampaign.id)))) or 0
            active_count = (
                await session.scalar(
                    select(func.count(StoryViewerCampaign.id)).where(
                        StoryViewerCampaign.status == StoryViewerCampaignStatus.ACTIVE
                    )
                )
            ) or 0

            total_tasks = (await session.scalar(select(func.count(StoryViewerTask.id)))) or 0
            succeeded_tasks = (
                await session.scalar(
                    select(func.count(StoryViewerTask.id)).where(
                        StoryViewerTask.status == StoryViewerTaskStatus.SUCCEEDED
                    )
                )
            ) or 0
            skipped_tasks = (
                await session.scalar(
                    select(func.count(StoryViewerTask.id)).where(
                        StoryViewerTask.status == StoryViewerTaskStatus.SKIPPED
                    )
                )
            ) or 0
            failed_tasks = (
                await session.scalar(
                    select(func.count(StoryViewerTask.id)).where(
                        StoryViewerTask.status == StoryViewerTaskStatus.FAILED
                    )
                )
            ) or 0

            total_stories_viewed = (
                await session.scalar(
                    select(func.coalesce(func.sum(StoryViewerTask.stories_viewed), 0))
                )
            ) or 0

            total_reactions = (
                await session.scalar(
                    select(func.count(StoryViewerTask.id)).where(
                        StoryViewerTask.reaction_sent.is_(True)
                    )
                )
            ) or 0

            return StoryViewerDashboard(
                campaign_count=int(campaign_count),
                active_campaign_count=int(active_count),
                total_tasks=int(total_tasks),
                succeeded_tasks=int(succeeded_tasks),
                skipped_tasks=int(skipped_tasks),
                failed_tasks=int(failed_tasks),
                total_stories_viewed=int(total_stories_viewed),
                total_reactions_sent=int(total_reactions),
                simulated=getattr(self._settings, "dry_run", False),
            )

    async def list_campaigns(
        self, limit: int = 50, offset: int = 0
    ) -> list[StoryViewerCampaignView]:
        """Return a page of campaigns with live progress statistics."""
        views: list[StoryViewerCampaignView] = []
        async with self._session_factory() as session:
            query = (
                select(StoryViewerCampaign, SubscriberList.name.label("list_name"))
                .join(
                    SubscriberList,
                    SubscriberList.id == StoryViewerCampaign.subscriber_list_id,
                )
                .order_by(StoryViewerCampaign.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            rows = (await session.execute(query)).all()

            for campaign, list_name in rows:
                stats_query = select(
                    func.count(StoryViewerTask.id).label("total"),
                    func.count(StoryViewerTask.id)
                    .filter(StoryViewerTask.status == StoryViewerTaskStatus.SUCCEEDED)
                    .label("succeeded"),
                    func.count(StoryViewerTask.id)
                    .filter(StoryViewerTask.status == StoryViewerTaskStatus.SKIPPED)
                    .label("skipped"),
                    func.count(StoryViewerTask.id)
                    .filter(StoryViewerTask.status == StoryViewerTaskStatus.FAILED)
                    .label("failed"),
                    func.coalesce(func.sum(StoryViewerTask.stories_viewed), 0).label("views"),
                    func.count(StoryViewerTask.id)
                    .filter(StoryViewerTask.reaction_sent.is_(True))
                    .label("reactions"),
                ).where(StoryViewerTask.campaign_id == campaign.id)
                stats = (await session.execute(stats_query)).one()

                views.append(
                    StoryViewerCampaignView(
                        id=campaign.id,
                        name=campaign.name,
                        subscriber_list_id=campaign.subscriber_list_id,
                        subscriber_list_name=str(list_name),
                        send_reaction=campaign.send_reaction,
                        reaction_emoji=campaign.reaction_emoji,
                        delay_min_seconds=campaign.delay_min_seconds,
                        delay_max_seconds=campaign.delay_max_seconds,
                        daily_limit_per_account=campaign.daily_limit_per_account,
                        max_targets=campaign.max_targets,
                        status=campaign.status,
                        enabled=campaign.enabled,
                        created_at=campaign.created_at,
                        simulated=campaign.simulated,
                        last_error=await self._last_error(session, campaign.id),
                        next_run_at=await self._next_run_at(session, campaign.id),
                        total_tasks=int(stats.total or 0),
                        succeeded_tasks=int(stats.succeeded or 0),
                        skipped_tasks=int(stats.skipped or 0),
                        failed_tasks=int(stats.failed or 0),
                        stories_viewed=int(stats.views or 0),
                        reactions_sent=int(stats.reactions or 0),
                    )
                )

        return views

    async def get_campaign(self, campaign_id: int) -> StoryViewerCampaignView | None:
        """Fetch one campaign view by ID with account status breakdown."""
        async with self._session_factory() as session:
            query = (
                select(StoryViewerCampaign, SubscriberList.name.label("list_name"))
                .join(
                    SubscriberList,
                    SubscriberList.id == StoryViewerCampaign.subscriber_list_id,
                )
                .where(StoryViewerCampaign.id == campaign_id)
            )
            row = (await session.execute(query)).first()
            if row is None:
                return None
            campaign, list_name = row

            stats_query = select(
                func.count(StoryViewerTask.id).label("total"),
                func.count(StoryViewerTask.id)
                .filter(StoryViewerTask.status == StoryViewerTaskStatus.SUCCEEDED)
                .label("succeeded"),
                func.count(StoryViewerTask.id)
                .filter(StoryViewerTask.status == StoryViewerTaskStatus.SKIPPED)
                .label("skipped"),
                func.count(StoryViewerTask.id)
                .filter(StoryViewerTask.status == StoryViewerTaskStatus.FAILED)
                .label("failed"),
                func.coalesce(func.sum(StoryViewerTask.stories_viewed), 0).label("views"),
                func.count(StoryViewerTask.id)
                .filter(StoryViewerTask.reaction_sent.is_(True))
                .label("reactions"),
            ).where(StoryViewerTask.campaign_id == campaign.id)
            stats = (await session.execute(stats_query)).one()

            # Load account details
            acc_query = (
                select(StoryViewerAccount, Account)
                .join(Account, Account.id == StoryViewerAccount.account_id)
                .where(StoryViewerAccount.campaign_id == campaign_id)
                .order_by(Account.id.asc())
            )
            acc_rows = (await session.execute(acc_query)).all()
            account_views = [
                StoryViewerAccountView(
                    account_id=acc.id,
                    label=acc.label,
                    phone=acc.phone,
                    views_today=s_acc.views_today,
                    reactions_today=s_acc.reactions_today,
                    last_view_at=ensure_utc(s_acc.last_view_at),
                    enabled=s_acc.enabled,
                    is_paused=bool(acc.operator_paused or acc.status != AccountStatus.ACTIVE),
                    cooldown_until=ensure_utc(acc.cooldown_until),
                )
                for s_acc, acc in acc_rows
            ]

            return StoryViewerCampaignView(
                id=campaign.id,
                name=campaign.name,
                subscriber_list_id=campaign.subscriber_list_id,
                subscriber_list_name=str(list_name),
                send_reaction=campaign.send_reaction,
                reaction_emoji=campaign.reaction_emoji,
                delay_min_seconds=campaign.delay_min_seconds,
                delay_max_seconds=campaign.delay_max_seconds,
                daily_limit_per_account=campaign.daily_limit_per_account,
                max_targets=campaign.max_targets,
                status=campaign.status,
                enabled=campaign.enabled,
                created_at=campaign.created_at,
                simulated=campaign.simulated,
                last_error=await self._last_error(session, campaign.id),
                next_run_at=await self._next_run_at(session, campaign.id),
                total_tasks=int(stats.total or 0),
                succeeded_tasks=int(stats.succeeded or 0),
                skipped_tasks=int(stats.skipped or 0),
                failed_tasks=int(stats.failed or 0),
                stories_viewed=int(stats.views or 0),
                reactions_sent=int(stats.reactions or 0),
                accounts=account_views,
            )

    async def list_available_accounts(self) -> list[Account]:
        async with self._session_factory() as session:
            return list(
                (
                    await session.scalars(
                        select(Account)
                        .where(
                            Account.status == AccountStatus.ACTIVE,
                            Account.operator_paused.is_(False),
                        )
                        .order_by(Account.id)
                    )
                ).all()
            )

    async def list_accounts_for_campaign(self, campaign_id: int) -> list[StoryViewerAccountView]:
        """Fetch all accounts configured for a campaign."""
        async with self._session_factory() as session:
            acc_query = (
                select(StoryViewerAccount, Account)
                .join(Account, Account.id == StoryViewerAccount.account_id)
                .where(StoryViewerAccount.campaign_id == campaign_id)
                .order_by(Account.id.asc())
            )
            rows = (await session.execute(acc_query)).all()
            return [
                StoryViewerAccountView(
                    account_id=acc.id,
                    label=acc.label,
                    phone=acc.phone,
                    views_today=s_acc.views_today,
                    reactions_today=s_acc.reactions_today,
                    last_view_at=ensure_utc(s_acc.last_view_at),
                    enabled=s_acc.enabled,
                    is_paused=bool(acc.operator_paused or acc.status != AccountStatus.ACTIVE),
                    cooldown_until=ensure_utc(acc.cooldown_until),
                )
                for s_acc, acc in rows
            ]

    async def toggle_campaign_account(self, campaign_id: int, account_id: int) -> bool:
        """Enable or disable one sender account within an active campaign."""
        async with self._session_factory() as session:
            sa = await session.scalar(
                select(StoryViewerAccount).where(
                    StoryViewerAccount.campaign_id == campaign_id,
                    StoryViewerAccount.account_id == account_id,
                )
            )
            if sa is not None:
                sa.enabled = not sa.enabled
                if sa.enabled:
                    await session.execute(
                        update(StoryViewerTask)
                        .where(
                            StoryViewerTask.campaign_id == campaign_id,
                            StoryViewerTask.status == StoryViewerTaskStatus.QUEUED,
                        )
                        .values(scheduled_for=utc_now())
                    )
                await session.commit()
                return sa.enabled
            return False

    async def create_campaign(
        self,
        *,
        name: str,
        subscriber_list_id: int,
        send_reaction: bool = True,
        reaction_emoji: str = "🔥",
        delay_min_seconds: int = 5,
        delay_max_seconds: int = 15,
        daily_limit_per_account: int = 100,
        max_targets: int = 500,
        account_ids: list[int] | None = None,
        auto_start: bool = True,
    ) -> StoryViewerCampaignView:
        """Create a story viewing campaign and its durable recipient tasks."""
        if delay_min_seconds < 0 or delay_max_seconds < delay_min_seconds:
            raise ValueError("Задержка delay_min_seconds должна быть >= 0 и <= delay_max_seconds")
        if not (1 <= max_targets <= 1_000_000):
            raise ValueError("max_targets must be between 1 and 1,000,000")
        if daily_limit_per_account < 1:
            raise ValueError("daily_limit_per_account must be at least 1")

        if auto_start and not self._settings.worker_enabled:
            raise ValueError("Фоновые отправки выключены. Включите worker перед запуском кампании.")
        name = validate_story_name(name)
        if not 1 <= daily_limit_per_account <= 100000 or delay_max_seconds > 86400:
            raise ValueError("Лимит должен быть 1–100000, задержка — до 86400 секунд")
        reaction_emoji = normalize_story_reactions(reaction_emoji)
        async with self._session_factory() as session:
            await lock_story_queue(session)
            # Check duplicate name
            existing = await session.scalar(
                select(StoryViewerCampaign.id).where(StoryViewerCampaign.name == name)
            )
            if existing is not None:
                raise ValueError(f"Кампания с названием '{name}' уже существует")

            # Check subscriber list
            sub_list = await session.scalar(
                select(SubscriberList).where(SubscriberList.id == subscriber_list_id)
            )
            if sub_list is None or sub_list.status != SubscriberListStatus.READY:
                raise ValueError("Выбранная база подписчиков не найдена или ещё не готова")

            # Create campaign
            campaign = StoryViewerCampaign(
                name=name,
                subscriber_list_id=subscriber_list_id,
                send_reaction=send_reaction,
                reaction_emoji=reaction_emoji or "🔥",
                delay_min_seconds=delay_min_seconds,
                delay_max_seconds=delay_max_seconds,
                daily_limit_per_account=daily_limit_per_account,
                max_targets=max_targets,
                status=StoryViewerCampaignStatus.ACTIVE,
                enabled=True,
                simulated=self._settings.dry_run,
            )
            session.add(campaign)
            await session.flush()

            accounts = await story_accounts(session, account_ids)

            for acc in accounts:
                session.add(
                    StoryViewerAccount(
                        campaign_id=campaign.id,
                        account_id=acc.id,
                        views_today=0,
                        reactions_today=0,
                        enabled=True,
                    )
                )

            # Query subscribers from the selected list
            # Filter out blacklisted users AND subscribers already viewed in this campaign
            sub_query = (
                select(Subscriber.id)
                .join(
                    SubscriberListMember,
                    SubscriberListMember.subscriber_id == Subscriber.id,
                )
                .where(
                    SubscriberListMember.subscriber_list_id == subscriber_list_id,
                    Subscriber.username.is_not(None),
                    Subscriber.username != "",
                    Subscriber.do_not_contact.is_(False),
                    ~subscriber_blacklisted_condition(),
                )
                .order_by(Subscriber.id.asc())
                .limit(max_targets)
            )
            subscriber_ids = (await session.execute(sub_query)).scalars().all()

            if not subscriber_ids:
                raise ValueError(
                    "В выбранной базе подписчиков нет доступных пользователей для проверки историй"
                )

            # Insert tasks with initial staggering
            now = utc_now()
            for idx, s_id in enumerate(subscriber_ids):
                scheduled_for = (
                    now + timedelta(milliseconds=idx * 200) if delay_min_seconds > 0 else now
                )
                session.add(
                    StoryViewerTask(
                        campaign_id=campaign.id,
                        subscriber_id=s_id,
                        status=StoryViewerTaskStatus.QUEUED,
                        scheduled_for=scheduled_for,
                    )
                )

            await session.commit()

        if auto_start:
            self.start()

        view = await self.get_campaign(campaign.id)
        if view is None:
            raise RuntimeError("Не удалось загрузить созданную кампанию")
        return view

    async def pause_campaign(self, campaign_id: int) -> bool:
        """Pause an active campaign."""
        async with self._session_factory() as session:
            res = await session.execute(
                update(StoryViewerCampaign)
                .where(
                    StoryViewerCampaign.id == campaign_id,
                    StoryViewerCampaign.status == StoryViewerCampaignStatus.ACTIVE,
                )
                .values(status=StoryViewerCampaignStatus.PAUSED)
            )
            await session.commit()
            return bool(res.rowcount > 0)

    async def resume_campaign(self, campaign_id: int, auto_start: bool = True) -> bool:
        """Resume a paused campaign."""
        if auto_start and not self._settings.worker_enabled:
            return False
        async with self._session_factory() as session:
            res = await session.execute(
                update(StoryViewerCampaign)
                .where(
                    StoryViewerCampaign.id == campaign_id,
                    StoryViewerCampaign.simulated == self._settings.dry_run,
                    StoryViewerCampaign.status == StoryViewerCampaignStatus.PAUSED,
                )
                .values(status=StoryViewerCampaignStatus.ACTIVE)
            )
            if res.rowcount > 0:
                await session.execute(
                    update(StoryViewerTask)
                    .where(
                        StoryViewerTask.campaign_id == campaign_id,
                        StoryViewerTask.status == self._queued_status(),
                    )
                    .values(scheduled_for=utc_now())
                )
            await session.commit()
            if res.rowcount > 0:
                if auto_start:
                    self.start()
                return True
            return False

    async def delete_campaign(self, campaign_id: int) -> bool:
        """Delete a campaign and cascade all associated tasks."""
        async with self._session_factory() as session:
            await lock_story_queue(session)
            if await session.scalar(
                select(StoryViewerTask.id)
                .where(
                    StoryViewerTask.campaign_id == campaign_id,
                    StoryViewerTask.status == StoryViewerTaskStatus.RUNNING,
                )
                .limit(1)
            ):
                raise ValueError(
                    "Сначала приостановите кампанию и дождитесь окончания текущего просмотра"
                )
            campaign = await session.scalar(
                select(StoryViewerCampaign).where(StoryViewerCampaign.id == campaign_id)
            )
            if campaign is None:
                return False
            await session.delete(campaign)
            await session.commit()
            return True


class StoryViewerWorker(StoryWorker):
    """Durable concurrent queue for story viewing and reactions."""

    def __init__(self, session_factory, gateway, settings, concurrency=4):
        super().__init__(session_factory, gateway, settings, viewing=True, concurrency=concurrency)
