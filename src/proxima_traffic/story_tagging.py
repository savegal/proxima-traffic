"""Story tagging service and background execution worker.

Publishes public Telegram stories with mentions of users from subscriber bases,
triggering native Telegram push notifications to view the story.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.blacklist_import import subscriber_blacklisted_condition
from proxima_traffic.config import Settings
from proxima_traffic.content import resolve_media_path
from proxima_traffic.gateways import MessageGateway
from proxima_traffic.models import (
    StoryCampaignStatus,
    StoryTaggingAccount,
    StoryTaggingCampaign,
    StoryTaggingMentionState,
    StoryTaggingTask,
    StoryTaskStatus,
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    utc_now,
)
from proxima_traffic.story_content import pack_story_mentions, story_video_attributes, utf16_length
from proxima_traffic.story_runtime import (
    StoryWorker,
    lock_story_queue,
    story_accounts,
    validate_story_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StoryTaggingDashboard:
    campaign_count: int
    active_campaign_count: int
    total_tasks: int
    succeeded_tasks: int
    failed_tasks: int
    total_tagged_users: int
    simulated: bool = False
    skipped_tasks: int = 0


@dataclass(frozen=True, slots=True)
class StoryTaggingCampaignView:
    id: int
    name: str
    subscriber_list_id: int
    subscriber_list_name: str
    media_path: str
    media_kind: str
    media_name: str | None
    caption: str
    tags_per_story: int
    total_target_tags: int
    daily_limit_per_account: int
    cooldown_seconds: int
    status: StoryCampaignStatus
    enabled: bool
    created_at: datetime
    total_tasks: int
    succeeded_tasks: int
    failed_tasks: int
    tagged_count: int
    last_story_url: str | None
    simulated: bool | None = True
    last_error: str | None = None
    next_run_at: datetime | None = None
    skipped_tasks: int = 0


class StoryTaggingService:
    """Manages story tagging campaigns, subscriber targeting, and background worker."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._worker: StoryTaggingWorker | None = None

    def start(self) -> None:
        """Start the background worker."""
        if self._worker is None or self._worker.is_stopped():
            self._worker = StoryTaggingWorker(
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
            select(StoryTaggingTask.error)
            .where(
                StoryTaggingTask.campaign_id == campaign_id,
                StoryTaggingTask.error.is_not(None),
            )
            .order_by(StoryTaggingTask.id.desc())
            .limit(1)
        )

    async def _next_run_at(self, session, campaign_id):
        return await session.scalar(
            select(func.min(StoryTaggingTask.scheduled_for)).where(
                StoryTaggingTask.campaign_id == campaign_id,
                StoryTaggingTask.status == self._queued_status(),
            )
        )

    @staticmethod
    def _queued_status():
        return StoryTaskStatus.QUEUED

    async def get_dashboard(self) -> StoryTaggingDashboard:
        """Calculate high-level dashboard metrics for story tagging."""
        async with self._session_factory() as session:
            campaign_count = (
                await session.scalar(select(func.count(StoryTaggingCampaign.id)))
            ) or 0
            active_count = (
                await session.scalar(
                    select(func.count(StoryTaggingCampaign.id)).where(
                        StoryTaggingCampaign.status == StoryCampaignStatus.ACTIVE
                    )
                )
            ) or 0

            total_tasks = (await session.scalar(select(func.count(StoryTaggingTask.id)))) or 0
            succeeded_tasks = (
                await session.scalar(
                    select(func.count(StoryTaggingTask.id)).where(
                        StoryTaggingTask.status == StoryTaskStatus.SUCCEEDED
                    )
                )
            ) or 0
            failed_tasks = (
                await session.scalar(
                    select(func.count(StoryTaggingTask.id)).where(
                        StoryTaggingTask.status == StoryTaskStatus.FAILED
                    )
                )
            ) or 0

            total_tagged = (
                await session.scalar(select(func.count(StoryTaggingMentionState.id)))
            ) or 0

            return StoryTaggingDashboard(
                campaign_count=int(campaign_count),
                active_campaign_count=int(active_count),
                total_tasks=int(total_tasks),
                succeeded_tasks=int(succeeded_tasks),
                failed_tasks=int(failed_tasks),
                total_tagged_users=int(total_tagged),
                simulated=getattr(self._settings, "dry_run", False),
                skipped_tasks=int(
                    await session.scalar(
                        select(func.count(StoryTaggingTask.id)).where(
                            StoryTaggingTask.status.in_(
                                [StoryTaskStatus.SKIPPED, StoryTaskStatus.CANCELLED]
                            ),
                        )
                    )
                    or 0
                ),
            )

    async def list_campaigns(
        self, limit: int = 50, offset: int = 0
    ) -> list[StoryTaggingCampaignView]:
        """Return a page of campaigns with live progress statistics."""
        views: list[StoryTaggingCampaignView] = []
        async with self._session_factory() as session:
            query = (
                select(StoryTaggingCampaign, SubscriberList.name.label("list_name"))
                .join(
                    SubscriberList,
                    SubscriberList.id == StoryTaggingCampaign.subscriber_list_id,
                )
                .order_by(StoryTaggingCampaign.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            rows = (await session.execute(query)).all()

            for campaign, list_name in rows:
                stats_query = select(
                    func.count(StoryTaggingTask.id).label("total"),
                    func.count(StoryTaggingTask.id)
                    .filter(StoryTaggingTask.status == StoryTaskStatus.SUCCEEDED)
                    .label("succeeded"),
                    func.count(StoryTaggingTask.id)
                    .filter(StoryTaggingTask.status == StoryTaskStatus.FAILED)
                    .label("failed"),
                ).where(StoryTaggingTask.campaign_id == campaign.id)
                stats = (await session.execute(stats_query)).one()

                tagged_count = (
                    await session.scalar(
                        select(func.count(StoryTaggingMentionState.id)).where(
                            StoryTaggingMentionState.campaign_id == campaign.id
                        )
                    )
                ) or 0

                last_url = await session.scalar(
                    select(StoryTaggingTask.story_url)
                    .where(
                        StoryTaggingTask.campaign_id == campaign.id,
                        StoryTaggingTask.status == StoryTaskStatus.SUCCEEDED,
                        StoryTaggingTask.story_url.is_not(None),
                    )
                    .order_by(StoryTaggingTask.finished_at.desc())
                    .limit(1)
                )

                views.append(
                    StoryTaggingCampaignView(
                        id=campaign.id,
                        name=campaign.name,
                        subscriber_list_id=campaign.subscriber_list_id,
                        subscriber_list_name=str(list_name),
                        media_path=campaign.media_path,
                        media_kind=campaign.media_kind,
                        media_name=campaign.media_name,
                        caption=campaign.caption,
                        tags_per_story=campaign.tags_per_story,
                        total_target_tags=campaign.total_target_tags,
                        daily_limit_per_account=campaign.daily_limit_per_account,
                        cooldown_seconds=campaign.cooldown_seconds,
                        status=campaign.status,
                        enabled=campaign.enabled,
                        created_at=campaign.created_at,
                        simulated=campaign.simulated,
                        last_error=await self._last_error(session, campaign.id),
                        next_run_at=await self._next_run_at(session, campaign.id),
                        skipped_tasks=int(
                            await session.scalar(
                                select(func.count(StoryTaggingTask.id)).where(
                                    StoryTaggingTask.campaign_id == campaign.id,
                                    StoryTaggingTask.status.in_(
                                        [StoryTaskStatus.SKIPPED, StoryTaskStatus.CANCELLED]
                                    ),
                                )
                            )
                            or 0
                        ),
                        total_tasks=int(stats.total or 0),
                        succeeded_tasks=int(stats.succeeded or 0),
                        failed_tasks=int(stats.failed or 0),
                        tagged_count=int(tagged_count),
                        last_story_url=last_url,
                    )
                )

        return views

    async def get_campaign(self, campaign_id: int) -> StoryTaggingCampaignView | None:
        """Fetch one campaign view by ID."""
        async with self._session_factory() as session:
            query = (
                select(StoryTaggingCampaign, SubscriberList.name.label("list_name"))
                .join(
                    SubscriberList,
                    SubscriberList.id == StoryTaggingCampaign.subscriber_list_id,
                )
                .where(StoryTaggingCampaign.id == campaign_id)
            )
            row = (await session.execute(query)).first()
            if row is None:
                return None
            campaign, list_name = row

            stats_query = select(
                func.count(StoryTaggingTask.id).label("total"),
                func.count(StoryTaggingTask.id)
                .filter(StoryTaggingTask.status == StoryTaskStatus.SUCCEEDED)
                .label("succeeded"),
                func.count(StoryTaggingTask.id)
                .filter(StoryTaggingTask.status == StoryTaskStatus.FAILED)
                .label("failed"),
            ).where(StoryTaggingTask.campaign_id == campaign.id)
            stats = (await session.execute(stats_query)).one()

            tagged_count = (
                await session.scalar(
                    select(func.count(StoryTaggingMentionState.id)).where(
                        StoryTaggingMentionState.campaign_id == campaign.id
                    )
                )
            ) or 0

            last_url = await session.scalar(
                select(StoryTaggingTask.story_url)
                .where(
                    StoryTaggingTask.campaign_id == campaign.id,
                    StoryTaggingTask.status == StoryTaskStatus.SUCCEEDED,
                    StoryTaggingTask.story_url.is_not(None),
                )
                .order_by(StoryTaggingTask.finished_at.desc())
                .limit(1)
            )

            return StoryTaggingCampaignView(
                id=campaign.id,
                name=campaign.name,
                subscriber_list_id=campaign.subscriber_list_id,
                subscriber_list_name=str(list_name),
                media_path=campaign.media_path,
                media_kind=campaign.media_kind,
                media_name=campaign.media_name,
                caption=campaign.caption,
                tags_per_story=campaign.tags_per_story,
                total_target_tags=campaign.total_target_tags,
                daily_limit_per_account=campaign.daily_limit_per_account,
                cooldown_seconds=campaign.cooldown_seconds,
                status=campaign.status,
                enabled=campaign.enabled,
                created_at=campaign.created_at,
                simulated=campaign.simulated,
                last_error=await self._last_error(session, campaign.id),
                next_run_at=await self._next_run_at(session, campaign.id),
                skipped_tasks=int(
                    await session.scalar(
                        select(func.count(StoryTaggingTask.id)).where(
                            StoryTaggingTask.campaign_id == campaign.id,
                            StoryTaggingTask.status.in_(
                                [StoryTaskStatus.SKIPPED, StoryTaskStatus.CANCELLED]
                            ),
                        )
                    )
                    or 0
                ),
                total_tasks=int(stats.total or 0),
                succeeded_tasks=int(stats.succeeded or 0),
                failed_tasks=int(stats.failed or 0),
                tagged_count=int(tagged_count),
                last_story_url=last_url,
            )

    async def create_campaign(
        self,
        *,
        name: str,
        subscriber_list_id: int,
        media_path: str,
        media_kind: str = "photo",
        media_name: str | None = None,
        media_size: int | None = None,
        caption: str = "",
        tags_per_story: int = 5,
        total_target_tags: int = 50,
        daily_limit_per_account: int = 3,
        cooldown_seconds: int = 180,
        account_ids: list[int] | None = None,
        auto_start: bool = True,
    ) -> StoryTaggingCampaignView:
        """Create a new story tagging campaign and pre-generate tasks from subscriber list."""
        if not (1 <= tags_per_story <= 50):
            raise ValueError("tags_per_story must be between 1 and 50")
        if not (1 <= total_target_tags <= 1_000_000):
            raise ValueError("total_target_tags must be between 1 and 1,000,000")

        if auto_start and not self._settings.worker_enabled:
            raise ValueError("Фоновые отправки выключены. Включите worker перед запуском кампании.")
        name = validate_story_name(name)
        if not 1 <= daily_limit_per_account <= 1000 or not 0 <= cooldown_seconds <= 86400:
            raise ValueError("Лимит должен быть 1–1000, пауза — 0–86400 секунд")
        if media_kind not in {"photo", "video"}:
            raise ValueError("Для истории поддерживаются только фото и видео")
        if not self._settings.dry_run:
            path = resolve_media_path(self._settings, media_path)
            if not 0 < path.stat().st_size <= 30 * 1024 * 1024:
                raise ValueError("Медиа истории должно быть не больше 30 МБ и не пустым")
            if media_kind == "video":
                await asyncio.to_thread(story_video_attributes, str(path))
        async with self._session_factory() as session:
            await lock_story_queue(session)
            # Check duplicate name
            existing = await session.scalar(
                select(StoryTaggingCampaign.id).where(StoryTaggingCampaign.name == name)
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
            campaign = StoryTaggingCampaign(
                name=name,
                subscriber_list_id=subscriber_list_id,
                media_path=media_path,
                media_kind=media_kind,
                media_name=media_name,
                media_size=media_size,
                caption=caption,
                tags_per_story=tags_per_story,
                total_target_tags=total_target_tags,
                daily_limit_per_account=daily_limit_per_account,
                cooldown_seconds=cooldown_seconds,
                status=StoryCampaignStatus.ACTIVE,
                enabled=True,
                simulated=self._settings.dry_run,
            )
            session.add(campaign)
            await session.flush()

            accounts = await story_accounts(session, account_ids)

            for acc in accounts:
                session.add(
                    StoryTaggingAccount(
                        campaign_id=campaign.id,
                        account_id=acc.id,
                        stories_today=0,
                        enabled=True,
                    )
                )

            # Query untagged subscribers with usernames from the selected list
            # Filter out blacklisted users
            sub_query = (
                select(Subscriber.id, Subscriber.username)
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
                .order_by(Subscriber.id)
                .limit(total_target_tags)
            )
            subscribers_to_tag = (await session.execute(sub_query)).all()

            if not subscribers_to_tag:
                raise ValueError(
                    "В выбранной базе не найдено доступных подписчиков с юзернеймами для отметки"
                )

            # Pack by UTF-16 length as well as count. No recipient is silently
            # removed when an ordinary account has a shorter caption limit.
            caption_limit = 2048 if all(a.is_premium for a in accounts) else 200
            if utf16_length(caption) > caption_limit - 34:
                raise ValueError(
                    f"Сократите подпись до {caption_limit - 34} символов: нужно место для отметок"
                )
            chunks = pack_story_mentions(subscribers_to_tag, caption, tags_per_story, caption_limit)
            now = utc_now()
            for i, chunk in enumerate(chunks):
                session.add(
                    StoryTaggingTask(
                        campaign_id=campaign.id,
                        tagged_subscriber_ids_json=json.dumps([row[0] for row in chunk]),
                        tagged_usernames_json=json.dumps([row[1] for row in chunk]),
                        status=StoryTaskStatus.QUEUED,
                        scheduled_for=now + timedelta(seconds=i * cooldown_seconds),
                    )
                )
            campaign.total_target_tags = sum(len(chunk) for chunk in chunks)

            await session.commit()

        # Ensure worker is running if requested
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
                update(StoryTaggingCampaign)
                .where(
                    StoryTaggingCampaign.id == campaign_id,
                    StoryTaggingCampaign.status == StoryCampaignStatus.ACTIVE,
                )
                .values(status=StoryCampaignStatus.PAUSED)
            )
            await session.commit()
            return bool(res.rowcount > 0)

    async def resume_campaign(self, campaign_id: int, auto_start: bool = True) -> bool:
        """Resume a paused campaign."""
        if auto_start and not self._settings.worker_enabled:
            return False
        async with self._session_factory() as session:
            res = await session.execute(
                update(StoryTaggingCampaign)
                .where(
                    StoryTaggingCampaign.id == campaign_id,
                    StoryTaggingCampaign.simulated == self._settings.dry_run,
                    StoryTaggingCampaign.status == StoryCampaignStatus.PAUSED,
                )
                .values(status=StoryCampaignStatus.ACTIVE)
            )
            if res.rowcount > 0:
                await session.execute(
                    update(StoryTaggingTask)
                    .where(
                        StoryTaggingTask.campaign_id == campaign_id,
                        StoryTaggingTask.status == self._queued_status(),
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
        """Delete a campaign and all associated tasks."""
        async with self._session_factory() as session:
            await lock_story_queue(session)
            if await session.scalar(
                select(StoryTaggingTask.id)
                .where(
                    StoryTaggingTask.campaign_id == campaign_id,
                    StoryTaggingTask.status == StoryTaskStatus.RUNNING,
                )
                .limit(1)
            ):
                raise ValueError(
                    "Сначала приостановите кампанию и дождитесь окончания текущей публикации"
                )
            campaign = await session.scalar(
                select(StoryTaggingCampaign).where(StoryTaggingCampaign.id == campaign_id)
            )
            if campaign is None:
                return False
            await session.delete(campaign)
            await session.commit()
            return True


class StoryTaggingWorker(StoryWorker):
    """Durable story publication queue."""

    def __init__(self, session_factory, gateway, settings):
        super().__init__(session_factory, gateway, settings)
