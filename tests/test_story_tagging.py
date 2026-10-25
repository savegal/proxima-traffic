"""Tests for Story Tagging (mentions in Telegram stories) service, worker, and UI."""

import shutil
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from proxima_traffic.bot.story_tagging_ui import (
    story_campaign_keyboard,
    story_campaign_text,
    story_confirm_keyboard,
    story_home_keyboard,
    story_list_keyboard,
    story_select_list_keyboard,
    story_tagging_home_text,
    story_tags_per_story_keyboard,
    story_total_tags_keyboard,
)
from proxima_traffic.config import Settings
from proxima_traffic.gateways import (
    DryRunGateway,
    PermanentGatewayError,
    StoryDelivery,
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
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
)
from proxima_traffic.story_tagging import (
    StoryTaggingCampaignView,
    StoryTaggingDashboard,
    StoryTaggingService,
    StoryTaggingWorker,
)


@pytest_asyncio.fixture
async def session_factory(tmp_path: Path, schema_template: Path):
    db_file = tmp_path / "story_test.db"
    shutil.copy2(schema_template, db_file)
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    yield factory
    await engine.dispose()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/dummy.db",
        dry_run=True,
        media_storage_path=str(tmp_path / "media"),
    )


@pytest_asyncio.fixture
async def seeded_data(session_factory: async_sessionmaker[AsyncSession]):
    """Seed test accounts, subscriber list and subscribers."""
    async with session_factory() as session:
        # Accounts
        acc1 = Account(
            label="acc1",
            phone="79991112233",
            status=AccountStatus.ACTIVE,
            operator_paused=False,
        )
        acc2 = Account(
            label="acc2",
            phone="79992223344",
            status=AccountStatus.ACTIVE,
            operator_paused=False,
        )
        session.add_all([acc1, acc2])
        await session.flush()

        # Subscriber list
        sub_list = SubscriberList(
            name="Целевые крипта подписчики",
            status=SubscriberListStatus.READY,
            member_count=10,
        )
        session.add(sub_list)
        await session.flush()

        # Subscribers (some with username, some without)
        for i in range(1, 11):
            sub = Subscriber(
                telegram_user_id=100000 + i,
                username=f"user_{i}" if i <= 8 else None,
            )
            session.add(sub)
            await session.flush()
            session.add(SubscriberListMember(subscriber_list_id=sub_list.id, subscriber_id=sub.id))

        await session.commit()
        return {"account_ids": [acc1.id, acc2.id], "list_id": sub_list.id}


@pytest.mark.asyncio
async def test_create_story_campaign_and_task_generation(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, object],
):
    gateway = DryRunGateway()
    service = StoryTaggingService(session_factory, gateway, settings)

    view = await service.create_campaign(
        name="Тестовый запуск теггинга",
        subscriber_list_id=seeded_data["list_id"],
        media_path="tests/fixtures/test.jpg",
        media_kind="photo",
        caption="Смотрите нашу новую историю! 🔥",
        tags_per_story=3,
        total_target_tags=6,
        daily_limit_per_account=3,
        cooldown_seconds=60,
        auto_start=False,
    )

    assert view.name == "Тестовый запуск теггинга"
    assert view.status == StoryCampaignStatus.ACTIVE
    assert view.tags_per_story == 3
    assert view.total_target_tags == 6
    assert view.total_tasks == 2  # 6 tags / 3 tags_per_story = 2 tasks

    # Verify tasks in DB
    async with session_factory() as session:
        tasks = (
            await session.execute(
                select(StoryTaggingTask).where(StoryTaggingTask.campaign_id == view.id)
            )
        ).scalars().all()
        assert len(tasks) == 2
        for t in tasks:
            assert t.status == StoryTaskStatus.QUEUED

        # Verify account links
        accs = (
            await session.execute(
                select(StoryTaggingAccount).where(StoryTaggingAccount.campaign_id == view.id)
            )
        ).scalars().all()
        assert len(accs) == 2


@pytest.mark.asyncio
async def test_story_worker_executes_tasks_and_completes_campaign(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, object],
):
    gateway = DryRunGateway()
    service = StoryTaggingService(session_factory, gateway, settings)

    view = await service.create_campaign(
        name="Быстрая кампания",
        subscriber_list_id=seeded_data["list_id"],
        media_path="tests/fixtures/test.jpg",
        media_kind="photo",
        tags_per_story=3,
        total_target_tags=3,  # Exactly 1 task
        cooldown_seconds=10,
        auto_start=False,
    )

    worker = StoryTaggingWorker(session_factory, gateway, settings)

    # Process first (and only) task
    processed = await worker._process_next_task()
    assert processed is True

    # Check task succeeded
    async with session_factory() as session:
        task = await session.scalar(
            select(StoryTaggingTask).where(StoryTaggingTask.campaign_id == view.id)
        )
        assert task.status == StoryTaskStatus.SUCCEEDED
        assert task.story_id is not None
        assert task.story_url is not None

        # Check mention state recorded
        mentions = (
            await session.execute(
                select(StoryTaggingMentionState).where(
                    StoryTaggingMentionState.campaign_id == view.id
                )
            )
        ).scalars().all()
        assert len(mentions) == 3

        # Check account stories_today incremented
        acc_link = await session.scalar(
            select(StoryTaggingAccount).where(StoryTaggingAccount.campaign_id == view.id)
        )
        assert acc_link.stories_today == 1

        # Check campaign status is COMPLETED
        camp = await session.scalar(
            select(StoryTaggingCampaign).where(StoryTaggingCampaign.id == view.id)
        )
        assert camp.status == StoryCampaignStatus.COMPLETED


@pytest.mark.asyncio
async def test_story_campaign_pause_resume_delete(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, object],
):
    gateway = DryRunGateway()
    service = StoryTaggingService(session_factory, gateway, settings)

    view = await service.create_campaign(
        name="Кампания для паузы",
        subscriber_list_id=seeded_data["list_id"],
        media_path="test.jpg",
        media_kind="photo",
        tags_per_story=5,
        total_target_tags=5,
        auto_start=False,
    )

    # Pause
    paused = await service.pause_campaign(view.id)
    assert paused is True
    updated = await service.get_campaign(view.id)
    assert updated.status == StoryCampaignStatus.PAUSED

    # Resume
    resumed = await service.resume_campaign(view.id, auto_start=False)
    assert resumed is True
    updated = await service.get_campaign(view.id)
    assert updated.status == StoryCampaignStatus.ACTIVE

    # Delete
    deleted = await service.delete_campaign(view.id)
    assert deleted is True
    assert await service.get_campaign(view.id) is None


def test_story_tagging_ui_formatters():
    dashboard = StoryTaggingDashboard(
        campaign_count=5,
        active_campaign_count=2,
        total_tasks=20,
        succeeded_tasks=12,
        failed_tasks=1,
        total_tagged_users=60,
        simulated=False,
    )

    text = story_tagging_home_text(dashboard)
    assert "Теггинг в историях Telegram" in text
    assert "60" in text
    assert "Реальная публикация" in text

    kb = story_home_keyboard()
    assert kb.inline_keyboard[0][0].callback_data == "story:new"

    view = StoryTaggingCampaignView(
        id=1,
        name="Промо Крипто",
        subscriber_list_id=4,
        subscriber_list_name="База 1",
        media_path="/path/to/media.jpg",
        media_kind="photo",
        media_name="promo.jpg",
        caption="Привет всем!",
        tags_per_story=5,
        total_target_tags=100,
        daily_limit_per_account=3,
        cooldown_seconds=120,
        status=StoryCampaignStatus.ACTIVE,
        enabled=True,
        created_at=None,
        total_tasks=20,
        succeeded_tasks=5,
        failed_tasks=0,
        tagged_count=25,
        last_story_url="https://t.me/test/s/42",
    )

    camp_text = story_campaign_text(view)
    assert "Промо Крипто" in camp_text
    assert "https://t.me/test/s/42" in camp_text

    camp_kb = story_campaign_keyboard(view)
    assert any(b.callback_data == "story:pause:1" for row in camp_kb.inline_keyboard for b in row)


class FailingAccountGateway(DryRunGateway):
    """Fails on account 1 with daily limit, succeeds on account 2."""

    async def publish_story(self, *, account, **kwargs):
        if account.id == 1:
            raise TemporaryGatewayError("дневной лимит историй", code="story_daily_limit_reached")
        return await super().publish_story(account=account, **kwargs)


@pytest.mark.asyncio
async def test_story_worker_account_daily_limit_rotation(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, object],
):
    gateway = FailingAccountGateway()
    service = StoryTaggingService(session_factory, gateway, settings)

    view = await service.create_campaign(
        name="Ротация при лимите",
        subscriber_list_id=seeded_data["list_id"],
        media_path="test.jpg",
        media_kind="photo",
        tags_per_story=3,
        total_target_tags=3,
        auto_start=False,
    )

    worker = StoryTaggingWorker(session_factory, gateway, settings)

    # First attempt: picks account 1, gets story_daily_limit_reached, cools down account 1
    # and keeps task QUEUED without burning attempt count!
    await worker._process_next_task()

    async with session_factory() as session:
        acc1 = await session.scalar(select(Account).where(Account.id == 1))
        assert acc1.cooldown_until is not None  # Account 1 is in cooldown!

        task = await session.scalar(
            select(StoryTaggingTask).where(StoryTaggingTask.campaign_id == view.id)
        )
        assert task.status == StoryTaskStatus.QUEUED
        assert task.attempt_count == 0  # Attempt count was NOT wasted!

    # Second attempt: Account 1 is in cooldown, worker picks Account 2 and succeeds!
    processed = await worker._process_next_task()
    assert processed is True

    async with session_factory() as session:
        task = await session.scalar(
            select(StoryTaggingTask).where(StoryTaggingTask.campaign_id == view.id)
        )
        assert task.status == StoryTaskStatus.SUCCEEDED
        assert task.account_id == 2  # Published using Account 2!

