"""Tests for Story Viewer (mass story watching and reactions) service, worker, and UI."""

import shutil
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from proxima_traffic.bot.story_viewer_ui import (
    story_viewer_campaign_keyboard,
    story_viewer_campaign_text,
    story_viewer_confirm_keyboard,
    story_viewer_delays_keyboard,
    story_viewer_emoji_keyboard,
    story_viewer_home_keyboard,
    story_viewer_home_text,
    story_viewer_limits_keyboard,
    story_viewer_list_keyboard,
    story_viewer_reaction_keyboard,
    story_viewer_targets_keyboard,
)
from proxima_traffic.config import Settings
from proxima_traffic.gateways import (
    DirectRecipient,
    DryRunGateway,
    PermanentGatewayError,
    StoryViewResult,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    StoryViewerAccount,
    StoryViewerCampaign,
    StoryViewerCampaignStatus,
    StoryViewerRecord,
    StoryViewerTask,
    StoryViewerTaskStatus,
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
)
from proxima_traffic.story_viewer import (
    StoryViewerCampaignView,
    StoryViewerDashboard,
    StoryViewerService,
    StoryViewerWorker,
)


@pytest_asyncio.fixture
async def session_factory(tmp_path: Path, schema_template: Path):
    db_file = tmp_path / "story_viewer_test.db"
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
    )


@pytest_asyncio.fixture
async def seeded_data(session_factory: async_sessionmaker[AsyncSession]):
    """Seed test accounts, subscriber list and subscribers."""
    async with session_factory() as session:
        # Accounts
        acc1 = Account(
            label="viewer_acc1",
            phone="79991112233",
            status=AccountStatus.ACTIVE,
            operator_paused=False,
        )
        acc2 = Account(
            label="viewer_acc2",
            phone="79992223344",
            status=AccountStatus.ACTIVE,
            operator_paused=False,
        )
        session.add_all([acc1, acc2])
        await session.flush()

        # Subscriber list
        sub_list = SubscriberList(
            name="База активных криптанов",
            status=SubscriberListStatus.READY,
            member_count=5,
        )
        session.add(sub_list)
        await session.flush()

        # Subscribers
        for i in range(1, 6):
            sub = Subscriber(
                telegram_user_id=100000 + i,
                username=f"user_{i}",
            )
            session.add(sub)
            await session.flush()

            member = SubscriberListMember(
                subscriber_list_id=sub_list.id,
                subscriber_id=sub.id,
            )
            session.add(member)

        await session.commit()
        return {"acc1": acc1.id, "acc2": acc2.id, "sub_list": sub_list.id}


@pytest.mark.asyncio
async def test_create_campaign_and_prepopulate_tasks(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, int],
):
    gateway = DryRunGateway()
    service = StoryViewerService(session_factory, gateway, settings)

    campaign = await service.create_campaign(
        name="Тестовый просмотр",
        subscriber_list_id=seeded_data["sub_list"],
        send_reaction=True,
        reaction_emoji="🔥",
        delay_min_seconds=3,
        delay_max_seconds=8,
        daily_limit_per_account=50,
        max_targets=5,
        auto_start=False,
    )

    assert campaign.id is not None
    assert campaign.name == "Тестовый просмотр"
    assert campaign.send_reaction is True
    assert campaign.reaction_emoji == "🔥"
    assert campaign.status == StoryViewerCampaignStatus.ACTIVE
    assert campaign.total_tasks == 5

    async with session_factory() as session:
        tasks = (
            await session.execute(
                select(StoryViewerTask).where(StoryViewerTask.campaign_id == campaign.id)
            )
        ).scalars().all()
        assert len(tasks) == 5
        assert all(t.status == StoryViewerTaskStatus.QUEUED for t in tasks)

        # Check account associations
        accs = (
            await session.execute(
                select(StoryViewerAccount).where(StoryViewerAccount.campaign_id == campaign.id)
            )
        ).scalars().all()
        assert len(accs) == 2


@pytest.mark.asyncio
async def test_worker_executes_view_and_reaction(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, int],
):
    gateway = DryRunGateway()
    service = StoryViewerService(session_factory, gateway, settings)

    campaign = await service.create_campaign(
        name="Кампания со сбором реакций",
        subscriber_list_id=seeded_data["sub_list"],
        send_reaction=True,
        reaction_emoji="❤️",
        delay_min_seconds=0,
        delay_max_seconds=0,
        daily_limit_per_account=100,
        max_targets=2,
        auto_start=False,
    )

    worker = StoryViewerWorker(session_factory, gateway, settings)

    # Process first task
    processed_1 = await worker._process_next_task()
    assert processed_1 is True

    # Process second task
    processed_2 = await worker._process_next_task()
    assert processed_2 is True

    async with session_factory() as session:
        succeeded = (
            await session.execute(
                select(StoryViewerTask).where(
                    StoryViewerTask.campaign_id == campaign.id,
                    StoryViewerTask.status == StoryViewerTaskStatus.SUCCEEDED,
                )
            )
        ).scalars().all()
        assert len(succeeded) == 2
        assert all(t.stories_viewed > 0 for t in succeeded)
        assert all(t.reaction_sent is True for t in succeeded)

        records = (
            await session.execute(
                select(StoryViewerRecord).where(StoryViewerRecord.campaign_id == campaign.id)
            )
        ).scalars().all()
        assert len(records) >= 2


@pytest.mark.asyncio
async def test_worker_skips_when_no_active_stories(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, int],
    monkeypatch,
):
    gateway = DryRunGateway()

    # Mock gateway returning no active stories
    async def mock_view_empty(*args, **kwargs):
        recipient = kwargs.get("recipient")
        uid = recipient.telegram_user_id if recipient else 0
        return StoryViewResult(
            telegram_user_id=uid,
            stories_found=0,
            stories_viewed=0,
            reaction_sent=False,
            has_active_stories=False,
            simulated=True,
        )

    monkeypatch.setattr(gateway, "view_peer_stories", mock_view_empty)

    service = StoryViewerService(session_factory, gateway, settings)
    campaign = await service.create_campaign(
        name="Кампания без историй",
        subscriber_list_id=seeded_data["sub_list"],
        send_reaction=True,
        reaction_emoji="🔥",
        delay_min_seconds=0,
        delay_max_seconds=0,
        daily_limit_per_account=100,
        max_targets=1,
        auto_start=False,
    )

    worker = StoryViewerWorker(session_factory, gateway, settings)
    processed = await worker._process_next_task()
    assert processed is True

    async with session_factory() as session:
        task = await session.scalar(
            select(StoryViewerTask).where(StoryViewerTask.campaign_id == campaign.id)
        )
        assert task is not None
        assert task.status == StoryViewerTaskStatus.SKIPPED
        assert task.stories_viewed == 0
        assert task.reaction_sent is False


@pytest.mark.asyncio
async def test_worker_flood_wait_rotates_account_without_dropping_task(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, int],
    monkeypatch,
):
    gateway = DryRunGateway()

    first_call = True

    async def mock_flood_then_success(*args, **kwargs):
        nonlocal first_call
        if first_call:
            first_call = False
            raise TemporaryGatewayError("Flood wait 300s", retry_after=300, code="flood_wait")
        return StoryViewResult(
            telegram_user_id=123,
            stories_found=1,
            stories_viewed=1,
            reaction_sent=True,
            reaction_emoji="🔥",
            has_active_stories=True,
            simulated=True,
            viewed_story_ids=(1,),
            reacted_story_id=1,
        )

    monkeypatch.setattr(gateway, "view_peer_stories", mock_flood_then_success)

    service = StoryViewerService(session_factory, gateway, settings)
    campaign = await service.create_campaign(
        name="Кампания с ротацией Flood",
        subscriber_list_id=seeded_data["sub_list"],
        send_reaction=True,
        delay_min_seconds=0,
        delay_max_seconds=0,
        daily_limit_per_account=100,
        max_targets=1,
        auto_start=False,
    )

    worker = StoryViewerWorker(session_factory, gateway, settings)

    # First attempt: hits flood wait on first account
    res1 = await worker._process_next_task()
    assert res1 is True

    # Task should be restored to QUEUED (not failed, not dropped!)
    async with session_factory() as session:
        t = await session.scalar(
            select(StoryViewerTask).where(StoryViewerTask.campaign_id == campaign.id)
        )
        assert t is not None
        assert t.status == StoryViewerTaskStatus.QUEUED

    # Second attempt: should rotate to the second account and succeed!
    res2 = await worker._process_next_task()
    assert res2 is True

    async with session_factory() as session:
        t = await session.scalar(
            select(StoryViewerTask).where(StoryViewerTask.campaign_id == campaign.id)
        )
        assert t is not None
        assert t.status == StoryViewerTaskStatus.SUCCEEDED
        assert t.stories_viewed == 1


@pytest.mark.asyncio
async def test_toggle_and_list_campaign_accounts(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    seeded_data: dict[str, int],
):
    gateway = DryRunGateway()
    service = StoryViewerService(session_factory, gateway, settings)

    campaign = await service.create_campaign(
        name="Кампания для теста аккаунтов",
        subscriber_list_id=seeded_data["sub_list"],
        send_reaction=True,
        delay_min_seconds=0,
        delay_max_seconds=0,
        daily_limit_per_account=100,
        max_targets=3,
        auto_start=False,
    )

    accs = await service.list_accounts_for_campaign(campaign.id)
    assert len(accs) == 2
    assert all(a.enabled is True for a in accs)

    # Toggle first account to disabled
    target_acc_id = accs[0].account_id
    new_state = await service.toggle_campaign_account(campaign.id, target_acc_id)
    assert new_state is False

    updated_accs = await service.list_accounts_for_campaign(campaign.id)
    disabled_acc = next(a for a in updated_accs if a.account_id == target_acc_id)
    assert disabled_acc.enabled is False


def test_story_viewer_ui_presentation():
    dashboard = StoryViewerDashboard(
        campaign_count=3,
        active_campaign_count=1,
        total_tasks=100,
        succeeded_tasks=45,
        skipped_tasks=10,
        failed_tasks=1,
        total_stories_viewed=82,
        total_reactions_sent=45,
        simulated=False,
    )
    home_text = story_viewer_home_text(dashboard)
    assert "Просмотр и реакции на истории" in home_text
    assert "82" in home_text

    home_kb = story_viewer_home_keyboard()
    assert any(
        btn.callback_data == "story_view:new"
        for row in home_kb.inline_keyboard
        for btn in row
    )

    reaction_kb = story_viewer_reaction_keyboard()
    assert any(
        btn.callback_data == "story_view:reaction:yes"
        for row in reaction_kb.inline_keyboard
        for btn in row
    )

    emoji_kb = story_viewer_emoji_keyboard()
    assert any(
        btn.callback_data == "story_view:emoji:🔥"
        for row in emoji_kb.inline_keyboard
        for btn in row
    )

    delays_kb = story_viewer_delays_keyboard()
    assert any(
        btn.callback_data == "story_view:delay:5:15"
        for row in delays_kb.inline_keyboard
        for btn in row
    )

