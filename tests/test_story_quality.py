"""Story failures, competing workers and real Telethon request contracts (offline)."""

import asyncio
import shutil
import sqlite3
from contextlib import asynccontextmanager
from datetime import timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from PIL import Image
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError
from telethon import functions, types

from proxima_traffic.bot.handlers import router, unknown_message
from proxima_traffic.bot.story_tagging_ui import process_story_name
from proxima_traffic.bot.story_viewer_ui import (
    handle_story_viewer_name_input,
    story_viewer_accounts_pick_keyboard,
    story_viewer_manage_accs_keyboard,
)
from proxima_traffic.config import Settings
from proxima_traffic.content import ContentError, store_media
from proxima_traffic.db import Database
from proxima_traffic.gateways import (
    DirectRecipient,
    DryRunGateway,
    PermanentGatewayError,
    StoryDelivery,
    StoryViewResult,
    TemporaryGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.maintenance import _media_references
from proxima_traffic.models import (
    Account,
    AccountStatus,
    MentionBlacklistUsernameEntry,
    StoryTaggingCampaign,
    StoryTaggingMentionState,
    StoryTaggingTask,
    StoryViewerCampaign,
    StoryViewerRecord,
    StoryViewerTask,
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.story_content import (
    pack_story_mentions,
    prepare_story_photo,
    story_caption,
    story_video_attributes,
    utf16_length,
)
from proxima_traffic.story_tagging import StoryTaggingService, StoryTaggingWorker
from proxima_traffic.story_viewer import StoryViewerService, StoryViewerWorker


@pytest_asyncio.fixture(params=[False, True], ids=["tagging", "viewing"])
async def story_case(request, tmp_path, schema_template):
    path = tmp_path / "stories.db"
    shutil.copy2(schema_template, path)
    database = Database(f"sqlite+aiosqlite:///{path}")
    settings = Settings(dry_run=True, media_storage_path=str(tmp_path / "media"))
    factory = database.session_factory
    async with factory() as session:
        session.add_all([Account(label=f"sender-{i}", status=AccountStatus.ACTIVE) for i in (1, 2)])
        audience = SubscriberList(
            name="Audience", member_count=8, status=SubscriberListStatus.READY
        )
        session.add(audience)
        await session.flush()
        for i in range(8):
            sub = Subscriber(telegram_user_id=5000 + i, username=f"User_{i}")
            session.add(sub)
            await session.flush()
            session.add(SubscriberListMember(subscriber_list_id=audience.id, subscriber_id=sub.id))
        await session.commit()
    viewing = request.param
    service_type = StoryViewerService if viewing else StoryTaggingService
    worker_type = StoryViewerWorker if viewing else StoryTaggingWorker
    gateway = DryRunGateway()
    service = service_type(factory, gateway, settings)

    async def create(name="Quality", **kwargs):
        defaults = dict(
            name=name,
            subscriber_list_id=audience.id,
            account_ids=[1, 2],
            auto_start=False,
        )
        defaults.update(
            dict(delay_min_seconds=0, delay_max_seconds=0, max_targets=1)
            if viewing
            else dict(media_path="test.jpg", total_target_tags=1, cooldown_seconds=0)
        )
        defaults.update(kwargs)
        return await service.create_campaign(**defaults)

    case = SimpleNamespace(
        viewing=viewing,
        factory=factory,
        service=service,
        settings=settings,
        gateway=gateway,
        create=create,
        worker=lambda: worker_type(factory, gateway, settings),
        Task=StoryViewerTask if viewing else StoryTaggingTask,
        Campaign=StoryViewerCampaign if viewing else StoryTaggingCampaign,
        method="view_peer_stories" if viewing else "publish_story",
    )
    try:
        yield case
    finally:
        service.stop()
        await service.wait_stopped()
        await database.dispose()


async def task_for(case, campaign_id):
    async with case.factory() as session:
        return await session.scalar(select(case.Task).where(case.Task.campaign_id == campaign_id))


@pytest.mark.asyncio
async def test_competing_workers_execute_each_task_once(story_case, monkeypatch):
    c = story_case
    campaign = await c.create(
        daily_limit_per_account=100,
        **(dict(max_targets=8) if c.viewing else dict(total_target_tags=8, tags_per_story=1)),
    )
    original = getattr(c.gateway, c.method)
    calls = []
    active_accounts = set()

    async def execute(**kwargs):
        account_id = kwargs["account"].id
        assert account_id not in active_accounts
        active_accounts.add(account_id)
        calls.append(kwargs.get("idempotency_key", kwargs.get("recipient")))
        await asyncio.sleep(0.01)
        try:
            return await original(**kwargs)
        finally:
            active_accounts.remove(account_id)

    monkeypatch.setattr(c.gateway, c.method, execute)
    workers = [c.worker() for _ in range(5)]
    for _ in range(8):
        async with c.factory() as session:
            await session.execute(
                update(c.Task).where(c.Task.status == "QUEUED").values(scheduled_for=utc_now())
            )
            await session.commit()
        await asyncio.gather(*(w._process_next_task() for w in workers))
    async with c.factory() as session:
        tasks = (
            await session.scalars(select(c.Task).where(c.Task.campaign_id == campaign.id))
        ).all()
    assert len(calls) == 8
    assert len({repr(call) for call in calls}) == 8
    assert all(task.status.value == "succeeded" for task in tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("account_ids", [[], [999], [1, 999]])
async def test_invalid_sender_selection_rolls_back_campaign(story_case, account_ids):
    c = story_case
    with pytest.raises(ValueError):
        await c.create(account_ids=account_ids)
    async with c.factory() as session:
        assert await session.scalar(select(func.count(c.Campaign.id))) == 0


@pytest.mark.asyncio
async def test_expired_execution_never_republishes_and_old_owner_cannot_write(story_case):
    c = story_case
    campaign = await c.create()
    worker = c.worker()
    task, camp, link, account, _ = await worker._claim()
    async with c.factory() as session:
        await session.execute(
            update(c.Task)
            .where(c.Task.id == task.id)
            .values(
                lease_until=utc_now() - timedelta(seconds=1),
            )
        )
        await session.commit()
    assert await c.worker()._process_next_task() is False
    await worker._finish(task, camp, link, account, None, [], None)
    finished = await task_for(c, campaign.id)
    assert finished.status.value == "failed"
    assert finished.error_code == "delivery_unconfirmed"
    assert finished.lease_token is None


@pytest.mark.asyncio
async def test_terminal_failure_completes_campaign(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    monkeypatch.setattr(
        c.gateway,
        c.method,
        AsyncMock(side_effect=PermanentGatewayError("bad media", code="content_invalid")),
    )
    assert await c.worker()._process_next_task()
    assert (await c.service.get_campaign(campaign.id)).status.value == "completed"
    assert (await c.service.get_campaign(campaign.id)).last_error == "bad media"
    assert (await task_for(c, campaign.id)).attempt_count == 1


@pytest.mark.asyncio
async def test_network_retries_are_bounded_and_use_stable_publication_key(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    keys = []

    async def offline(**kwargs):
        keys.append(kwargs.get("idempotency_key"))
        raise TemporaryGatewayError(
            "offline before dispatch", code="proxy_unavailable", retry_after=1
        )

    monkeypatch.setattr(c.gateway, c.method, offline)
    worker = c.worker()
    for _ in range(3):
        async with c.factory() as session:
            await session.execute(update(c.Task).values(scheduled_for=utc_now()))
            await session.commit()
        assert await worker._process_next_task()
    assert (await task_for(c, campaign.id)).status.value == "failed"
    assert len(keys) == 3
    if not c.viewing:
        assert len(set(keys)) == 1


@pytest.mark.asyncio
async def test_flood_wait_never_shortens_existing_pause_and_rotates(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    original = getattr(c.gateway, c.method)
    calls = []

    async def send(**kwargs):
        calls.append(kwargs["account"].id)
        if len(calls) == 1:
            raise TemporaryGatewayError("wait", code="flood_wait", retry_after=90000)
        return await original(**kwargs)

    monkeypatch.setattr(c.gateway, c.method, send)
    worker = c.worker()
    before = utc_now()
    await worker._process_next_task()
    async with c.factory() as session:
        sender = await session.get(Account, 1)
        assert ensure_utc(sender.cooldown_until) >= before + timedelta(seconds=90000)
        assert sender.operator_paused is False
    await worker._process_next_task()
    assert calls == [1, 2]
    assert (await task_for(c, campaign.id)).status.value == "succeeded"


@pytest.mark.asyncio
async def test_no_healthy_senders_pauses_with_actionable_reason(story_case):
    c = story_case
    campaign = await c.create()
    async with c.factory() as session:
        await session.execute(update(Account).values(operator_paused=True))
        await session.commit()
    assert await c.worker()._process_next_task() is False
    view = await c.service.get_campaign(campaign.id)
    assert view.status.value == "paused"
    assert "аккаунт" in view.last_error


@pytest.mark.asyncio
async def test_mode_change_does_not_dispatch_queued_simulation(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    c.settings.dry_run = False
    send = AsyncMock()
    monkeypatch.setattr(c.gateway, c.method, send)
    assert await c.worker()._process_next_task() is False
    send.assert_not_awaited()
    assert (await c.service.get_campaign(campaign.id)).status.value == "paused"


@pytest.mark.asyncio
async def test_blacklist_and_opt_out_rechecked_after_campaign_creation(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    async with c.factory() as session:
        session.add(MentionBlacklistUsernameEntry(username="user_0", source="test"))
        await session.commit()
    send = AsyncMock()
    monkeypatch.setattr(c.gateway, c.method, send)
    await c.worker()._process_next_task()
    send.assert_not_awaited()
    assert (await task_for(c, campaign.id)).status.value == "skipped"


@pytest.mark.asyncio
async def test_pause_between_claim_and_dispatch_and_delete_guard(story_case):
    c = story_case
    campaign = await c.create()
    worker = c.worker()
    task, camp, link, account, daily = await worker._claim()
    with pytest.raises(ValueError, match="приостановите"):
        await c.service.delete_campaign(campaign.id)
    await c.service.pause_campaign(campaign.id)
    with pytest.raises(TemporaryGatewayError) as error:
        await worker._execute(task, camp, link, account, daily)
    assert error.value.code == "dispatch_cancelled"
    await worker._finish(task, camp, link, account, None, [], error.value)
    assert (await task_for(c, campaign.id)).status.value == "queued"


@pytest.mark.asyncio
async def test_cancellation_is_persisted_without_replaying(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    entered = asyncio.Event()

    async def block(**kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(c.gateway, c.method, block)
    running = asyncio.create_task(c.worker()._process_next_task())
    await asyncio.wait_for(entered.wait(), 5)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    task = await task_for(c, campaign.id)
    assert task.status.value == "failed"
    assert task.error_code == "delivery_unconfirmed"
    assert task.lease_token is None


def test_caption_utf16_offsets_and_packing_preserve_all_recipients():
    text, entities = story_caption("🔥 <b>literal</b>", ["alice", "bobbb"], 200)
    assert text.startswith("🔥 <b>literal</b>")
    assert entities[0].offset == utf16_length("🔥 <b>literal</b>\n\n")
    encoded = text.encode("utf-16-le")
    for entity, name in zip(entities, ("alice", "bobbb"), strict=True):
        assert (
            encoded[entity.offset * 2 : (entity.offset + entity.length) * 2].decode("utf-16-le")
            == "@" + name
        )
    rows = [(i, "user" + str(i) + "x" * 24) for i in range(50)]
    chunks = pack_story_mentions(rows, "🔥" * 25, 50, 200)
    assert [row for chunk in chunks for row in chunk] == rows
    assert len(chunks) > 1
    for chunk in chunks:
        assert utf16_length(story_caption("🔥" * 25, [r[1] for r in chunk], 200)[0]) <= 200


def test_story_message_handlers_precede_catch_all():
    callbacks = [handler.callback for handler in router.message.handlers]
    assert callbacks.index(process_story_name) < callbacks.index(unknown_message)
    assert callbacks.index(handle_story_viewer_name_input) < callbacks.index(unknown_message)


@asynccontextmanager
async def no_account_lock(_):
    yield


def live_gateway(tmp_path, *, failure=None, no_update=False):
    """Actual Telethon TL types; no network/session files are used."""
    settings = Settings(dry_run=True, media_storage_path=str(tmp_path / "media"))
    gateway = object.__new__(TelethonGateway)
    gateway._settings = settings
    gateway._account_operation = no_account_lock
    gateway._ensure_profile_not_blacklisted = AsyncMock()
    gateway._prepare_uploaded_media = AsyncMock(return_value=types.InputMediaEmpty())
    account = Account(id=1, label="test")
    recipient = DirectRecipient(telegram_user_id=5000, username="alice")
    requests = []

    class Client:
        async def get_me(self):
            return SimpleNamespace(id=100, premium=False, username="sender")

        async def get_entity(self, _):
            return SimpleNamespace(id=5000, username="alice", bot=False, deleted=False)

        async def get_input_entity(self, _):
            return types.InputPeerUser(5000, 1)

        async def __call__(self, request):
            requests.append(request)
            if failure and isinstance(request, failure[0]):
                raise failure[1]
            if isinstance(request, functions.stories.CanSendStoryRequest):
                return SimpleNamespace(count_remains=3)
            if isinstance(request, functions.stories.SendStoryRequest):
                updates = (
                    [] if no_update else [types.UpdateStoryID(id=42, random_id=request.random_id)]
                )
                return SimpleNamespace(updates=updates)
            if isinstance(request, functions.stories.GetPeerStoriesRequest):
                stories = [
                    types.StoryItem(
                        id=i,
                        date=utc_now(),
                        expire_date=utc_now() + timedelta(hours=1),
                        media=types.MessageMediaPhoto(),
                    )
                    for i in (4, 5, 6)
                ]
                return SimpleNamespace(stories=SimpleNamespace(stories=stories))
            if isinstance(request, functions.stories.ReadStoriesRequest):
                return [request.max_id]
            return True

    gateway._ensure_client = AsyncMock(return_value=Client())
    return gateway, account, recipient, requests, settings


@pytest.mark.asyncio
async def test_real_request_contains_explicit_mentions_and_matching_story_id(tmp_path):
    gateway, account, _, requests, settings = live_gateway(tmp_path)
    photo = BytesIO()
    Image.new("RGB", (720, 1280), (40, 90, 160)).save(photo, "JPEG")
    media = store_media(settings, photo.getvalue(), kind="photo", original_name="story.jpg")
    result = await gateway.publish_story(
        account=account,
        media_path=media.path,
        media_kind="photo",
        caption="🔥 literal <b>",
        tagged_subscribers=[Subscriber(id=1, telegram_user_id=5000, username="alice")],
        idempotency_key="stable-key",
    )
    sent = next(r for r in requests if isinstance(r, functions.stories.SendStoryRequest))
    assert result.story_id == 42
    assert result.tagged_subscriber_ids == (1,)
    assert result.story_url == "https://t.me/sender/s/42"
    assert sent.caption == "🔥 literal <b>\n\n@alice"
    assert isinstance(sent.entities[0], types.MessageEntityMention)
    assert sent.entities[0].offset == utf16_length("🔥 literal <b>\n\n")


@pytest.mark.asyncio
async def test_missing_story_id_does_not_guess_latest_story_or_report_success(tmp_path):
    gateway, account, _, requests, settings = live_gateway(tmp_path, no_update=True)
    photo = BytesIO()
    Image.new("RGB", (720, 1280), (40, 90, 160)).save(photo, "JPEG")
    media = store_media(settings, photo.getvalue(), kind="photo", original_name="story.jpg")
    with pytest.raises(PermanentGatewayError) as error:
        await gateway.publish_story(
            account=account,
            media_path=media.path,
            media_kind="photo",
            caption="",
            tagged_subscribers=[Subscriber(id=1, telegram_user_id=5000, username="alice")],
            idempotency_key="stable-key",
        )
    assert error.value.code == "delivery_unconfirmed"
    assert not any(isinstance(r, functions.stories.GetPeerStoriesRequest) for r in requests)


@pytest.mark.asyncio
async def test_increment_failure_never_reports_successful_views(tmp_path):
    gateway, account, recipient, requests, _ = live_gateway(
        tmp_path,
        failure=(
            functions.stories.IncrementStoryViewsRequest,
            TemporaryGatewayError("wait", code="flood_wait", retry_after=3600),
        ),
    )
    with pytest.raises(TemporaryGatewayError):
        await gateway.view_peer_stories(account=account, recipient=recipient)
    assert not any(isinstance(r, functions.stories.SendReactionRequest) for r in requests)


@pytest.mark.asyncio
async def test_reaction_failure_preserves_confirmed_views_and_exact_error(tmp_path):
    gateway, account, recipient, _, _ = live_gateway(
        tmp_path,
        failure=(
            functions.stories.SendReactionRequest,
            TemporaryGatewayError("wait", code="flood_wait", retry_after=90000),
        ),
    )
    result = await gateway.view_peer_stories(account=account, recipient=recipient)
    assert result.stories_viewed == 3
    assert result.viewed_story_ids == (4, 5, 6)
    assert result.reaction_sent is False
    assert result.error.code == "flood_wait"
    assert result.error.retry_after == 90000


@pytest.mark.asyncio
async def test_view_limit_and_dedup_are_applied_before_telegram_rpc(tmp_path):
    gateway, account, recipient, requests, _ = live_gateway(tmp_path)
    result = await gateway.view_peer_stories(
        account=account,
        recipient=recipient,
        excluded_story_ids=(4,),
        max_stories=1,
    )
    assert result.viewed_story_ids == (5,)
    assert result.reacted_story_id == 5
    increment = next(
        r for r in requests if isinstance(r, functions.stories.IncrementStoryViewsRequest)
    )
    assert increment.id == [5]


@pytest.mark.asyncio
async def test_partial_result_records_actual_ids_and_cools_sender(story_case, monkeypatch):
    c = story_case
    if not c.viewing:
        pytest.skip("viewer contract")
    campaign = await c.create()

    async def partial(**kwargs):
        return StoryViewResult(
            telegram_user_id=kwargs["recipient"].telegram_user_id,
            stories_found=3,
            stories_viewed=2,
            has_active_stories=True,
            simulated=True,
            viewed_story_ids=(101, 102),
            error=TemporaryGatewayError("reaction flood", code="flood_wait", retry_after=3600),
        )

    monkeypatch.setattr(c.gateway, c.method, partial)
    await c.worker()._process_next_task()
    task = await task_for(c, campaign.id)
    assert task.status.value == "failed"
    assert task.stories_viewed == 2
    async with c.factory() as session:
        ids = tuple(
            (
                await session.scalars(
                    select(StoryViewerRecord.story_id).order_by(StoryViewerRecord.story_id)
                )
            ).all()
        )
        assert ids == (101, 102)
        assert ensure_utc((await session.get(Account, 1)).cooldown_until) > utc_now()


@pytest.mark.asyncio
async def test_verified_tag_subset_is_accounted_without_false_mentions(story_case, monkeypatch):
    c = story_case
    if c.viewing:
        pytest.skip("publication contract")
    campaign = await c.create(total_target_tags=3)

    async def subset(**kwargs):
        return StoryDelivery(
            story_id=42,
            story_url=None,
            tagged_count=1,
            simulated=True,
            tagged_subscriber_ids=(kwargs["tagged_subscribers"][0].id,),
        )

    monkeypatch.setattr(c.gateway, c.method, subset)
    await c.worker()._process_next_task()
    view = await c.service.get_campaign(campaign.id)
    assert view.tagged_count == 1
    assert "2" in view.last_error
    async with c.factory() as session:
        assert await session.scalar(select(func.count(StoryTaggingMentionState.id))) == 1


@pytest.mark.asyncio
async def test_sqlite_outcome_retry_does_not_repeat_telegram_operation(story_case, monkeypatch):
    c = story_case
    campaign = await c.create()
    worker = c.worker()
    finish = worker._finish
    attempts = 0
    send = AsyncMock(wraps=getattr(c.gateway, c.method))
    monkeypatch.setattr(c.gateway, c.method, send)

    async def flaky_finish(*args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("UPDATE story", {}, Exception("database is locked"))
        await finish(*args)

    monkeypatch.setattr(worker, "_finish", flaky_finish)
    assert await worker._process_next_task()
    assert attempts == 2
    assert send.await_count == 1
    assert (await task_for(c, campaign.id)).status.value == "succeeded"


@pytest.mark.asyncio
async def test_daily_limit_cannot_be_reset_by_creating_another_campaign(story_case):
    c = story_case
    first = await c.create(name="First", account_ids=[1], daily_limit_per_account=1)
    await c.worker()._process_next_task()
    assert (await task_for(c, first.id)).status.value == "succeeded"
    second = await c.create(name="Second", account_ids=[1], daily_limit_per_account=1)
    assert await c.worker()._process_next_task() is False
    assert (await task_for(c, second.id)).status.value == "queued"


@pytest.mark.asyncio
async def test_story_name_wizards_use_paginated_ready_list_api():
    for handler in (process_story_name, handle_story_viewer_name_input):
        message = SimpleNamespace(text="Campaign", answer=AsyncMock())
        state = SimpleNamespace(update_data=AsyncMock(), set_state=AsyncMock())

        async def ready_lists(page, page_size):
            assert (page, page_size) == (0, 20)
            return [SimpleNamespace(id=1, name="Ready", member_count=10)], 45

        await handler(message, state, SimpleNamespace(list_ready_subscriber_lists=ready_lists))
        keyboard = message.answer.call_args.kwargs["reply_markup"]
        assert any(
            "lists:1" in (button.callback_data or "")
            for row in keyboard.inline_keyboard
            for button in row
        )


def test_all_sender_accounts_remain_accessible_in_wizard_and_campaign():
    accounts = [
        SimpleNamespace(
            id=i, account_id=i, label=f"sender{i}", phone="", enabled=True, views_today=0
        )
        for i in range(1, 46)
    ]
    wizard = story_viewer_accounts_pick_keyboard(accounts, {45}, page=2)
    assert any(
        button.callback_data == "story_view:acc:toggle:45"
        for row in wizard.inline_keyboard
        for button in row
    )
    management = story_viewer_manage_accs_keyboard(1, accounts, page=2)
    assert any(
        button.callback_data == "story_view:toggle_acc:1:45:2"
        for row in management.inline_keyboard
        for button in row
    )


@pytest.mark.asyncio
async def test_dead_sender_is_disabled_and_task_rotates_without_losing_recipient(
    story_case, monkeypatch
):
    c = story_case
    campaign = await c.create()
    original = getattr(c.gateway, c.method)

    async def execute(**kwargs):
        if kwargs["account"].id == 1:
            raise PermanentGatewayError("session revoked", code="account_dead")
        return await original(**kwargs)

    monkeypatch.setattr(c.gateway, c.method, execute)
    worker = c.worker()
    await worker._process_next_task()
    assert (await task_for(c, campaign.id)).status.value == "queued"
    async with c.factory() as session:
        account = await session.get(Account, 1)
        assert account.status == AccountStatus.PAUSED
        assert account.authorized is False
        assert account.cooldown_reason == "account_dead"
    await worker._process_next_task()
    assert (await task_for(c, campaign.id)).status.value == "succeeded"


def test_media_gc_keeps_story_campaign_media(tmp_path):
    media_root = tmp_path / "media"
    media_root.mkdir()
    story = media_root / "story.jpg"
    story.write_bytes(b"fixture")
    database_path = tmp_path / "gc.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE story_tagging_campaigns (media_path TEXT)")
        connection.execute("INSERT INTO story_tagging_campaigns VALUES (?)", (str(story),))
    assert story.resolve() in _media_references(
        f"sqlite+aiosqlite:///{database_path.as_posix()}",
        media_root,
    )


@pytest.mark.asyncio
async def test_disabled_workers_cannot_be_started_or_resumed_through_story_services(story_case):
    c = story_case
    campaign = await c.create()
    await c.service.pause_campaign(campaign.id)
    c.settings.worker_enabled = False
    c.service.start()
    assert c.service._worker is None or c.service._worker.is_stopped()
    assert await c.worker()._process_next_task() is False
    assert await c.service.resume_campaign(campaign.id) is False
    assert (await c.service.get_campaign(campaign.id)).status.value == "paused"
    with pytest.raises(ValueError, match="Фоновые отправки выключены"):
        await c.create(name="Disabled new campaign", auto_start=True)


@pytest.mark.asyncio
async def test_long_cooldown_defers_entire_due_campaign_and_resume_wakes_it(story_case):
    c = story_case
    campaign = await c.create(
        **(dict(max_targets=5) if c.viewing else dict(total_target_tags=5, tags_per_story=1))
    )
    before = utc_now()
    ready = before + timedelta(hours=2)
    async with c.factory() as session:
        first, second = await session.get(Account, 1), await session.get(Account, 2)
        first.cooldown_until = ready
        second.cooldown_until = before + timedelta(hours=4)
        await session.commit()
    assert await c.worker()._process_next_task() is False
    async with c.factory() as session:
        tasks = (
            await session.scalars(select(c.Task).where(c.Task.campaign_id == campaign.id))
        ).all()
        assert len(tasks) == 5
        assert all(ensure_utc(task.scheduled_for) == ready for task in tasks)
        assert all(task.attempt_count == 0 for task in tasks)
    assert ensure_utc((await c.service.get_campaign(campaign.id)).next_run_at) == ready
    await c.service.pause_campaign(campaign.id)
    assert await c.service.resume_campaign(campaign.id, auto_start=False)
    async with c.factory() as session:
        tasks = (
            await session.scalars(select(c.Task).where(c.Task.campaign_id == campaign.id))
        ).all()
        assert all(ensure_utc(task.scheduled_for) < ready for task in tasks)
        assert ensure_utc((await session.get(Account, 1)).cooldown_until) == ready


def test_story_photo_is_vertical_rgb_jpeg_and_retains_square_image():
    image = Image.new("RGBA", (512, 512), (200, 0, 0, 128))
    source = BytesIO()
    image.save(source, "PNG")
    result = prepare_story_photo(source.getvalue())
    with Image.open(BytesIO(result)) as prepared:
        assert prepared.format == "JPEG"
        assert prepared.mode == "RGB"
        assert prepared.size == (1080, 1920)
        assert max(prepared.getpixel((10, 10))) < 5
        red, green, blue = prepared.getpixel((540, 960))
        assert 80 < red < 130
        assert green < 5 and blue < 5


@pytest.mark.parametrize("data", [b"", b"not-an-image", b"\xff\xd8\xff\xe0broken-jpeg"])
def test_invalid_story_photo_fails_before_upload(data):
    with pytest.raises(ContentError):
        prepare_story_photo(data)


@pytest.mark.asyncio
async def test_invalid_saved_story_photo_never_reaches_upload_or_send(tmp_path):
    gateway, account, _, requests, settings = live_gateway(tmp_path)
    media = store_media(settings, b"corrupt", kind="photo", original_name="bad.jpg")
    with pytest.raises(PermanentGatewayError) as error:
        await gateway.publish_story(
            account=account,
            media_path=media.path,
            media_kind="photo",
            caption="",
            tagged_subscribers=[Subscriber(id=1, telegram_user_id=5000, username="alice")],
            idempotency_key="bad-photo",
        )
    assert error.value.code == "content_invalid"
    gateway._prepare_uploaded_media.assert_not_awaited()
    assert not any(isinstance(r, functions.stories.SendStoryRequest) for r in requests)


@pytest.mark.parametrize("width,height,duration", [(1, 1, 0), (720, 1280, 61)])
def test_story_video_rejects_unknown_dimensions_and_excessive_duration(
    monkeypatch, width, height, duration
):
    from telethon import utils

    monkeypatch.setattr(
        utils,
        "get_attributes",
        lambda *args, **kwargs: (
            [types.DocumentAttributeVideo(w=width, h=height, duration=duration)],
            "video/mp4",
        ),
    )
    with pytest.raises(ContentError):
        story_video_attributes("test.mp4")


@pytest.mark.asyncio
async def test_story_video_metadata_is_validated_before_upload(tmp_path, monkeypatch):
    from telethon import utils

    gateway, _, _, _, _ = live_gateway(tmp_path)
    client = SimpleNamespace(
        upload_file=AsyncMock(return_value=types.InputFile(1, 1, "test.mp4", "0" * 32))
    )
    monkeypatch.setattr(
        utils,
        "get_attributes",
        lambda *args, **kwargs: (
            [types.DocumentAttributeVideo(w=1, h=1, duration=0)],
            "video/mp4",
        ),
    )
    with pytest.raises(PermanentGatewayError):
        await gateway._prepare_story_media(client, "test.mp4", "video")
    client.upload_file.assert_not_awaited()
    monkeypatch.setattr(
        utils,
        "get_attributes",
        lambda *args, **kwargs: (
            [types.DocumentAttributeVideo(w=720, h=1280, duration=12.5, supports_streaming=True)],
            "video/mp4",
        ),
    )
    media = await gateway._prepare_story_media(client, "test.mp4", "video")
    assert media.attributes[0].w == 720
    assert media.attributes[0].h == 1280
    assert media.attributes[0].duration == 12.5
    assert media.mime_type == "video/mp4"
