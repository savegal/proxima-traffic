import asyncio
from datetime import timedelta

import httpx
from sqlalchemy import select

from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.config import Settings
from proxima_traffic.gateways import SentMessage
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignMode,
    CampaignRun,
    DirectCampaign,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
    Target,
    utc_now,
)


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def send_message(self, **kwargs: object) -> SentMessage:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return SentMessage(message_id=700 + self.calls)


class BlockingReactionGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_reaction(self, **kwargs: object) -> None:
        del kwargs
        self.started.set()
        await self.release.wait()


async def test_cancellation_during_primary_step_is_terminal(tmp_path) -> None:
    gateway = BlockingGateway()
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'cancel.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        campaign_id = await _create_ready_campaign(client, headers)
        queued = await client.post(
            f"/api/v1/campaigns/{campaign_id}/runs",
            headers=headers,
            json={"request_key": "cancel-during-primary"},
        )
        assert queued.status_code == 202
        run_id = queued.json()[0]["id"]

        worker_task = asyncio.create_task(app.state.queue_worker.process_next())
        await gateway.started.wait()
        cancellation = await app.state.run_queue.cancel(run_id)
        assert cancellation.status.value == "running"
        assert cancellation.cancel_requested is True

        gateway.release.set()
        result = await worker_task
        assert result.status.value == "cancelled"
        assert result.primary_message_id == 701
        assert gateway.calls == 1
        assert await app.state.queue_worker.process_next() is None

        details = await client.get(f"/api/v1/runs/{run_id}", headers=headers)
        body = details.json()
        assert body["status"] == "cancelled"
        assert len(body["actions"]) == 1
        assert body["actions"][0]["kind"] == "primary_message"

    await app.state.database.dispose()


async def test_paused_campaign_keeps_queued_runs_for_resume(tmp_path) -> None:
    gateway = BlockingGateway()
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'pause.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        campaign_id = await _create_ready_campaign(client, headers)
        queued = await client.post(
            f"/api/v1/campaigns/{campaign_id}/runs",
            headers=headers,
            json={"request_key": "pause-and-resume"},
        )
        run_id = queued.json()[0]["id"]
        repository = AdminRepository(app.state.database.session_factory)

        paused = await repository.toggle_campaign(campaign_id)
        assert paused.enabled is False
        assert await app.state.queue_worker.process_next() is None

        details = await client.get(f"/api/v1/runs/{run_id}", headers=headers)
        assert details.json()["status"] == "queued"
        assert details.json()["error_code"] is None

        resumed = await repository.toggle_campaign(campaign_id)
        assert resumed.enabled is True
        gateway.release.set()
        processed = await app.state.queue_worker.process_next()
        assert processed is not None
        assert processed.primary_message_id == 701
        assert processed.status.value == "queued"

    await app.state.database.dispose()


async def test_global_stop_preserves_running_reaction_lease_until_telegram_returns(
    tmp_path,
) -> None:
    gateway = BlockingReactionGateway()
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'reaction-cancel.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    async with app.state.database.session_factory() as session:
        source = Account(
            label="reaction-source",
            authorized=True,
            ownership_confirmed=True,
        )
        reactor = Account(
            label="reaction-reactor",
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@reaction_cancel", title="Reaction cancel")
        session.add_all([source, reactor, target])
        await session.flush()
        campaign = Campaign(
            name="Reaction cancel",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="Published",
            reactions_enabled=True,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="reaction-cancel-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        reaction = ReactionTask(
            campaign_id=campaign.id,
            run_id=run.id,
            target_id=target.id,
            source_account_id=source.id,
            reactor_account_id=reactor.id,
            message_id=500,
            emoji="👍",
            scheduled_for=utc_now() - timedelta(seconds=1),
        )
        session.add(reaction)
        await session.commit()
        reaction_id = int(reaction.id)

    processing = asyncio.create_task(app.state.reactions.process_next())
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    assert await app.state.run_queue.cancel_all() == 1
    async with app.state.database.session_factory() as session:
        running = await session.get(ReactionTask, reaction_id)
        assert running is not None
        assert running.status == ReactionTaskStatus.RUNNING
        assert running.cancel_requested is True
        assert running.lease_token is not None

    gateway.release.set()
    assert await processing is True
    async with app.state.database.session_factory() as session:
        completed = await session.scalar(select(ReactionTask).where(ReactionTask.id == reaction_id))
        assert completed is not None
        assert completed.status == ReactionTaskStatus.SUCCEEDED
        assert completed.error_code == "completed_during_cancel"
        assert completed.lease_token is None
    await app.state.database.dispose()


async def test_global_stop_cancels_direct_messages_and_subscriber_invites(app) -> None:
    """The emergency stop must cover every delivery queue, not only chat posts."""

    now = utc_now()
    async with app.state.database.session_factory() as session:
        subscriber_list = SubscriberList(name="Global stop list", status=SubscriberListStatus.READY)
        direct_subscriber = Subscriber(telegram_user_id=81001)
        invite_subscriber = Subscriber(telegram_user_id=81002)
        session.add_all([subscriber_list, direct_subscriber, invite_subscriber])
        await session.flush()

        direct_campaign = DirectCampaign(
            name="Global stop direct",
            subscriber_list_id=subscriber_list.id,
            primary_text="Message",
        )
        invite_campaign = SubscriberInviteCampaign(
            name="Global stop invite",
            subscriber_list_id=subscriber_list.id,
            destination_ref="@owned_channel",
            destination_key="owned_channel",
        )
        session.add_all([direct_campaign, invite_campaign])
        await session.flush()

        direct_launch = DirectLaunch(
            campaign_id=direct_campaign.id,
            subscriber_list_id=subscriber_list.id,
            status=DirectLaunchStatus.QUEUED,
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=1,
            contact_cooldown_days_snapshot=30,
            account_ids_snapshot_json="[]",
            primary_text_snapshot="Message",
            consent_confirmed_at=now,
        )
        invite_launch = SubscriberInviteLaunch(
            campaign_id=invite_campaign.id,
            subscriber_list_id=subscriber_list.id,
            status=DirectLaunchStatus.QUEUED,
            destination_ref_snapshot="@owned_channel",
            destination_key_snapshot="owned_channel",
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=1,
            contact_cooldown_days_snapshot=30,
            account_ids_snapshot_json="[]",
            consent_confirmed_at=now,
        )
        session.add_all([direct_launch, invite_launch])
        await session.flush()
        direct_subscriber.reserved_direct_launch_id = direct_launch.id
        invite_subscriber.reserved_invite_launch_id = invite_launch.id
        session.add_all(
            [
                DirectMessageTask(
                    launch_id=direct_launch.id,
                    subscriber_id=direct_subscriber.id,
                    scheduled_for=now,
                ),
                SubscriberInviteTask(
                    launch_id=invite_launch.id,
                    subscriber_id=invite_subscriber.id,
                    scheduled_for=now,
                ),
            ]
        )
        await session.commit()

    summary = await app.state.run_queue.cancel_all_with_summary()
    assert summary.direct_launches == 1
    assert summary.direct_message_tasks == 1
    assert summary.subscriber_invite_launches == 1
    assert summary.subscriber_invite_tasks == 1

    async with app.state.database.session_factory() as session:
        direct_task = await session.scalar(select(DirectMessageTask))
        invite_task = await session.scalar(select(SubscriberInviteTask))
        stored_direct_launch = await session.get(DirectLaunch, direct_launch.id)
        stored_invite_launch = await session.get(SubscriberInviteLaunch, invite_launch.id)
        stored_direct_subscriber = await session.get(Subscriber, direct_subscriber.id)
        stored_invite_subscriber = await session.get(Subscriber, invite_subscriber.id)
        assert direct_task is not None and direct_task.status == DirectMessageTaskStatus.CANCELLED
        assert invite_task is not None and invite_task.status == DirectMessageTaskStatus.CANCELLED
        assert stored_direct_launch is not None and stored_direct_launch.cancel_requested is True
        assert stored_direct_launch.status == DirectLaunchStatus.CANCELLED
        assert stored_invite_launch is not None and stored_invite_launch.cancel_requested is True
        assert stored_invite_launch.status == DirectLaunchStatus.CANCELLED
        assert stored_direct_subscriber is not None
        assert stored_direct_subscriber.reserved_direct_launch_id is None
        assert stored_invite_subscriber is not None
        assert stored_invite_subscriber.reserved_invite_launch_id is None


async def test_expired_cancelled_campaign_run_has_unknown_outcome(app) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="expired-cancelled-campaign-sender",
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@expired_campaign", title="Expired campaign")
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Expired cancelled campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Message",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="expired-cancelled-campaign",
            status=RunStatus.RUNNING,
            attempt_count=1,
            max_attempts=3,
            lease_owner="expired-worker",
            lease_token="expired-campaign-token",
            lease_until=utc_now() - timedelta(seconds=1),
            cancel_requested=True,
        )
        session.add(run)
        await session.commit()
        run_id = int(run.id)

    assert await app.state.run_queue.recover_expired_leases() == 1
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run_id)
        assert stored is not None
        assert stored.status == RunStatus.FAILED
        assert stored.error_code == "cancelled_outcome_unknown"
        assert stored.lease_token is None


async def _create_ready_campaign(
    client: httpx.AsyncClient,
    headers: dict[str, str],
) -> int:
    account_ids: list[int] = []
    for index in (1, 2):
        response = await client.post(
            "/api/v1/accounts",
            headers=headers,
            json={
                "label": f"cancel-{index}",
                "telegram_user_id": 900 + index,
                "ownership_confirmed": True,
            },
        )
        account_ids.append(response.json()["id"])
    target = await client.post(
        "/api/v1/targets",
        headers=headers,
        json={"chat_ref": "@cancel_test", "title": "Cancel test"},
    )
    target_id = target.json()["id"]
    await client.post(f"/api/v1/targets/{target_id}/approve", headers=headers)
    campaign = await client.post(
        "/api/v1/campaigns",
        headers=headers,
        json={
            "name": "cancellation-race",
            "primary_account_id": account_ids[0],
            "reply_account_id": account_ids[1],
            "target_id": target_id,
            "primary_text": "First",
            "reply_text": "Reply",
        },
    )
    return campaign.json()["id"]
