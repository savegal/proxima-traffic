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
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
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
        completed = await session.scalar(
            select(ReactionTask).where(ReactionTask.id == reaction_id)
        )
        assert completed is not None
        assert completed.status == ReactionTaskStatus.SUCCEEDED
        assert completed.error_code == "completed_during_cancel"
        assert completed.lease_token is None
    await app.state.database.dispose()


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
