"""Real SQLite limits and competing lease renewal during queue recovery."""

import sqlite3
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.models import (
    DirectCampaign,
    DirectLaunch,
    DirectMessageTask,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.subscriber_invites import SubscriberInviteService


async def queue_case(app, *, inviting, count, cancelled=True):
    Task = SubscriberInviteTask if inviting else DirectMessageTask
    Campaign = SubscriberInviteCampaign if inviting else DirectCampaign
    Launch = SubscriberInviteLaunch if inviting else DirectLaunch
    extra = (
        {"destination_ref": "@owned", "destination_key": "@owned"}
        if inviting
        else {"primary_text": "Hello"}
    )
    snapshot = (
        {"destination_ref_snapshot": "@owned", "destination_key_snapshot": "@owned"}
        if inviting
        else {"primary_text_snapshot": "Hello"}
    )
    async with app.state.database.session_factory() as session:
        audience = SubscriberList(name="Recovery")
        session.add(audience)
        await session.flush()
        campaign = Campaign(name="Recovery", subscriber_list_id=audience.id, **extra)
        session.add(campaign)
        await session.flush()
        launch = Launch(
            campaign_id=campaign.id,
            subscriber_list_id=audience.id,
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=count,
            contact_cooldown_days_snapshot=0,
            account_ids_snapshot_json="[]",
            consent_confirmed_at=utc_now(),
            total_tasks=count,
            cancel_requested=cancelled,
            simulated_snapshot=True,
            **snapshot,
        )
        session.add(launch)
        await session.flush()
        recipients = [Subscriber(telegram_user_id=900000 + i) for i in range(count)]
        session.add_all(recipients)
        await session.flush()
        for recipient in recipients:
            setattr(
                recipient,
                "reserved_invite_launch_id" if inviting else "reserved_direct_launch_id",
                launch.id,
            )
            session.add(
                Task(
                    launch_id=launch.id,
                    subscriber_id=recipient.id,
                    status=DirectMessageTaskStatus.RUNNING,
                    lease_owner="old-owner",
                    lease_token="old-token",
                    lease_until=utc_now() - timedelta(seconds=1),
                )
            )
        await session.commit()
        lid = launch.id
    Service = SubscriberInviteService if inviting else DirectMessageService
    service = Service(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="recovering-worker",
    )
    return SimpleNamespace(service=service, Task=Task, launch_id=lid)


@pytest.mark.parametrize("inviting", [False, True], ids=["direct", "invite"])
async def test_large_cancelled_recovery_does_not_exceed_sqlite_parameter_limit(app, inviting):
    c = await queue_case(app, inviting=inviting, count=1005)

    def limit(connection, record):
        connection.run_async(
            lambda driver: driver._execute(
                driver._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
            )
        )

    await app.state.database.engine.dispose()
    event.listen(app.state.database.engine.sync_engine, "connect", limit)
    assert await c.service._claim_next() is None
    async with app.state.database.session_factory() as session:
        statuses = list(await session.scalars(select(c.Task.status)))
        assert len(statuses) == 1005
        expected = DirectMessageTaskStatus.CANCELLED if inviting else DirectMessageTaskStatus.FAILED
        assert set(statuses) == {expected}
        reserved = (
            Subscriber.reserved_invite_launch_id
            if inviting
            else Subscriber.reserved_direct_launch_id
        )
        assert not await session.scalar(select(Subscriber.id).where(reserved.is_not(None)).limit(1))


async def test_invite_recovery_cannot_overwrite_a_renewal_that_committed_first(app):
    c = await queue_case(app, inviting=True, count=1, cancelled=False)
    factory = app.state.database.session_factory
    async with factory() as session:
        task = await session.scalar(select(c.Task))
        task.delivery_attempted_at = utc_now()
        await session.commit()
        task_id = task.id
    renewed = []
    future = utc_now() + timedelta(hours=1)

    class CompetingSession(AsyncSession):
        async def scalars(self, statement, *args, **kwargs):
            result = await super().scalars(statement, *args, **kwargs)
            if not renewed and "delivery_attempted_at IS NOT NULL" in str(statement):
                try:
                    async with factory() as owner:
                        await owner.execute(text("PRAGMA busy_timeout=1"))
                        changed = await owner.execute(
                            update(c.Task)
                            .where(
                                c.Task.id == task_id,
                                c.Task.lease_token == "old-token",
                                c.Task.status == DirectMessageTaskStatus.RUNNING,
                            )
                            .values(lease_until=future)
                        )
                        await owner.commit()
                        renewed.append(changed.rowcount == 1)
                except OperationalError as exc:
                    assert is_transient_sqlite_lock(exc)
                    renewed.append(False)
            return result

    c.service._session_factory = async_sessionmaker(
        app.state.database.engine, class_=CompetingSession, expire_on_commit=False
    )
    assert await c.service._claim_next() is None
    assert len(renewed) == 1
    async with factory() as session:
        task = await session.get(c.Task, task_id)
        if renewed[0]:
            assert task.status == DirectMessageTaskStatus.RUNNING
            assert ensure_utc(task.lease_until) == future
        else:
            assert task.status == DirectMessageTaskStatus.FAILED
            assert task.error_code == "invite_delivery_unconfirmed"
