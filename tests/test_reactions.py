import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select, update

from proxima_traffic.dialogues import DialogueStep, serialize_dialogue_steps
from proxima_traffic.gateways import PermanentGatewayError
from proxima_traffic.models import (
    Account,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    MembershipStatus,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    Target,
    TargetAccountState,
    utc_now,
)
from proxima_traffic.reactions import ReactionWorker
from proxima_traffic.worker_supervision import WorkerSupervisor


@pytest.mark.asyncio
async def test_dialogue_reactions_wait_for_any_selected_step(app) -> None:
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(
                label=f"dialogue-reactor-{index}",
                authorized=True,
                ownership_confirmed=True,
            )
            for index in range(3)
        ]
        target = Target(chat_ref="@dialogue_reactions", title="Dialogue reactions")
        session.add_all([*accounts, target])
        await session.flush()
        steps = [
            DialogueStep(
                account_id=accounts[index % 3].id,
                text=f"Step {index + 1}",
                reply_to_previous=index > 0,
            )
            for index in range(5)
        ]
        campaign = Campaign(
            name="Dialogue reaction step",
            mode=CampaignMode.DIALOGUE,
            primary_account_id=accounts[0].id,
            target_id=target.id,
            primary_text="Step 1",
            dialogue_steps_json=serialize_dialogue_steps(steps),
            reactions_enabled=True,
            reaction_count=2,
            reaction_emoji="🔥",
            reaction_dialogue_step=5,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="dialogue-reaction-step",
            status=RunStatus.SUCCEEDED,
            dialogue_steps_snapshot_json=serialize_dialogue_steps(steps),
        )
        session.add(run)
        await session.flush()

        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            int(accounts[0].id),
            404,
            dialogue_step=4,
        )
        assert list(await session.scalars(select(ReactionTask))) == []

        source_id = int(accounts[1].id)
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            source_id,
            505,
            dialogue_step=5,
        )
        await session.commit()

    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ReactionTask).order_by(ReactionTask.id)))
    assert len(tasks) == 2
    assert {task.message_id for task in tasks} == {505}
    assert {task.source_account_id for task in tasks} == {source_id}
    assert source_id not in {task.reactor_account_id for task in tasks}


@pytest.mark.asyncio
async def test_reactions_exclude_author_and_persist_the_actual_fallback_emoji(
    app,
    monkeypatch,
) -> None:
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(label=f"reactor-{index}", authorized=True, ownership_confirmed=True)
            for index in range(1, 4)
        ]
        target = Target(chat_ref="@reaction_test", title="Reaction test", approved_by_operator=True)
        session.add_all([*accounts, target])
        await session.flush()
        campaign = Campaign(
            name="Reaction campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=accounts[0].id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
            reaction_count=2,
            reaction_emoji="🔥",
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=account.id)
                for account in accounts
            ]
        )
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=accounts[0].id,
            request_key="reaction-test-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            accounts[0].id,
            321,
            dialogue_step=1,
        )
        await session.commit()

    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ReactionTask).order_by(ReactionTask.id)))
        assert {task.reactor_account_id for task in tasks} == {accounts[1].id, accounts[2].id}
        assert {task.emoji for task in tasks} == {"🔥"}
        await session.execute(
            update(ReactionTask).values(scheduled_for=utc_now() - timedelta(seconds=1))
        )
        await session.commit()

    async def send_reaction_with_fallback(**kwargs: object) -> str:
        assert kwargs["emoji"] == "🔥"
        return "👍"

    monkeypatch.setattr(app.state.gateway, "send_reaction", send_reaction_with_fallback)
    assert await app.state.reactions.process_next() is True
    assert await app.state.reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ReactionTask).order_by(ReactionTask.id)))
        assert [task.status for task in tasks] == [
            ReactionTaskStatus.SUCCEEDED,
            ReactionTaskStatus.SUCCEEDED,
        ]
        assert {task.emoji for task in tasks} == {"👍"}


@pytest.mark.asyncio
async def test_live_reactions_are_not_queued_for_an_unverified_reactor(app) -> None:
    app.state.settings.dry_run = False
    async with app.state.database.session_factory() as session:
        source = Account(label="live-source", authorized=True, ownership_confirmed=True)
        unverified = Account(label="live-unverified", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@live_reactions", title="Live reactions")
        session.add_all([source, unverified, target])
        await session.flush()
        campaign = Campaign(
            name="Live reaction preflight",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
            reaction_count=1,
            reaction_emoji="🔥",
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=source.id),
                CampaignAccount(campaign_id=campaign.id, account_id=unverified.id),
            ]
        )
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="live-reaction-unverified",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()

        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            int(source.id),
            991,
            dialogue_step=1,
        )
        await session.commit()

    async with app.state.database.session_factory() as session:
        assert list(await session.scalars(select(ReactionTask))) == []


@pytest.mark.asyncio
async def test_live_reactions_queue_a_fresh_auto_join_route(app) -> None:
    app.state.settings.dry_run = False
    app.state.settings.account_auto_join = True
    async with app.state.database.session_factory() as session:
        source = Account(label="auto-join-source", authorized=True, ownership_confirmed=True)
        reactor = Account(label="auto-join-reactor", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@auto_join_reactions", title="Auto join reactions")
        session.add_all([source, reactor, target])
        await session.flush()
        campaign = Campaign(
            name="Auto join reaction campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
            reaction_count=1,
            reaction_emoji="🔥",
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=source.id),
                CampaignAccount(campaign_id=campaign.id, account_id=reactor.id),
            ]
        )
        route = TargetAccountState(
            target_id=target.id,
            account_id=reactor.id,
            membership_status=MembershipStatus.NOT_MEMBER,
            can_send=False,
            membership_checked_at=utc_now(),
            membership_error_code="target_not_joined",
        )
        session.add(route)
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="live-reaction-auto-join",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            int(source.id),
            992,
            dialogue_step=1,
        )
        await session.commit()

    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ReactionTask)))
    assert len(tasks) == 1
    assert tasks[0].reactor_account_id == reactor.id
    assert app.state.reactions._route_is_ready(route) is True  # noqa: SLF001


@pytest.mark.asyncio
async def test_reaction_pacing_deferrals_do_not_consume_delivery_attempts(app, monkeypatch) -> None:
    async with app.state.database.session_factory() as session:
        source = Account(label="paced-reaction-source", authorized=True, ownership_confirmed=True)
        reactor = Account(label="paced-reactor", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@paced_reactions", title="Paced reactions")
        session.add_all([source, reactor, target])
        await session.flush()
        campaign = Campaign(
            name="Paced reactions",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="paced-reaction-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        task = ReactionTask(
            campaign_id=campaign.id,
            run_id=run.id,
            target_id=target.id,
            source_account_id=source.id,
            reactor_account_id=reactor.id,
            message_id=333,
            emoji="🔥",
            scheduled_for=utc_now() - timedelta(seconds=1),
        )
        session.add(task)
        await session.commit()
        task_id = int(task.id)

    monkeypatch.setattr(
        app.state.campaign_runner,
        "account_resume_at",
        lambda _account: utc_now() + timedelta(minutes=3),
    )
    assert await app.state.reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.get(ReactionTask, task_id)
        assert task is not None
        assert task.status == ReactionTaskStatus.QUEUED
        assert task.attempt_count == 0
        assert task.error_code == "account_pacing"
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    monkeypatch.setattr(app.state.campaign_runner, "account_resume_at", lambda _account: None)
    assert await app.state.reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.get(ReactionTask, task_id)
        assert task is not None
        assert task.status == ReactionTaskStatus.SUCCEEDED
        assert task.attempt_count == 1


@pytest.mark.asyncio
async def test_reaction_worker_recovers_expired_lease(app) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="expired-reactor", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@expired_reaction", title="Expired reaction")
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Expired reaction campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="test",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="expired-reaction-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        session.add_all(
            [
                ReactionTask(
                    campaign_id=campaign.id,
                    run_id=run.id,
                    target_id=target.id,
                    source_account_id=account.id,
                    reactor_account_id=account.id,
                    message_id=77,
                    emoji="👍",
                    status=ReactionTaskStatus.RUNNING,
                    lease_owner="dead-worker",
                    lease_token="expired",
                    lease_until=utc_now() - timedelta(seconds=1),
                ),
                ReactionTask(
                    campaign_id=campaign.id,
                    run_id=run.id,
                    target_id=target.id,
                    source_account_id=account.id,
                    reactor_account_id=account.id,
                    message_id=78,
                    emoji="👍",
                    status=ReactionTaskStatus.RUNNING,
                    cancel_requested=True,
                    lease_owner="dead-worker",
                    lease_token="expired-cancelled",
                    lease_until=utc_now() - timedelta(seconds=1),
                ),
            ]
        )
        await session.commit()

    assert await app.state.reactions.recover_expired_leases() == 2
    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ReactionTask).order_by(ReactionTask.id)))
        assert tasks[0].status == ReactionTaskStatus.QUEUED
        assert tasks[0].lease_token is None
        assert tasks[0].error_code == "lease_expired"
        assert tasks[1].status == ReactionTaskStatus.FAILED
        assert tasks[1].lease_token is None
        assert tasks[1].error_code == "cancelled_outcome_unknown"


@pytest.mark.asyncio
async def test_reaction_worker_marks_an_unexpected_step_crash_as_terminal(app, monkeypatch) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="crashed-reactor", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@crashed_reaction", title="Crashed reaction")
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Crashed reaction campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="test",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="crashed-reaction-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        task = ReactionTask(
            campaign_id=campaign.id,
            run_id=run.id,
            target_id=target.id,
            source_account_id=account.id,
            reactor_account_id=account.id,
            message_id=88,
            emoji="👍",
            scheduled_for=utc_now() - timedelta(seconds=1),
        )
        session.add(task)
        await session.commit()
        task_id = int(task.id)

    async def crash(*args: object) -> None:
        del args
        raise RuntimeError("unexpected reaction crash")

    monkeypatch.setattr(app.state.reactions, "_process", crash)
    assert await app.state.reactions.process_next() is True

    async with app.state.database.session_factory() as session:
        task = await session.get(ReactionTask, task_id)
        assert task is not None
        assert task.status == ReactionTaskStatus.FAILED
        assert task.error_code == "reaction_failed"
        assert task.lease_token is None


@pytest.mark.asyncio
async def test_reaction_chat_ban_is_persisted_and_excluded_from_future_tasks(
    app,
    monkeypatch,
) -> None:
    async with app.state.database.session_factory() as session:
        source = Account(label="reaction-source", authorized=True, ownership_confirmed=True)
        reactor = Account(label="banned-reactor", authorized=True, ownership_confirmed=True)
        target = Target(chat_ref="@reaction_ban", title="Reaction ban")
        session.add_all([source, reactor, target])
        await session.flush()
        campaign = Campaign(
            name="Ban-aware reactions",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
            reaction_count=1,
            reaction_emoji="🔥",
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=source.id),
                CampaignAccount(campaign_id=campaign.id, account_id=reactor.id),
            ]
        )
        first_run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            sender_pool_snapshot_json=f"[{source.id}, {reactor.id}]",
            request_key="reaction-ban-first",
            status=RunStatus.SUCCEEDED,
        )
        session.add(first_run)
        await session.flush()
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            first_run,
            campaign,
            target,
            int(source.id),
            501,
            dialogue_step=1,
        )
        await session.execute(
            update(ReactionTask).values(scheduled_for=utc_now() - timedelta(seconds=1))
        )
        await session.commit()
        source_id = int(source.id)
        reactor_id = int(reactor.id)
        target_id = int(target.id)
        campaign_id = int(campaign.id)

    async def banned_reaction(**kwargs: object) -> None:
        del kwargs
        raise PermanentGatewayError(
            "account was banned in chat",
            code="account_banned_in_chat",
        )

    monkeypatch.setattr(app.state.gateway, "send_reaction", banned_reaction)
    assert await app.state.reactions.process_next() is True

    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == reactor_id,
                TargetAccountState.target_id == target_id,
            )
        )
        assert task is not None and task.status == ReactionTaskStatus.SKIPPED
        assert task.error_code == "account_banned_in_chat"
        assert state is not None and state.send_blocked is True

        campaign = await session.get(Campaign, campaign_id)
        target = await session.get(Target, target_id)
        assert campaign is not None and target is not None
        second_run = CampaignRun(
            campaign_id=campaign_id,
            target_id=target_id,
            sender_account_id=source_id,
            sender_pool_snapshot_json=f"[{source_id}, {reactor_id}]",
            request_key="reaction-ban-second",
            status=RunStatus.SUCCEEDED,
        )
        session.add(second_run)
        await session.flush()
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            second_run,
            campaign,
            target,
            source_id,
            502,
            dialogue_step=1,
        )
        await session.commit()
        task_count = len(list(await session.scalars(select(ReactionTask))))
    assert task_count == 1


@pytest.mark.asyncio
async def test_campaign_reaction_fails_over_to_an_unused_reactor(app, monkeypatch) -> None:
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(
                label=f"reaction-failover-{index}",
                authorized=True,
                ownership_confirmed=True,
            )
            for index in range(3)
        ]
        target = Target(chat_ref="@reaction_failover", title="Reaction failover")
        session.add_all([*accounts, target])
        await session.flush()
        campaign = Campaign(
            name="Reaction failover campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=accounts[0].id,
            target_id=target.id,
            primary_text="test",
            reactions_enabled=True,
            reaction_count=1,
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=account.id)
                for account in accounts
            ]
        )
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=accounts[0].id,
            sender_pool_snapshot_json=f"[{accounts[0].id}, {accounts[1].id}, {accounts[2].id}]",
            request_key="reaction-failover-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            int(accounts[0].id),
            777,
            dialogue_step=1,
        )
        await session.flush()
        task = await session.scalar(select(ReactionTask))
        assert task is not None
        first_reactor_id = int(task.reactor_account_id)
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    calls: list[int] = []

    async def react(**kwargs: object) -> None:
        account = kwargs["account"]
        calls.append(int(account.id))
        if int(account.id) == first_reactor_id:
            raise PermanentGatewayError(
                "account is blocked in chat",
                code="account_banned_in_chat",
            )

    monkeypatch.setattr(app.state.gateway, "send_reaction", react)
    assert await app.state.reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        assert task is not None and task.status == ReactionTaskStatus.QUEUED
        assert int(task.reactor_account_id) != first_reactor_id
        replacement_id = int(task.reactor_account_id)
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await app.state.reactions.process_next() is True
    assert calls == [first_reactor_id, replacement_id]
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == first_reactor_id,
            )
        )
        assert task is not None and task.status == ReactionTaskStatus.SUCCEEDED
        assert state is not None and state.send_blocked is True


@pytest.mark.asyncio
async def test_reaction_worker_runs_under_the_supervisor(app) -> None:
    worker = app.state.reactions
    supervisor = WorkerSupervisor(restart_delay_seconds=0.01)
    task = supervisor.start("reactions", worker.run_forever)
    for _ in range(50):
        snapshot = supervisor.snapshots()[0]
        if snapshot.last_success_at is not None:
            break
        await asyncio.sleep(0.01)

    assert supervisor.snapshots()[0].state == "running"
    worker.stop()
    supervisor.stop()
    await task


@pytest.mark.asyncio
async def test_campaign_scoped_reaction_worker_claims_only_selected_campaign(app) -> None:
    now = utc_now()
    async with app.state.database.session_factory() as session:
        source = Account(label="scoped-reaction-source", ownership_confirmed=True)
        reactor = Account(label="scoped-reaction-reactor", ownership_confirmed=True)
        target = Target(chat_ref="@scoped_reaction_target", title="Scoped reaction target")
        session.add_all([source, reactor, target])
        await session.flush()
        older_campaign = Campaign(
            name="scoped reaction older campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="Older",
        )
        scoped_campaign = Campaign(
            name="scoped reaction selected campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="Selected",
        )
        session.add_all([older_campaign, scoped_campaign])
        await session.flush()
        older_run = CampaignRun(
            campaign_id=older_campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="scoped-reaction-older",
            status=RunStatus.SUCCEEDED,
        )
        selected_run = CampaignRun(
            campaign_id=scoped_campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="scoped-reaction-selected",
            status=RunStatus.SUCCEEDED,
        )
        session.add_all([older_run, selected_run])
        await session.flush()
        older_task = ReactionTask(
            campaign_id=older_campaign.id,
            run_id=older_run.id,
            target_id=target.id,
            source_account_id=source.id,
            reactor_account_id=reactor.id,
            message_id=801,
            emoji="🔥",
            scheduled_for=now - timedelta(minutes=2),
        )
        selected_task = ReactionTask(
            campaign_id=scoped_campaign.id,
            run_id=selected_run.id,
            target_id=target.id,
            source_account_id=source.id,
            reactor_account_id=reactor.id,
            message_id=802,
            emoji="🔥",
            scheduled_for=now - timedelta(minutes=1),
        )
        session.add_all([older_task, selected_task])
        await session.commit()
        selected_campaign_id = int(scoped_campaign.id)
        older_task_id = int(older_task.id)
        selected_task_id = int(selected_task.id)

    worker = ReactionWorker(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="scoped-reaction-worker",
        campaign_id_scope=frozenset({selected_campaign_id}),
    )
    claimed = await worker._claim_next()  # noqa: SLF001 - scope is enforced at claim time

    assert claimed is not None and claimed[0] == selected_task_id
    async with app.state.database.session_factory() as session:
        older = await session.get(ReactionTask, older_task_id)
        selected = await session.get(ReactionTask, selected_task_id)
    assert older is not None and older.status == ReactionTaskStatus.QUEUED
    assert selected is not None and selected.status == ReactionTaskStatus.RUNNING


@pytest.mark.asyncio
async def test_independent_broadcast_uses_campaign_pool_for_reactions(app) -> None:
    async with app.state.database.session_factory() as session:
        source = Account(label="independent-reaction-source", ownership_confirmed=True)
        reactor = Account(label="independent-reaction-reactor", ownership_confirmed=True)
        target = Target(chat_ref="@independent_reaction_target", title="Independent target")
        session.add_all([source, reactor, target])
        await session.flush()
        campaign = Campaign(
            name="independent reaction campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=source.id,
            target_id=target.id,
            primary_text="Test",
            sender_mode=BroadcastSenderMode.INDEPENDENT,
            reactions_enabled=True,
            reaction_count=1,
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                CampaignAccount(campaign_id=campaign.id, account_id=source.id),
                CampaignAccount(campaign_id=campaign.id, account_id=reactor.id),
            ]
        )
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            sender_pool_snapshot_json=f"[{source.id}]",
            sender_mode_snapshot=BroadcastSenderMode.INDEPENDENT,
            request_key="independent-reaction-run",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()

        created = await app.state.campaign_runner._schedule_reactions(  # noqa: SLF001
            session,
            run,
            campaign,
            target,
            int(source.id),
            901,
            dialogue_step=1,
        )
        await session.commit()

    assert created == 1
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
    assert task is not None and task.reactor_account_id == reactor.id
