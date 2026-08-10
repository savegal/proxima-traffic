from datetime import timedelta

import pytest
from sqlalchemy import select

from proxima_traffic.gateways import (
    ParticipantMessage,
    ParticipantMessageScan,
    PermanentGatewayError,
)
from proxima_traffic.models import (
    Account,
    ParticipantReactionRule,
    ParticipantReactionTargetState,
    ParticipantReactionTask,
    ParticipantReactionTaskStatus,
    Target,
    TargetAccountState,
    TargetList,
    TargetListItem,
    utc_now,
)


@pytest.mark.asyncio
async def test_participant_reactions_prime_history_exclude_own_and_deliver_one(app) -> None:
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(
                label=f"participant-reactor-{index}",
                telegram_user_id=1000 + index,
                authorized=True,
                ownership_confirmed=True,
            )
            for index in range(2)
        ]
        target = Target(
            chat_ref="@participant_reaction_test",
            title="Participant reaction test",
        )
        target_list = TargetList(name="Participant reaction chats")
        session.add_all([*accounts, target, target_list])
        await session.flush()
        session.add(
            TargetListItem(target_list_id=target_list.id, target_id=target.id, enabled=True)
        )
        await session.commit()
        account_ids = [int(account.id) for account in accounts]
        target_id = int(target.id)
        target_list_id = int(target_list.id)

    observed = [ParticipantMessage(message_id=100, sender_id=9999)]

    scan_calls: list[dict[str, object]] = []

    async def scan_participant_messages(**kwargs: object) -> ParticipantMessageScan:
        scan_calls.append(kwargs)
        return ParticipantMessageScan(messages=tuple(observed))

    app.state.gateway.scan_participant_messages = scan_participant_messages
    rule = await app.state.participant_reactions.create_rule(
        name="Participant reactions test",
        target_list_id=target_list_id,
        account_ids=account_ids,
        emoji="👍",
        sample_numerator=10,
        sample_denominator=10,
    )

    await app.state.participant_reactions.scan_rule(int(rule.id))
    assert scan_calls[0]["bootstrap"] is True
    async with app.state.database.session_factory() as session:
        state = await session.scalar(
            select(ParticipantReactionTargetState).where(
                ParticipantReactionTargetState.rule_id == rule.id,
                ParticipantReactionTargetState.target_id == target_id,
            )
        )
        assert state is not None and state.primed and state.last_seen_message_id == 100
        assert not list(await session.scalars(select(ParticipantReactionTask)))

    observed[:] = [
        ParticipantMessage(message_id=101, sender_id=1001),  # a selected service account
        ParticipantMessage(message_id=102, sender_id=9998),
    ]
    await app.state.participant_reactions.scan_rule(int(rule.id))
    assert scan_calls[1]["bootstrap"] is False
    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ParticipantReactionTask)))
        assert len(tasks) == 1
        assert tasks[0].message_id == 102
        assert tasks[0].source_sender_id == 9998
        tasks[0].scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await app.state.participant_reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        status = await session.scalar(select(ParticipantReactionTask.status))
        assert status == ParticipantReactionTaskStatus.SUCCEEDED

    observed[:] = [ParticipantMessage(message_id=103, sender_id=9997)]
    await app.state.participant_reactions.scan_rule(int(rule.id))
    async with app.state.database.session_factory() as session:
        task = await session.scalar(
            select(ParticipantReactionTask).where(ParticipantReactionTask.message_id == 103)
        )
        assert task is not None
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        list_item = await session.scalar(
            select(TargetListItem).where(
                TargetListItem.target_list_id == target_list_id,
                TargetListItem.target_id == target_id,
            )
        )
        assert list_item is not None
        list_item.enabled = False
        await session.commit()

    assert await app.state.participant_reactions.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.scalar(
            select(ParticipantReactionTask).where(ParticipantReactionTask.message_id == 103)
        )
        assert task is not None
        assert task.status == ParticipantReactionTaskStatus.CANCELLED
        assert task.error_code == "target_removed_from_list"

    async with app.state.database.session_factory() as session:
        replacement_target = Target(
            chat_ref="@participant_reaction_replacement",
            title="Participant reaction replacement",
        )
        replacement_list = TargetList(name="Participant reaction replacement chats")
        session.add_all([replacement_target, replacement_list])
        await session.flush()
        session.add(
            TargetListItem(
                target_list_id=replacement_list.id,
                target_id=replacement_target.id,
                enabled=True,
            )
        )
        await session.commit()
        replacement_list_id = int(replacement_list.id)

    updated = await app.state.participant_reactions.update_rule(
        int(rule.id),
        target_list_id=replacement_list_id,
        account_ids=account_ids,
        emoji="🔥",
        sample_numerator=3,
        sample_denominator=10,
    )
    assert updated.target_list_id == replacement_list_id
    assert updated.emoji == "🔥"
    assert (updated.sample_numerator, updated.sample_denominator) == (3, 10)
    async with app.state.database.session_factory() as session:
        states = list(
            await session.scalars(
                select(ParticipantReactionTargetState).where(
                    ParticipantReactionTargetState.rule_id == rule.id
                )
            )
        )
        assert states == []


@pytest.mark.asyncio
async def test_participant_reaction_worker_marks_an_unexpected_step_crash_as_terminal(
    app, monkeypatch
) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="crashed-participant-reactor",
            telegram_user_id=7001,
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@crashed_participant_reaction", title="Crashed participant")
        target_list = TargetList(name="Crashed participant chats")
        session.add_all([account, target, target_list])
        await session.flush()
        session.add(
            TargetListItem(target_list_id=target_list.id, target_id=target.id, enabled=True)
        )
        rule = ParticipantReactionRule(
            name="Crashed participant rule",
            target_list_id=target_list.id,
            account_ids_json=f"[{account.id}]",
            emoji="👍",
            sample_numerator=1,
            sample_denominator=1,
            approved_by_operator=True,
        )
        session.add(rule)
        await session.flush()
        task = ParticipantReactionTask(
            rule_id=rule.id,
            target_id=target.id,
            reactor_account_id=account.id,
            message_id=500,
            source_sender_id=8001,
            emoji="👍",
            scheduled_for=utc_now() - timedelta(seconds=1),
        )
        session.add(task)
        await session.commit()
        task_id = int(task.id)

    async def crash(*args: object) -> None:
        del args
        raise RuntimeError("unexpected participant reaction crash")

    monkeypatch.setattr(app.state.participant_reactions, "_process", crash)
    assert await app.state.participant_reactions.process_next() is True

    async with app.state.database.session_factory() as session:
        task = await session.get(ParticipantReactionTask, task_id)
        assert task is not None
        assert task.status == ParticipantReactionTaskStatus.FAILED
        assert task.error_code == "participant_reaction_failed"
        assert task.lease_token is None


@pytest.mark.asyncio
async def test_participant_scan_fails_over_per_chat_and_excludes_every_owned_account(app) -> None:
    async with app.state.database.session_factory() as session:
        selected = [
            Account(
                label=f"selected-{index}",
                telegram_user_id=8100 + index,
                authorized=True,
                ownership_confirmed=True,
            )
            for index in range(2)
        ]
        unselected_owned = Account(
            label="unselected-owned",
            telegram_user_id=8199,
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@participant_failover", title="Participant failover")
        target_list = TargetList(name="Participant failover chats")
        session.add_all([*selected, unselected_owned, target, target_list])
        await session.flush()
        session.add(
            TargetListItem(target_list_id=target_list.id, target_id=target.id, enabled=True)
        )
        await session.commit()
        selected_ids = [int(account.id) for account in selected]
        target_id = int(target.id)
        target_list_id = int(target_list.id)

    scans: list[int] = []
    messages = [ParticipantMessage(message_id=100, sender_id=9000)]

    async def scan_participant_messages(**kwargs: object) -> ParticipantMessageScan:
        account = kwargs["account"]
        scans.append(int(account.id))
        if int(account.id) == selected_ids[0]:
            raise PermanentGatewayError(
                "admin rights are missing",
                code="account_missing_admin_rights",
            )
        return ParticipantMessageScan(messages=tuple(messages))

    app.state.gateway.scan_participant_messages = scan_participant_messages
    rule = await app.state.participant_reactions.create_rule(
        name="Per-chat participant failover",
        target_list_id=target_list_id,
        account_ids=selected_ids,
        emoji="👍",
        sample_numerator=1,
        sample_denominator=1,
    )
    await app.state.participant_reactions.scan_rule(int(rule.id))
    assert scans == selected_ids

    async with app.state.database.session_factory() as session:
        blocked = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == selected_ids[0],
                TargetAccountState.target_id == target_id,
            )
        )
        assert blocked is not None and blocked.send_blocked is True
        assert blocked.blocked_reason == "account_missing_admin_rights"

    scans.clear()
    messages[:] = [
        ParticipantMessage(message_id=101, sender_id=8199),
        ParticipantMessage(message_id=102, sender_id=9001),
    ]
    await app.state.participant_reactions.scan_rule(int(rule.id))
    assert scans == [selected_ids[1]]
    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ParticipantReactionTask)))
        assert [task.message_id for task in tasks] == [102]
        assert tasks[0].reactor_account_id == selected_ids[1]

    await app.state.campaign_runner.record_observed_target_failure(
        selected_ids[1],
        target_id,
        PermanentGatewayError(
            "admin rights are missing",
            code="account_missing_admin_rights",
        ),
    )
    async with app.state.database.session_factory() as session:
        stored_target = await session.get(Target, target_id)
        assert stored_target is not None
        assert stored_target.quality_status != "bad"
        assert stored_target.quality_banned_accounts == 0


@pytest.mark.asyncio
async def test_participant_reactions_only_choose_recent_distinct_human_messages(app) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="human-message-reactor",
            telegram_user_id=9100,
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@human_messages", title="Human messages")
        target_list = TargetList(name="Human message chats")
        session.add_all([account, target, target_list])
        await session.flush()
        session.add(
            TargetListItem(target_list_id=target_list.id, target_id=target.id, enabled=True)
        )
        await session.commit()
        account_id = int(account.id)
        target_list_id = int(target_list.id)

    observed = [ParticipantMessage(message_id=100, sender_id=9200)]

    async def scan_participant_messages(**kwargs: object) -> ParticipantMessageScan:
        del kwargs
        return ParticipantMessageScan(messages=tuple(observed))

    app.state.gateway.scan_participant_messages = scan_participant_messages
    rule = await app.state.participant_reactions.create_rule(
        name="Only recent human messages",
        target_list_id=target_list_id,
        account_ids=[account_id],
        emoji="👍",
        sample_numerator=1,
        sample_denominator=1,
    )
    await app.state.participant_reactions.scan_rule(int(rule.id))

    observed[:] = [
        ParticipantMessage(message_id=101, sender_id=9201, is_bot=True),
        ParticipantMessage(message_id=102, sender_id=9202, is_outgoing=True),
        ParticipantMessage(message_id=103, sender_id=-100123),
        ParticipantMessage(
            message_id=104,
            sender_id=9203,
            sent_at=utc_now() - timedelta(hours=2),
        ),
        ParticipantMessage(message_id=105, sender_id=9204, sent_at=utc_now()),
        ParticipantMessage(message_id=106, sender_id=9204, sent_at=utc_now()),
    ]
    async with app.state.database.session_factory() as session:
        stored_rule = await session.get(ParticipantReactionRule, int(rule.id))
        assert stored_rule is not None
        stored_rule.last_error = "old scan error"
        stored_rule.last_error_code = "rpc_error"
        await session.commit()

    await app.state.participant_reactions.scan_rule(int(rule.id))
    async with app.state.database.session_factory() as session:
        tasks = list(await session.scalars(select(ParticipantReactionTask)))
        stored_rule = await session.get(ParticipantReactionRule, int(rule.id))
        assert [task.message_id for task in tasks] == [105]
        assert stored_rule is not None
        assert stored_rule.last_error is None
        assert stored_rule.last_error_code is None


@pytest.mark.asyncio
async def test_expired_cancelled_participant_reaction_has_unknown_outcome(app) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="expired-cancelled-participant-reactor",
            telegram_user_id=8201,
            authorized=True,
            ownership_confirmed=True,
        )
        target = Target(chat_ref="@expired_participant_reaction", title="Expired reaction")
        target_list = TargetList(name="Expired participant reaction chats")
        session.add_all([account, target, target_list])
        await session.flush()
        session.add(
            TargetListItem(target_list_id=target_list.id, target_id=target.id, enabled=True)
        )
        rule = ParticipantReactionRule(
            name="Expired participant reaction rule",
            target_list_id=target_list.id,
            account_ids_json=f"[{account.id}]",
            emoji="👍",
            sample_numerator=1,
            sample_denominator=1,
            approved_by_operator=True,
        )
        session.add(rule)
        await session.flush()
        task = ParticipantReactionTask(
            rule_id=rule.id,
            target_id=target.id,
            reactor_account_id=account.id,
            message_id=700,
            source_sender_id=8301,
            emoji="👍",
            status=ParticipantReactionTaskStatus.RUNNING,
            attempt_count=1,
            max_attempts=3,
            lease_owner="expired-worker",
            lease_token="expired-participant-token",
            lease_until=utc_now() - timedelta(seconds=1),
            cancel_requested=True,
        )
        session.add(task)
        await session.commit()
        task_id = int(task.id)

    assert await app.state.participant_reactions.recover_expired_leases() == 1
    async with app.state.database.session_factory() as session:
        stored = await session.get(ParticipantReactionTask, task_id)
        assert stored is not None
        assert stored.status == ParticipantReactionTaskStatus.FAILED
        assert stored.error_code == "cancelled_outcome_unknown"
        assert stored.lease_token is None
