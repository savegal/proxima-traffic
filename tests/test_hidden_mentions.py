import json
from datetime import timedelta

from fastapi import FastAPI
from sqlalchemy import select
from telethon import TelegramClient, types
from telethon.sessions import MemorySession

from proxima_traffic.bot.renderers import _hidden_mention_delivery_lines
from proxima_traffic.gateways import SentMessage
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.mentions import (
    HIDDEN_MENTION_CHARACTER,
    append_hidden_mentions,
    select_hidden_mentions,
)
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignMentionState,
    CampaignMode,
    CampaignRun,
    MentionBlacklistEntry,
    RunStatus,
    Target,
    utc_now,
)
from proxima_traffic.services import CampaignRunner


class MentionRecordingGateway:
    def __init__(self) -> None:
        self.content_calls: list[dict[str, object]] = []
        self.collect_calls = 0
        self.participants = (901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912)

    async def collect_mentionable_user_ids(self, **kwargs: object) -> tuple[int, ...]:
        del kwargs
        self.collect_calls += 1
        return self.participants

    async def send_content(self, **kwargs: object) -> SentMessage:
        self.content_calls.append(kwargs)
        return SentMessage(message_id=71_001)


def test_hidden_mention_helpers_are_stable_and_preserve_visible_text() -> None:
    candidates = (100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 110, -1)
    first = select_hidden_mentions(candidates, selection_key="stable", exclude_user_id=100)
    second = select_hidden_mentions(
        reversed(candidates),
        selection_key="stable",
        exclude_user_id=100,
    )
    assert 5 <= len(first) <= 10
    assert first == second
    assert 100 not in first
    html = append_hidden_mentions(text="<ready>", html_text=None, user_ids=first)
    assert html is not None
    assert html.startswith("&lt;ready&gt;")
    assert html.count(HIDDEN_MENTION_CHARACTER) == len(first)
    assert html.count('href="tg://user?id=') == len(first)
    assert append_hidden_mentions(text="x" * 4093, html_text=None, user_ids=first) is None
    assert (
        append_hidden_mentions(
            text="x" * 1020,
            html_text=None,
            user_ids=first,
            message_limit=1024,
        )
        is None
    )


def test_hidden_mention_report_distinguishes_prepared_from_delivered() -> None:
    run = CampaignRun(
        campaign_id=1,
        target_id=1,
        sender_account_id=1,
        request_key="prepared-hidden-mentions",
        status=RunStatus.FAILED,
        hidden_mentions_enabled_snapshot=True,
        hidden_mention_user_ids_snapshot_json="[101, 102, 103, 104, 105]",
        hidden_mention_resolved_count=5,
    )

    prepared_lines = _hidden_mention_delivery_lines([run])
    assert "<b>0</b>" in prepared_lines[1]
    assert "<b>5</b>" not in prepared_lines[1]
    assert len(prepared_lines) == 4

    run.primary_message_id = 7001
    delivered_lines = _hidden_mention_delivery_lines([run])
    assert "<b>1</b>" in delivered_lines[1]
    assert "<b>5</b>" in delivered_lines[1]
    assert len(delivered_lines) == 3


async def test_live_html_parser_builds_real_telegram_mention_entities() -> None:
    session = MemorySession()
    client = TelegramClient(session, 1, "0" * 32)
    user_ids = (101, 102, 103, 104, 105)
    session.process_entities(
        [
            types.User(
                id=user_id,
                access_hash=10_000 + user_id,
                first_name=f"User {user_id}",
            )
            for user_id in user_ids
        ]
    )
    html_text = append_hidden_mentions(text="Post", html_text=None, user_ids=user_ids)
    assert html_text is not None

    text, entities = await TelethonGateway._parse_html_message(client, html_text)

    assert text == f"Post{HIDDEN_MENTION_CHARACTER * len(user_ids)}"
    mentions = [
        entity
        for entity in entities
        if isinstance(entity, types.InputMessageEntityMentionName)
    ]
    assert len(mentions) == len(user_ids)
    assert {int(entity.user_id.user_id) for entity in mentions} == set(user_ids)


async def test_broadcast_resolves_members_once_and_sends_hidden_html_mentions(
    app: FastAPI,
) -> None:
    gateway = MentionRecordingGateway()
    blocked_user_id = 7_588_293_197
    gateway.participants = (blocked_user_id, *range(1001, 1013))
    runner = CampaignRunner(
        app.state.database.session_factory,
        gateway,  # type: ignore[arg-type]
        settings=app.state.settings,
    )
    async with app.state.database.session_factory() as session:
        account = Account(
            label="mention-broadcast-sender",
            telegram_user_id=901,
            ownership_confirmed=True,
        )
        other_owned_account = Account(
            label="other-owned-mention-account",
            telegram_user_id=902,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@mention_broadcast",
            title="Mention broadcast",
            approved_by_operator=True,
        )
        session.add_all(
            [
                account,
                other_owned_account,
                target,
                MentionBlacklistEntry(
                    telegram_user_id=blocked_user_id,
                    source="test",
                ),
            ]
        )
        await session.flush()
        campaign = Campaign(
            name="Mention broadcast",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="Важный пост",
            reply_text="",
            hidden_mentions_enabled=True,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="hidden-mentions-broadcast",
            status=RunStatus.RUNNING,
            lease_owner="test-worker",
            lease_token="mention-lease",
            primary_text_snapshot=campaign.primary_text,
            hidden_mentions_enabled_snapshot=True,
        )
        session.add(run)
        await session.commit()
        run_id = run.id

    await runner.process_step(run_id, "mention-lease")

    assert gateway.collect_calls == 1
    assert len(gateway.content_calls) == 1
    html_text = gateway.content_calls[0]["html_text"]
    assert isinstance(html_text, str)
    assert html_text.startswith("Важный пост")
    assert html_text.count(HIDDEN_MENTION_CHARACTER) in range(5, 11)
    assert "tg://user?id=901" not in html_text
    assert "tg://user?id=902" not in html_text
    assert f"tg://user?id={blocked_user_id}" not in html_text

    async with app.state.database.session_factory() as session:
        stored = await session.scalar(select(CampaignRun).where(CampaignRun.id == run_id))
    assert stored is not None
    assert stored.hidden_mention_user_ids_snapshot_json is not None
    assert blocked_user_id not in json.loads(stored.hidden_mention_user_ids_snapshot_json)
    assert stored.hidden_mention_resolved_count in range(5, 11)
    assert stored.hidden_mention_skip_reason is None
    assert stored.status == RunStatus.SUCCEEDED


async def test_broadcast_mentions_respect_per_chat_cooldown_and_rotate(
    app: FastAPI,
) -> None:
    gateway = MentionRecordingGateway()
    gateway.participants = tuple(range(2001, 2011))
    runner = CampaignRunner(
        app.state.database.session_factory,
        gateway,  # type: ignore[arg-type]
        settings=app.state.settings,
    )
    async with app.state.database.session_factory() as session:
        account = Account(
            label="rotating-broadcast-mention-sender",
            telegram_user_id=991,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@rotating_broadcast_mentions",
            title="Rotating broadcast mentions",
            approved_by_operator=True,
        )
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Rotating broadcast mentions",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Post",
            hidden_mentions_enabled=True,
            mention_cooldown_days=30,
        )
        session.add(campaign)
        await session.flush()
        account_id = int(account.id)
        target_id = int(target.id)
        campaign_id = int(campaign.id)
        await session.commit()

    async def send_run(request_key: str) -> CampaignRun:
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, account_id)
            assert account is not None
            account.next_send_at = None
            account.cooldown_until = None
            account.sent_since_break = 0
            run = CampaignRun(
                campaign_id=campaign_id,
                target_id=target_id,
                sender_account_id=account_id,
                request_key=request_key,
                status=RunStatus.RUNNING,
                lease_owner="test-worker",
                lease_token=f"lease-{request_key}",
                primary_text_snapshot="Post",
                hidden_mentions_enabled_snapshot=True,
                mention_cooldown_days_snapshot=30,
            )
            session.add(run)
            await session.commit()
            run_id = int(run.id)
            lease_token = str(run.lease_token)
        await runner.process_step(run_id, lease_token)
        async with app.state.database.session_factory() as session:
            stored = await session.get(CampaignRun, run_id)
            assert stored is not None
            return stored

    first = await send_run("rotation-0")
    first_mentions = tuple(json.loads(first.hidden_mention_user_ids_snapshot_json or "[]"))
    assert len(first_mentions) == 10

    second = await send_run("rotation-1")
    assert second.hidden_mention_resolved_count == 0
    assert second.hidden_mention_skip_reason == "mention_cooldown_active"
    assert gateway.content_calls[-1]["html_text"] is None

    async with app.state.database.session_factory() as session:
        states = list(
            await session.scalars(
                select(CampaignMentionState).where(
                    CampaignMentionState.campaign_id == campaign_id,
                    CampaignMentionState.target_id == target_id,
                )
            )
        )
        assert len(states) == 10
        assert all(state.mention_count == 1 for state in states)
        assert all(state.reserved_run_id is None for state in states)
        for state in states:
            state.last_mentioned_at = utc_now() - timedelta(days=31)
        await session.commit()

    third = await send_run("rotation-2")
    assert 5 <= int(third.hidden_mention_resolved_count or 0) <= 10
    assert gateway.content_calls[-1]["html_text"] is not None


async def test_terminal_broadcast_outcomes_release_or_hold_mention_reservations(
    app: FastAPI,
) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="terminal-mention-sender")
        target = Target(
            chat_ref="@terminal_mention_outcomes",
            title="Terminal mention outcomes",
            approved_by_operator=True,
        )
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Terminal mention outcomes",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Post",
            hidden_mentions_enabled=True,
            mention_cooldown_days=45,
        )
        session.add(campaign)
        await session.flush()
        known_failure = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="known-terminal-mention-failure",
            status=RunStatus.RUNNING,
            lease_owner="test-worker",
            lease_token="known-terminal-lease",
            mention_cooldown_days_snapshot=45,
            hidden_mention_user_ids_snapshot_json="[3001, 3002, 3003, 3004, 3005]",
        )
        uncertain_failure = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="uncertain-terminal-mention-failure",
            status=RunStatus.RUNNING,
            lease_owner="test-worker",
            lease_token="uncertain-terminal-lease",
            mention_cooldown_days_snapshot=45,
            hidden_mention_user_ids_snapshot_json="[4001, 4002, 4003, 4004, 4005]",
        )
        session.add_all([known_failure, uncertain_failure])
        await session.flush()
        known_run_id = int(known_failure.id)
        uncertain_run_id = int(uncertain_failure.id)
        session.add_all(
            [
                CampaignMentionState(
                    campaign_id=campaign.id,
                    target_id=target.id,
                    telegram_user_id=user_id,
                    reserved_run_id=run_id,
                    reserved_until=utc_now() + timedelta(days=7),
                )
                for run_id, values in (
                    (known_run_id, range(3001, 3006)),
                    (uncertain_run_id, range(4001, 4006)),
                )
                for user_id in values
            ]
        )
        await session.commit()

    await app.state.run_queue.mark_step_error(
        known_run_id,
        "known rejection",
        permanent=True,
        lease_token="known-terminal-lease",
        error_code="account_banned_in_chat",
    )
    hold_started_at = utc_now()
    await app.state.run_queue.mark_step_error(
        uncertain_run_id,
        "unknown timeout outcome",
        permanent=True,
        lease_token="uncertain-terminal-lease",
        error_code="step_timeout",
    )

    async with app.state.database.session_factory() as session:
        states = list(
            await session.scalars(
                select(CampaignMentionState).where(
                    CampaignMentionState.reserved_run_id.in_(
                        [known_run_id, uncertain_run_id]
                    )
                )
            )
        )
    assert all(state.reserved_run_id != known_run_id for state in states)
    uncertain_states = [
        state for state in states if state.reserved_run_id == uncertain_run_id
    ]
    assert len(uncertain_states) == 5
    assert all(
        state.reserved_until is not None
        and state.reserved_until.replace(tzinfo=hold_started_at.tzinfo)
        >= hold_started_at + timedelta(days=44)
        for state in uncertain_states
    )


async def test_saved_broadcast_mentions_are_reconciled_with_owned_accounts(
    app: FastAPI,
) -> None:
    gateway = MentionRecordingGateway()
    runner = CampaignRunner(
        app.state.database.session_factory,
        gateway,  # type: ignore[arg-type]
        settings=app.state.settings,
    )
    async with app.state.database.session_factory() as session:
        account = Account(
            label="reconciled-mention-sender",
            telegram_user_id=961,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@reconciled_mention_broadcast",
            title="Reconciled mention broadcast",
            approved_by_operator=True,
        )
        session.add_all(
            [
                account,
                target,
                MentionBlacklistEntry(telegram_user_id=1001, source="test"),
            ]
        )
        await session.flush()
        campaign = Campaign(
            name="Reconciled mention broadcast",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Post",
            hidden_mentions_enabled=True,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="reconciled-hidden-mentions",
            status=RunStatus.RUNNING,
            lease_owner="test-worker",
            lease_token="reconciled-mention-lease",
            primary_text_snapshot=campaign.primary_text,
            hidden_mentions_enabled_snapshot=True,
            hidden_mention_user_ids_snapshot_json="[1001, 1002, 1003, 1004, 1005]",
            hidden_mention_resolved_count=5,
        )
        session.add(run)
        await session.commit()
        run_id = int(run.id)

    await runner.process_step(run_id, "reconciled-mention-lease")

    assert gateway.collect_calls == 0
    assert gateway.content_calls[0]["html_text"] is None
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run_id)
        assert stored is not None
        assert stored.hidden_mention_user_ids_snapshot_json == "[]"
        assert stored.hidden_mention_resolved_count == 0
        assert stored.hidden_mention_skip_reason == "excluded_user_after_resolution"
        assert stored.status == RunStatus.SUCCEEDED


async def test_legacy_broadcast_run_does_not_inherit_later_mentions_toggle(
    app: FastAPI,
) -> None:
    gateway = MentionRecordingGateway()
    runner = CampaignRunner(
        app.state.database.session_factory,
        gateway,  # type: ignore[arg-type]
        settings=app.state.settings,
    )
    async with app.state.database.session_factory() as session:
        account = Account(
            label="legacy-mention-sender",
            telegram_user_id=951,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@legacy_mention_broadcast",
            title="Legacy mention broadcast",
            approved_by_operator=True,
        )
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Legacy mention broadcast",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="Обычный пост",
            reply_text="",
            hidden_mentions_enabled=True,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="legacy-hidden-mentions-broadcast",
            status=RunStatus.RUNNING,
            lease_owner="test-worker",
            lease_token="legacy-mention-lease",
            primary_text_snapshot=campaign.primary_text,
            hidden_mentions_enabled_snapshot=None,
        )
        session.add(run)
        await session.commit()
        run_id = run.id

    await runner.process_step(run_id, "legacy-mention-lease")

    assert gateway.collect_calls == 0
    assert gateway.content_calls[0]["html_text"] is None
