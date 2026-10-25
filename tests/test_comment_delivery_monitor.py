from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from proxima_traffic.bot.channel_comment_ui import (
    channel_comment_details_text,
    channel_comment_runs_text,
)
from proxima_traffic.channel_comments import ChannelCommentDeliverySummary
from proxima_traffic.config import Settings
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.gateways import DryRunGateway, PermanentGatewayError, TemporaryGatewayError
from proxima_traffic.models import (
    Account,
    AccountStatus,
    AuditEvent,
    ChannelCommentMessage,
    ChannelCommentMode,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    ensure_utc,
    utc_now,
)


class ObservingGateway(DryRunGateway):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict] = []
        self.error: BaseException | None = None
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def verify_channel_comment(self, **kwargs: object) -> None:
        self.calls.append(kwargs)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error


async def _sent_comment(app, *, historical: bool = False) -> tuple[int, int, int, int | None]:
    async with app.state.database.session_factory() as session:
        account = Account(label="comment-monitor", ownership_confirmed=True, authorized=True)
        session.add(account)
        await session.flush()
        rule = ChannelCommentRule(
            name="Observed comments",
            channel_ref="@original_channel",
            mode=ChannelCommentMode.SINGLE,
            primary_account_id=account.id,
            primary_text="Comment",
            approved_by_operator=True,
        )
        session.add(rule)
        await session.flush()
        sent_at = utc_now() - timedelta(minutes=11)
        run = ChannelCommentRun(
            rule_id=rule.id,
            source_post_id=100,
            status=ChannelCommentRunStatus.SUCCEEDED,
            sent_steps=1,
            step_index=1,
            last_comment_message_id=101,
            finished_at=sent_at,
        )
        session.add(run)
        await session.flush()
        message = None
        if not historical:
            message = ChannelCommentMessage(
                run_id=run.id,
                account_id=account.id,
                step_index=0,
                channel_ref=rule.channel_ref,
                source_post_id=run.source_post_id,
                message_id=101,
                discussion_peer_id=-1000000000123,
                discussion_access_hash=456,
                delivered_at=sent_at,
                next_check_at=sent_at + timedelta(minutes=10),
            )
            session.add(message)
        await session.commit()
        return int(account.id), int(rule.id), int(run.id), int(message.id) if message else None


def _monitor(app, gateway: ObservingGateway) -> DelayedDeliveryMonitor:
    return DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        app.state.campaign_runner,
        app.state.settings,
    )


async def _due(app, message_id: int) -> None:
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        message.next_check_at = utc_now() - timedelta(seconds=1)
        await session.commit()


async def test_first_observation_keeps_delivery_and_schedules_final_check(app) -> None:
    account_id, rule_id, run_id, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    monitor = _monitor(app, gateway)
    assert await monitor.check_due_once() == 1
    assert await monitor.check_due_once() == 0
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        run = await session.get(ChannelCommentRun, run_id)
        account = await session.get(Account, account_id)
        assert message.status == "present" and message.first_check_status == "present"
        assert message.check_stage == 1 and message.lease_token is None
        assert ensure_utc(message.next_check_at) == (
            ensure_utc(message.delivered_at) + timedelta(days=1)
        )
        assert run.status == ChannelCommentRunStatus.SUCCEEDED and run.sent_steps == 1
        assert account.next_send_at is None and account.send_reservation_token is None
        assert await session.scalar(
            select(func.count(AuditEvent.id)).where(
                AuditEvent.event_type == "channel_comment.delivery_observed"
            )
        ) == 1
    summary = (await app.state.channel_comments.get_rule_view(rule_id)).delivery
    assert summary.confirmed == summary.present == summary.scheduled == 1
    assert summary.missing == summary.unavailable == summary.pending == 0


async def test_late_missing_preserves_first_observation_and_successful_send(app) -> None:
    _, rule_id, run_id, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    monitor = _monitor(app, gateway)
    await monitor.check_due_once()
    await _due(app, message_id)
    gateway.error = PermanentGatewayError("message absent, cause unknown", code="comment_missing")
    assert await monitor.check_due_once() == 1
    assert await monitor.check_due_once() == 0
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        run = await session.get(ChannelCommentRun, run_id)
        rule = await session.get(ChannelCommentRule, rule_id)
        assert message.status == "missing" and message.first_check_status == "present"
        assert message.first_checked_at is not None and message.next_check_at is None
        assert message.check_stage == 2
        assert run.status == ChannelCommentRunStatus.SUCCEEDED and run.sent_steps == 1
        assert rule.enabled is True and rule.last_error_code is None
        assert await session.scalar(select(func.count(ChannelCommentRun.id))) == 1
    summaries = await app.state.channel_comments.recent_run_delivery_summaries([run_id])
    assert summaries[run_id].confirmed == summaries[run_id].missing == 1
    assert summaries[run_id].present == summaries[run_id].scheduled == 0
    assert len(gateway.calls) == 2


@pytest.mark.parametrize("code", ["account_chat_access_lost", "target_unavailable"])
async def test_inaccessible_discussion_is_unknown_and_gets_a_final_check(app, code) -> None:
    _, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.error = PermanentGatewayError("cannot read discussion", code=code)
    assert await _monitor(app, gateway).check_due_once() == 1
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == message.first_check_status == "unavailable"
        assert message.error_code == code
        assert message.check_stage == 1 and message.next_check_at is not None


async def test_network_failures_are_bounded_at_each_checkpoint(app) -> None:
    _, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.error = TemporaryGatewayError("network unavailable", code="network_error")
    monitor = _monitor(app, gateway)
    for attempt in range(6):
        await _due(app, message_id)
        assert await monitor.check_due_once() == 1
        async with app.state.database.session_factory() as session:
            message = await session.get(ChannelCommentMessage, message_id)
            assert message.status == "unavailable" and message.lease_token is None
            assert message.error_code == "network_error"
            if attempt < 2:
                assert message.check_stage == 0 and message.attempt_count == attempt + 1
            elif attempt == 2:
                assert message.check_stage == 1 and message.first_check_status == "unavailable"
            elif attempt == 5:
                assert message.check_stage == 2 and message.next_check_at is None
    assert len(gateway.calls) == 6


async def test_timeout_is_unknown_releases_lease_and_retries(app) -> None:
    _, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.release = asyncio.Event()
    app.state.settings.delayed_delivery_verify_timeout_seconds = 0.05
    assert await _monitor(app, gateway).check_due_once() == 1
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == "unavailable" and message.attempt_count == 1
        assert message.next_check_at is not None and message.lease_token is None


async def test_two_workers_cannot_read_one_comment_concurrently(app) -> None:
    await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.release = asyncio.Event()
    first = _monitor(app, gateway)
    second = _monitor(app, gateway)
    task = asyncio.create_task(first.check_due_once())
    try:
        await asyncio.wait_for(gateway.started.wait(), timeout=3)
        assert await second.check_due_once() == 0
    finally:
        gateway.release.set()
        await task
    assert len(gateway.calls) == 1


async def test_expired_lease_recovery_rejects_the_previous_workers_result(app) -> None:
    _, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    first = _monitor(app, gateway)._comments
    second = _monitor(app, gateway)._comments
    claimed_id, old_token = await first._claim()
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        message.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    new_id, new_token = await second._claim()
    assert claimed_id == new_id == message_id
    await first._finish(message_id, old_token, "missing")
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == "pending" and message.lease_token == new_token
    await second._finish(message_id, new_token, "present")
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == "present"


async def test_account_cooldown_defers_without_read_or_spending_attempts(app) -> None:
    account_id, _, _, message_id = await _sent_comment(app)
    resume = utc_now() + timedelta(seconds=700)
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        account.cooldown_until = resume
        await session.commit()
    gateway = ObservingGateway()
    assert await _monitor(app, gateway).check_due_once() == 1
    assert gateway.calls == []
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == "pending" and message.checked_at is None
        assert message.attempt_count == 0 and ensure_utc(message.next_check_at) == resume


async def test_flood_wait_uses_the_account_pause_without_marking_message_missing(app) -> None:
    account_id, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.error = TemporaryGatewayError("FLOOD_WAIT", code="flood_wait", retry_after=900)
    before = utc_now()
    assert await _monitor(app, gateway).check_due_once() == 1
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        message = await session.get(ChannelCommentMessage, message_id)
        assert ensure_utc(account.cooldown_until) >= before + timedelta(seconds=900)
        assert ensure_utc(message.next_check_at) >= ensure_utc(account.cooldown_until)
        assert message.status == "unavailable" and message.attempt_count == 0
        assert message.check_stage == 0 and message.first_check_status is None


async def test_archived_rule_and_changed_route_keep_the_original_message_context(app) -> None:
    account_id, rule_id, _, _ = await _sent_comment(app)
    async with app.state.database.session_factory() as session:
        replacement = Account(label="replacement", ownership_confirmed=True)
        session.add(replacement)
        await session.flush()
        rule = await session.get(ChannelCommentRule, rule_id)
        rule.primary_account_id = replacement.id
        rule.channel_ref = "@another_channel"
        rule.enabled = False
        rule.archived_at = utc_now()
        await session.commit()
    gateway = ObservingGateway()
    assert await _monitor(app, gateway).check_due_once() == 1
    assert gateway.calls[0]["account"].id == account_id
    assert gateway.calls[0]["channel_ref"] == "@original_channel"
    assert gateway.calls[0]["discussion_peer_id"] == -1000000000123
    assert gateway.calls[0]["discussion_access_hash"] == 456


async def test_disabled_account_and_disabled_monitor_do_not_contact_telegram(app) -> None:
    account_id, _, _, message_id = await _sent_comment(app)
    gateway = ObservingGateway()
    app.state.settings.delayed_delivery_verify_enabled = False
    monitor = _monitor(app, gateway)
    assert await monitor.check_due_once() == 0
    app.state.settings.delayed_delivery_verify_enabled = True
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        account.status = AccountStatus.DISABLED
        await session.commit()
    assert await monitor.check_due_once() == 1
    assert gateway.calls == []
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        assert message.status == "unavailable" and message.check_stage == 1


async def test_historical_run_is_reported_without_fabricating_monitoring_success(app) -> None:
    _, rule_id, run_id, _ = await _sent_comment(app, historical=True)
    gateway = ObservingGateway()
    assert await _monitor(app, gateway).check_due_once() == 0
    view = await app.state.channel_comments.get_rule_view(rule_id)
    assert view.legacy_runs_with_incomplete_delivery == 1 and view.delivery.confirmed == 0
    text = channel_comment_details_text(view, accounts={})
    assert "Старые обработки с неполным журналом: <b>1</b>" in text
    runs = await app.state.channel_comments.recent_runs(rule_id)
    summaries = await app.state.channel_comments.recent_run_delivery_summaries([run_id])
    text = channel_comment_runs_text(
        runs, channel_ref=view.rule.channel_ref, delivery_summaries=summaries
    )
    assert "Нет подробного журнала наблюдения" in text
    assert "Подтверждено отправлено" not in text


async def test_report_distinguishes_confirmed_delivery_from_unavailable_observation(app) -> None:
    _, rule_id, run_id, _ = await _sent_comment(app)
    gateway = ObservingGateway()
    gateway.error = PermanentGatewayError("no access", code="target_unavailable")
    await _monitor(app, gateway).check_due_once()
    view = await app.state.channel_comments.get_rule_view(rule_id)
    summaries = await app.state.channel_comments.recent_run_delivery_summaries([run_id])
    text = channel_comment_runs_text(
        await app.state.channel_comments.recent_runs(rule_id),
        channel_ref=view.rule.channel_ref,
        delivery_summaries=summaries,
    )
    assert "Подтверждено отправлено: <b>1</b>" in text
    assert "отсутствуют <b>0</b>" in text
    assert "проверка недоступна <b>1</b>" in text
    assert "Запланировано проверок: <b>1</b>" in text


async def test_chain_started_before_upgrade_reports_its_partial_history(app) -> None:
    _, rule_id, run_id, message_id = await _sent_comment(app)
    async with app.state.database.session_factory() as session:
        message = await session.get(ChannelCommentMessage, message_id)
        message.step_index = 1
        run = await session.get(ChannelCommentRun, run_id)
        run.sent_steps = 2
        run.step_index = 2
        await session.commit()
    view = await app.state.channel_comments.get_rule_view(rule_id)
    assert view.legacy_runs_with_incomplete_delivery == 1
    assert view.delivery.confirmed == 1
    summaries = await app.state.channel_comments.recent_run_delivery_summaries([run_id])
    assert summaries[run_id].history_incomplete
    text = channel_comment_runs_text(
        await app.state.channel_comments.recent_runs(rule_id),
        channel_ref=view.rule.channel_ref,
        delivery_summaries=summaries,
    )
    assert "ранние сообщения этой обработки не охвачены наблюдением" in text


async def test_large_observation_report_keeps_complete_entries_within_telegram_limit(app) -> None:
    _, rule_id, _, _ = await _sent_comment(app)
    runs = [
        ChannelCommentRun(
            id=index + 1,
            rule_id=rule_id,
            source_post_id=index + 100,
            status=ChannelCommentRunStatus.SUCCEEDED,
            sent_steps=3,
            last_comment_message_id=200 + index,
            error="🧩" * 250,
        )
        for index in range(30)
    ]
    summary = ChannelCommentDeliverySummary(
        confirmed=3, present=1, missing=1, unavailable=1, scheduled=2,
        checked_at=utc_now(), next_check_at=utc_now() + timedelta(days=1),
    )
    text = channel_comment_runs_text(
        runs,
        channel_ref="@original_channel",
        delivery_summaries={run.id: summary for run in runs},
    )
    assert len(text.encode("utf-16-le")) // 2 <= 4096
    assert "Ещё обработок:" in text and "из-за лимита" in text
    assert text.count("<b>") == text.count("</b>")
    assert text.count("<code>") == text.count("</code>")


@pytest.mark.parametrize(
    "values",
    [
        {"channel_comments_verify_max_attempts": 0},
        {"channel_comments_verify_max_attempts": 11},
        {"channel_comments_verify_final_delay_seconds": 599},
        {"channel_comments_verify_final_delay_seconds": 604801},
    ],
)
def test_comment_monitor_settings_have_valid_bounds(values) -> None:
    with pytest.raises(ValueError):
        Settings(**values)
