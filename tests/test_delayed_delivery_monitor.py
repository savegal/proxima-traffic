import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select

from proxima_traffic.config import Settings
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.gateways import (
    MembershipGateRecovery,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ActionKind,
    ActionLog,
    ActionStatus,
    Campaign,
    CampaignMode,
    CampaignRun,
    Target,
    TargetAccountState,
    utc_now,
)
from proxima_traffic.services import CampaignRunner


class PresentGateway:
    async def verify_sent_message(self, **kwargs: object) -> None:
        del kwargs


class DeletedGateway:
    async def verify_sent_message(self, **kwargs: object) -> None:
        raise PermanentGatewayError(
            "message was removed by moderation",
            code="post_deleted_by_moderation",
            external_message_id=int(kwargs["message_id"]),
        )


class GateDeletedGateway(DeletedGateway):
    def __init__(self) -> None:
        self.recovery_calls = 0

    async def recover_membership_gate(self, **kwargs: object) -> MembershipGateRecovery:
        del kwargs
        self.recovery_calls += 1
        return MembershipGateRecovery(
            detected=True,
            recovered=True,
            notice="Subscribe before sending",
            joined_channel_refs=("@required_news",),
            verification_callback_count=1,
        )


class BannedGateway:
    async def verify_sent_message(self, **kwargs: object) -> None:
        raise PermanentGatewayError(
            "account was removed from the chat",
            code="account_banned_in_chat",
        )


class TemporaryGateway:
    async def verify_sent_message(self, **kwargs: object) -> None:
        del kwargs
        raise TemporaryGatewayError("temporary read error", code="network_error")


class DeadAccountGateway:
    async def verify_sent_message(self, **kwargs: object) -> None:
        del kwargs
        raise PermanentGatewayError("session revoked", code="account_dead")


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def verify_sent_message(self, **kwargs: object) -> None:
        del kwargs
        self.calls += 1
        self.started.set()
        await self.release.wait()


async def _sent_action(app, *, account_label: str = "monitor-account") -> tuple[int, int, int]:
    async with app.state.database.session_factory() as session:
        account = Account(label=account_label, ownership_confirmed=True)
        target = Target(
            chat_ref=f"@{account_label}_target",
            title="Monitor target",
            approved_by_operator=True,
        )
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name=f"{account_label}-campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Allowed update",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key=f"{account_label}-request",
            status="succeeded",
            primary_message_id=77_001,
        )
        session.add(run)
        await session.flush()
        action = ActionLog(
            run_id=run.id,
            account_id=account.id,
            target_id=target.id,
            kind=ActionKind.PRIMARY_MESSAGE,
            status=ActionStatus.SUCCEEDED,
            external_message_id=77_001,
            text="Allowed update",
            created_at=utc_now() - timedelta(minutes=11),
        )
        session.add(action)
        await session.commit()
        return int(action.id), int(target.id), int(account.id)


def _settings(app) -> Settings:
    return Settings(
        database_url=app.state.settings.database_url,
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        delayed_delivery_verify_delay_seconds=600,
        delayed_delivery_verify_poll_seconds=5,
    )


async def test_delayed_monitor_marks_confirmed_removed_post_as_bad_target(app) -> None:
    action_id, target_id, _ = await _sent_action(app)
    gateway = DeletedGateway()
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=_settings(app))
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        _settings(app),
    )

    assert await monitor.check_due_once() == 1

    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        target = await session.get(Target, target_id)
        assert action is not None and action.status == ActionStatus.FAILED
        assert action.error_code == "post_deleted_by_moderation"
        assert action.delayed_delivery_checked_at is not None
        assert target is not None and target.quality_status == "bad"
        assert "moderation" in (target.quality_reason or "")


async def test_delayed_monitor_recovers_a_subscription_gate_and_requeues_latest_step(app) -> None:
    action_id, _, _ = await _sent_action(app, account_label="gate-monitor")
    gateway = GateDeletedGateway()
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=_settings(app))
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        _settings(app),
    )

    assert await monitor.check_due_once() == 1
    assert gateway.recovery_calls == 1
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        run = await session.scalar(select(CampaignRun).where(CampaignRun.id == action.run_id))
    assert action is not None
    assert action.status == ActionStatus.FAILED
    assert action.error_code == "membership_gate_recovered"
    assert action.delayed_delivery_checked_at is not None
    assert run is not None
    assert run.status.value == "queued"
    assert run.membership_gate_retry_count == 1
    assert run.primary_message_id is None
    assert run.error_code == "membership_gate_retry"


async def test_delayed_monitor_records_chat_ban_without_reclassifying_one_account(app) -> None:
    action_id, target_id, account_id = await _sent_action(app, account_label="banned-monitor")
    gateway = BannedGateway()
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=_settings(app))
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        _settings(app),
    )

    assert await monitor.check_due_once() == 1

    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        target = await session.get(Target, target_id)
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.target_id == target_id,
                TargetAccountState.account_id == account_id,
            )
        )
        assert action is not None and action.status == ActionStatus.SUCCEEDED
        assert action.delayed_delivery_checked_at is not None
        assert state is not None and state.send_blocked is True
        assert target is not None and target.quality_status == "learning"


async def test_delayed_monitor_marks_a_present_message_only_once(app) -> None:
    action_id, _, _ = await _sent_action(app, account_label="present-monitor")
    gateway = PresentGateway()
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=_settings(app))
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        _settings(app),
    )

    assert await monitor.check_due_once() == 1
    assert await monitor.check_due_once() == 0
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        assert action is not None and action.delayed_delivery_checked_at is not None


async def test_delayed_monitor_does_not_recheck_a_post_before_manual_restore(app) -> None:
    action_id, target_id, _ = await _sent_action(app, account_label="restored-monitor")
    gateway = DeletedGateway()
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=_settings(app))
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        _settings(app),
    )
    async with app.state.database.session_factory() as session:
        target = await session.get(Target, target_id)
        assert target is not None
        target.quality_reset_at = utc_now()
        await session.commit()

    assert await monitor.check_due_once() == 1
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        target = await session.get(Target, target_id)
        assert action is not None and action.delayed_delivery_checked_at is not None
        assert action.status == ActionStatus.SUCCEEDED
        assert target is not None and target.quality_status == "learning"


async def test_delayed_monitor_defers_retryable_read_failures(app) -> None:
    action_id, _, _ = await _sent_action(app, account_label="retry-monitor")
    gateway = TemporaryGateway()
    settings = _settings(app)
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=settings)
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )

    assert await monitor.check_due_once() == 1
    assert await monitor.check_due_once() == 0
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        assert action is not None and action.delayed_delivery_checked_at is None
        assert action.delayed_delivery_retry_at is not None


async def test_delayed_monitor_disables_account_when_verification_finds_dead_session(
    app,
) -> None:
    action_id, _, account_id = await _sent_action(app, account_label="dead-monitor")
    gateway = DeadAccountGateway()
    settings = _settings(app)
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=settings)
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )

    assert await monitor.check_due_once() == 1
    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        account = await session.get(Account, account_id)
    assert action is not None and action.delayed_delivery_checked_at is not None
    assert account is not None and account.status == AccountStatus.DISABLED
    assert account.authorized is False


async def test_delayed_monitor_fences_a_check_from_another_worker(app) -> None:
    await _sent_action(app, account_label="fenced-monitor")
    gateway = BlockingGateway()
    settings = _settings(app)
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=settings)
    primary = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )
    secondary = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )

    first_check = asyncio.create_task(primary.check_due_once())
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    assert await secondary.check_due_once() == 0
    gateway.release.set()
    assert await first_check == 1
    assert gateway.calls == 1


async def test_delayed_monitor_times_out_and_releases_the_lease(app) -> None:
    action_id, _, _ = await _sent_action(app, account_label="timeout-monitor")
    gateway = BlockingGateway()
    settings = _settings(app)
    settings.delayed_delivery_verify_timeout_seconds = 0.05
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=settings)
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )

    with pytest.raises(TimeoutError, match="timed out"):
        await monitor.check_due_once()

    async with app.state.database.session_factory() as session:
        action = await session.get(ActionLog, action_id)
        assert action is not None
        assert action.delayed_delivery_checked_at is None
        assert action.delayed_delivery_retry_at is not None
        assert action.delayed_delivery_lease_owner is None
        assert action.delayed_delivery_lease_token is None
        assert action.delayed_delivery_lease_until is None


async def test_delayed_monitor_reports_progress_before_a_long_gateway_check(app) -> None:
    await _sent_action(app, account_label="progress-monitor")
    gateway = BlockingGateway()
    settings = _settings(app)
    runner = CampaignRunner(app.state.database.session_factory, gateway, settings=settings)
    monitor = DelayedDeliveryMonitor(
        app.state.database.session_factory,
        gateway,
        runner,
        settings,
    )
    progress = asyncio.Event()

    async def reporter(error: BaseException | None = None) -> None:
        assert error is None
        progress.set()

    checking = asyncio.create_task(monitor.check_due_once(reporter=reporter))
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    await asyncio.wait_for(progress.wait(), timeout=1)
    gateway.release.set()
    assert await checking == 1
