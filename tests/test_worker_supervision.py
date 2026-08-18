import asyncio
from datetime import timedelta
from inspect import signature

import pytest

from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.models import utc_now
from proxima_traffic.notifications import RunNotifier
from proxima_traffic.participant_reactions import ParticipantReactionService
from proxima_traffic.reactions import ReactionWorker
from proxima_traffic.services import QueueWorker
from proxima_traffic.spambot_checks import SpamBotMaintenanceWorker
from proxima_traffic.worker_supervision import WorkerSupervisor


def test_all_supervised_workers_accept_an_iteration_reporter() -> None:
    for worker in (
        QueueWorker,
        ChannelCommentService,
        ReactionWorker,
        ParticipantReactionService,
        DelayedDeliveryMonitor,
        SpamBotMaintenanceWorker,
        RunNotifier,
    ):
        assert "reporter" in signature(worker.run_forever).parameters


@pytest.mark.asyncio
async def test_supervisor_alerts_on_iteration_failure_and_recovers_after_a_healthy_pass() -> None:
    alerts: list[str] = []
    release = asyncio.Event()
    attempts = 0

    async def send_alert(text: str) -> None:
        alerts.append(text)

    async def runner(report) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            await report(RuntimeError("database unavailable"))
            return
        await report()
        await release.wait()

    supervisor = WorkerSupervisor(
        alert_sender=send_alert,
        alert_cooldown_seconds=60,
        restart_delay_seconds=0.01,
    )
    task = supervisor.start("reactions", runner)
    for _ in range(50):
        if attempts >= 2:
            break
        await asyncio.sleep(0.01)

    assert attempts == 2
    snapshot = supervisor.snapshots()[0]
    assert snapshot.name == "reactions"
    assert snapshot.state == "running"
    assert snapshot.consecutive_failures == 0
    assert snapshot.restart_count == 1
    assert len(alerts) == 1
    assert "database unavailable" in alerts[0]

    release.set()
    supervisor.stop()
    await task
    assert supervisor.snapshots()[0].state == "stopped"


@pytest.mark.asyncio
async def test_supervisor_keeps_persistent_iteration_failure_visible() -> None:
    release = asyncio.Event()

    async def runner(report) -> None:
        await report(RuntimeError("Telegram temporarily unavailable"))
        await release.wait()

    supervisor = WorkerSupervisor(restart_delay_seconds=0.01)
    task = supervisor.start("participant-reactions", runner)
    await asyncio.sleep(0)

    snapshot = supervisor.snapshots()[0]
    assert snapshot.state == "degraded"
    assert snapshot.consecutive_failures == 1
    assert snapshot.last_error == "Telegram temporarily unavailable"

    release.set()
    supervisor.stop()
    await task


@pytest.mark.asyncio
async def test_supervisor_readiness_rejects_a_stale_running_worker() -> None:
    release = asyncio.Event()

    async def runner(report) -> None:
        await report()
        await release.wait()

    supervisor = WorkerSupervisor()
    task = supervisor.start("delayed_delivery", runner)
    await asyncio.sleep(0)
    assert supervisor.ready(max_stale_seconds=30) is True
    supervisor._states["delayed_delivery"].last_success_at = utc_now() - timedelta(minutes=2)
    assert supervisor.ready(max_stale_seconds=30) is False
    release.set()
    supervisor.stop()
    await task


@pytest.mark.asyncio
async def test_supervisor_marks_an_externally_cancelled_worker_as_stopped() -> None:
    entered = asyncio.Event()

    async def runner(report) -> None:
        await report()
        entered.set()
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    task = supervisor.start("campaigns", runner)
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert supervisor.snapshots()[0].state == "stopped"
