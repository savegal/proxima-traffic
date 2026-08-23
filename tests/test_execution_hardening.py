from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select

from proxima_traffic.config import Settings
from proxima_traffic.gateways import (
    PermanentGatewayError,
    SentMessage,
    TemporaryGatewayError,
)
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignRun,
    RunStatus,
    Target,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import LeaseLostError, QueueWorker, safe_error_message


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.idempotency_keys: list[str] = []

    async def send_message(self, **kwargs: object) -> SentMessage:
        self.idempotency_keys.append(str(kwargs["idempotency_key"]))
        self.started.set()
        await self.release.wait()
        return SentMessage(message_id=9001)


class ErrorGateway:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise self.error


class LeaseReleasingRunner:
    """Mimics the durable terminal write performed inside CampaignRunner."""

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def process_step(self, run_id: int, lease_token: str) -> CampaignRun:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            assert run is not None
            assert run.lease_token == lease_token
            run.status = RunStatus.SUCCEEDED
            run.primary_message_id = 4242
            run.finished_at = utc_now()
            run.lease_owner = None
            run.lease_token = None
            run.lease_until = None
            run.heartbeat_at = None
            await session.commit()
            await session.refresh(run)
        # Give QueueWorker's heartbeat a chance to observe the deliberate
        # lease release before this method returns.
        await asyncio.sleep(0.12)
        return run


async def test_stale_runner_is_fenced_after_lease_is_reclaimed(tmp_path) -> None:
    gateway = BlockingGateway()
    app = create_app(settings=_settings(tmp_path, "fencing.db"), gateway=gateway)
    await app.state.database.create_schema()
    campaign_id = await _create_ready_campaign(app)
    queued = await app.state.run_queue.enqueue(campaign_id, "fencing-request")
    first = await app.state.run_queue.claim_next("worker-old")
    assert first is not None and first.lease_token is not None

    old_execution = asyncio.create_task(
        app.state.campaign_runner.process_step(first.id, first.lease_token)
    )
    await gateway.started.wait()
    await _expire_lease(app, first.id)
    assert await app.state.run_queue.recover_expired_leases() == 1
    second = await app.state.run_queue.claim_next("worker-new")
    assert second is not None and second.lease_token is not None
    assert second.lease_token != first.lease_token

    gateway.release.set()
    with pytest.raises(LeaseLostError):
        await old_execution
    stored = await app.state.run_queue.get_run(queued.id)
    assert stored.status.value == "running"
    assert stored.lease_token == second.lease_token
    assert stored.primary_message_id is None
    assert gateway.idempotency_keys == ["fencing-request:primary"]

    with pytest.raises(LeaseLostError):
        await app.state.run_queue.mark_step_error(
            stored.id,
            "stale write",
            lease_token=first.lease_token,
            error_code="stale",
        )
    assert await app.state.run_queue.heartbeat(stored.id, "worker-old", first.lease_token) is False
    await app.state.database.dispose()


async def test_flood_wait_is_not_shortened_by_queue_cap(tmp_path) -> None:
    settings = _settings(tmp_path, "full-flood-wait.db")
    settings.max_flood_wait_seconds = 10
    app = create_app(settings=settings, gateway=ErrorGateway(RuntimeError("unused")))
    await app.state.database.create_schema()
    campaign_id = await _create_ready_campaign(app)
    queued = await app.state.run_queue.enqueue(campaign_id, "full-flood-wait")
    claimed = await app.state.run_queue.claim_next("flood-worker")
    assert claimed is not None and claimed.lease_token is not None
    before = utc_now()

    rescheduled = await app.state.run_queue.mark_step_error(
        queued.id,
        "Telegram requested a long wait",
        retry_after=90_000,
        lease_token=claimed.lease_token,
        error_code="flood_wait",
    )

    assert rescheduled.scheduled_for is not None
    assert ensure_utc(rescheduled.scheduled_for) >= before + timedelta(seconds=89_999)
    await app.state.database.dispose()


async def test_heartbeat_extends_current_lease_and_worker_metrics(app) -> None:
    campaign_id = await _create_ready_campaign(app)
    run = await app.state.run_queue.enqueue(campaign_id, "heartbeat-request")
    claimed = await app.state.run_queue.claim_next("heartbeat-worker")
    assert claimed is not None and claimed.lease_token is not None
    assert (
        await app.state.run_queue.heartbeat(run.id, "heartbeat-worker", claimed.lease_token) is True
    )

    stored = await app.state.run_queue.get_run(run.id)
    assert stored.heartbeat_at is not None
    assert stored.lease_until is not None
    assert stored.lease_token == claimed.lease_token
    snapshot = app.state.queue_worker.snapshot()
    assert snapshot.active_steps == 0
    assert snapshot.processed_steps == 0


async def test_terminal_write_does_not_report_a_false_heartbeat_lease_loss(
    tmp_path,
    caplog,
) -> None:
    settings = _settings(tmp_path, "terminal-heartbeat-race.db")
    settings.worker_heartbeat_seconds = 0.02
    app = create_app(settings=settings, gateway=ErrorGateway(RuntimeError("unused")))
    await app.state.database.create_schema()
    campaign_id = await _create_ready_campaign(app)
    await app.state.run_queue.enqueue(campaign_id, "terminal-heartbeat-race")
    worker = QueueWorker(
        app.state.run_queue,
        LeaseReleasingRunner(app.state.database.session_factory),
        worker_id="terminal-heartbeat-race-worker",
        poll_seconds=0.01,
        heartbeat_seconds=settings.worker_heartbeat_seconds,
        step_timeout_seconds=settings.worker_step_timeout_seconds,
    )

    result = await worker.process_next()

    assert result is not None
    assert result.status == RunStatus.SUCCEEDED
    assert result.primary_message_id == 4242
    assert "lost its lease" not in caplog.text
    assert worker.snapshot().processed_steps == 1
    await app.state.database.dispose()


@pytest.mark.parametrize(
    ("gateway_error", "expected_status", "expected_code"),
    [
        (
            TemporaryGatewayError("temporary outage", retry_after=9),
            "queued",
            "gateway_temporary",
        ),
        (PermanentGatewayError("permission denied"), "failed", "gateway_permanent"),
    ],
)
async def test_gateway_error_classification(
    tmp_path,
    gateway_error: Exception,
    expected_status: str,
    expected_code: str,
) -> None:
    app = create_app(
        settings=_settings(tmp_path, f"{expected_code}.db"),
        gateway=ErrorGateway(gateway_error),
    )
    await app.state.database.create_schema()
    campaign_id = await _create_ready_campaign(app)
    await app.state.run_queue.enqueue(campaign_id, f"request-{expected_code}")
    result = await app.state.queue_worker.process_next()
    assert result is not None
    assert result.status.value == expected_status
    assert result.error_code == expected_code
    assert result.lease_token is None
    assert result.heartbeat_at is None
    await app.state.database.dispose()


async def test_step_timeout_is_terminal_to_avoid_ambiguous_duplicate_send(tmp_path) -> None:
    gateway = BlockingGateway()
    settings = _settings(tmp_path, "timeout.db")
    settings.worker_step_timeout_seconds = 0.05
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    campaign_id = await _create_ready_campaign(app)
    await app.state.run_queue.enqueue(campaign_id, "timeout-request")
    result = await app.state.queue_worker.process_next()
    assert result is not None
    assert result.status.value == "failed"
    assert result.error_code == "step_timeout"
    assert result.attempt_count == 1
    await app.state.database.dispose()


def test_error_messages_redact_proxy_passwords() -> None:
    error = RuntimeError("connect failed: socks5://alice:super-secret@proxy.example:1080")
    message = safe_error_message(error)
    assert "super-secret" not in message
    assert "socks5://alice:***@proxy.example:1080" in message


async def _create_ready_campaign(app) -> int:
    async with app.state.database.session_factory() as session:
        primary = Account(
            label=f"primary-{id(app)}",
            telegram_user_id=1001,
            ownership_confirmed=True,
        )
        reply = Account(
            label=f"reply-{id(app)}",
            telegram_user_id=1002,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref=f"@target_{id(app)}",
            title="Approved target",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all([primary, reply, target])
        await session.flush()
        campaign = Campaign(
            name=f"campaign-{id(app)}",
            primary_account_id=primary.id,
            reply_account_id=reply.id,
            target_id=target.id,
            primary_text="First",
            reply_text="Reply",
            reply_delay_seconds=0,
        )
        session.add(campaign)
        await session.commit()
        return campaign.id


async def _expire_lease(app, run_id: int) -> None:
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(CampaignRun).where(CampaignRun.id == run_id))
        assert run is not None
        run.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()


def _settings(tmp_path, filename: str) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / filename}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        worker_heartbeat_seconds=0.05,
        worker_step_timeout_seconds=2,
    )
