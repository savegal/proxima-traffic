import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

from fastapi import FastAPI, Response, status

from proxima_traffic.accounts import AccountService
from proxima_traffic.api import router
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.config import Settings, get_settings
from proxima_traffic.db import Database
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.gateways import MessageGateway
from proxima_traffic.live_gateway import build_gateway
from proxima_traffic.participant_reactions import ParticipantReactionService
from proxima_traffic.preflight import PreflightService
from proxima_traffic.proxies import ProxyService
from proxima_traffic.reactions import ReactionWorker
from proxima_traffic.services import CampaignRunner, QueueWorker, RunQueue
from proxima_traffic.spambot_checks import SpamBotCheckService, SpamBotMaintenanceWorker
from proxima_traffic.worker_supervision import WorkerSupervisor

logger = logging.getLogger(__name__)


async def _finish_background_task(
    task: asyncio.Task[None] | None,
    *,
    name: str,
    grace_seconds: float,
) -> None:
    """Give a stopped background task time to finish, then cancel it safely."""
    if task is None:
        return
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=grace_seconds)
    except TimeoutError:
        logger.warning("%s did not stop within %.0f seconds; cancelling it", name, grace_seconds)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("%s stopped with an unexpected error", name)


def create_app(
    settings: Settings | None = None,
    gateway: MessageGateway | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    database = Database(resolved_settings.database_url)
    proxy_service = ProxyService(database.session_factory, resolved_settings)
    account_service = AccountService(database.session_factory, resolved_settings, proxy_service)
    resolved_gateway = gateway or build_gateway(
        database.session_factory,
        resolved_settings,
        account_service=account_service,
        proxy_service=proxy_service,
    )
    spambot_checks = SpamBotCheckService(
        database.session_factory,
        resolved_gateway,
        resolved_settings,
    )
    runner = CampaignRunner(
        database.session_factory,
        resolved_gateway,
        settings=resolved_settings,
        spambot_checks=spambot_checks,
    )
    preflight_service = PreflightService(
        database.session_factory,
        resolved_gateway,
        spambot_checks,
        runner,
        allow_auto_join=resolved_settings.account_auto_join,
        require_live_session=not resolved_settings.dry_run,
    )
    delayed_delivery_monitor = DelayedDeliveryMonitor(
        database.session_factory,
        resolved_gateway,
        runner,
        resolved_settings,
    )
    channel_comments = ChannelCommentService(
        database.session_factory,
        resolved_gateway,
        runner,
        resolved_settings,
        worker_id=f"api-comments-{uuid4().hex[:10]}",
    )
    reactions = ReactionWorker(
        database.session_factory,
        resolved_gateway,
        runner,
        resolved_settings,
        worker_id=f"api-reactions-{uuid4().hex[:10]}",
    )
    participant_reactions = ParticipantReactionService(
        database.session_factory,
        resolved_gateway,
        runner,
        resolved_settings,
        worker_id=f"api-participant-reactions-{uuid4().hex[:10]}",
    )
    run_queue = RunQueue(
        database.session_factory,
        lease_seconds=resolved_settings.worker_lease_seconds,
        max_attempts=resolved_settings.worker_max_attempts,
        retry_base_seconds=resolved_settings.worker_retry_base_seconds,
        retry_max_seconds=resolved_settings.worker_retry_max_seconds,
        max_retry_after_seconds=int(resolved_settings.max_flood_wait_seconds),
        require_live_session=not resolved_settings.dry_run,
        require_proxy_route=resolved_settings.account_require_proxy,
        account_auto_join=resolved_settings.account_auto_join,
        account_pace_seconds=resolved_settings.account_pace_max_seconds,
        account_daily_limit=resolved_settings.account_daily_limit,
        account_micro_break_every=resolved_settings.account_micro_break_every,
        account_micro_break_seconds=resolved_settings.account_micro_break_max_seconds,
        preflight_required=(
            resolved_settings.preflight_required_live and not resolved_settings.dry_run
        ),
        preflight_max_age_seconds=resolved_settings.preflight_max_age_seconds,
        proxy_service=proxy_service,
    )
    queue_worker = QueueWorker(
        run_queue,
        runner,
        worker_id=f"local-{uuid4().hex[:10]}",
        poll_seconds=resolved_settings.worker_poll_seconds,
        heartbeat_seconds=resolved_settings.worker_heartbeat_seconds,
        step_timeout_seconds=resolved_settings.worker_step_timeout_seconds,
        concurrency=resolved_settings.worker_concurrency,
    )
    spambot_maintenance = SpamBotMaintenanceWorker(spambot_checks, resolved_settings)
    background_tasks: dict[str, asyncio.Task[None]] = {}
    worker_supervisor = WorkerSupervisor()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        worker_task: asyncio.Task[None] | None = None
        channel_comments_task: asyncio.Task[None] | None = None
        reactions_task: asyncio.Task[None] | None = None
        participant_reactions_task: asyncio.Task[None] | None = None
        delayed_delivery_task: asyncio.Task[None] | None = None
        spambot_task: asyncio.Task[None] | None = None
        background_tasks.clear()
        if resolved_settings.auto_create_schema:
            await database.create_schema()
        await AdminRepository(database.session_factory).ensure_system_account_groups(
            actor="api-startup"
        )
        await spambot_checks.prepare_legacy_unknown_rechecks()
        if resolved_settings.api_worker_enabled:
            worker_task = worker_supervisor.start("campaigns", queue_worker.run_forever)
            background_tasks["campaigns"] = worker_task
            if resolved_settings.channel_comments_enabled:
                channel_comments_task = worker_supervisor.start(
                    "channel_comments", channel_comments.run_forever
                )
                background_tasks["channel_comments"] = channel_comments_task
            reactions_task = worker_supervisor.start("reactions", reactions.run_forever)
            background_tasks["reactions"] = reactions_task
            if resolved_settings.participant_reactions_enabled:
                participant_reactions_task = worker_supervisor.start(
                    "participant_reactions", participant_reactions.run_forever
                )
                background_tasks["participant_reactions"] = participant_reactions_task
            if resolved_settings.delayed_delivery_verify_enabled:
                delayed_delivery_task = worker_supervisor.start(
                    "delayed_delivery", delayed_delivery_monitor.run_forever
                )
                background_tasks["delayed_delivery"] = delayed_delivery_task
        if (
            resolved_settings.api_worker_enabled
            and resolved_settings.spambot_enabled
            and not resolved_settings.dry_run
        ):
            spambot_task = worker_supervisor.start("spambot", spambot_maintenance.run_forever)
            background_tasks["spambot"] = spambot_task
        try:
            yield
        finally:
            queue_worker.stop()
            channel_comments.stop()
            reactions.stop()
            participant_reactions.stop()
            delayed_delivery_monitor.stop()
            spambot_maintenance.stop()
            worker_supervisor.stop()
            await _finish_background_task(
                worker_task,
                name="Campaign worker",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            await _finish_background_task(
                channel_comments_task,
                name="Channel comments worker",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            await _finish_background_task(
                reactions_task,
                name="Reactions worker",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            await _finish_background_task(
                participant_reactions_task,
                name="Participant reactions worker",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            await _finish_background_task(
                delayed_delivery_task,
                name="Delayed delivery monitor",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            await _finish_background_task(
                spambot_task,
                name="@SpamBot maintenance",
                grace_seconds=resolved_settings.shutdown_grace_seconds,
            )
            close = getattr(resolved_gateway, "close", None)
            if close is not None:
                await close()
            await database.dispose()

    app = FastAPI(
        title=resolved_settings.app_name,
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.database = database
    app.state.gateway = resolved_gateway
    app.state.campaign_runner = runner
    app.state.run_queue = run_queue
    app.state.queue_worker = queue_worker
    app.state.proxy_service = proxy_service
    app.state.account_service = account_service
    app.state.preflight_service = preflight_service
    app.state.channel_comments = channel_comments
    app.state.reactions = reactions
    app.state.participant_reactions = participant_reactions
    app.state.delayed_delivery_monitor = delayed_delivery_monitor
    app.state.spambot_checks = spambot_checks
    app.state.worker_supervisor = worker_supervisor
    app.state.background_tasks = background_tasks
    app.include_router(router)

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str | bool]:
        return {"status": "ok", "dry_run": resolved_settings.dry_run}

    @app.get("/ready", tags=["system"])
    async def ready(response: Response) -> dict[str, str | bool]:
        database_ready = await database.ready()
        worker = queue_worker.snapshot()
        supervised_workers_ready = worker_supervisor.ready(
            max_stale_seconds=resolved_settings.worker_readiness_max_stale_seconds
        )
        background_workers = {name: not task.done() for name, task in background_tasks.items()}
        worker_ready = not resolved_settings.api_worker_enabled or (
            worker.running
            and bool(background_workers)
            and all(background_workers.values())
            and supervised_workers_ready
        )
        ready = database_ready and worker_ready
        if not ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "ready" if ready else "not_ready",
            "database": database_ready,
            "worker": worker_ready,
            "dry_run": resolved_settings.dry_run,
        }

    return app


logging.basicConfig(level=getattr(logging, get_settings().log_level.upper(), logging.INFO))
app = create_app()
