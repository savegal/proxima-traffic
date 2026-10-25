from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from contextlib import suppress
from uuid import uuid4

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError
from aiogram.types import BotCommand

from proxima_traffic.accounts import AccountService
from proxima_traffic.blacklist_parser import ChatBlacklistParser
from proxima_traffic.bot.app import create_bot, create_dispatcher
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.bot.storage import DatabaseFsmStorage
from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.config import Settings, get_settings
from proxima_traffic.db import Database
from proxima_traffic.delivery_monitor import DelayedDeliveryMonitor
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.instance_lock import (
    BotInstanceAlreadyRunning,
    BotInstanceLock,
    control_bot_lock_path,
)
from proxima_traffic.live_gateway import build_gateway
from proxima_traffic.notifications import RunNotifier
from proxima_traffic.participant_reactions import ParticipantReactionService
from proxima_traffic.preflight import PreflightService
from proxima_traffic.proxies import ProxyService
from proxima_traffic.reactions import ReactionWorker
from proxima_traffic.services import CampaignRunner, QueueWorker, RunQueue
from proxima_traffic.spambot_checks import SpamBotCheckService, SpamBotMaintenanceWorker
from proxima_traffic.story_tagging import StoryTaggingService
from proxima_traffic.story_viewer import StoryViewerService
from proxima_traffic.subscriber_invites import SubscriberInviteService
from proxima_traffic.worker_supervision import WorkerSupervisor

logger = logging.getLogger(__name__)

_CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS = 15.0
_CONTROL_BOT_POLL_RETRY_INITIAL_SECONDS = 1.0
_CONTROL_BOT_POLL_RETRY_MAX_SECONDS = 30.0
_CONTROL_BOT_READY_RETRY_INITIAL_SECONDS = 30.0
_CONTROL_BOT_READY_RETRY_MAX_SECONDS = 300.0
_RESTART_UPDATE_NOTICE = (
    "Откройте /admin, чтобы посмотреть текущие задания и состояние сервисов. "
    "Сохранённые очереди и черновики доступны после перезапуска."
)



async def _notify_control_bot_ready(
    bot: Bot,
    admin_ids: frozenset[int],
    *,
    worker_enabled: bool,
    campaign_id_scope: frozenset[int] | None,
) -> frozenset[int]:
    """Confirm a completed control-bot restart to every configured operator.

    This is intentionally best-effort: an administrator may have blocked the
    bot or never opened a private chat, and that must not turn a healthy
    restart into an unavailable control panel for the remaining operators.
    """

    if not worker_enabled:
        worker_status = (
            "Очередь рассылок и реакций временно выключена: накопленные задания не будут "
            "отправляться до отдельного включения."
        )
    elif campaign_id_scope is None:
        worker_status = "Очередь рассылок и реакций включена."
    else:
        campaign_labels = ", ".join(str(item) for item in sorted(campaign_id_scope))
        worker_status = (
            "Включен безопасный режим восстановления: обрабатываются только рассылки "
            f"#{campaign_labels}; остальные накопленные задания не затрагиваются."
        )
    message = (
        "✓ <b>Proxima перезапущен.</b>\n\n"
        "Панель управления запущена. "
        + worker_status
        + "\n\n"
        + _RESTART_UPDATE_NOTICE
    )
    results = await asyncio.gather(
        *(
            asyncio.wait_for(
                bot.send_message(chat_id=admin_id, text=message),
                timeout=_CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS,
            )
            for admin_id in sorted(admin_ids)
        ),
        return_exceptions=True,
    )
    failed_admin_ids: set[int] = set()
    for admin_id, result in zip(sorted(admin_ids), results, strict=True):
        if isinstance(result, TimeoutError):
            failed_admin_ids.add(admin_id)
            logger.warning(
                "Ready notification to administrator %s timed out after %.0f seconds",
                admin_id,
                _CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS,
            )
        elif isinstance(result, TelegramAPIError):
            failed_admin_ids.add(admin_id)
            logger.warning(
                "Could not send ready notification to administrator %s: %s",
                admin_id,
                result,
            )
        elif isinstance(result, Exception):  # noqa: BLE001 - readiness notice must stay best-effort
            failed_admin_ids.add(admin_id)
            logger.warning(
                "Unexpected error sending ready notification to administrator %s: %s",
                admin_id,
                result,
            )
    return frozenset(failed_admin_ids)


async def _retry_control_bot_ready_notification(
    bot: Bot,
    admin_ids: frozenset[int],
    *,
    worker_enabled: bool,
    campaign_id_scope: frozenset[int] | None,
) -> None:
    """Deliver a restart notice after a transient Telegram API outage.

    Startup must remain non-blocking, but a timed-out initial request should
    not silently discard the operator's requested deployment notification.
    Successful recipients are removed on every round, so retries never spam
    operators who already received it.
    """

    pending_admin_ids = admin_ids
    delay = _CONTROL_BOT_READY_RETRY_INITIAL_SECONDS
    while pending_admin_ids:
        await asyncio.sleep(delay)
        pending_admin_ids = await _notify_control_bot_ready(
            bot,
            pending_admin_ids,
            worker_enabled=worker_enabled,
            campaign_id_scope=campaign_id_scope,
        )
        if pending_admin_ids:
            logger.warning(
                "Restart notification remains pending for %d administrator(s); retrying later",
                len(pending_admin_ids),
            )
            delay = min(delay * 2, _CONTROL_BOT_READY_RETRY_MAX_SECONDS)


async def _set_control_bot_commands(bot: Bot) -> None:
    """Update optional command hints without delaying control-panel startup."""

    try:
        await asyncio.wait_for(
            bot.set_my_commands(
                [
                    BotCommand(command="admin", description="Открыть панель управления"),
                    BotCommand(command="proxies", description="Открыть пул прокси"),
                    BotCommand(command="help", description="Краткая справка"),
                    BotCommand(command="cancel", description="Отменить текущий ввод"),
                ]
            ),
            timeout=_CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "Updating control-bot commands timed out after %.0f seconds",
            _CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS,
        )
    except TelegramAPIError as exc:
        logger.warning("Could not update control-bot commands: %s", exc)


async def _start_polling_with_network_retries(
    dispatcher: Dispatcher,
    bot: Bot,
    **dependencies: object,
) -> None:
    """Keep workers alive if Telegram is unreachable during the initial getMe call.

    Aiogram retries transient failures after polling is established, but an outage
    during the initial connection raises out of ``start_polling``. Without this
    wrapper that one short outage tears down the already started campaign and
    reaction workers as well.
    """

    retry_seconds = _CONTROL_BOT_POLL_RETRY_INITIAL_SECONDS
    while True:
        try:
            await dispatcher.start_polling(
                bot,
                close_bot_session=False,
                **dependencies,
            )
            return
        except TelegramNetworkError as exc:
            logger.warning(
                "Control bot cannot reach Telegram yet: %s. "
                "Workers remain active; retrying polling in %.0f seconds.",
                exc,
                retry_seconds,
            )
            await asyncio.sleep(retry_seconds)
            retry_seconds = min(
                _CONTROL_BOT_POLL_RETRY_MAX_SECONDS,
                max(_CONTROL_BOT_POLL_RETRY_INITIAL_SECONDS, retry_seconds * 2),
            )


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
        try:
            with suppress(asyncio.CancelledError):
                await task
        except Exception:
            logger.exception("%s failed while cancelling it during shutdown", name)
    except asyncio.CancelledError:
        if asyncio.current_task().cancelling():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        return
    except Exception:
        logger.exception("%s stopped with an unexpected error", name)


async def _close_resource(name: str, close: Awaitable[None] | None) -> None:
    if close is None:
        return
    try:
        await close
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Could not close %s during shutdown", name)


async def main() -> None:
    settings = get_settings()
    token, admin_ids = settings.require_control_bot()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Telethon's update synchronizer is useful internally but can emit tens of
    # thousands of INFO lines while idle. Keep warnings/errors visible.
    logging.getLogger("telethon.client.updates").setLevel(logging.WARNING)
    logging.getLogger("telethon.network.mtprotosender").setLevel(logging.WARNING)
    logging.getLogger("telethon.crypto.libssl").setLevel(logging.WARNING)

    instance_lock = BotInstanceLock(control_bot_lock_path(token))
    try:
        instance_lock.acquire()
    except BotInstanceAlreadyRunning as exc:
        logger.error("Control bot was not started: %s", exc)
        raise SystemExit(2) from None
    try:
        await _run_control_bot(settings, token, admin_ids)
    finally:
        instance_lock.release()


async def _run_control_bot(
    settings: Settings,
    token: str,
    admin_ids: frozenset[int],
) -> None:

    database = Database(settings.database_url)
    if settings.auto_create_schema:
        await database.migrate_to_head_if_needed()

    proxy_service = ProxyService(database.session_factory, settings)
    account_service = AccountService(database.session_factory, settings, proxy_service)
    campaign_id_scope = settings.worker_campaign_id_scope
    scoped_worker_concurrency = 1 if campaign_id_scope is not None else settings.worker_concurrency
    gateway = build_gateway(
        database.session_factory,
        settings,
        account_service=account_service,
        proxy_service=proxy_service,
    )
    spambot_checks = SpamBotCheckService(database.session_factory, gateway, settings)
    scheduled_unknown_rechecks = await spambot_checks.prepare_legacy_unknown_rechecks()
    if scheduled_unknown_rechecks:
        logger.warning(
            "Scheduled %d legacy @SpamBot UNKNOWN account(s) for reclassification",
            scheduled_unknown_rechecks,
        )
    logger.info("Gateway mode: %s", "DRY RUN" if settings.dry_run else "LIVE (Telethon)")
    try:
        auto_assigned = await proxy_service.auto_assign_healthy_proxies(actor="startup")
        if auto_assigned:
            logger.info("Auto-assigned healthy proxies to %d account(s)", auto_assigned)
    except Exception as exc:
        logger.warning("Proxy auto-assignment encountered an issue: %s", exc)
    runner = CampaignRunner(
        database.session_factory,
        gateway,
        settings=settings,
        spambot_checks=spambot_checks,
    )
    preflight_service = PreflightService(
        database.session_factory,
        gateway,
        spambot_checks,
        runner,
        allow_auto_join=settings.account_auto_join,
        require_live_session=not settings.dry_run,
        max_pairs=settings.preflight_max_pairs,
        account_concurrency=settings.preflight_account_concurrency,
    )
    delayed_delivery_monitor = DelayedDeliveryMonitor(
        database.session_factory,
        gateway,
        runner,
        settings,
    )
    channel_comments = ChannelCommentService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id=f"bot-comments-{uuid4().hex[:10]}",
    )
    reactions = ReactionWorker(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id=f"bot-reactions-{uuid4().hex[:10]}",
        campaign_id_scope=campaign_id_scope,
    )
    participant_reactions = ParticipantReactionService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id=f"bot-participant-reactions-{uuid4().hex[:10]}",
    )
    direct_messages = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id=f"bot-direct-messages-{uuid4().hex[:10]}",
        proxy_service=proxy_service,
    )
    subscriber_invites = SubscriberInviteService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id=f"bot-subscriber-invites-{uuid4().hex[:10]}",
    )
    story_tagging = StoryTaggingService(
        database.session_factory,
        gateway,
        settings,
    )
    story_viewer = StoryViewerService(
        database.session_factory,
        gateway,
        settings,
    )
    if settings.worker_enabled:
        story_tagging.start()
        story_viewer.start()
    blacklist_parser = ChatBlacklistParser(database.session_factory, gateway, settings)
    run_queue = RunQueue(
        database.session_factory,
        lease_seconds=settings.worker_lease_seconds,
        max_attempts=settings.worker_max_attempts,
        retry_base_seconds=settings.worker_retry_base_seconds,
        retry_max_seconds=settings.worker_retry_max_seconds,
        max_retry_after_seconds=int(settings.max_flood_wait_seconds),
        require_live_session=not settings.dry_run,
        require_proxy_route=settings.account_require_proxy,
        account_auto_join=settings.account_auto_join,
        account_pace_seconds=settings.account_pace_max_seconds,
        account_daily_limit=settings.account_daily_limit,
        account_micro_break_every=settings.account_micro_break_every,
        account_micro_break_seconds=settings.account_micro_break_max_seconds,
        preflight_required=(settings.preflight_required_live and not settings.dry_run),
        preflight_max_age_seconds=settings.preflight_max_age_seconds,
        proxy_service=proxy_service,
        campaign_id_scope=campaign_id_scope,
        delivery_mode=getattr(gateway, "simulated", None),
    )
    spambot_maintenance = SpamBotMaintenanceWorker(spambot_checks, settings)
    queue_worker = QueueWorker(
        run_queue,
        runner,
        worker_id=f"bot-{uuid4().hex[:10]}",
        poll_seconds=settings.worker_poll_seconds,
        heartbeat_seconds=settings.worker_heartbeat_seconds,
        step_timeout_seconds=settings.worker_step_timeout_seconds,
        concurrency=scoped_worker_concurrency,
    )
    repository = AdminRepository(database.session_factory)
    await repository.ensure_system_account_groups(actor="bot-startup")
    bot = create_bot(token)

    async def send_worker_health_alert(text: str) -> None:
        if settings.bot_log_chat_id is None:
            return
        await bot.send_message(chat_id=settings.bot_log_chat_id, text=text)

    worker_supervisor = WorkerSupervisor(
        alert_sender=send_worker_health_alert if settings.bot_log_chat_id is not None else None
    )
    commands_task = asyncio.create_task(_set_control_bot_commands(bot), name="control-bot-commands")
    dispatcher = create_dispatcher(
        admin_ids,
        storage=DatabaseFsmStorage(database.session_factory),
    )
    worker_task: asyncio.Task[None] | None = None
    channel_comments_task: asyncio.Task[None] | None = None
    reactions_task: asyncio.Task[None] | None = None
    participant_reactions_task: asyncio.Task[None] | None = None
    direct_messages_task: asyncio.Task[None] | None = None
    subscriber_invites_task: asyncio.Task[None] | None = None
    delayed_delivery_task: asyncio.Task[None] | None = None
    spambot_task: asyncio.Task[None] | None = None
    notifier: RunNotifier | None = None
    notifier_task: asyncio.Task[None] | None = None
    ready_notification_task: asyncio.Task[None] | None = None
    if settings.worker_enabled:
        worker_task = worker_supervisor.start("campaigns", queue_worker.run_forever)
        if campaign_id_scope is None and settings.channel_comments_enabled:
            channel_comments_task = worker_supervisor.start(
                "channel-comments", channel_comments.run_forever
            )
        reactions_task = worker_supervisor.start("reactions", reactions.run_forever)
        if campaign_id_scope is None and settings.participant_reactions_enabled:
            participant_reactions_task = worker_supervisor.start(
                "participant-reactions", participant_reactions.run_forever
            )
        if campaign_id_scope is None and settings.direct_messages_enabled:
            direct_messages_task = worker_supervisor.start(
                "direct-messages", direct_messages.run_forever
            )
        if campaign_id_scope is None and settings.subscriber_invites_enabled:
            subscriber_invites_task = worker_supervisor.start(
                "subscriber-invites", subscriber_invites.run_forever
            )
        if campaign_id_scope is None and settings.delayed_delivery_verify_enabled:
            delayed_delivery_task = worker_supervisor.start(
                "delayed-delivery", delayed_delivery_monitor.run_forever
            )
    if (
        settings.worker_enabled
        and campaign_id_scope is None
        and settings.spambot_enabled
        and not settings.dry_run
    ):
        spambot_task = worker_supervisor.start("spambot", spambot_maintenance.run_forever)
    if settings.bot_log_chat_id is not None:

        async def send_notification(text: str) -> None:
            await bot.send_message(chat_id=settings.bot_log_chat_id, text=text)

        notifier = RunNotifier(database.session_factory, send_notification)
        notifier_task = worker_supervisor.start("notifier", notifier.run_forever)

    logger.info(
        "Control services are ready: worker=%s, comments=%s, direct_messages=%s, "
        "subscriber_invites=%s, campaign_scope=%s, worker_concurrency=%s, "
        "graceful stop=%.0f seconds",
        "enabled" if settings.worker_enabled else "disabled",
        "enabled" if channel_comments_task is not None else "disabled",
        "enabled" if direct_messages_task is not None else "disabled",
        "enabled" if subscriber_invites_task is not None else "disabled",
        ",".join(str(item) for item in sorted(campaign_id_scope))
        if campaign_id_scope is not None
        else "all",
        scoped_worker_concurrency,
        settings.shutdown_grace_seconds,
    )

    async def notify_ready() -> None:
        failed_ids = await _notify_control_bot_ready(
            bot,
            admin_ids,
            worker_enabled=settings.worker_enabled,
            campaign_id_scope=campaign_id_scope,
        )
        if failed_ids:
            await _retry_control_bot_ready_notification(
                bot,
                failed_ids,
                worker_enabled=settings.worker_enabled,
                campaign_id_scope=campaign_id_scope,
            )

    ready_notification_task = asyncio.create_task(
        notify_ready(), name="control-bot-ready-notification"
    )
    logger.info("Starting Proxima control bot for %d administrator(s)", len(admin_ids))
    try:
        await _start_polling_with_network_retries(
            dispatcher,
            bot,
            settings=settings,
            repository=repository,
            run_queue=run_queue,
            queue_worker=queue_worker,
            worker_supervisor=worker_supervisor,
            proxy_service=proxy_service,
            account_service=account_service,
            spambot_checks=spambot_checks,
            preflight_service=preflight_service,
            gateway=gateway,
            channel_comments=channel_comments,
            participant_reactions=participant_reactions,
            direct_messages=direct_messages,
            subscriber_invites=subscriber_invites,
            story_tagging=story_tagging,
            story_viewer=story_viewer,
            blacklist_parser=blacklist_parser,
        )
    except asyncio.CancelledError:
        # Windows sends Ctrl+C as a cancellation. Do not let it become a traceback.
        logger.info("Stop request received; finishing active work safely")
    finally:
        logger.info("Stopping Proxima control bot")
        queue_worker.stop()
        channel_comments.stop()
        reactions.stop()
        participant_reactions.stop()
        direct_messages.stop()
        subscriber_invites.stop()
        story_tagging.stop()
        story_viewer.stop()
        await asyncio.gather(story_tagging.wait_stopped(), story_viewer.wait_stopped())
        delayed_delivery_monitor.stop()
        spambot_maintenance.stop()
        if notifier is not None:
            notifier.stop()
        worker_supervisor.stop()
        await _finish_background_task(
            worker_task,
            name="Campaign worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            channel_comments_task,
            name="Channel comments worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            reactions_task,
            name="Reactions worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            participant_reactions_task,
            name="Participant reactions worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            direct_messages_task,
            name="Direct messages worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            subscriber_invites_task,
            name="Subscriber invites worker",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            delayed_delivery_task,
            name="Delayed delivery monitor",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            spambot_task,
            name="@SpamBot maintenance",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            notifier_task,
            name="Run notifier",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            ready_notification_task,
            name="Restart notification retry",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        await _finish_background_task(
            commands_task,
            name="Command-menu update",
            grace_seconds=settings.shutdown_grace_seconds,
        )
        close = getattr(gateway, "close", None)
        await _close_resource("Telegram gateway", close() if close is not None else None)
        await _close_resource("Bot session", bot.session.close())
        await database.dispose()
        logger.info("Proxima control bot stopped cleanly")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Covers Ctrl+C pressed before the async runtime has fully started.
        logger.info("Proxima control bot stopped by keyboard")


if __name__ == "__main__":
    run()
