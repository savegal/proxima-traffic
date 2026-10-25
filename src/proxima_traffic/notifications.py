from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable
from datetime import timedelta
from html import escape
from uuid import uuid4

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from proxima_traffic.failure_policy import (
    ACCEPTED_CAMPAIGN_ACTION_CODES,
    CAMPAIGN_UNCERTAIN_CODES,
    failure_label,
)
from proxima_traffic.models import (
    ActionLog,
    ActionStatus,
    Campaign,
    CampaignRun,
    RunStatus,
    Target,
    ensure_utc,
    utc_now,
)
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)
NotificationSender = Callable[[str], Awaitable[None]]


class RunNotifier:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: NotificationSender,
        *,
        poll_seconds: float = 2.0,
        lease_seconds: float = 120.0,
        heartbeat_seconds: float = 5.0,
        retry_base_seconds: float = 15.0,
        retry_max_seconds: float = 3600.0,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._poll_seconds = poll_seconds
        self._lease_seconds = max(5.0, lease_seconds)
        self._heartbeat_seconds = min(max(1.0, heartbeat_seconds), self._lease_seconds / 2)
        self._retry_base_seconds = max(1.0, retry_base_seconds)
        self._retry_max_seconds = max(self._retry_base_seconds, retry_max_seconds)
        self._owner_id = f"{socket.gethostname()[:40]}:{os.getpid()}:{uuid4().hex}"
        self._stop_event = asyncio.Event()

    async def process_pending(self, limit: int = 20) -> int:
        sent = 0
        for _ in range(limit):
            claimed = await self._claim_next()
            if claimed is None:
                break
            run, token = claimed
            heartbeat_stop = asyncio.Event()
            heartbeat_task = asyncio.create_task(
                self._heartbeat(run.id, token, heartbeat_stop),
                name=f"notification-lease-{run.id}",
            )
            try:
                message = await self._render(run)
                if message is not None:
                    await self._sender(message)
            except Exception as exc:  # noqa: BLE001 - notification delivery is best effort
                logger.exception("Could not send notification for run %s", run.id)
                await self._mark_failed(run.id, token, str(exc))
            else:
                if await self._mark_delivered(run.id, token) and message is not None:
                    sent += 1
            finally:
                heartbeat_stop.set()
                await heartbeat_task
        return sent

    async def _claim_next(self) -> tuple[CampaignRun, str] | None:
        now = utc_now()
        token = uuid4().hex
        available = or_(
            CampaignRun.notification_lease_until.is_(None),
            CampaignRun.notification_lease_until <= now,
        )
        due = or_(
            CampaignRun.notification_next_attempt_at.is_(None),
            CampaignRun.notification_next_attempt_at <= now,
        )
        launch_peer = aliased(CampaignRun)
        launch_has_active_work = (
            select(launch_peer.id)
            .where(
                launch_peer.launch_key == CampaignRun.launch_key,
                launch_peer.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]),
            )
            .exists()
        )
        launch_anchor_id = (
            select(func.min(launch_peer.id))
            .where(launch_peer.launch_key == CampaignRun.launch_key)
            .scalar_subquery()
        )
        launch_summary_due = and_(
            CampaignRun.launch_key.is_not(None),
            CampaignRun.repeat_until.is_(None),
            CampaignRun.id == launch_anchor_id,
            ~launch_has_active_work,
        )
        # Legacy standalone runs retain their individual delivery message. A
        # finite launch with a shared key is represented by its durable anchor
        # once all of its tasks have reached a terminal state. Repeating smart
        # runs deliberately preserve their existing per-run suppression.
        notification_due = or_(
            CampaignRun.launch_key.is_(None),
            CampaignRun.repeat_until.is_not(None),
            launch_summary_due,
        )
        async with self._session_factory() as session:
            run_id = await session.scalar(
                select(CampaignRun.id)
                .where(
                    CampaignRun.status.in_(
                        [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED]
                    ),
                    CampaignRun.notified_at.is_(None),
                    available,
                    due,
                    notification_due,
                )
                .order_by(CampaignRun.id)
                .limit(1)
            )
            if run_id is None:
                return None
            claimed = await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run_id,
                    CampaignRun.notified_at.is_(None),
                    available,
                    due,
                )
                .values(
                    notification_lease_owner=self._owner_id,
                    notification_lease_token=token,
                    notification_lease_until=now + timedelta(seconds=self._lease_seconds),
                    notification_attempt_count=CampaignRun.notification_attempt_count + 1,
                    notification_last_error=None,
                )
            )
            if claimed.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
            run = await session.get(CampaignRun, run_id)
            if run is None:
                return None
            return run, token

    async def _heartbeat(self, run_id: int, token: str, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._heartbeat_seconds)
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await session.execute(
                        update(CampaignRun)
                        .where(
                            CampaignRun.id == run_id,
                            CampaignRun.notified_at.is_(None),
                            CampaignRun.notification_lease_owner == self._owner_id,
                            CampaignRun.notification_lease_token == token,
                        )
                        .values(
                            notification_lease_until=utc_now()
                            + timedelta(seconds=self._lease_seconds)
                        )
                    )
                    await session.commit()
            except Exception:  # noqa: BLE001 - expiry lets another notifier recover safely
                logger.exception("Notification lease heartbeat failed for run %s", run_id)
                return
            if renewed.rowcount != 1:
                logger.warning("Notification lease lost for run %s", run_id)
                return

    async def _mark_delivered(self, run_id: int, token: str) -> bool:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                return False
            is_launch_summary = self._is_finite_launch_summary(run)
            now = utc_now()
            stored = await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run_id,
                    CampaignRun.notified_at.is_(None),
                    CampaignRun.notification_lease_owner == self._owner_id,
                    CampaignRun.notification_lease_token == token,
                )
                .values(
                    notified_at=now,
                    notification_lease_owner=None,
                    notification_lease_token=None,
                    notification_lease_until=None,
                    notification_next_attempt_at=None,
                    notification_last_error=None,
                )
            )
            if stored.rowcount == 1 and is_launch_summary and run.launch_key is not None:
                # The anchor owns the summary notification. Mark all terminal
                # siblings delivered together only after that notification is
                # accepted, so retry and leasing remain durable and no later
                # per-chat messages can flood the operator's log chat.
                await session.execute(
                    update(CampaignRun)
                    .where(
                        CampaignRun.launch_key == run.launch_key,
                        CampaignRun.status.in_(
                            [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED]
                        ),
                        CampaignRun.notified_at.is_(None),
                    )
                    .values(
                        notified_at=now,
                        notification_lease_owner=None,
                        notification_lease_token=None,
                        notification_lease_until=None,
                        notification_next_attempt_at=None,
                        notification_last_error=None,
                    )
                )
            await session.commit()
            return stored.rowcount == 1

    async def _mark_failed(self, run_id: int, token: str, error: str) -> None:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if (
                run is None
                or run.notified_at is not None
                or run.notification_lease_owner != self._owner_id
                or run.notification_lease_token != token
            ):
                return
            retry_seconds = min(
                self._retry_max_seconds,
                self._retry_base_seconds * (2 ** max(run.notification_attempt_count - 1, 0)),
            )
            run.notification_lease_owner = None
            run.notification_lease_token = None
            run.notification_lease_until = None
            run.notification_next_attempt_at = utc_now() + timedelta(seconds=retry_seconds)
            run.notification_last_error = error[:500] or "notification delivery failed"
            await session.commit()

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop_event.is_set():
            try:
                processed = await self.process_pending()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - notification failures must not end the worker
                logger.exception("Run-notifier iteration failed")
                if reporter is not None:
                    await reporter(exc)
                processed = 0
            else:
                if reporter is not None:
                    await reporter()
            if processed:
                continue
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()

    async def _render(self, run: CampaignRun) -> str | None:
        if self._is_finite_launch_summary(run):
            return await self._render_finite_launch_summary(run)
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, run.campaign_id)
            target = await session.get(Target, run.target_id)
            delivery, _ = await self._delivery_summary(session, ActionLog.run_id == run.id)
        if run.repeat_until is not None:
            if run.status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED}:
                return None
        icon = {
            RunStatus.SUCCEEDED: "✓",
            RunStatus.FAILED: "×",
            RunStatus.CANCELLED: "Ⅱ",
        }[run.status]
        status = {
            RunStatus.SUCCEEDED: "завершён",
            RunStatus.FAILED: "ошибка",
            RunStatus.CANCELLED: "остановлен",
        }[run.status]
        if run.status == RunStatus.FAILED and run.error_code in CAMPAIGN_UNCERTAIN_CODES:
            status = "результат не подтверждён"
        campaign_name = escape(campaign.name) if campaign is not None else f"#{run.campaign_id}"
        target_name = escape(target.title) if target is not None else f"#{run.target_id}"
        lines = [
            f"<b>{icon} Запуск #{run.id}: {status}</b>",
            f"<i>{campaign_name}</i>",
            "",
            f"Чат: <b>{target_name}</b>",
            f"Попытки: {run.attempt_count}/{run.max_attempts}",
            f"Идентификаторы шагов: <code>{run.primary_message_id or '—'}</code> → "
            f"<code>{run.reply_message_id or '—'}</code>",
            *delivery,
        ]
        if run.error:
            lines.extend(
                [
                    "",
                    f"<b>Причина: {escape(failure_label(run.error_code))}</b>",
                    f"<code>{escape(run.error[:500])}</code>",
                ]
            )
        return "\n".join(lines)

    @staticmethod
    def _is_finite_launch_summary(run: CampaignRun) -> bool:
        return run.launch_key is not None and run.repeat_until is None

    async def _render_finite_launch_summary(self, anchor: CampaignRun) -> str:
        """Render one concise result for all tasks created by a finite launch."""

        if anchor.launch_key is None:  # pragma: no cover - guarded by caller
            raise RuntimeError("finite launch summary requires a launch key")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, anchor.campaign_id)
            runs = list(
                await session.scalars(
                    select(CampaignRun)
                    .where(CampaignRun.launch_key == anchor.launch_key)
                    .order_by(CampaignRun.id)
                )
            )
            delivery, delivered_targets = await self._delivery_summary(
                session,
                ActionLog.run_id.in_(
                    select(CampaignRun.id).where(
                        CampaignRun.launch_key == anchor.launch_key,
                    )
                ),
            )
        succeeded = sum(run.status == RunStatus.SUCCEEDED for run in runs)
        failed = sum(run.status == RunStatus.FAILED for run in runs)
        uncertain = sum(
            run.status == RunStatus.FAILED and run.error_code in CAMPAIGN_UNCERTAIN_CODES
            for run in runs
        )
        cancelled = sum(run.status == RunStatus.CANCELLED for run in runs)
        started_at = min((run.created_at for run in runs), default=anchor.created_at)
        finished_at = max(
            (run.finished_at or run.updated_at for run in runs),
            default=anchor.updated_at,
        )
        duration_seconds = max(
            0, int((ensure_utc(finished_at) - ensure_utc(started_at)).total_seconds())
        )
        if duration_seconds >= 3600:
            duration = f"{duration_seconds // 3600} ч {duration_seconds % 3600 // 60} мин"
        elif duration_seconds >= 60:
            duration = f"{duration_seconds // 60} мин"
        else:
            duration = "меньше минуты"
        campaign_name = escape(campaign.name) if campaign is not None else f"#{anchor.campaign_id}"
        lines = [
            "<b>Рассылка завершена</b>",
            f"<i>{campaign_name} · запуск #{anchor.id}</i>",
            "",
            "<b>Результат</b>",
            f"✓ Успешных задач: <b>{succeeded}</b>",
            *delivery,
            f"× Задач с ошибкой: <b>{max(0, failed - uncertain)}</b>",
            *([f"? Результат задач не подтверждён: <b>{uncertain}</b>"] if uncertain else []),
            f"Ⅱ Пропущено/остановлено: <b>{cancelled}</b>",
            "",
            f"Чатов с доставкой: <b>{delivered_targets}</b>",
            f"Длительность: <b>{duration}</b>",
        ]
        if failed:
            failed_codes: dict[str, int] = {}
            for run in runs:
                if run.status == RunStatus.FAILED:
                    code = run.error_code or "неизвестная ошибка"
                    failed_codes[code] = failed_codes.get(code, 0) + 1
            summary = ", ".join(
                f"{escape(failure_label(code))} × {count}"
                for code, count in sorted(
                    failed_codes.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:3]
            )
            lines.extend(["", "<b>Причины ошибок</b>", summary])
        return "\n".join(lines)

    @staticmethod
    async def _delivery_summary(session: AsyncSession, condition: object) -> tuple[list[str], int]:
        """Count transport receipts, never completed runs or synthetic message IDs."""

        category = case(
            (ActionLog.simulated.is_(True), "simulated"),
            (
                and_(ActionLog.simulated.is_(False), ActionLog.external_message_id.is_not(None)),
                "confirmed",
            ),
            else_="unknown",
        )
        rows = (
            await session.execute(
                select(
                    category,
                    func.count(ActionLog.id),
                    func.count(func.distinct(ActionLog.target_id)),
                )
                .where(
                    condition,
                    or_(
                        ActionLog.status == ActionStatus.SUCCEEDED,
                        and_(
                            ActionLog.external_message_id.is_not(None),
                            ActionLog.error_code.in_(ACCEPTED_CAMPAIGN_ACTION_CODES),
                        ),
                    ),
                )
                .group_by(category)
            )
        ).all()
        counts = {kind: int(count) for kind, count, _ in rows}
        targets = next((int(total) for kind, _, total in rows if kind == "confirmed"), 0)
        lines = [f"✓ Отправлено и подтверждено: <b>{counts.get('confirmed', 0)}</b>"]
        if counts.get("simulated"):
            lines.append(f"◇ Симуляций: <b>{counts['simulated']}</b>")
        if counts.get("unknown"):
            lines.append(f"? Успешных шагов без сохранённого режима: <b>{counts['unknown']}</b>")
        if not counts:
            lines.append("Режим и доставка старых шагов не подтверждены журналом.")
        return lines, targets
