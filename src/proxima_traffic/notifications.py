from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable
from datetime import timedelta
from html import escape
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.models import Campaign, CampaignRun, RunStatus, Target, utc_now
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
            stored = await session.execute(
                update(CampaignRun)
                .where(
                    CampaignRun.id == run_id,
                    CampaignRun.notified_at.is_(None),
                    CampaignRun.notification_lease_owner == self._owner_id,
                    CampaignRun.notification_lease_token == token,
                )
                .values(
                    notified_at=utc_now(),
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
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, run.campaign_id)
            target = await session.get(Target, run.target_id)
        if run.repeat_until is not None:
            if run.status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED}:
                return None
        icon = {
            RunStatus.SUCCEEDED: "✅",
            RunStatus.FAILED: "❌",
            RunStatus.CANCELLED: "🚫",
        }[run.status]
        campaign_name = escape(campaign.name) if campaign is not None else f"#{run.campaign_id}"
        target_name = escape(target.title) if target is not None else f"#{run.target_id}"
        lines = [
            f"{icon} <b>Запуск #{run.id}: {run.status.value}</b>",
            f"Кампания: <b>{campaign_name}</b>",
            f"Цель: <b>{target_name}</b>",
            f"Попытки: {run.attempt_count}/{run.max_attempts}",
            f"Message ID: <code>{run.primary_message_id or '—'}</code> → "
            f"<code>{run.reply_message_id or '—'}</code>",
        ]
        if run.error:
            lines.append(f"Ошибка: <code>{escape(run.error[:500])}</code>")
        return "\n".join(lines)
