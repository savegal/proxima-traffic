"""Durable late observations of confirmed auto-comments, without any re-sends."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.failure_policy import ACCOUNT_WIDE_CODES, failure_code
from proxima_traffic.gateways import GatewayError, MessageGateway, PermanentGatewayError
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ChannelCommentMessage,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, add_audit_event, safe_error_message
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)


class ChannelCommentDeliveryMonitor:
    """A bounded part of the shared delayed-delivery worker and batch budget."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        owner_id: str,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._owner_id = owner_id
        self._lease_seconds = 120.0
        self._heartbeat_seconds = 30.0

    def _owned(self, message_id: int, token: str) -> tuple:
        return (
            ChannelCommentMessage.id == message_id,
            ChannelCommentMessage.lease_owner == self._owner_id,
            ChannelCommentMessage.lease_token == token,
            ChannelCommentMessage.lease_until > utc_now(),
            ChannelCommentMessage.next_check_at.is_not(None),
        )

    async def check_next(self, reporter: WorkerIterationReporter | None = None) -> bool:
        claimed = await self._claim()
        if claimed is None:
            return False
        message_id, token = claimed
        stop = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(message_id, token, stop),
            name=f"comment-delivery-lease-{message_id}",
        )
        try:
            if reporter is not None:
                await reporter()
            try:
                async with asyncio.timeout(
                    float(self._settings.delayed_delivery_verify_timeout_seconds)
                ):
                    await self._check(message_id, token)
            except asyncio.CancelledError:
                # A process restart may reclaim the expired lease safely.
                raise
            except Exception as exc:  # noqa: BLE001 - persist inconclusive reads, keep worker alive
                await self._finish(message_id, token, "unavailable", exc, retry=True)
                if reporter is not None:
                    await reporter(exc)
                if not isinstance(exc, (GatewayError, TimeoutError)):
                    logger.exception("Auto-comment verification failed for message %s", message_id)
        finally:
            stop.set()
            await heartbeat
        return True

    async def _claim(self) -> tuple[int, str] | None:
        now = utc_now()
        available = or_(
            ChannelCommentMessage.lease_until.is_(None),
            ChannelCommentMessage.lease_until <= now,
        )
        token = uuid4().hex
        async with self._session_factory() as session:
            message_id = await session.scalar(
                select(ChannelCommentMessage.id)
                .where(ChannelCommentMessage.next_check_at <= now, available)
                .order_by(ChannelCommentMessage.next_check_at, ChannelCommentMessage.id)
                .limit(1)
            )
            if message_id is None:
                return None
            claimed = await session.execute(
                update(ChannelCommentMessage)
                .where(
                    ChannelCommentMessage.id == message_id,
                    ChannelCommentMessage.next_check_at <= now,
                    available,
                )
                .values(
                    lease_owner=self._owner_id,
                    lease_token=token,
                    lease_until=now + timedelta(seconds=self._lease_seconds),
                )
                .execution_options(synchronize_session=False)
            )
            await session.commit()
            return (int(message_id), token) if claimed.rowcount == 1 else None

    async def _heartbeat(self, message_id: int, token: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._heartbeat_seconds)
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    result = await session.execute(
                        update(ChannelCommentMessage)
                        .where(*self._owned(message_id, token))
                        .values(lease_until=utc_now() + timedelta(seconds=self._lease_seconds))
                        .execution_options(synchronize_session=False)
                    )
                    await session.commit()
                    if result.rowcount != 1:
                        return
            except Exception:  # noqa: BLE001 - expired leases are recoverable
                logger.exception("Auto-comment verification heartbeat failed for %s", message_id)
                return

    async def _check(self, message_id: int, token: str) -> None:
        async with self._session_factory() as session:
            message = await session.scalar(
                select(ChannelCommentMessage).where(*self._owned(message_id, token))
            )
            if message is None:
                return

        async def inspect() -> datetime | None:
            # Re-read after obtaining the account lock: a preceding operation
            # may have paused the account or invalidated this message's lease.
            async with self._session_factory() as session:
                if await session.scalar(
                    select(ChannelCommentMessage.id).where(*self._owned(message_id, token))
                ) is None:
                    return None
                account = await session.get(Account, message.account_id)
                if account is None:
                    raise PermanentGatewayError(
                        "Исходный аккаунт недоступен для проверки",
                        code="comment_verification_unavailable",
                    )
                cooldown = ensure_utc(account.cooldown_until)
                if cooldown is not None and cooldown > utc_now():
                    return cooldown
                if account.status != AccountStatus.ACTIVE:
                    raise PermanentGatewayError(
                        "Исходный аккаунт остановлен; проверить сообщение нельзя",
                        code="comment_verification_unavailable",
                    )
            await self._gateway.verify_channel_comment(
                account=account,
                channel_ref=message.channel_ref,
                channel_post_id=message.source_post_id,
                message_id=message.message_id,
                discussion_peer_id=message.discussion_peer_id,
                discussion_access_hash=message.discussion_access_hash,
            )
            return None

        try:
            resume_at = await self._runner.run_account_operation(message.account_id, inspect)
        except GatewayError as exc:
            code = failure_code(exc, "comment_verification_failed")
            if code in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(message.account_id, exc)
            if code == "comment_missing":
                await self._finish(message_id, token, "missing", exc)
            else:
                await self._finish(
                    message_id, token, "unavailable", exc, retry=bool(exc.retryable)
                )
            return
        if resume_at is not None:
            await self._defer(message_id, token, resume_at)
        else:
            await self._finish(message_id, token, "present")

    async def _defer(self, message_id: int, token: str, resume_at: datetime) -> None:
        async with self._session_factory() as session:
            await session.execute(
                update(ChannelCommentMessage)
                .where(*self._owned(message_id, token))
                .values(
                    next_check_at=resume_at,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
                .execution_options(synchronize_session=False)
            )
            await session.commit()

    async def _finish(
        self,
        message_id: int,
        token: str,
        status: str,
        exc: BaseException | None = None,
        *,
        retry: bool = False,
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            message = await session.scalar(
                select(ChannelCommentMessage).where(*self._owned(message_id, token))
            )
            if message is None:
                return
            final_at = ensure_utc(message.edited_at or message.delivered_at) + timedelta(
                seconds=int(self._settings.channel_comments_verify_final_delay_seconds)
            )
            code = failure_code(exc, "comment_verification_failed") if exc is not None else None
            attempts = int(message.attempt_count) + 1
            next_check_at: datetime | None = None
            stage = int(message.check_stage)
            first_status = message.first_check_status
            first_at = message.first_checked_at
            # Telegram throttling uses the account cooldown and does not exhaust
            # the retry budget or get misclassified as a disappeared comment.
            throttled = code in {"flood_wait", "peer_flood", "account_restricted"}
            if (
                status == "unavailable"
                and retry
                and (
                    throttled or attempts < int(self._settings.channel_comments_verify_max_attempts)
                )
            ):
                delay = max(
                    int(self._settings.delayed_delivery_verify_retry_seconds),
                    int(getattr(exc, "retry_after", 0) or 0),
                )
                next_check_at = now + timedelta(seconds=delay)
                if throttled:
                    attempts = int(message.attempt_count)
                account = await session.get(Account, message.account_id)
                cooldown = ensure_utc(account.cooldown_until) if account is not None else None
                if cooldown is not None:
                    next_check_at = max(next_check_at, cooldown)
            else:
                if stage == 0:
                    first_status, first_at = status, now
                if stage == 0 and status != "missing" and final_at > now:
                    stage, next_check_at = 1, final_at
                else:
                    stage = 2
                attempts = 0
            result = await session.execute(
                update(ChannelCommentMessage)
                .where(*self._owned(message_id, token))
                .values(
                    status=status,
                    checked_at=now,
                    first_check_status=first_status,
                    first_checked_at=first_at,
                    check_stage=stage,
                    next_check_at=next_check_at,
                    attempt_count=attempts,
                    error=safe_error_message(exc) if exc is not None else None,
                    error_code=code,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                await session.rollback()
                return
            add_audit_event(
                session,
                event_type="channel_comment.delivery_observed",
                entity_type="channel_comment_run",
                entity_id=message.run_id,
                details={
                    "message_id": message.message_id,
                    "account_id": message.account_id,
                    "status": status,
                    "check_stage": stage,
                    "error_code": code,
                },
                actor="comment-delivery-monitor",
            )
            await session.commit()
