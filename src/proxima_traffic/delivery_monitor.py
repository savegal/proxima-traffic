"""Low-volume delayed verification of messages accepted by Telegram."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.failure_policy import (
    ACCOUNT_WIDE_CODES,
    OBSERVED_TARGET_FAILURE_CODES,
    failure_code,
)
from proxima_traffic.gateways import GatewayError, MessageGateway
from proxima_traffic.models import Account, ActionLog, ActionStatus, Target, ensure_utc, utc_now
from proxima_traffic.services import CampaignRunner
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)


class DelayedDeliveryMonitor:
    """Check each sent campaign message once after a configurable grace period."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        runner: CampaignRunner,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._stopped = asyncio.Event()
        self._owner_id = f"{socket.gethostname()[:40]}:{os.getpid()}:{uuid4().hex}"
        self._lease_seconds = 120.0
        self._heartbeat_seconds = 30.0

    def stop(self) -> None:
        self._stopped.set()

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stopped.is_set():
            try:
                checked = await self.check_due_once(reporter=reporter)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - monitoring must never stop deliveries
                logger.exception("Delayed delivery monitor iteration failed")
                if reporter is not None:
                    await reporter(exc)
            else:
                if reporter is not None and checked == 0:
                    await reporter()
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=float(self._settings.delayed_delivery_verify_poll_seconds),
                )
            except TimeoutError:
                pass

    async def check_due_once(self, reporter: WorkerIterationReporter | None = None) -> int:
        if not self._settings.delayed_delivery_verify_enabled:
            return 0
        checked = 0
        for _ in range(max(1, int(self._settings.delayed_delivery_verify_batch_size))):
            claimed = await self._claim_due_action()
            if claimed is None:
                break
            action_id, token = claimed
            heartbeat_stop = asyncio.Event()
            heartbeat_task = asyncio.create_task(
                self._heartbeat(action_id, token, heartbeat_stop, reporter=reporter),
                name=f"delayed-delivery-lease-{action_id}",
            )
            if reporter is not None:
                await reporter()
            try:
                try:
                    async with asyncio.timeout(
                        float(self._settings.delayed_delivery_verify_timeout_seconds)
                    ):
                        await self._check_action(action_id, token)
                except TimeoutError:
                    await self._defer_retry(action_id, token)
                    raise TimeoutError(
                        f"delayed delivery check timed out for action {action_id}"
                    ) from None
            finally:
                heartbeat_stop.set()
                await heartbeat_task
            checked += 1
            if reporter is not None:
                await reporter()
        return checked

    async def _claim_due_action(self) -> tuple[int, str] | None:
        now = utc_now()
        cutoff = now - timedelta(seconds=int(self._settings.delayed_delivery_verify_delay_seconds))
        token = uuid4().hex
        available = or_(
            ActionLog.delayed_delivery_lease_until.is_(None),
            ActionLog.delayed_delivery_lease_until <= now,
        )
        due = or_(
            ActionLog.delayed_delivery_retry_at.is_(None),
            ActionLog.delayed_delivery_retry_at <= now,
        )
        async with self._session_factory() as session:
            action_id = await session.scalar(
                select(ActionLog.id)
                .where(
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.external_message_id.is_not(None),
                    ActionLog.delayed_delivery_checked_at.is_(None),
                    due,
                    available,
                    ActionLog.created_at <= cutoff,
                )
                .order_by(ActionLog.created_at)
                .limit(1)
            )
            if action_id is None:
                return None
            claimed = await session.execute(
                update(ActionLog)
                .where(
                    ActionLog.id == action_id,
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.delayed_delivery_checked_at.is_(None),
                    due,
                    available,
                )
                .values(
                    delayed_delivery_lease_owner=self._owner_id,
                    delayed_delivery_lease_token=token,
                    delayed_delivery_lease_until=now + timedelta(seconds=self._lease_seconds),
                )
            )
            if claimed.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
            return int(action_id), token

    async def _heartbeat(
        self,
        action_id: int,
        token: str,
        stop_event: asyncio.Event,
        *,
        reporter: WorkerIterationReporter | None = None,
    ) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._heartbeat_seconds)
                return
            except TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    renewed = await session.execute(
                        update(ActionLog)
                        .where(
                            ActionLog.id == action_id,
                            ActionLog.delayed_delivery_checked_at.is_(None),
                            ActionLog.delayed_delivery_lease_owner == self._owner_id,
                            ActionLog.delayed_delivery_lease_token == token,
                        )
                        .values(
                            delayed_delivery_lease_until=utc_now()
                            + timedelta(seconds=self._lease_seconds)
                        )
                    )
                    await session.commit()
                    if reporter is not None:
                        await reporter()
            except Exception:  # noqa: BLE001 - the expired lease safely permits recovery
                logger.exception("Delayed-delivery lease heartbeat failed for action %s", action_id)
                return
            if renewed.rowcount != 1:
                logger.warning("Delayed-delivery lease lost for action %s", action_id)
                return

    async def _check_action(self, action_id: int, token: str) -> None:
        async with self._session_factory() as session:
            action = await session.get(ActionLog, action_id)
            if (
                action is None
                or action.status != ActionStatus.SUCCEEDED
                or action.external_message_id is None
                or action.delayed_delivery_checked_at is not None
                or action.delayed_delivery_lease_owner != self._owner_id
                or action.delayed_delivery_lease_token != token
            ):
                return
            account = await session.get(Account, action.account_id)
            target = await session.get(Target, action.target_id)
            if account is None or target is None:
                action.delayed_delivery_checked_at = utc_now()
                action.delayed_delivery_lease_owner = None
                action.delayed_delivery_lease_token = None
                action.delayed_delivery_lease_until = None
                await session.commit()
                return
            account_id = int(account.id)
            target_id = int(target.id)
            message_id = int(action.external_message_id)
            reset_at = ensure_utc(target.quality_reset_at)
            action_created_at = ensure_utc(action.created_at)
            if (
                reset_at is not None
                and action_created_at is not None
                and action_created_at < reset_at
            ):
                action.delayed_delivery_checked_at = utc_now()
                action.delayed_delivery_lease_owner = None
                action.delayed_delivery_lease_token = None
                action.delayed_delivery_lease_until = None
                await session.commit()
                return

        async def inspect() -> None:
            await self._gateway.verify_sent_message(
                account=account,
                target=target,
                message_id=message_id,
            )

        try:
            await self._runner.run_account_operation(account_id, inspect)
        except GatewayError as exc:
            code = failure_code(exc, "delivery_verification_failed")
            if code in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(account_id, exc)
            if code == "post_deleted_by_moderation":
                recovered = await self._runner.recover_delayed_membership_gate(
                    action_id=action_id,
                    delayed_delivery_token=token,
                    account_id=account_id,
                    target_id=target_id,
                    message_id=message_id,
                    exc=exc,
                )
                if not recovered:
                    await self._runner.record_observed_post_deletion(
                        account_id=account_id,
                        target_id=target_id,
                        message_id=message_id,
                        exc=exc,
                    )
            elif code in OBSERVED_TARGET_FAILURE_CODES:
                await self._runner.record_observed_target_failure(account_id, target_id, exc)
            elif getattr(exc, "retryable", True):
                await self._defer_retry(action_id, token)
                return
            await self._mark_checked(action_id, token)
            return
        await self._mark_checked(action_id, token)

    async def _mark_checked(self, action_id: int, token: str) -> None:
        async with self._session_factory() as session:
            action = await session.get(ActionLog, action_id)
            if (
                action is not None
                and action.delayed_delivery_checked_at is None
                and action.delayed_delivery_lease_owner == self._owner_id
                and action.delayed_delivery_lease_token == token
            ):
                action.delayed_delivery_checked_at = utc_now()
                action.delayed_delivery_retry_at = None
                action.delayed_delivery_lease_owner = None
                action.delayed_delivery_lease_token = None
                action.delayed_delivery_lease_until = None
                await session.commit()

    async def _defer_retry(self, action_id: int, token: str) -> None:
        async with self._session_factory() as session:
            action = await session.get(ActionLog, action_id)
            if (
                action is not None
                and action.delayed_delivery_checked_at is None
                and action.delayed_delivery_lease_owner == self._owner_id
                and action.delayed_delivery_lease_token == token
            ):
                action.delayed_delivery_retry_at = utc_now() + timedelta(
                    seconds=int(self._settings.delayed_delivery_verify_retry_seconds)
                )
                action.delayed_delivery_lease_owner = None
                action.delayed_delivery_lease_token = None
                action.delayed_delivery_lease_until = None
                await session.commit()
