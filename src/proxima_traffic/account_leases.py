"""Database-backed fencing leases for live Telegram account operations."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.gateways import TemporaryGatewayError
from proxima_traffic.models import AccountOperationLease, utc_now

logger = logging.getLogger(__name__)


class AccountOperationLeaseLost(TemporaryGatewayError):
    """Raised inside an operation when its cross-process lease is no longer valid."""

    def __init__(self) -> None:
        super().__init__(
            "account operation lease was lost",
            retry_after=5,
            code="account_operation_lease_lost",
        )


class AccountOperationLeaseCoordinator:
    """Serialize one account's Telegram session across independent processes."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner_id: str,
        lease_seconds: float,
        heartbeat_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._owner_id = owner_id
        self._lease_seconds = max(5.0, float(lease_seconds))
        self._heartbeat_seconds = min(
            max(1.0, float(heartbeat_seconds)),
            self._lease_seconds / 2,
        )

    @asynccontextmanager
    async def hold(self, account_id: int):
        token = await self._acquire(account_id)
        if token is None:
            yield None
            return
        heartbeat_stop = asyncio.Event()
        lease_lost = asyncio.Event()
        owner_task = asyncio.current_task()
        heartbeat_task = asyncio.create_task(
            self._heartbeat(
                account_id,
                token,
                heartbeat_stop,
                lease_lost,
                owner_task,
            ),
            name=f"account-operation-lease-{account_id}",
        )
        try:
            yield token
            if lease_lost.is_set():
                raise AccountOperationLeaseLost()
        except asyncio.CancelledError:
            if lease_lost.is_set():
                raise AccountOperationLeaseLost() from None
            raise
        finally:
            heartbeat_stop.set()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._release(account_id, token)

    async def _acquire(self, account_id: int) -> str | None:
        now = utc_now()
        token = uuid4().hex
        lease_until = now + timedelta(seconds=self._lease_seconds)
        try:
            async with self._session_factory() as session:
                renewed = await session.execute(
                    update(AccountOperationLease)
                    .where(
                        AccountOperationLease.account_id == account_id,
                        AccountOperationLease.lease_until <= now,
                    )
                    .values(
                        owner_id=self._owner_id,
                        lease_token=token,
                        lease_until=lease_until,
                    )
                )
                if renewed.rowcount == 1:
                    await session.commit()
                    return token
                existing = await session.get(AccountOperationLease, account_id)
                if existing is not None:
                    return None
                session.add(
                    AccountOperationLease(
                        account_id=account_id,
                        owner_id=self._owner_id,
                        lease_token=token,
                        lease_until=lease_until,
                    )
                )
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    return None
        except SQLAlchemyError:
            logger.exception("Could not acquire account operation lease for %s", account_id)
            return None
        return token

    async def _heartbeat(
        self,
        account_id: int,
        token: str,
        stop_event: asyncio.Event,
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
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
                        update(AccountOperationLease)
                        .where(
                            AccountOperationLease.account_id == account_id,
                            AccountOperationLease.owner_id == self._owner_id,
                            AccountOperationLease.lease_token == token,
                        )
                        .values(lease_until=utc_now() + timedelta(seconds=self._lease_seconds))
                    )
                    await session.commit()
            except Exception:  # noqa: BLE001 - caller remains bounded by its durable task lease
                logger.exception("Account operation lease heartbeat failed for %s", account_id)
                self._signal_lost(lost_event, owner_task)
                return
            if renewed.rowcount != 1:
                logger.warning("Account operation lease lost for %s", account_id)
                self._signal_lost(lost_event, owner_task)
                return

    @staticmethod
    def _signal_lost(
        lost_event: asyncio.Event,
        owner_task: asyncio.Task[object] | None,
    ) -> None:
        """Interrupt an RPC that no longer has exclusive session ownership."""

        lost_event.set()
        if owner_task is not None and not owner_task.done():
            owner_task.cancel()

    async def _release(self, account_id: int, token: str) -> None:
        try:
            async with self._session_factory() as session:
                await session.execute(
                    update(AccountOperationLease)
                    .where(
                        AccountOperationLease.account_id == account_id,
                        AccountOperationLease.owner_id == self._owner_id,
                        AccountOperationLease.lease_token == token,
                    )
                    .values(lease_until=utc_now())
                )
                await session.commit()
        except Exception:  # noqa: BLE001 - expiry lets another worker recover safely
            logger.exception("Account operation lease release failed for %s", account_id)
