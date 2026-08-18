from __future__ import annotations

import asyncio

import pytest

from proxima_traffic.account_leases import (
    AccountOperationLeaseCoordinator,
    AccountOperationLeaseLost,
)
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.gateways import DryRunGateway
from proxima_traffic.models import Account, AccountOperationLease
from proxima_traffic.services import AccountSendDeferred, CampaignRunner


@pytest.mark.asyncio
async def test_account_operation_lease_fences_a_second_process(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'account-leases.db'}",
        worker_enabled=False,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="lease-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        first = AccountOperationLeaseCoordinator(
            database.session_factory,
            owner_id="process-one",
            lease_seconds=5,
            heartbeat_seconds=1,
        )
        second = AccountOperationLeaseCoordinator(
            database.session_factory,
            owner_id="process-two",
            lease_seconds=5,
            heartbeat_seconds=1,
        )
        async with first.hold(account.id) as first_token:
            assert first_token is not None
            async with second.hold(account.id) as second_token:
                assert second_token is None
        async with second.hold(account.id) as second_token:
            assert second_token is not None
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_lost_account_operation_lease_interrupts_the_owner(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'account-lease-loss.db'}",
        worker_enabled=False,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="lease-loss-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        coordinator = AccountOperationLeaseCoordinator(
            database.session_factory,
            owner_id="process-one",
            lease_seconds=5,
            heartbeat_seconds=1,
        )
        coordinator._heartbeat_seconds = 0.01  # type: ignore[attr-defined]
        entered = asyncio.Event()

        async def hold_operation() -> None:
            with pytest.raises(AccountOperationLeaseLost):
                async with coordinator.hold(account.id) as token:
                    assert token is not None
                    entered.set()
                    await asyncio.Event().wait()

        operation = asyncio.create_task(hold_operation())
        await asyncio.wait_for(entered.wait(), timeout=1)
        async with database.session_factory() as session:
            lease = await session.get(AccountOperationLease, account.id)
            assert lease is not None
            lease.lease_token = "taken-over"
            await session.commit()
        await asyncio.wait_for(operation, timeout=1)
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_account_send_reservation_is_atomic_across_runner_instances(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'account-send-reservation.db'}",
        worker_enabled=False,
        account_pace_min_seconds=60,
        account_pace_max_seconds=60,
        account_micro_break_every=0,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="reserved-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        first = CampaignRunner(
            database.session_factory,
            DryRunGateway(),
            settings=settings,
        )
        second = CampaignRunner(
            database.session_factory,
            DryRunGateway(),
            settings=settings,
        )
        started = asyncio.Event()
        release = asyncio.Event()

        async def first_send() -> None:
            started.set()
            await release.wait()
            await first.record_external_account_send(account.id)

        first_task = asyncio.create_task(first.run_account_send_operation(account.id, first_send))
        await asyncio.wait_for(started.wait(), timeout=1)

        with pytest.raises(AccountSendDeferred):
            await second.run_account_send_operation(account.id, lambda: asyncio.sleep(0))

        release.set()
        await first_task
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            assert stored.sent_today == 1
            assert stored.next_send_at is not None
            assert stored.send_reservation_token is None
            assert stored.send_reservation_until is None
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_external_account_send_is_counted_once_per_operation_key(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'account-send-receipt.db'}",
        worker_enabled=False,
        account_micro_break_every=0,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="idempotent-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        runner = CampaignRunner(database.session_factory, DryRunGateway(), settings=settings)

        assert (
            await runner.record_external_account_send(
                int(account.id), operation_key="reaction:stable-task"
            )
            is True
        )
        assert (
            await runner.record_external_account_send(
                int(account.id), operation_key="reaction:stable-task"
            )
            is False
        )

        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            assert stored.sent_today == 1
            assert stored.sent_since_break == 1
    finally:
        await database.dispose()
