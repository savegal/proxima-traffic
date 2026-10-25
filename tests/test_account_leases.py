from __future__ import annotations

import asyncio
import shutil

import pytest

from proxima_traffic.account_leases import (
    AccountOperationLeaseCoordinator,
    AccountOperationLeaseLost,
)
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.gateways import DryRunGateway
from proxima_traffic.models import Account, AccountOperationLease, ensure_utc
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
        with pytest.raises(AccountSendDeferred) as paced:
            await second.run_account_send_operation(account.id, lambda: asyncio.sleep(0))
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            assert stored.sent_today == 1
            assert stored.next_send_at is not None
            assert paced.value.resume_at == ensure_utc(stored.next_send_at)
            assert stored.send_reservation_token is None
            assert stored.send_reservation_until is None
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_failed_send_attempt_still_paces_account(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'failed-send-pacing.db'}",
        worker_enabled=False,
        account_pace_min_seconds=60,
        account_pace_max_seconds=60,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="failed-send-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        runner = CampaignRunner(database.session_factory, DryRunGateway(), settings=settings)

        async def failed_send() -> None:
            raise RuntimeError("recipient rejected the attempt")

        with pytest.raises(RuntimeError, match="recipient rejected"):
            await runner.run_account_send_operation(account.id, failed_send)
        with pytest.raises(AccountSendDeferred):
            await runner.run_account_send_operation(account.id, lambda: asyncio.sleep(0))
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            assert stored.sent_today == 0
            assert stored.next_send_at is not None
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_account_send_reservation_heartbeat_is_durably_committed(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'account-send-heartbeat.db'}",
        worker_enabled=False,
        worker_heartbeat_seconds=0.01,
        account_micro_break_every=0,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        async with database.session_factory() as session:
            account = Account(label="heartbeat-account", ownership_confirmed=True)
            session.add(account)
            await session.commit()
            await session.refresh(account)
        assert account.id is not None
        runner = CampaignRunner(database.session_factory, DryRunGateway(), settings=settings)
        token, original_until = await runner._claim_account_send_slot(account.id)
        assert token is not None and original_until is None

        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None and stored.send_reservation_until is not None
            initial_until = ensure_utc(stored.send_reservation_until)
        assert initial_until is not None

        stop = asyncio.Event()
        lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            runner._heartbeat_account_send_slot(account.id, token, stop, lost, None)
        )
        await asyncio.sleep(1.1)
        stop.set()
        await heartbeat

        assert not lost.is_set()
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored is not None
            renewed_until = ensure_utc(stored.send_reservation_until)
        assert renewed_until is not None
        assert renewed_until > initial_until
        await runner._release_account_send_slot(account.id, token)
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


@pytest.mark.parametrize("external_action", [False, True])
async def test_accounting_keeps_reservation_until_result_is_saved(
    tmp_path, schema_template, external_action
):
    path = tmp_path / "accounting-reservation.db"
    shutil.copy2(schema_template, path)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{path}",
        worker_enabled=False,
        worker_heartbeat_seconds=1,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_micro_break_every=0,
    )
    database = Database(settings.database_url)
    try:
        async with database.session_factory() as session:
            account = Account(label="Reserved until saved", ownership_confirmed=True)
            session.add(account)
            await session.commit()
        runner = CampaignRunner(database.session_factory, DryRunGateway(), settings=settings)
        competitor = CampaignRunner(database.session_factory, DryRunGateway(), settings=settings)

        async def save_result():
            if external_action:
                await runner.record_external_account_send(account.id, operation_key="delivery")
            else:
                async with database.session_factory() as session:
                    await runner._record_account_send(session, account.id)
                    await session.commit()
            async with database.session_factory() as session:
                stored = await session.get(Account, account.id)
                assert stored.send_reservation_token is not None
            with pytest.raises(AccountSendDeferred):
                await competitor.run_account_send_operation(account.id, lambda: asyncio.sleep(0))
            await asyncio.sleep(1.1)

        await runner.run_account_send_operation(account.id, save_result)
        async with database.session_factory() as session:
            stored = await session.get(Account, account.id)
            assert stored.sent_today == 1
            assert stored.send_reservation_token is None
    finally:
        await database.dispose()
