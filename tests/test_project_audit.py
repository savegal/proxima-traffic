"""Failure paths discovered during the full project audit."""

import asyncio
import shutil
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from proxima_traffic import bot_main
from proxima_traffic import main as api_main
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.models import SubscriberList, SubscriberListStatus, utc_now
from proxima_traffic.worker_supervision import WorkerSupervisor


@pytest.mark.parametrize("module", [bot_main, api_main], ids=["bot", "api"])
async def test_shutdown_cleanup_failure_does_not_abort_remaining_resource_cleanup(module, caplog):
    started = asyncio.Event()

    async def worker():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            raise RuntimeError("worker cleanup failed")

    task = asyncio.create_task(worker())
    await started.wait()
    await module._finish_background_task(task, name="audit worker", grace_seconds=0.01)
    assert task.done()
    assert "worker cleanup failed" in caplog.text


async def test_non_ascii_wrong_admin_header_is_rejected_without_server_error(client):
    response = await client.get("/api/v1/status", headers=[(b"x-admin-key", b"wrong-\xff")])
    assert response.status_code == 401


async def test_worker_endpoint_rejects_a_stale_but_still_running_worker(
    app, client, admin_headers, monkeypatch
):
    import proxima_traffic.worker_supervision as health_module

    started = asyncio.Event()

    async def hung_worker(reporter):
        started.set()
        await asyncio.Event().wait()

    supervisor = WorkerSupervisor()
    task = supervisor.start("campaigns", hung_worker)
    await started.wait()
    app.state.worker_supervisor = supervisor
    app.state.background_tasks = {"campaigns": task}
    app.state.settings.api_worker_enabled = True
    running = replace(app.state.queue_worker.snapshot(), running=True)
    monkeypatch.setattr(app.state.queue_worker, "snapshot", lambda: running)
    future = utc_now() + timedelta(hours=1)
    monkeypatch.setattr(health_module, "utc_now", lambda: future)
    try:
        assert not supervisor.ready(max_stale_seconds=300)
        response = await client.get("/api/v1/worker", headers=admin_headers)
        assert response.status_code == 200
        assert response.json()["healthy"] is False
    finally:
        supervisor.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_api_database_is_closed_even_when_gateway_close_fails(app, monkeypatch):
    dispose = AsyncMock(wraps=app.state.database.dispose)
    monkeypatch.setattr(app.state.database, "dispose", dispose)
    monkeypatch.setattr(
        app.state.gateway,
        "close",
        AsyncMock(side_effect=RuntimeError("gateway close failed")),
        raising=False,
    )
    with pytest.raises(RuntimeError, match="gateway close failed"):
        async with app.router.lifespan_context(app):
            pass
    dispose.assert_awaited_once()


async def test_utf8_admin_secret_can_be_compared_as_header_bytes(app, client):
    secret = "тестовый-ключ-api"
    app.state.settings.admin_api_key = SecretStr(secret)
    response = await client.get("/api/v1/status", headers=[(b"x-admin-key", secret.encode())])
    assert response.status_code == 200


@pytest.mark.parametrize("module", [bot_main, api_main], ids=["bot", "api"])
async def test_cancelling_the_shutdown_waiter_reaps_the_worker_and_propagates(module):
    started = asyncio.Event()

    async def worker():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(worker())
    await started.wait()
    waiter = asyncio.create_task(
        module._finish_background_task(task, name="cancelled waiter", grace_seconds=10)
    )
    await asyncio.sleep(0)
    waiter.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert task.cancelled()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_bundled_migrations_create_and_validate_a_database_outside_the_checkout(
    tmp_path, monkeypatch
):
    import proxima_traffic.migration_config as paths

    root = Path(__file__).resolve().parents[1]  # noqa: ASYNC240 - isolated fixture setup
    package = tmp_path / "installed" / "proxima_traffic"
    package.mkdir(parents=True)
    shutil.copy2(root / "alembic.ini", package / "_alembic.ini")
    shutil.copytree(
        root / "migrations", package / "_migrations", ignore=shutil.ignore_patterns("__pycache__")
    )
    monkeypatch.setattr(paths, "__file__", str(package / "migration_config.py"))
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'isolated.db'}")
    try:
        await database.create_schema()
        assert await database.ready()
    finally:
        await database.dispose()


@pytest.mark.parametrize("importing", [False, True])
async def test_primary_audience_creation_is_durable_and_safe_for_competing_requests(app, importing):
    service = DirectMessageService(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="primary-audience-audit",
    )
    if importing:
        await service._import_lock.acquire()
    try:
        results = await asyncio.gather(
            *(service.get_primary_subscriber_list() for _ in range(8)),
            return_exceptions=True,
        )
    finally:
        if importing:
            service._import_lock.release()
    assert all(isinstance(row, SubscriberList) for row in results)
    assert len({row.id for row in results}) == 1
    async with app.state.database.session_factory() as session:
        assert await session.scalar(select(func.count(SubscriberList.id))) == 1
        row = await session.get(SubscriberList, results[0].id)
        assert row.status == (
            SubscriberListStatus.IMPORTING if importing else SubscriberListStatus.READY
        )
