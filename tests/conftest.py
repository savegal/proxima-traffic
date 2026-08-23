import asyncio
import shutil
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.main import create_app


@pytest.fixture(autouse=True)
def isolate_settings_from_working_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit tests must never inherit credentials or LIVE flags from the real .env."""
    monkeypatch.setitem(Settings.model_config, "env_file", None)


@pytest.fixture(scope="session")
def schema_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the blank head schema once; each app test receives a private copy."""

    database_path = tmp_path_factory.mktemp("schema-template") / "head.db"

    async def create_template() -> None:
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        try:
            await database.create_schema()
        finally:
            await database.dispose()

    asyncio.run(create_template())
    return database_path


@pytest_asyncio.fixture
async def app(tmp_path, schema_template: Path) -> AsyncIterator[FastAPI]:
    database_path = tmp_path / "test.db"
    shutil.copy2(schema_template, database_path)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{database_path}",
        admin_api_key="test-admin-key",
        account_encryption_key="test-account-encryption-key-12345",
        auto_create_schema=False,
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_micro_break_every=0,
        account_join_daily_limit=0,
        account_join_min_interval_seconds=0,
    )
    app = create_app(settings=settings)
    yield app
    await app.state.database.dispose()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as test_client:
        yield test_client


@pytest.fixture
def admin_headers() -> dict[str, str]:
    return {"X-Admin-Key": "test-admin-key"}
