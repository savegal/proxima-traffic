from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from proxima_traffic.config import Settings
from proxima_traffic.main import create_app


@pytest.fixture(autouse=True)
def isolate_settings_from_working_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit tests must never inherit credentials or LIVE flags from the real .env."""
    monkeypatch.setitem(Settings.model_config, "env_file", None)


@pytest_asyncio.fixture
async def app(tmp_path) -> AsyncIterator[FastAPI]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        admin_api_key="test-admin-key",
        auto_create_schema=True,
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
    await app.state.database.create_schema()
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
