import asyncio
import logging

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from proxima_traffic.config import get_settings
from proxima_traffic.db import Database
from proxima_traffic.models import Base


def test_alembic_head_matches_sqlalchemy_metadata(tmp_path, monkeypatch) -> None:
    """Migration regressions must not hide behind create_all-based unit fixtures."""

    database_path = tmp_path / "migrated.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("PROXIMA_DATABASE_URL", database_url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.upgrade(config, "head")
        command.check(config)

        engine = create_engine(f"sqlite:///{database_path.as_posix()}")
        try:
            database_tables = set(inspect(engine).get_table_names())
        finally:
            engine.dispose()
        assert set(Base.metadata.tables).issubset(database_tables)
    finally:
        get_settings.cache_clear()


def test_create_schema_runs_data_migrations_and_serializes_first_start(tmp_path) -> None:
    """Fresh app instances must receive seed data without a startup race."""

    database_path = tmp_path / "fresh.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    application_logger = logging.getLogger("proxima_traffic.migration_regression")
    application_logger.disabled = False

    async def initialize() -> list[int]:
        first = Database(database_url)
        second = Database(database_url)
        try:
            await asyncio.gather(first.create_schema(), second.create_schema())
            assert application_logger.disabled is False
            async with first.engine.connect() as connection:
                count = int(
                    await connection.scalar(text("SELECT COUNT(*) FROM mention_blacklist_entries"))
                    or 0
                )
                revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
                return [
                    count,
                    int(revision == "0062_system_premium_account_group"),
                    int(await first.ready()),
                ]
        finally:
            await first.dispose()
            await second.dispose()

    assert asyncio.run(initialize()) == [1057, 1, 1]
