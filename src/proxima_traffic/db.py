import asyncio
import time
from collections.abc import AsyncIterator
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import event, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from proxima_traffic.instance_lock import BotInstanceAlreadyRunning, BotInstanceLock


class Database:
    def __init__(self, url: str) -> None:
        sqlite_prefix = "sqlite+aiosqlite:///"
        if url.startswith(sqlite_prefix):
            database_path = url.removeprefix(sqlite_prefix)
            if database_path and database_path != ":memory:":
                Path(database_path).expanduser().parent.mkdir(parents=True, exist_ok=True)

        self._url = url
        self.engine = create_async_engine(url, pool_pre_ping=True)
        if url.startswith("sqlite+aiosqlite:"):
            event.listen(self.engine.sync_engine, "connect", self._enable_sqlite_foreign_keys)
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def create_schema(self) -> None:
        """Bring the SQLite database to the bundled Alembic head safely.

        ``metadata.create_all()`` followed by stamping the revision is not a
        migration: it silently skips data migrations.  It also lets the API
        and control bot race when both are started against a fresh database.
        A short OS-level lock serialises the actual Alembic upgrade across
        processes; calling upgrade again after another process completed is a
        safe no-op.
        """

        lock = BotInstanceLock(self._schema_lock_path())
        deadline = time.monotonic() + 60
        while True:
            try:
                lock.acquire()
                break
            except BotInstanceAlreadyRunning:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "timed out waiting for the database migration lock"
                    ) from None
                await asyncio.sleep(0.05)
        try:
            async with self.engine.begin() as connection:
                await connection.run_sync(self._upgrade_schema)
        finally:
            lock.release()

    async def migrate_to_head_if_needed(self) -> bool:
        """Upgrade only a database that is not already ready for this checkout.

        Alembic is still the authoritative path for a fresh or older database,
        but repeatedly invoking it against a healthy production SQLite file
        can hold the startup lock while it re-inspects a large schema.  The
        readiness check already verifies the revision, required tables,
        columns and seed rows, so it is safe to skip that no-op upgrade.
        """

        if await self.ready():
            return False
        await self.create_schema()
        return True

    def _schema_lock_path(self) -> Path:
        sqlite_prefix = "sqlite+aiosqlite:///"
        if not self._url.startswith(sqlite_prefix):
            raise ValueError("automatic schema creation currently supports only SQLite")
        database_path = Path(self._url.removeprefix(sqlite_prefix)).expanduser().resolve()
        return database_path.with_name(f"{database_path.name}.schema.lock")

    def _upgrade_schema(self, connection: Connection) -> None:
        project_root = Path(__file__).resolve().parents[2]
        config = Config(str(project_root / "alembic.ini"))
        config.set_main_option("script_location", str(project_root / "migrations"))
        config.set_main_option("sqlalchemy.url", self._url.replace("%", "%%"))
        config.attributes["database_url"] = self._url
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    async def sessions(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def ready(self) -> bool:
        """Return true only when the database is reachable and matches this code checkout."""

        from proxima_traffic.maintenance import (  # noqa: PLC0415
            REQUIRED_COLUMNS,
            REQUIRED_SEEDED_ROW_COUNTS,
            REQUIRED_TABLES,
            expected_migration_revision,
        )

        expected_revision = expected_migration_revision()
        try:
            async with self.engine.connect() as connection:
                tables = await connection.run_sync(
                    lambda sync_connection: set(inspect(sync_connection).get_table_names())
                )
                if not REQUIRED_TABLES.issubset(tables):
                    return False
                revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
                if revision != expected_revision:
                    return False
                for table_name, minimum_row_count in REQUIRED_SEEDED_ROW_COUNTS.items():
                    row_count = await connection.scalar(text(f"SELECT COUNT(*) FROM {table_name}"))
                    if int(row_count or 0) < minimum_row_count:
                        return False

                def missing_required_columns(sync_connection: object) -> bool:
                    inspector = inspect(sync_connection)
                    for table_name, required_columns in REQUIRED_COLUMNS.items():
                        present = {
                            str(column["name"]) for column in inspector.get_columns(table_name)
                        }
                        if not required_columns.issubset(present):
                            return True
                    return False

                return not await connection.run_sync(missing_required_columns)
        except Exception:
            return False

    @staticmethod
    def _enable_sqlite_foreign_keys(dbapi_connection: object, connection_record: object) -> None:
        del connection_record
        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()
