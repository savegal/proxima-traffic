from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy import event, inspect, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from proxima_traffic.models import Base


class Database:
    def __init__(self, url: str) -> None:
        sqlite_prefix = "sqlite+aiosqlite:///"
        if url.startswith(sqlite_prefix):
            database_path = url.removeprefix(sqlite_prefix)
            if database_path and database_path != ":memory:":
                Path(database_path).expanduser().parent.mkdir(parents=True, exist_ok=True)

        self.engine = create_async_engine(url, pool_pre_ping=True)
        if url.startswith("sqlite+aiosqlite:"):
            event.listen(self.engine.sync_engine, "connect", self._enable_sqlite_foreign_keys)
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def create_schema(self) -> None:
        async with self.engine.begin() as connection:
            existing_tables = await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
            await connection.run_sync(Base.metadata.create_all)
            if not existing_tables:
                from proxima_traffic.maintenance import (  # noqa: PLC0415
                    expected_migration_revision,
                )

                await connection.execute(
                    text("CREATE TABLE alembic_version (version_num VARCHAR(64) NOT NULL)")
                )
                await connection.execute(
                    text("INSERT INTO alembic_version (version_num) VALUES (:revision)"),
                    {"revision": expected_migration_revision()},
                )

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
