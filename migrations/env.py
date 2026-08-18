from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from proxima_traffic.config import get_settings
from proxima_traffic.models import Base

config = context.config
if config.config_file_name is not None:
    # Alembic runs in-process during normal startup.  Its default
    # ``disable_existing_loggers=True`` silently turns off every already
    # imported application logger after the first schema upgrade.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

configured_database_url = config.attributes.get("database_url")
database_url = (
    str(configured_database_url)
    if configured_database_url is not None
    else get_settings().database_url
)
config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: object) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    # Database.create_schema injects its already-open synchronous connection
    # through Alembic.  This keeps migrations inside the calling async
    # transaction instead of attempting a forbidden nested ``asyncio.run``.
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        do_run_migrations(supplied_connection)
    else:
        asyncio.run(run_async_migrations())
