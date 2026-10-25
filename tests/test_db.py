import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.direct_messages import DirectMessageService


def test_transient_sqlite_lock_detection_is_narrow() -> None:
    locked = OperationalError(
        "UPDATE subscriber_invite_tasks",
        {},
        RuntimeError("database is locked"),
    )
    unrelated = OperationalError("SELECT 1", {}, RuntimeError("no such table: accounts"))

    assert is_transient_sqlite_lock(locked)
    assert not is_transient_sqlite_lock(unrelated)


@pytest.mark.asyncio
async def test_direct_worker_retries_transient_sqlite_lock_without_reporting_failure() -> None:
    service = object.__new__(DirectMessageService)
    service._stop_event = asyncio.Event()
    service._settings = SimpleNamespace(direct_messages_poll_seconds=0.01)
    calls = 0
    reported: list[BaseException] = []

    async def process_next() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError(
                "UPDATE direct_message_tasks",
                {},
                RuntimeError("database is locked"),
            )
        service._stop_event.set()

    async def reporter(exc: BaseException | None = None) -> None:
        if exc is not None:
            reported.append(exc)

    service.process_next = process_next
    await service._worker_loop(reporter)

    assert calls == 2
    assert reported == []
