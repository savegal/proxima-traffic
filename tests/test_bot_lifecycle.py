import asyncio
import logging

from proxima_traffic import bot_main
from proxima_traffic import main as api_main


async def test_shutdown_waits_for_a_completed_background_task() -> None:
    async def finishes() -> None:
        await asyncio.sleep(0)

    task = asyncio.create_task(finishes())

    await bot_main._finish_background_task(
        task,
        name="test worker",
        grace_seconds=1,
    )

    assert task.done()
    assert not task.cancelled()


async def test_shutdown_cancels_a_stuck_background_task() -> None:
    blocker = asyncio.Event()

    async def never_finishes() -> None:
        await blocker.wait()

    task = asyncio.create_task(never_finishes())
    await asyncio.sleep(0)

    await bot_main._finish_background_task(
        task,
        name="test worker",
        grace_seconds=0.01,
    )

    assert task.cancelled()


async def test_api_shutdown_cancels_a_stuck_background_task() -> None:
    blocker = asyncio.Event()

    async def never_finishes() -> None:
        await blocker.wait()

    task = asyncio.create_task(never_finishes())
    await asyncio.sleep(0)

    await api_main._finish_background_task(
        task,
        name="test API worker",
        grace_seconds=0.01,
    )

    assert task.cancelled()


def test_run_suppresses_keyboard_interrupt_during_early_startup(monkeypatch, caplog) -> None:
    def interrupted(coroutine: object) -> None:
        coroutine.close()  # type: ignore[union-attr]
        raise KeyboardInterrupt

    monkeypatch.setattr(bot_main.asyncio, "run", interrupted)

    with caplog.at_level(logging.INFO, logger="proxima_traffic.bot_main"):
        bot_main.run()

    assert "stopped by keyboard" in caplog.text
