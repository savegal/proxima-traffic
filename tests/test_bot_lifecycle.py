import asyncio
import logging

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

from proxima_traffic import bot_main
from proxima_traffic import main as api_main


async def test_ready_notification_reaches_available_administrators(caplog) -> None:
    class Bot:
        sent: list[tuple[int, str]] = []

        async def send_message(self, *, chat_id: int, text: str) -> None:
            if chat_id == 2:
                raise TelegramBadRequest(method=None, message="chat not found")
            self.sent.append((chat_id, text))

    bot = Bot()
    with caplog.at_level(logging.WARNING, logger="proxima_traffic.bot_main"):
        await bot_main._notify_control_bot_ready(  # type: ignore[arg-type]
            bot,
            frozenset({1, 2}),
            worker_enabled=True,
            campaign_id_scope=None,
        )

    assert bot.sent == [
        (
            1,
            "🟢 <b>Proxima перезапущен и готов к работе.</b>\n\n"
            "Панель управления запущена. Очередь рассылок и реакций включена.\n\n"
            + bot_main._RESTART_UPDATE_NOTICE,
        )
    ]
    assert "Could not send ready notification to administrator 2" in caplog.text


async def test_ready_notification_timeout_does_not_block_startup(monkeypatch, caplog) -> None:
    class Bot:
        async def send_message(self, *, chat_id: int, text: str) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(bot_main, "_CONTROL_BOT_BOOTSTRAP_TIMEOUT_SECONDS", 0.01)

    with caplog.at_level(logging.WARNING, logger="proxima_traffic.bot_main"):
        await bot_main._notify_control_bot_ready(  # type: ignore[arg-type]
            Bot(),
            frozenset({1}),
            worker_enabled=True,
            campaign_id_scope=frozenset({16}),
        )

    assert "Ready notification to administrator 1 timed out" in caplog.text


async def test_ready_notification_retries_only_administrators_that_missed_it(monkeypatch) -> None:
    attempts: list[frozenset[int]] = []

    async def notify(*args: object, **kwargs: object) -> frozenset[int]:
        del args, kwargs
        attempts.append(frozenset({2}) if not attempts else frozenset())
        return attempts[-1]

    monkeypatch.setattr(bot_main, "_notify_control_bot_ready", notify)
    monkeypatch.setattr(bot_main, "_CONTROL_BOT_READY_RETRY_INITIAL_SECONDS", 0.0)

    await bot_main._retry_control_bot_ready_notification(  # type: ignore[arg-type]
        object(),
        frozenset({2}),
        worker_enabled=True,
        campaign_id_scope=None,
    )

    assert attempts == [frozenset({2}), frozenset()]


async def test_polling_retries_a_temporary_telegram_network_error(monkeypatch, caplog) -> None:
    class Dispatcher:
        attempts = 0

        async def start_polling(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.attempts += 1
            if self.attempts == 1:
                raise TelegramNetworkError(method=None, message="initial connection timed out")

    dispatcher = Dispatcher()
    monkeypatch.setattr(bot_main, "_CONTROL_BOT_POLL_RETRY_INITIAL_SECONDS", 0.0)

    with caplog.at_level(logging.WARNING, logger="proxima_traffic.bot_main"):
        await bot_main._start_polling_with_network_retries(  # type: ignore[arg-type]
            dispatcher,
            object(),  # type: ignore[arg-type]
            settings=object(),
        )

    assert dispatcher.attempts == 2
    assert "Workers remain active; retrying polling" in caplog.text


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
