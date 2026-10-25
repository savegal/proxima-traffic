import asyncio

from aiogram.types import CallbackQuery, Chat, Message, User

from proxima_traffic.bot.handlers import open_direct_launches
from proxima_traffic.bot.middleware import CallbackTimingMiddleware, callback_is_current


def _callback(callback_id: str, data: str = "dm:launches:0") -> CallbackQuery:
    return CallbackQuery.model_construct(
        id=callback_id,
        from_user=User.model_construct(id=7, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data=data,
        message=Message.model_construct(
            message_id=11,
            date=None,
            chat=Chat.model_construct(id=7, type="private"),
        ),
    )


async def test_direct_launch_navigation_acknowledges_before_database_work(monkeypatch) -> None:
    events: list[str] = []

    class Callback:
        data = "dm:launches:0"

        async def answer(self, **kwargs) -> None:
            del kwargs
            events.append("ack")

    class DirectMessages:
        async def list_launches(self, page: int, page_size: int):
            del page, page_size
            events.append("data")
            return [], 0

    async def capture_edit(*args, **kwargs) -> None:
        del args, kwargs
        events.append("edit")

    monkeypatch.setattr("proxima_traffic.bot.handlers._edit_callback", capture_edit)
    await open_direct_launches(Callback(), DirectMessages())  # type: ignore[arg-type]
    assert events == ["ack", "data", "edit"]


async def test_new_callback_fences_older_screen_update() -> None:
    middleware = CallbackTimingMiddleware()
    first = _callback("first")
    second = _callback("second")
    first_started = asyncio.Event()
    allow_first_to_finish = asyncio.Event()
    observed: list[bool] = []

    async def first_handler(event, data):
        del data
        first_started.set()
        await allow_first_to_finish.wait()
        observed.append(callback_is_current(event))

    async def second_handler(event, data):
        del data
        observed.append(callback_is_current(event))

    first_task = asyncio.create_task(middleware(first_handler, first, {}))
    await first_started.wait()
    await middleware(second_handler, second, {})
    allow_first_to_finish.set()
    await first_task
    assert observed == [True, False]
