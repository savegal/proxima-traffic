from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from aiogram import BaseMiddleware
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, TelegramObject


class AdminOnlyMiddleware(BaseMiddleware):
    def __init__(self, admin_ids: frozenset[int]) -> None:
        self._admin_ids = admin_ids

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user is not None and user.id in self._admin_ids:
            if _is_private_control_event(event):
                return await handler(event, data)
            await _answer_private_chat_required(event)
            return None
        if isinstance(event, CallbackQuery):
            await event.answer("Нет доступа", show_alert=True)
        elif isinstance(event, Message):
            await event.answer("Доступ к панели запрещён.")
        return None


def _is_private_control_event(event: TelegramObject) -> bool:
    if isinstance(event, Message):
        return event.chat.type == ChatType.PRIVATE
    if isinstance(event, CallbackQuery):
        message = event.message
        return message is not None and message.chat.type == ChatType.PRIVATE
    return False


async def _answer_private_chat_required(event: TelegramObject) -> None:
    if isinstance(event, CallbackQuery):
        await event.answer("Управление доступно только в ЛС с ботом.", show_alert=True)
    elif isinstance(event, Message):
        await event.answer("Управление доступно только в ЛС с ботом.")


class CleanFsmInputMiddleware(BaseMiddleware):
    """Keeps wizard chats clean by removing handled operator input messages."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        state = data.get("state")
        should_clean = (
            isinstance(event, Message)
            and isinstance(state, FSMContext)
            and await state.get_state() is not None
            and not (event.text or "").startswith("/")
        )
        result = await handler(event, data)
        if should_clean:
            with suppress(TelegramBadRequest, TelegramForbiddenError):
                await event.delete()
        return result
