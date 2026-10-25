"""Keep wizard replies in their original panel and recover deleted screens."""

from contextvars import ContextVar

from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from proxima_traffic.bot.ui import fit_panel_text, polish_markup

input_panel_origin: ContextVar[tuple[int, int] | None] = ContextVar(
    "input_panel_origin", default=None
)
current_panel_state: ContextVar[FSMContext | None] = ContextVar("current_panel_state", default=None)


async def remember_recovered_panel(message: Message) -> None:
    state = current_panel_state.get()
    if state is not None and await state.get_state() is not None:
        await state.update_data(
            origin_chat_id=message.chat.id, origin_message_id=message.message_id
        )


def screen_is_uneditable(exception: TelegramBadRequest) -> bool:
    message = str(exception).lower()
    return any(
        reason in message
        for reason in (
            "message to edit not found",
            "message can't be edited",
            "message can not be edited",
            "message_id_invalid",
        )
    )


async def answer_panel_input(
    message: Message,
    state: FSMContext,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> Message:
    """Update the wizard, including its final reply after the FSM is cleared."""

    data = await state.get_data()
    origin = input_panel_origin.get()
    if data.get("origin_chat_id") is not None and data.get("origin_message_id") is not None:
        origin = int(data["origin_chat_id"]), int(data["origin_message_id"])
    text = fit_panel_text(text)
    if reply_markup is None and await state.get_state() is not None:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="× Отменить", callback_data="fsm:cancel")]]
        )
    if reply_markup is not None:
        reply_markup = polish_markup(reply_markup)
    if origin is not None and origin[0] == message.chat.id:
        try:
            result = await message.bot.edit_message_text(
                chat_id=origin[0],
                message_id=origin[1],
                text=text,
                reply_markup=reply_markup,
            )
            if isinstance(result, Message):
                return result
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return message.model_copy(update={"message_id": origin[1], "text": text})
            if not screen_is_uneditable(exc):
                raise
    result = await message.answer(text, reply_markup=reply_markup)
    if isinstance(result, Message):
        input_panel_origin.set((result.chat.id, result.message_id))
        if await state.get_state() is not None:
            await state.update_data(
                origin_chat_id=result.chat.id,
                origin_message_id=result.message_id,
            )
    return result
