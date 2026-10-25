"""Last-resort, operator-safe handling for unexpected control-panel errors."""

from __future__ import annotations

import logging
from html import escape

from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from sqlalchemy.exc import SQLAlchemyError

from proxima_traffic.accounts import (
    AccountImportError,
    AccountOperationError,
    AccountProfileError,
    ChatFolderError,
)
from proxima_traffic.content import ContentError
from proxima_traffic.proxies import ProxyInputError, ProxyRouteUnavailable
from proxima_traffic.services import ConflictError, DomainError, NotFoundError, safe_error_message
from proxima_traffic.subscriber_import import SubscriberImportError

logger = logging.getLogger(__name__)


def operator_error_message(exception: Exception) -> str:
    """Turn known domain errors into short, safe next steps.

    The detailed exception stays in structured logs.  The operator never gets
    a credential, a SQL trace, nor a raw Telegram/RPC payload in the panel.
    """

    if isinstance(exception, NotFoundError):
        return "Этот объект уже изменён или удалён. Обновите текущий раздел."
    if isinstance(exception, ProxyRouteUnavailable):
        return (
            "Для аккаунта не найден рабочий прокси. Добавьте или включите рабочий "
            "HTTP(S)/SOCKS5-прокси и повторите действие."
        )
    if isinstance(exception, ProxyInputError):
        return "Проверьте формат прокси: поддерживаются HTTP(S) и SOCKS5."
    if isinstance(exception, TelegramForbiddenError):
        return "Telegram больше не даёт боту изменить этот экран. Откройте /admin заново."
    if isinstance(exception, TelegramBadRequest):
        return "Экран уже изменился или устарел. Обновите раздел и повторите действие."
    if isinstance(exception, TelegramAPIError):
        return "Telegram временно недоступен для панели. Действие не повторялось автоматически."
    if isinstance(exception, (TimeoutError, ConnectionError, OSError)):
        return "Временная ошибка сети. Действие не повторялось автоматически; попробуйте ещё раз."
    if isinstance(exception, SQLAlchemyError):
        return (
            "База данных временно занята. Обновите экран и повторите действие через "
            "несколько секунд."
        )
    if isinstance(exception, ConflictError):
        return safe_error_message(exception)[:500]
    if isinstance(
        exception,
        (AccountImportError, AccountProfileError, ChatFolderError, AccountOperationError),
    ):
        return safe_error_message(exception)[:500]
    if isinstance(exception, ContentError):
        return safe_error_message(exception)[:500]
    if isinstance(exception, SubscriberImportError):
        return str(exception)[:500]
    if isinstance(exception, (ValueError, KeyError, IndexError)):
        return "Эта кнопка или введённые данные устарели. Обновите экран и повторите действие."
    if isinstance(exception, DomainError):
        return safe_error_message(exception)[:500]
    return (
        "Не удалось выполнить действие из-за временной внутренней ошибки. Ничего не было "
        "отправлено повторно; обновите экран и попробуйте ещё раз."
    )


def callback_error_message(exception: Exception, *, prefix: str | None = None) -> str:
    """Return a user-safe callback alert within Telegram's 200-character limit."""

    message = operator_error_message(exception)
    if prefix:
        message = f"{prefix}: {message}"
    return message[:190]


# Kept as a private alias while integrations and older tests migrate to the public helper.
_operator_error = operator_error_message


async def handle_control_panel_error(event: ErrorEvent) -> bool:
    """Acknowledge the originating update and keep the panel recoverable."""

    exception = event.exception
    message = operator_error_message(exception)
    update: Update = event.update
    callback = update.callback_query
    if callback is not None:
        await _answer_callback(callback, message)
    else:
        source = update.message
        if source is not None:
            await _answer_message(source, message)

    if isinstance(exception, (DomainError, ValueError, KeyError, IndexError)):
        logger.info("Control-panel action was rejected safely: %s", safe_error_message(exception))
    else:
        logger.exception("Unexpected control-panel error", exc_info=exception)
    return True


async def _answer_callback(callback: CallbackQuery, message: str) -> None:
    try:
        await callback.answer(message[:190], show_alert=True)
    except Exception:  # Callback may have expired while the failed action ran.
        logger.debug("Could not acknowledge failed callback", exc_info=True)
        if isinstance(callback.message, Message):
            await _answer_message(callback.message, message)


async def _answer_message(message: Message, text: str) -> None:
    try:
        await message.answer(
            "! <b>Не удалось завершить действие</b>\n" + escape(text),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⌂ Открыть панель", callback_data="menu:home")]
                ]
            ),
        )
    except Exception:
        logger.debug("Could not answer failed message", exc_info=True)
