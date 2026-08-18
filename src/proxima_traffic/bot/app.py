from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.storage.memory import MemoryStorage

from proxima_traffic.bot.error_handling import handle_control_panel_error
from proxima_traffic.bot.handlers import router
from proxima_traffic.bot.middleware import (
    AdminOnlyMiddleware,
    BulkAvatarDraftCleanupMiddleware,
    CleanFsmInputMiddleware,
)


def create_bot(token: str) -> Bot:
    return Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))


def create_dispatcher(
    admin_ids: frozenset[int],
    *,
    storage: BaseStorage | None = None,
) -> Dispatcher:
    dispatcher = Dispatcher(storage=storage or MemoryStorage())
    middleware = AdminOnlyMiddleware(admin_ids)
    dispatcher.message.middleware(middleware)
    dispatcher.message.middleware(CleanFsmInputMiddleware())
    dispatcher.message.middleware(BulkAvatarDraftCleanupMiddleware())
    dispatcher.callback_query.middleware(middleware)
    dispatcher.callback_query.middleware(BulkAvatarDraftCleanupMiddleware())
    dispatcher.include_router(router)
    dispatcher.errors.register(handle_control_panel_error)
    return dispatcher
