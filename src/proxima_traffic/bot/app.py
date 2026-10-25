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
    CallbackTimingMiddleware,
    CleanFsmInputMiddleware,
    ControlEventIsolation,
    DraftMediaCleanupMiddleware,
    PanelContextMiddleware,
)


def create_bot(token: str) -> Bot:
    return Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
    )


def create_dispatcher(
    admin_ids: frozenset[int],
    *,
    storage: BaseStorage | None = None,
) -> Dispatcher:
    dispatcher = Dispatcher(
        storage=storage or MemoryStorage(), events_isolation=ControlEventIsolation(admin_ids)
    )
    middleware = AdminOnlyMiddleware(admin_ids)
    dispatcher.message.middleware(PanelContextMiddleware())
    dispatcher.message.middleware(CleanFsmInputMiddleware())
    dispatcher.message.middleware(DraftMediaCleanupMiddleware())
    dispatcher.message.middleware(BulkAvatarDraftCleanupMiddleware())
    dispatcher.callback_query.middleware(PanelContextMiddleware())
    dispatcher.callback_query.middleware(DraftMediaCleanupMiddleware())
    dispatcher.callback_query.middleware(BulkAvatarDraftCleanupMiddleware())
    # Update-level placement starts the clock before aiogram loads durable FSM
    # state, so callback-to-ack and SQL metrics cover the whole local pipeline.
    dispatcher.update.outer_middleware.unregister(dispatcher.fsm)
    dispatcher.update.outer_middleware(middleware)
    dispatcher.update.outer_middleware(CallbackTimingMiddleware(admin_ids))
    dispatcher.update.outer_middleware(dispatcher.fsm)
    dispatcher.include_router(router)
    dispatcher.errors.register(handle_control_panel_error)
    return dispatcher
