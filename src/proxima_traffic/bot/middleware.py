import asyncio
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiogram import BaseMiddleware
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import SimpleEventIsolation
from aiogram.types import CallbackQuery, Message, TelegramObject, Update

from proxima_traffic.bot.panel import current_panel_state, input_panel_origin
from proxima_traffic.bot.states import AccountBulkAvatarState
from proxima_traffic.db import begin_sql_timing, end_sql_timing

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CallbackTiming:
    callback_data: str
    received_at: float
    ack_started_at: float | None = None
    data_ready_at: float | None = None
    screen_updated_at: float | None = None
    telegram_ack_seconds: float = 0.0
    telegram_edit_seconds: float = 0.0
    acknowledged: bool = False


_callback_timing: ContextVar[CallbackTiming | None] = ContextVar("callback_timing", default=None)
_latest_callback_ids: OrderedDict[tuple[int, int], str] = OrderedDict()
_MAX_TRACKED_MESSAGES = 1024


def _callback_message_key(callback: CallbackQuery) -> tuple[int, int] | None:
    message = callback.message
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    message_id = getattr(message, "message_id", None)
    if chat_id is None or message_id is None:
        return None
    return int(chat_id), int(message_id)


def callback_is_current(callback: CallbackQuery) -> bool:
    key = _callback_message_key(callback)
    return key is None or _latest_callback_ids.get(key) == callback.id


def mark_callback_ack_started() -> float:
    started_at = time.perf_counter()
    timing = _callback_timing.get()
    if timing is not None and timing.ack_started_at is None:
        timing.ack_started_at = started_at
    return started_at


def start_callback_telegram_api() -> float:
    return time.perf_counter()


def mark_callback_data_ready() -> None:
    timing = _callback_timing.get()
    if timing is not None and timing.data_ready_at is None:
        timing.data_ready_at = time.perf_counter()


def mark_callback_screen_updated() -> None:
    timing = _callback_timing.get()
    if timing is not None:
        timing.screen_updated_at = time.perf_counter()


def record_callback_telegram_api(started_at: float, *, operation: str) -> None:
    timing = _callback_timing.get()
    if timing is not None:
        elapsed = time.perf_counter() - started_at
        if operation == "answer":
            timing.telegram_ack_seconds += elapsed
        else:
            timing.telegram_edit_seconds += elapsed


async def acknowledge_callback(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool | None = None,
) -> bool:
    """Keep acknowledgements quick, deduplicated and safe after a restart."""

    timing = _callback_timing.get()
    if timing is not None and timing.acknowledged and text is None and not show_alert:
        return True
    kwargs: dict[str, Any] = {}
    if text is not None:
        kwargs["text"] = text[:190]
    if show_alert is not None:
        kwargs["show_alert"] = show_alert
    started_at = mark_callback_ack_started()
    try:
        await callback.answer(**kwargs)
    except TelegramBadRequest as exc:
        message = str(exc).lower()
        if "query is too old" in message or "query id is invalid" in message:
            return False
        raise
    else:
        if timing is not None:
            timing.acknowledged = True
        return True
    finally:
        record_callback_telegram_api(started_at, operation="answer")


async def _acknowledge_slow_callback(callback: CallbackQuery, timing: CallbackTiming) -> None:
    await asyncio.sleep(0.2)
    if timing.ack_started_at is None:
        try:
            await acknowledge_callback(callback)
        except Exception:  # A best-effort acknowledgement must not discard the action result.
            logger.debug("Could not acknowledge pending callback", exc_info=True)


class CallbackTimingMiddleware(BaseMiddleware):
    """Measure callback latency and fence stale concurrent screen updates."""

    def __init__(self, admin_ids: frozenset[int] | None = None) -> None:
        self._admin_ids = admin_ids

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        callback = (
            event
            if isinstance(event, CallbackQuery)
            else event.callback_query
            if isinstance(event, Update)
            else None
        )
        if callback is None:
            return await handler(event, data)
        timing = CallbackTiming(str(callback.data or ""), time.perf_counter())
        timing_token = _callback_timing.set(timing)
        sql_token = begin_sql_timing()
        authorized = (
            self._admin_ids is not None
            and callback.from_user.id in self._admin_ids
            and callback.message is not None
            and callback.message.chat.type == ChatType.PRIVATE
        )
        ack_task = (
            asyncio.create_task(_acknowledge_slow_callback(callback, timing))
            if authorized
            else None
        )
        key = _callback_message_key(callback)
        if key is not None:
            _latest_callback_ids[key] = callback.id
            _latest_callback_ids.move_to_end(key)
            while len(_latest_callback_ids) > _MAX_TRACKED_MESSAGES:
                _latest_callback_ids.popitem(last=False)
        try:
            return await handler(event, data)
        finally:
            if ack_task is not None:
                ack_task.cancel()
                with suppress(asyncio.CancelledError):
                    await ack_task
            sql = end_sql_timing(sql_token)
            finished_at = time.perf_counter()
            logger.info(
                "control_callback callback=%s ack_start_ms=%s data_ready_ms=%s "
                "screen_updated_ms=%s total_ms=%.1f sql_ms=%.1f sql_queries=%d "
                "telegram_ack_ms=%.1f telegram_edit_ms=%.1f stale=%s",
                timing.callback_data,
                (
                    f"{(timing.ack_started_at - timing.received_at) * 1000:.1f}"
                    if timing.ack_started_at is not None
                    else "none"
                ),
                (
                    f"{(timing.data_ready_at - timing.received_at) * 1000:.1f}"
                    if timing.data_ready_at is not None
                    else "none"
                ),
                (
                    f"{(timing.screen_updated_at - timing.received_at) * 1000:.1f}"
                    if timing.screen_updated_at is not None
                    else "none"
                ),
                (finished_at - timing.received_at) * 1000,
                sql.total_seconds * 1000,
                sql.query_count,
                timing.telegram_ack_seconds * 1000,
                timing.telegram_edit_seconds * 1000,
                not callback_is_current(callback),
            )
            _callback_timing.reset(timing_token)


class ControlEventIsolation(SimpleEventIsolation):
    """Serialize drafts while keeping existing stop controls immediately usable."""

    def __init__(self, admin_ids: frozenset[int]) -> None:
        super().__init__()
        self._admin_ids = admin_ids

    @asynccontextmanager
    async def lock(self, key: StorageKey):
        timing = _callback_timing.get()
        urgent = timing is not None and timing.callback_data.startswith(
            (
                "queue:stopall:",
                "campaign:stopall:",
                "launch:cancel:",
                "dm:launchcancel:",
                "dm:launchcancelok:",
                "inv:launchcancel:",
                "inv:launchcancelok:",
            )
        )
        # Stop handlers operate on durable queues and never change the FSM draft.
        # Unauthorised users must not populate the per-operator lock registry.
        if urgent or key.user_id not in self._admin_ids:
            yield
        else:
            async with super().lock(key):
                yield


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
            and data.get("raw_state") is not None
            and not (event.text or "").startswith("/")
        )
        origin = None
        if should_clean:
            draft = await state.get_data()
            if (
                draft.get("origin_chat_id") is not None
                and draft.get("origin_message_id") is not None
            ):
                origin = int(draft["origin_chat_id"]), int(draft["origin_message_id"])
        token = input_panel_origin.set(origin)
        try:
            result = await handler(event, data)
            if should_clean:
                with suppress(TelegramBadRequest, TelegramForbiddenError):
                    await event.delete()
            return result
        finally:
            input_panel_origin.reset(token)


class PanelContextMiddleware(BaseMiddleware):
    """Let screen recovery update the active wizard's original message."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        state = data.get("state")
        token = current_panel_state.set(state if isinstance(state, FSMContext) else None)
        try:
            return await handler(event, data)
        finally:
            current_panel_state.reset(token)


class BulkAvatarDraftCleanupMiddleware(BaseMiddleware):
    """Remove a temporary batch-avatar file as soon as its FSM draft ends.

    A draft can be abandoned through `/cancel`, the global menu, or any other
    screen transition, not only through the avatar screen's own callback.
    Keep the file while the same draft is still active so a normal confirmation
    is unaffected.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        state = data.get("state")
        if not isinstance(state, FSMContext):
            return await handler(event, data)
        # Aiogram's FSM middleware has already loaded ``raw_state``. Avoid an
        # additional database read for every control-panel event: a draft file
        # can exist only after the avatar wizard reaches its confirmation state.
        if data.get("raw_state") != AccountBulkAvatarState.confirm.state:
            return await handler(event, data)
        before = await state.get_data()
        raw_path = before.get("profile_avatar_draft_path")
        try:
            return await handler(event, data)
        finally:
            if isinstance(raw_path, str):
                after = await state.get_data()
                if after.get("profile_avatar_draft_path") != raw_path:
                    _delete_bulk_avatar_draft(data.get("settings"), raw_path)


def _delete_bulk_avatar_draft(settings: object | None, raw_path: str) -> None:
    media_storage_path = getattr(settings, "media_storage_path", None)
    if not media_storage_path:
        return
    try:
        drafts = Path(str(media_storage_path)).expanduser().resolve() / "bulk-profile-avatars"
        candidate = Path(raw_path).expanduser().resolve()
        if candidate.is_relative_to(drafts.resolve()):
            candidate.unlink(missing_ok=True)
    except OSError:
        # Cleanup is best effort: a later service cleanup can remove any stale
        # file, and a Telegram UI action must never fail because of it.
        return
