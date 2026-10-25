from aiogram.types import CallbackQuery, ErrorEvent, Update, User
from sqlalchemy.exc import OperationalError

from proxima_traffic.bot import error_handling
from proxima_traffic.bot.error_handling import (
    _operator_error,
    callback_error_message,
    handle_control_panel_error,
)
from proxima_traffic.content import ContentError
from proxima_traffic.proxies import ProxyRouteUnavailable
from proxima_traffic.services import NotFoundError
from proxima_traffic.subscriber_import import SubscriberImportError


def test_operator_error_never_exposes_internal_exception_for_bad_input() -> None:
    assert "устарели" in _operator_error(ValueError("secret=do-not-show"))
    assert "рабочий прокси" in _operator_error(ProxyRouteUnavailable("internal details"))
    assert "изменён" in _operator_error(NotFoundError("account 1"))
    assert "База данных" in _operator_error(OperationalError("SELECT 1", {}, RuntimeError("busy")))
    assert "Временная ошибка сети" in _operator_error(ConnectionError("socket details"))
    assert "Spintax" in _operator_error(ContentError("Spintax: лишняя скобка"))
    assert "JSON" in _operator_error(SubscriberImportError("некорректный JSON"))


def test_callback_error_is_short_and_never_exposes_transport_details() -> None:
    text = callback_error_message(
        OSError("C:/private/session/credentials.sqlite is unavailable"),
        prefix="Предпросмотр не отправлен",
    )

    assert len(text) <= 190
    assert "credentials" not in text
    assert "Временная ошибка сети" in text


async def test_unexpected_callback_error_is_acknowledged_safely(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def answer(_callback: CallbackQuery, text: str) -> None:
        captured["text"] = text

    callback = CallbackQuery(
        id="callback-id",
        from_user=User(id=1, is_bot=False, first_name="Admin"),
        chat_instance="chat-instance",
        data="broken:button",
    )
    monkeypatch.setattr(error_handling, "_answer_callback", answer)
    event = ErrorEvent(
        update=Update(update_id=1, callback_query=callback),
        exception=RuntimeError("credentials must never be visible"),
    )

    assert await handle_control_panel_error(event) is True
    assert "credentials" not in str(captured["text"])
