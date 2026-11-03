"""Regression tests for changing a Telegram account's primary @username."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from telethon.tl.functions.account import UpdateUsernameRequest

from proxima_traffic.accounts import (
    AccountProfileError,
    _safe_account_operation_error,
    normalize_telegram_username,
)
from proxima_traffic.bot import handlers
from proxima_traffic.bot.keyboards import (
    account_details_menu,
    account_username_confirmation,
)
from proxima_traffic.bot.states import AccountUsernameEditState
from proxima_traffic.models import Account, AccountSessionSource, AccountStatus


async def _account(app, *, username="old_handle"):
    async with app.state.database.session_factory() as session:
        account = Account(
            label="username-account",
            telegram_user_id=123456,
            username=username,
            first_name="Existing",
            last_name="Name",
            about="Bio stays",
            ownership_confirmed=True,
            authorized=True,
            status=AccountStatus.ACTIVE,
            session_source=AccountSessionSource.SESSION_STRING,
            session_encrypted="encrypted-session-placeholder",
            api_id=1,
            api_hash_encrypted="encrypted-api-hash-placeholder",
        )
        session.add(account)
        await session.commit()
        return int(account.id)


@pytest.mark.parametrize(
    ("raw", "normalized"),
    [
        ("@new_handle", "new_handle"),
        ("  NewHandle  ", "NewHandle"),
        ("", ""),
        ("__", None),
        ("bad-name", None),
        ("имя_тест", None),
        ("a" * 33, None),
        ("@", None),
    ],
)
def test_username_validation(raw, normalized):
    if normalized is None:
        with pytest.raises(AccountProfileError):
            normalize_telegram_username(raw)
    else:
        assert normalize_telegram_username(raw) == normalized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested", "expected"),
    [("@new_handle", "new_handle"), ("-", "")],
)
async def test_profile_username_updates_and_removes_primary_username(
    app, monkeypatch, requested, expected
):
    account_id = await _account(app)
    requested_usernames = []

    async def fake_operation(aid, operation):
        assert aid == account_id

        class Client:
            async def __call__(self, request):
                assert isinstance(request, UpdateUsernameRequest)
                requested_usernames.append(request.username)
                return SimpleNamespace(
                    id=123456,
                    username=request.username or None,
                    first_name="Existing",
                    last_name="Name",
                    photo=None,
                )

        user = await operation(Client())
        stale_me = SimpleNamespace(username="old_handle")
        return stale_me, user

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", fake_operation)
    new_value = "" if requested == "-" else requested
    result = await app.state.account_service.update_profile_username(
        account_id, username=new_value
    )

    assert requested_usernames == [expected]
    assert result.username == (expected or None)
    assert result.first_name == "Existing"
    assert result.last_name == "Name"
    assert result.about == "Bio stays"
    assert (await app.state.account_service.get_account(account_id)).username == (
        expected or None
    )


@pytest.mark.asyncio
async def test_occupied_username_preserves_saved_profile(app, monkeypatch):
    account_id = await _account(app)
    rpc_error = type("UsernameOccupiedError", (Exception,), {})

    async def fake_operation(aid, operation):
        class Client:
            async def __call__(self, request):
                assert isinstance(request, UpdateUsernameRequest)
                raise rpc_error("USERNAME_OCCUPIED")

        try:
            await operation(Client())
        except rpc_error as exc:
            raise AccountProfileError(_safe_account_operation_error(exc)) from exc

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", fake_operation)
    with pytest.raises(AccountProfileError, match="занят.*Попробуйте другой"):
        await app.state.account_service.update_profile_username(
            account_id, username="occupied_handle"
        )

    account = await app.state.account_service.get_account(account_id)
    assert account.username == "old_handle"
    assert account.first_name == "Existing"
    assert account.about == "Bio stays"


@pytest.mark.asyncio
async def test_username_response_must_confirm_change_before_save(app, monkeypatch):
    account_id = await _account(app)

    async def fake_operation(aid, operation):
        class Client:
            async def __call__(self, request):
                return SimpleNamespace(
                    id=123456,
                    username="other_handle",
                    first_name="Existing",
                    last_name="Name",
                    photo=None,
                )

        return SimpleNamespace(username="old_handle"), await operation(Client())

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", fake_operation)
    with pytest.raises(AccountProfileError, match="не подтвердил"):
        await app.state.account_service.update_profile_username(
            account_id, username="new_handle"
        )
    assert (await app.state.account_service.get_account(account_id)).username == "old_handle"


def test_username_card_has_edit_and_confirmation_actions():
    account = Account(
        id=42,
        label="Test",
        status=AccountStatus.ACTIVE,
        session_source=AccountSessionSource.SESSION_STRING,
    )
    detail = account_details_menu(account, 7, has_proxy=False)
    detail_actions = [
        button.callback_data for row in detail.inline_keyboard for button in row
    ]
    assert "account:username:edit:42:7" in detail_actions
    confirm = account_username_confirmation(42, 7)
    confirm_actions = [
        button.callback_data for row in confirm.inline_keyboard for button in row
    ]
    assert "account:username:confirm:42:7" in confirm_actions
    assert all(len(action.encode()) <= 64 for action in detail_actions if action)


@pytest.mark.asyncio
async def test_username_edit_flow_uses_account_fsm_and_confirms(app, monkeypatch):
    account_id = await _account(app)
    service = app.state.account_service
    monkeypatch.setattr(handlers, "_remember_origin", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_callback", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_origin", AsyncMock())
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    show_details = AsyncMock()
    monkeypatch.setattr(handlers, "_show_account_details", show_details)
    change_username = AsyncMock()
    monkeypatch.setattr(service, "update_profile_username", change_username)
    storage = MemoryStorage()
    state = FSMContext(storage, StorageKey(bot_id=1, chat_id=2, user_id=2))
    callback = SimpleNamespace(data=f"account:username:edit:{account_id}:0", message=None)
    try:
        await handlers.begin_account_username_edit(callback, state, service)
        assert await state.get_state() == AccountUsernameEditState.value.state
        await handlers.account_profile_username(
            SimpleNamespace(text="@new_handle"), state, None
        )
        assert await state.get_state() == AccountUsernameEditState.confirm.state
        assert (await state.get_data())["profile_username"] == "new_handle"
        callback.data = f"account:username:confirm:{account_id}:0"
        await handlers.confirm_account_username_edit(callback, state, service, None)
        change_username.assert_awaited_once_with(account_id, username="new_handle")
        assert await state.get_state() is None
        assert "обновлён" in show_details.await_args.kwargs["banner"]
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_occupied_username_is_shown_in_card_and_fsm_is_cleared(app, monkeypatch):
    account_id = await _account(app)
    service = app.state.account_service
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    show_details = AsyncMock()
    monkeypatch.setattr(handlers, "_show_account_details", show_details)
    attempt = AsyncMock(side_effect=AccountProfileError("Этот юзернейм занят. Попробуйте другой."))
    monkeypatch.setattr(service, "update_profile_username", attempt)
    storage = MemoryStorage()
    state = FSMContext(storage, StorageKey(bot_id=1, chat_id=2, user_id=2))
    callback = SimpleNamespace(
        data=f"account:username:confirm:{account_id}:0", message=None
    )
    try:
        await state.update_data(
            profile_account_id=account_id, profile_page=0, profile_username="taken_handle"
        )
        await state.set_state(AccountUsernameEditState.confirm)
        await handlers.confirm_account_username_edit(callback, state, service, None)
        attempt.assert_awaited_once_with(account_id, username="taken_handle")
        assert "Этот юзернейм занят. Попробуйте другой." in (
            show_details.await_args.kwargs["banner"]
        )
        assert await state.get_state() is None
    finally:
        await storage.close()


@pytest.mark.asyncio
async def test_stale_username_confirmation_does_not_change_another_account(
    app, monkeypatch
):
    account_id = await _account(app)
    service = app.state.account_service
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    attempt = AsyncMock()
    monkeypatch.setattr(service, "update_profile_username", attempt)
    storage = MemoryStorage()
    state = FSMContext(storage, StorageKey(bot_id=1, chat_id=2, user_id=2))
    try:
        await state.update_data(
            profile_account_id=account_id, profile_page=0, profile_username="new_handle"
        )
        await state.set_state(AccountUsernameEditState.confirm)
        callback = SimpleNamespace(
            data=f"account:username:confirm:{account_id + 1}:0", message=None
        )
        await handlers.confirm_account_username_edit(callback, state, service, None)
        attempt.assert_not_awaited()
        assert await state.get_state() == AccountUsernameEditState.confirm.state
    finally:
        await storage.close()
