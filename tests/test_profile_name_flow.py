"""Catalogue names reach the reviewed Telegram profiles without changing the scope."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import functions, types

from proxima_traffic.bot import handlers
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.bot.states import AccountBulkNameState
from proxima_traffic.models import Account, AccountSessionSource


@pytest.mark.parametrize("locale", ["ru", "en", "mixed"])
@pytest.mark.parametrize("count", [1, 2])
async def test_catalogue_preview_and_apply_updates_only_reviewed_accounts(
    app,
    monkeypatch,
    locale,
    count,
):
    service = app.state.account_service
    repository = AdminRepository(app.state.database.session_factory)
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(
                label=f"Account {index}",
                ownership_confirmed=True,
                authorized=True,
                session_source=AccountSessionSource.SESSION_STRING,
                session_encrypted="unit-test-session",
                api_id=1,
                api_hash_encrypted="unit-test-api-hash",
                first_name="Existing name",
            )
            for index in range(3)
        ]
        session.add_all(accounts)
        await session.commit()
    sent = {}

    async def profile_operation(account_id, operation):
        class Client:
            async def __call__(self, request):
                assert isinstance(request, functions.account.UpdateProfileRequest)
                sent[account_id] = (request.first_name, request.last_name)

        await operation(Client())
        first_name, last_name = sent[account_id]
        return types.User(id=1000 + account_id, first_name=first_name, last_name=last_name), None

    monkeypatch.setattr(service, "_run_profile_operation", profile_operation)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    monkeypatch.setattr(handlers, "_remember_origin", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_callback", AsyncMock())
    details = AsyncMock()
    monkeypatch.setattr(handlers, "_show_account_details", details)
    storage = MemoryStorage()
    state = FSMContext(storage, StorageKey(bot_id=1, chat_id=2, user_id=2))
    selected_id = accounts[0].id if count == 1 else 0
    selected_ids = [account.id for account in accounts[:count]]
    if count > 1:
        await state.update_data(
            profile_name_scope_ids=selected_ids,
            profile_name_scope_label="Selected group",
        )
    callback = SimpleNamespace(
        data=f"account:names:preview:{locale}:{selected_id}:0",
        message=None,
    )
    try:
        await handlers.preview_generated_account_names(callback, state, repository, service)
        assert await state.get_state() == AccountBulkNameState.confirm.state
        data = await state.get_data()
        assignments = data["profile_name_assignments"]
        assert {item["account_id"] for item in assignments} == set(selected_ids)
        assert len(assignments) == count and not sent
        expected = {
            item["account_id"]: (item["first_name"], item["last_name"]) for item in assignments
        }
        callback.data = "account:names:apply"
        await handlers.apply_generated_account_names(callback, state, repository, service, None)
        assert sent == expected
        assert await state.get_state() is None
        for account in accounts:
            saved = await service.get_account(account.id)
            if account.id in expected:
                assert (saved.first_name, saved.last_name or "") == expected[account.id]
            else:
                assert saved.first_name == "Existing name"
        if count == 1:
            assert "Имена обновлены: <b>1</b>" in details.call_args.kwargs["banner"]
    finally:
        await storage.close()
