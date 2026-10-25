from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from telethon import errors

from proxima_traffic.accounts import (
    AccountBulkCheckEntry,
    AccountBulkCheckSummary,
    AccountCheckResult,
    AccountInvalidSessionCleanupSummary,
    DecryptedSession,
    _account_check_failure_code,
)
from proxima_traffic.bot import handlers
from proxima_traffic.bot.keyboards import account_bulk_cleanup_confirmation
from proxima_traffic.gateways import PermanentGatewayError, TemporaryGatewayError
from proxima_traffic.models import (
    Account,
    AccountOperationLease,
    AccountSessionSource,
    AccountStatus,
    utc_now,
)
from proxima_traffic.services import ConflictError


async def create_accounts(app, count=2):
    async with app.state.database.session_factory() as session:
        accounts = [Account(
            label=f"candidate-{i}", ownership_confirmed=True, authorized=True,
            session_source=AccountSessionSource.SESSION_STRING,
            session_encrypted=f"encrypted-{i}", api_id=1, api_hash_encrypted="encrypted-api",
        ) for i in range(count)]
        session.add_all(accounts)
        await session.commit()
    return accounts


def revoked(account_id):
    return AccountCheckResult(
        account_id, False, None, None, None, "session revoked", "account_unauthorized"
    )


@pytest.mark.parametrize("code", [
    "account_dead", "account_unauthorized", "proxy_unavailable", "flood_wait",
    "session_material_invalid", "account_operation_busy",
])
def test_normalized_gateway_check_preserves_the_reason(code):
    assert _account_check_failure_code(PermanentGatewayError("reason", code=code)) == code


@pytest.mark.parametrize("origin", [
    errors.ApiIdPublishedFloodError(None), errors.AuthKeyPermEmptyError(None),
    errors.PremiumCurrentlyUnavailableError(None), errors.AuthKeyError(None, "UNKNOWN_AUTH_ERROR"),
])
def test_old_gateway_wrapper_does_not_turn_api_or_temporary_key_errors_into_deletion(origin):
    wrapper = PermanentGatewayError("old classification", code="account_dead")
    wrapper.__cause__ = origin
    assert _account_check_failure_code(wrapper) == "account_check_unavailable"


@pytest.mark.parametrize(
    "code,confirmed",
    [
        ("account_unauthorized", True),
        ("account_dead", True),
        ("session_material_invalid", False),
        ("proxy_unavailable", False),
        ("flood_wait", False),
    ],
)
async def test_session_check_marks_only_definite_telegram_refusals_for_cleanup(
    app, monkeypatch, code, confirmed
):
    account = (await create_accounts(app, 1))[0]
    service = app.state.account_service
    monkeypatch.setattr(
        service,
        "load_session",
        AsyncMock(
            return_value=DecryptedSession(
                account.id, "irrelevant-test-session", 1, "irrelevant-test-api"
            )
        ),
    )
    monkeypatch.setattr(
        service, "_fetch_me", AsyncMock(side_effect=PermanentGatewayError("reason", code=code))
    )
    result = await service.check_account(account.id)
    assert AccountBulkCheckEntry(account.id, account.label, result).session_revoked is confirmed
    assert result.authorized is (not confirmed)
    stored = await service.get_account(account.id)
    expected_status = AccountStatus.DISABLED if confirmed else AccountStatus.ACTIVE
    assert stored.status == expected_status
    if code == "flood_wait":
        assert stored.cooldown_reason == "flood_wait"
        assert stored.cooldown_until is not None


async def test_confirmed_cleanup_removes_only_the_reviewed_accounts(app, monkeypatch):
    accounts = await create_accounts(app)
    service = app.state.account_service
    monkeypatch.setattr(service, "check_account", AsyncMock(side_effect=lambda account_id, **kw:
        revoked(account_id)))
    preview = await service.check_all_sessions(account_ids=[accounts[0].id])
    fingerprint = preview.session_revoked[0].session_fingerprint
    result = await service.remove_confirmed_invalid_sessions(
        account_ids=[accounts[0].id], expected_session_fingerprints={accounts[0].id: fingerprint}
    )
    assert [item.account_id for item in result.deleted] == [accounts[0].id]
    assert (await service.get_account(accounts[1].id)).session_encrypted is not None


@pytest.mark.parametrize("when", ["before_recheck", "before_delete", "history_fallback"])
async def test_cleanup_preserves_session_restored_after_preview(app, monkeypatch, when):
    account = (await create_accounts(app, 1))[0]
    service = app.state.account_service
    monkeypatch.setattr(service, "check_account", AsyncMock(side_effect=lambda account_id, **kw:
        revoked(account_id)))
    preview = await service.check_all_sessions()
    expected = {account.id: preview.session_revoked[0].session_fingerprint}

    async def restore():
        async with app.state.database.session_factory() as session:
            stored = await session.get(Account, account.id)
            stored.session_encrypted = "restored-session"
            await session.commit()

    if when == "before_recheck":
        await restore()
    else:
        delete = service.delete_account

        async def changed_delete(account_id, **kwargs):
            await restore()
            if when == "history_fallback":
                raise ConflictError("referenced by history")
            return await delete(account_id, **kwargs)

        monkeypatch.setattr(service, "delete_account", changed_delete)
    result = await service.remove_confirmed_invalid_sessions(
        account_ids=[account.id], expected_session_fingerprints=expected
    )
    assert result.removed == ()
    assert "сессия изменилась" in result.skipped[0].reason
    assert (await service.get_account(account.id)).session_encrypted == "restored-session"


async def test_a_recovered_or_temporarily_unreachable_session_is_preserved(app, monkeypatch):
    accounts = await create_accounts(app, 2)
    service = app.state.account_service
    monkeypatch.setattr(service, "check_account", AsyncMock(side_effect=lambda account_id, **kw:
        revoked(account_id)))
    preview = await service.check_all_sessions()
    expected = {item.account_id: item.session_fingerprint for item in preview.session_revoked}

    async def recheck(account_id, **kwargs):
        if account_id == accounts[1].id:
            raise TemporaryGatewayError("timeout", code="network_error")
        return AccountCheckResult(account_id, True, None, None, None, None)

    monkeypatch.setattr(service, "check_account", recheck)
    result = await service.remove_confirmed_invalid_sessions(
        account_ids=list(expected), expected_session_fingerprints=expected
    )
    assert not result.removed
    async with app.state.database.session_factory() as session:
        assert len(list(await session.scalars(select(Account)))) == 2


async def test_cleanup_does_not_erase_work_started_after_initial_removal_plan(app, monkeypatch):
    account = (await create_accounts(app, 1))[0]
    service = app.state.account_service
    monkeypatch.setattr(service, "check_account", AsyncMock(side_effect=lambda account_id, **kw:
        revoked(account_id)))
    delete = service.delete_account

    async def start_work_then_delete(account_id, **kwargs):
        async with app.state.database.session_factory() as session:
            session.add(AccountOperationLease(
                account_id=account_id, owner_id="new-worker", lease_token="new-operation",
                lease_until=utc_now() + timedelta(minutes=2),
            ))
            await session.commit()
        return await delete(account_id, **kwargs)

    monkeypatch.setattr(service, "delete_account", start_work_then_delete)
    result = await service.remove_confirmed_invalid_sessions()
    assert not result.removed
    assert "активная задача" in result.skipped[0].reason
    assert (await service.get_account(account.id)).session_encrypted is not None


class State:
    def __init__(self):
        self.data = {}

    async def clear(self):
        self.data.clear()

    async def get_data(self):
        return self.data.copy()

    async def update_data(self, **kwargs):
        self.data.update(kwargs)


async def test_preview_is_required_and_confirmation_is_bound_to_reviewed_list(monkeypatch):
    state = State()
    service = SimpleNamespace(
        check_all_sessions=AsyncMock(return_value=AccountBulkCheckSummary((
            AccountBulkCheckEntry(2, "candidate", revoked(2), session_fingerprint="proof"),
        ))),
        remove_confirmed_invalid_sessions=AsyncMock(return_value=AccountInvalidSessionCleanupSummary(
            AccountBulkCheckSummary(()), (), (),
        )),
    )
    callback = SimpleNamespace(data="account:check:purge:confirm:0:stale", answer=AsyncMock())
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.confirm_purge_confirmed_invalid_sessions(callback, state, service)
    service.remove_confirmed_invalid_sessions.assert_not_awaited()
    callback.data = "account:check:purge:ask:0"
    await handlers.ask_purge_confirmed_invalid_sessions(callback, state, service)
    assert "candidate" in edit.call_args.args[1]
    token = state.data["account_cleanup"]["token"]
    callback.data = f"account:check:purge:confirm:0:{token}"
    await handlers.confirm_purge_confirmed_invalid_sessions(callback, state, service)
    service.remove_confirmed_invalid_sessions.assert_awaited_once_with(
        account_ids=[2], expected_session_fingerprints={2: "proof"}
    )
    await handlers.confirm_purge_confirmed_invalid_sessions(callback, state, service)
    assert service.remove_confirmed_invalid_sessions.await_count == 1


def test_empty_cleanup_preview_has_no_delete_button():
    buttons = account_bulk_cleanup_confirmation(0, token="proof", count=0).inline_keyboard
    assert not any("confirm" in button.callback_data for row in buttons for button in row)
