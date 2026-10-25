"""Account management regressions with private databases and simulated Telegram."""

import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from telethon.errors import FloodWaitError
from telethon.tl import types

from proxima_traffic.accounts import AccountProfileError, AccountSessionChangedError
from proxima_traffic.bot import handlers
from proxima_traffic.bot.repository import AdminRepository, Page
from proxima_traffic.gateways import PermanentGatewayError, TemporaryGatewayError
from proxima_traffic.models import (
    Account,
    AccountOperationLease,
    AccountSessionSource,
    AccountStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.profile_names import ProfileName
from proxima_traffic.services import ConflictError
from proxima_traffic.spambot import SpamBotResult, SpamBotStatus
from proxima_traffic.spambot_checks import SpamBotCheckService


async def account_row(app, **values):
    async with app.state.database.session_factory() as session:
        account = Account(label=values.pop("label", "quality-account"), **values)
        session.add(account)
        await session.commit()
        return int(account.id)


class SpamGateway:
    def __init__(self, result):
        self.result = result
        self.calls = 0

    async def check_spambot_status(self, *, account):
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.asyncio
async def test_manual_pause_survives_restriction_and_clear(app):
    aid = await account_row(app, status=AccountStatus.PAUSED, operator_paused=True)
    gateway = SpamGateway(SpamBotResult(SpamBotStatus.ACTIVE_RESTRICTION, "restricted"))
    service = SpamBotCheckService(app.state.database.session_factory, gateway)
    await service.check_account(aid, trigger="test", force=True)
    gateway.result = SpamBotResult(SpamBotStatus.FREE, "free")
    await service.check_account(aid, trigger="test", force=True)
    stored = await app.state.account_service.get_account(aid)
    assert stored.status == AccountStatus.PAUSED
    assert stored.operator_paused is True
    assert stored.cooldown_reason is None


@pytest.mark.asyncio
async def test_spambot_keeps_longer_flood_and_its_own_expiry(app):
    deadline = utc_now() + timedelta(hours=2)
    restriction_end = utc_now() + timedelta(minutes=20)
    aid = await account_row(
        app, status=AccountStatus.PAUSED, cooldown_reason="flood_wait", cooldown_until=deadline
    )
    gateway = SpamGateway(None)
    service = SpamBotCheckService(app.state.database.session_factory, gateway)
    await service._persist_result(
        aid,
        SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION, "limited", restricted_until=restriction_end
        ),
        trigger="test",
    )
    stored = await app.state.account_service.get_account(aid)
    assert ensure_utc(stored.cooldown_until) == deadline
    assert stored.cooldown_reason == "flood_wait"
    cached = service._recent_result(stored, now=utc_now(), force=False)
    assert cached is not None
    assert cached.restricted_until == restriction_end
    await service._persist_result(aid, SpamBotResult(SpamBotStatus.FREE, "free"), trigger="test")
    stored = await app.state.account_service.get_account(aid)
    assert ensure_utc(stored.cooldown_until) == deadline
    assert stored.status == AccountStatus.PAUSED


@pytest.mark.asyncio
async def test_expired_spambot_restriction_is_rechecked_before_cache_interval(app):
    aid = await account_row(
        app, session_source=AccountSessionSource.SESSION_STRING, session_encrypted="test-session"
    )
    gateway = SpamGateway(
        SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION,
            "expired",
            restricted_until=utc_now() - timedelta(seconds=1),
        )
    )
    service = SpamBotCheckService(app.state.database.session_factory, gateway)
    await service.check_account(aid, trigger="test", force=True)
    gateway.result = SpamBotResult(SpamBotStatus.FREE, "free")
    assert await service.check_due_accounts(limit=10) == 1
    assert gateway.calls == 2
    stored = await app.state.account_service.get_account(aid)
    assert stored.status == AccountStatus.ACTIVE


@pytest.mark.asyncio
async def test_spambot_flood_is_account_wide_and_not_retried_by_force(app):
    aid = await account_row(
        app, session_source=AccountSessionSource.SESSION_STRING, session_encrypted="test-session"
    )
    gateway = SpamGateway(TemporaryGatewayError("Flood", code="flood_wait", retry_after=12))
    service = SpamBotCheckService(app.state.database.session_factory, gateway)
    before = utc_now()
    await service.check_account(aid, trigger="test", force=True)
    stored = await app.state.account_service.get_account(aid)
    assert stored.status == AccountStatus.ACTIVE
    assert ensure_utc(stored.cooldown_until) >= before + timedelta(seconds=500)
    assert await service.check_account(aid, trigger="test", force=True) is None
    assert gateway.calls == 1
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, aid)
        stored.cooldown_until = utc_now() - timedelta(seconds=1)
        await session.commit()
    gateway.result = SpamBotResult(SpamBotStatus.FREE, "free")
    assert await service.check_due_accounts(limit=10) == 1
    assert gateway.calls == 2
    assert (await app.state.account_service.get_account(aid)).status == AccountStatus.ACTIVE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        SpamBotResult(SpamBotStatus.PERMANENT_RESTRICTION, "restricted"),
        TemporaryGatewayError("revoked", code="account_dead"),
    ],
)
async def test_spambot_old_result_cannot_disable_replaced_session(app, result):
    aid = await account_row(app, session_encrypted="old", authorized=True)
    started, finish = asyncio.Event(), asyncio.Event()

    class Gateway:
        async def check_spambot_status(self, *, account):
            started.set()
            await asyncio.wait_for(finish.wait(), 10)
            if isinstance(result, Exception):
                raise result
            return result

    service = SpamBotCheckService(app.state.database.session_factory, Gateway())
    task = asyncio.create_task(service.check_account(aid, trigger="test", force=True))
    await asyncio.wait_for(started.wait(), 10)
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, aid)
        stored.session_encrypted = "new"
        await session.commit()
    finish.set()
    assert await asyncio.wait_for(task, 10) is None
    stored = await app.state.account_service.get_account(aid)
    assert stored.authorized is True
    assert stored.status == AccountStatus.ACTIVE
    assert stored.spambot_status is None


@pytest.mark.asyncio
async def test_session_check_cannot_write_after_replacement(app, monkeypatch):
    aid = await account_row(app, authorized=True, session_encrypted="old")
    service = app.state.account_service
    started, finish = asyncio.Event(), asyncio.Event()

    async def load(account_id):
        return SimpleNamespace(account_id=account_id)

    async def fetch(*args):
        started.set()
        await asyncio.wait_for(finish.wait(), 10)
        return None

    monkeypatch.setattr(service, "load_session", load)
    monkeypatch.setattr(service, "_fetch_me", fetch)
    task = asyncio.create_task(service.check_account(aid))
    await asyncio.wait_for(started.wait(), 10)
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, aid)
        stored.session_encrypted = "new"
        await session.commit()
    finish.set()
    with pytest.raises(AccountSessionChangedError):
        await asyncio.wait_for(task, 10)
    stored = await service.get_account(aid)
    assert stored.authorized is True
    assert stored.status == AccountStatus.ACTIVE


@pytest.mark.asyncio
async def test_profile_result_cannot_revive_cleared_session(app, monkeypatch):
    aid = await account_row(app, first_name="Old", session_encrypted="old")
    service = app.state.account_service

    async def operation(account_id, callback):
        async with app.state.database.session_factory() as session:
            stored = await session.get(Account, account_id)
            stored.session_encrypted = None
            stored.status = AccountStatus.DISABLED
            await session.commit()
        return SimpleNamespace(first_name="New", last_name="", photo=None), None

    monkeypatch.setattr(service, "_run_profile_operation", operation)
    with pytest.raises(AccountSessionChangedError):
        await service.update_profile_name(aid, first_name="New")
    stored = await service.get_account(aid)
    assert stored.first_name == "Old"
    assert stored.status == AccountStatus.DISABLED
    assert stored.authorized is False


@pytest.mark.asyncio
async def test_unconfirmed_profile_name_is_not_reported_as_saved(app, monkeypatch):
    aid = await account_row(app, first_name="Old")

    async def operation(*args):
        return SimpleNamespace(first_name="Old", last_name="", photo=None), None

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", operation)
    with pytest.raises(AccountProfileError, match="не подтвердил"):
        await app.state.account_service.update_profile_name(aid, first_name="New")
    assert (await app.state.account_service.get_account(aid)).first_name == "Old"


@pytest.mark.asyncio
async def test_bio_is_confirmed_from_full_profile(app, monkeypatch):
    aid = await account_row(app, about="Old")

    class Client:
        async def __call__(self, request):
            if type(request).__name__ == "GetFullUserRequest":
                return SimpleNamespace(full_user=SimpleNamespace(about="Old"))
            return SimpleNamespace()

    async def operation(account_id, callback):
        result = await callback(Client())
        return SimpleNamespace(first_name="Name", photo=None), result

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", operation)
    with pytest.raises(AccountProfileError, match="не подтвердил"):
        await app.state.account_service.update_profile_about(aid, about="New")
    assert (await app.state.account_service.get_account(aid)).about == "Old"


@pytest.mark.asyncio
async def test_profile_photo_requires_matching_server_photo(app, monkeypatch):
    aid = await account_row(app, has_profile_photo=False)

    async def operation(*args):
        return SimpleNamespace(photo=SimpleNamespace(photo_id=41)), SimpleNamespace(
            photo=SimpleNamespace(id=42)
        )

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", operation)
    with pytest.raises(AccountProfileError, match="не подтвердил"):
        await app.state.account_service.update_profile_photo(
            aid, photo_bytes=b"test", file_name="test.jpg"
        )
    assert (await app.state.account_service.get_account(aid)).has_profile_photo is False


@pytest.mark.asyncio
async def test_photo_deletion_records_only_confirmed_ids_and_actual_photo(app, monkeypatch):
    aid = await account_row(app, has_profile_photo=True)
    photos = [
        types.InputPhoto(id=value, access_hash=1, file_reference=b"test") for value in (41, 42)
    ]

    class Client:
        calls = 0

        async def get_profile_photos(self, entity):
            self.calls += 1
            return photos if self.calls == 1 else [photos[1]]

        async def __call__(self, request):
            return [41, 41, 999]

    async def operation(account_id, callback):
        result = await callback(Client())
        return SimpleNamespace(photo=SimpleNamespace(photo_id=42)), result

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", operation)
    with pytest.raises(AccountProfileError, match="подтвердил 1, осталось 1"):
        await app.state.account_service.delete_profile_photos(aid)
    assert (await app.state.account_service.get_account(aid)).has_profile_photo is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        FloodWaitError(request=None, capture=900),
        TemporaryGatewayError("FloodWait", code="flood_wait", retry_after=900),
    ],
)
async def test_profile_flood_preserves_server_wait_and_manual_pause(app, error):
    aid = await account_row(app, operator_paused=True, status=AccountStatus.PAUSED)

    class Coordinator:
        async def run_account_operation(self, account_id, operation):
            raise error

    service = app.state.account_service
    service.bind_client_coordinator(Coordinator())
    before = utc_now()
    with pytest.raises(AccountProfileError):
        await service.update_profile_name(aid, first_name="New")
    stored = await service.get_account(aid)
    assert ensure_utc(stored.cooldown_until) >= before + timedelta(seconds=900)
    assert stored.operator_paused is True
    assert stored.status == AccountStatus.PAUSED
    assert stored.cooldown_reason == "flood_wait"


@pytest.mark.asyncio
async def test_bulk_profile_unexpected_failure_does_not_stop_other_accounts(app, monkeypatch):
    aid = await account_row(app)

    async def update(account_id, **kwargs):
        if account_id == 999:
            raise RuntimeError("unexpected failure")
        return await app.state.account_service.get_account(account_id)

    monkeypatch.setattr(app.state.account_service, "update_profile_name", update)
    name = ProfileName(first_name="Name", last_name="", locale="ru", style="realistic")
    result = await app.state.account_service.bulk_update_profile_names({999: name, aid: name})
    assert [item.id for item in result.updated] == [aid]
    assert [item.account_id for item in result.failures] == [999]


@pytest.mark.asyncio
async def test_concurrent_group_changes_keep_both_operators_edits(app):
    repository = AdminRepository(app.state.database.session_factory)
    first = await account_row(app, label="first")
    second = await account_row(app, label="second")
    group = await repository.create_account_group(name="Test group")
    await asyncio.gather(
        repository.update_account_group_members(group.id, {first}, selected=True),
        repository.update_account_group_members(group.id, {second}, selected=True),
    )
    assert await repository.list_account_group_member_ids(group.id) == {first, second}
    await repository.update_account_group_members(group.id, {first}, selected=False)
    assert await repository.list_account_group_member_ids(group.id) == {second}


@pytest.mark.asyncio
async def test_duplicate_bulk_name_confirmation_runs_once_and_keeps_new_draft(monkeypatch):
    storage = MemoryStorage()
    key = StorageKey(bot_id=1, chat_id=1, user_id=1)
    state = FSMContext(storage=storage, key=key)
    second_state = FSMContext(storage=storage, key=key)
    await state.update_data(
        profile_name_assignments=[
            {
                "account_id": 1,
                "first_name": "Name",
                "last_name": "",
                "locale": "ru",
                "style": "realistic",
                "label": "Test",
            }
        ]
    )
    started, finish = asyncio.Event(), asyncio.Event()
    calls = []

    class Service:
        async def bulk_update_profile_names(self, assignments):
            calls.append(assignments)
            started.set()
            await asyncio.wait_for(finish.wait(), 10)
            return SimpleNamespace(updated=(), failures=())

    class Repository:
        async def list_accounts(self, *args):
            return Page(items=[], total=0, page=0, page_size=7)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(handlers, "_safe_callback_answer", noop)
    monkeypatch.setattr(handlers, "_edit_callback", noop)
    callback = SimpleNamespace(message=None)
    task = asyncio.create_task(
        handlers.apply_generated_account_names(callback, state, Repository(), Service(), None)
    )
    await asyncio.wait_for(started.wait(), 10)
    await handlers.apply_generated_account_names(
        callback, second_state, Repository(), Service(), None
    )
    await state.update_data(profile_account_id=2, profile_first_name="Another draft")
    finish.set()
    await asyncio.wait_for(task, 10)
    assert len(calls) == 1
    assert (await state.get_data())["profile_account_id"] == 2
    await storage.close()


@pytest.mark.asyncio
async def test_maintenance_skips_metadata_cards(app):
    await account_row(app)
    gateway = SpamGateway(SpamBotResult(SpamBotStatus.FREE, "free"))
    service = SpamBotCheckService(app.state.database.session_factory, gateway)
    assert await service.check_due_accounts(limit=10) == 0
    assert gateway.calls == 0


@pytest.mark.asyncio
async def test_cached_free_result_has_no_unrelated_flood_expiry(app):
    aid = await account_row(
        app,
        cooldown_until=utc_now() + timedelta(hours=1),
        cooldown_reason="flood_wait",
        spambot_status="free",
        spambot_checked_at=utc_now(),
        spambot_response_json=json.dumps({"restriction_scope": "none"}),
    )
    stored = await app.state.account_service.get_account(aid)
    service = SpamBotCheckService(app.state.database.session_factory, SpamGateway(None))
    result = service._recent_result(stored, now=utc_now(), force=False)
    assert result is not None
    assert result.restricted_until is None


@pytest.mark.asyncio
async def test_check_different_telegram_identity_pauses_without_marking_session_revoked(
    app, monkeypatch
):
    aid = await account_row(app, telegram_user_id=100, authorized=True)
    service = app.state.account_service

    async def load(account_id):
        return SimpleNamespace(account_id=account_id)

    async def fetch(*args):
        return SimpleNamespace(id=200, first_name="Wrong person", photo=None)

    monkeypatch.setattr(service, "load_session", load)
    monkeypatch.setattr(service, "_fetch_me", fetch)
    result = await service.check_account(aid)
    assert result.failure_code == "account_identity_mismatch"
    assert result.error
    stored = await service.get_account(aid)
    assert stored.telegram_user_id == 100
    assert stored.first_name is None
    assert stored.status == AccountStatus.PAUSED


@pytest.mark.asyncio
async def test_session_check_fetches_actual_description(app, monkeypatch):
    aid = await account_row(app, about=None)
    service = app.state.account_service

    async def load(account_id):
        return SimpleNamespace(account_id=account_id)

    class Client:
        async def __call__(self, request):
            assert type(request).__name__ == "GetFullUserRequest"
            return SimpleNamespace(full_user=SimpleNamespace(about="Observed bio"))

    class Coordinator:
        async def run_account_operation(self, account_id, operation):
            result = await operation(Client())
            return SimpleNamespace(first_name="Name", photo=None), result

    monkeypatch.setattr(service, "load_session", load)
    service.bind_client_coordinator(Coordinator())
    result = await service.check_account(aid)
    assert result.authorized is True and result.error is None
    assert (await service.get_account(aid)).about == "Observed bio"


@pytest.mark.asyncio
async def test_api_cannot_bypass_protective_pause(app, client, admin_headers):
    aid = await account_row(
        app,
        status=AccountStatus.PAUSED,
        authorized=True,
        cooldown_reason="flood_wait",
        cooldown_until=utc_now() + timedelta(minutes=20),
    )
    response = await client.patch(
        f"/api/v1/accounts/{aid}", headers=admin_headers, json={"status": "active"}
    )
    assert response.status_code == 409
    assert (await app.state.account_service.get_account(aid)).status == AccountStatus.PAUSED


@pytest.mark.asyncio
async def test_api_manual_pause_is_preserved_by_fresh_spambot_clear(app, client, admin_headers):
    aid = await account_row(app)
    response = await client.patch(
        f"/api/v1/accounts/{aid}", headers=admin_headers, json={"status": "paused"}
    )
    assert response.status_code == 200
    service = SpamBotCheckService(
        app.state.database.session_factory, SpamGateway(SpamBotResult(SpamBotStatus.FREE, "free"))
    )
    await service.check_account(aid, trigger="test", force=True)
    stored = await app.state.account_service.get_account(aid)
    assert stored.operator_paused is True
    assert stored.status == AccountStatus.PAUSED


@pytest.mark.asyncio
async def test_reimport_keeps_known_restriction_and_historical_card(app):
    from proxima_traffic.accounts import _ConvertedSession

    aid = await account_row(
        app,
        telegram_user_id=100,
        status=AccountStatus.DISABLED,
        session_source=AccountSessionSource.NONE,
        spambot_status="permanent_restriction",
        spambot_response_json=json.dumps({"text": "old restriction"}),
    )
    restored = await app.state.account_service._store_session(
        label="New label",
        source=AccountSessionSource.SESSION_STRING,
        converted=_ConvertedSession(
            session_string="test", api_id=1, api_hash="test", telegram_user_id=100
        ),
    )
    assert restored.id == aid
    assert restored.label == "quality-account"
    assert restored.status == AccountStatus.PAUSED
    assert restored.spambot_checked_at is None


@pytest.mark.asyncio
async def test_expired_flood_check_does_not_release_operator_pause(app, monkeypatch):
    aid = await account_row(
        app,
        status=AccountStatus.PAUSED,
        operator_paused=True,
        cooldown_reason="flood_wait",
        cooldown_until=utc_now() - timedelta(seconds=1),
    )
    service = app.state.account_service

    async def load(account_id):
        return SimpleNamespace(account_id=account_id)

    async def fetch(*args):
        return SimpleNamespace(first_name="Name", photo=None)

    monkeypatch.setattr(service, "load_session", load)
    monkeypatch.setattr(service, "_fetch_me", fetch)
    result = await service.check_account(aid)
    assert result.authorized is True
    stored = await service.get_account(aid)
    assert stored.status == AccountStatus.PAUSED
    assert stored.cooldown_reason is None
    repository = AdminRepository(app.state.database.session_factory)
    assert (await repository.toggle_account(aid)).status == AccountStatus.ACTIVE


@pytest.mark.asyncio
async def test_wrapped_gateway_revocation_during_profile_change_disables_session(app):
    aid = await account_row(app, authorized=True)

    class Coordinator:
        async def run_account_operation(self, account_id, operation):
            raise PermanentGatewayError("Session revoked", code="account_unauthorized")

    service = app.state.account_service
    service.bind_client_coordinator(Coordinator())
    with pytest.raises(AccountProfileError):
        await service.update_profile_name(aid, first_name="Name")
    stored = await service.get_account(aid)
    assert stored.status == AccountStatus.DISABLED
    assert stored.authorized is False


@pytest.mark.asyncio
async def test_unconfirmed_photo_deletion_is_not_success_even_with_empty_gallery(app, monkeypatch):
    aid = await account_row(app, has_profile_photo=True)

    class Client:
        calls = 0

        async def get_profile_photos(self, entity):
            self.calls += 1
            return (
                [types.InputPhoto(id=42, access_hash=1, file_reference=b"test")]
                if self.calls == 1
                else []
            )

        async def __call__(self, request):
            return []

    async def operation(account_id, callback):
        result = await callback(Client())
        return SimpleNamespace(photo=None), result

    monkeypatch.setattr(app.state.account_service, "_run_profile_operation", operation)
    with pytest.raises(AccountProfileError, match="Не подтверждено удалений: 1"):
        await app.state.account_service.delete_profile_photos(aid)
    assert (await app.state.account_service.get_account(aid)).has_profile_photo is False


@pytest.mark.asyncio
async def test_ordinary_account_deletion_rechecks_work_after_preview(app, monkeypatch):
    aid = await account_row(app)
    service = app.state.account_service
    original = service.account_removal_plan
    calls = 0

    async def plan(account_id):
        nonlocal calls
        result = await original(account_id)
        calls += 1
        if calls == 1:
            async with app.state.database.session_factory() as session:
                session.add(
                    AccountOperationLease(
                        account_id=account_id,
                        owner_id="another-worker",
                        lease_token="test-token",
                        lease_until=utc_now() + timedelta(minutes=5),
                    )
                )
                await session.commit()
        return result

    monkeypatch.setattr(service, "account_removal_plan", plan)
    with pytest.raises(ConflictError, match="активных задачах|занят"):
        await service.remove_account_safely(aid)
    assert (await service.get_account(aid)).id == aid


@pytest.mark.asyncio
async def test_avatar_download_cannot_apply_to_newly_selected_account(monkeypatch):
    storage = MemoryStorage()
    state = FSMContext(storage=storage, key=StorageKey(bot_id=1, chat_id=1, user_id=1))
    await state.set_state(handlers.AccountAvatarEditState.photo)
    await state.update_data(profile_account_id=1, profile_page=0)
    notices = []

    async def download(message, bot):
        await state.update_data(profile_account_id=2)
        return b"image", "avatar.jpg"

    class Service:
        async def update_profile_photo(self, *args, **kwargs):
            pytest.fail("a file for the previous account must not reach Telegram")

    class Message:
        async def answer(self, text):
            notices.append(text)

    monkeypatch.setattr(handlers, "_download_profile_photo", download)
    await handlers.account_avatar_photo(Message(), state, None, Service(), None)
    assert notices == ["Загрузка отменена: выбор аккаунта изменился"]
    assert (await state.get_data())["profile_account_id"] == 2
    await storage.close()
