from __future__ import annotations

import asyncio
import io
import json
import os
import pathlib
import sqlite3
import struct
import tempfile
import zipfile
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy import select
from telethon.crypto import AuthKey  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

from proxima_traffic.accounts import (  # noqa: E402
    AccountArchiveEntry,
    AccountCheckResult,
    AccountImportError,
    AccountProfileError,
    AccountService,
    _ConvertedSession,
    _safe_extract_zip,
)
from proxima_traffic.config import Settings  # noqa: E402
from proxima_traffic.db import Database  # noqa: E402
from proxima_traffic.models import (  # noqa: E402
    Account,
    AccountGroupMember,
    AccountSessionSource,
    AccountStatus,
)
from proxima_traffic.services import ConflictError  # noqa: E402

TEST_USER_ID = 123456789
SERVER_AUTH_KEY = bytes([42]) * 256


def _build_tdata_zip() -> tuple[bytes, bytes, int]:
    """Create a valid tdata archive fully offline via opentele2 round-trip."""
    from opentele2.api import API, UseCurrentSession
    from opentele2.tl import TelegramClient

    async def build() -> tuple[bytes, bytes, int]:
        session = StringSession()
        session.set_dc(2, "149.154.167.51", 443)
        auth_key = os.urandom(256)
        session.auth_key = AuthKey(auth_key)
        api = API.TelegramDesktop.Generate()
        client = TelegramClient(session, api=api)
        client.UserId = TEST_USER_ID
        tdesk = await client.ToTDesktop(flag=UseCurrentSession)
        with tempfile.TemporaryDirectory() as tmp:
            tdata_dir = pathlib.Path(tmp) / "tdata"
            tdesk.SaveTData(str(tdata_dir))
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
                for path in tdata_dir.rglob("*"):
                    if path.is_file():
                        archive.write(path, path.relative_to(tdata_dir.parent))
            return buffer.getvalue(), auth_key, int(client.api_id)

    return asyncio.run(build())


@pytest.fixture(scope="module")
def tdata_archive() -> tuple[bytes, bytes, int]:
    return _build_tdata_zip()


@pytest.fixture
def settings(tmp_path: pathlib.Path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'accounts.db'}",
        admin_api_key="test-admin-key-1234567890",
        account_encryption_key="account-secret-key-abcdefgh",
        telegram_api_id=99999,
        telegram_api_hash="0123456789abcdef0123456789abcdef",
        auto_create_schema=True,
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
    )


@pytest_asyncio.fixture
async def service(settings: Settings) -> AsyncIterator[AccountService]:
    database = Database(settings.database_url)
    await database.create_schema()
    account_service = AccountService(database.session_factory, settings)

    async def create_separate_session(
        _tdata_path: pathlib.Path,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
    ) -> _ConvertedSession:
        assert proxy is None
        assert two_factor_password is None
        session = StringSession()
        session.set_dc(2, "149.154.167.51", 443)
        session.auth_key = AuthKey(SERVER_AUTH_KEY)
        return _ConvertedSession(
            session_string=session.save(),
            api_id=api_id,
            api_hash=api_hash,
            telegram_user_id=TEST_USER_ID,
            first_name="Alice",
            last_name="Example",
            phone="15551234567",
            username="alice_example",
            has_profile_photo=True,
        )

    account_service._convert_tdata_path = create_separate_session  # type: ignore[method-assign]

    async def create_from_existing_session(
        source_session: str,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
    ) -> _ConvertedSession:
        assert proxy is None
        assert two_factor_password is None
        source = StringSession(source_session)
        assert source.auth_key is not None
        assert source.auth_key.key != SERVER_AUTH_KEY
        server = StringSession()
        server.set_dc(source.dc_id, source.server_address, source.port)
        server.auth_key = AuthKey(SERVER_AUTH_KEY)
        return _ConvertedSession(
            session_string=server.save(),
            api_id=api_id,
            api_hash=api_hash,
            telegram_user_id=TEST_USER_ID,
            first_name="Alice",
            last_name="Example",
            phone="15551234567",
            username="alice_example",
            has_profile_photo=True,
        )

    account_service._convert_existing_session = create_from_existing_session  # type: ignore[method-assign]
    yield account_service
    await database.dispose()


@pytest.fixture(autouse=True)
def _shim_ready() -> Iterator[None]:
    yield


async def test_import_tdata_extracts_and_encrypts_session(
    service: AccountService,
    settings: Settings,
    tdata_archive: tuple[bytes, bytes, int],
) -> None:
    zip_bytes, desktop_auth_key, _ = tdata_archive

    account = await service.import_tdata(label="Личный EU", archive_bytes=zip_bytes)

    assert account.session_source == AccountSessionSource.TDATA
    assert account.telegram_user_id == TEST_USER_ID
    assert account.api_id == settings.telegram_api_id
    assert account.ownership_confirmed is True
    # The stored blob must never be the plaintext session string.
    assert account.session_encrypted is not None

    decrypted = await service.load_session(account.id)
    restored = StringSession(decrypted.session_string)
    assert restored.auth_key is not None
    assert restored.auth_key.key == SERVER_AUTH_KEY
    assert restored.auth_key.key != desktop_auth_key
    assert restored.dc_id == 2
    assert decrypted.api_id == settings.telegram_api_id
    assert account.session_encrypted != decrypted.session_string


async def test_duplicate_tdata_is_rejected(
    service: AccountService, tdata_archive: tuple[bytes, bytes, int]
) -> None:
    zip_bytes, _, _ = tdata_archive
    await service.import_tdata(label="First", archive_bytes=zip_bytes)
    with pytest.raises(ConflictError):
        await service.import_tdata(label="Second", archive_bytes=zip_bytes)


async def test_import_rejects_non_zip(service: AccountService) -> None:
    with pytest.raises(AccountImportError):
        await service.import_tdata(label="Broken", archive_bytes=b"not a zip file")


async def test_import_rejects_zip_without_tdata(service: AccountService) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("notes.txt", "hello")
    with pytest.raises(AccountImportError):
        await service.import_tdata(label="Empty", archive_bytes=buffer.getvalue())


async def test_bulk_profile_photo_updates_are_bounded_and_continue_after_failure(
    service: AccountService,
) -> None:
    active = 0
    peak_active = 0

    async def update_photo(
        account_id: int,
        *,
        photo_bytes: bytes,
        file_name: str,
        actor: str = "control-bot",
    ) -> Account:
        nonlocal active, peak_active
        assert photo_bytes == b"photo"
        assert file_name == "avatar.jpg"
        assert actor == "test"
        active += 1
        peak_active = max(peak_active, active)
        try:
            await asyncio.sleep(0.01)
            if account_id == 3:
                raise AccountProfileError("session expired")
            return Account(id=account_id, label=f"Account {account_id}")
        finally:
            active -= 1

    service.update_profile_photo = update_photo  # type: ignore[method-assign]

    result = await service.bulk_update_profile_photos(
        [1, 2, 3, 4, 4],
        photo_bytes=b"photo",
        file_name="avatar.jpg",
        concurrency=2,
        actor="test",
    )

    assert peak_active == 2
    assert [account.id for account in result.updated] == [1, 2, 4]
    assert [(failure.account_id, failure.error) for failure in result.failures] == [
        (3, "session expired")
    ]


def test_safe_extract_rejects_sibling_prefix_traversal(tmp_path: pathlib.Path) -> None:
    destination = tmp_path / "extract"
    destination.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("../extract_evil/key_datas", "not really tdata")

    with pytest.raises(AccountImportError):
        _safe_extract_zip(buffer.getvalue(), destination)

    assert not (tmp_path / "extract_evil").exists()


async def test_import_session_string_roundtrip(service: AccountService, settings: Settings) -> None:
    session = StringSession()
    session.set_dc(4, "149.154.167.91", 443)
    auth_key = os.urandom(256)
    session.auth_key = AuthKey(auth_key)
    string = session.save()

    account = await service.import_session_string(label="From string", session_string=string)
    assert account.session_source == AccountSessionSource.SESSION_STRING

    decrypted = await service.load_session(account.id)
    restored = StringSession(decrypted.session_string)
    assert restored.auth_key.key == SERVER_AUTH_KEY
    assert restored.auth_key.key != auth_key
    assert decrypted.api_id == settings.telegram_api_id


async def test_import_without_label_uses_telegram_profile_name(service: AccountService) -> None:
    source = StringSession()
    source.set_dc(2, "149.154.167.51", 443)
    source.auth_key = AuthKey(os.urandom(256))

    account = await service.import_session_string(session_string=source.save())

    assert account.label == "Alice Example"
    assert account.first_name == "Alice"
    assert account.last_name == "Example"
    assert account.phone == "15551234567"
    assert account.username == "alice_example"
    assert account.has_profile_photo is True
    assert account.authorized is True
    assert account.last_checked_at is not None


async def test_automatic_label_gets_suffix_when_profile_name_exists(
    service: AccountService,
) -> None:
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        session.add(Account(label="Alice Example", ownership_confirmed=True))
        await session.commit()
    source = StringSession()
    source.set_dc(2, "149.154.167.51", 443)
    source.auth_key = AuthKey(os.urandom(256))

    account = await service.import_session_string(session_string=source.save())

    assert account.label == "Alice Example (2)"


async def test_archive_import_group_contains_only_its_imported_accounts(
    service: AccountService,
) -> None:
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        first = Account(label="archive first", ownership_confirmed=True)
        second = Account(label="archive second", ownership_confirmed=True)
        session.add_all([first, second])
        await session.commit()
        first_id, second_id = first.id, second.id

    group = await service.create_archive_import_group([second_id, first_id, first_id])
    duplicate_group = await service.create_archive_import_group([first_id, second_id])

    assert group.name.startswith("ZIP-импорт · ")
    assert group.name != duplicate_group.name
    assert group.description.endswith("аккаунтов: 2")
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        member_ids = set(
            await session.scalars(
                select(AccountGroupMember.account_id).where(AccountGroupMember.group_id == group.id)
            )
        )
    assert member_ids == {first_id, second_id}


async def test_private_label_rename_does_not_change_telegram_name(
    service: AccountService,
) -> None:
    source = StringSession()
    source.set_dc(2, "149.154.167.51", 443)
    source.auth_key = AuthKey(os.urandom(256))
    account = await service.import_session_string(session_string=source.save())

    renamed = await service.rename_account_label(account.id, label="Мой рабочий аккаунт")

    assert renamed.label == "Мой рабочий аккаунт"
    assert renamed.first_name == "Alice"
    assert renamed.last_name == "Example"


async def test_import_session_file_creates_separate_server_key(
    service: AccountService,
    tmp_path: pathlib.Path,
) -> None:
    from telethon.sessions import SQLiteSession

    source_path = tmp_path / "source"
    source = SQLiteSession(str(source_path))
    source.set_dc(2, "149.154.167.51", 443)
    source_key = os.urandom(256)
    source.auth_key = AuthKey(source_key)
    source.save()
    source.close()

    account = await service.import_session_file(
        label="From file",
        session_bytes=source_path.with_suffix(".session").read_bytes(),
    )

    decrypted = await service.load_session(account.id)
    restored = StringSession(decrypted.session_string)
    assert account.session_source == AccountSessionSource.SESSION_FILE
    assert restored.auth_key is not None
    assert restored.auth_key.key == SERVER_AUTH_KEY
    assert restored.auth_key.key != source_key


async def test_import_pyrogram_session_file(
    service: AccountService,
    tmp_path: pathlib.Path,
) -> None:
    source_key = os.urandom(256)
    path = tmp_path / "pyrogram.session"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE sessions ("
            "dc_id INTEGER PRIMARY KEY, api_id INTEGER, test_mode INTEGER, "
            "auth_key BLOB, date INTEGER, user_id INTEGER, is_bot INTEGER)"
        )
        connection.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (4, 12345, 0, source_key, 0, TEST_USER_ID, 0),
        )

    account = await service.import_session_file(session_bytes=path.read_bytes())

    decrypted = await service.load_session(account.id)
    restored = StringSession(decrypted.session_string)
    assert account.label == "Alice Example"
    assert restored.auth_key is not None
    assert restored.auth_key.key == SERVER_AUTH_KEY
    assert restored.auth_key.key != source_key


async def test_import_json_with_telethon_session(service: AccountService) -> None:
    source = StringSession()
    source.set_dc(3, "149.154.175.100", 443)
    source_key = os.urandom(256)
    source.auth_key = AuthKey(source_key)

    account = await service.import_json(
        json_bytes=json.dumps({"account": {"session_string": source.save()}}).encode()
    )

    decrypted = await service.load_session(account.id)
    assert StringSession(decrypted.session_string).auth_key.key == SERVER_AUTH_KEY


def test_read_json_with_raw_auth_key() -> None:
    source_key = os.urandom(256)
    session_string = AccountService._read_json_session(
        json.dumps({"dc_id": 5, "auth_key": source_key.hex()}).encode()
    )

    restored = StringSession(session_string)
    assert restored.dc_id == 5
    assert restored.auth_key is not None
    assert restored.auth_key.key == source_key


def test_read_json_rejects_metadata_only_export() -> None:
    with pytest.raises(AccountImportError, match="только данные профиля"):
        AccountService._read_json_session(
            json.dumps({"phone": "+15551234567", "first_name": "Alice"}).encode()
        )


def test_read_account_archive_pairs_session_and_companion_json() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("7085388912.session", b"first-session")
        archive.writestr("7085388912.json", b'{"twoFA":"secret"}')
        archive.writestr("nested/5804639627.session", b"second-session")
        archive.writestr("readme.txt", b"ignored")

    entries = AccountService.read_account_archive(buffer.getvalue())

    assert [entry.source_name for entry in entries] == [
        "7085388912",
        "nested/5804639627",
    ]
    assert entries[0].session_bytes == b"first-session"
    assert entries[0].json_bytes == b'{"twoFA":"secret"}'
    assert entries[1].session_bytes == b"second-session"
    assert entries[1].json_bytes is None
    assert "first-session" not in repr(entries[0])
    assert "secret" not in repr(entries[0])


def test_read_account_archive_rejects_unsafe_paths_and_duplicate_credentials() -> None:
    unsafe = io.BytesIO()
    with zipfile.ZipFile(unsafe, "w") as archive:
        archive.writestr("../stolen.session", b"session")
    with pytest.raises(AccountImportError, match="небезопасные пути"):
        AccountService.read_account_archive(unsafe.getvalue())

    duplicate = io.BytesIO()
    with zipfile.ZipFile(duplicate, "w") as archive:
        archive.writestr("account.session", b"one")
        archive.writestr("ACCOUNT.SESSION", b"two")
    with pytest.raises(AccountImportError, match="повторяется"):
        AccountService.read_account_archive(duplicate.getvalue())


async def test_import_archive_entry_prefers_session_and_uses_companion_twofa() -> None:
    captured: dict[str, object] = {}

    class ArchiveService:
        async def import_session_file(self, **kwargs):
            captured.update(kwargs)
            return "imported"

        async def import_json(self, **kwargs):  # pragma: no cover - must not be used
            raise AssertionError("JSON fallback should not run")

    entry = AccountArchiveEntry(
        source_name="7085388912",
        session_bytes=b"session",
        json_bytes=json.dumps({"account": {"twoFA": " account-password "}}).encode(),
    )

    result = await AccountService.import_archive_entry(  # type: ignore[arg-type]
        ArchiveService(),
        entry,
        proxy_route=None,
    )

    assert result == "imported"
    assert captured["session_bytes"] == b"session"
    assert captured["two_factor_password"] == "account-password"


async def test_import_archive_entry_falls_back_to_authorization_json() -> None:
    calls: list[str] = []

    class ArchiveService:
        async def import_session_file(self, **kwargs):
            calls.append("session")
            raise AccountImportError("damaged session")

        async def import_json(self, **kwargs):
            calls.append("json")
            assert kwargs["json_bytes"] == b'{"session":"valid"}'
            return "fallback"

    result = await AccountService.import_archive_entry(  # type: ignore[arg-type]
        ArchiveService(),
        AccountArchiveEntry(
            source_name="account",
            session_bytes=b"broken",
            json_bytes=b'{"session":"valid"}',
        ),
    )

    assert result == "fallback"
    assert calls == ["session", "json"]


def test_read_pyrogram_session_string() -> None:
    import base64

    source_key = os.urandom(256)
    raw = struct.pack(">BI?256sQ?", 2, 12345, False, source_key, TEST_USER_ID, False)
    pyrogram_string = base64.urlsafe_b64encode(raw).decode().rstrip("=")

    restored = StringSession(AccountService._read_any_session_string(pyrogram_string))

    assert restored.dc_id == 2
    assert restored.auth_key is not None
    assert restored.auth_key.key == source_key


async def test_import_session_string_requires_api(tmp_path: pathlib.Path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'noapi.db'}",
        admin_api_key="test-admin-key-1234567890",
        auto_create_schema=True,
        dry_run=True,
        worker_enabled=False,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    try:
        service = AccountService(database.session_factory, settings)
        with pytest.raises(AccountImportError, match="API"):
            await service.import_session_string(label="x", session_string="1BQ")
    finally:
        await database.dispose()


async def test_tdata_conversion_creates_and_closes_a_separate_session(
    service: AccountService,
    settings: Settings,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import opentele2.td

    calls: dict[str, object] = {}

    class ServerClient:
        def __init__(self, session: StringSession) -> None:
            self.session = session
            self.disconnected = False

        async def is_user_authorized(self) -> bool:
            return True

        async def get_me(self):
            return type("Me", (), {"id": TEST_USER_ID})()

        async def disconnect(self) -> None:
            self.disconnected = True

    class SourceClient:
        def __init__(self) -> None:
            self.disconnected = False
            self.session = StringSession()
            self.session.set_dc(4, "149.154.167.91", 443)
            self.session.auth_key = AuthKey(bytes([7]) * 256)

        async def connect(self) -> None:
            calls["source_connected"] = True

        async def is_user_authorized(self) -> bool:
            return True

        async def QRLoginToNewClient(self, *, session, api, password, proxy, **kwargs):
            calls["new_api_id"] = api.api_id
            calls["password"] = password
            calls["proxy"] = proxy
            calls["connect_timeout"] = kwargs["timeout"]
            calls["request_retries"] = kwargs["request_retries"]
            session.set_dc(4, "149.154.167.91", 443)
            session.auth_key = AuthKey(SERVER_AUTH_KEY)
            client = ServerClient(session)
            calls["server_client"] = client
            return client

        async def disconnect(self) -> None:
            self.disconnected = True

    source_client = SourceClient()

    class FakeTDesktop:
        accountsCount = 1

        def __init__(self, path: str) -> None:
            calls["tdata_path"] = path

        @staticmethod
        def isLoaded() -> bool:
            return True

        async def ToTelethon(self, **kwargs):
            calls["source_proxy"] = kwargs["proxy"]
            return source_client

    monkeypatch.setattr(opentele2.td, "TDesktop", FakeTDesktop)
    proxy = (2, "127.0.0.1", 1080, True, "user", "password")
    converted = await AccountService._convert_tdata_path(
        service,
        tmp_path,
        api_id=int(settings.telegram_api_id or 0),
        api_hash=str(settings.telegram_api_hash or ""),
        proxy=proxy,
        two_factor_password=None,
    )

    restored = StringSession(converted.session_string)
    assert restored.auth_key is not None
    assert restored.auth_key.key == SERVER_AUTH_KEY
    assert converted.api_id == settings.telegram_api_id
    assert converted.telegram_user_id == TEST_USER_ID
    assert calls["new_api_id"] == settings.telegram_api_id
    assert calls["source_proxy"] == proxy
    assert calls["proxy"] == proxy
    assert calls["request_retries"] == 2
    assert source_client.disconnected is True
    server_client = calls["server_client"]
    assert isinstance(server_client, ServerClient)
    assert server_client.disconnected is True


async def test_delete_account_erases_session(
    service: AccountService, tdata_archive: tuple[bytes, bytes, int]
) -> None:
    zip_bytes, _, _ = tdata_archive
    account = await service.import_tdata(label="To delete", archive_bytes=zip_bytes)
    await service.delete_account(account.id)
    from proxima_traffic.services import NotFoundError

    with pytest.raises(NotFoundError):
        await service.get_account(account.id)


async def test_load_session_missing_for_metadata_account(service: AccountService) -> None:
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        account = Account(label="Meta only", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
        account_id = account.id
    with pytest.raises(AccountImportError):
        await service.load_session(account_id)


async def test_account_check_keeps_last_known_authorization_on_network_failure(
    service: AccountService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = StringSession()
    source.set_dc(2, "149.154.167.51", 443)
    source.auth_key = AuthKey(os.urandom(256))
    account = await service.import_session_string(session_string=source.save())

    async def unavailable(*args: object) -> object:
        del args
        raise TimeoutError("network timeout")

    monkeypatch.setattr(service, "_fetch_me", unavailable)
    result = await service.check_account(int(account.id))

    async with service._session_factory() as session:  # type: ignore[attr-defined]
        stored = await session.get(Account, account.id)
    assert result.authorized is True
    assert result.error == "network timeout"
    assert stored is not None and stored.authorized is True
    assert stored.status == AccountStatus.ACTIVE


async def test_account_check_disables_a_definitively_revoked_session(
    service: AccountService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = StringSession()
    source.set_dc(2, "149.154.167.51", 443)
    source.auth_key = AuthKey(os.urandom(256))
    account = await service.import_session_string(session_string=source.save())

    class SessionRevokedError(Exception):
        pass

    async def revoked(*args: object) -> object:
        del args
        raise SessionRevokedError("session revoked")

    monkeypatch.setattr(service, "_fetch_me", revoked)
    result = await service.check_account(int(account.id))

    async with service._session_factory() as session:  # type: ignore[attr-defined]
        stored = await session.get(Account, account.id)
    assert result.authorized is False
    assert stored is not None and stored.authorized is False
    assert stored.status == AccountStatus.DISABLED
    assert stored.cooldown_reason == "account_dead"


async def test_bulk_session_check_separates_revoked_sessions_from_other_issues(
    service: AccountService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        accounts = [
            Account(
                label="ready-session",
                ownership_confirmed=True,
                session_source=AccountSessionSource.SESSION_STRING,
            ),
            Account(
                label="revoked-session",
                ownership_confirmed=True,
                session_source=AccountSessionSource.SESSION_STRING,
            ),
            Account(
                label="broken-import",
                ownership_confirmed=True,
                session_source=AccountSessionSource.SESSION_STRING,
            ),
            Account(label="metadata-only", ownership_confirmed=True),
        ]
        session.add_all(accounts)
        await session.commit()
        account_ids = {account.label: int(account.id) for account in accounts}

    async def fake_check(account_id: int, *, actor: str = "control-bot") -> AccountCheckResult:
        del actor
        if account_id == account_ids["broken-import"]:
            raise AccountImportError("encrypted session cannot be read")
        revoked = account_id == account_ids["revoked-session"]
        return AccountCheckResult(
            account_id=account_id,
            authorized=not revoked,
            telegram_user_id=None,
            phone=None,
            username=None,
            error="session revoked" if revoked else None,
            failure_code="account_dead" if revoked else None,
        )

    monkeypatch.setattr(service, "check_account", fake_check)

    summary = await service.check_all_sessions()

    assert [entry.label for entry in summary.entries] == [
        "ready-session",
        "revoked-session",
        "broken-import",
    ]
    assert [entry.label for entry in summary.ready] == ["ready-session"]
    assert [entry.label for entry in summary.session_revoked] == ["revoked-session"]
    assert [entry.label for entry in summary.issues] == ["broken-import"]


async def test_bulk_session_check_never_marks_unknown_authorization_for_deletion(
    service: AccountService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed proxy must not make an unverified bought session look revoked."""
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        account = Account(
            label="network-unknown",
            ownership_confirmed=True,
            session_source=AccountSessionSource.SESSION_STRING,
            authorized=False,
        )
        session.add(account)
        await session.commit()

    async def failed_route(account_id: int, *, actor: str = "control-bot") -> AccountCheckResult:
        del actor
        return AccountCheckResult(
            account_id=account_id,
            authorized=False,
            telegram_user_id=None,
            phone=None,
            username=None,
            error="proxy unavailable",
            failure_code="proxy_unavailable",
        )

    monkeypatch.setattr(service, "check_account", failed_route)

    summary = await service.check_all_sessions()

    assert [entry.label for entry in summary.session_revoked] == []
    assert [entry.label for entry in summary.issues] == ["network-unknown"]


async def test_bulk_session_check_runs_a_bounded_parallel_batch(
    service: AccountService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with service._session_factory() as session:  # type: ignore[attr-defined]
        accounts = [
            Account(
                label=f"parallel-{index}",
                ownership_confirmed=True,
                session_source=AccountSessionSource.SESSION_STRING,
            )
            for index in range(6)
        ]
        session.add_all(accounts)
        await session.commit()

    active = 0
    peak = 0

    async def fake_check(account_id: int, *, actor: str = "control-bot") -> AccountCheckResult:
        nonlocal active, peak
        del actor
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return AccountCheckResult(
            account_id=account_id,
            authorized=True,
            telegram_user_id=None,
            phone=None,
            username=None,
            error=None,
        )

    monkeypatch.setattr(service, "check_account", fake_check)

    summary = await service.check_all_sessions(concurrency=3)

    assert [entry.label for entry in summary.entries] == [f"parallel-{index}" for index in range(6)]
    assert peak == 3
