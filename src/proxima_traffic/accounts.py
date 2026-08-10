"""Import and manage personal Telegram accounts (tdata / .session / JSON).

This module turns Telegram Desktop ``tdata`` folders (uploaded as a ``.zip``)
and Telethon/Pyrogram session exports into a portable, **encrypted**
``StringSession`` stored on the account row. The decrypted session never leaves
the process and is only ever handed to the live gateway for sending.

The heavy Telegram stack (``opentele2`` / ``telethon``) is imported lazily and
always after registering the ``tgcrypto`` compatibility shim, so importing this
module stays cheap and side-effect free for tests that do not touch accounts.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import sqlite3
import stat
import struct
import tempfile
import zipfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Protocol

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    ProxyAssignment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.profile_names import ProfileName
from proxima_traffic.proxies import ProxyNetworkRoute, ProxyRouteUnavailable, ProxyService
from proxima_traffic.services import ConflictError, DomainError, NotFoundError, add_audit_event

logger = logging.getLogger(__name__)


MAX_ARCHIVE_BYTES = 25 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 80 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 4000
MAX_PROFILE_PHOTO_BYTES = 10 * 1024 * 1024
MAX_SESSION_FILE_BYTES = 5 * 1024 * 1024
MAX_JSON_SESSION_BYTES = 5 * 1024 * 1024
MAX_BATCH_IMPORT_ACCOUNTS = 100
MAX_BATCH_ARCHIVE_MEMBERS = 500
TDATA_MARKER = "key_datas"

# Telegram production DCs used when a Pyrogram/JSON export contains a raw
# auth_key but no server address. The resulting source session is used only to
# approve a fresh, separately encrypted service session.
PRODUCTION_DC_ADDRESSES = {
    1: "149.154.175.50",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "149.154.171.5",
}


class AccountImportError(DomainError):
    """Raised when an uploaded tdata/session cannot be turned into a session."""


class AccountOperationError(DomainError):
    """Base error for an authenticated Telegram account operation."""


class AccountProfileError(AccountOperationError):
    """Raised when Telegram rejects or cannot apply a profile change."""


class ChatFolderError(AccountOperationError):
    """Raised when a shared Telegram chat folder cannot be inspected."""


class AccountClientCoordinator(Protocol):
    """Owns the single connected Telegram client and lock for each account."""

    async def run_account_operation(
        self,
        account_id: int,
        operation: Callable[[object], Awaitable[object]],
    ) -> tuple[object, object]: ...


@dataclass(frozen=True, slots=True)
class DecryptedSession:
    account_id: int
    session_string: str
    api_id: int
    api_hash: str


@dataclass(frozen=True, slots=True)
class AccountCheckResult:
    account_id: int
    authorized: bool
    telegram_user_id: int | None
    phone: str | None
    username: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class AccountArchiveEntry:
    """One logical account found in a seller-provided sessions ZIP.

    Market exports commonly place ``<id>.session`` and ``<id>.json`` next to
    each other.  The session is the primary credential; the companion JSON may
    provide a fallback authorization or the account-specific 2FA password.
    Raw credentials are deliberately excluded from ``repr`` so an exception or
    debug log cannot disclose them accidentally.
    """

    source_name: str
    session_bytes: bytes | None = field(default=None, repr=False)
    json_bytes: bytes | None = field(default=None, repr=False)

    @property
    def preferred_file_name(self) -> str:
        suffix = ".session" if self.session_bytes is not None else ".json"
        return f"{self.source_name}{suffix}"


@dataclass(frozen=True, slots=True)
class ProfileNameFailure:
    account_id: int
    error: str


@dataclass(frozen=True, slots=True)
class BulkProfileNameResult:
    updated: tuple[Account, ...]
    failures: tuple[ProfileNameFailure, ...]

    @property
    def total(self) -> int:
        return len(self.updated) + len(self.failures)


@dataclass(frozen=True, slots=True)
class ChatFolderEntry:
    peer_id: int
    title: str
    username: str | None
    chat_type: str
    accessible: bool
    already_joined: bool
    can_send_hint: bool

    @property
    def chat_ref(self) -> str:
        return f"@{self.username.lower()}" if self.username else str(self.peer_id)


@dataclass(frozen=True, slots=True)
class ChatFolderPreview:
    slug: str
    title: str
    entries: tuple[ChatFolderEntry, ...]

    @property
    def importable_groups(self) -> tuple[ChatFolderEntry, ...]:
        return tuple(
            entry
            for entry in self.entries
            if entry.accessible
            and entry.can_send_hint
            and entry.chat_type in {"group", "supergroup"}
        )

    @property
    def skipped_channels(self) -> int:
        return sum(entry.chat_type == "channel" for entry in self.entries)

    @property
    def inaccessible(self) -> int:
        return sum(not entry.accessible for entry in self.entries)

    @property
    def restricted_groups(self) -> int:
        return sum(
            entry.chat_type in {"group", "supergroup"}
            and entry.accessible
            and not entry.can_send_hint
            for entry in self.entries
        )


@dataclass(frozen=True, slots=True)
class _ConvertedSession:
    session_string: str
    api_id: int
    api_hash: str
    telegram_user_id: int | None
    first_name: str | None = None
    last_name: str | None = None
    phone: str | None = None
    username: str | None = None
    has_profile_photo: bool = False


class AccountSecretCipher:
    """Fernet cipher for account sessions (same scheme as the proxy cipher)."""

    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("account encryption key cannot be empty")
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
        self._fernet = Fernet(key)

    @classmethod
    def from_settings(cls, settings: Settings) -> AccountSecretCipher:
        return cls(settings.account_encryption_secret)

    def encrypt(self, value: str | None) -> str | None:
        if value is None:
            return None
        return self._fernet.encrypt(value.encode()).decode()

    def decrypt(self, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except InvalidToken as exc:
            raise AccountImportError(
                "не удалось расшифровать сессию; проверьте PROXIMA_ACCOUNT_ENCRYPTION_KEY"
            ) from exc


class AccountService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        proxy_service: ProxyService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._proxy_service = proxy_service
        self._cipher = AccountSecretCipher.from_settings(settings)
        self._client_coordinator: AccountClientCoordinator | None = None

    def bind_client_coordinator(self, coordinator: AccountClientCoordinator) -> None:
        """Route checks/profile/folder operations through the live gateway client pool."""
        self._client_coordinator = coordinator

    # ------------------------------------------------------------------ import
    async def import_tdata(
        self,
        *,
        label: str | None = None,
        archive_bytes: bytes,
        proxy_route: ProxyNetworkRoute | None = None,
        two_factor_password: str | None = None,
    ) -> Account:
        normalized_label = _validate_label(label) if label is not None else None
        api_id, api_hash = self._require_api()
        proxy = self._import_proxy(proxy_route)
        two_factor_password = self._configured_two_factor_password(two_factor_password)
        converted = await asyncio.to_thread(
            self._convert_tdata_archive,
            archive_bytes,
            api_id,
            api_hash,
            proxy,
            two_factor_password,
        )
        return await self._store_session(
            label=normalized_label,
            source=AccountSessionSource.TDATA,
            converted=converted,
        )

    async def import_session_file(
        self,
        *,
        label: str | None = None,
        session_bytes: bytes,
        proxy_route: ProxyNetworkRoute | None = None,
        two_factor_password: str | None = None,
    ) -> Account:
        normalized_label = _validate_label(label) if label is not None else None
        api_id, api_hash = self._require_api()
        proxy = self._import_proxy(proxy_route)
        source_session = await asyncio.to_thread(
            self._read_session_file,
            session_bytes,
        )
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
        )
        return await self._store_session(
            label=normalized_label,
            source=AccountSessionSource.SESSION_FILE,
            converted=converted,
        )

    async def import_session_string(
        self,
        *,
        label: str | None = None,
        session_string: str,
        proxy_route: ProxyNetworkRoute | None = None,
        two_factor_password: str | None = None,
    ) -> Account:
        normalized_label = _validate_label(label) if label is not None else None
        api_id, api_hash = self._require_api()
        proxy = self._import_proxy(proxy_route)
        source_session = self._read_any_session_string(session_string)
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
        )
        return await self._store_session(
            label=normalized_label,
            source=AccountSessionSource.SESSION_STRING,
            converted=converted,
        )

    async def import_json(
        self,
        *,
        label: str | None = None,
        json_bytes: bytes,
        proxy_route: ProxyNetworkRoute | None = None,
        two_factor_password: str | None = None,
    ) -> Account:
        """Import an authorization-bearing JSON export.

        Seller JSON formats are not standardized, so the parser accepts a
        Telethon/Pyrogram session string or a raw ``dc_id`` + ``auth_key``
        object. Metadata-only JSON is rejected instead of pretending that it
        can authorize an account.
        """
        normalized_label = _validate_label(label) if label is not None else None
        api_id, api_hash = self._require_api()
        proxy = self._import_proxy(proxy_route)
        source_session = await asyncio.to_thread(self._read_json_session, json_bytes)
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
        )
        return await self._store_session(
            label=normalized_label,
            source=AccountSessionSource.SESSION_STRING,
            converted=converted,
        )

    @staticmethod
    def read_account_archive(archive_bytes: bytes) -> tuple[AccountArchiveEntry, ...]:
        """Read a ZIP containing one or more ``.session``/``.json`` accounts.

        Parsing is intentionally offline and does not extract files to disk.
        The returned entries pair files with the same relative stem and prefer
        the SQLite session while retaining JSON as a fallback/2FA companion.
        """

        return _read_account_archive(archive_bytes)

    async def import_archive_entry(
        self,
        entry: AccountArchiveEntry,
        *,
        proxy_route: ProxyNetworkRoute | None = None,
    ) -> Account:
        """Import one parsed archive entry, preferring its ``.session`` file."""

        two_factor_password = _two_factor_password_from_json(entry.json_bytes)
        session_error: AccountImportError | None = None
        if entry.session_bytes is not None:
            try:
                return await self.import_session_file(
                    session_bytes=entry.session_bytes,
                    proxy_route=proxy_route,
                    two_factor_password=two_factor_password,
                )
            except AccountImportError as exc:
                session_error = exc
                if entry.json_bytes is None:
                    raise

        if entry.json_bytes is not None:
            try:
                return await self.import_json(
                    json_bytes=entry.json_bytes,
                    proxy_route=proxy_route,
                    two_factor_password=two_factor_password,
                )
            except AccountImportError as json_error:
                if session_error is None:
                    raise
                raise AccountImportError(
                    f".session: {session_error}; резервный .json: {json_error}"
                ) from json_error

        raise AccountImportError("в записи архива нет .session или .json")

    # ---------------------------------------------------------------- lifecycle
    async def get_account(self, account_id: int) -> Account:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            return account

    async def delete_account(self, account_id: int, *, actor: str = "control-bot") -> None:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            add_audit_event(
                session,
                event_type="account.deleted",
                entity_type="account",
                entity_id=account_id,
                details={"label": account.label},
                actor=actor,
            )
            assignment = await session.scalar(
                select(ProxyAssignment).where(
                    ProxyAssignment.assignment_key == f"account:{account_id}"
                )
            )
            if assignment is not None:
                await session.delete(assignment)
            await session.delete(account)
            try:
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError(
                    "аккаунт связан с сохранённым сценарием или историей запусков. "
                    "Сотрите сессию и отключите доступ, а запись оставьте для журнала"
                ) from exc

    async def clear_session(self, account_id: int, *, actor: str = "control-bot") -> Account:
        """Revoke stored Telegram access while keeping the historical account row."""
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            account.status = AccountStatus.DISABLED
            account.session_source = AccountSessionSource.NONE
            account.session_encrypted = None
            account.api_id = None
            account.api_hash_encrypted = None
            account.first_name = None
            account.last_name = None
            account.phone = None
            account.username = None
            account.has_profile_photo = False
            account.authorized = False
            account.last_checked_at = None
            account.last_error = None
            account.next_send_at = None
            account.cooldown_until = None
            account.cooldown_reason = None
            assignment = await session.scalar(
                select(ProxyAssignment).where(
                    ProxyAssignment.assignment_key == f"account:{account_id}"
                )
            )
            if assignment is not None:
                await session.delete(assignment)
            add_audit_event(
                session,
                event_type="account.session_cleared",
                entity_type="account",
                entity_id=account.id,
                details={"label": account.label},
                actor=actor,
            )
            await session.commit()
            await session.refresh(account)
            return account

    async def join_wait_seconds(
        self, account_id: int, *, daily_limit: int, min_interval: float
    ) -> float:
        """Return seconds to wait before the account may join another chat (0 = ok)."""
        if daily_limit <= 0 and min_interval <= 0:
            return 0.0
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return 0.0
            now = utc_now()
            window_start = ensure_utc(account.join_window_start)
            joins_today = account.joins_today
            if window_start is None or (now - window_start).total_seconds() >= 86400:
                joins_today = 0
            if daily_limit > 0 and joins_today >= daily_limit and window_start is not None:
                remaining = 86400 - (now - window_start).total_seconds()
                if remaining > 0:
                    return remaining
            last_join = ensure_utc(account.last_join_at)
            if min_interval > 0 and last_join is not None:
                elapsed = (now - last_join).total_seconds()
                if elapsed < min_interval:
                    return min_interval - elapsed
            return 0.0

    async def record_join(self, account_id: int) -> None:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return
            now = utc_now()
            window_start = ensure_utc(account.join_window_start)
            if window_start is None or (now - window_start).total_seconds() >= 86400:
                account.join_window_start = now
                account.joins_today = 0
            account.joins_today += 1
            account.last_join_at = now
            await session.commit()

    async def load_session(self, account_id: int) -> DecryptedSession:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            if account.session_source == AccountSessionSource.NONE or not account.session_encrypted:
                raise AccountImportError(f"у аккаунта #{account_id} нет загруженной сессии")
            if account.api_id is None or not account.api_hash_encrypted:
                raise AccountImportError(f"у аккаунта #{account_id} отсутствуют API-креды")
            session_string = self._cipher.decrypt(account.session_encrypted)
            api_hash = self._cipher.decrypt(account.api_hash_encrypted)
            assert session_string is not None and api_hash is not None
            return DecryptedSession(
                account_id=account_id,
                session_string=session_string,
                api_id=account.api_id,
                api_hash=api_hash,
            )

    async def check_account(
        self, account_id: int, *, actor: str = "control-bot"
    ) -> AccountCheckResult:
        """Connect through the assigned proxy and confirm the session is alive."""
        decrypted = await self.load_session(account_id)
        proxy = None
        if self._proxy_service is not None:
            try:
                route = await self._proxy_service.require_account_route(account_id)
                proxy = _telethon_proxy(route)
            except ProxyRouteUnavailable:
                if self._settings.account_require_proxy:
                    return await self._record_check(
                        account_id,
                        authorized=None,
                        me=None,
                        error="прокси для аккаунта не назначен (PROXIMA_ACCOUNT_REQUIRE_PROXY=1)",
                        failure_code="proxy_unavailable",
                        actor=actor,
                    )
        try:
            me = await self._fetch_me(decrypted, proxy)
        except Exception as exc:  # noqa: BLE001 - report any connection failure safely
            failure_code = _account_check_failure_code(exc)
            return await self._record_check(
                account_id,
                authorized=False if failure_code == "account_dead" else None,
                me=None,
                error=_safe_account_error(exc),
                failure_code=failure_code,
                actor=actor,
            )
        return await self._record_check(
            account_id,
            authorized=me is not None,
            me=me,
            error=None,
            failure_code=None if me is not None else "account_unauthorized",
            actor=actor,
        )

    async def update_profile_name(
        self,
        account_id: int,
        *,
        first_name: str,
        last_name: str = "",
        actor: str = "control-bot",
    ) -> Account:
        normalized_first = _validate_telegram_name(first_name, required=True)
        normalized_last = _validate_telegram_name(last_name, required=False)

        async def update(client: object) -> None:
            from telethon.tl.functions.account import (  # noqa: PLC0415
                UpdateProfileRequest,
            )

            await client(  # type: ignore[operator]
                UpdateProfileRequest(
                    first_name=normalized_first,
                    last_name=normalized_last,
                )
            )

        me, _ = await self._run_profile_operation(account_id, update)
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_name_updated",
            details={"first_name": normalized_first, "last_name": normalized_last},
            actor=actor,
        )

    async def rename_account_label(
        self,
        account_id: int,
        *,
        label: str,
        actor: str = "control-bot",
    ) -> Account:
        """Rename only the private label shown by the control bot."""
        normalized_label = _validate_label(label)
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            old_label = account.label
            account.label = normalized_label
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("такое название в боте уже используется") from exc
            add_audit_event(
                session,
                event_type="account.label_updated",
                entity_type="account",
                entity_id=account_id,
                details={"old_label": old_label, "new_label": normalized_label},
                actor=actor,
            )
            await session.commit()
            await session.refresh(account)
            return account

    async def update_profile_about(
        self,
        account_id: int,
        *,
        about: str,
        actor: str = "control-bot",
    ) -> Account:
        normalized_about = _validate_telegram_about(about)

        async def update(client: object) -> None:
            from telethon.tl.functions.account import (  # noqa: PLC0415
                UpdateProfileRequest,
            )

            await client(UpdateProfileRequest(about=normalized_about))  # type: ignore[operator]

        me, _ = await self._run_profile_operation(account_id, update)
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_about_updated",
            details={"has_about": bool(normalized_about), "length": len(normalized_about)},
            actor=actor,
            about_override=normalized_about,
        )

    async def bulk_update_profile_about(
        self,
        account_ids: list[int],
        *,
        about: str,
        concurrency: int = 3,
        actor: str = "control-bot",
    ) -> BulkProfileNameResult:
        normalized_ids = list(dict.fromkeys(account_ids))
        _validate_telegram_about(about)
        if not normalized_ids:
            return BulkProfileNameResult(updated=(), failures=())
        if len(normalized_ids) > 200:
            raise AccountProfileError("за одну операцию можно изменить не более 200 аккаунтов")
        semaphore = asyncio.Semaphore(max(1, min(concurrency, 5)))

        async def apply_about(account_id: int) -> Account | ProfileNameFailure:
            async with semaphore:
                try:
                    return await self.update_profile_about(account_id, about=about, actor=actor)
                except (NotFoundError, AccountImportError, AccountProfileError) as exc:
                    return ProfileNameFailure(account_id=account_id, error=str(exc))

        results = await asyncio.gather(*(apply_about(account_id) for account_id in normalized_ids))
        updated = tuple(item for item in results if isinstance(item, Account))
        failures = tuple(item for item in results if isinstance(item, ProfileNameFailure))
        async with self._session_factory() as session:
            add_audit_event(
                session,
                event_type="account.profile_about_bulk_updated",
                entity_type="account_batch",
                entity_id=None,
                details={
                    "requested": len(normalized_ids),
                    "updated": len(updated),
                    "failed": len(failures),
                    "has_about": bool(about),
                },
                actor=actor,
            )
            await session.commit()
        return BulkProfileNameResult(updated=updated, failures=failures)

    async def bulk_update_profile_names(
        self,
        assignments: dict[int, ProfileName],
        *,
        concurrency: int = 3,
        actor: str = "control-bot",
    ) -> BulkProfileNameResult:
        if not assignments:
            return BulkProfileNameResult(updated=(), failures=())
        if len(assignments) > 200:
            raise AccountProfileError("за одну операцию можно изменить не более 200 аккаунтов")

        semaphore = asyncio.Semaphore(max(1, min(concurrency, 5)))

        async def apply_name(
            account_id: int,
            profile_name: ProfileName,
        ) -> Account | ProfileNameFailure:
            async with semaphore:
                try:
                    return await self.update_profile_name(
                        account_id,
                        first_name=profile_name.first_name,
                        last_name=profile_name.last_name,
                        actor=actor,
                    )
                except (NotFoundError, AccountImportError, AccountProfileError) as exc:
                    return ProfileNameFailure(account_id=account_id, error=str(exc))

        results = await asyncio.gather(
            *(
                apply_name(account_id, profile_name)
                for account_id, profile_name in assignments.items()
            )
        )
        updated = tuple(item for item in results if isinstance(item, Account))
        failures = tuple(item for item in results if isinstance(item, ProfileNameFailure))
        async with self._session_factory() as session:
            add_audit_event(
                session,
                event_type="account.profile_names_bulk_updated",
                entity_type="account_batch",
                entity_id=None,
                details={
                    "requested": len(assignments),
                    "updated": len(updated),
                    "failed": len(failures),
                    "updated_account_ids": [account.id for account in updated],
                    "failed_account_ids": [failure.account_id for failure in failures],
                },
                actor=actor,
            )
            await session.commit()
        return BulkProfileNameResult(updated=updated, failures=failures)

    async def update_profile_photo(
        self,
        account_id: int,
        *,
        photo_bytes: bytes,
        file_name: str,
        actor: str = "control-bot",
    ) -> Account:
        if not photo_bytes:
            raise AccountProfileError("файл аватара пуст")
        if len(photo_bytes) > MAX_PROFILE_PHOTO_BYTES:
            raise AccountProfileError("аватар больше 10 МБ")
        safe_name = _validate_profile_photo_name(file_name)

        async def update(client: object) -> None:
            from telethon.tl.functions.photos import (  # noqa: PLC0415
                UploadProfilePhotoRequest,
            )

            uploaded = await client.upload_file(  # type: ignore[attr-defined]
                photo_bytes,
                file_name=safe_name,
            )
            await client(UploadProfilePhotoRequest(file=uploaded))  # type: ignore[operator]

        me, _ = await self._run_profile_operation(account_id, update)
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_photo_updated",
            details={"file_name": safe_name, "size": len(photo_bytes)},
            actor=actor,
            photo_override=True,
        )

    async def delete_profile_photos(
        self,
        account_id: int,
        *,
        actor: str = "control-bot",
    ) -> tuple[Account, int]:
        async def delete(client: object) -> int:
            from telethon import utils  # noqa: PLC0415
            from telethon.tl.functions.photos import DeletePhotosRequest  # noqa: PLC0415

            photos = list(await client.get_profile_photos("me"))  # type: ignore[attr-defined]
            deleted = 0
            for offset in range(0, len(photos), 100):
                photo_ids = [
                    utils.get_input_photo(photo) for photo in photos[offset : offset + 100]
                ]
                if photo_ids:
                    await client(DeletePhotosRequest(id=photo_ids))  # type: ignore[operator]
                    deleted += len(photo_ids)
            return deleted

        me, deleted = await self._run_profile_operation(account_id, delete)
        account = await self._record_profile(
            account_id,
            me,
            event_type="account.profile_photos_deleted",
            details={"count": int(deleted)},
            actor=actor,
            photo_override=False,
        )
        return account, int(deleted)

    async def inspect_chat_folder(
        self,
        account_id: int,
        *,
        slug: str,
    ) -> ChatFolderPreview:
        normalized_slug = slug.strip()
        if not normalized_slug or len(normalized_slug) > 128:
            raise ChatFolderError("некорректная ссылка на папку Telegram")

        async def inspect_folder(client: object) -> object:
            from telethon.tl.functions.chatlists import (  # noqa: PLC0415
                CheckChatlistInviteRequest,
            )

            return await client(  # type: ignore[operator]
                CheckChatlistInviteRequest(slug=normalized_slug)
            )

        try:
            _, result = await self._run_account_operation(account_id, inspect_folder)
        except AccountOperationError as exc:
            raise ChatFolderError(str(exc)) from exc
        return _chat_folder_preview(normalized_slug, result)

    # ------------------------------------------------------------------ helpers
    async def _store_session(
        self,
        *,
        label: str | None,
        source: AccountSessionSource,
        converted: _ConvertedSession,
    ) -> Account:
        encrypted_session = self._cipher.encrypt(converted.session_string)
        encrypted_hash = self._cipher.encrypt(converted.api_hash)
        async with self._session_factory() as session:
            if converted.telegram_user_id is not None:
                existing = await session.scalar(
                    select(Account).where(Account.telegram_user_id == converted.telegram_user_id)
                )
                if existing is not None:
                    raise ConflictError(f"этот аккаунт уже загружен как «{existing.label}»")
            resolved_label = label or await self._available_import_label(session, converted)
            account = Account(
                label=resolved_label,
                telegram_user_id=converted.telegram_user_id,
                ownership_confirmed=True,
                status=AccountStatus.ACTIVE,
                session_source=source,
                session_encrypted=encrypted_session,
                api_id=converted.api_id,
                api_hash_encrypted=encrypted_hash,
                first_name=_limited_optional(converted.first_name, 64),
                last_name=_limited_optional(converted.last_name, 64),
                phone=_limited_optional(converted.phone, 32),
                username=_limited_optional(converted.username, 64),
                has_profile_photo=converted.has_profile_photo,
                authorized=True,
                last_checked_at=utc_now(),
            )
            session.add(account)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("название или Telegram ID аккаунта уже заняты") from exc
            add_audit_event(
                session,
                event_type="account.session_imported",
                entity_type="account",
                entity_id=account.id,
                details={"label": resolved_label, "source": source.value},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(account)
            return account

    @staticmethod
    async def _available_import_label(
        session: AsyncSession,
        converted: _ConvertedSession,
    ) -> str:
        full_name = " ".join(
            value.strip()
            for value in (converted.first_name or "", converted.last_name or "")
            if value.strip()
        )
        base = full_name
        if not base and converted.username:
            base = f"@{converted.username.strip().lstrip('@')}"
        if not base:
            identifier = converted.telegram_user_id or "без ID"
            base = f"Аккаунт {identifier}"
        base = " ".join(base.split())[:100].strip() or "Telegram-аккаунт"
        labels = set((await session.scalars(select(Account.label))).all())
        if base not in labels:
            return base
        for number in range(2, 10_000):
            suffix = f" ({number})"
            candidate = f"{base[: 100 - len(suffix)].rstrip()}{suffix}"
            if candidate not in labels:
                return candidate
        raise ConflictError("не удалось подобрать свободное название аккаунта")

    async def _record_check(
        self,
        account_id: int,
        *,
        authorized: bool | None,
        me: object | None,
        error: str | None,
        failure_code: str | None = None,
        actor: str,
    ) -> AccountCheckResult:
        telegram_user_id = getattr(me, "id", None)
        first_name = getattr(me, "first_name", None)
        last_name = getattr(me, "last_name", None)
        phone = getattr(me, "phone", None)
        username = getattr(me, "username", None)
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            if authorized is not None:
                account.authorized = authorized
            account.last_checked_at = utc_now()
            account.last_error = None if authorized is True else (error or "не авторизован")[:500]
            if authorized is False and failure_code in {"account_dead", "account_unauthorized"}:
                account.status = AccountStatus.DISABLED
                account.cooldown_reason = failure_code
            if authorized is True:
                if telegram_user_id is not None and account.telegram_user_id is None:
                    account.telegram_user_id = telegram_user_id
                if phone:
                    account.phone = str(phone)[:32]
                account.first_name = str(first_name)[:64] if first_name else None
                account.last_name = str(last_name)[:64] if last_name else None
                account.username = str(username)[:64] if username else None
                account.has_profile_photo = getattr(me, "photo", None) is not None
            add_audit_event(
                session,
                event_type="account.check_succeeded" if authorized else "account.check_failed",
                entity_type="account",
                entity_id=account_id,
                details={
                    "authorized": authorized,
                    "authorization_known": authorized is not None,
                    "failure_code": failure_code,
                },
                actor=actor,
            )
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                raise ConflictError(
                    "этот Telegram аккаунт уже загружен под другой меткой"
                ) from None
            effective_authorized = bool(account.authorized)
        return AccountCheckResult(
            account_id=account_id,
            authorized=effective_authorized,
            telegram_user_id=telegram_user_id,
            phone=str(phone) if phone else None,
            username=str(username) if username else None,
            error=None if authorized else error,
        )

    async def _record_profile(
        self,
        account_id: int,
        me: object,
        *,
        event_type: str,
        details: dict[str, object],
        actor: str,
        photo_override: bool | None = None,
        about_override: str | None = None,
    ) -> Account:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            account.first_name = _optional_profile_value(getattr(me, "first_name", None))
            account.last_name = _optional_profile_value(getattr(me, "last_name", None))
            account.username = _optional_profile_value(getattr(me, "username", None))
            if about_override is not None:
                account.about = about_override
            phone = getattr(me, "phone", None)
            account.phone = str(phone)[:32] if phone else account.phone
            account.authorized = True
            account.last_checked_at = utc_now()
            account.last_error = None
            account.has_profile_photo = (
                photo_override
                if photo_override is not None
                else getattr(me, "photo", None) is not None
            )
            add_audit_event(
                session,
                event_type=event_type,
                entity_type="account",
                entity_id=account_id,
                details=details,
                actor=actor,
            )
            await session.commit()
            await session.refresh(account)
            return account

    async def _run_profile_operation(
        self,
        account_id: int,
        operation: Callable[[object], Awaitable[object]],
    ) -> tuple[object, object]:
        try:
            return await self._run_account_operation(account_id, operation)
        except AccountOperationError as exc:
            raise AccountProfileError(str(exc)) from exc

    async def _run_account_operation(
        self,
        account_id: int,
        operation: Callable[[object], Awaitable[object]],
    ) -> tuple[object, object]:
        if self._client_coordinator is not None:
            try:
                return await self._client_coordinator.run_account_operation(
                    account_id,
                    operation,
                )
            except AccountOperationError:
                raise
            except Exception as exc:
                raise AccountOperationError(_safe_account_operation_error(exc)) from exc

        from telethon import TelegramClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        decrypted = await self.load_session(account_id)
        proxy = None
        if self._proxy_service is not None:
            try:
                route = await self._proxy_service.require_account_route(account_id)
                proxy = _telethon_proxy(route)
            except ProxyRouteUnavailable as exc:
                if self._settings.account_require_proxy:
                    raise AccountOperationError(
                        "для операции нужен назначенный рабочий прокси"
                    ) from exc
        elif self._settings.account_require_proxy:
            raise AccountOperationError("сервис прокси не настроен")

        client = TelegramClient(
            StringSession(decrypted.session_string),
            decrypted.api_id,
            decrypted.api_hash,
            proxy=proxy,
            connection_retries=1,
            timeout=int(self._settings.account_connect_timeout_seconds),
            flood_sleep_threshold=self._settings.account_flood_sleep_threshold,
        )
        try:
            await asyncio.wait_for(
                client.connect(), timeout=self._settings.account_connect_timeout_seconds
            )
            if not await client.is_user_authorized():
                raise AccountOperationError("сессия аккаунта недействительна")
            result = await operation(client)
            me = await client.get_me()
            if me is None:
                raise AccountOperationError("Telegram не вернул профиль аккаунта")
            return me, result
        except AccountOperationError:
            raise
        except Exception as exc:
            raise AccountOperationError(_safe_account_operation_error(exc)) from exc
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                logger.debug("profile client disconnect failed", exc_info=True)

    async def _fetch_me(self, decrypted: DecryptedSession, proxy: object | None) -> object | None:
        if self._client_coordinator is not None:

            async def no_operation(_client: object) -> None:
                return None

            me, _ = await self._client_coordinator.run_account_operation(
                decrypted.account_id,
                no_operation,
            )
            return me

        from telethon import TelegramClient  # noqa: PLC0415 - lazy heavy import
        from telethon.sessions import StringSession  # noqa: PLC0415

        client = TelegramClient(
            StringSession(decrypted.session_string),
            decrypted.api_id,
            decrypted.api_hash,
            proxy=proxy,
            connection_retries=1,
            timeout=int(self._settings.account_connect_timeout_seconds),
        )
        try:
            await asyncio.wait_for(
                client.connect(), timeout=self._settings.account_connect_timeout_seconds
            )
            if not await client.is_user_authorized():
                return None
            return await client.get_me()
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                logger.debug("account %s disconnect failed", decrypted.account_id, exc_info=True)

    def _require_api(self) -> tuple[int, str]:
        api = self._settings.telegram_api
        if api is None:
            raise AccountImportError(
                "для импорта аккаунта нужны PROXIMA_TELEGRAM_API_ID и PROXIMA_TELEGRAM_API_HASH"
            )
        return api

    def _configured_two_factor_password(self, supplied: str | None) -> str | None:
        if supplied is not None:
            return supplied.strip() or None
        configured = self._settings.tdata_two_factor_password
        if configured is None:
            return None
        return configured.get_secret_value().strip() or None

    def _import_proxy(self, route: ProxyNetworkRoute | None) -> object | None:
        if route is None:
            if self._settings.account_require_proxy:
                raise AccountImportError("для импорта аккаунта нужен назначенный рабочий прокси")
            return None
        return _telethon_proxy(route)

    # ----------------------------------------------------- blocking converters
    def _convert_tdata_archive(
        self,
        archive_bytes: bytes,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
    ) -> _ConvertedSession:
        if len(archive_bytes) > MAX_ARCHIVE_BYTES:
            raise AccountImportError("архив tdata больше 25 МБ")
        with tempfile.TemporaryDirectory(prefix="proxima-tdata-") as tmp:
            tmp_path = Path(tmp)
            extract_root = tmp_path / "extracted"
            extract_root.mkdir()
            _safe_extract_zip(archive_bytes, extract_root)
            tdata_path = _locate_tdata_dir(extract_root)
            return asyncio.run(
                self._convert_tdata_path(
                    tdata_path,
                    api_id=api_id,
                    api_hash=api_hash,
                    proxy=proxy,
                    two_factor_password=two_factor_password,
                )
            )

    async def _convert_tdata_path(
        self,
        tdata_path: Path,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
    ) -> _ConvertedSession:
        from opentele2.api import UseCurrentSession  # noqa: PLC0415
        from opentele2.td import TDesktop  # noqa: PLC0415

        tdesk = TDesktop(str(tdata_path))
        if not tdesk.isLoaded() or tdesk.accountsCount < 1:
            raise AccountImportError("в архиве не найден авторизованный аккаунт tdata")
        try:
            # The tdata key is used only once to approve a fresh authorization.
            # It is never persisted as the service session.
            source_client = await tdesk.ToTelethon(
                session=None,
                flag=UseCurrentSession,
                proxy=proxy,
                connection_retries=1,
                timeout=int(self._settings.account_connect_timeout_seconds),
            )
        except AccountImportError:
            raise
        except Exception as exc:
            raise AccountImportError(f"не удалось прочитать tdata: {type(exc).__name__}") from exc
        return await self._authorize_new_server_session(
            source_client,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            two_factor_password=two_factor_password,
            source_name="Telegram Desktop",
        )

    async def _convert_existing_session(
        self,
        source_session: str,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
    ) -> _ConvertedSession:
        from opentele2.api import APIData  # noqa: PLC0415
        from opentele2.tl import TelegramClient as OpenTelethonClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        try:
            source_client = OpenTelethonClient(
                StringSession(source_session),
                api=APIData(api_id=api_id, api_hash=api_hash),
                proxy=proxy,
                connection_retries=1,
                timeout=int(self._settings.account_connect_timeout_seconds),
            )
        except Exception as exc:
            raise AccountImportError(
                f"не удалось открыть исходную сессию: {type(exc).__name__}"
            ) from exc
        return await self._authorize_new_server_session(
            source_client,
            api_id=api_id,
            api_hash=api_hash,
            proxy=proxy,
            two_factor_password=two_factor_password,
            source_name="исходная Telethon-сессия",
        )

    async def _authorize_new_server_session(
        self,
        source_client: object,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
        source_name: str,
    ) -> _ConvertedSession:
        from opentele2.api import APIData  # noqa: PLC0415
        from opentele2.exception import NoPasswordProvided, PasswordIncorrect  # noqa: PLC0415
        from telethon import errors  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        server_client = None
        try:
            await asyncio.wait_for(
                source_client.connect(),  # type: ignore[attr-defined]
                timeout=self._settings.account_connect_timeout_seconds,
            )
            if not await source_client.is_user_authorized():  # type: ignore[attr-defined]
                raise AccountImportError(f"{source_name} больше не авторизована")

            server_session = StringSession()
            login_timeout = max(60.0, self._settings.account_connect_timeout_seconds * 2)
            server_client = await asyncio.wait_for(
                source_client.QRLoginToNewClient(  # type: ignore[attr-defined]
                    session=server_session,
                    api=APIData(api_id=api_id, api_hash=api_hash),
                    password=two_factor_password,
                    proxy=proxy,
                    connection_retries=1,
                    request_retries=2,
                    timeout=int(self._settings.account_connect_timeout_seconds),
                ),
                timeout=login_timeout,
            )
            if not await server_client.is_user_authorized():
                raise AccountImportError("Telegram не подтвердил отдельную серверную сессию")
            source_auth_key = getattr(getattr(source_client, "session", None), "auth_key", None)
            server_auth_key = getattr(server_client.session, "auth_key", None)
            if server_auth_key is None:
                raise AccountImportError("новая серверная сессия не содержит auth_key")
            if source_auth_key is not None and source_auth_key.key == server_auth_key.key:
                raise AccountImportError(
                    "Telegram не создал отдельный auth_key; импорт отменён без сохранения"
                )
            me = await server_client.get_me()
            if me is None or getattr(me, "id", None) is None:
                raise AccountImportError("Telegram не вернул профиль импортированного аккаунта")
            session_string = server_client.session.save()
            if not session_string:
                raise AccountImportError("не удалось сохранить отдельную серверную сессию")
            return _ConvertedSession(
                session_string=session_string,
                api_id=api_id,
                api_hash=api_hash,
                telegram_user_id=int(me.id),
                first_name=getattr(me, "first_name", None),
                last_name=getattr(me, "last_name", None),
                phone=getattr(me, "phone", None),
                username=getattr(me, "username", None),
                has_profile_photo=getattr(me, "photo", None) is not None,
            )
        except AccountImportError:
            raise
        except errors.AuthKeyDuplicatedError as exc:
            raise AccountImportError(
                f"{source_name} уже используется в другой программе; "
                "полностью закройте её и повторите импорт"
            ) from exc
        except errors.ApiIdInvalidError as exc:
            raise AccountImportError(
                "Telegram отклонил PROXIMA_TELEGRAM_API_ID/HASH; проверьте пару на my.telegram.org"
            ) from exc
        except (errors.SessionPasswordNeededError, NoPasswordProvided) as exc:
            raise AccountImportError(
                "на аккаунте включена двухэтапная защита; временно задайте "
                "PROXIMA_TDATA_TWO_FACTOR_PASSWORD и повторите импорт"
            ) from exc
        except PasswordIncorrect as exc:
            raise AccountImportError("неверный пароль двухэтапной защиты Telegram") from exc
        except Exception as exc:
            raise AccountImportError(
                f"не удалось создать отдельную серверную сессию: {type(exc).__name__}"
            ) from exc
        finally:
            for client in (server_client, source_client):
                if client is None:
                    continue
                try:
                    await client.disconnect()  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - best-effort import cleanup
                    logger.debug("account import client disconnect failed", exc_info=True)

    @staticmethod
    def _read_session_file(session_bytes: bytes) -> str:
        from telethon.sessions import SQLiteSession  # noqa: PLC0415

        if not session_bytes:
            raise AccountImportError("файл .session пуст")
        if len(session_bytes) > MAX_SESSION_FILE_BYTES:
            raise AccountImportError("файл .session больше 5 МБ")

        with tempfile.TemporaryDirectory(prefix="proxima-session-") as tmp:
            path = Path(tmp) / "account.session"
            path.write_bytes(session_bytes)
            connection = None
            try:
                connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(sessions)").fetchall()
                }
                if {
                    "dc_id",
                    "api_id",
                    "test_mode",
                    "auth_key",
                    "user_id",
                    "is_bot",
                } <= columns:
                    row = connection.execute(
                        "SELECT dc_id, test_mode, auth_key, is_bot FROM sessions LIMIT 1"
                    ).fetchone()
                    if row is None:
                        raise AccountImportError("в Pyrogram .session нет данных авторизации")
                    return _string_session_from_auth_key(
                        dc_id=row[0],
                        auth_key=row[2],
                        test_mode=_json_bool(row[1]),
                        is_bot=_json_bool(row[3]),
                    )
                if not {"dc_id", "server_address", "port", "auth_key"} <= columns:
                    raise AccountImportError(
                        "неизвестный формат .session; поддерживаются Telethon и Pyrogram"
                    )
            except AccountImportError:
                raise
            except sqlite3.DatabaseError as exc:
                raise AccountImportError("файл .session повреждён или не является SQLite") from exc
            finally:
                if connection is not None:
                    connection.close()

            sqlite = None
            try:
                sqlite = SQLiteSession(str(path.with_suffix("")))
            except Exception as exc:  # noqa: BLE001
                raise AccountImportError(
                    "файл Telethon .session повреждён или несовместим"
                ) from exc
            try:
                if sqlite.auth_key is None or sqlite.dc_id is None:
                    raise AccountImportError("в Telethon .session нет ключа авторизации")
                return _string_session_from_auth_key(
                    dc_id=sqlite.dc_id,
                    auth_key=sqlite.auth_key.key,
                    server_address=sqlite.server_address,
                    port=sqlite.port,
                )
            finally:
                sqlite.close()

    @staticmethod
    def _read_session_string(session_string: str) -> str:
        from telethon.sessions import StringSession  # noqa: PLC0415

        cleaned = session_string.strip()
        try:
            parsed = StringSession(cleaned)
        except Exception as exc:  # noqa: BLE001
            raise AccountImportError("строка сессии Telethon не распознана") from exc
        if parsed.auth_key is None:
            raise AccountImportError("строка сессии не содержит ключа авторизации")
        return parsed.save()

    @classmethod
    def _read_any_session_string(cls, session_string: str) -> str:
        try:
            return cls._read_session_string(session_string)
        except AccountImportError:
            return _read_pyrogram_session_string(session_string)

    @classmethod
    def _read_json_session(cls, json_bytes: bytes) -> str:
        if not json_bytes:
            raise AccountImportError("файл .json пуст")
        if len(json_bytes) > MAX_JSON_SESSION_BYTES:
            raise AccountImportError("файл .json больше 5 МБ")
        try:
            document = json.loads(json_bytes.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AccountImportError("файл не является корректным JSON в UTF-8") from exc

        if isinstance(document, str) and document.strip():
            return cls._read_any_session_string(document)

        session_keys = {
            "session",
            "session_string",
            "stringsession",
            "string_session",
            "telethon_session",
            "pyrogram_session",
        }
        mappings: list[dict[str, object]] = []
        for node in _walk_json_nodes(document):
            if not isinstance(node, dict):
                continue
            normalized = {str(key).strip().lower(): value for key, value in node.items()}
            mappings.append(normalized)
            for key in session_keys:
                candidate = normalized.get(key)
                if not isinstance(candidate, str) or not candidate.strip():
                    continue
                try:
                    return cls._read_any_session_string(candidate)
                except AccountImportError:
                    continue

        for mapping in mappings:
            if "auth_key" not in mapping or "dc_id" not in mapping:
                continue
            return _string_session_from_auth_key(
                dc_id=mapping["dc_id"],
                auth_key=mapping["auth_key"],
                server_address=mapping.get("server_address") or mapping.get("server"),
                port=mapping.get("port", 443),
                test_mode=_json_bool(mapping.get("test_mode", False)),
                is_bot=_json_bool(mapping.get("is_bot", False)),
            )

        raise AccountImportError(
            "JSON содержит только данные профиля, но не авторизацию. "
            "Скачайте этот аккаунт как TData или .session"
        )


def _validate_label(label: str) -> str:
    normalized = label.strip()
    if not normalized or len(normalized) > 100:
        raise AccountImportError("название должно содержать от 1 до 100 символов")
    if any(ord(character) < 32 for character in normalized):
        raise AccountImportError("название должно быть в одну строку")
    return normalized


def _limited_optional(value: object, limit: int) -> str | None:
    normalized = str(value).strip() if value is not None else ""
    return normalized[:limit] or None


def _json_bool(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _walk_json_nodes(value: object, *, limit: int = 2_000) -> list[object]:
    """Return bounded nested JSON nodes without recursion-depth surprises."""
    result: list[object] = []
    queue = [value]
    while queue and len(result) < limit:
        node = queue.pop(0)
        result.append(node)
        if isinstance(node, dict):
            queue.extend(node.values())
        elif isinstance(node, list):
            queue.extend(node)
    return result


def _decode_auth_key(value: object) -> bytes:
    if isinstance(value, bytes):
        key = value
    elif isinstance(value, (list, tuple)):
        try:
            key = bytes(int(item) for item in value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise AccountImportError("auth_key в JSON повреждён") from exc
    elif isinstance(value, str):
        cleaned = value.strip()
        hex_value = cleaned[2:] if cleaned.lower().startswith("0x") else cleaned
        try:
            if len(hex_value) == 512 and all(
                character in "0123456789abcdefABCDEF" for character in hex_value
            ):
                key = bytes.fromhex(hex_value)
            else:
                padded = cleaned + "=" * (-len(cleaned) % 4)
                key = base64.b64decode(padded, altchars=b"-_", validate=True)
        except (ValueError, TypeError) as exc:
            raise AccountImportError("auth_key в JSON не является hex/base64") from exc
    else:
        raise AccountImportError("auth_key имеет неподдерживаемый формат")
    if len(key) != 256:
        raise AccountImportError("auth_key должен содержать ровно 256 байт")
    return key


def _string_session_from_auth_key(
    *,
    dc_id: object,
    auth_key: object,
    server_address: object | None = None,
    port: object = 443,
    test_mode: bool = False,
    is_bot: bool = False,
) -> str:
    from telethon.crypto import AuthKey  # noqa: PLC0415
    from telethon.sessions import StringSession  # noqa: PLC0415

    if test_mode:
        raise AccountImportError("тестовые Telegram-сессии не поддерживаются")
    if is_bot:
        raise AccountImportError("это сессия Telegram-бота, а нужен пользовательский аккаунт")
    try:
        normalized_dc_id = int(dc_id)
        normalized_port = int(port)
    except (TypeError, ValueError) as exc:
        raise AccountImportError("в сессии указан некорректный DC или порт") from exc
    if normalized_dc_id not in PRODUCTION_DC_ADDRESSES:
        raise AccountImportError(f"неподдерживаемый Telegram DC: {normalized_dc_id}")
    if not 1 <= normalized_port <= 65_535:
        raise AccountImportError("в сессии указан некорректный порт")
    address = str(server_address or PRODUCTION_DC_ADDRESSES[normalized_dc_id]).strip()
    if not address or len(address) > 255:
        raise AccountImportError("в сессии указан некорректный адрес Telegram DC")
    session = StringSession()
    session.set_dc(normalized_dc_id, address, normalized_port)
    session.auth_key = AuthKey(_decode_auth_key(auth_key))
    return session.save()


def _read_pyrogram_session_string(session_string: str) -> str:
    cleaned = session_string.strip()
    try:
        padded = cleaned + "=" * (-len(cleaned) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise AccountImportError("строка сессии Telethon/Pyrogram не распознана") from exc

    formats = (
        (">BI?256sQ?", True),
        (">B?256sI?", False),
        (">B?256sQ?", False),
    )
    for format_string, has_api_id in formats:
        if len(raw) != struct.calcsize(format_string):
            continue
        values = struct.unpack(format_string, raw)
        if has_api_id:
            dc_id, _api_id, test_mode, auth_key, _user_id, is_bot = values
        else:
            dc_id, test_mode, auth_key, _user_id, is_bot = values
        return _string_session_from_auth_key(
            dc_id=dc_id,
            auth_key=auth_key,
            test_mode=bool(test_mode),
            is_bot=bool(is_bot),
        )
    raise AccountImportError("строка сессии Telethon/Pyrogram не распознана")


def _read_account_archive(archive_bytes: bytes) -> tuple[AccountArchiveEntry, ...]:
    if not archive_bytes:
        raise AccountImportError("ZIP-архив аккаунтов пуст")
    if len(archive_bytes) > MAX_ARCHIVE_BYTES:
        raise AccountImportError("ZIP-архив аккаунтов больше 25 МБ")
    try:
        archive = zipfile.ZipFile(io.BytesIO(archive_bytes))
    except zipfile.BadZipFile as exc:
        raise AccountImportError("файл не является корректным ZIP-архивом") from exc

    groups: dict[str, dict[str, str | bytes]] = {}
    try:
        with archive:
            members = archive.infolist()
            if len(members) > MAX_BATCH_ARCHIVE_MEMBERS:
                raise AccountImportError(f"в архиве больше {MAX_BATCH_ARCHIVE_MEMBERS} файлов")
            total = sum(member.file_size for member in members)
            if total > MAX_UNCOMPRESSED_BYTES:
                raise AccountImportError("распакованный архив аккаунтов больше 80 МБ")

            for member in members:
                normalized_name = member.filename.replace("\\", "/")
                if len(normalized_name) > 512:
                    raise AccountImportError("архив содержит слишком длинное имя файла")
                path = PurePosixPath(normalized_name)
                if (
                    not normalized_name
                    or "\x00" in normalized_name
                    or path.is_absolute()
                    or ".." in path.parts
                    or (path.parts and ":" in path.parts[0])
                ):
                    raise AccountImportError("архив содержит небезопасные пути")
                if member.is_dir():
                    continue
                mode = member.external_attr >> 16
                if mode and stat.S_ISLNK(mode):
                    raise AccountImportError("архив содержит символические ссылки")
                if member.flag_bits & 0x1:
                    raise AccountImportError("архив защищён паролем и не может быть прочитан")

                suffix = path.suffix.lower()
                if suffix not in {".session", ".json"}:
                    continue
                size_limit = (
                    MAX_SESSION_FILE_BYTES if suffix == ".session" else MAX_JSON_SESSION_BYTES
                )
                if member.file_size > size_limit:
                    raise AccountImportError(
                        f"файл {path.name} больше {size_limit // (1024 * 1024)} МБ"
                    )
                stem = path.with_suffix("").as_posix().lstrip("./")
                if not stem:
                    raise AccountImportError("в архиве найден файл без имени аккаунта")
                key = stem.casefold()
                field_name = "session" if suffix == ".session" else "json"
                group = groups.setdefault(key, {"source_name": stem})
                if field_name in group:
                    raise AccountImportError(f"в архиве повторяется файл {stem}{suffix}")
                try:
                    payload = archive.read(member)
                except (zipfile.BadZipFile, RuntimeError, NotImplementedError) as exc:
                    raise AccountImportError(f"не удалось прочитать {path.name}") from exc
                if len(payload) != member.file_size:
                    raise AccountImportError(f"файл {path.name} прочитан не полностью")
                group[field_name] = payload
    finally:
        archive.close()

    if not groups:
        raise AccountImportError("в архиве нет файлов .session или .json")
    if len(groups) > MAX_BATCH_IMPORT_ACCOUNTS:
        raise AccountImportError(
            f"в одном архиве поддерживается не больше {MAX_BATCH_IMPORT_ACCOUNTS} аккаунтов"
        )

    return tuple(
        AccountArchiveEntry(
            source_name=str(group["source_name"]),
            session_bytes=group.get("session") if isinstance(group.get("session"), bytes) else None,
            json_bytes=group.get("json") if isinstance(group.get("json"), bytes) else None,
        )
        for _, group in sorted(groups.items())
    )


def _two_factor_password_from_json(json_bytes: bytes | None) -> str | None:
    """Read a common seller-export 2FA field without persisting or logging it."""

    if not json_bytes or len(json_bytes) > MAX_JSON_SESSION_BYTES:
        return None
    try:
        document = json.loads(json_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    password_keys = {
        "2fa",
        "2fa_password",
        "cloud_password",
        "two_fa",
        "two_fa_password",
        "two_factor",
        "two_factor_password",
        "twofa",
        "twofactor",
    }
    for node in _walk_json_nodes(document):
        if not isinstance(node, dict):
            continue
        for key, value in node.items():
            normalized_key = str(key).strip().lower().replace("-", "_").replace(" ", "_")
            if normalized_key not in password_keys or not isinstance(value, str):
                continue
            candidate = value.strip()
            if candidate and "\x00" not in candidate and len(candidate) <= 512:
                return candidate
    return None


def _safe_extract_zip(archive_bytes: bytes, destination: Path) -> None:
    try:
        archive = zipfile.ZipFile(io.BytesIO(archive_bytes))
    except zipfile.BadZipFile as exc:
        raise AccountImportError("файл не является корректным ZIP-архивом") from exc
    with archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise AccountImportError("в архиве слишком много файлов")
        total = sum(member.file_size for member in members)
        if total > MAX_UNCOMPRESSED_BYTES:
            raise AccountImportError("распакованный tdata больше 80 МБ")
        destination_root = destination.resolve()
        for member in members:
            target = (destination / member.filename).resolve()
            if not target.is_relative_to(destination_root):
                raise AccountImportError("архив содержит небезопасные пути (path traversal)")
        archive.extractall(destination)


def _locate_tdata_dir(root: Path) -> Path:
    """Find the folder that directly holds the tdata ``key_datas`` map file."""
    candidates: list[Path] = []
    for path in root.rglob("*"):
        if path.is_file() and path.name.lower() == TDATA_MARKER:
            candidates.append(path.parent)
    if not candidates:
        # Some exports store the map differently; fall back to a "tdata" folder.
        for path in root.rglob("tdata"):
            if path.is_dir():
                candidates.append(path)
    if not candidates:
        raise AccountImportError("в архиве не найдена папка tdata (файл key_datas отсутствует)")
    # Prefer the shallowest match to avoid nested backup copies.
    return min(candidates, key=lambda item: len(item.parts))


def _telethon_proxy(route: object) -> tuple[object, str, int, bool, str | None, str | None]:
    import socks  # noqa: PLC0415 - PySocks constants for telethon

    protocol = route.protocol
    value = getattr(protocol, "value", str(protocol))
    if value in {"socks5", "socks5h"}:
        proxy_type = socks.SOCKS5
        rdns = True
    elif value in {"http", "https"}:
        proxy_type = socks.HTTP
        rdns = True
    else:  # pragma: no cover - defensive
        raise ProxyRouteUnavailable(f"неподдерживаемый протокол прокси: {value}")
    return (
        proxy_type,
        route.host,
        int(route.port),
        rdns,
        getattr(route, "username", None),
        getattr(route, "password", None),
    )


def _safe_account_error(error: BaseException) -> str:
    message = str(error) or type(error).__name__
    return message.replace("\x00", "")[:500]


def _account_check_failure_code(error: BaseException) -> str:
    """Separate a dead authorization from a temporary connectivity failure."""
    error_name = type(error).__name__
    rpc_message = str(getattr(error, "message", "") or "").upper()
    fatal_names = {
        "AuthKeyDuplicatedError",
        "AuthKeyInvalidError",
        "AuthKeyUnregisteredError",
        "PhoneNumberBannedError",
        "SessionExpiredError",
        "SessionRevokedError",
        "UnauthorizedError",
        "UserDeactivatedBanError",
        "UserDeactivatedError",
    }
    fatal_messages = {
        "AUTH_KEY_DUPLICATED",
        "AUTH_KEY_INVALID",
        "AUTH_KEY_UNREGISTERED",
        "PHONE_NUMBER_BANNED",
        "SESSION_EXPIRED",
        "SESSION_REVOKED",
        "USER_DEACTIVATED",
        "USER_DEACTIVATED_BAN",
    }
    if error_name in fatal_names or rpc_message in fatal_messages:
        return "account_dead"
    return "account_check_unavailable"


def _validate_telegram_name(value: str, *, required: bool) -> str:
    normalized = value.strip()
    if not normalized and required:
        raise AccountProfileError("имя Telegram не может быть пустым")
    if len(normalized) > 64:
        raise AccountProfileError("имя или фамилия длиннее 64 символов")
    if any(ord(character) < 32 for character in normalized):
        raise AccountProfileError("имя и фамилия должны быть в одну строку")
    return normalized


def _validate_telegram_about(value: str) -> str:
    normalized = value.strip()
    if len(normalized) > 70:
        raise AccountProfileError("описание Telegram не должно быть длиннее 70 символов")
    return normalized


def _validate_profile_photo_name(value: str) -> str:
    name = Path(value or "avatar.jpg").name
    suffix = Path(name).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png"}:
        raise AccountProfileError("аватар должен быть JPG или PNG")
    return f"avatar{suffix}"


def _optional_profile_value(value: object) -> str | None:
    normalized = str(value).strip() if value is not None else ""
    return normalized[:64] or None


def _safe_account_operation_error(error: BaseException) -> str:
    error_name = type(error).__name__
    if "Flood" in error_name and getattr(error, "seconds", None) is not None:
        seconds = int(getattr(error, "seconds", 0) or 0)
        return f"Telegram просит подождать {seconds} секунд перед изменением профиля"
    messages = {
        "FirstNameInvalidError": "Telegram отклонил это имя",
        "PhotoExtInvalidError": "Telegram принимает для аватара только изображение",
        "PhotoInvalidDimensionsError": "неподходящие размеры изображения",
        "ImageProcessFailedError": "Telegram не смог обработать изображение",
        "FilePartsInvalidError": "не удалось загрузить изображение целиком",
        "InviteSlugEmptyError": "в ссылке на папку отсутствует код",
        "InviteSlugExpiredError": "ссылка на папку Telegram истекла или отозвана",
        "ChatlistsTooMuchError": "достигнут лимит подключённых папок Telegram",
    }
    rpc_message = str(getattr(error, "message", "") or "").upper()
    rpc_messages = {
        "INVITE_SLUG_EMPTY": "в ссылке на папку отсутствует код",
        "INVITE_SLUG_EXPIRED": "ссылка на папку Telegram истекла или отозвана",
        "CHATLISTS_TOO_MUCH": "достигнут лимит подключённых папок Telegram",
    }
    return messages.get(error_name, rpc_messages.get(rpc_message, _safe_account_error(error)))


def _chat_folder_preview(slug: str, result: object) -> ChatFolderPreview:
    from telethon import utils  # noqa: PLC0415
    from telethon.tl.types import (  # noqa: PLC0415
        ChannelForbidden,
        ChatForbidden,
    )

    chats = list(getattr(result, "chats", None) or [])
    chat_by_id = {utils.get_peer_id(chat): chat for chat in chats}
    peers = list(getattr(result, "peers", None) or [])
    missing_peers = list(getattr(result, "missing_peers", None) or [])
    already_peers = list(getattr(result, "already_peers", None) or [])
    if not peers:
        peers = [*missing_peers, *already_peers]
    already_ids = {utils.get_peer_id(peer) for peer in already_peers}
    entries: list[ChatFolderEntry] = []
    seen: set[int] = set()
    for peer in peers:
        peer_id = utils.get_peer_id(peer)
        if peer_id in seen:
            continue
        seen.add(peer_id)
        chat = chat_by_id.get(peer_id)
        if chat is None:
            continue
        inaccessible = isinstance(chat, ChannelForbidden | ChatForbidden)
        is_supergroup = bool(getattr(chat, "megagroup", False))
        is_channel = bool(getattr(chat, "broadcast", False)) and not is_supergroup
        chat_type = "supergroup" if is_supergroup else "channel" if is_channel else "group"
        username = _chat_username(chat)
        entries.append(
            ChatFolderEntry(
                peer_id=peer_id,
                title=str(getattr(chat, "title", None) or peer_id)[:150],
                username=username,
                chat_type=chat_type,
                accessible=not inaccessible,
                already_joined=(peer_id in already_ids or getattr(chat, "left", None) is False),
                can_send_hint=_chat_can_send_hint(chat, chat_type),
            )
        )
    title_value = getattr(result, "title", None)
    title = getattr(title_value, "text", None) or title_value or f"Папка {slug[:8]}"
    return ChatFolderPreview(slug=slug, title=str(title)[:150], entries=tuple(entries))


def _chat_username(chat: object) -> str | None:
    username = getattr(chat, "username", None)
    if username:
        return str(username)
    for item in getattr(chat, "usernames", None) or []:
        if getattr(item, "active", False) and getattr(item, "username", None):
            return str(item.username)
    return None


def _chat_can_send_hint(chat: object, chat_type: str) -> bool:
    if chat_type == "channel":
        rights = getattr(chat, "admin_rights", None)
        return bool(
            getattr(chat, "creator", False)
            or (rights is not None and getattr(rights, "post_messages", False))
        )
    if getattr(chat, "creator", False) or getattr(chat, "admin_rights", None) is not None:
        return True
    banned = getattr(chat, "banned_rights", None)
    default_banned = getattr(chat, "default_banned_rights", None)
    effective_rights = banned if banned is not None else default_banned
    return not bool(
        effective_rights is not None
        and (
            getattr(effective_rights, "send_messages", False)
            or getattr(effective_rights, "send_plain", False)
            or getattr(effective_rights, "view_messages", False)
        )
    )


def build_telethon_proxy(route: object) -> tuple[object, str, int, bool, str | None, str | None]:
    """Public wrapper so the live gateway can reuse the proxy mapping."""
    return _telethon_proxy(route)
