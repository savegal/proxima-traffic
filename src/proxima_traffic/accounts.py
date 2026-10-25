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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Protocol
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.account_groups import sync_premium_account_group
from proxima_traffic.account_leases import AccountOperationLeaseCoordinator
from proxima_traffic.account_state import apply_flood_pause, locked_account, session_fingerprint
from proxima_traffic.config import Settings
from proxima_traffic.gateways import GatewayError, PermanentGatewayError
from proxima_traffic.models import (
    Account,
    AccountGroup,
    AccountGroupMember,
    AccountOperationLease,
    AccountSessionSource,
    AccountStatus,
    CampaignRun,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    ParticipantReactionRule,
    ParticipantReactionTask,
    ParticipantReactionTaskStatus,
    ProxyAssignment,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    ensure_utc,
    utc_now,
)
from proxima_traffic.profile_names import ProfileName
from proxima_traffic.proxies import (
    ProxyNetworkRoute,
    ProxyRouteUnavailable,
    ProxyService,
    mtproto_connection_type,
)
from proxima_traffic.services import (
    ConflictError,
    DomainError,
    NotFoundError,
    add_audit_event,
    safe_error_message,
)

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


def _serialized_account_ids(raw_value: str | None) -> frozenset[int]:
    """Read account references from a trusted, durable JSON snapshot safely."""

    if not raw_value:
        return frozenset()
    try:
        payload = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return frozenset()
    account_ids: set[int] = set()

    def visit(value: object) -> None:
        if isinstance(value, dict):
            raw_account_id = value.get("account_id")
            if isinstance(raw_account_id, int) and raw_account_id > 0:
                account_ids.add(raw_account_id)
            elif isinstance(raw_account_id, str) and raw_account_id.isdecimal():
                account_ids.add(int(raw_account_id))
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                if isinstance(nested, int) and nested > 0:
                    account_ids.add(nested)
                elif isinstance(nested, str) and nested.isdecimal():
                    account_ids.add(int(nested))
                else:
                    visit(nested)

    visit(payload)
    return frozenset(account_ids)


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


class AccountSessionChangedError(ConflictError):
    """A removal must not erase credentials restored after the session check."""


class AccountOperationError(DomainError):
    """Base error for an authenticated Telegram account operation."""


class AccountProfileError(AccountOperationError):
    """Raised when Telegram rejects or cannot apply a profile change."""


class AccountIdentityError(AccountProfileError):
    """The session belongs to a different Telegram user than the account card."""


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
class TelethonTransport:
    """Transport settings needed by Telethon without exposing proxy secrets."""

    proxy: object | None = field(repr=False)
    connection: object | None = field(default=None, repr=False)

    def client_kwargs(self) -> dict[str, object]:
        values: dict[str, object] = {"proxy": self.proxy}
        if self.connection is not None:
            values["connection"] = self.connection
        return values


@dataclass(frozen=True, slots=True)
class AccountCheckResult:
    account_id: int
    authorized: bool
    telegram_user_id: int | None
    phone: str | None
    username: str | None
    error: str | None
    # ``authorized`` records the last known account state.  A transient route
    # error intentionally preserves it, so callers must use this code (rather
    # than ``authorized is False``) before offering destructive actions.
    failure_code: str | None = None


@dataclass(frozen=True, slots=True)
class AccountRemovalResult:
    """Outcome of a requested account removal without losing historical reports."""

    account_id: int
    label: str
    deleted: bool
    preserved_for_history: bool


@dataclass(frozen=True, slots=True)
class AccountRemovalPlan:
    """Whether a live account can be removed without interrupting work."""

    account_id: int
    label: str
    active_work: tuple[str, ...]

    @property
    def can_remove(self) -> bool:
        return not self.active_work


@dataclass(frozen=True, slots=True)
class AccountBulkCheckEntry:
    account_id: int
    label: str
    result: AccountCheckResult | None
    error: str | None = None
    session_fingerprint: str | None = field(default=None, repr=False)

    @property
    def ready(self) -> bool:
        return bool(self.result is not None and self.result.authorized and not self.result.error)

    @property
    def session_revoked(self) -> bool:
        """True only when Telegram conclusively rejected the session.

        A bought, not-yet-checked account starts with ``authorized=False``.
        Network/proxy failures must remain an actionable diagnostic issue, not
        an invitation to delete a potentially valid session.
        """
        return bool(
            self.result is not None
            and not self.result.authorized
            and self.result.failure_code in {"account_dead", "account_unauthorized"}
        )


@dataclass(frozen=True, slots=True)
class AccountBulkCheckSummary:
    entries: tuple[AccountBulkCheckEntry, ...]

    @property
    def ready(self) -> tuple[AccountBulkCheckEntry, ...]:
        return tuple(entry for entry in self.entries if entry.ready)

    @property
    def session_revoked(self) -> tuple[AccountBulkCheckEntry, ...]:
        return tuple(entry for entry in self.entries if entry.session_revoked)

    @property
    def issues(self) -> tuple[AccountBulkCheckEntry, ...]:
        return tuple(
            entry for entry in self.entries if not entry.ready and not entry.session_revoked
        )


@dataclass(frozen=True, slots=True)
class AccountInvalidSessionCleanupIssue:
    """One account deliberately left untouched during batch cleanup."""

    account_id: int
    label: str
    reason: str


@dataclass(frozen=True, slots=True)
class AccountInvalidSessionCleanupSummary:
    """Result of a fresh check followed by safe removal of dead access."""

    check_summary: AccountBulkCheckSummary
    removed: tuple[AccountRemovalResult, ...]
    skipped: tuple[AccountInvalidSessionCleanupIssue, ...]

    @property
    def deleted(self) -> tuple[AccountRemovalResult, ...]:
        return tuple(result for result in self.removed if result.deleted)

    @property
    def preserved_for_history(self) -> tuple[AccountRemovalResult, ...]:
        return tuple(result for result in self.removed if result.preserved_for_history)


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
    is_premium: bool | None = None


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
        self._operation_leases = AccountOperationLeaseCoordinator(
            session_factory,
            owner_id=f"account-service-{uuid4().hex}",
            lease_seconds=settings.worker_lease_seconds,
            heartbeat_seconds=settings.worker_heartbeat_seconds,
        )

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
        transport = self._import_transport(proxy_route)
        two_factor_password = self._configured_two_factor_password(two_factor_password)
        converted = await asyncio.to_thread(
            self._convert_tdata_archive,
            archive_bytes,
            api_id,
            api_hash,
            transport.proxy,
            two_factor_password,
            **_connection_kwargs(transport),
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
        transport = self._import_transport(proxy_route)
        source_session = await asyncio.to_thread(
            self._read_session_file,
            session_bytes,
        )
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=transport.proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
            **_connection_kwargs(transport),
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
        transport = self._import_transport(proxy_route)
        source_session = self._read_any_session_string(session_string)
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=transport.proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
            **_connection_kwargs(transport),
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
        transport = self._import_transport(proxy_route)
        source_session = await asyncio.to_thread(self._read_json_session, json_bytes)
        converted = await self._convert_existing_session(
            source_session,
            api_id=api_id,
            api_hash=api_hash,
            proxy=transport.proxy,
            two_factor_password=self._configured_two_factor_password(two_factor_password),
            **_connection_kwargs(transport),
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

    async def create_archive_import_group(
        self,
        account_ids: list[int] | tuple[int, ...],
        *,
        actor: str = "control-bot",
    ) -> AccountGroup:
        """Create an operator-owned group for one completed ZIP import.

        Import batches are processed entry by entry, so their account IDs stay
        durable even if some archive members fail.  Keeping this as a regular
        custom group lets the operator prepare just this batch afterwards.
        """

        normalized_ids = tuple(
            sorted({int(account_id) for account_id in account_ids if int(account_id) > 0})
        )
        if not normalized_ids:
            raise ValueError("для группы ZIP-импорта нужен хотя бы один аккаунт")

        account_range = (
            f"#{normalized_ids[0]}"
            if len(normalized_ids) == 1
            else f"#{normalized_ids[0]}–#{normalized_ids[-1]}"
        )
        base_name = f"ZIP-импорт · {utc_now():%Y-%m-%d %H:%M} · {account_range}"
        description = f"Создано автоматически после ZIP-импорта · аккаунтов: {len(normalized_ids)}"

        async with self._session_factory() as session:
            found_ids = {
                int(account_id)
                for account_id in await session.scalars(
                    select(Account.id).where(Account.id.in_(normalized_ids))
                )
            }
            if found_ids != set(normalized_ids):
                raise NotFoundError("некоторые импортированные аккаунты уже недоступны")

            # The account-ID range makes collisions unlikely.  The suffix also
            # covers an operator-created group with the same readable title.
            existing_names = set(
                await session.scalars(
                    select(AccountGroup.name).where(AccountGroup.name.like(f"{base_name}%"))
                )
            )
            group_name = base_name
            for number in range(2, 10_000):
                if group_name not in existing_names:
                    break
                group_name = f"{base_name} ({number})"
            else:  # pragma: no cover - defensive guard for a corrupted database
                raise ConflictError("не удалось подобрать название для группы ZIP-импорта")

            group = AccountGroup(name=group_name, description=description)
            session.add(group)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("не удалось создать группу ZIP-импорта") from exc
            session.add_all(
                [
                    AccountGroupMember(group_id=group.id, account_id=account_id)
                    for account_id in normalized_ids
                ]
            )
            add_audit_event(
                session,
                event_type="account_group.archive_import_created",
                entity_type="account_group",
                entity_id=group.id,
                details={
                    "name": group.name,
                    "account_ids": list(normalized_ids),
                    "account_count": len(normalized_ids),
                },
                actor=actor,
            )
            await session.commit()
            await session.refresh(group)
            return group

    # ---------------------------------------------------------------- lifecycle
    async def get_account(self, account_id: int) -> Account:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            return account

    async def delete_account(
        self, account_id: int, *, actor: str = "control-bot",
        expected_session_fingerprint: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            account = await self._account_for_removal(
                session, account_id, expected_session_fingerprint
            )
            configured_comment_rule = await session.scalar(
                select(ChannelCommentRule.id).where(
                    ChannelCommentRule.primary_account_id == account_id
                )
            )
            configured_comment_json = any(
                account_id in _serialized_account_ids(dialogue_steps_json)
                or account_id in _serialized_account_ids(reaction_account_ids_json)
                for dialogue_steps_json, reaction_account_ids_json in await session.execute(
                    select(
                        ChannelCommentRule.dialogue_steps_json,
                        ChannelCommentRule.reaction_account_ids_json,
                    )
                )
            )
            configured_participant_rule = any(
                account_id in _serialized_account_ids(account_ids_json)
                for (account_ids_json,) in await session.execute(
                    select(ParticipantReactionRule.account_ids_json)
                )
            )
            if (
                configured_comment_rule is not None
                or configured_comment_json
                or configured_participant_rule
            ):
                raise ConflictError(
                    "аккаунт входит в настройки автоматизации. Сессия будет отозвана, "
                    "а историческая запись сохранена, чтобы не сломать правила"
                )
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

    async def account_removal_plan(self, account_id: int) -> AccountRemovalPlan:
        """Describe only work that makes a destructive removal unsafe right now."""
        active_run_statuses = (RunStatus.QUEUED, RunStatus.RUNNING)
        active_reaction_statuses = (ReactionTaskStatus.QUEUED, ReactionTaskStatus.RUNNING)
        active_direct_statuses = (DirectMessageTaskStatus.QUEUED, DirectMessageTaskStatus.RUNNING)
        active_comment_statuses = (
            ChannelCommentRunStatus.QUEUED,
            ChannelCommentRunStatus.RUNNING,
        )
        active_participant_statuses = (
            ParticipantReactionTaskStatus.QUEUED,
            ParticipantReactionTaskStatus.RUNNING,
        )
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            now = utc_now()
            campaign_run_ids = [
                int(run_id)
                for (
                    run_id,
                    sender_pool_snapshot_json,
                    dialogue_steps_snapshot_json,
                ) in await session.execute(
                    select(
                        CampaignRun.id,
                        CampaignRun.sender_pool_snapshot_json,
                        CampaignRun.dialogue_steps_snapshot_json,
                    ).where(CampaignRun.status.in_(active_run_statuses))
                )
                if account_id in _serialized_account_ids(sender_pool_snapshot_json)
                or account_id in _serialized_account_ids(dialogue_steps_snapshot_json)
            ]
            direct_launch_ids = [
                int(launch_id)
                for launch_id, account_ids_snapshot_json in await session.execute(
                    select(DirectLaunch.id, DirectLaunch.account_ids_snapshot_json).where(
                        DirectLaunch.status.in_(
                            [DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING]
                        )
                    )
                )
                if account_id in _serialized_account_ids(account_ids_snapshot_json)
            ]
            invite_launch_ids = [
                int(launch_id)
                for launch_id, account_ids_snapshot_json in await session.execute(
                    select(
                        SubscriberInviteLaunch.id,
                        SubscriberInviteLaunch.account_ids_snapshot_json,
                    ).where(
                        SubscriberInviteLaunch.status.in_(
                            [DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING]
                        )
                    )
                )
                if account_id in _serialized_account_ids(account_ids_snapshot_json)
            ]
            comment_rule_ids = [
                int(rule_id)
                for (
                    rule_id,
                    primary_account_id,
                    dialogue_steps_json,
                    reaction_account_ids_json,
                ) in await session.execute(
                    select(
                        ChannelCommentRule.id,
                        ChannelCommentRule.primary_account_id,
                        ChannelCommentRule.dialogue_steps_json,
                        ChannelCommentRule.reaction_account_ids_json,
                    )
                )
                if primary_account_id == account_id
                or account_id in _serialized_account_ids(dialogue_steps_json)
                or account_id in _serialized_account_ids(reaction_account_ids_json)
            ]
            participant_rule_ids = [
                int(rule_id)
                for rule_id, account_ids_json in await session.execute(
                    select(ParticipantReactionRule.id, ParticipantReactionRule.account_ids_json)
                )
                if account_id in _serialized_account_ids(account_ids_json)
            ]
            direct_conditions = [
                DirectMessageTask.status.in_(active_direct_statuses),
                DirectMessageTask.sender_account_id == account_id,
            ]
            if direct_launch_ids:
                direct_conditions[1] = or_(
                    DirectMessageTask.sender_account_id == account_id,
                    DirectMessageTask.launch_id.in_(direct_launch_ids),
                )
            invite_conditions = [
                SubscriberInviteTask.status.in_(active_direct_statuses),
                SubscriberInviteTask.sender_account_id == account_id,
            ]
            if invite_launch_ids:
                invite_conditions[1] = or_(
                    SubscriberInviteTask.sender_account_id == account_id,
                    SubscriberInviteTask.launch_id.in_(invite_launch_ids),
                )
            comment_conditions = [ChannelCommentRun.status.in_(active_comment_statuses)]
            if comment_rule_ids:
                comment_conditions.append(ChannelCommentRun.rule_id.in_(comment_rule_ids))
            else:
                comment_conditions.append(ChannelCommentRule.primary_account_id == account_id)
            participant_conditions = [
                ParticipantReactionTask.status.in_(active_participant_statuses),
                ParticipantReactionTask.reactor_account_id == account_id,
            ]
            if participant_rule_ids:
                participant_conditions[1] = or_(
                    ParticipantReactionTask.reactor_account_id == account_id,
                    ParticipantReactionTask.rule_id.in_(participant_rule_ids),
                )
            checks = (
                (
                    "рассылки",
                    select(func.count())
                    .select_from(CampaignRun)
                    .where(
                        or_(
                            CampaignRun.sender_account_id == account_id,
                            CampaignRun.id.in_(campaign_run_ids),
                        ),
                        CampaignRun.status.in_(active_run_statuses),
                    ),
                ),
                (
                    "реакции на публикации",
                    select(func.count())
                    .select_from(ReactionTask)
                    .where(
                        (ReactionTask.source_account_id == account_id)
                        | (ReactionTask.reactor_account_id == account_id),
                        ReactionTask.status.in_(active_reaction_statuses),
                    ),
                ),
                (
                    "рассылки в ЛС",
                    select(func.count()).select_from(DirectMessageTask).where(*direct_conditions),
                ),
                (
                    "приглашения",
                    select(func.count())
                    .select_from(SubscriberInviteTask)
                    .where(*invite_conditions),
                ),
                (
                    "авто-реакции в чатах",
                    select(func.count())
                    .select_from(ParticipantReactionTask)
                    .where(*participant_conditions),
                ),
                (
                    "автокомментинг",
                    select(func.count())
                    .select_from(ChannelCommentRun)
                    .join(ChannelCommentRule, ChannelCommentRule.id == ChannelCommentRun.rule_id)
                    .where(*comment_conditions),
                ),
                (
                    "текущая Telegram-операция",
                    select(func.count())
                    .select_from(AccountOperationLease)
                    .where(
                        AccountOperationLease.account_id == account_id,
                        AccountOperationLease.lease_until > now,
                    ),
                ),
            )
            active_work: list[str] = []
            for label, statement in checks:
                count = int(await session.scalar(statement) or 0)
                if count > 0:
                    active_work.append(f"{label}: {count}")
            return AccountRemovalPlan(
                account_id=account_id,
                label=account.label,
                active_work=tuple(active_work),
            )

    async def remove_account_safely(
        self,
        account_id: int,
        *,
        actor: str = "control-bot",
        expected_session_fingerprint: str | None = None,
    ) -> AccountRemovalResult:
        """Delete an account when possible, otherwise revoke only its live access.

        Account rows may be protected by immutable campaign or delivery history.
        A removal request must not strand the operator behind a foreign-key
        error: in that case the account is disabled, credentials and proxy
        assignment are erased, and the historical row remains valid.
        """
        plan = await self.account_removal_plan(account_id)
        if not plan.can_remove:
            raise ConflictError(
                "аккаунт ещё участвует в активных задачах; остановите их или дождитесь "
                "завершения: " + "; ".join(plan.active_work)
            )
        label = plan.label
        try:
            await self.delete_account(
                account_id, actor=actor,
                expected_session_fingerprint=expected_session_fingerprint,
            )
        except AccountSessionChangedError:
            raise
        except ConflictError:
            await self.clear_session(
                account_id, actor=actor,
                expected_session_fingerprint=expected_session_fingerprint,
            )
            async with self._session_factory() as session:
                add_audit_event(
                    session,
                    event_type="account.removal_preserved_history",
                    entity_type="account",
                    entity_id=account_id,
                    details={"label": label, "reason": "referenced_by_history"},
                    actor=actor,
                )
                await session.commit()
            return AccountRemovalResult(
                account_id=account_id,
                label=label,
                deleted=False,
                preserved_for_history=True,
            )
        return AccountRemovalResult(
            account_id=account_id,
            label=label,
            deleted=True,
            preserved_for_history=False,
        )

    async def _account_for_removal(
        self, session: AsyncSession, account_id: int, expected_fingerprint: str | None
    ) -> Account:
        # Serialize ordinary removal as well as cleanup against new work and
        # session replacement before checking the live references.
        await session.execute(
            update(Account)
            .where(Account.id == account_id)
            .values(connection_generation=Account.connection_generation)
        )
        account = await session.get(Account, account_id)
        if account is None:
            raise NotFoundError("account not found")
        if (
            expected_fingerprint is not None
            and _session_fingerprint(account) != expected_fingerprint
        ):
            raise AccountSessionChangedError("сессия изменилась после проверки; удаление отменено")
        plan = await self.account_removal_plan(account_id)
        if not plan.can_remove:
            raise ConflictError("аккаунт занят: " + "; ".join(plan.active_work))
        return account

    async def clear_session(
        self, account_id: int, *, actor: str = "control-bot",
        expected_session_fingerprint: str | None = None,
    ) -> Account:
        """Revoke stored Telegram access while keeping the historical account row.

        Access must never be removed from underneath a queued or running task.
        Keeping this guard in the service (rather than only in the bot screen)
        also protects maintenance callers and makes a confirmation click safe if
        work appeared after the confirmation screen was opened.
        """
        plan = await self.account_removal_plan(account_id)
        if not plan.can_remove:
            raise ConflictError(
                "аккаунт ещё участвует в активных задачах; остановите их или дождитесь "
                "завершения: " + "; ".join(plan.active_work)
            )
        async with self._session_factory() as session:
            account = await self._account_for_removal(
                session, account_id, expected_session_fingerprint
            )
            account.status = AccountStatus.DISABLED
            account.session_source = AccountSessionSource.NONE
            account.session_encrypted = None
            account.api_id = None
            account.api_hash_encrypted = None
            account.first_name = None
            account.last_name = None
            account.phone = None
            account.username = None
            account.about = None
            account.has_profile_photo = False
            account.authorized = False
            account.is_premium = None
            account.premium_checked_at = None
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
            await sync_premium_account_group(session, actor=actor)
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
        """Connect through the assigned proxy and confirm the session is alive.

        An unreadable local session is deliberately recorded as a diagnostic,
        rather than escaping only from the bulk-check flow.  It is not proof
        that Telegram revoked the account, so the last known authorization is
        preserved and the safe cleanup action will never remove it.
        """
        expected_fingerprint = await self._observation_fingerprint(account_id)
        try:
            decrypted = await self.load_session(account_id)
        except NotFoundError:
            raise
        except AccountImportError as exc:
            return await self._record_check(
                account_id,
                authorized=None,
                me=None,
                error=_safe_account_error(exc),
                failure_code="session_material_invalid",
                actor=actor,
                expected_session_fingerprint=expected_fingerprint,
            )
        except Exception as exc:  # noqa: BLE001 - keep a damaged local cipher actionable
            logger.warning(
                "Unable to read session material for account %s: %s",
                account_id,
                _safe_account_error(exc),
            )
            return await self._record_check(
                account_id,
                authorized=None,
                me=None,
                error=_safe_account_error(exc),
                failure_code="session_material_invalid",
                actor=actor,
                expected_session_fingerprint=expected_fingerprint,
            )
        transport = TelethonTransport(proxy=None)
        if self._proxy_service is not None:
            try:
                recovery = getattr(self._proxy_service, "ensure_account_route", None)
                route = (
                    await recovery(account_id, actor=actor)
                    if callable(recovery)
                    else await self._proxy_service.require_account_route(account_id)
                )
                transport = build_telethon_transport(route)
            except ProxyRouteUnavailable:
                if self._settings.account_require_proxy:
                    return await self._record_check(
                        account_id,
                        authorized=None,
                        me=None,
                        error=(
                            "для аккаунта нет рабочего прокси; добавьте или включите "
                            "рабочий HTTP(S)/SOCKS5/MTProto-прокси"
                        ),
                        failure_code="proxy_unavailable",
                        actor=actor,
                        expected_session_fingerprint=expected_fingerprint,
                    )
        try:
            me = await self._fetch_me(decrypted, transport)
        except Exception as exc:  # noqa: BLE001 - report any connection failure safely
            failure_code = _account_check_failure_code(exc)
            await self._record_operation_failure(account_id, exc, expected_fingerprint)
            return await self._record_check(
                account_id,
                authorized=(
                    False if failure_code in {"account_dead", "account_unauthorized"} else None
                ),
                me=None,
                error=_safe_account_error(exc),
                failure_code=failure_code,
                actor=actor,
                expected_session_fingerprint=expected_fingerprint,
            )
        return await self._record_check(
            account_id,
            authorized=me is not None,
            me=me,
            error=None,
            failure_code=None if me is not None else "account_unauthorized",
            actor=actor,
            expected_session_fingerprint=expected_fingerprint,
        )

    async def check_all_sessions(
        self,
        *,
        actor: str = "control-bot",
        concurrency: int = 3,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        account_ids: Sequence[int] | None = None,
    ) -> AccountBulkCheckSummary:
        """Verify every stored Telegram session without sending any messages.

        Bought batches often contain dozens of accounts.  Checks make a real
        network connection, so a strictly sequential pass turns one transient
        timeout into a very long operator wait.  The small, bounded fan-out
        retains independent account leases/routes without a connection burst.
        """
        conditions = [Account.session_source != AccountSessionSource.NONE]
        if account_ids is not None:
            conditions.append(Account.id.in_(tuple(set(account_ids))))
        async with self._session_factory() as session:
            accounts = list(
                await session.execute(
                    select(
                        Account.id, Account.label, Account.session_source,
                        Account.session_encrypted, Account.api_id, Account.api_hash_encrypted,
                    )
                    .where(*conditions)
                    .order_by(Account.id)
                )
            )

        semaphore = asyncio.Semaphore(max(1, min(int(concurrency), 5)))
        total = len(accounts)
        completed = 0
        progress_lock = asyncio.Lock()

        async def report_progress() -> None:
            nonlocal completed
            if on_progress is None:
                return
            async with progress_lock:
                completed += 1
                await on_progress(completed, total)

        if on_progress is not None:
            await on_progress(0, total)

        async def run_check(
            account_id: int, label: str, fingerprint: str
        ) -> AccountBulkCheckEntry:
            try:
                result = await self.check_account(account_id, actor=actor)
            except (AccountImportError, NotFoundError) as exc:
                return AccountBulkCheckEntry(
                    account_id=account_id,
                    label=label,
                    result=None,
                    error=_safe_account_error(exc),
                )
            except Exception as exc:  # noqa: BLE001 - one bad session must not stop the batch
                logger.warning(
                    "Bulk session check failed for account %s: %s",
                    account_id,
                    _safe_account_error(exc),
                )
                return AccountBulkCheckEntry(
                    account_id=account_id,
                    label=label,
                    result=None,
                    error=_safe_account_error(exc),
                )
            return AccountBulkCheckEntry(
                account_id=account_id,
                label=label,
                result=result,
                error=result.error,
                session_fingerprint=fingerprint,
            )

        async def check_one(account) -> AccountBulkCheckEntry:
            async with semaphore:
                try:
                    return await run_check(int(account.id), str(account.label),
                                           _session_fingerprint(account))
                finally:
                    await report_progress()

        # gather preserves database order, so the report remains easy to
        # reconcile with the account list despite parallel checks.
        entries = await asyncio.gather(
            *(check_one(account) for account in accounts)
        )
        return AccountBulkCheckSummary(entries=tuple(entries))

    async def remove_confirmed_invalid_sessions(
        self,
        *,
        actor: str = "control-bot",
        concurrency: int = 3,
        account_ids: Sequence[int] | None = None,
        expected_session_fingerprints: Mapping[int, str] | None = None,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> AccountInvalidSessionCleanupSummary:
        """Recheck all stored sessions, then remove only Telegram-confirmed dead access.

        The result of a previous screen must not be trusted for a destructive
        action: a proxy can recover and an operator can restore a session while
        the confirmation prompt is open. A fresh check also guarantees that
        timeouts, proxy failures and flood waits are merely reported, never
        converted into account deletion.
        """
        check_summary = await self.check_all_sessions(
            actor=actor, concurrency=concurrency, account_ids=account_ids, on_progress=on_progress
        )
        removed: list[AccountRemovalResult] = []
        skipped: list[AccountInvalidSessionCleanupIssue] = []
        for entry in check_summary.session_revoked:
            try:
                if (
                    entry.session_fingerprint is None
                    or (
                        expected_session_fingerprints is not None
                        and expected_session_fingerprints.get(entry.account_id)
                        != entry.session_fingerprint
                    )
                ):
                    raise AccountSessionChangedError("сессия изменилась; повторите просмотр списка")
                removed.append(await self.remove_account_safely(
                    entry.account_id, actor=actor,
                    expected_session_fingerprint=entry.session_fingerprint,
                ))
            except AccountSessionChangedError as exc:
                skipped.append(AccountInvalidSessionCleanupIssue(
                    account_id=entry.account_id, label=entry.label, reason=str(exc)
                ))
            except ConflictError:
                try:
                    plan = await self.account_removal_plan(entry.account_id)
                except NotFoundError:
                    skipped.append(
                        AccountInvalidSessionCleanupIssue(
                            account_id=entry.account_id,
                            label=entry.label,
                            reason="запись уже удалена другой операцией",
                        )
                    )
                else:
                    active_work = "; ".join(plan.active_work)
                    skipped.append(
                        AccountInvalidSessionCleanupIssue(
                            account_id=entry.account_id,
                            label=entry.label,
                            reason=(
                                "появилась активная задача"
                                + (f": {active_work}" if active_work else "")
                            ),
                        )
                    )
            except NotFoundError:
                skipped.append(
                    AccountInvalidSessionCleanupIssue(
                        account_id=entry.account_id,
                        label=entry.label,
                        reason="запись уже удалена другой операцией",
                    )
                )
            except Exception as exc:  # noqa: BLE001 - keep the remaining confirmed entries safe
                logger.warning(
                    "Could not remove confirmed invalid account %s: %s",
                    entry.account_id,
                    _safe_account_error(exc),
                )
                skipped.append(
                    AccountInvalidSessionCleanupIssue(
                        account_id=entry.account_id,
                        label=entry.label,
                        reason=_safe_account_error(exc),
                    )
                )
        return AccountInvalidSessionCleanupSummary(
            check_summary=check_summary,
            removed=tuple(removed),
            skipped=tuple(skipped),
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

        expected_fingerprint = await self._observation_fingerprint(account_id)

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
        if (getattr(me, "first_name", None) or "") != normalized_first or (
            getattr(me, "last_name", None) or ""
        ) != normalized_last:
            raise AccountProfileError(
                "Telegram не подтвердил новое имя. Проверьте профиль перед повторением операции"
            )
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_name_updated",
            details={"first_name": normalized_first, "last_name": normalized_last},
            actor=actor,
            expected_session_fingerprint=expected_fingerprint,
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

        expected_fingerprint = await self._observation_fingerprint(account_id)

        async def update(client: object) -> str:
            from telethon.tl.functions.account import (  # noqa: PLC0415
                UpdateProfileRequest,
            )

            await client(UpdateProfileRequest(about=normalized_about))  # type: ignore[operator]
            observed_about = await _read_profile_about(client)
            if observed_about != normalized_about:
                raise AccountProfileError(
                    "Telegram не подтвердил описание. Проверьте профиль перед повторением операции"
                )
            return observed_about

        me, observed_about = await self._run_profile_operation(account_id, update)
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_about_updated",
            details={"has_about": bool(normalized_about), "length": len(normalized_about)},
            actor=actor,
            about_override=str(observed_about),
            expected_session_fingerprint=expected_fingerprint,
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
                except Exception as exc:  # noqa: BLE001 - each account has its own outcome
                    return ProfileNameFailure(
                        account_id=account_id, error=_safe_account_operation_error(exc)
                    )

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
                except Exception as exc:  # noqa: BLE001 - each account has its own outcome
                    return ProfileNameFailure(
                        account_id=account_id, error=_safe_account_operation_error(exc)
                    )

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

        expected_fingerprint = await self._observation_fingerprint(account_id)

        async def update(client: object) -> object:
            from telethon.tl.functions.photos import (  # noqa: PLC0415
                UploadProfilePhotoRequest,
            )

            uploaded = await client.upload_file(  # type: ignore[attr-defined]
                photo_bytes,
                file_name=safe_name,
            )
            return await client(UploadProfilePhotoRequest(file=uploaded))  # type: ignore[operator]

        me, response = await self._run_profile_operation(account_id, update)
        photo_id = getattr(getattr(response, "photo", None), "id", None)
        current_photo_id = getattr(getattr(me, "photo", None), "photo_id", None)
        if not photo_id or current_photo_id != photo_id:
            raise AccountProfileError(
                "Telegram не подтвердил новый аватар. Проверьте профиль перед повторением операции"
            )
        return await self._record_profile(
            account_id,
            me,
            event_type="account.profile_photo_updated",
            details={"file_name": safe_name, "size": len(photo_bytes)},
            actor=actor,
            expected_session_fingerprint=expected_fingerprint,
        )

    async def bulk_update_profile_photos(
        self,
        account_ids: list[int],
        *,
        photo_bytes: bytes,
        file_name: str,
        concurrency: int = 3,
        actor: str = "control-bot",
    ) -> BulkProfileNameResult:
        """Set one validated avatar for a frozen, operator-selected account set."""

        normalized_ids = list(dict.fromkeys(int(account_id) for account_id in account_ids))
        if not normalized_ids:
            return BulkProfileNameResult(updated=(), failures=())
        if len(normalized_ids) > 200:
            raise AccountProfileError("за одну операцию можно изменить не более 200 аккаунтов")
        if not photo_bytes:
            raise AccountProfileError("файл аватара пуст")
        if len(photo_bytes) > MAX_PROFILE_PHOTO_BYTES:
            raise AccountProfileError("аватар больше 10 МБ")
        safe_name = _validate_profile_photo_name(file_name)
        semaphore = asyncio.Semaphore(max(1, min(concurrency, 5)))

        async def apply_photo(account_id: int) -> Account | ProfileNameFailure:
            async with semaphore:
                try:
                    return await self.update_profile_photo(
                        account_id,
                        photo_bytes=photo_bytes,
                        file_name=safe_name,
                        actor=actor,
                    )
                except Exception as exc:  # noqa: BLE001 - each account has its own outcome
                    return ProfileNameFailure(
                        account_id=account_id, error=_safe_account_operation_error(exc)
                    )

        results = await asyncio.gather(*(apply_photo(account_id) for account_id in normalized_ids))
        updated = tuple(item for item in results if isinstance(item, Account))
        failures = tuple(item for item in results if isinstance(item, ProfileNameFailure))
        async with self._session_factory() as session:
            add_audit_event(
                session,
                event_type="account.profile_photos_bulk_updated",
                entity_type="account_batch",
                entity_id=None,
                details={
                    "requested": len(normalized_ids),
                    "updated": len(updated),
                    "failed": len(failures),
                    "file_name": safe_name,
                    "size": len(photo_bytes),
                    "updated_account_ids": [account.id for account in updated],
                    "failed_account_ids": [failure.account_id for failure in failures],
                },
                actor=actor,
            )
            await session.commit()
        return BulkProfileNameResult(updated=updated, failures=failures)

    async def delete_profile_photos(
        self,
        account_id: int,
        *,
        actor: str = "control-bot",
    ) -> tuple[Account, int]:
        expected_fingerprint = await self._observation_fingerprint(account_id)

        async def delete(client: object) -> tuple[int, int, int]:
            from telethon import utils  # noqa: PLC0415
            from telethon.tl.functions.photos import DeletePhotosRequest  # noqa: PLC0415

            photos = list(await client.get_profile_photos("me"))  # type: ignore[attr-defined]
            confirmed_ids: set[int] = set()
            for offset in range(0, len(photos), 100):
                photo_ids = [
                    utils.get_input_photo(photo) for photo in photos[offset : offset + 100]
                ]
                if photo_ids:
                    response = await client(DeletePhotosRequest(id=photo_ids))  # type: ignore[operator]
                    requested = {int(photo.id) for photo in photo_ids}
                    if isinstance(response, (list, tuple)):
                        confirmed_ids.update(
                            int(value)
                            for value in response
                            if isinstance(value, int) and value in requested
                        )
            remaining = list(await client.get_profile_photos("me"))  # type: ignore[attr-defined]
            return (
                len(confirmed_ids),
                len(remaining),
                len({int(photo.id) for photo in photos}) - len(confirmed_ids),
            )

        me, outcome = await self._run_profile_operation(account_id, delete)
        deleted, remaining, unconfirmed = outcome
        account = await self._record_profile(
            account_id,
            me,
            event_type="account.profile_photos_deleted",
            details={
                "count": int(deleted),
                "remaining": int(remaining),
                "unconfirmed": int(unconfirmed),
            },
            actor=actor,
            expected_session_fingerprint=expected_fingerprint,
        )
        if remaining or unconfirmed or getattr(me, "photo", None) is not None:
            raise AccountProfileError(
                f"Удаление завершено частично: Telegram подтвердил {deleted}, "
                f"осталось {remaining}. "
                f"Не подтверждено удалений: {unconfirmed}. Проверьте профиль"
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
        self._require_dedicated_account_encryption_key()
        encrypted_session = self._cipher.encrypt(converted.session_string)
        encrypted_hash = self._cipher.encrypt(converted.api_hash)
        async with self._session_factory() as session:
            restored = False
            account: Account
            if converted.telegram_user_id is not None:
                existing = await session.scalar(
                    select(Account).where(Account.telegram_user_id == converted.telegram_user_id)
                )
                if existing is not None:
                    if (
                        existing.status is AccountStatus.DISABLED
                        and existing.session_source is AccountSessionSource.NONE
                        and existing.session_encrypted is None
                    ):
                        # A preserved historical card deliberately keeps the
                        # Telegram ID.  Re-importing the same account must
                        # restore that card instead of trapping the operator
                        # behind a duplicate-ID error or losing its history.
                        account = existing
                        restored = True
                    else:
                        raise ConflictError(f"этот аккаунт уже загружен как «{existing.label}»")
            if restored:
                # Preserve the operator's label and historical links; refresh
                # only the live Telegram access and the profile snapshot.
                account.ownership_confirmed = True
                account.status = AccountStatus.ACTIVE
                account.session_source = source
                account.session_encrypted = encrypted_session
                account.api_id = converted.api_id
                account.api_hash_encrypted = encrypted_hash
                account.first_name = _limited_optional(converted.first_name, 64)
                account.last_name = _limited_optional(converted.last_name, 64)
                account.phone = _limited_optional(converted.phone, 32)
                account.username = _limited_optional(converted.username, 64)
                account.has_profile_photo = converted.has_profile_photo
                account.authorized = True
                account.is_premium = converted.is_premium
                account.premium_checked_at = utc_now() if converted.is_premium is not None else None
                account.last_checked_at = utc_now()
                account.last_error = None
                account.next_send_at = None
                account.cooldown_until = None
                account.cooldown_reason = None
                if account.operator_paused:
                    account.status = AccountStatus.PAUSED
                if account.spambot_status in {
                    "temporary_restriction",
                    "active_restriction",
                    "restriction_unclear",
                    "permanent_restriction",
                }:
                    account.status = AccountStatus.PAUSED
                    account.cooldown_reason = "spambot_restriction_no_expiry"
                    account.cooldown_until = utc_now()
                    account.spambot_checked_at = None
                    account.last_error = "@SpamBot: прежнее ограничение требует новой проверки"
            else:
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
                    is_premium=converted.is_premium,
                    premium_checked_at=(utc_now() if converted.is_premium is not None else None),
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
                event_type=("account.session_restored" if restored else "account.session_imported"),
                entity_type="account",
                entity_id=account.id,
                details={"label": account.label, "source": source.value},
                actor="control-bot",
            )
            await sync_premium_account_group(session, actor="account-import")
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
        expected_session_fingerprint: str | None = None,
    ) -> AccountCheckResult:
        telegram_user_id = getattr(me, "id", None)
        first_name = getattr(me, "first_name", None)
        last_name = getattr(me, "last_name", None)
        phone = getattr(me, "phone", None)
        username = getattr(me, "username", None)
        is_premium = _telegram_premium_value(me)
        checked_at = utc_now()
        recovered_expired_cooldown = False
        async with self._session_factory() as session:
            account = await self._account_for_observation(
                session, account_id, expected_session_fingerprint
            )
            if account is None:
                raise NotFoundError("account not found")
            if authorized is True:
                try:
                    _require_profile_identity(account, me)
                except AccountIdentityError as exc:
                    authorized = None
                    error = str(exc)
                    failure_code = "account_identity_mismatch"
                    telegram_user_id = phone = username = None
                    account.status = AccountStatus.PAUSED
                    account.cooldown_reason = failure_code
            if authorized is not None:
                account.authorized = authorized
            account.last_checked_at = checked_at
            account.last_error = None if authorized is True else (error or "не авторизован")[:500]
            if authorized is False and failure_code in {"account_dead", "account_unauthorized"}:
                account.status = AccountStatus.DISABLED
                account.cooldown_reason = failure_code
            if authorized is True:
                # Older runs could leave a successfully connected account in
                # ``disabled`` after a temporary FloodWait had already
                # elapsed.  A fresh successful session check is sufficient
                # evidence to restore only those temporary states; definitive
                # Telegram/@SpamBot bans remain disabled.
                cooldown_until = ensure_utc(account.cooldown_until)
                if (
                    account.status in {AccountStatus.DISABLED, AccountStatus.PAUSED}
                    and account.spambot_status
                    not in {
                        "temporary_restriction",
                        "active_restriction",
                        "restriction_unclear",
                        "permanent_restriction",
                    }
                    and account.cooldown_reason
                    in {
                        "account_restricted",
                        "flood_escalation",
                        "flood_wait",
                        "peer_flood",
                        "account_identity_mismatch",
                    }
                    and (cooldown_until is None or cooldown_until <= checked_at)
                ):
                    account.status = (
                        AccountStatus.PAUSED if account.operator_paused else AccountStatus.ACTIVE
                    )
                    account.cooldown_until = None
                    account.cooldown_reason = None
                    recovered_expired_cooldown = True
                if telegram_user_id is not None and account.telegram_user_id is None:
                    account.telegram_user_id = telegram_user_id
                if phone:
                    account.phone = str(phone)[:32]
                account.first_name = str(first_name)[:64] if first_name else None
                account.last_name = str(last_name)[:64] if last_name else None
                account.username = str(username)[:64] if username else None
                account.has_profile_photo = getattr(me, "photo", None) is not None
                observed_about = getattr(me, "_proxima_observed_about", None)
                if isinstance(observed_about, str):
                    account.about = observed_about
                if is_premium is not None:
                    account.is_premium = is_premium
                    account.premium_checked_at = checked_at
            add_audit_event(
                session,
                event_type="account.check_succeeded" if authorized else "account.check_failed",
                entity_type="account",
                entity_id=account_id,
                details={
                    "authorized": authorized,
                    "authorization_known": authorized is not None,
                    "failure_code": failure_code,
                    "recovered_expired_cooldown": recovered_expired_cooldown,
                },
                actor=actor,
            )
            await sync_premium_account_group(session, actor=actor)
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
            failure_code=failure_code,
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
        expected_session_fingerprint: str | None = None,
    ) -> Account:
        async with self._session_factory() as session:
            account = await self._account_for_observation(
                session, account_id, expected_session_fingerprint
            )
            if account is None:
                raise NotFoundError("account not found")
            _require_profile_identity(account, me)
            account.first_name = _optional_profile_value(getattr(me, "first_name", None))
            account.last_name = _optional_profile_value(getattr(me, "last_name", None))
            account.username = _optional_profile_value(getattr(me, "username", None))
            if about_override is not None:
                account.about = about_override
            phone = getattr(me, "phone", None)
            account.phone = str(phone)[:32] if phone else account.phone
            account.authorized = True
            checked_at = utc_now()
            account.last_checked_at = checked_at
            account.last_error = None
            account.has_profile_photo = (
                photo_override
                if photo_override is not None
                else getattr(me, "photo", None) is not None
            )
            is_premium = _telegram_premium_value(me)
            if is_premium is not None:
                account.is_premium = is_premium
                account.premium_checked_at = checked_at
            add_audit_event(
                session,
                event_type=event_type,
                entity_type="account",
                entity_id=account_id,
                details=details,
                actor=actor,
            )
            await sync_premium_account_group(session, actor=actor)
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
        except (AccountOperationError, AccountSessionChangedError) as exc:
            raise AccountProfileError(str(exc)) from exc

    async def _observation_fingerprint(self, account_id: int) -> str:
        return _session_fingerprint(await self.get_account(account_id))

    async def _account_for_observation(
        self,
        session: AsyncSession,
        account_id: int,
        expected: str | None,
    ) -> Account:
        account = await locked_account(session, account_id)
        if account is None:
            raise NotFoundError("account not found")
        if expected is not None and _session_fingerprint(account) != expected:
            raise AccountSessionChangedError(
                "Сессия изменилась во время операции. Проверьте аккаунт заново"
            )
        return account

    async def _record_operation_failure(
        self, account_id: int, error: BaseException, expected: str
    ) -> None:
        code = _account_check_failure_code(error)
        if code not in {
            "flood_wait",
            "account_dead",
            "account_unauthorized",
            "account_identity_mismatch",
        }:
            return
        origin = error
        while origin.__cause__ is not None:
            origin = origin.__cause__
        async with self._session_factory() as session:
            account = await locked_account(session, account_id)
            if account is None or _session_fingerprint(account) != expected:
                return
            if code == "flood_wait":
                seconds = (
                    getattr(error, "retry_after", None)
                    or getattr(origin, "retry_after", None)
                    or getattr(origin, "seconds", 0)
                    or 0
                )
                apply_flood_pause(
                    account, float(seconds), minimum=self._settings.flood_min_pause_seconds
                )
            elif code == "account_identity_mismatch":
                account.status = AccountStatus.PAUSED
                account.cooldown_reason = code
                account.last_error = _safe_account_operation_error(error)[:500]
            else:
                account.status = AccountStatus.DISABLED
                account.authorized = False
                account.cooldown_reason = code
                account.last_error = _safe_account_operation_error(error)[:500]
            add_audit_event(
                session,
                event_type="account.operation_failed",
                entity_type="account",
                entity_id=account_id,
                details={"failure_code": code},
                actor="account-service",
            )
            await session.commit()

    async def _run_account_operation(
        self,
        account_id: int,
        operation: Callable[[object], Awaitable[object]],
    ) -> tuple[object, object]:
        expected = await self._observation_fingerprint(account_id)

        async def guarded_operation(client: object) -> object:
            account = await self.get_account(account_id)
            if _session_fingerprint(account) != expected:
                raise AccountSessionChangedError(
                    "Сессия изменилась. Повторите операцию из новой карточки"
                )
            cooldown = ensure_utc(account.cooldown_until)
            if account.cooldown_reason == "flood_wait" and cooldown and cooldown > utc_now():
                raise AccountOperationError(
                    "Telegram FloodWait: дождитесь окончания защитной паузы"
                )
            if account.telegram_user_id is not None:
                me = await client.get_me()  # type: ignore[attr-defined]
                if me is None:
                    raise PermanentGatewayError(
                        "сессия аккаунта недействительна", code="account_unauthorized"
                    )
                _require_profile_identity(account, me)
            return await operation(client)

        try:
            async with asyncio.timeout(max(60, self._settings.account_connect_timeout_seconds * 3)):
                if self._client_coordinator is not None:
                    return await self._perform_account_operation(account_id, guarded_operation)
                async with self._operation_leases.hold(account_id) as token:
                    if token is None:
                        raise AccountOperationError(
                            "Аккаунт сейчас выполняет другую операцию. Повторите позже"
                        )
                    return await self._perform_account_operation(account_id, guarded_operation)
        except Exception as exc:  # noqa: BLE001 - translate every operational failure safely
            await self._record_operation_failure(account_id, exc, expected)
            if isinstance(exc, (AccountOperationError, AccountSessionChangedError, NotFoundError)):
                raise
            if isinstance(exc, TimeoutError):
                raise AccountOperationError(
                    "Telegram не подтвердил результат вовремя. "
                    "Проверьте профиль перед повторением операции"
                ) from exc
            raise AccountOperationError(_safe_account_operation_error(exc)) from exc

    async def _perform_account_operation(
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
        transport = TelethonTransport(proxy=None)
        if self._proxy_service is not None:
            try:
                recovery = getattr(self._proxy_service, "ensure_account_route", None)
                route = (
                    await recovery(account_id, actor="account-profile-operation")
                    if callable(recovery)
                    else await self._proxy_service.require_account_route(account_id)
                )
                transport = build_telethon_transport(route)
            except ProxyRouteUnavailable as exc:
                if self._settings.account_require_proxy:
                    raise AccountOperationError(
                        "для операции не найден рабочий прокси; добавьте его в разделе «Прокси»"
                    ) from exc
        elif self._settings.account_require_proxy:
            raise AccountOperationError("сервис прокси не настроен")

        client = TelegramClient(
            StringSession(decrypted.session_string),
            decrypted.api_id,
            decrypted.api_hash,
            **transport.client_kwargs(),
            connection_retries=1,
            timeout=int(self._settings.account_connect_timeout_seconds),
            request_retries=0,
            flood_sleep_threshold=0,
            raise_last_call_error=True,
        )
        try:
            await asyncio.wait_for(
                client.connect(), timeout=self._settings.account_connect_timeout_seconds
            )
            if not await client.is_user_authorized():
                raise PermanentGatewayError(
                    "сессия аккаунта недействительна", code="account_unauthorized"
                )
            result = await operation(client)
            me = await client.get_me()
            if me is None:
                raise PermanentGatewayError(
                    "Telegram не вернул профиль аккаунта", code="account_unauthorized"
                )
            return me, result
        except AccountOperationError:
            raise
        except Exception as exc:
            raise AccountOperationError(_safe_account_operation_error(exc)) from exc
        finally:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                logger.debug("profile client disconnect failed", exc_info=True)

    async def _fetch_me(
        self,
        decrypted: DecryptedSession,
        transport: TelethonTransport,
    ) -> object | None:
        async def inspect_profile(client: object) -> str | None:
            try:
                return await _read_profile_about(client)
            except Exception as exc:  # noqa: BLE001 - a bio read failure does not revoke a session
                if _account_check_failure_code(exc) in {
                    "flood_wait",
                    "account_dead",
                    "account_unauthorized",
                }:
                    raise
                return None

        me, about = await self._run_account_operation(decrypted.account_id, inspect_profile)
        if isinstance(about, str):
            me._proxima_observed_about = about  # type: ignore[attr-defined]
        return me

    def _require_api(self) -> tuple[int, str]:
        api = self._settings.telegram_api
        if api is None:
            raise AccountImportError(
                "для импорта аккаунта нужны PROXIMA_TELEGRAM_API_ID и PROXIMA_TELEGRAM_API_HASH"
            )
        return api

    def _require_dedicated_account_encryption_key(self) -> None:
        """Prevent new Telegram sessions from using the public dev fallback key."""

        key = self._settings.account_encryption_key
        if key is None or len(key.get_secret_value()) < 16:
            raise AccountImportError(
                "для импорта сессий задайте отдельный PROXIMA_ACCOUNT_ENCRYPTION_KEY "
                "длиной не менее 16 символов"
            )

    def _configured_two_factor_password(self, supplied: str | None) -> str | None:
        if supplied is not None:
            return supplied.strip() or None
        configured = self._settings.tdata_two_factor_password
        if configured is None:
            return None
        return configured.get_secret_value().strip() or None

    def _import_transport(self, route: ProxyNetworkRoute | None) -> TelethonTransport:
        if route is None:
            if self._settings.account_require_proxy:
                raise AccountImportError("для импорта аккаунта нужен назначенный рабочий прокси")
            return TelethonTransport(proxy=None)
        return build_telethon_transport(route)

    def _import_proxy(self, route: ProxyNetworkRoute | None) -> object | None:
        """Backward-compatible proxy-only view for integrations using this helper."""

        return self._import_transport(route).proxy

    # ----------------------------------------------------- blocking converters
    def _convert_tdata_archive(
        self,
        archive_bytes: bytes,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
        connection: object | None = None,
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
                    **_connection_kwargs(TelethonTransport(proxy, connection)),
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
        connection: object | None = None,
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
                **_connection_kwargs(TelethonTransport(proxy, connection)),
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
            **_connection_kwargs(TelethonTransport(proxy, connection)),
        )

    async def _convert_existing_session(
        self,
        source_session: str,
        *,
        api_id: int,
        api_hash: str,
        proxy: object | None,
        two_factor_password: str | None,
        connection: object | None = None,
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
                **_connection_kwargs(TelethonTransport(proxy, connection)),
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
            **_connection_kwargs(TelethonTransport(proxy, connection)),
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
        connection: object | None = None,
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
                    **_connection_kwargs(TelethonTransport(proxy, connection)),
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
                is_premium=_telegram_premium_value(me),
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


def build_telethon_transport(route: object) -> TelethonTransport:
    """Map a stored route to both Telethon proxy data and connection type."""

    protocol = route.protocol
    value = getattr(protocol, "value", str(protocol))
    if value == "mtproto":
        secret = getattr(route, "password", None)
        if not isinstance(secret, str) or not secret:
            raise ProxyRouteUnavailable("у MTProto-прокси отсутствует secret")
        return TelethonTransport(
            proxy=(route.host, int(route.port), secret),
            connection=mtproto_connection_type(secret),
        )
    return TelethonTransport(proxy=_telethon_proxy(route))


def _connection_kwargs(transport: TelethonTransport) -> dict[str, object]:
    return {"connection": transport.connection} if transport.connection is not None else {}


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
    elif value == "mtproto":
        raise ProxyRouteUnavailable("для MTProto нужен специальный транспорт Telethon")
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
    """Persist account-check errors without exposing proxy credentials."""

    return safe_error_message(error)[:500]


def _account_check_failure_code(error: BaseException) -> str:
    """Separate a dead authorization from a temporary connectivity failure."""
    origin = error
    while origin.__cause__ is not None:
        origin = origin.__cause__
    if isinstance(origin, AccountIdentityError):
        return "account_identity_mismatch"
    origin_name = type(origin).__name__
    origin_message = str(getattr(origin, "message", "") or "").upper()
    # API credentials and temporary key binding are not evidence of a revoked
    # user session, even if an older gateway called their wrapper account_dead.
    if (
        origin_name
        in {
            "ApiIdPublishedFloodError",
            "ApiIdInvalidError",
            "AuthKeyPermEmptyError",
            "PremiumCurrentlyUnavailableError",
        }
        or origin_message
        in {
            "API_ID_PUBLISHED_FLOOD",
            "API_ID_INVALID",
            "AUTH_KEY_PERM_EMPTY",
            "AUTH_KEY_UNSYNCHRONIZED",
        }
        or (
            origin_name == "AuthKeyError"
            and origin_message
            not in {
                "AUTH_KEY_DUPLICATED",
                "AUTH_KEY_INVALID",
                "AUTH_KEY_UNREGISTERED",
            }
        )
    ):
        return "account_check_unavailable"
    if origin_name in {
        "FloodWaitError",
        "FloodPremiumWaitError",
        "FloodTestPhoneWaitError",
    } or origin_message.startswith("FLOOD_WAIT"):
        return "flood_wait"
    if isinstance(error, GatewayError):
        return error.code or "account_check_unavailable"
    if isinstance(origin, GatewayError):
        return origin.code or "account_check_unavailable"
    error_name = type(origin).__name__
    rpc_message = str(getattr(origin, "message", "") or "").upper()
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


def _session_fingerprint(account: object) -> str:
    return session_fingerprint(account)


async def _read_profile_about(client: object) -> str:
    from telethon.tl.functions.users import GetFullUserRequest  # noqa: PLC0415
    from telethon.tl.types import InputUserSelf  # noqa: PLC0415

    response = await client(GetFullUserRequest(id=InputUserSelf()))  # type: ignore[operator]
    full_user = getattr(response, "full_user", None)
    if full_user is None:
        raise AccountProfileError("Telegram не подтвердил описание профиля")
    return str(getattr(full_user, "about", None) or "")


def _require_profile_identity(account: Account, me: object) -> None:
    observed_id = getattr(me, "id", None)
    if (
        account.telegram_user_id is not None
        and observed_id is not None
        and account.telegram_user_id != observed_id
    ):
        raise AccountIdentityError("Telegram вернул другой аккаунт. Проверьте загруженную сессию")


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


def _telegram_premium_value(profile: object | None) -> bool | None:
    """Return Telegram's value only when the response actually includes it."""
    if profile is None:
        return None
    value = getattr(profile, "premium", None)
    return bool(value) if value is not None else None


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


def build_telethon_proxy(route: object) -> object:
    """Public wrapper so the live gateway can reuse the proxy mapping."""
    return build_telethon_transport(route).proxy
