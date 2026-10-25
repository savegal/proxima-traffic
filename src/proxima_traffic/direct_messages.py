"""Subscriber databases and durable consent-based personal-message campaigns."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import islice
from pathlib import Path
from time import monotonic
from uuid import uuid4

from sqlalchemy import and_, case, delete, func, literal, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import load_only

from proxima_traffic.blacklist_import import (
    normalize_blacklist_username,
    subscriber_blacklisted_condition,
    user_is_blacklisted,
)
from proxima_traffic.config import Settings
from proxima_traffic.content import (
    ContentError,
    render_spintax,
    resolve_media_path,
    validate_telegram_text,
)
from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.direct_actions import (
    AUTO_DELETE_PERIODS,
    DIRECT_SERVICE_ACTIONS,
    service_action_delay_seconds,
)
from proxima_traffic.failure_policy import ACCOUNT_WIDE_CODES, failure_code
from proxima_traffic.gateways import (
    DirectMessageDelivery,
    DirectRecipient,
    DirectServiceActionResult,
    GatewayError,
    MessageGateway,
    PermanentGatewayError,
)
from proxima_traffic.models import (
    Account,
    AuditEvent,
    DirectCampaign,
    DirectCampaignAccount,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    MentionBlacklistEntry,
    MentionBlacklistUsernameEntry,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyStatus,
    Subscriber,
    SubscriberAccountPeer,
    SubscriberInviteCampaign,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    SubscriberSegment,
    SubscriberStats,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import ProxyService
from proxima_traffic.recipient_reports import (
    RecipientReportFilter,
    RecipientReportKind,
    RecipientReportPage,
    list_recipient_report,
)
from proxima_traffic.services import (
    AccountSendDeferred,
    CampaignRunner,
    ConflictError,
    NotFoundError,
    safe_error_message,
)
from proxima_traffic.subscriber_import import (
    SubscriberImportError,
    SubscriberParseStats,
    SubscriberRecord,
    iter_subscriber_records,
)
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)
IMPORT_BATCH_SIZE = 500
# SQLite has one writer.  Commit each bounded batch so an import of a large
# subscriber export does not block delivery queues and control-panel writes
# behind a multi-thousand-row transaction.
PRIMARY_SUBSCRIBER_LIST_NAME = "Общая база подписчиков"
IMPORT_COMMIT_EVERY_BATCHES = 1
# SQLite builds may allow a different maximum number of bound variables. Keep every
# ``IN (...)`` query below the most conservative common limit instead of relying on
# the local SQLite build used during development.
SQLITE_PARAMETER_BATCH_SIZE = 500
DIRECT_MESSAGE_MAX_RECIPIENTS = 1_000_000
DIRECT_UNCERTAIN_CODES = (
    "delivery_unconfirmed",
    "delivery_outcome_unknown",
    "cancelled_outcome_unknown",
    "recipient_identity_mismatch",
)
DIRECT_MESSAGE_TERMINAL_STATUSES = frozenset(
    {
        DirectMessageTaskStatus.SUCCEEDED,
        DirectMessageTaskStatus.FAILED,
        DirectMessageTaskStatus.SKIPPED,
        DirectMessageTaskStatus.CANCELLED,
    }
)
DIRECT_RECIPIENT_GLOBAL_SKIP_CODES = frozenset(
    {
        "recipient_identity_mismatch",
        "recipient_paid_message_required",
        "recipient_content_forbidden",
        "recipient_unavailable",
    }
)
DIRECT_RECIPIENT_QUARANTINE_CODES = frozenset(
    {"recipient_unavailable", "recipient_paid_message_required", "recipient_identity_mismatch"}
)


def _parameter_batches(values: Iterable[int]) -> Iterator[tuple[int, ...]]:
    """Yield SQL-safe batches without materialising another copy of a large ID set."""

    iterator = iter(values)
    while batch := tuple(islice(iterator, SQLITE_PARAMETER_BATCH_SIZE)):
        yield batch


DIRECT_RECIPIENT_ROUTE_CODES = frozenset(
    {
        "recipient_privacy_restricted",
        "recipient_requires_premium",
        "recipient_route_blocked",
        "recipient_unresolvable",
    }
)
# A recipient-level refusal is final for that person in this launch, but it
# must not silently reduce the operator's requested audience.  We can safely
# reserve a different, previously untried person from the same snapshot.
DIRECT_RECIPIENT_REPLACEMENT_CODES = (
    DIRECT_RECIPIENT_GLOBAL_SKIP_CODES
    | DIRECT_RECIPIENT_ROUTE_CODES
    | frozenset({"recipient_opted_out", "recipient_blacklisted"})
)
DIRECT_CONTENT_FAILURE_CODES = frozenset(
    {"content_forbidden", "content_invalid", "media_unavailable"}
)
TELEGRAM_USER_ID_MAX = (1 << 63) - 1


def parse_direct_blacklist_ids(value: str, *, max_ids: int = 500) -> tuple[int, ...]:
    """Parse a pasted comma/semicolon/whitespace-separated Telegram ID list."""

    tokens = [token for token in re.split(r"[\s,;]+", value.strip()) if token]
    if not tokens:
        raise ValueError("укажите хотя бы один Telegram ID")
    if len(tokens) > max_ids:
        raise ValueError(f"за один раз можно добавить не больше {max_ids} ID")
    values: list[int] = []
    seen: set[int] = set()
    for token in tokens:
        try:
            telegram_user_id = int(token)
        except ValueError as exc:
            raise ValueError(f"некорректный Telegram ID: {token[:30]}") from exc
        if not 1 <= telegram_user_id <= TELEGRAM_USER_ID_MAX:
            raise ValueError(f"Telegram ID вне допустимого диапазона: {token[:30]}")
        if telegram_user_id not in seen:
            seen.add(telegram_user_id)
            values.append(telegram_user_id)
    return tuple(values)


def _normalize_blacklist_ids(values: Sequence[int]) -> tuple[int, ...]:
    normalized: dict[int, None] = {}
    for value in values:
        try:
            user_id = int(value)
        except (TypeError, ValueError) as exc:
            raise ConflictError("стоп-лист содержит некорректный Telegram ID") from exc
        if not 1 <= user_id <= TELEGRAM_USER_ID_MAX:
            raise ConflictError("стоп-лист содержит Telegram ID вне допустимого диапазона")
        normalized[user_id] = None
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class SubscriberImportResult:
    subscriber_list: SubscriberList
    total_lines: int
    valid_records: int
    added_members: int
    inserted_profiles: int
    updated_profiles: int
    duplicates: int
    invalid: int
    issues: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class SubscriberImportProgress:
    total_lines: int
    valid_records: int
    added_members: int
    updated_profiles: int
    duplicates: int
    invalid: int


@dataclass(frozen=True, slots=True)
class DirectBlacklistAddResult:
    requested: int
    added: int
    already_present: int
    cancelled_tasks: int
    cancelled_invite_tasks: int = 0
    affected_invite_launch_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class DirectMessageDashboard:
    list_count: int
    subscriber_count: int
    premium_count: int
    username_count: int
    blacklist_count: int
    campaign_count: int
    active_launches: int
    queued_tasks: int
    running_tasks: int
    succeeded_tasks: int
    failed_tasks: int
    skipped_tasks: int
    uncertain_tasks: int = 0
    simulated: bool = False
    simulated_succeeded_tasks: int = 0
    unknown_mode_succeeded_tasks: int = 0
    quarantined_recipients: int = 0
    service_succeeded_tasks: int = 0
    service_simulated_tasks: int = 0


@dataclass(frozen=True, slots=True)
class DirectCampaignView:
    campaign: DirectCampaign
    list_name: str
    account_ids: tuple[int, ...]
    account_labels: tuple[str, ...]
    eligible_count: int
    recent_count: int
    reserved_count: int
    username_count: int


@dataclass(frozen=True, slots=True)
class DirectLaunchView:
    launch: DirectLaunch
    campaign_name: str
    list_name: str
    queued: int
    running: int
    succeeded: int
    failed: int
    skipped: int
    cancelled: int
    replacement_tasks: int = 0
    uncertain: int = 0
    simulated: bool | None = None
    next_scheduled_for: datetime | None = None
    last_error_code: str | None = None
    last_error: str | None = None
    last_error_status: DirectMessageTaskStatus | None = None
    last_error_account_label: str | None = None
    last_error_recipient: str | None = None
    sender_states: tuple[DirectSenderState, ...] = ()
    recent_failures: tuple[dict[str, object], ...] = ()
    issues: tuple[tuple[DirectMessageTaskStatus, str, int], ...] = ()
    last_success_at: datetime | None = None
    service_notifications: int = 0

    @property
    def active(self) -> int:
        return self.queued + self.running


@dataclass(frozen=True, slots=True)
class DirectSenderState:
    account_id: int
    label: str
    succeeded: int
    reason: str | None
    resume_at: datetime | None
    blocked_recipients: int = 0
    cooldown_reason: str | None = None
    spambot_status: str | None = None
    spambot_checked_at: datetime | None = None


class DirectMessageService:
    """Owns imports, recipient reservations, delivery tasks and reporting."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        worker_id: str,
        proxy_service: ProxyService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._worker_id = worker_id
        self._proxy_service = proxy_service
        self._stop_event = asyncio.Event()
        self._import_lock = asyncio.Lock()
        self._running = False
        self._active_steps = 0

    async def import_subscribers(
        self,
        path: str | Path,
        *,
        list_name: str,
        source_filename: str,
        on_progress: Callable[[SubscriberImportProgress], Awaitable[None]] | None = None,
    ) -> SubscriberImportResult:
        if self._import_lock.locked():
            raise ConflictError("импорт общей базы уже идёт; дождитесь его завершения")
        async with self._import_lock:
            return await self._import_subscribers_locked(
                path,
                list_name=list_name,
                source_filename=source_filename,
                on_progress=on_progress,
            )

    async def _import_subscribers_locked(
        self,
        path: str | Path,
        *,
        list_name: str,
        source_filename: str,
        on_progress: Callable[[SubscriberImportProgress], Awaitable[None]] | None,
    ) -> SubscriberImportResult:
        normalized_name = " ".join(list_name.split()).strip()
        if not 1 <= len(normalized_name) <= 150:
            raise SubscriberImportError("название базы должно содержать от 1 до 150 символов")
        stats = SubscriberParseStats()
        inserted_profiles = 0
        updated_profiles = 0
        added_members = 0
        now = utc_now()
        list_id: int | None = None
        last_progress_at = monotonic()

        async def report_progress() -> None:
            nonlocal last_progress_at
            if on_progress is None or monotonic() - last_progress_at < 2:
                return
            await on_progress(
                SubscriberImportProgress(
                    total_lines=stats.total_lines,
                    valid_records=stats.valid_records,
                    added_members=added_members,
                    updated_profiles=updated_profiles,
                    duplicates=stats.duplicates,
                    invalid=stats.invalid,
                )
            )
            last_progress_at = monotonic()

        try:
            async with self._session_factory() as session:
                subscriber_list = await session.scalar(
                    select(SubscriberList).where(SubscriberList.name == normalized_name)
                )
                if subscriber_list is None:
                    subscriber_list = SubscriberList(
                        name=normalized_name,
                        source_filename=Path(source_filename).name[:255],
                        status=SubscriberListStatus.IMPORTING,
                    )
                    session.add(subscriber_list)
                    await session.flush()
                else:
                    if not subscriber_list.member_count:
                        subscriber_list.status = SubscriberListStatus.IMPORTING
                    subscriber_list.source_filename = Path(source_filename).name[:255]
                    subscriber_list.last_error = None
                list_id = int(subscriber_list.id)

                batch: list[SubscriberRecord] = []
                affected_list_ids: set[int] = {list_id}
                imported_batches = 0
                for record in iter_subscriber_records(
                    path, stats=stats, progress_interval_lines=5_000
                ):
                    if record is None:
                        await report_progress()
                        await asyncio.sleep(0)
                        continue
                    batch.append(record)
                    if len(batch) < IMPORT_BATCH_SIZE:
                        continue
                    inserted, updated, members, affected_ids = await self._import_batch(
                        session,
                        list_id=list_id,
                        records=batch,
                        imported_at=now,
                    )
                    inserted_profiles += inserted
                    updated_profiles += updated
                    added_members += members
                    affected_list_ids.update(affected_ids)
                    batch.clear()
                    imported_batches += 1
                    if imported_batches % IMPORT_COMMIT_EVERY_BATCHES == 0:
                        await session.commit()
                    await report_progress()
                    await asyncio.sleep(0)
                if batch:
                    inserted, updated, members, affected_ids = await self._import_batch(
                        session,
                        list_id=list_id,
                        records=batch,
                        imported_at=now,
                    )
                    inserted_profiles += inserted
                    updated_profiles += updated
                    added_members += members
                    affected_list_ids.update(affected_ids)
                    imported_batches += 1
                    if imported_batches % IMPORT_COMMIT_EVERY_BATCHES == 0:
                        await session.commit()
                if stats.valid_records == 0:
                    raise SubscriberImportError("в файле нет ни одной корректной записи")

                if added_members or updated_profiles:
                    counts_by_list = await self._refresh_list_counts(
                        session,
                        affected_list_ids,
                    )
                    counts = counts_by_list.get(list_id, (0, 0, 0, 0))
                else:
                    counts = (
                        subscriber_list.member_count,
                        subscriber_list.premium_count,
                        subscriber_list.non_premium_count,
                        subscriber_list.username_count,
                    )
                subscriber_list.status = SubscriberListStatus.READY
                subscriber_list.member_count = counts[0]
                subscriber_list.premium_count = counts[1]
                subscriber_list.non_premium_count = counts[2]
                subscriber_list.username_count = counts[3]
                subscriber_list.last_import_total = stats.total_lines
                subscriber_list.last_import_added = added_members
                subscriber_list.last_import_updated = updated_profiles
                subscriber_list.last_import_duplicates = stats.duplicates
                subscriber_list.last_import_invalid = stats.invalid
                subscriber_list.last_imported_at = now
                subscriber_list.last_error = None
                subscriber_list.updated_at = now
                await session.commit()
                await session.refresh(subscriber_list)
                return SubscriberImportResult(
                    subscriber_list=subscriber_list,
                    total_lines=stats.total_lines,
                    valid_records=stats.valid_records,
                    added_members=added_members,
                    inserted_profiles=inserted_profiles,
                    updated_profiles=updated_profiles,
                    duplicates=stats.duplicates,
                    invalid=stats.invalid,
                    issues=tuple(stats.issues),
                )
        except Exception as exc:
            if list_id is not None:
                await self._mark_import_failed(list_id, exc)
            raise

    async def get_primary_subscriber_list(self) -> SubscriberList:
        """Return the one operator-facing subscriber base, merging legacy lists once.

        Subscriber profiles have always been globally unique by Telegram ID.
        This method makes the UI match that fact without deleting historic list
        rows referenced by completed campaigns and launches.
        """

        now = utc_now()
        async with self._session_factory() as session:
            created_id = await session.scalar(
                sqlite_insert(SubscriberList)
                .values(name=PRIMARY_SUBSCRIBER_LIST_NAME, status=SubscriberListStatus.IMPORTING)
                .on_conflict_do_nothing(index_elements=[SubscriberList.name])
                .returning(SubscriberList.id)
            )
            primary = await session.scalar(
                select(SubscriberList).where(SubscriberList.name == PRIMARY_SUBSCRIBER_LIST_NAME)
            )
            if primary is None:
                raise RuntimeError("Не удалось создать общую базу подписчиков")
            subscriber_stats = await session.get(SubscriberStats, 1)
            global_count = (
                int(subscriber_stats.subscriber_count)
                if subscriber_stats is not None
                else int(await session.scalar(select(func.count(Subscriber.id))) or 0)
            )
            # A new row must commit even while another import keeps it IMPORTING.
            changed = created_id is not None
            if primary.member_count < global_count and not self._import_lock.locked():
                legacy_members = select(
                    literal(int(primary.id)).label("subscriber_list_id"),
                    SubscriberListMember.subscriber_id,
                    SubscriberListMember.imported_at,
                ).where(SubscriberListMember.subscriber_list_id != primary.id)
                await session.execute(
                    sqlite_insert(SubscriberListMember)
                    .from_select(
                        ["subscriber_list_id", "subscriber_id", "imported_at"], legacy_members
                    )
                    .on_conflict_do_nothing(
                        index_elements=[
                            SubscriberListMember.subscriber_list_id,
                            SubscriberListMember.subscriber_id,
                        ]
                    )
                )
                counts = (await self._refresh_list_counts(session, {int(primary.id)})).get(
                    int(primary.id), (0, 0, 0, 0)
                )
                (
                    primary.member_count,
                    primary.premium_count,
                    primary.non_premium_count,
                    primary.username_count,
                ) = counts
                primary.status = SubscriberListStatus.READY
                primary.updated_at = now
                changed = True
            elif (
                primary.status == SubscriberListStatus.IMPORTING
                and primary.member_count == 0
                and not self._import_lock.locked()
            ):
                primary.status = SubscriberListStatus.READY
                changed = True
            if not self._import_lock.locked():
                direct_campaigns = await session.execute(
                    update(DirectCampaign)
                    .where(DirectCampaign.subscriber_list_id != primary.id)
                    .values(subscriber_list_id=primary.id, updated_at=now)
                )
                invite_campaigns = await session.execute(
                    update(SubscriberInviteCampaign)
                    .where(SubscriberInviteCampaign.subscriber_list_id != primary.id)
                    .values(subscriber_list_id=primary.id, updated_at=now)
                )
                changed = changed or bool(direct_campaigns.rowcount or invite_campaigns.rowcount)
            if changed:
                await session.commit()
                await session.refresh(primary)
            return primary

    @staticmethod
    async def _import_batch(
        session: AsyncSession,
        *,
        list_id: int,
        records: Sequence[SubscriberRecord],
        imported_at: datetime,
    ) -> tuple[int, int, int, set[int]]:
        telegram_ids = [item.telegram_user_id for item in records]
        existing_profiles = {
            int(row.telegram_user_id): (row.username, row.is_premium)
            for row in (
                await session.execute(
                    select(
                        Subscriber.telegram_user_id, Subscriber.username, Subscriber.is_premium
                    ).where(Subscriber.telegram_user_id.in_(telegram_ids))
                )
            ).all()
        }
        payload = [
            {
                "telegram_user_id": item.telegram_user_id,
                "username": item.username,
                "is_premium": item.is_premium,
                "premium_checked_at": imported_at if item.is_premium is not None else None,
                "created_at": imported_at,
                "updated_at": imported_at,
            }
            for item in records
        ]
        statement = sqlite_insert(Subscriber).values(payload)
        excluded = statement.excluded
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=[Subscriber.telegram_user_id],
                set_={
                    "username": func.coalesce(excluded.username, Subscriber.username),
                    "is_premium": func.coalesce(excluded.is_premium, Subscriber.is_premium),
                    "premium_checked_at": case(
                        (excluded.is_premium.is_not(None), imported_at),
                        else_=Subscriber.premium_checked_at,
                    ),
                    "updated_at": imported_at,
                },
                where=or_(
                    func.coalesce(excluded.username, Subscriber.username).is_distinct_from(
                        Subscriber.username
                    ),
                    func.coalesce(excluded.is_premium, Subscriber.is_premium).is_distinct_from(
                        Subscriber.is_premium
                    ),
                ),
            )
        )
        profile_rows = list(
            (
                await session.execute(
                    select(Subscriber.id, Subscriber.telegram_user_id).where(
                        Subscriber.telegram_user_id.in_(telegram_ids)
                    )
                )
            ).all()
        )
        subscriber_ids = [int(row.id) for row in profile_rows]
        usernames_by_telegram_id = {record.telegram_user_id: record.username for record in records}
        routable_subscriber_ids = {
            int(row.id)
            for row in profile_rows
            if usernames_by_telegram_id.get(int(row.telegram_user_id)) is not None
        }
        if routable_subscriber_ids:
            await session.execute(
                update(SubscriberAccountPeer)
                .where(
                    SubscriberAccountPeer.subscriber_id.in_(routable_subscriber_ids),
                    SubscriberAccountPeer.blocked_reason == "recipient_unresolvable",
                )
                .values(
                    send_blocked=False,
                    blocked_reason=None,
                    last_error=None,
                    updated_at=imported_at,
                )
            )
        # This query is bounded by ``IMPORT_BATCH_SIZE``. Besides finding
        # existing entries in the imported list it captures other lists whose
        # Premium/username counters may change after a profile refresh.
        membership_rows = list(
            (
                await session.execute(
                    select(
                        SubscriberListMember.subscriber_id,
                        SubscriberListMember.subscriber_list_id,
                    ).where(SubscriberListMember.subscriber_id.in_(subscriber_ids))
                )
            ).all()
        )
        existing_members = {
            int(row.subscriber_id)
            for row in membership_rows
            if int(row.subscriber_list_id) == list_id
        }
        affected_list_ids = {list_id} | {int(row.subscriber_list_id) for row in membership_rows}
        new_member_ids = [item for item in subscriber_ids if item not in existing_members]
        if new_member_ids:
            await session.execute(
                sqlite_insert(SubscriberListMember)
                .values(
                    [
                        {
                            "subscriber_list_id": list_id,
                            "subscriber_id": subscriber_id,
                            "imported_at": imported_at,
                        }
                        for subscriber_id in new_member_ids
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        SubscriberListMember.subscriber_list_id,
                        SubscriberListMember.subscriber_id,
                    ]
                )
            )
        updated = sum(
            1
            for record in records
            if (previous := existing_profiles.get(record.telegram_user_id)) is not None
            and (
                (record.username is not None and record.username != previous[0])
                or (record.is_premium is not None and record.is_premium != previous[1])
            )
        )
        return (
            len(records) - len(existing_profiles),
            updated,
            len(new_member_ids),
            affected_list_ids,
        )

    @staticmethod
    async def _refresh_list_counts_for_subscribers(
        session: AsyncSession,
        subscriber_ids: set[int],
    ) -> dict[int, tuple[int, int, int, int]]:
        """Refresh denormalized counters for every list sharing changed profiles."""

        list_ids: set[int] = set()
        for subscriber_id_batch in _parameter_batches(subscriber_ids):
            list_ids.update(
                int(value)
                for value in await session.scalars(
                    select(SubscriberListMember.subscriber_list_id)
                    .where(SubscriberListMember.subscriber_id.in_(subscriber_id_batch))
                    .distinct()
                )
            )
        return await DirectMessageService._refresh_list_counts(session, list_ids)

    @staticmethod
    async def _refresh_list_counts(
        session: AsyncSession,
        list_ids: set[int],
    ) -> dict[int, tuple[int, int, int, int]]:
        """Refresh list counters while respecting SQLite's parameter limit."""

        if not list_ids:
            return {}
        counts_by_list: dict[int, tuple[int, int, int, int]] = {}
        for list_id_batch in _parameter_batches(list_ids):
            rows = list(
                (
                    await session.execute(
                        select(
                            SubscriberListMember.subscriber_list_id,
                            func.count(Subscriber.id),
                            func.sum(case((Subscriber.is_premium.is_(True), 1), else_=0)),
                            func.sum(case((Subscriber.is_premium.is_(False), 1), else_=0)),
                            func.sum(case((Subscriber.username.is_not(None), 1), else_=0)),
                        )
                        .join(Subscriber, Subscriber.id == SubscriberListMember.subscriber_id)
                        .where(SubscriberListMember.subscriber_list_id.in_(list_id_batch))
                        .group_by(SubscriberListMember.subscriber_list_id)
                    )
                ).all()
            )
            counts_by_list.update(
                {int(row[0]): tuple(int(value or 0) for value in row[1:]) for row in rows}
            )
            lists = list(
                await session.scalars(
                    select(SubscriberList).where(SubscriberList.id.in_(list_id_batch))
                )
            )
            for subscriber_list in lists:
                counts = counts_by_list.get(int(subscriber_list.id), (0, 0, 0, 0))
                (
                    subscriber_list.member_count,
                    subscriber_list.premium_count,
                    subscriber_list.non_premium_count,
                    subscriber_list.username_count,
                ) = counts
        return counts_by_list

    async def _mark_import_failed(self, list_id: int, exc: BaseException) -> None:
        try:
            async with self._session_factory() as session:
                subscriber_list = await session.get(SubscriberList, list_id)
                if subscriber_list is None:
                    return
                counts = (await self._refresh_list_counts(session, {list_id})).get(
                    list_id, (0, 0, 0, 0)
                )
                (
                    subscriber_list.member_count,
                    subscriber_list.premium_count,
                    subscriber_list.non_premium_count,
                    subscriber_list.username_count,
                ) = counts
                subscriber_list.status = (
                    SubscriberListStatus.READY if counts[0] else SubscriberListStatus.FAILED
                )
                subscriber_list.last_error = (
                    str(exc)[:500]
                    if isinstance(exc, SubscriberImportError)
                    else "Не удалось обработать файл. Проверьте формат и повторите импорт."
                )
                subscriber_list.updated_at = utc_now()
                await session.commit()
        except Exception:  # noqa: BLE001 - preserve the original import failure
            logger.exception("Could not persist subscriber import failure")

    async def _task_status_counts(
        self, session: AsyncSession, *, launch_id: int | None = None
    ) -> tuple[dict[DirectMessageTaskStatus, int], int]:
        # Failed and uncertain must come from the same database snapshot. Two
        # reads can straddle a worker completion and produce uncertain > failed.
        statement = select(
            DirectMessageTask.status,
            func.count(DirectMessageTask.id),
            func.sum(case((DirectMessageTask.error_code.in_(DIRECT_UNCERTAIN_CODES), 1), else_=0)),
        )
        if launch_id is not None:
            statement = statement.where(DirectMessageTask.launch_id == launch_id)
        rows = (await session.execute(statement.group_by(DirectMessageTask.status))).all()
        counts: dict[DirectMessageTaskStatus, int] = {}
        uncertain = 0
        for status, count, uncertain_count in rows:
            counts[DirectMessageTaskStatus(status)] = int(count)
            if status == DirectMessageTaskStatus.FAILED:
                uncertain = int(uncertain_count or 0)
        return counts, uncertain

    async def dashboard(self) -> DirectMessageDashboard:
        async with self._session_factory() as session:
            list_count = int(await session.scalar(select(func.count(SubscriberList.id))) or 0)
            campaign_count = int(await session.scalar(select(func.count(DirectCampaign.id))) or 0)
            subscriber_stats = await session.get(SubscriberStats, 1)
            if subscriber_stats is None:
                # A service must normally run only after migrations. Keep a safe
                # fallback for an operator restoring an older snapshot by hand.
                subscriber_row = (
                    await session.execute(
                        select(
                            func.count(Subscriber.id),
                            func.sum(case((Subscriber.is_premium.is_(True), 1), else_=0)),
                            func.sum(case((Subscriber.username.is_not(None), 1), else_=0)),
                        )
                    )
                ).one()
                subscriber_count, premium_count, username_count = (
                    int(value or 0) for value in subscriber_row
                )
            else:
                subscriber_count = int(subscriber_stats.subscriber_count)
                premium_count = int(subscriber_stats.premium_count)
                username_count = int(subscriber_stats.username_count)
            task_counts, uncertain_tasks = await self._task_status_counts(session)
            active_launches = int(
                await session.scalar(
                    select(func.count(DirectLaunch.id)).where(
                        DirectLaunch.status.in_(
                            [DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING]
                        )
                    )
                )
                or 0
            )
            blacklist_count = int(
                await session.scalar(select(func.count(MentionBlacklistEntry.telegram_user_id)))
                or 0
            )
            blacklist_count += int(
                await session.scalar(select(func.count(MentionBlacklistUsernameEntry.username)))
                or 0
            )
            success_rows = (
                await session.execute(
                    select(
                        DirectLaunch.operation_kind_snapshot,
                        DirectLaunch.simulated_snapshot,
                        func.count(DirectMessageTask.id),
                    )
                    .join(DirectMessageTask, DirectMessageTask.launch_id == DirectLaunch.id)
                    .where(DirectMessageTask.status == DirectMessageTaskStatus.SUCCEEDED)
                    .group_by(DirectLaunch.operation_kind_snapshot, DirectLaunch.simulated_snapshot)
                )
            ).all()
            mode_counts: dict[bool | None, int] = {}
            service_mode_counts: dict[bool | None, int] = {}
            for kind, mode, count in success_rows:
                bucket = mode_counts if kind == "message" else service_mode_counts
                bucket[mode] = bucket.get(mode, 0) + int(count)
            return DirectMessageDashboard(
                list_count=list_count,
                subscriber_count=subscriber_count,
                premium_count=premium_count,
                username_count=username_count,
                blacklist_count=blacklist_count,
                campaign_count=campaign_count,
                active_launches=active_launches,
                queued_tasks=int(task_counts.get(DirectMessageTaskStatus.QUEUED, 0)),
                running_tasks=int(task_counts.get(DirectMessageTaskStatus.RUNNING, 0)),
                succeeded_tasks=sum(int(value) for value in mode_counts.values()),
                failed_tasks=int(task_counts.get(DirectMessageTaskStatus.FAILED, 0)),
                skipped_tasks=int(task_counts.get(DirectMessageTaskStatus.SKIPPED, 0)),
                uncertain_tasks=uncertain_tasks,
                simulated=self._settings.dry_run,
                simulated_succeeded_tasks=int(mode_counts.get(True, 0)),
                unknown_mode_succeeded_tasks=int(mode_counts.get(None, 0)),
                service_succeeded_tasks=int(service_mode_counts.get(False, 0)),
                service_simulated_tasks=int(service_mode_counts.get(True, 0)),
                quarantined_recipients=int(
                    await session.scalar(
                        select(func.count(Subscriber.id)).where(
                            Subscriber.direct_quarantine_code.is_not(None)
                        )
                    )
                    or 0
                ),
            )

    async def list_quarantined_recipients(
        self, page: int, page_size: int = 5
    ) -> tuple[list[Subscriber], int]:
        page_size = max(1, min(20, page_size))
        async with self._session_factory() as session:
            condition = Subscriber.direct_quarantine_code.is_not(None)
            total = int(
                await session.scalar(select(func.count(Subscriber.id)).where(condition)) or 0
            )
            rows = list(await session.scalars(
                select(Subscriber).where(condition).order_by(Subscriber.id)
                .offset(max(0, page) * page_size).limit(page_size)
            ))
            return rows, total

    async def recheck_quarantined_recipient(self, subscriber_id: int) -> str:
        """One explicit read-only check; never release on import, cache or send success."""

        if self._settings.dry_run:
            raise ConflictError("Симуляция не проверяет Telegram; карантин сохранён")
        async with self._session_factory() as session:
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None:
                raise NotFoundError("получатель не найден")
            if subscriber.direct_quarantine_code is None:
                return "Получатель уже не находится в карантине"
            if subscriber.reserved_direct_launch_id is not None:
                raise ConflictError(
                    "Получатель занят запуском; повторите проверку после его завершения"
                )
            # Reuse the refusing account. Changing accounts can hide a paid
            # requirement through a contact/privacy exception.
            account = (
                await session.get(Account, subscriber.direct_quarantine_account_id)
                if subscriber.direct_quarantine_account_id is not None else None
            )
            if account is None:
                raise ConflictError("Исходный аккаунт отказа недоступен; карантин сохранён")
            try:
                CampaignRunner._validate_sender(account, require_live_session=True)
            except ConflictError as exc:
                raise ConflictError(
                    "Восстановите сессию и доступ исходного аккаунта; карантин сохранён"
                ) from exc
            if self._sender_ready_at(account) is not None:
                raise ConflictError("Исходный аккаунт на паузе или занят; карантин сохранён")
            if account.id not in await self._healthy_proxy_account_ids(session, [account.id]):
                raise ConflictError("Прокси исходного аккаунта недоступен; карантин сохранён")
            peer = await session.scalar(select(SubscriberAccountPeer).where(
                SubscriberAccountPeer.subscriber_id == subscriber_id,
                SubscriberAccountPeer.account_id == account.id,
            ))
            recipient = DirectRecipient(
                subscriber.telegram_user_id, subscriber.username,
                peer.access_hash if peer is not None else None,
            )
            quarantine_code = subscriber.direct_quarantine_code
            quarantine_at = subscriber.direct_quarantine_at
            last_checked_at = subscriber.direct_quarantine_checked_at
            username = subscriber.username

        started_at = utc_now()
        release = False
        note = "Свежие данные не подтвердили снятие ограничения; карантин сохранён"
        try:
            async def check():
                return await self._gateway.check_direct_recipient(
                    account=account, recipient=recipient
                )

            async with asyncio.timeout(30):
                result = await self._runner.run_account_operation(account.id, check)
            checked_at = ensure_utc(result.checked_at)
            if (
                result.telegram_user_id == recipient.telegram_user_id
                and checked_at is not None
                and started_at <= checked_at <= utc_now()
                and result.available is True
            ):
                if result.paid_message_required is False:
                    release = True
                    note = "Свежие данные Telegram подтвердили получателя без оплаты; карантин снят"
                elif result.paid_message_required is True:
                    note = "Telegram по-прежнему требует оплату Stars; карантин сохранён"
                else:
                    note = (
                        "Получатель подтверждён, но требование оплаты неизвестно; карантин сохранён"
                    )
        except GatewayError as exc:
            note = f"Проверка: {safe_error_message(exc)}; карантин сохранён"[:500]
            if failure_code(exc, "gateway_error") in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(account.id, exc)
        except TimeoutError:
            note = "Telegram не ответил за 30 секунд; карантин сохранён"

        now = utc_now()
        values: dict[str, object] = {
            "direct_quarantine_checked_at": now,
            "direct_quarantine_check_note": note,
        }
        if release:
            values.update(
                direct_quarantine_code=None, direct_quarantine_error=None,
                direct_quarantine_at=None, direct_quarantine_account_id=None,
            )
        async with self._session_factory() as session:
            changed = await session.execute(update(Subscriber).where(
                Subscriber.id == subscriber_id,
                Subscriber.telegram_user_id == recipient.telegram_user_id,
                Subscriber.direct_quarantine_code == quarantine_code,
                Subscriber.direct_quarantine_at == quarantine_at,
                Subscriber.direct_quarantine_account_id == account.id,
                Subscriber.direct_quarantine_checked_at.is_not_distinct_from(last_checked_at),
                Subscriber.username.is_not_distinct_from(username),
                Subscriber.reserved_direct_launch_id.is_(None),
            ).values(**values))
            if changed.rowcount != 1:
                return "Запись изменилась во время проверки; её новое состояние сохранено"
            session.add(AuditEvent(
                actor=self._worker_id, event_type="direct_recipient_quarantine_check",
                entity_type="subscriber", entity_id=str(subscriber_id),
                details_json=json.dumps({
                    "code": quarantine_code, "account_id": account.id,
                    "released": release, "note": note,
                }, ensure_ascii=False),
            ))
            await session.commit()
        return note

    async def add_blacklist_entries(
        self,
        telegram_user_ids: Sequence[int],
        *,
        usernames: Sequence[str] = (),
        source: str = "control_bot_direct_messages",
    ) -> DirectBlacklistAddResult:
        values = _normalize_blacklist_ids(telegram_user_ids)
        try:
            names = tuple(dict.fromkeys(normalize_blacklist_username(name) for name in usernames))
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc
        requested = len(values) + len(names)
        if not requested:
            raise ConflictError("укажите хотя бы один Telegram ID или @username")
        if requested > 500:
            raise ConflictError("за один раз можно добавить не больше 500 записей")
        now = utc_now()
        normalized_source = " ".join(source.split()).strip()[:100] or "manual"
        async with self._session_factory() as session:
            existing = set(
                await session.scalars(
                    select(MentionBlacklistEntry.telegram_user_id).where(
                        MentionBlacklistEntry.telegram_user_id.in_(values)
                    )
                )
            )
            existing_names = set(
                await session.scalars(
                    select(MentionBlacklistUsernameEntry.username).where(
                        MentionBlacklistUsernameEntry.username.in_(names)
                    )
                )
            )
            if values:
                await session.execute(
                    sqlite_insert(MentionBlacklistEntry)
                    .values(
                        [
                            {
                                "telegram_user_id": telegram_user_id,
                                "source": normalized_source,
                                "created_at": now,
                            }
                            for telegram_user_id in values
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=[MentionBlacklistEntry.telegram_user_id])
                )
            if names:
                await session.execute(
                    sqlite_insert(MentionBlacklistUsernameEntry)
                    .values(
                        [
                            {"username": name, "source": normalized_source, "created_at": now}
                            for name in names
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=[MentionBlacklistUsernameEntry.username])
                )
            subscriber_ids = select(Subscriber.id).where(
                or_(
                    Subscriber.telegram_user_id.in_(values),
                    func.lower(Subscriber.username).in_(names),
                )
            )
            affected_rows: list[tuple[int, int]] = []
            affected_invite_rows: list[tuple[int, int]] = []
            if requested:
                affected_rows = [
                    (int(row.launch_id), int(row.subscriber_id))
                    for row in (
                        await session.execute(
                            update(DirectMessageTask)
                            .where(
                                DirectMessageTask.subscriber_id.in_(subscriber_ids),
                                DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                            )
                            .values(
                                status=DirectMessageTaskStatus.SKIPPED,
                                error_code="recipient_blacklisted",
                                error="получатель добавлен в глобальный стоп-лист",
                                finished_at=now,
                                updated_at=now,
                            )
                            .returning(
                                DirectMessageTask.launch_id,
                                DirectMessageTask.subscriber_id,
                            )
                        )
                    ).all()
                ]
                affected_invite_rows = [
                    (int(row.launch_id), int(row.subscriber_id))
                    for row in (
                        await session.execute(
                            update(SubscriberInviteTask)
                            .where(
                                SubscriberInviteTask.subscriber_id.in_(subscriber_ids),
                                SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                            )
                            .values(
                                status=DirectMessageTaskStatus.SKIPPED,
                                error_code="recipient_blacklisted",
                                error="получатель добавлен в глобальный стоп-лист",
                                finished_at=now,
                                updated_at=now,
                            )
                            .returning(
                                SubscriberInviteTask.launch_id,
                                SubscriberInviteTask.subscriber_id,
                            )
                        )
                    ).all()
                ]
                if affected_rows:
                    affected_subscriber_ids = {row[1] for row in affected_rows}
                    affected_launch_ids = {row[0] for row in affected_rows}
                    for batch in _parameter_batches(affected_subscriber_ids):
                        await session.execute(
                            update(Subscriber)
                            .where(
                                Subscriber.id.in_(batch),
                                Subscriber.reserved_direct_launch_id.in_(affected_launch_ids),
                            )
                            .values(
                                reserved_direct_launch_id=None,
                                last_error_code="recipient_blacklisted",
                                last_error="получатель добавлен в глобальный стоп-лист",
                                updated_at=now,
                            )
                        )
                if affected_invite_rows:
                    affected_invite_subscriber_ids = {row[1] for row in affected_invite_rows}
                    affected_invite_launch_ids = {row[0] for row in affected_invite_rows}
                    for batch in _parameter_batches(affected_invite_subscriber_ids):
                        await session.execute(
                            update(Subscriber)
                            .where(
                                Subscriber.id.in_(batch),
                                Subscriber.reserved_invite_launch_id.in_(affected_invite_launch_ids),
                            )
                            .values(
                                reserved_invite_launch_id=None,
                                last_error_code="recipient_blacklisted",
                                last_error="получатель добавлен в глобальный стоп-лист",
                                updated_at=now,
                            )
                        )
            await session.commit()
        for launch_id in sorted({row[0] for row in affected_rows}):
            await self._refresh_launch(launch_id)
        return DirectBlacklistAddResult(
            requested=requested,
            added=requested - len(existing) - len(existing_names),
            already_present=len(existing) + len(existing_names),
            cancelled_tasks=len(affected_rows) + len(affected_invite_rows),
            cancelled_invite_tasks=len(affected_invite_rows),
            affected_invite_launch_ids=tuple(sorted({row[0] for row in affected_invite_rows})),
        )

    async def add_blacklist_entries_bulk(
        self,
        telegram_user_ids: Sequence[int],
        *,
        usernames: Sequence[str] = (),
        source: str = "control_bot_direct_messages",
    ) -> DirectBlacklistAddResult:
        """Validate the entire import before writing bounded ID/username batches."""

        values = _normalize_blacklist_ids(telegram_user_ids)
        try:
            names = tuple(dict.fromkeys(normalize_blacklist_username(name) for name in usernames))
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc
        if not values and not names:
            raise ConflictError("укажите хотя бы один Telegram ID или @username")
        parts = [
            await self.add_blacklist_entries(values[index : index + 500], source=source)
            for index in range(0, len(values), 500)
        ]
        for index in range(0, len(names), 500):
            parts.append(
                await self.add_blacklist_entries(
                    (), usernames=names[index : index + 500], source=source
                )
            )
        return DirectBlacklistAddResult(
            requested=sum(item.requested for item in parts),
            added=sum(item.added for item in parts),
            already_present=sum(item.already_present for item in parts),
            cancelled_tasks=sum(item.cancelled_tasks for item in parts),
            cancelled_invite_tasks=sum(item.cancelled_invite_tasks for item in parts),
            affected_invite_launch_ids=tuple(
                sorted(
                    {launch_id for item in parts for launch_id in item.affected_invite_launch_ids}
                )
            ),
        )

    async def list_subscriber_lists(
        self, page: int, page_size: int
    ) -> tuple[list[SubscriberList], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(SubscriberList.id))) or 0)
            items = list(
                await session.scalars(
                    select(SubscriberList)
                    .order_by(SubscriberList.updated_at.desc(), SubscriberList.id.desc())
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return items, total

    async def list_ready_subscriber_lists(
        self, page: int, page_size: int
    ) -> tuple[list[SubscriberList], int]:
        """Return only lists that can be selected in a new direct campaign."""

        page = max(0, page)
        page_size = max(1, min(50, page_size))
        conditions = (
            SubscriberList.status == SubscriberListStatus.READY,
            SubscriberList.member_count > 0,
        )
        async with self._session_factory() as session:
            total = int(
                await session.scalar(select(func.count(SubscriberList.id)).where(*conditions)) or 0
            )
            items = list(
                await session.scalars(
                    select(SubscriberList)
                    .where(*conditions)
                    .order_by(SubscriberList.updated_at.desc(), SubscriberList.id.desc())
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return items, total

    async def get_subscriber_list(self, list_id: int) -> SubscriberList:
        async with self._session_factory() as session:
            item = await session.get(SubscriberList, list_id)
            if item is None:
                raise NotFoundError("база подписчиков не найдена")
            return item

    async def delete_subscriber_list(self, list_id: int) -> None:
        async with self._session_factory() as session:
            item = await session.get(SubscriberList, list_id)
            if item is None:
                raise NotFoundError("база подписчиков не найдена")
            if item.name == PRIMARY_SUBSCRIBER_LIST_NAME:
                raise ConflictError(
                    "общую базу нельзя удалить: она используется ЛС и приглашениями"
                )
            campaign_count = int(
                await session.scalar(
                    select(func.count(DirectCampaign.id)).where(
                        DirectCampaign.subscriber_list_id == list_id
                    )
                )
                or 0
            )
            if campaign_count:
                raise ConflictError("база используется сохранёнными ЛС-рассылками")
            await session.delete(item)
            await session.flush()
            await session.execute(
                delete(Subscriber).where(
                    ~Subscriber.id.in_(select(SubscriberListMember.subscriber_id))
                )
            )
            await session.commit()

    async def create_campaign(
        self,
        *,
        name: str,
        subscriber_list_id: int,
        segment: SubscriberSegment,
        recipient_limit: int,
        contact_cooldown_days: int,
        account_ids: Sequence[int],
        text: str = "",
        html_text: str | None = None,
        media_path: str | None = None,
        media_kind: str | None = None,
        media_name: str | None = None,
        media_size: int | None = None,
        operation_kind: str = "message",
        auto_delete_seconds: int | None = None,
    ) -> DirectCampaign:
        normalized_name = " ".join(name.split()).strip()
        if not 1 <= len(normalized_name) <= 150:
            raise ConflictError("название ЛС-рассылки должно содержать от 1 до 150 символов")
        if not 1 <= recipient_limit <= DIRECT_MESSAGE_MAX_RECIPIENTS:
            raise ConflictError("количество получателей должно быть от 1 до 1 000 000")
        if not 0 <= contact_cooldown_days <= 3650:
            raise ConflictError("защита от повтора должна быть от 0 до 3650 дней")
        unique_account_ids = tuple(dict.fromkeys(int(value) for value in account_ids if value > 0))
        if not unique_account_ids:
            raise ConflictError("выберите хотя бы один аккаунт-отправитель")
        service_action = operation_kind in DIRECT_SERVICE_ACTIONS
        if operation_kind != "message" and not service_action:
            raise ConflictError("неизвестный тип задачи ЛС")
        if operation_kind == "auto_delete":
            if auto_delete_seconds not in AUTO_DELETE_PERIODS:
                raise ConflictError("выберите таймер: выключен, 1, 7 или 30 дней")
        elif auto_delete_seconds is not None:
            raise ConflictError("таймер допустим только для автоудаления")
        if service_action and (
            text or html_text or media_path or media_kind or media_name or media_size
        ):
            raise ConflictError("сервисное действие создаётся без текста и вложений")
        if service_action and segment not in {SubscriberSegment.ALL, SubscriberSegment.PREMIUM}:
            raise ConflictError("выберите всех получателей или только Premium")
        has_media = media_path is not None
        if not has_media and any(
            value is not None for value in (media_kind, media_name, media_size)
        ):
            raise ConflictError("метаданные вложения нельзя сохранить без самого файла")
        media_without_caption = has_media
        try:
            validate_telegram_text(
                text,
                html_text,
                has_media=has_media,
                allow_empty=media_without_caption or service_action,
            )
            if media_path is not None:
                resolve_media_path(self._settings, media_path)
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        if has_media and media_kind not in {"photo", "video", "document", "voice", "video_note"}:
            raise ConflictError("тип вложения не поддерживается")
        if media_kind == "video_note" and (text or html_text):
            raise ConflictError("к видеокружку нельзя добавить подпись")

        async with self._session_factory() as session:
            subscriber_list = await session.get(SubscriberList, subscriber_list_id)
            if subscriber_list is None or subscriber_list.status != SubscriberListStatus.READY:
                raise ConflictError("выбранная база подписчиков не готова")
            if await session.scalar(
                select(DirectCampaign.id).where(DirectCampaign.name == normalized_name)
            ):
                raise ConflictError("ЛС-рассылка с таким названием уже существует")
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(unique_account_ids)))
            )
            if len(accounts) != len(unique_account_ids):
                raise ConflictError("один из выбранных аккаунтов не найден")
            for account in accounts:
                CampaignRunner._validate_sender(
                    account,
                    require_live_session=not self._settings.dry_run,
                )
                if (
                    operation_kind == "protect_content"
                    and not self._settings.dry_run
                    and account.is_premium is False
                ):
                    raise ConflictError(
                        f"для запрета копирования нужен Premium у аккаунта «{account.label}»"
                    )
            campaign = DirectCampaign(
                name=normalized_name,
                subscriber_list_id=subscriber_list_id,
                segment=segment,
                recipient_limit=recipient_limit,
                contact_cooldown_days=contact_cooldown_days,
                operation_kind=operation_kind,
                auto_delete_seconds=auto_delete_seconds,
                service_account_ids_json=(
                    json.dumps(unique_account_ids) if service_action else None
                ),
                primary_text=text,
                primary_html=html_text,
                primary_media_path=media_path,
                primary_media_kind=media_kind,
                primary_media_name=media_name,
                primary_media_size=media_size,
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                [
                    DirectCampaignAccount(campaign_id=campaign.id, account_id=account_id)
                    for account_id in unique_account_ids
                ]
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def list_campaigns(self, page: int, page_size: int) -> tuple[list[DirectCampaign], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(DirectCampaign.id))) or 0)
            items = list(
                await session.scalars(
                    select(DirectCampaign)
                    .order_by(DirectCampaign.created_at.desc(), DirectCampaign.id.desc())
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return items, total

    async def get_campaign_view(self, campaign_id: int) -> DirectCampaignView:
        async with self._session_factory() as session:
            campaign = await session.get(DirectCampaign, campaign_id)
            if campaign is None:
                raise NotFoundError("ЛС-рассылка не найдена")
            subscriber_list = await session.get(SubscriberList, campaign.subscriber_list_id)
            if subscriber_list is None:
                raise ConflictError("база этой ЛС-рассылки удалена")
            account_rows = list(
                (
                    await session.execute(
                        select(Account.id, Account.label)
                        .join(
                            DirectCampaignAccount,
                            DirectCampaignAccount.account_id == Account.id,
                        )
                        .where(DirectCampaignAccount.campaign_id == campaign.id)
                        .order_by(DirectCampaignAccount.id)
                    )
                ).all()
            )
            eligible, recent, reserved, usernames = await self._eligibility_counts(
                session, campaign
            )
            account_ids = tuple(int(row.id) for row in account_rows)
            account_labels = tuple(str(row.label) for row in account_rows)
            if campaign.operation_kind in DIRECT_SERVICE_ACTIONS:
                account_ids = self._account_ids(campaign.service_account_ids_json or "[]")
                labels = {int(row.id): str(row.label) for row in account_rows}
                account_labels = tuple(
                    labels.get(id_, f"Аккаунт #{id_} · удалён") for id_ in account_ids
                )
            return DirectCampaignView(
                campaign=campaign,
                list_name=subscriber_list.name,
                account_ids=account_ids,
                account_labels=account_labels,
                eligible_count=eligible,
                recent_count=recent,
                reserved_count=reserved,
                username_count=usernames,
            )

    @staticmethod
    def _segment_condition(segment: SubscriberSegment) -> object:
        if segment == SubscriberSegment.PREMIUM:
            return Subscriber.is_premium.is_(True)
        if segment == SubscriberSegment.NON_PREMIUM:
            return Subscriber.is_premium.is_(False)
        return Subscriber.id.is_not(None)

    async def _eligibility_counts(
        self,
        session: AsyncSession,
        campaign: DirectCampaign,
    ) -> tuple[int, int, int, int]:
        cooldown_before = utc_now() - timedelta(days=campaign.contact_cooldown_days)
        uncertain_delivery_before = utc_now() - timedelta(
            days=max(1, campaign.contact_cooldown_days)
        )
        base = [
            SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
            Subscriber.do_not_contact.is_(False),
            Subscriber.direct_quarantine_code.is_(None),
            self._segment_condition(campaign.segment),
            ~subscriber_blacklisted_condition(),
        ]
        if campaign.operation_kind in DIRECT_SERVICE_ACTIONS:
            base.extend((Subscriber.username.is_not(None), Subscriber.username != ""))
        recent_contact_condition = (
            Subscriber.last_contacted_at.is_not(None)
            & (Subscriber.last_contacted_at > cooldown_before)
            if campaign.contact_cooldown_days > 0
            else Subscriber.id.is_(None)
        )
        # A missing delivery receipt needs an independent, non-zero safety
        # window even when the campaign has no normal repeat-contact pause.
        # It expires deterministically instead of turning a transient outcome
        # into a permanent, invisible exclusion.
        uncertain_delivery_condition = Subscriber.last_delivery_uncertain_at.is_not(None) & (
            Subscriber.last_delivery_uncertain_at > uncertain_delivery_before
        )
        recent_condition = or_(
            recent_contact_condition,
            uncertain_delivery_condition,
        )
        row = (
            await session.execute(
                select(
                    func.sum(
                        case(
                            (
                                ~recent_condition & Subscriber.reserved_direct_launch_id.is_(None),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    func.sum(case((recent_condition, 1), else_=0)),
                    func.sum(case((Subscriber.reserved_direct_launch_id.is_not(None), 1), else_=0)),
                    func.sum(case((Subscriber.username.is_not(None), 1), else_=0)),
                )
                .join(
                    SubscriberListMember,
                    SubscriberListMember.subscriber_id == Subscriber.id,
                )
                .where(*base)
            )
        ).one()
        return tuple(int(value or 0) for value in row)  # type: ignore[return-value]

    async def launch_campaign(
        self,
        campaign_id: int,
        *,
        consent_confirmed: bool,
        request_key: str | None = None,
    ) -> DirectLaunch:
        if not consent_confirmed:
            raise ConflictError("нужно подтвердить, что получатели дали согласие на сообщения")
        if request_key is not None and not 1 <= len(request_key) <= 100:
            raise ConflictError("некорректный ключ подтверждения запуска")
        if request_key:
            async with self._session_factory() as session:
                existing = await session.scalar(
                    select(DirectLaunch).where(DirectLaunch.request_key == request_key)
                )
                if existing is not None:
                    if existing.campaign_id != campaign_id:
                        raise ConflictError("подтверждение относится к другой рассылке")
                    return existing
        await self._refresh_live_proxy_routes(campaign_id)
        now = utc_now()
        async with self._session_factory() as session:
            # Acquire SQLite's writer before reading/reserving. Concurrent
            # confirmations must not create two different audiences.
            await session.execute(
                update(DirectCampaign)
                .where(DirectCampaign.id == campaign_id)
                .values(updated_at=DirectCampaign.updated_at)
            )
            if request_key:
                existing = await session.scalar(
                    select(DirectLaunch).where(DirectLaunch.request_key == request_key)
                )
                if existing is not None:
                    if existing.campaign_id != campaign_id:
                        raise ConflictError("подтверждение относится к другой рассылке")
                    return existing
            active_launch = await session.scalar(
                select(DirectLaunch.id)
                .where(
                    DirectLaunch.campaign_id == campaign_id,
                    DirectLaunch.status.in_(
                        (DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING)
                    ),
                )
                .limit(1)
            )
            if active_launch is not None:
                raise ConflictError(f"у этой рассылки уже есть активный запуск #{active_launch}")
            campaign = await session.get(DirectCampaign, campaign_id)
            if campaign is None or not campaign.enabled:
                raise ConflictError("ЛС-рассылка недоступна")
            subscriber_list = await session.get(SubscriberList, campaign.subscriber_list_id)
            if subscriber_list is None or subscriber_list.status != SubscriberListStatus.READY:
                raise ConflictError("база подписчиков не готова")
            account_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(DirectCampaignAccount.account_id)
                    .where(DirectCampaignAccount.campaign_id == campaign.id)
                    .order_by(DirectCampaignAccount.id)
                )
            )
            if campaign.operation_kind in DIRECT_SERVICE_ACTIONS:
                account_ids = self._account_ids(campaign.service_account_ids_json or "[]")
            if not account_ids:
                raise ConflictError("в ЛС-рассылке нет отправителей")
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(account_ids)))
            )
            healthy_proxy_account_ids = await self._healthy_proxy_account_ids(
                session,
                account_ids,
            )
            ready_account_ids: list[int] = []
            missing_proxy_count = 0
            for account in accounts:
                try:
                    CampaignRunner._validate_sender(
                        account,
                        require_live_session=not self._settings.dry_run,
                    )
                except ConflictError:
                    continue
                if int(account.id) not in healthy_proxy_account_ids:
                    missing_proxy_count += 1
                    continue
                ready_account_ids.append(int(account.id))
            if not ready_account_ids:
                if missing_proxy_count:
                    raise ConflictError(
                        "нет готовых аккаунтов-отправителей: обязательный прокси "
                        "недоступен. Проверьте прокси выбранных аккаунтов и повторите запуск"
                    )
                raise ConflictError("нет готовых аккаунтов-отправителей")
            service_action = campaign.operation_kind in DIRECT_SERVICE_ACTIONS
            if service_action and set(ready_account_ids) != set(account_ids):
                raise ConflictError(
                    "не все выбранные аккаунты готовы: проверьте сессии и прокси; "
                    "состав аккаунтов автоматически не изменяется"
                )
            if campaign.operation_kind == "protect_content" and not self._settings.dry_run:
                non_premium = [account.label for account in accounts if account.is_premium is False]
                if non_premium:
                    raise ConflictError(
                        "запрет копирования требует Premium у каждого выбранного аккаунта: "
                        + ", ".join(non_premium[:5])
                    )

            cooldown_before = now - timedelta(days=campaign.contact_cooldown_days)
            uncertain_delivery_before = now - timedelta(days=max(1, campaign.contact_cooldown_days))
            conditions = [
                SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
                Subscriber.do_not_contact.is_(False),
                Subscriber.direct_quarantine_code.is_(None),
                self._segment_condition(campaign.segment),
                ~subscriber_blacklisted_condition(),
                Subscriber.reserved_direct_launch_id.is_(None),
                or_(
                    Subscriber.last_delivery_uncertain_at.is_(None),
                    Subscriber.last_delivery_uncertain_at <= uncertain_delivery_before,
                ),
            ]
            if service_action:
                conditions.extend((Subscriber.username.is_not(None), Subscriber.username != ""))
            if campaign.contact_cooldown_days > 0:
                conditions.append(
                    or_(
                        Subscriber.last_contacted_at.is_(None),
                        Subscriber.last_contacted_at <= cooldown_before,
                    )
                )
            _, recent, reserved, _ = await self._eligibility_counts(session, campaign)
            if not await session.scalar(
                select(Subscriber.id)
                .join(
                    SubscriberListMember,
                    SubscriberListMember.subscriber_id == Subscriber.id,
                )
                .where(*conditions)
                .limit(1)
            ):
                raise ConflictError("в выбранном сегменте нет доступных получателей")
            launch = DirectLaunch(
                request_key=request_key,
                campaign_id=campaign.id,
                subscriber_list_id=campaign.subscriber_list_id,
                status=DirectLaunchStatus.QUEUED,
                segment_snapshot=campaign.segment,
                recipient_limit_snapshot=campaign.recipient_limit,
                contact_cooldown_days_snapshot=campaign.contact_cooldown_days,
                account_ids_snapshot_json=json.dumps(ready_account_ids, separators=(",", ":")),
                simulated_snapshot=self._settings.dry_run,
                operation_kind_snapshot=campaign.operation_kind,
                auto_delete_seconds_snapshot=campaign.auto_delete_seconds,
                primary_text_snapshot=campaign.primary_text,
                primary_html_snapshot=campaign.primary_html,
                primary_media_path_snapshot=campaign.primary_media_path,
                primary_media_kind_snapshot=campaign.primary_media_kind,
                primary_media_name_snapshot=campaign.primary_media_name,
                primary_media_size_snapshot=campaign.primary_media_size,
                total_tasks=0,
                excluded_recent=recent,
                excluded_reserved=reserved,
                consent_confirmed_at=now,
            )
            session.add(launch)
            await session.flush()
            # A separate launch can select the same IDs before either one has
            # reserved them.  Re-read candidates until this launch gets its
            # requested amount or the available segment is exhausted.
            reserved_subscriber_ids: list[int] = []
            reservation_misses = 0
            while len(reserved_subscriber_ids) < campaign.recipient_limit:
                remaining = campaign.recipient_limit - len(reserved_subscriber_ids)
                candidate_batch = list(
                    await session.scalars(
                        select(Subscriber.id)
                        .join(
                            SubscriberListMember,
                            SubscriberListMember.subscriber_id == Subscriber.id,
                        )
                        .where(*conditions)
                        .order_by(
                            case(
                                (
                                    and_(
                                        Subscriber.username.is_not(None),
                                        Subscriber.username != "",
                                    ),
                                    0,
                                ),
                                else_=1,
                            ),
                            case((Subscriber.last_contacted_at.is_(None), 0), else_=1),
                            Subscriber.last_contacted_at,
                            Subscriber.id,
                        )
                        .limit(min(IMPORT_BATCH_SIZE, remaining))
                    )
                )
                if not candidate_batch:
                    break
                claimed_batch = list(
                    await session.scalars(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(candidate_batch),
                            Subscriber.reserved_direct_launch_id.is_(None),
                            Subscriber.direct_quarantine_code.is_(None),
                        )
                        .values(reserved_direct_launch_id=launch.id, updated_at=now)
                        .returning(Subscriber.id)
                    )
                )
                if claimed_batch:
                    reserved_subscriber_ids.extend(int(value) for value in claimed_batch)
                    reservation_misses = 0
                else:
                    reservation_misses += 1
                    if reservation_misses >= 3:
                        break
            if not reserved_subscriber_ids:
                raise ConflictError("получателей уже забронировал другой запуск")
            launch.total_tasks = len(reserved_subscriber_ids)
            launch.excluded_reserved = reserved
            pace_seconds = max(0.0, float(self._settings.account_pace_min_seconds))
            account_count = max(1, len(ready_account_ids))
            account_due = dict.fromkeys(ready_account_ids, now)
            max_attempts = min(
                100,
                max(
                    int(self._settings.worker_max_attempts),
                    len(ready_account_ids) + 2,
                ),
            )
            for offset in range(0, len(reserved_subscriber_ids), IMPORT_BATCH_SIZE):
                task_rows = []
                for index, subscriber_id in enumerate(
                    reserved_subscriber_ids[offset : offset + IMPORT_BATCH_SIZE],
                    start=offset,
                ):
                    wave = index // account_count
                    assigned_sender = ready_account_ids[index % account_count]
                    if service_action:
                        account_due[assigned_sender] += timedelta(
                            seconds=service_action_delay_seconds()
                        )
                    task_rows.append(
                        {
                            "launch_id": launch.id,
                            "subscriber_id": subscriber_id,
                            "status": DirectMessageTaskStatus.QUEUED,
                            "attempt_count": 0,
                            "max_attempts": max_attempts,
                            "sender_account_id": assigned_sender if service_action else None,
                            "scheduled_for": (
                                account_due[assigned_sender]
                                if service_action
                                else now + timedelta(seconds=wave * pace_seconds)
                            ),
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
                await session.execute(sqlite_insert(DirectMessageTask).values(task_rows))
            await session.commit()
            await session.refresh(launch)
            return launch

    async def list_recipients(
        self, *, launch_id: int | None = None, page: int = 0, query: str = "",
        result_filter: RecipientReportFilter = RecipientReportFilter.SUCCESS,
    ) -> RecipientReportPage:
        return await list_recipient_report(
            self._session_factory, kind=RecipientReportKind.DIRECT,
            launch_id=launch_id, page=page, query=query, result_filter=result_filter,
        )

    async def list_subscriber_directory(
        self, *, page: int = 0, query: str = ""
    ) -> RecipientReportPage:
        return await list_recipient_report(
            self._session_factory, kind=RecipientReportKind.DIRECTORY,
            page=page, query=query, result_filter=RecipientReportFilter.ALL,
        )

    async def list_launches(self, page: int, page_size: int) -> tuple[list[DirectLaunchView], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(DirectLaunch.id))) or 0)
            rows = list(
                (
                    await session.execute(
                        select(DirectLaunch, DirectCampaign.name, SubscriberList.name)
                        .join(DirectCampaign, DirectCampaign.id == DirectLaunch.campaign_id)
                        .join(SubscriberList, SubscriberList.id == DirectLaunch.subscriber_list_id)
                        .options(
                            load_only(
                                DirectLaunch.id,
                                DirectLaunch.campaign_id,
                                DirectLaunch.subscriber_list_id,
                                DirectLaunch.status,
                                DirectLaunch.total_tasks,
                                DirectLaunch.simulated_snapshot,
                                DirectLaunch.operation_kind_snapshot,
                                DirectLaunch.created_at,
                            )
                        )
                        .order_by(DirectLaunch.created_at.desc(), DirectLaunch.id.desc())
                        .offset(page * page_size)
                        .limit(page_size)
                    )
                ).all()
            )
            launch_ids = [int(row[0].id) for row in rows]
            counts_by_launch: dict[int, dict[DirectMessageTaskStatus, int]] = {
                launch_id: {} for launch_id in launch_ids
            }
            uncertain_by_launch: dict[int, int] = {}
            if launch_ids:
                count_rows = (
                    await session.execute(
                        select(
                            DirectMessageTask.launch_id,
                            DirectMessageTask.status,
                            func.count(DirectMessageTask.id),
                            func.sum(
                                case(
                                    (DirectMessageTask.error_code.in_(DIRECT_UNCERTAIN_CODES), 1),
                                    else_=0,
                                )
                            ),
                        )
                        .where(DirectMessageTask.launch_id.in_(launch_ids))
                        .group_by(DirectMessageTask.launch_id, DirectMessageTask.status)
                    )
                ).all()
                for launch_id, status, count, uncertain_count in count_rows:
                    counts_by_launch[int(launch_id)][DirectMessageTaskStatus(status)] = int(count)
                    if DirectMessageTaskStatus(status) == DirectMessageTaskStatus.FAILED:
                        uncertain_by_launch[int(launch_id)] = int(uncertain_count or 0)

            views: list[DirectLaunchView] = []
            for launch, campaign_name, list_name in rows:
                counts = counts_by_launch[int(launch.id)]
                task_total = sum(counts.values())
                views.append(
                    DirectLaunchView(
                        launch=launch,
                        campaign_name=str(campaign_name),
                        list_name=str(list_name),
                        queued=counts.get(DirectMessageTaskStatus.QUEUED, 0),
                        running=counts.get(DirectMessageTaskStatus.RUNNING, 0),
                        succeeded=counts.get(DirectMessageTaskStatus.SUCCEEDED, 0),
                        failed=counts.get(DirectMessageTaskStatus.FAILED, 0),
                        skipped=counts.get(DirectMessageTaskStatus.SKIPPED, 0),
                        cancelled=counts.get(DirectMessageTaskStatus.CANCELLED, 0),
                        replacement_tasks=max(0, task_total - int(launch.total_tasks or 0)),
                        uncertain=int(uncertain_by_launch.get(int(launch.id), 0)),
                        simulated=launch.simulated_snapshot,
                    )
                )
            return views, total

    async def get_launch_view(self, launch_id: int) -> DirectLaunchView:
        async with self._session_factory() as session:
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
            return await self._launch_view(session, launch)

    async def _launch_view(self, session: AsyncSession, launch: DirectLaunch) -> DirectLaunchView:
        campaign_name = await session.scalar(
            select(DirectCampaign.name).where(DirectCampaign.id == launch.campaign_id)
        )
        list_name = await session.scalar(
            select(SubscriberList.name).where(SubscriberList.id == launch.subscriber_list_id)
        )
        counts, uncertain = await self._task_status_counts(session, launch_id=launch.id)
        next_scheduled_for = await session.scalar(
            select(func.min(DirectMessageTask.scheduled_for)).where(
                DirectMessageTask.launch_id == launch.id,
                DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
            )
        )
        task_total = sum(int(value or 0) for value in counts.values())
        error_row = (
            await session.execute(
                select(
                    DirectMessageTask.error_code,
                    DirectMessageTask.error,
                    DirectMessageTask.status,
                    Account.label,
                    Subscriber.username,
                    Subscriber.telegram_user_id,
                )
                .join(Subscriber, Subscriber.id == DirectMessageTask.subscriber_id)
                .outerjoin(Account, Account.id == DirectMessageTask.sender_account_id)
                .where(
                    DirectMessageTask.launch_id == launch.id,
                    DirectMessageTask.error_code.is_not(None),
                )
                .order_by(DirectMessageTask.updated_at.desc())
                .limit(1)
            )
        ).first()
        issue_rows = (
            await session.execute(
                select(
                    DirectMessageTask.status,
                    DirectMessageTask.error_code,
                    func.count(DirectMessageTask.id),
                )
                .where(
                    DirectMessageTask.launch_id == launch.id,
                    DirectMessageTask.status.in_(
                        (
                            DirectMessageTaskStatus.QUEUED,
                            DirectMessageTaskStatus.FAILED,
                            DirectMessageTaskStatus.SKIPPED,
                        )
                    ),
                    DirectMessageTask.error_code.is_not(None),
                )
                .group_by(DirectMessageTask.status, DirectMessageTask.error_code)
                .order_by(func.count(DirectMessageTask.id).desc())
                .limit(5)
            )
        ).all()
        selected_account_ids = self._account_ids(launch.account_ids_snapshot_json)
        refused_account_ids = (
            await self._refused_launch_account_ids(session, launch.id)
            if launch.operation_kind_snapshot == "message"
            else set()
        )
        successes_by_account = dict(
            (
                await session.execute(
                    select(DirectMessageTask.sender_account_id, func.count(DirectMessageTask.id))
                    .where(
                        DirectMessageTask.launch_id == launch.id,
                        DirectMessageTask.status == DirectMessageTaskStatus.SUCCEEDED,
                    )
                    .group_by(DirectMessageTask.sender_account_id)
                )
            ).all()
        )
        blocked_by_account = dict(
            (
                await session.execute(
                    select(SubscriberAccountPeer.account_id, func.count(SubscriberAccountPeer.id))
                    .where(
                        SubscriberAccountPeer.account_id.in_(selected_account_ids),
                        SubscriberAccountPeer.send_blocked.is_(True),
                        SubscriberAccountPeer.subscriber_id.in_(
                            select(DirectMessageTask.subscriber_id).where(
                                DirectMessageTask.launch_id == launch.id
                            )
                        ),
                    )
                    .group_by(SubscriberAccountPeer.account_id)
                )
            ).all()
        )
        sender_states = []
        for account in await session.scalars(
            select(Account)
            .where(Account.id.in_(self._account_ids(launch.account_ids_snapshot_json)))
            .order_by(Account.id)
        ):
            reason = "account_restricted" if account.id in refused_account_ids else None
            resume_at = (
                None if account.id in refused_account_ids else self._sender_ready_at(account)
            )
            try:
                CampaignRunner._validate_sender(
                    account, require_live_session=not self._settings.dry_run
                )
            except ConflictError:
                reason = "sender_unavailable"
            if reason is None and resume_at is not None:
                reason = self._sender_wait_code(account, resume_at)
            succeeded = int(successes_by_account.get(account.id, 0))
            blocked = int(blocked_by_account.get(account.id, 0))
            sender_states.append(
                DirectSenderState(
                    account.id,
                    account.label,
                    succeeded,
                    reason,
                    resume_at,
                    blocked,
                    cooldown_reason=account.cooldown_reason,
                    spambot_status=account.spambot_status,
                    spambot_checked_at=ensure_utc(account.spambot_checked_at),
                )
            )
        failures = tuple(
            json.loads(raw)
            for raw in await session.scalars(
                select(AuditEvent.details_json)
                .where(
                    AuditEvent.entity_type == "direct_launch",
                    AuditEvent.entity_id == str(launch.id),
                    AuditEvent.event_type == "direct_message_failure",
                )
                .order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
                .limit(3)
            )
        )
        last_success_at = await session.scalar(
            select(func.max(DirectMessageTask.finished_at)).where(
                DirectMessageTask.launch_id == launch.id,
                DirectMessageTask.status == DirectMessageTaskStatus.SUCCEEDED,
            )
        )
        return DirectLaunchView(
            launch=launch,
            campaign_name=str(campaign_name or f"ЛС-рассылка #{launch.campaign_id}"),
            list_name=str(list_name or f"База #{launch.subscriber_list_id}"),
            queued=int(counts.get(DirectMessageTaskStatus.QUEUED, 0)),
            running=int(counts.get(DirectMessageTaskStatus.RUNNING, 0)),
            succeeded=int(counts.get(DirectMessageTaskStatus.SUCCEEDED, 0)),
            failed=int(counts.get(DirectMessageTaskStatus.FAILED, 0)),
            skipped=int(counts.get(DirectMessageTaskStatus.SKIPPED, 0)),
            cancelled=int(counts.get(DirectMessageTaskStatus.CANCELLED, 0)),
            replacement_tasks=max(0, task_total - int(launch.total_tasks or 0)),
            uncertain=uncertain,
            simulated=launch.simulated_snapshot,
            sender_states=tuple(sender_states),
            recent_failures=failures,
            issues=tuple(
                (DirectMessageTaskStatus(status), str(code), int(count))
                for status, code, count in issue_rows
            ),
            last_success_at=ensure_utc(last_success_at),
            service_notifications=int(
                await session.scalar(
                    select(func.count(DirectMessageTask.id)).where(
                        DirectMessageTask.launch_id == launch.id,
                        DirectMessageTask.status == DirectMessageTaskStatus.SUCCEEDED,
                        DirectMessageTask.result_code == "service_notification_confirmed",
                    )
                )
                or 0
            )
            if launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
            else 0,
            next_scheduled_for=ensure_utc(next_scheduled_for),
            last_error_code=str(error_row.error_code)
            if error_row and error_row.error_code
            else None,
            last_error=str(error_row.error) if error_row and error_row.error else None,
            last_error_status=(
                DirectMessageTaskStatus(error_row.status)
                if error_row and error_row.status
                else None
            ),
            last_error_account_label=(
                str(error_row.label) if error_row and error_row.label else None
            ),
            last_error_recipient=(
                (
                    f"@{error_row.username} · ID {error_row.telegram_user_id}"
                    if error_row.username and error_row.telegram_user_id is not None
                    else (
                        f"@{error_row.username}"
                        if error_row.username
                        else f"ID {error_row.telegram_user_id}"
                    )
                )
                if error_row and (error_row.username or error_row.telegram_user_id is not None)
                else None
            ),
        )

    async def cancel_launch(self, launch_id: int) -> int:
        now = utc_now()
        async with self._session_factory() as session:
            await session.execute(
                update(DirectLaunch)
                .where(DirectLaunch.id == launch_id)
                .values(updated_at=DirectLaunch.updated_at)
            )
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
            if launch.status not in {DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING}:
                return 0
            launch.cancel_requested = True
            queued_subscriber_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(DirectMessageTask.subscriber_id).where(
                        DirectMessageTask.launch_id == launch_id,
                        DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                )
            )
            cancelled = await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.launch_id == launch_id,
                    DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                )
                .values(
                    status=DirectMessageTaskStatus.CANCELLED,
                    error_code="launch_cancelled",
                    error="запуск остановлен оператором до отправки",
                    finished_at=now,
                    updated_at=now,
                )
            )
            if queued_subscriber_ids:
                for batch in _parameter_batches(queued_subscriber_ids):
                    await session.execute(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(batch),
                            Subscriber.reserved_direct_launch_id == launch_id,
                        )
                        .values(reserved_direct_launch_id=None, updated_at=now)
                    )
            running = int(
                await session.scalar(
                    select(func.count(DirectMessageTask.id)).where(
                        DirectMessageTask.launch_id == launch_id,
                        DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                    )
                )
                or 0
            )
            if running == 0:
                launch.status = DirectLaunchStatus.CANCELLED
                launch.finished_at = now
            await session.commit()
            return int(cancelled.rowcount or 0)

    async def process_next(self) -> DirectMessageTask | None:
        task = await self._claim_next()
        if task is None:
            return None
        if task.lease_token is None:
            return await self._finish_task(
                task.id,
                task.lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="lease_expired",
                error="задача не получила токен выполнения",
            )
        lease_token = task.lease_token
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(task.id, lease_token, heartbeat_stop),
            name=f"direct-message-heartbeat-{task.id}",
        )
        self._active_steps += 1
        try:
            async with asyncio.timeout(float(self._settings.worker_step_timeout_seconds)):
                return await self._process_claimed(task.id, lease_token)
        except TimeoutError:
            return await self._handle_interrupted_task(task.id, lease_token, "step_timeout")
        except OperationalError as exc:
            if not is_transient_sqlite_lock(exc):
                raise
            return await self._handle_interrupted_task(task.id, lease_token, "database_busy")
        except Exception:  # noqa: BLE001 - keep the durable worker alive
            logger.exception("Direct message task %s failed unexpectedly", task.id)
            return await self._handle_interrupted_task(
                task.id, lease_token, "unhandled_execution_error"
            )
        finally:
            heartbeat_stop.set()
            await heartbeat
            self._active_steps -= 1

    async def _handle_interrupted_task(
        self, task_id: int, lease_token: str, code: str
    ) -> DirectMessageTask:
        async with self._session_factory() as session:
            task = await session.get(DirectMessageTask, task_id)
            if task is None:
                raise NotFoundError("задача личного сообщения не найдена")
            # A commit can succeed even if refreshing/reporting subsequently fails.
            if task.status in DIRECT_MESSAGE_TERMINAL_STATUSES or task.lease_token != lease_token:
                return task
            dispatch_started = task.send_started_at is not None
            sender_account_id = task.sender_account_id
            exhausted = task.attempt_count + 1 >= task.max_attempts
            launch = await session.get(DirectLaunch, task.launch_id)
            operation_key = self._accounting_key(task, launch)
        if not dispatch_started and not exhausted:
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=utc_now()
                + timedelta(seconds=self._settings.worker_retry_base_seconds),
                error_code=code,
                error="сбой до обращения к Telegram; задача будет повторена",
                increment_attempt=True,
            )
        if dispatch_started and sender_account_id is not None:
            try:
                await self._runner.record_external_account_send(
                    int(sender_account_id), operation_key=operation_key
                )
            except Exception:  # noqa: BLE001 - preserve unknown Telegram outcome
                logger.exception("Could not reserve quota for interrupted direct task %s", task_id)
        return await self._finish_task(
            task_id,
            lease_token,
            status=DirectMessageTaskStatus.FAILED,
            error_code="delivery_outcome_unknown" if dispatch_started else code,
            error=(
                f"отправка прервалась ({code}); результат Telegram неизвестен, повтор запрещён"
                if dispatch_started
                else "исчерпаны попытки подготовки отправки"
            ),
            conservative_cooldown=dispatch_started,
        )

    async def _claim_next(self) -> DirectMessageTask | None:
        now = utc_now()
        lease_token = uuid4().hex
        lease_until = now + timedelta(seconds=max(30, self._settings.worker_lease_seconds))
        recovered_launch_ids: set[int] = set()
        recovered_send_attempts: list[tuple[int, int, str]] = []
        async with self._session_factory() as session:
            # Serialize recovery with claim/renewal before reading expired
            # rows. Otherwise two workers can both recover the same old lease.
            await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                    DirectMessageTask.lease_until <= now,
                )
                .values(updated_at=DirectMessageTask.updated_at)
            )
            # A task that was running when its launch was cancelled cannot be
            # returned to QUEUED: the claim query deliberately excludes the
            # launch, which would otherwise strand both task and recipient
            # reservation forever.  We do not know whether a dead worker had
            # already reached Telegram, so retain an explicit unknown-outcome
            # failure rather than claiming that the message was not sent.
            cancelled_expired_tasks = list(
                await session.scalars(
                    select(DirectMessageTask)
                    .join(DirectLaunch, DirectLaunch.id == DirectMessageTask.launch_id)
                    .where(
                        DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                        DirectMessageTask.lease_until.is_not(None),
                        DirectMessageTask.lease_until <= now,
                        or_(
                            DirectLaunch.cancel_requested.is_(True),
                            DirectMessageTask.send_started_at.is_not(None),
                        ),
                    )
                )
            )
            for task in cancelled_expired_tasks:
                launch = await session.get(DirectLaunch, task.launch_id)
                task.status = DirectMessageTaskStatus.FAILED
                task.error_code = (
                    "cancelled_outcome_unknown"
                    if launch and launch.cancel_requested
                    else "delivery_outcome_unknown"
                )
                task.error = (
                    "worker завершился; результат уже начатой отправки в Telegram неизвестен"
                )
                task.finished_at = now
                self._release_task_lease(task)
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id == task.subscriber_id,
                        Subscriber.reserved_direct_launch_id == task.launch_id,
                    )
                    .values(
                        reserved_direct_launch_id=None,
                        last_delivery_uncertain_at=now,
                        last_error_code=task.error_code,
                        last_error=task.error,
                        updated_at=now,
                    )
                )
                recovered_launch_ids.add(int(task.launch_id))
                if task.send_started_at is not None and task.sender_account_id is not None:
                    recovered_send_attempts.append(
                        (
                            int(task.sender_account_id),
                            int(task.id),
                            self._accounting_key(task, launch),
                        )
                    )
            # Terminal statuses already exclude recovered rows. Flush them
            # before the bulk update instead of binding every cancelled ID.
            await session.flush()
            await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                    DirectMessageTask.lease_until.is_not(None),
                    DirectMessageTask.lease_until <= now,
                    DirectMessageTask.send_started_at.is_(None),
                )
                .values(
                    status=DirectMessageTaskStatus.QUEUED,
                    lease_owner=None,
                    lease_token=None,
                    lease_until=None,
                    error_code="lease_recovered",
                    error="задача восстановлена после прерванного worker",
                    scheduled_for=now,
                    updated_at=now,
                )
            )
            candidate = (
                select(DirectMessageTask.id)
                .join(DirectLaunch, DirectLaunch.id == DirectMessageTask.launch_id)
                .where(
                    DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                    DirectMessageTask.scheduled_for <= now,
                    DirectLaunch.cancel_requested.is_(False),
                )
                .order_by(DirectMessageTask.scheduled_for, DirectMessageTask.id)
                .limit(1)
                .scalar_subquery()
            )
            claimed = await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.id == candidate,
                    DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                )
                .values(
                    status=DirectMessageTaskStatus.RUNNING,
                    lease_owner=self._worker_id,
                    lease_token=lease_token,
                    lease_until=lease_until,
                    started_at=func.coalesce(DirectMessageTask.started_at, now),
                    updated_at=now,
                )
                .returning(DirectMessageTask.id, DirectMessageTask.launch_id)
            )
            claim = claimed.one_or_none()
            task_id = int(claim.id) if claim is not None else None
            if claim is not None:
                await session.execute(
                    update(DirectLaunch)
                    .where(
                        DirectLaunch.id == claim.launch_id,
                        DirectLaunch.status == DirectLaunchStatus.QUEUED,
                    )
                    .values(
                        status=DirectLaunchStatus.RUNNING,
                        started_at=func.coalesce(DirectLaunch.started_at, now),
                        updated_at=now,
                    )
                )
            await session.commit()
        for account_id, recovered_task_id, operation_key in recovered_send_attempts:
            try:
                await self._runner.record_external_account_send(
                    account_id, operation_key=operation_key
                )
            except Exception:  # noqa: BLE001 - unknown delivery remains terminal
                logger.exception(
                    "Could not reserve quota for recovered direct task %s", recovered_task_id
                )
        for launch_id in recovered_launch_ids:
            await self._refresh_launch(launch_id)
        if task_id is None:
            return None
        async with self._session_factory() as session:
            return await session.get(DirectMessageTask, int(task_id))

    async def _process_claimed(self, task_id: int, lease_token: str) -> DirectMessageTask:
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token)
            launch = await session.get(DirectLaunch, task.launch_id)
            subscriber = await session.get(Subscriber, task.subscriber_id)
            if launch is None or subscriber is None:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.FAILED,
                    error_code="preflight_failed",
                    error="запуск или получатель больше не существует",
                )
            if launch.cancel_requested:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.CANCELLED,
                    error_code="launch_cancelled",
                    error="запуск остановлен оператором",
                )
            if (
                launch.simulated_snapshot is not None
                and launch.simulated_snapshot != self._settings.dry_run
            ):
                return await self._fail_launch(
                    task_id,
                    lease_token,
                    "launch_mode_changed",
                    "режим симуляции изменился после создания запуска",
                )
            if subscriber.do_not_contact:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_opted_out",
                    error="получатель находится в списке «не писать»",
                )
            if subscriber.direct_quarantine_code is not None:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code=subscriber.direct_quarantine_code,
                    error=subscriber.direct_quarantine_error or "получатель в карантине ЛС",
                )
            if await user_is_blacklisted(session, subscriber.telegram_user_id, subscriber.username):
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_blacklisted",
                    error="получатель находится в глобальном стоп-листе",
                )
            if subscriber.reserved_direct_launch_id != launch.id:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_reservation_lost",
                    error="получатель уже зарезервирован другим запуском",
                )
            account_ids = self._account_ids(launch.account_ids_snapshot_json)
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(account_ids)))
            )
            peers = {
                int(peer.account_id): peer
                for peer in await session.scalars(
                    select(SubscriberAccountPeer).where(
                        SubscriberAccountPeer.subscriber_id == subscriber.id,
                        SubscriberAccountPeer.account_id.in_(account_ids),
                    )
                )
            }
        if launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS:
            return await self._process_service_action(task, launch, subscriber, lease_token)
        await self._refresh_live_proxy_routes_for_accounts(account_ids)
        async with self._session_factory() as session:
            # Proxy checks can take seconds; account cooldowns/reservations may
            # have changed while another worker was sending.
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(account_ids)))
            )
            healthy_proxy_account_ids = await self._healthy_proxy_account_ids(
                session,
                account_ids,
            )
            refused_account_ids = await self._refused_launch_account_ids(session, launch.id)
        operation_key = f"direct-message:{task_id}"
        try:
            delivery_text = render_spintax(launch.primary_text_snapshot, seed=operation_key)
            delivery_html = (
                render_spintax(launch.primary_html_snapshot, seed=operation_key)
                if launch.primary_html_snapshot is not None
                else None
            )
        except ContentError as exc:
            return await self._fail_launch(
                task_id,
                lease_token,
                "content_invalid",
                safe_error_message(exc),
            )

        candidates: list[tuple[datetime | None, Account, SubscriberAccountPeer | None]] = []
        eligible_sender_count = 0
        blocked_recipient_route_count = 0
        has_proxy_blocked_sender = False
        for account in accounts:
            if int(account.id) in refused_account_ids:
                continue
            peer = peers.get(int(account.id))
            try:
                CampaignRunner._validate_sender(
                    account,
                    require_live_session=not self._settings.dry_run,
                )
            except ConflictError:
                continue
            if int(account.id) not in healthy_proxy_account_ids:
                has_proxy_blocked_sender = True
                continue
            eligible_sender_count += 1
            if peer is not None and peer.send_blocked:
                blocked_recipient_route_count += 1
                continue
            candidates.append((self._sender_ready_at(account), account, peer))
        if not candidates:
            if has_proxy_blocked_sender and eligible_sender_count == 0:
                return await self._fail_launch(
                    task_id,
                    lease_token,
                    "proxy_unavailable",
                    "обязательный прокси выбранного аккаунта недоступен",
                )
            if eligible_sender_count == 0:
                # This is an account-pool problem, not a problem with the
                # subscriber.  Calling it ``recipient_unresolvable`` made the
                # operator update an otherwise valid recipient record while
                # every selected sender was simply disabled or unauthorized.
                return await self._fail_launch(
                    task_id,
                    lease_token,
                    "sender_unavailable",
                    "выбранные отправители не готовы или уже получили отказ Telegram "
                    "в этом запуске; дальнейшие получатели не отправлялись",
                )
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code="recipient_unresolvable",
                error=(
                    "ни один готовый аккаунт не может безопасно разрешить получателя"
                    if blocked_recipient_route_count >= eligible_sender_count
                    else "не найден безопасный маршрут к получателю"
                ),
            )
        now = utc_now()
        candidates.sort(key=lambda item: (item[0] is not None, item[0] or now, int(item[1].id)))
        resume_at, account, peer = candidates[0]
        if resume_at is not None and resume_at > now:
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=resume_at,
                error_code=self._sender_wait_code(account, resume_at),
                error="отправка ждёт доступности выбранного аккаунта",
                increment_attempt=False,
                sender_account_id=int(account.id),
            )

        recipient = DirectRecipient(
            telegram_user_id=subscriber.telegram_user_id,
            username=subscriber.username,
            access_hash=peer.access_hash if peer is not None else None,
        )

        async def send() -> DirectMessageTask:
            stop_code = await self._pre_send_stop_code(
                task_id,
                lease_token,
                launch_id=launch.id,
                subscriber_id=subscriber.id,
                telegram_user_id=subscriber.telegram_user_id,
                sender_account_id=int(account.id),
            )
            if stop_code is not None:
                raise PermanentGatewayError(
                    "отправка отменена повторной проверкой состояния задачи",
                    code=stop_code,
                )
            try:
                delivery = await self._gateway.send_direct_content(
                    account=account,
                    recipient=recipient,
                    text=delivery_text,
                    html_text=delivery_html,
                    media_path=launch.primary_media_path_snapshot,
                    media_kind=launch.primary_media_kind_snapshot,
                    media_name=launch.primary_media_name_snapshot,
                    idempotency_key=operation_key,
                )
            except GatewayError as exc:
                if (
                    isinstance(exc.__cause__, GatewayError)
                    and failure_code(exc.__cause__, "") in ACCOUNT_WIDE_CODES
                ):
                    await self._runner.handle_external_account_failure(
                        int(account.id), exc.__cause__
                    )
                if failure_code(exc, "") in ACCOUNT_WIDE_CODES:
                    await self._runner.handle_external_account_failure(int(account.id), exc)
                # Save a definite refusal while the sender reservation is held.
                return await self._handle_gateway_failure(
                    task_id,
                    lease_token,
                    subscriber_id=subscriber.id,
                    account_id=int(account.id),
                    exc=exc,
                    account_failure_applied=True,
                )
            try:
                await self._runner.record_external_account_send(
                    int(account.id), operation_key=operation_key
                )
            except Exception as exc:  # noqa: BLE001 - Telegram may already have accepted it
                raise PermanentGatewayError(
                    "Telegram принял личное сообщение, но локальная фиксация "
                    "лимита не подтвердилась",
                    code="delivery_unconfirmed",
                ) from exc
            if delivery.telegram_user_id != subscriber.telegram_user_id:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.FAILED,
                    error_code="recipient_identity_mismatch",
                    error="шлюз вернул доставку другому Telegram ID; повтор заблокирован",
                    sender_account_id=int(account.id),
                    conservative_cooldown=True,
                    quarantine_recipient=True,
                )
            return await self._record_success(
                task_id,
                lease_token,
                subscriber_id=subscriber.id,
                account_id=int(account.id),
                delivery=delivery,
            )

        try:
            return await self._runner.run_account_send_operation(int(account.id), send)
        except AccountSendDeferred as exc:
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=exc.resume_at,
                error_code="account_pacing",
                error="аккаунт занят другой отправкой",
                increment_attempt=False,
            )
        except GatewayError as exc:
            return await self._handle_gateway_failure(
                task_id,
                lease_token,
                subscriber_id=subscriber.id,
                account_id=int(account.id),
                exc=exc,
            )

    @staticmethod
    def _accounting_key(task: DirectMessageTask, launch: DirectLaunch | None) -> str:
        key = f"direct-message:{task.id}"
        if (
            launch is not None
            and launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
            and task.attempt_count
        ):
            key += f":retry:{task.attempt_count}"
        return key

    async def _reserve_service_spacing(self, account_id: int) -> None:
        """Service probes and mutations share the account's global next-action window."""
        resume_at = utc_now() + timedelta(seconds=service_action_delay_seconds())
        async with self._session_factory() as session:
            await session.execute(
                update(Account)
                .where(Account.id == account_id)
                .values(
                    next_send_at=case(
                        (
                            or_(Account.next_send_at.is_(None), Account.next_send_at < resume_at),
                            resume_at,
                        ),
                        else_=Account.next_send_at,
                    )
                )
            )
            await session.commit()

    async def _process_service_action(
        self,
        task: DirectMessageTask,
        launch: DirectLaunch,
        subscriber: Subscriber,
        lease_token: str,
    ) -> DirectMessageTask:
        account_id = task.sender_account_id
        if account_id is None or account_id not in self._account_ids(
            launch.account_ids_snapshot_json
        ):
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="service_sender_unavailable",
                error="назначенный аккаунт удалён или не входит в выбранный состав; "
                "замена запрещена",
            )
        await self._refresh_live_proxy_routes_for_accounts((account_id,))
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            healthy = await self._healthy_proxy_account_ids(session, (account_id,))
        try:
            if account is None:
                raise ConflictError("аккаунт удалён")
            CampaignRunner._validate_sender(
                account, require_live_session=not self._settings.dry_run
            )
        except ConflictError:
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="service_sender_unavailable",
                error="назначенный аккаунт не готов; замена запрещена",
                sender_account_id=account_id,
            )
        if account_id not in healthy:
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="proxy_unavailable",
                error="прокси назначенного аккаунта недоступен",
                sender_account_id=account_id,
            )
        resume_at = self._sender_ready_at(account)
        if resume_at is not None:
            return await self._reschedule_task(
                task.id,
                lease_token,
                scheduled_for=resume_at,
                error_code=self._sender_wait_code(account, resume_at),
                error="ожидаем назначенный аккаунт; замена запрещена",
                increment_attempt=False,
                sender_account_id=account_id,
            )
        operation_key = self._accounting_key(task, launch)
        account_failure_applied = False

        async def before_action() -> None:
            stop_code = await self._pre_send_stop_code(
                task.id,
                lease_token,
                launch_id=launch.id,
                subscriber_id=subscriber.id,
                telegram_user_id=subscriber.telegram_user_id,
                sender_account_id=account_id,
            )
            if stop_code is not None:
                raise PermanentGatewayError("сервисное действие остановлено", code=stop_code)
            # Count attempts even on a definite Telegram refusal. This isn't a success counter.
            await self._runner.record_external_account_send(
                account_id, operation_key=operation_key, keep_reservation=True
            )
            await self._reserve_service_spacing(account_id)

        async def perform() -> DirectServiceActionResult:
            nonlocal account_failure_applied
            await self._reserve_service_spacing(account_id)
            try:
                result = await self._gateway.perform_direct_service_action(
                    account=account,
                    recipient=DirectRecipient(subscriber.telegram_user_id, subscriber.username),
                    operation_kind=launch.operation_kind_snapshot,
                    auto_delete_seconds=launch.auto_delete_seconds_snapshot,
                    premium_only=launch.segment_snapshot == SubscriberSegment.PREMIUM,
                    before_action=before_action,
                )
            except GatewayError as exc:
                # Persist Telegram's pause before releasing the global account reservation.
                if isinstance(exc.__cause__, GatewayError):
                    await self._apply_service_account_failure(account_id, exc.__cause__)
                await self._apply_service_account_failure(account_id, exc)
                account_failure_applied = True
                raise
            if result.verification_error is not None:
                await self._apply_service_account_failure(account_id, result.verification_error)
            return result

        try:
            result = await self._runner.run_account_send_operation(account_id, perform)
        except AccountSendDeferred as exc:
            return await self._reschedule_task(
                task.id,
                lease_token,
                scheduled_for=exc.resume_at,
                error_code="account_pacing",
                error="назначенный аккаунт занят другой задачей",
                increment_attempt=False,
                sender_account_id=account_id,
            )
        except GatewayError as exc:
            return await self._handle_gateway_failure(
                task.id,
                lease_token,
                subscriber_id=subscriber.id,
                account_id=account_id,
                exc=exc,
                account_failure_applied=account_failure_applied,
            )
        if result.telegram_user_id != subscriber.telegram_user_id:
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="recipient_identity_mismatch",
                error="результат относится к другому человеку",
                sender_account_id=account_id,
                conservative_cooldown=True,
                quarantine_recipient=True,
            )
        if not result.changed:
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code="service_already_set",
                error="настройка уже установлена; новое сервисное сообщение не создано",
                sender_account_id=account_id,
            )
        return await self._record_success(
            task.id,
            lease_token,
            subscriber_id=subscriber.id,
            account_id=account_id,
            delivery=result,
        )

    async def _apply_service_account_failure(self, account_id: int, exc: GatewayError) -> None:
        if failure_code(exc, "") in ACCOUNT_WIDE_CODES:
            if failure_code(exc, "") == "flood_wait":
                exc.retry_after = max(500, int(exc.retry_after or 0))
            await self._runner.handle_external_account_failure(account_id, exc)

    async def _handle_service_failure(
        self,
        task_id: int,
        lease_token: str,
        *,
        account_id: int,
        exc: GatewayError,
        account_failure_applied: bool = False,
    ) -> DirectMessageTask:
        code = failure_code(exc, "gateway_error")
        message = safe_error_message(exc)[:500]
        # Read-back can fail with FloodWait after Telegram accepted the mutation.
        # Preserve both the account cooldown and the terminal unknown outcome.
        cause = exc.__cause__
        if not account_failure_applied:
            if isinstance(cause, GatewayError):
                await self._apply_service_account_failure(account_id, cause)
            await self._apply_service_account_failure(account_id, exc)
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token)
            next_attempt = task.attempt_count + 1
            dispatch_started = task.send_started_at is not None
            account = await session.get(Account, account_id)
            account_resume = self._sender_ready_at(account) if account is not None else None
        if code in DIRECT_UNCERTAIN_CODES and (
            code != "recipient_identity_mismatch" or dispatch_started
        ):
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                conservative_cooldown=True,
                quarantine_recipient=code == "recipient_identity_mismatch",
            )
        if code == "launch_cancelled":
            status = DirectMessageTaskStatus.CANCELLED
        elif code in DIRECT_RECIPIENT_GLOBAL_SKIP_CODES | DIRECT_RECIPIENT_ROUTE_CODES | {
            "recipient_blacklisted",
            "recipient_opted_out",
            "recipient_reservation_lost",
            "service_already_set",
            "service_username_required",
            "service_recipient_not_premium",
        }:
            status = DirectMessageTaskStatus.SKIPPED
        else:
            status = DirectMessageTaskStatus.FAILED
        if (
            status == DirectMessageTaskStatus.FAILED
            and exc.retryable
            and next_attempt < task.max_attempts
        ):
            delay = max(service_action_delay_seconds(), float(exc.retry_after or 0))
            if code in {"flood_wait", "peer_flood", "account_restricted"}:
                delay = max(500, delay)
            scheduled_for = utc_now() + timedelta(seconds=delay)
            if account_resume is not None:
                scheduled_for = max(scheduled_for, account_resume)
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=scheduled_for,
                error_code=code,
                error=message,
                increment_attempt=True,
                sender_account_id=account_id,
            )
        return await self._finish_task(
            task_id,
            lease_token,
            status=status,
            error_code=code,
            error=message,
            sender_account_id=account_id,
            quarantine_recipient=code in {"recipient_unavailable", "recipient_identity_mismatch"},
        )

    async def _handle_gateway_failure(
        self,
        task_id: int,
        lease_token: str,
        *,
        subscriber_id: int,
        account_id: int,
        exc: GatewayError,
        account_failure_applied: bool = False,
    ) -> DirectMessageTask:
        code = failure_code(exc, "gateway_error")
        message = safe_error_message(exc)[:500]
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            launch = await session.get(DirectLaunch, task.launch_id)
            service_action = (
                launch is not None and launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
            )
            session.add(
                AuditEvent(
                    actor=self._worker_id,
                    event_type="direct_message_failure",
                    entity_type="direct_launch",
                    entity_id=str(task.launch_id),
                    details_json=json.dumps(
                        {
                            "task_id": task_id,
                            "account_id": account_id,
                            "code": code,
                            "error": message,
                            "at": utc_now().isoformat(),
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            await session.commit()
        if service_action:
            return await self._handle_service_failure(
                task_id,
                lease_token,
                account_id=account_id,
                exc=exc,
                account_failure_applied=account_failure_applied,
            )
        if code == "sender_refused_in_launch":
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=utc_now()
                + timedelta(seconds=self._settings.worker_retry_base_seconds),
                error_code=code,
                error=message,
                increment_attempt=False,
                sender_account_id=account_id,
            )
        if not account_failure_applied and isinstance(exc.__cause__, GatewayError):
            if failure_code(exc.__cause__, "") in ACCOUNT_WIDE_CODES:
                await self._runner.handle_external_account_failure(account_id, exc.__cause__)
        if code in ACCOUNT_WIDE_CODES and not account_failure_applied:
            await self._runner.handle_external_account_failure(account_id, exc)
        if code in {"account_restricted", "peer_flood"}:
            # The runner already requested an authoritative SpamBot check when
            # available. An RPC refusal must not overwrite that observation.
            if self._settings.account_restriction_cooldown_hours == 0:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.FAILED,
                    error_code=code,
                    error=message,
                    sender_account_id=account_id,
                )
        if code in DIRECT_CONTENT_FAILURE_CODES:
            return await self._fail_launch(task_id, lease_token, code, message)
        if code == "launch_cancelled":
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.CANCELLED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        if code in {
            "recipient_blacklisted",
            "recipient_opted_out",
            "recipient_reservation_lost",
        }:
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        if code in DIRECT_RECIPIENT_GLOBAL_SKIP_CODES:
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                quarantine_recipient=code in DIRECT_RECIPIENT_QUARANTINE_CODES,
            )
        if code == "delivery_unconfirmed":
            try:
                await self._runner.record_external_account_send(
                    account_id, operation_key=f"direct-message:{task_id}"
                )
            except Exception:  # noqa: BLE001 - preserve the unknown Telegram outcome
                logger.exception("Could not reserve quota for uncertain direct task %s", task_id)
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                conservative_cooldown=True,
            )
        if code in DIRECT_RECIPIENT_ROUTE_CODES:
            await self._block_recipient_route(subscriber_id, account_id, code, message)
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token)
            next_attempt = task.attempt_count + 1
            max_attempts = task.max_attempts
        if next_attempt >= max_attempts:
            return await self._finish_task(
                task_id,
                lease_token,
                status=(
                    DirectMessageTaskStatus.SKIPPED
                    if code in DIRECT_RECIPIENT_ROUTE_CODES or code in ACCOUNT_WIDE_CODES
                    else DirectMessageTaskStatus.FAILED
                ),
                error_code=code,
                error=message,
                sender_account_id=account_id,
                attempt_count=next_attempt,
            )
        if not exc.retryable and code not in DIRECT_RECIPIENT_ROUTE_CODES | ACCOUNT_WIDE_CODES:
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        # ``retry_after`` for a Telegram-wide sender restriction is the
        # account cooldown (often 24 hours), not a reason to freeze this
        # recipient.  The cooldown recorded above removes this sender from
        # immediate choice; requeue quickly so another selected sender can
        # continue the same launch.
        if code in ACCOUNT_WIDE_CODES:
            delay = max(1, int(self._settings.worker_retry_base_seconds))
        else:
            delay = max(
                int(exc.retry_after or 0),
                min(
                    int(self._settings.worker_retry_max_seconds),
                    int(self._settings.worker_retry_base_seconds) * (2**next_attempt),
                ),
            )
        return await self._reschedule_task(
            task_id,
            lease_token,
            scheduled_for=utc_now() + timedelta(seconds=max(1, delay)),
            error_code=code,
            error=message,
            increment_attempt=True,
            sender_account_id=account_id,
        )

    async def _refused_launch_account_ids(self, session: AsyncSession, launch_id: int) -> set[int]:
        """Remember terminal sender refusals for this launch without guessing an expiry."""
        refused: set[int] = set()
        spambot_blocked = await session.scalars(
            select(Account.id).where(Account.spambot_status == "active_restriction")
        )
        refused.update(int(aid) for aid in spambot_blocked)

        if self._settings.account_restriction_cooldown_hours == 0:
            failed_senders = await session.scalars(
                select(DirectMessageTask.sender_account_id)
                .where(
                    DirectMessageTask.launch_id == launch_id,
                    DirectMessageTask.status == DirectMessageTaskStatus.FAILED,
                    DirectMessageTask.error_code.in_(("account_restricted", "peer_flood")),
                    DirectMessageTask.sender_account_id.is_not(None),
                )
                .distinct()
            )
            refused.update(int(aid) for aid in failed_senders)
        return refused

    def _sender_ready_at(self, account: Account) -> datetime | None:
        """Return the first safe instant for a sender across every worker flow.

        ``CampaignRunner`` serializes sends with a durable account reservation.
        A direct-message worker must honour that reservation while choosing a
        sender, otherwise several tasks can pile up behind the lowest ID even
        while another selected account is immediately available.
        """

        now = utc_now()
        candidates = [self._runner.account_resume_at(account)]
        reservation_until = ensure_utc(account.send_reservation_until)
        if reservation_until is not None and reservation_until > now:
            candidates.append(reservation_until)
        future = [value for value in candidates if value is not None and value > now]
        return max(future) if future else None

    @staticmethod
    def _sender_wait_code(account: Account, resume_at: datetime) -> str:
        cooldown = ensure_utc(account.cooldown_until)
        if cooldown is not None and cooldown >= resume_at:
            reason = account.cooldown_reason or ""
            if reason in {"account_restricted", "peer_flood"} or reason.startswith("spambot_"):
                return "account_restricted"
            if reason in {"flood_wait", "flood_escalation"}:
                return "flood_wait"
        return "account_pacing"

    async def _pre_send_stop_code(
        self,
        task_id: int,
        lease_token: str,
        *,
        launch_id: int,
        subscriber_id: int,
        telegram_user_id: int,
        sender_account_id: int | None = None,
    ) -> str | None:
        """Close cancellation/stop-list races while an account send slot is awaited."""

        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            if task.launch_id != launch_id or task.subscriber_id != subscriber_id:
                return "recipient_reservation_lost"
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None or launch.cancel_requested:
                return "launch_cancelled"
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None or subscriber.reserved_direct_launch_id != launch_id:
                return "recipient_reservation_lost"
            if subscriber.do_not_contact:
                return "recipient_opted_out"
            if subscriber.direct_quarantine_code is not None:
                return subscriber.direct_quarantine_code
            if await user_is_blacklisted(session, telegram_user_id, subscriber.username):
                return "recipient_blacklisted"
            if sender_account_id is not None:
                service_action = launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS
                unavailable_code = (
                    "service_sender_unavailable" if service_action else "sender_unavailable"
                )
                if sender_account_id not in self._account_ids(launch.account_ids_snapshot_json):
                    return unavailable_code
                account = await session.get(Account, sender_account_id)
                if account is None:
                    return unavailable_code
                try:
                    CampaignRunner._validate_sender(
                        account, require_live_session=not self._settings.dry_run
                    )
                except ConflictError:
                    return unavailable_code
                if launch.operation_kind_snapshot in DIRECT_SERVICE_ACTIONS:
                    if (
                        task.sender_account_id != sender_account_id
                        or sender_account_id
                        not in self._account_ids(launch.account_ids_snapshot_json)
                    ):
                        return "service_sender_unavailable"
                elif sender_account_id in await self._refused_launch_account_ids(
                    session, launch_id
                ):
                    return "sender_refused_in_launch"
                task.sender_account_id = sender_account_id
                task.send_started_at = utc_now()
                await session.commit()
        return None

    async def _block_recipient_route(
        self,
        subscriber_id: int,
        account_id: int,
        code: str,
        error: str,
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            statement = sqlite_insert(SubscriberAccountPeer).values(
                subscriber_id=subscriber_id,
                account_id=account_id,
                send_blocked=True,
                blocked_reason=code,
                last_error=error[:500],
                updated_at=now,
            )
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberAccountPeer.subscriber_id,
                        SubscriberAccountPeer.account_id,
                    ],
                    set_={
                        "send_blocked": True,
                        "blocked_reason": code,
                        "last_error": error[:500],
                        "access_hash": (
                            None
                            if code == "recipient_unresolvable"
                            else SubscriberAccountPeer.access_hash
                        ),
                        "updated_at": now,
                    },
                )
            )
            await session.commit()

    async def _record_success(
        self,
        task_id: int,
        lease_token: str,
        *,
        subscriber_id: int,
        account_id: int,
        delivery: DirectMessageDelivery | DirectServiceActionResult,
    ) -> DirectMessageTask:
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None:
                raise NotFoundError("получатель исчез после доставки")
            task.status = DirectMessageTaskStatus.SUCCEEDED
            task.sender_account_id = account_id
            service_action = isinstance(delivery, DirectServiceActionResult)
            task.external_message_id = (
                delivery.service_message_id if service_action else delivery.message_id
            )
            if service_action:
                task.result_code = (
                    "service_notification_confirmed"
                    if delivery.service_message_id is not None
                    else "service_change_confirmed"
                )
            task.error_code = None
            task.error = None
            if service_action and delivery.verification_error is not None:
                task.error_code = "service_notification_unconfirmed"
                task.error = safe_error_message(delivery.verification_error)[:500]
                session.add(
                    AuditEvent(
                        actor=self._worker_id,
                        event_type="direct_service_verification_failure",
                        entity_type="direct_launch",
                        entity_id=str(task.launch_id),
                        details_json=json.dumps(
                            {
                                "task_id": task_id,
                                "account_id": account_id,
                                "code": failure_code(delivery.verification_error, "gateway_error"),
                                "error": task.error,
                            },
                            ensure_ascii=False,
                        ),
                    )
                )
            task.finished_at = now
            self._release_task_lease(task)
            subscriber.last_contacted_at = now
            subscriber.last_delivery_uncertain_at = None
            if not service_action:
                subscriber.message_count += 1
            subscriber.last_error_code = None
            subscriber.last_error = None
            subscriber.reserved_direct_launch_id = None
            profile_changed = False
            if delivery.username is not None:
                if subscriber.username != delivery.username:
                    subscriber.username = delivery.username
                    profile_changed = True
            if delivery.is_premium is not None:
                profile_changed = profile_changed or subscriber.is_premium != delivery.is_premium
                subscriber.is_premium = delivery.is_premium
                subscriber.premium_checked_at = now
            if profile_changed:
                await self._refresh_list_counts_for_subscribers(session, {subscriber_id})
            statement = sqlite_insert(SubscriberAccountPeer).values(
                subscriber_id=subscriber_id,
                account_id=account_id,
                access_hash=delivery.access_hash,
                send_blocked=False,
                blocked_reason=None,
                last_error=None,
                resolved_at=now,
                updated_at=now,
            )
            peer_updates = {
                "access_hash": func.coalesce(
                    statement.excluded.access_hash,
                    SubscriberAccountPeer.access_hash,
                ),
                "resolved_at": now,
                "updated_at": now,
            }
            if not service_action:
                peer_updates.update(send_blocked=False, blocked_reason=None, last_error=None)
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberAccountPeer.subscriber_id,
                        SubscriberAccountPeer.account_id,
                    ],
                    set_=peer_updates,
                )
            )
            await session.commit()
            await session.refresh(task)
        await self._refresh_launch(task.launch_id)
        return task

    async def _reschedule_task(
        self,
        task_id: int,
        lease_token: str,
        *,
        scheduled_for: datetime,
        error_code: str,
        error: str,
        increment_attempt: bool,
        sender_account_id: int | None = None,
    ) -> DirectMessageTask:
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            launch = await session.get(DirectLaunch, task.launch_id)
            if launch is not None and launch.cancel_requested:
                task.status = DirectMessageTaskStatus.CANCELLED
                task.finished_at = utc_now()
                error_code = "launch_cancelled"
                error = "запуск остановлен до повторной попытки"
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id == task.subscriber_id,
                        Subscriber.reserved_direct_launch_id == task.launch_id,
                    )
                    .values(reserved_direct_launch_id=None)
                )
            else:
                task.status = DirectMessageTaskStatus.QUEUED
            # Rescheduling is only allowed after a definite refusal or a
            # failure before dispatch. An ambiguous result is always terminal.
            task.send_started_at = None
            task.scheduled_for = max(ensure_utc(scheduled_for) or utc_now(), utc_now())
            if increment_attempt:
                task.attempt_count += 1
            if sender_account_id is not None:
                task.sender_account_id = sender_account_id
            task.error_code = error_code
            task.error = error[:500]
            self._release_task_lease(task)
            await session.commit()
            await session.refresh(task)
        await self._refresh_launch(task.launch_id)
        return task

    async def _finish_task(
        self,
        task_id: int,
        lease_token: str | None,
        *,
        status: DirectMessageTaskStatus,
        error_code: str,
        error: str,
        sender_account_id: int | None = None,
        conservative_cooldown: bool = False,
        attempt_count: int | None = None,
        quarantine_recipient: bool = False,
    ) -> DirectMessageTask:
        if status not in DIRECT_MESSAGE_TERMINAL_STATUSES:
            raise ValueError("terminal task status required")
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            task.status = status
            task.error_code = error_code
            task.error = error[:500]
            if attempt_count is not None:
                task.attempt_count = max(task.attempt_count, attempt_count)
            if sender_account_id is not None:
                task.sender_account_id = sender_account_id
            task.finished_at = now
            self._release_task_lease(task)
            subscriber_values: dict[str, object] = {
                "reserved_direct_launch_id": None,
                "last_error_code": error_code,
                "last_error": error[:500],
                "updated_at": now,
            }
            if conservative_cooldown:
                subscriber_values["last_delivery_uncertain_at"] = now
            if quarantine_recipient and error_code in DIRECT_RECIPIENT_QUARANTINE_CODES:
                subscriber_values.update(
                    direct_quarantine_code=error_code,
                    direct_quarantine_error=error[:500],
                    direct_quarantine_at=now,
                    direct_quarantine_account_id=sender_account_id,
                    direct_quarantine_checked_at=None,
                    direct_quarantine_check_note=None,
                )
                session.add(
                    AuditEvent(
                        actor=self._worker_id,
                        event_type="direct_recipient_quarantined",
                        entity_type="subscriber",
                        entity_id=str(task.subscriber_id),
                        details_json=json.dumps(
                            {
                                "code": error_code,
                                "task_id": task_id,
                                "account_id": sender_account_id,
                            }
                        ),
                    )
                )
            await session.execute(
                update(Subscriber)
                .where(
                    Subscriber.id == task.subscriber_id,
                    Subscriber.reserved_direct_launch_id == task.launch_id,
                )
                .values(**subscriber_values)
            )
            launch = await session.get(DirectLaunch, task.launch_id)
            if (
                launch is not None
                and not launch.cancel_requested
                and status == DirectMessageTaskStatus.SKIPPED
                and launch.operation_kind_snapshot == "message"
                and not conservative_cooldown
                and error_code in DIRECT_RECIPIENT_REPLACEMENT_CODES
            ):
                await self._reserve_replacement_recipient(
                    session,
                    launch=launch,
                    source_task=task,
                    now=now,
                )
            launch_id = task.launch_id
            await session.commit()
            await session.refresh(task)
        await self._refresh_launch(launch_id)
        return task

    async def _reserve_replacement_recipient(
        self,
        session: AsyncSession,
        *,
        launch: DirectLaunch,
        source_task: DirectMessageTask,
        now: datetime,
    ) -> bool:
        """Reserve one new eligible subscriber after a terminal recipient refusal.

        ``DirectLaunch.total_tasks`` is the number that was actually reserved at
        launch time (it can be lower than the requested limit when the segment is
        small).  A skipped person must therefore be replaced only while successful
        or still-active tasks are below that immutable target.  The task table's
        unique launch/subscriber constraint, plus the reservation update below,
        protects both against duplicate messages and concurrent workers selecting
        the same replacement.
        """

        desired = max(0, int(launch.total_tasks or 0))
        if desired == 0:
            return False
        covered = int(
            await session.scalar(
                select(func.count(DirectMessageTask.id)).where(
                    DirectMessageTask.launch_id == launch.id,
                    DirectMessageTask.status.in_(
                        (
                            DirectMessageTaskStatus.QUEUED,
                            DirectMessageTaskStatus.RUNNING,
                            DirectMessageTaskStatus.SUCCEEDED,
                        )
                    ),
                )
            )
            or 0
        )
        if covered >= desired:
            return False

        cooldown_before = now - timedelta(days=launch.contact_cooldown_days_snapshot)
        uncertain_delivery_before = now - timedelta(
            days=max(1, launch.contact_cooldown_days_snapshot)
        )
        conditions = [
            SubscriberListMember.subscriber_list_id == launch.subscriber_list_id,
            Subscriber.do_not_contact.is_(False),
            Subscriber.direct_quarantine_code.is_(None),
            self._segment_condition(launch.segment_snapshot),
            ~subscriber_blacklisted_condition(),
            Subscriber.reserved_direct_launch_id.is_(None),
            ~Subscriber.id.in_(
                select(DirectMessageTask.subscriber_id).where(
                    DirectMessageTask.launch_id == launch.id
                )
            ),
            or_(
                Subscriber.last_delivery_uncertain_at.is_(None),
                Subscriber.last_delivery_uncertain_at <= uncertain_delivery_before,
            ),
        ]
        if launch.contact_cooldown_days_snapshot > 0:
            conditions.append(
                or_(
                    Subscriber.last_contacted_at.is_(None),
                    Subscriber.last_contacted_at <= cooldown_before,
                )
            )

        # Another worker can win the reservation after the SELECT. Retry with a
        # fresh batch a few times; a miss is harmless because the launch can still
        # finish with its clearly reported shortfall when no eligible person exists.
        for _ in range(3):
            candidate_ids = list(
                await session.scalars(
                    select(Subscriber.id)
                    .join(
                        SubscriberListMember,
                        SubscriberListMember.subscriber_id == Subscriber.id,
                    )
                    .where(*conditions)
                    .order_by(
                        case(
                            (
                                and_(
                                    Subscriber.username.is_not(None),
                                    Subscriber.username != "",
                                ),
                                0,
                            ),
                            else_=1,
                        ),
                        case((Subscriber.last_contacted_at.is_(None), 0), else_=1),
                        Subscriber.last_contacted_at,
                        Subscriber.id,
                    )
                    .limit(1)
                )
            )
            if not candidate_ids:
                return False
            claimed = list(
                await session.scalars(
                    update(Subscriber)
                    .where(
                        Subscriber.id.in_(candidate_ids),
                        Subscriber.reserved_direct_launch_id.is_(None),
                        Subscriber.direct_quarantine_code.is_(None),
                    )
                    .values(reserved_direct_launch_id=launch.id, updated_at=now)
                    .returning(Subscriber.id)
                )
            )
            if not claimed:
                continue
            replacement_id = int(claimed[0])
            session.add(
                DirectMessageTask(
                    launch_id=launch.id,
                    subscriber_id=replacement_id,
                    status=DirectMessageTaskStatus.QUEUED,
                    attempt_count=0,
                    max_attempts=source_task.max_attempts,
                    scheduled_for=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            return True
        return False

    async def _fail_launch(
        self,
        task_id: int,
        lease_token: str,
        code: str,
        error: str,
    ) -> DirectMessageTask:
        """Stop a launch-wide unsafe condition and release every pending recipient.

        This is used for invalid content and for the case where every selected
        sender lost its mandatory proxy route after the launch was created.
        Neither condition can become safe by retrying the same queued entries.
        """
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token, for_update=True)
            launch = await session.get(DirectLaunch, task.launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
            pending_subscriber_ids = select(DirectMessageTask.subscriber_id).where(
                DirectMessageTask.launch_id == launch.id,
                or_(
                    DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                    DirectMessageTask.id == task.id,
                ),
            )
            # Release only the queued recipients and this attempt. Other
            # RUNNING tasks keep their reservations until their own outcome.
            # A subquery also keeps the bind count independent of queue size.
            await session.execute(
                update(Subscriber)
                .where(
                    Subscriber.id.in_(pending_subscriber_ids),
                    Subscriber.reserved_direct_launch_id == launch.id,
                )
                .values(reserved_direct_launch_id=None, updated_at=now)
                .execution_options(synchronize_session=False)
            )
            task.status = DirectMessageTaskStatus.FAILED
            task.error_code = code
            task.error = error[:500]
            task.finished_at = now
            self._release_task_lease(task)
            await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.launch_id == launch.id,
                    DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                )
                .values(
                    status=DirectMessageTaskStatus.FAILED,
                    error_code=code,
                    error=error[:500],
                    finished_at=now,
                    updated_at=now,
                )
            )
            launch.cancel_requested = True
            launch.status = DirectLaunchStatus.FAILED
            launch.finished_at = now
            await session.commit()
            await session.refresh(task)
            return task

    async def _refresh_launch(self, launch_id: int) -> None:
        async with self._session_factory() as session:
            await session.execute(
                update(DirectLaunch)
                .where(DirectLaunch.id == launch_id)
                .values(updated_at=DirectLaunch.updated_at)
            )
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
                return
            if launch.status == DirectLaunchStatus.FAILED and launch.cancel_requested:
                return
            counts = dict(
                (
                    await session.execute(
                        select(DirectMessageTask.status, func.count(DirectMessageTask.id))
                        .where(DirectMessageTask.launch_id == launch_id)
                        .group_by(DirectMessageTask.status)
                    )
                ).all()
            )
            active = int(counts.get(DirectMessageTaskStatus.QUEUED, 0)) + int(
                counts.get(DirectMessageTaskStatus.RUNNING, 0)
            )
            if active:
                launch.status = DirectLaunchStatus.RUNNING
                launch.started_at = launch.started_at or utc_now()
            elif launch.cancel_requested:
                launch.status = DirectLaunchStatus.CANCELLED
                launch.finished_at = utc_now()
            else:
                succeeded = int(counts.get(DirectMessageTaskStatus.SUCCEEDED, 0))
                failed_or_skipped = int(counts.get(DirectMessageTaskStatus.FAILED, 0)) + int(
                    counts.get(DirectMessageTaskStatus.SKIPPED, 0)
                )
                if succeeded >= launch.total_tasks and launch.total_tasks > 0:
                    launch.status = DirectLaunchStatus.SUCCEEDED
                elif succeeded and failed_or_skipped:
                    launch.status = DirectLaunchStatus.PARTIAL
                elif succeeded:
                    launch.status = DirectLaunchStatus.SUCCEEDED
                else:
                    launch.status = DirectLaunchStatus.FAILED
                launch.finished_at = utc_now()
            await session.commit()

    async def _require_task(
        self,
        session: AsyncSession,
        task_id: int,
        lease_token: str | None,
        *,
        for_update: bool = False,
    ) -> DirectMessageTask:
        if for_update:
            locked = await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.id == task_id,
                    DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                    DirectMessageTask.lease_token == lease_token,
                    DirectMessageTask.lease_until > utc_now(),
                )
                .values(updated_at=DirectMessageTask.updated_at)
            )
            if locked.rowcount != 1:
                raise ConflictError("задача потеряла право выполнения")
        task = await session.get(DirectMessageTask, task_id)
        if task is None:
            raise NotFoundError("задача личного сообщения не найдена")
        if (
            task.status != DirectMessageTaskStatus.RUNNING
            or task.lease_token != lease_token
            or (ensure_utc(task.lease_until) or utc_now()) <= utc_now()
        ):
            raise ConflictError("задача потеряла право выполнения")
        return task

    @staticmethod
    def _release_task_lease(task: DirectMessageTask) -> None:
        task.lease_owner = None
        task.lease_token = None
        task.lease_until = None

    async def _healthy_proxy_account_ids(
        self,
        session: AsyncSession,
        account_ids: Sequence[int],
    ) -> set[int]:
        """Return sender IDs that have a usable mandatory proxy route.

        A direct-message launch must never reserve recipients for an account
        whose configured live route is already marked unavailable.  The worker
        repeats the same check because a proxy can fail after a launch was made.
        Test runs intentionally bypass the live-route requirement.
        """

        normalized_ids = tuple(dict.fromkeys(int(value) for value in account_ids if int(value) > 0))
        if not normalized_ids:
            return set()
        if self._settings.dry_run or not self._settings.account_require_proxy:
            return set(normalized_ids)
        assignment_keys = tuple(f"account:{account_id}" for account_id in normalized_ids)
        healthy_keys = set(
            await session.scalars(
                select(ProxyAssignment.assignment_key)
                .join(ProxyEndpoint, ProxyEndpoint.id == ProxyAssignment.proxy_id)
                .where(
                    ProxyAssignment.assignment_key.in_(assignment_keys),
                    ProxyEndpoint.enabled.is_(True),
                    ProxyEndpoint.status == ProxyStatus.HEALTHY,
                )
            )
        )
        return {
            account_id for account_id in normalized_ids if f"account:{account_id}" in healthy_keys
        }

    async def _refresh_live_proxy_routes(self, campaign_id: int) -> None:
        """Verify and, when possible, replace live routes before reservation."""

        if (
            self._settings.dry_run
            or not self._settings.account_require_proxy
            or self._proxy_service is None
        ):
            return
        async with self._session_factory() as session:
            account_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(DirectCampaignAccount.account_id).where(
                        DirectCampaignAccount.campaign_id == campaign_id
                    )
                )
            )
        await self._refresh_live_proxy_routes_for_accounts(account_ids)

    async def _refresh_live_proxy_routes_for_accounts(self, account_ids: Sequence[int]) -> None:
        """Keep selected senders on freshly checked proxy-only routes.

        ``ensure_account_route`` performs the recovery atomically.  A failed
        recovery is deliberately not fatal here: the normal sender filter below
        gives the operator one clear ``proxy_unavailable`` result if no selected
        sender can be recovered.
        """

        if (
            self._settings.dry_run
            or not self._settings.account_require_proxy
            or self._proxy_service is None
        ):
            return
        normalized_ids = tuple(dict.fromkeys(int(value) for value in account_ids if int(value) > 0))
        if not normalized_ids:
            return
        semaphore = asyncio.Semaphore(6)

        async def ensure_route(account_id: int) -> object:
            async with semaphore:
                return await self._proxy_service.ensure_account_route(
                    account_id,
                    actor="direct-message-route-recovery",
                )

        results = await asyncio.gather(
            *(ensure_route(account_id) for account_id in normalized_ids),
            return_exceptions=True,
        )
        for account_id, result in zip(normalized_ids, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "Could not recover proxy route for direct sender %s: %s",
                    account_id,
                    result,
                )

    @staticmethod
    def _account_ids(raw: str) -> tuple[int, ...]:
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConflictError("список отправителей запуска повреждён") from exc
        if not isinstance(values, list):
            raise ConflictError("список отправителей запуска повреждён")
        result = tuple(dict.fromkeys(int(value) for value in values if int(value) > 0))
        if not result:
            raise ConflictError("в запуске нет отправителей")
        return result

    async def _heartbeat(self, task_id: int, lease_token: str, stop_event: asyncio.Event) -> None:
        interval = max(1.0, float(self._settings.worker_heartbeat_seconds))
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except TimeoutError:
                pass
            async with self._session_factory() as session:
                renewed = await session.execute(
                    update(DirectMessageTask)
                    .where(
                        DirectMessageTask.id == task_id,
                        DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                        DirectMessageTask.lease_token == lease_token,
                    )
                    .values(
                        lease_until=utc_now()
                        + timedelta(seconds=max(30, self._settings.worker_lease_seconds)),
                        updated_at=utc_now(),
                    )
                )
                await session.commit()
            if renewed.rowcount != 1:
                return

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        self._running = True
        concurrency = max(1, min(8, int(self._settings.direct_messages_worker_concurrency)))
        workers = [
            asyncio.create_task(
                self._worker_loop(reporter),
                name=f"direct-message-worker-{index + 1}",
            )
            for index in range(concurrency)
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            self._running = False

    async def _worker_loop(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop_event.is_set():
            try:
                task = await self.process_next()
            except asyncio.CancelledError:
                raise
            except OperationalError as exc:
                if is_transient_sqlite_lock(exc):
                    logger.warning("Direct-message worker waits for a busy SQLite writer")
                    await asyncio.sleep(0.5)
                    continue
                logger.exception("Direct-message worker iteration failed")
                if reporter is not None:
                    await reporter(exc)
                task = None
            except Exception as exc:  # noqa: BLE001 - one failure cannot stop the queue
                logger.exception("Direct-message worker iteration failed")
                if reporter is not None:
                    await reporter(exc)
                task = None
            else:
                if reporter is not None:
                    await reporter()
            if task is not None:
                continue
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=max(0.2, float(self._settings.direct_messages_poll_seconds)),
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()
