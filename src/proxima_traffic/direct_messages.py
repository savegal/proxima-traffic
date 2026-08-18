"""Subscriber databases and durable consent-based personal-message campaigns."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import case, delete, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.content import (
    ContentError,
    render_spintax,
    resolve_media_path,
    validate_telegram_text,
)
from proxima_traffic.failure_policy import ACCOUNT_WIDE_CODES, failure_code
from proxima_traffic.gateways import (
    DirectMessageDelivery,
    DirectRecipient,
    GatewayError,
    MessageGateway,
    PermanentGatewayError,
)
from proxima_traffic.models import (
    Account,
    DirectCampaign,
    DirectCampaignAccount,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    MentionBlacklistEntry,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyStatus,
    Subscriber,
    SubscriberAccountPeer,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import ProxyService
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
# Keep imports responsive on SQLite while still amortising commits for large bases.
IMPORT_COMMIT_EVERY_BATCHES = 20
DIRECT_MESSAGE_MAX_RECIPIENTS = 1_000_000
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
        "recipient_unavailable",
    }
)
DIRECT_RECIPIENT_ROUTE_CODES = frozenset(
    {
        "recipient_privacy_restricted",
        "recipient_requires_premium",
        "recipient_route_blocked",
        "recipient_unresolvable",
    }
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
    last_error_code: str | None = None
    last_error: str | None = None
    last_error_status: DirectMessageTaskStatus | None = None
    last_error_account_label: str | None = None
    last_error_recipient: str | None = None

    @property
    def active(self) -> int:
        return self.queued + self.running


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
        self._running = False
        self._active_steps = 0

    async def import_subscribers(
        self,
        path: str | Path,
        *,
        list_name: str,
        source_filename: str,
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
                    subscriber_list.status = SubscriberListStatus.IMPORTING
                    subscriber_list.source_filename = Path(source_filename).name[:255]
                    subscriber_list.last_error = None
                list_id = int(subscriber_list.id)

                batch: list[SubscriberRecord] = []
                imported_batches = 0
                for record in iter_subscriber_records(path, stats=stats):
                    batch.append(record)
                    if len(batch) < IMPORT_BATCH_SIZE:
                        continue
                    inserted, updated, members = await self._import_batch(
                        session,
                        list_id=list_id,
                        records=batch,
                        imported_at=now,
                    )
                    inserted_profiles += inserted
                    updated_profiles += updated
                    added_members += members
                    batch.clear()
                    imported_batches += 1
                    if imported_batches % IMPORT_COMMIT_EVERY_BATCHES == 0:
                        await session.commit()
                    await asyncio.sleep(0)
                if batch:
                    inserted, updated, members = await self._import_batch(
                        session,
                        list_id=list_id,
                        records=batch,
                        imported_at=now,
                    )
                    inserted_profiles += inserted
                    updated_profiles += updated
                    added_members += members
                    imported_batches += 1
                    if imported_batches % IMPORT_COMMIT_EVERY_BATCHES == 0:
                        await session.commit()
                if stats.valid_records == 0:
                    raise SubscriberImportError("в файле нет ни одной корректной записи")

                counts = await self._list_counts(session, list_id)
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

    @staticmethod
    async def _import_batch(
        session: AsyncSession,
        *,
        list_id: int,
        records: Sequence[SubscriberRecord],
        imported_at: datetime,
    ) -> tuple[int, int, int]:
        telegram_ids = [item.telegram_user_id for item in records]
        existing_ids = set(
            await session.scalars(
                select(Subscriber.telegram_user_id).where(
                    Subscriber.telegram_user_id.in_(telegram_ids)
                )
            )
        )
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
        existing_members = set(
            await session.scalars(
                select(SubscriberListMember.subscriber_id).where(
                    SubscriberListMember.subscriber_list_id == list_id,
                    SubscriberListMember.subscriber_id.in_(subscriber_ids),
                )
            )
        )
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
        updated = len(existing_ids)
        return len(records) - updated, updated, len(new_member_ids)

    @staticmethod
    async def _list_counts(session: AsyncSession, list_id: int) -> tuple[int, int, int, int]:
        row = (
            await session.execute(
                select(
                    func.count(Subscriber.id),
                    func.sum(case((Subscriber.is_premium.is_(True), 1), else_=0)),
                    func.sum(case((Subscriber.is_premium.is_(False), 1), else_=0)),
                    func.sum(case((Subscriber.username.is_not(None), 1), else_=0)),
                )
                .join(
                    SubscriberListMember,
                    SubscriberListMember.subscriber_id == Subscriber.id,
                )
                .where(SubscriberListMember.subscriber_list_id == list_id)
            )
        ).one()
        return tuple(int(value or 0) for value in row)  # type: ignore[return-value]

    async def _mark_import_failed(self, list_id: int, exc: BaseException) -> None:
        try:
            async with self._session_factory() as session:
                subscriber_list = await session.get(SubscriberList, list_id)
                if subscriber_list is None:
                    return
                subscriber_list.status = SubscriberListStatus.FAILED
                subscriber_list.last_error = safe_error_message(exc)[:500]
                subscriber_list.updated_at = utc_now()
                await session.commit()
        except Exception:  # noqa: BLE001 - preserve the original import failure
            logger.exception("Could not persist subscriber import failure")

    async def dashboard(self) -> DirectMessageDashboard:
        async with self._session_factory() as session:
            list_count = int(await session.scalar(select(func.count(SubscriberList.id))) or 0)
            campaign_count = int(await session.scalar(select(func.count(DirectCampaign.id))) or 0)
            subscriber_row = (
                await session.execute(
                    select(
                        func.count(Subscriber.id),
                        func.sum(case((Subscriber.is_premium.is_(True), 1), else_=0)),
                        func.sum(case((Subscriber.username.is_not(None), 1), else_=0)),
                    )
                )
            ).one()
            task_counts = dict(
                (
                    await session.execute(
                        select(DirectMessageTask.status, func.count(DirectMessageTask.id)).group_by(
                            DirectMessageTask.status
                        )
                    )
                ).all()
            )
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
            return DirectMessageDashboard(
                list_count=list_count,
                subscriber_count=int(subscriber_row[0] or 0),
                premium_count=int(subscriber_row[1] or 0),
                username_count=int(subscriber_row[2] or 0),
                blacklist_count=blacklist_count,
                campaign_count=campaign_count,
                active_launches=active_launches,
                queued_tasks=int(task_counts.get(DirectMessageTaskStatus.QUEUED, 0)),
                running_tasks=int(task_counts.get(DirectMessageTaskStatus.RUNNING, 0)),
                succeeded_tasks=int(task_counts.get(DirectMessageTaskStatus.SUCCEEDED, 0)),
                failed_tasks=int(task_counts.get(DirectMessageTaskStatus.FAILED, 0)),
                skipped_tasks=int(task_counts.get(DirectMessageTaskStatus.SKIPPED, 0)),
            )

    async def add_blacklist_entries(
        self,
        telegram_user_ids: Sequence[int],
        *,
        source: str = "control_bot_direct_messages",
    ) -> DirectBlacklistAddResult:
        normalized_values: list[int] = []
        for value in telegram_user_ids:
            try:
                telegram_user_id = int(value)
            except (TypeError, ValueError) as exc:
                raise ConflictError("стоп-лист содержит некорректный Telegram ID") from exc
            if not 1 <= telegram_user_id <= TELEGRAM_USER_ID_MAX:
                raise ConflictError("стоп-лист содержит Telegram ID вне допустимого диапазона")
            normalized_values.append(telegram_user_id)
        values = tuple(dict.fromkeys(normalized_values))
        if not values:
            raise ConflictError("укажите хотя бы один корректный Telegram ID")
        if len(values) > 500:
            raise ConflictError("за один раз можно добавить не больше 500 ID")
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
            subscriber_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(Subscriber.id).where(Subscriber.telegram_user_id.in_(values))
                )
            )
            affected_rows: list[tuple[int, int]] = []
            affected_invite_rows: list[tuple[int, int]] = []
            if subscriber_ids:
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
                    await session.execute(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(affected_subscriber_ids),
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
                    await session.execute(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(affected_invite_subscriber_ids),
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
            requested=len(values),
            added=len(values) - len(existing),
            already_present=len(existing),
            cancelled_tasks=len(affected_rows) + len(affected_invite_rows),
            cancelled_invite_tasks=len(affected_invite_rows),
            affected_invite_launch_ids=tuple(sorted({row[0] for row in affected_invite_rows})),
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
        text: str,
        html_text: str | None,
        media_path: str | None,
        media_kind: str | None,
        media_name: str | None,
        media_size: int | None,
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
        has_media = media_path is not None
        media_without_caption = has_media
        try:
            validate_telegram_text(
                text,
                html_text,
                has_media=has_media,
                allow_empty=media_without_caption,
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
            campaign = DirectCampaign(
                name=normalized_name,
                subscriber_list_id=subscriber_list_id,
                segment=segment,
                recipient_limit=recipient_limit,
                contact_cooldown_days=contact_cooldown_days,
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
            return DirectCampaignView(
                campaign=campaign,
                list_name=subscriber_list.name,
                account_ids=tuple(int(row.id) for row in account_rows),
                account_labels=tuple(str(row.label) for row in account_rows),
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
            self._segment_condition(campaign.segment),
            ~Subscriber.telegram_user_id.in_(select(MentionBlacklistEntry.telegram_user_id)),
        ]
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
    ) -> DirectLaunch:
        if not consent_confirmed:
            raise ConflictError("нужно подтвердить, что получатели дали согласие на сообщения")
        await self._refresh_live_proxy_routes(campaign_id)
        now = utc_now()
        async with self._session_factory() as session:
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

            cooldown_before = now - timedelta(days=campaign.contact_cooldown_days)
            uncertain_delivery_before = now - timedelta(days=max(1, campaign.contact_cooldown_days))
            conditions = [
                SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
                Subscriber.do_not_contact.is_(False),
                self._segment_condition(campaign.segment),
                ~Subscriber.telegram_user_id.in_(select(MentionBlacklistEntry.telegram_user_id)),
                Subscriber.reserved_direct_launch_id.is_(None),
                or_(
                    Subscriber.last_delivery_uncertain_at.is_(None),
                    Subscriber.last_delivery_uncertain_at <= uncertain_delivery_before,
                ),
            ]
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
                campaign_id=campaign.id,
                subscriber_list_id=campaign.subscriber_list_id,
                status=DirectLaunchStatus.QUEUED,
                segment_snapshot=campaign.segment,
                recipient_limit_snapshot=campaign.recipient_limit,
                contact_cooldown_days_snapshot=campaign.contact_cooldown_days,
                account_ids_snapshot_json=json.dumps(ready_account_ids, separators=(",", ":")),
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
                    task_rows.append(
                        {
                            "launch_id": launch.id,
                            "subscriber_id": subscriber_id,
                            "status": DirectMessageTaskStatus.QUEUED,
                            "attempt_count": 0,
                            "max_attempts": max_attempts,
                            "scheduled_for": now + timedelta(seconds=wave * pace_seconds),
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
                await session.execute(sqlite_insert(DirectMessageTask).values(task_rows))
            await session.commit()
            await session.refresh(launch)
            return launch

    async def list_launches(self, page: int, page_size: int) -> tuple[list[DirectLaunchView], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(DirectLaunch.id))) or 0)
            launches = list(
                await session.scalars(
                    select(DirectLaunch)
                    .order_by(DirectLaunch.created_at.desc(), DirectLaunch.id.desc())
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return [await self._launch_view(session, item) for item in launches], total

    async def get_launch_view(self, launch_id: int) -> DirectLaunchView:
        async with self._session_factory() as session:
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
            return await self._launch_view(session, launch)

    @staticmethod
    async def _launch_view(session: AsyncSession, launch: DirectLaunch) -> DirectLaunchView:
        campaign_name = await session.scalar(
            select(DirectCampaign.name).where(DirectCampaign.id == launch.campaign_id)
        )
        list_name = await session.scalar(
            select(SubscriberList.name).where(SubscriberList.id == launch.subscriber_list_id)
        )
        counts = dict(
            (
                await session.execute(
                    select(DirectMessageTask.status, func.count(DirectMessageTask.id))
                    .where(DirectMessageTask.launch_id == launch.id)
                    .group_by(DirectMessageTask.status)
                )
            ).all()
        )
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
                f"@{error_row.username}"
                if error_row and error_row.username
                else (
                    f"ID {error_row.telegram_user_id}"
                    if error_row and error_row.telegram_user_id is not None
                    else None
                )
            ),
        )

    async def cancel_launch(self, launch_id: int) -> int:
        now = utc_now()
        async with self._session_factory() as session:
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
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
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id.in_(queued_subscriber_ids),
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
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="step_timeout",
                error="личное сообщение не завершилось вовремя; автоматический дубль запрещён",
                conservative_cooldown=True,
            )
        except Exception as exc:  # noqa: BLE001 - keep the durable worker alive
            logger.exception("Direct message task %s failed unexpectedly", task.id)
            return await self._finish_task(
                task.id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="unhandled_execution_error",
                error=safe_error_message(exc),
            )
        finally:
            heartbeat_stop.set()
            await heartbeat
            self._active_steps -= 1

    async def _claim_next(self) -> DirectMessageTask | None:
        now = utc_now()
        lease_token = uuid4().hex
        lease_until = now + timedelta(seconds=max(30, self._settings.worker_lease_seconds))
        recovered_launch_ids: set[int] = set()
        async with self._session_factory() as session:
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
                        DirectLaunch.cancel_requested.is_(True),
                    )
                )
            )
            cancelled_task_ids = [int(task.id) for task in cancelled_expired_tasks]
            for task in cancelled_expired_tasks:
                task.status = DirectMessageTaskStatus.FAILED
                task.error_code = "cancelled_outcome_unknown"
                task.error = (
                    "worker завершился после отмены запуска; результат уже начатой "
                    "отправки в Telegram неизвестен"
                )
                task.finished_at = now
                self._release_task_lease(task)
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id == task.subscriber_id,
                        Subscriber.reserved_direct_launch_id == task.launch_id,
                    )
                    .values(reserved_direct_launch_id=None, updated_at=now)
                )
                recovered_launch_ids.add(int(task.launch_id))
            await session.execute(
                update(DirectMessageTask)
                .where(
                    DirectMessageTask.status == DirectMessageTaskStatus.RUNNING,
                    DirectMessageTask.lease_until.is_not(None),
                    DirectMessageTask.lease_until <= now,
                    DirectMessageTask.id.not_in(cancelled_task_ids),
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
            if subscriber.do_not_contact:
                return await self._finish_task(
                    task_id,
                    lease_token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_opted_out",
                    error="получатель находится в списке «не писать»",
                )
            if await session.get(MentionBlacklistEntry, subscriber.telegram_user_id) is not None:
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
        await self._refresh_live_proxy_routes_for_accounts(account_ids)
        async with self._session_factory() as session:
            healthy_proxy_account_ids = await self._healthy_proxy_account_ids(
                session,
                account_ids,
            )
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
        has_proxy_blocked_sender = False
        for account in accounts:
            peer = peers.get(int(account.id))
            if peer is not None and peer.send_blocked:
                continue
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
            candidates.append((self._sender_ready_at(account), account, peer))
        if not candidates:
            if has_proxy_blocked_sender:
                return await self._fail_launch(
                    task_id,
                    lease_token,
                    "proxy_unavailable",
                    "обязательный прокси выбранного аккаунта недоступен",
                )
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code="recipient_unresolvable",
                error="ни один выбранный аккаунт не может безопасно разрешить получателя",
            )
        now = utc_now()
        candidates.sort(key=lambda item: (item[0] is not None, item[0] or now, int(item[1].id)))
        resume_at, account, peer = candidates[0]
        if resume_at is not None and resume_at > now:
            return await self._reschedule_task(
                task_id,
                lease_token,
                scheduled_for=resume_at,
                error_code="account_pacing",
                error="отправка ждёт свободный лимит аккаунта",
                increment_attempt=False,
            )

        recipient = DirectRecipient(
            telegram_user_id=subscriber.telegram_user_id,
            username=subscriber.username,
            access_hash=peer.access_hash if peer is not None else None,
        )

        async def send() -> DirectMessageDelivery:
            stop_code = await self._pre_send_stop_code(
                task_id,
                lease_token,
                launch_id=launch.id,
                subscriber_id=subscriber.id,
                telegram_user_id=subscriber.telegram_user_id,
            )
            if stop_code is not None:
                raise PermanentGatewayError(
                    "отправка отменена повторной проверкой состояния задачи",
                    code=stop_code,
                )
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
            return delivery

        try:
            delivery = await self._runner.run_account_send_operation(int(account.id), send)
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
        if delivery.telegram_user_id != subscriber.telegram_user_id:
            return await self._finish_task(
                task_id,
                lease_token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="recipient_identity_mismatch",
                error="шлюз вернул доставку другому Telegram ID; повтор заблокирован",
                sender_account_id=int(account.id),
                conservative_cooldown=True,
            )
        return await self._record_success(
            task_id,
            lease_token,
            subscriber_id=subscriber.id,
            account_id=int(account.id),
            delivery=delivery,
        )

    async def _handle_gateway_failure(
        self,
        task_id: int,
        lease_token: str,
        *,
        subscriber_id: int,
        account_id: int,
        exc: GatewayError,
    ) -> DirectMessageTask:
        code = failure_code(exc, "gateway_error")
        message = safe_error_message(exc)[:500]
        if code in ACCOUNT_WIDE_CODES:
            await self._runner.handle_external_account_failure(account_id, exc)
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
            )
        if code == "delivery_unconfirmed":
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

    async def _pre_send_stop_code(
        self,
        task_id: int,
        lease_token: str,
        *,
        launch_id: int,
        subscriber_id: int,
        telegram_user_id: int,
    ) -> str | None:
        """Close cancellation/stop-list races while an account send slot is awaited."""

        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token)
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
            if await session.get(MentionBlacklistEntry, telegram_user_id) is not None:
                return "recipient_blacklisted"
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
        delivery: DirectMessageDelivery,
    ) -> DirectMessageTask:
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, lease_token)
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None:
                raise NotFoundError("получатель исчез после доставки")
            task.status = DirectMessageTaskStatus.SUCCEEDED
            task.sender_account_id = account_id
            task.external_message_id = delivery.message_id
            task.error_code = None
            task.error = None
            task.finished_at = now
            self._release_task_lease(task)
            subscriber.last_contacted_at = now
            subscriber.last_delivery_uncertain_at = None
            subscriber.message_count += 1
            subscriber.last_error_code = None
            subscriber.last_error = None
            subscriber.reserved_direct_launch_id = None
            if delivery.username is not None:
                subscriber.username = delivery.username
            if delivery.is_premium is not None:
                subscriber.is_premium = delivery.is_premium
                subscriber.premium_checked_at = now
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
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberAccountPeer.subscriber_id,
                        SubscriberAccountPeer.account_id,
                    ],
                    set_={
                        "access_hash": func.coalesce(
                            statement.excluded.access_hash,
                            SubscriberAccountPeer.access_hash,
                        ),
                        "send_blocked": False,
                        "blocked_reason": None,
                        "last_error": None,
                        "resolved_at": now,
                        "updated_at": now,
                    },
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
            task = await self._require_task(session, task_id, lease_token)
            task.status = DirectMessageTaskStatus.QUEUED
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
    ) -> DirectMessageTask:
        if status not in DIRECT_MESSAGE_TERMINAL_STATUSES:
            raise ValueError("terminal task status required")
        now = utc_now()
        async with self._session_factory() as session:
            task = await session.get(DirectMessageTask, task_id)
            if task is None:
                raise NotFoundError("задача личного сообщения не найдена")
            if lease_token is not None and task.lease_token != lease_token:
                raise ConflictError("задача потеряла токен выполнения")
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
            await session.execute(
                update(Subscriber)
                .where(
                    Subscriber.id == task.subscriber_id,
                    Subscriber.reserved_direct_launch_id == task.launch_id,
                )
                .values(**subscriber_values)
            )
            launch_id = task.launch_id
            await session.commit()
            await session.refresh(task)
        await self._refresh_launch(launch_id)
        return task

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
            task = await self._require_task(session, task_id, lease_token)
            launch = await session.get(DirectLaunch, task.launch_id)
            if launch is None:
                raise NotFoundError("запуск ЛС-рассылки не найден")
            queued_subscriber_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(DirectMessageTask.subscriber_id).where(
                        DirectMessageTask.launch_id == launch.id,
                        DirectMessageTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                )
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
            await session.execute(
                update(Subscriber)
                .where(
                    Subscriber.id.in_((*queued_subscriber_ids, int(task.subscriber_id))),
                    Subscriber.reserved_direct_launch_id == launch.id,
                )
                .values(reserved_direct_launch_id=None, updated_at=now)
            )
            launch.cancel_requested = True
            launch.status = DirectLaunchStatus.FAILED
            launch.finished_at = now
            await session.commit()
            await session.refresh(task)
            return task

    async def _refresh_launch(self, launch_id: int) -> None:
        async with self._session_factory() as session:
            launch = await session.get(DirectLaunch, launch_id)
            if launch is None:
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
                if succeeded and failed_or_skipped:
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
        lease_token: str,
    ) -> DirectMessageTask:
        task = await session.get(DirectMessageTask, task_id)
        if task is None:
            raise NotFoundError("задача личного сообщения не найдена")
        if task.status != DirectMessageTaskStatus.RUNNING or task.lease_token != lease_token:
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
