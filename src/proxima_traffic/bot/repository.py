from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from sqlalchemy import String, case, cast, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.content import (
    MAX_ALBUM_ITEMS,
    MAX_CONTENT_VARIANTS,
    BroadcastContentVariant,
    ContentError,
    StoredContact,
    StoredMedia,
    new_content_variant,
    parse_content_variants,
    serialize_content_variants,
    validate_spintax,
    validate_telegram_text,
)
from proxima_traffic.dialogues import (
    parse_dialogue_steps,
    serialize_dialogue_steps,
    validate_dialogue_steps,
)
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    ActionLog,
    ActionStatus,
    AuditEvent,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    MessageTemplate,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyStatus,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    Target,
    TargetAccountState,
    TargetList,
    TargetListItem,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import (
    TARGET_QUALITY_BAD,
    TARGET_QUALITY_LEARNING,
    ConflictError,
    NotFoundError,
    add_audit_event,
)


@dataclass(frozen=True, slots=True)
class DashboardStats:
    accounts_total: int
    accounts_active: int
    targets_total: int
    targets_approved: int
    target_lists_total: int
    templates_total: int
    campaigns_total: int
    queued_runs: int
    running_runs: int
    succeeded_runs: int
    failed_runs: int
    accounts_ready: int = 0
    accounts_with_healthy_proxy: int = 0
    proxies_total: int = 0
    proxies_healthy: int = 0
    proxies_quarantined: int = 0
    proxies_assigned: int = 0
    active_launches: int = 0
    smart_active_chats: int = 0
    sent_messages_24h: int = 0
    failed_messages_24h: int = 0
    blocked_pairs: int = 0
    next_scheduled_at: datetime | None = None
    standalone_targets_total: int = 0
    standalone_targets_approved: int = 0
    audience_member_count: int = 0
    audience_enabled_count: int = 0
    bad_targets: int = 0


@dataclass(frozen=True, slots=True)
class Page[T]:
    items: list[T]
    page: int
    page_size: int
    total: int

    @property
    def pages(self) -> int:
        return max(1, (self.total + self.page_size - 1) // self.page_size)


@dataclass(frozen=True, slots=True)
class TargetListView:
    id: int
    name: str
    item_count: int
    approved_count: int
    bad_count: int = 0
    problem_count: int = 0

    @property
    def disabled_count(self) -> int:
        return max(0, self.item_count - self.approved_count - self.bad_count)


@dataclass(frozen=True, slots=True)
class TargetSeed:
    chat_ref: str
    title: str
    chat_type: str | None = None
    folder_slug: str | None = None
    telegram_peer_id: int | None = None


@dataclass(frozen=True, slots=True)
class TargetTimingView:
    target_id: int
    account_id: int
    account_label: str
    slowmode_seconds: int | None
    next_send_at: datetime | None
    last_checked_at: datetime | None
    last_success_at: datetime | None
    last_message_id: int | None
    adaptive_interval_seconds: int | None = None
    activity_messages_per_minute: float | None = None
    adaptive_tier: str | None = None
    activity_sampled_at: datetime | None = None
    send_blocked: bool = False
    blocked_reason: str | None = None
    blocked_at: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class SenderFailoverView:
    run_id: int
    target_id: int
    failed_account_id: int
    replacement_account_id: int
    occurred_at: datetime
    scheduled_for: datetime | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class LaunchView:
    anchor_run_id: int
    campaign_id: int
    campaign_name: str
    mode: CampaignMode
    total: int
    queued: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    created_at: datetime
    first_scheduled_for: datetime
    last_scheduled_for: datetime
    smart_timer_mode: bool = False
    active_until: datetime | None = None
    no_timer_interval_seconds: int = 0
    auto_sender_failover: bool = False
    sender_mode: BroadcastSenderMode = BroadcastSenderMode.DISTRIBUTED
    sent_publications: int = 0
    next_scheduled_for: datetime | None = None
    target_count: int = 0
    account_count: int = 0
    failover_count: int = 0
    account_labels: tuple[tuple[int, str], ...] = ()
    failovers: tuple[SenderFailoverView, ...] = ()
    publications: tuple[ActionLog, ...] = ()
    attempt_failures: tuple[ActionLog, ...] = ()
    timings: tuple[TargetTimingView, ...] = ()
    archived: bool = False
    reaction_queued: int = 0
    reaction_running: int = 0
    reaction_succeeded: int = 0
    reaction_failed: int = 0
    reaction_skipped: int = 0
    reaction_cancelled: int = 0
    reaction_last_error: str | None = None

    @property
    def active(self) -> int:
        return self.queued + self.running

    @property
    def reaction_active(self) -> int:
        return self.reaction_queued + self.reaction_running

    @property
    def pending_work(self) -> int:
        return self.active + self.reaction_active

    @property
    def finished(self) -> int:
        return self.succeeded + self.failed + self.cancelled


def _launch_membership(anchor: CampaignRun) -> object:
    """Keep modern launch keys separate from legacy per-run request keys."""
    if anchor.launch_key is not None:
        return CampaignRun.launch_key == anchor.launch_key
    return CampaignRun.launch_key.is_(None) & (CampaignRun.request_key == anchor.request_key)


class AdminRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record_backup_created(
        self,
        *,
        path: str,
        size_bytes: int,
        sha256: str,
    ) -> None:
        async with self._session_factory() as session:
            add_audit_event(
                session,
                event_type="maintenance.backup_created",
                entity_type="database",
                entity_id=None,
                details={
                    "path": path,
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                },
                actor="control-bot",
            )
            await session.commit()

    async def dashboard(self) -> DashboardStats:
        async with self._session_factory() as session:
            now = utc_now()
            since = now - timedelta(hours=24)
            active_launch_groups = (
                select(
                    CampaignRun.launch_key,
                    case(
                        (CampaignRun.launch_key.is_(None), CampaignRun.request_key),
                        else_=None,
                    ).label("legacy_request_key"),
                )
                .where(CampaignRun.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]))
                .group_by(CampaignRun.launch_key, "legacy_request_key")
                .subquery()
            )
            ready_conditions = [
                Account.status == AccountStatus.ACTIVE,
                Account.ownership_confirmed.is_(True),
                Account.session_source != AccountSessionSource.NONE,
                Account.session_encrypted.is_not(None),
                Account.api_id.is_not(None),
                Account.api_hash_encrypted.is_not(None),
                Account.authorized.is_(True),
            ]
            ready_account_ids = list(
                await session.scalars(select(Account.id).where(*ready_conditions))
            )
            healthy_route_count = 0
            if ready_account_ids:
                healthy_route_count = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(ProxyAssignment)
                        .join(ProxyEndpoint, ProxyEndpoint.id == ProxyAssignment.proxy_id)
                        .where(
                            ProxyAssignment.assignment_key.in_(
                                [f"account:{account_id}" for account_id in ready_account_ids]
                            ),
                            ProxyEndpoint.enabled.is_(True),
                            ProxyEndpoint.status == ProxyStatus.HEALTHY,
                        )
                    )
                    or 0
                )
            return DashboardStats(
                accounts_total=await self._count(session, Account),
                accounts_active=await self._count(
                    session, Account, Account.status == AccountStatus.ACTIVE
                ),
                targets_total=await self._count(session, Target),
                targets_approved=await self._count(
                    session, Target, Target.approved_by_operator.is_(True)
                ),
                target_lists_total=await self._count(session, TargetList),
                templates_total=await self._count(session, MessageTemplate),
                campaigns_total=await self._count(session, Campaign),
                queued_runs=await self._count(
                    session, CampaignRun, CampaignRun.status == RunStatus.QUEUED
                ),
                running_runs=await self._count(
                    session, CampaignRun, CampaignRun.status == RunStatus.RUNNING
                ),
                succeeded_runs=await self._count(
                    session, CampaignRun, CampaignRun.status == RunStatus.SUCCEEDED
                ),
                failed_runs=await self._count(
                    session, CampaignRun, CampaignRun.status == RunStatus.FAILED
                ),
                accounts_ready=len(ready_account_ids),
                accounts_with_healthy_proxy=healthy_route_count,
                proxies_total=await self._count(session, ProxyEndpoint),
                proxies_healthy=await self._count(
                    session, ProxyEndpoint, ProxyEndpoint.status == ProxyStatus.HEALTHY
                ),
                proxies_quarantined=await self._count(
                    session, ProxyEndpoint, ProxyEndpoint.status == ProxyStatus.QUARANTINED
                ),
                proxies_assigned=await self._count(session, ProxyAssignment),
                active_launches=int(
                    await session.scalar(select(func.count()).select_from(active_launch_groups))
                    or 0
                ),
                smart_active_chats=int(
                    await session.scalar(
                        select(func.count(func.distinct(CampaignRun.target_id))).where(
                            CampaignRun.repeat_until.is_not(None),
                            CampaignRun.status.in_([RunStatus.QUEUED, RunStatus.RUNNING]),
                        )
                    )
                    or 0
                ),
                sent_messages_24h=await self._count(
                    session,
                    ActionLog,
                    (
                        (ActionLog.status == ActionStatus.SUCCEEDED)
                        & (ActionLog.created_at >= since)
                    ),
                ),
                failed_messages_24h=await self._count(
                    session,
                    ActionLog,
                    ((ActionLog.status == ActionStatus.FAILED) & (ActionLog.created_at >= since)),
                ),
                blocked_pairs=await self._count(
                    session,
                    TargetAccountState,
                    TargetAccountState.send_blocked.is_(True),
                ),
                next_scheduled_at=await session.scalar(
                    select(func.min(CampaignRun.scheduled_for)).where(
                        CampaignRun.status == RunStatus.QUEUED
                    )
                ),
                standalone_targets_total=await self._count(
                    session,
                    Target,
                    Target.is_standalone.is_(True),
                ),
                standalone_targets_approved=await self._count(
                    session,
                    Target,
                    (
                        Target.is_standalone.is_(True)
                        & Target.approved_by_operator.is_(True)
                        & (Target.quality_status != TARGET_QUALITY_BAD)
                    ),
                ),
                audience_member_count=int(
                    await session.scalar(
                        select(func.count(func.distinct(TargetListItem.target_id)))
                    )
                    or 0
                ),
                audience_enabled_count=int(
                    await session.scalar(
                        select(func.count(func.distinct(TargetListItem.target_id)))
                        .join(Target, Target.id == TargetListItem.target_id)
                        .where(
                            TargetListItem.enabled.is_(True),
                            Target.quality_status != TARGET_QUALITY_BAD,
                        )
                    )
                    or 0
                ),
                bad_targets=await self._count(
                    session,
                    Target,
                    Target.quality_status == TARGET_QUALITY_BAD,
                ),
            )

    async def list_accounts(self, page: int, page_size: int) -> Page[Account]:
        return await self._page(
            Account,
            page,
            page_size,
            Account.id,
            descending=True,
        )

    async def active_owned_accounts(
        self, *, require_authorized_session: bool = False
    ) -> list[Account]:
        async with self._session_factory() as session:
            conditions = [
                Account.status == AccountStatus.ACTIVE,
                Account.ownership_confirmed.is_(True),
            ]
            if require_authorized_session:
                conditions.extend(
                    [
                        Account.session_source != AccountSessionSource.NONE,
                        Account.session_encrypted.is_not(None),
                        Account.api_id.is_not(None),
                        Account.api_hash_encrypted.is_not(None),
                        Account.authorized.is_(True),
                    ]
                )
            return list(
                await session.scalars(select(Account).where(*conditions).order_by(Account.id))
            )

    async def existing_account_profile_names(self) -> set[str]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(Account.first_name, Account.last_name).where(
                        or_(Account.first_name.is_not(None), Account.last_name.is_not(None))
                    )
                )
            ).all()
        return {
            " ".join(value for value in (first_name, last_name) if value)
            for first_name, last_name in rows
            if first_name or last_name
        }

    async def create_account(
        self,
        *,
        label: str,
        telegram_user_id: int | None,
        ownership_confirmed: bool,
    ) -> Account:
        async with self._session_factory() as session:
            account = Account(
                label=label,
                telegram_user_id=telegram_user_id,
                ownership_confirmed=ownership_confirmed,
            )
            session.add(account)
            await self._flush_or_conflict(session, "account label or Telegram ID already exists")
            add_audit_event(
                session,
                event_type="account.created",
                entity_type="account",
                entity_id=account.id,
                details={"label": label, "ownership_confirmed": ownership_confirmed},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(account)
            return account

    async def toggle_account(self, account_id: int) -> Account:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                raise NotFoundError("account not found")
            account.status = (
                AccountStatus.PAUSED
                if account.status == AccountStatus.ACTIVE
                else AccountStatus.ACTIVE
            )
            add_audit_event(
                session,
                event_type="account.status_changed",
                entity_type="account",
                entity_id=account.id,
                details={"status": account.status.value},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(account)
            return account

    async def list_targets(self, page: int, page_size: int) -> Page[Target]:
        normalized_page = max(page, 0)
        async with self._session_factory() as session:
            condition = Target.is_standalone.is_(True)
            total = await self._count(session, Target, condition)
            items = list(
                await session.scalars(
                    select(Target)
                    .where(condition)
                    .order_by(Target.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            )
            return Page(items, normalized_page, page_size, total)

    async def list_bad_targets(self, page: int, page_size: int) -> Page[Target]:
        normalized_page = max(page, 0)
        async with self._session_factory() as session:
            condition = Target.quality_status == TARGET_QUALITY_BAD
            total = await self._count(session, Target, condition)
            items = list(
                await session.scalars(
                    select(Target)
                    .where(condition)
                    .order_by(Target.quality_bad_at.desc(), Target.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            )
            return Page(items, normalized_page, page_size, total)

    async def search_targets(
        self,
        query: str,
        limit: int = 20,
        *,
        approved_only: bool = False,
    ) -> list[Target]:
        normalized = query.strip()
        if not normalized:
            return []
        conditions = [
            Target.title.ilike(f"%{normalized}%"),
            Target.chat_ref.ilike(f"%{normalized}%"),
        ]
        if normalized.isdecimal():
            conditions.append(Target.id == int(normalized))
        filters = [Target.is_standalone.is_(True), or_(*conditions)]
        if approved_only:
            filters.extend(
                [
                    Target.approved_by_operator.is_(True),
                    Target.quality_status != TARGET_QUALITY_BAD,
                ]
            )
        async with self._session_factory() as session:
            return list(
                await session.scalars(
                    select(Target)
                    .where(*filters)
                    .order_by(Target.id.desc())
                    .limit(max(1, min(limit, 50)))
                )
            )

    async def approved_targets(self) -> list[Target]:
        async with self._session_factory() as session:
            return list(
                await session.scalars(
                    select(Target)
                    .where(
                        Target.approved_by_operator.is_(True),
                        Target.is_standalone.is_(True),
                        Target.quality_status != TARGET_QUALITY_BAD,
                    )
                    .order_by(Target.id.desc())
                )
            )

    async def list_approved_targets(self, page: int, page_size: int) -> Page[Target]:
        normalized_page = max(page, 0)
        async with self._session_factory() as session:
            condition = (
                Target.approved_by_operator.is_(True)
                & Target.is_standalone.is_(True)
                & (Target.quality_status != TARGET_QUALITY_BAD)
            )
            total = int(await self._count(session, Target, condition) or 0)
            items = list(
                await session.scalars(
                    select(Target)
                    .where(condition)
                    .order_by(Target.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            )
            return Page(items=items, page=normalized_page, page_size=page_size, total=total)

    async def list_target_lists(self, page: int, page_size: int) -> Page[TargetListView]:
        normalized_page = max(page, 0)
        async with self._session_factory() as session:
            total = await self._count(session, TargetList)
            item_count = func.count(TargetListItem.id)
            approved_count = func.sum(
                case(
                    (
                        TargetListItem.enabled.is_(True)
                        & (Target.quality_status != TARGET_QUALITY_BAD),
                        1,
                    ),
                    else_=0,
                )
            )
            bad_count = func.sum(case((Target.quality_status == TARGET_QUALITY_BAD, 1), else_=0))
            problem_count = func.sum(
                case(
                    (
                        (Target.quality_status != TARGET_QUALITY_BAD)
                        & Target.last_error_code.is_not(None),
                        1,
                    ),
                    else_=0,
                )
            )
            rows = (
                await session.execute(
                    select(
                        TargetList.id,
                        TargetList.name,
                        item_count.label("item_count"),
                        approved_count.label("approved_count"),
                        bad_count.label("bad_count"),
                        problem_count.label("problem_count"),
                    )
                    .outerjoin(
                        TargetListItem,
                        TargetListItem.target_list_id == TargetList.id,
                    )
                    .outerjoin(Target, Target.id == TargetListItem.target_id)
                    .group_by(TargetList.id, TargetList.name)
                    .order_by(TargetList.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            ).all()
            items = [
                TargetListView(
                    id=int(row.id),
                    name=str(row.name),
                    item_count=int(row.item_count or 0),
                    approved_count=int(row.approved_count or 0),
                    bad_count=int(row.bad_count or 0),
                    problem_count=int(row.problem_count or 0),
                )
                for row in rows
            ]
            return Page(items=items, page=normalized_page, page_size=page_size, total=total)

    async def search_target_lists(
        self,
        query: str,
        limit: int = 20,
        *,
        approved_only: bool = False,
    ) -> list[TargetListView]:
        normalized = query.strip()
        if not normalized:
            return []
        conditions = [TargetList.name.ilike(f"%{normalized}%")]
        if normalized.isdecimal():
            conditions.append(TargetList.id == int(normalized))
        item_count = func.count(TargetListItem.id)
        approved_count = func.sum(
            case(
                (
                    TargetListItem.enabled.is_(True)
                    & (Target.quality_status != TARGET_QUALITY_BAD),
                    1,
                ),
                else_=0,
            )
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        TargetList.id,
                        TargetList.name,
                        item_count.label("item_count"),
                        approved_count.label("approved_count"),
                    )
                    .outerjoin(
                        TargetListItem,
                        TargetListItem.target_list_id == TargetList.id,
                    )
                    .outerjoin(Target, Target.id == TargetListItem.target_id)
                    .where(or_(*conditions))
                    .group_by(TargetList.id, TargetList.name)
                    .having(approved_count > 0 if approved_only else item_count >= 0)
                    .order_by(TargetList.id.desc())
                    .limit(max(1, min(limit, 50)))
                )
            ).all()
        return [
            TargetListView(
                id=int(row.id),
                name=str(row.name),
                item_count=int(row.item_count or 0),
                approved_count=int(row.approved_count or 0),
            )
            for row in rows
        ]

    async def available_target_lists(self) -> list[TargetListView]:
        page = await self.list_target_lists(0, 500)
        return [item for item in page.items if item.approved_count > 0]

    async def list_available_target_lists(
        self,
        page: int,
        page_size: int,
    ) -> Page[TargetListView]:
        normalized_page = max(page, 0)
        item_count = func.count(TargetListItem.id)
        approved_count = func.sum(
            case(
                (
                    TargetListItem.enabled.is_(True)
                    & (Target.quality_status != TARGET_QUALITY_BAD),
                    1,
                ),
                else_=0,
            )
        )
        grouped = (
            select(
                TargetList.id.label("id"),
                TargetList.name.label("name"),
                item_count.label("item_count"),
                approved_count.label("approved_count"),
            )
            .outerjoin(
                TargetListItem,
                TargetListItem.target_list_id == TargetList.id,
            )
            .outerjoin(Target, Target.id == TargetListItem.target_id)
            .group_by(TargetList.id, TargetList.name)
            .having(approved_count > 0)
        )
        async with self._session_factory() as session:
            total = int(
                await session.scalar(select(func.count()).select_from(grouped.subquery())) or 0
            )
            rows = (
                await session.execute(
                    grouped.order_by(TargetList.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            ).all()
            return Page(
                items=[
                    TargetListView(
                        id=int(row.id),
                        name=str(row.name),
                        item_count=int(row.item_count or 0),
                        approved_count=int(row.approved_count or 0),
                    )
                    for row in rows
                ],
                page=normalized_page,
                page_size=page_size,
                total=total,
            )

    async def get_target_list_view(self, target_list_id: int) -> TargetListView:
        item_count = func.count(TargetListItem.id)
        approved_count = func.sum(
            case(
                (
                    TargetListItem.enabled.is_(True)
                    & (Target.quality_status != TARGET_QUALITY_BAD),
                    1,
                ),
                else_=0,
            )
        )
        bad_count = func.sum(case((Target.quality_status == TARGET_QUALITY_BAD, 1), else_=0))
        problem_count = func.sum(
            case(
                (
                    (Target.quality_status != TARGET_QUALITY_BAD)
                    & Target.last_error_code.is_not(None),
                    1,
                ),
                else_=0,
            )
        )
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(
                        TargetList.id,
                        TargetList.name,
                        item_count.label("item_count"),
                        approved_count.label("approved_count"),
                        bad_count.label("bad_count"),
                        problem_count.label("problem_count"),
                    )
                    .outerjoin(
                        TargetListItem,
                        TargetListItem.target_list_id == TargetList.id,
                    )
                    .outerjoin(Target, Target.id == TargetListItem.target_id)
                    .where(TargetList.id == target_list_id)
                    .group_by(TargetList.id, TargetList.name)
                )
            ).one_or_none()
        if row is None:
            raise NotFoundError("target list not found")
        return TargetListView(
            id=int(row.id),
            name=str(row.name),
            item_count=int(row.item_count or 0),
            approved_count=int(row.approved_count or 0),
            bad_count=int(row.bad_count or 0),
            problem_count=int(row.problem_count or 0),
        )

    async def rename_target_list(self, target_list_id: int, name: str) -> TargetListView:
        normalized = " ".join(name.split())
        if not 1 <= len(normalized) <= 150:
            raise ConflictError("название списка должно содержать от 1 до 150 символов")
        async with self._session_factory() as session:
            target_list = await session.get(TargetList, target_list_id)
            if target_list is None:
                raise NotFoundError("target list not found")
            previous_name = target_list.name
            target_list.name = normalized
            await self._flush_or_conflict(session, "список с таким названием уже существует")
            add_audit_event(
                session,
                event_type="target_list.renamed",
                entity_type="target_list",
                entity_id=target_list_id,
                details={"from": previous_name, "to": normalized},
                actor="control-bot",
            )
            await session.commit()
        return await self.get_target_list_view(target_list_id)

    async def list_target_list_targets(
        self,
        target_list_id: int,
        page: int,
        page_size: int,
        *,
        filter_name: str = "all",
    ) -> Page[Target]:
        normalized_page = max(page, 0)
        if filter_name not in {"all", "enabled", "disabled", "bad", "problem"}:
            raise ConflictError("unknown target list filter")
        async with self._session_factory() as session:
            if await session.get(TargetList, target_list_id) is None:
                raise NotFoundError("target list not found")
            conditions = [TargetListItem.target_list_id == target_list_id]
            if filter_name == "enabled":
                conditions.extend(
                    [
                        TargetListItem.enabled.is_(True),
                        Target.quality_status != TARGET_QUALITY_BAD,
                    ]
                )
            elif filter_name == "disabled":
                conditions.extend(
                    [
                        TargetListItem.enabled.is_(False),
                        Target.quality_status != TARGET_QUALITY_BAD,
                    ]
                )
            elif filter_name == "bad":
                conditions.append(Target.quality_status == TARGET_QUALITY_BAD)
            elif filter_name == "problem":
                conditions.extend(
                    [
                        Target.quality_status != TARGET_QUALITY_BAD,
                        Target.last_error_code.is_not(None),
                    ]
                )
            total = int(
                await session.scalar(
                    select(func.count())
                    .select_from(TargetListItem)
                    .join(Target, Target.id == TargetListItem.target_id)
                    .where(*conditions)
                )
                or 0
            )
            rows = (
                await session.execute(
                    select(Target, TargetListItem.enabled)
                    .join(TargetListItem, TargetListItem.target_id == Target.id)
                    .where(*conditions)
                    .order_by(func.lower(Target.title), Target.id)
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            ).all()
            items: list[Target] = []
            for target, enabled in rows:
                target.approved_by_operator = bool(enabled)
                items.append(target)
            return Page(
                items=items,
                page=normalized_page,
                page_size=page_size,
                total=total,
            )

    async def search_target_list_targets(
        self,
        target_list_id: int,
        query: str,
        limit: int = 20,
    ) -> list[Target]:
        normalized = query.strip()
        if not normalized:
            return []
        search_conditions = [
            Target.title.ilike(f"%{normalized}%"),
            Target.chat_ref.ilike(f"%{normalized}%"),
        ]
        if normalized.isdecimal():
            search_conditions.append(Target.id == int(normalized))
        async with self._session_factory() as session:
            if await session.get(TargetList, target_list_id) is None:
                raise NotFoundError("target list not found")
            rows = (
                await session.execute(
                    select(Target, TargetListItem.enabled)
                    .join(TargetListItem, TargetListItem.target_id == Target.id)
                    .where(
                        TargetListItem.target_list_id == target_list_id,
                        or_(*search_conditions),
                    )
                    .order_by(func.lower(Target.title), Target.id)
                    .limit(max(1, min(limit, 50)))
                )
            ).all()
            items: list[Target] = []
            for target, enabled in rows:
                target.approved_by_operator = bool(enabled)
                items.append(target)
            return items

    async def set_target_list_targets_approval(
        self,
        target_list_id: int,
        target_ids: list[int],
        approved: bool,
    ) -> tuple[int, int]:
        unique_ids = sorted(set(target_ids))
        if not unique_ids:
            return 0, 0
        async with self._session_factory() as session:
            if await session.get(TargetList, target_list_id) is None:
                raise NotFoundError("target list not found")
            rows = (
                await session.execute(
                    select(TargetListItem, Target.quality_status)
                    .join(Target, Target.id == TargetListItem.target_id)
                    .where(
                        TargetListItem.target_list_id == target_list_id,
                        TargetListItem.target_id.in_(unique_ids),
                    )
                )
            ).all()
            changed = 0
            skipped_bad = 0
            for item, quality_status in rows:
                if approved and quality_status == TARGET_QUALITY_BAD:
                    skipped_bad += 1
                    # Quarantine wins over every bulk-enable action. Keeping
                    # the membership row disabled also prevents an accidental
                    # reactivation if the quality label is later cleared.
                    item.enabled = False
                    continue
                if item.enabled != approved:
                    item.enabled = approved
                    changed += 1
            add_audit_event(
                session,
                event_type="target_list.page_approval_changed",
                entity_type="target_list",
                entity_id=target_list_id,
                details={
                    "target_ids": unique_ids,
                    "approved": approved,
                    "changed": changed,
                    "skipped_bad": skipped_bad,
                },
                actor="control-bot",
            )
            await session.commit()
            return changed, skipped_bad

    async def get_target_list_target(self, target_list_id: int, target_id: int) -> Target:
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(Target, TargetListItem.enabled)
                    .join(TargetListItem, TargetListItem.target_id == Target.id)
                    .where(
                        TargetListItem.target_list_id == target_list_id,
                        Target.id == target_id,
                    )
                )
            ).one_or_none()
            if row is None:
                raise NotFoundError("target is not part of this list")
            target, enabled = row
            target.approved_by_operator = bool(enabled)
            return target

    async def set_target_list_target_approval(
        self,
        target_list_id: int,
        target_id: int,
        approved: bool,
    ) -> Target:
        async with self._session_factory() as session:
            item = await session.scalar(
                select(TargetListItem).where(
                    TargetListItem.target_list_id == target_list_id,
                    TargetListItem.target_id == target_id,
                )
            )
            if item is None:
                raise NotFoundError("target is not part of this list")
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("target not found")
            if approved and target.quality_status == TARGET_QUALITY_BAD:
                raise ConflictError(
                    "чат находится в плохих; сначала верните его в работу из этого раздела"
                )
            item.enabled = approved
            add_audit_event(
                session,
                event_type="target_list.target_approval_changed",
                entity_type="target_list",
                entity_id=target_list_id,
                details={"target_id": target_id, "approved": approved},
                actor="control-bot",
            )
            await session.commit()
            target.approved_by_operator = approved
            return target

    async def remove_target_from_list(self, target_list_id: int, target_id: int) -> None:
        async with self._session_factory() as session:
            target_list = await session.get(TargetList, target_list_id)
            if target_list is None:
                raise NotFoundError("target list not found")
            item = await session.scalar(
                select(TargetListItem).where(
                    TargetListItem.target_list_id == target_list_id,
                    TargetListItem.target_id == target_id,
                )
            )
            if item is None:
                raise NotFoundError("target is not part of this list")
            await session.delete(item)
            await session.flush()
            await self._delete_orphan_imported_targets(session, [target_id])
            add_audit_event(
                session,
                event_type="target_list.target_removed",
                entity_type="target_list",
                entity_id=target_list_id,
                details={"target_id": target_id},
                actor="control-bot",
            )
            await session.commit()

    async def delete_target_list(self, target_list_id: int) -> None:
        async with self._session_factory() as session:
            target_list = await session.get(TargetList, target_list_id)
            if target_list is None:
                raise NotFoundError("target list not found")
            campaign_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(Campaign)
                    .where(Campaign.target_list_id == target_list_id)
                )
                or 0
            )
            if campaign_count:
                raise ConflictError(
                    "список используется сохранённой рассылкой или диалогом; "
                    "сначала удалите этот черновик"
                )
            add_audit_event(
                session,
                event_type="target_list.deleted",
                entity_type="target_list",
                entity_id=target_list.id,
                details={"name": target_list.name},
                actor="control-bot",
            )
            target_ids = list(
                await session.scalars(
                    select(TargetListItem.target_id).where(
                        TargetListItem.target_list_id == target_list_id
                    )
                )
            )
            await session.delete(target_list)
            await session.flush()
            deleted_targets = await self._delete_orphan_imported_targets(
                session,
                target_ids,
            )
            add_audit_event(
                session,
                event_type="target_list.orphan_targets_cleaned",
                entity_type="target_list",
                entity_id=target_list.id,
                details={"count": deleted_targets},
                actor="control-bot",
            )
            await session.commit()

    @staticmethod
    async def _delete_orphan_imported_targets(
        session: AsyncSession,
        target_ids: list[int],
    ) -> int:
        """Delete list-owned rows only when no list, scenario or report needs them."""
        if not target_ids:
            return 0
        targets = list(
            await session.scalars(
                select(Target).where(
                    Target.id.in_(target_ids),
                    Target.is_standalone.is_(False),
                )
            )
        )
        candidate_ids = [target.id for target in targets]
        membership_ids = set(
            await session.scalars(
                select(TargetListItem.target_id)
                .where(TargetListItem.target_id.in_(candidate_ids))
                .distinct()
            )
        )
        scenario_ids = set(
            await session.scalars(
                select(Campaign.target_id).where(Campaign.target_id.in_(candidate_ids)).distinct()
            )
        )
        history_ids = set(
            await session.scalars(
                select(CampaignRun.target_id)
                .where(CampaignRun.target_id.in_(candidate_ids))
                .distinct()
            )
        )
        protected_ids = membership_ids | scenario_ids | history_ids
        deleted = 0
        for target in targets:
            if target.id in protected_ids:
                continue
            await session.delete(target)
            deleted += 1
        if deleted:
            await session.flush()
        return deleted

    async def set_target_list_approval(self, target_list_id: int, approved: bool) -> int:
        async with self._session_factory() as session:
            target_list = await session.get(TargetList, target_list_id)
            if target_list is None:
                raise NotFoundError("target list not found")
            rows = (
                await session.execute(
                    select(TargetListItem, Target.quality_status)
                    .join(Target, Target.id == TargetListItem.target_id)
                    .where(TargetListItem.target_list_id == target_list_id)
                )
            ).all()
            changed = 0
            skipped_bad = 0
            for item, quality_status in rows:
                desired = approved
                if approved and quality_status == TARGET_QUALITY_BAD:
                    desired = False
                    skipped_bad += 1
                if item.enabled != desired:
                    item.enabled = desired
                    if quality_status != TARGET_QUALITY_BAD or not approved:
                        changed += 1
            add_audit_event(
                session,
                event_type=(
                    "target_list.targets_approved" if approved else "target_list.targets_revoked"
                ),
                entity_type="target_list",
                entity_id=target_list_id,
                details={
                    "matched": len(rows),
                    "changed": changed,
                    "skipped_bad": skipped_bad,
                },
                actor="control-bot",
            )
            await session.commit()
            return changed

    async def create_target_list(
        self,
        *,
        name: str,
        chat_refs: list[str] | None = None,
        entries: list[TargetSeed] | None = None,
    ) -> tuple[TargetList, int, int]:
        seeds = [
            TargetSeed(chat_ref=value.strip(), title=value.strip())
            for value in (chat_refs or [])
            if value.strip()
        ]
        seeds.extend(entries or [])
        unique_seeds = {
            seed.chat_ref.strip(): TargetSeed(
                chat_ref=seed.chat_ref.strip(),
                title=seed.title.strip()[:150] or seed.chat_ref.strip(),
                chat_type=seed.chat_type,
                folder_slug=seed.folder_slug,
                telegram_peer_id=seed.telegram_peer_id,
            )
            for seed in seeds
            if seed.chat_ref.strip()
        }
        if not unique_seeds:
            raise ConflictError("target list must contain at least one target")
        unique_refs = list(unique_seeds)
        async with self._session_factory() as session:
            target_list = TargetList(name=name)
            session.add(target_list)
            await self._flush_or_conflict(session, "target list name already exists")
            existing_targets = list(
                await session.scalars(select(Target).where(Target.chat_ref.in_(unique_refs)))
            )
            by_ref = {target.chat_ref: target for target in existing_targets}
            added = 0
            for chat_ref in unique_refs:
                seed = unique_seeds[chat_ref]
                if chat_ref not in by_ref:
                    target = Target(
                        chat_ref=chat_ref,
                        title=seed.title,
                        chat_type=seed.chat_type,
                        folder_slug=seed.folder_slug,
                        telegram_peer_id=seed.telegram_peer_id,
                        is_standalone=False,
                    )
                    session.add(target)
                    await session.flush()
                    by_ref[chat_ref] = target
                    added += 1
                else:
                    existing = by_ref[chat_ref]
                    if existing.title == existing.chat_ref and seed.title != seed.chat_ref:
                        existing.title = seed.title
                    if existing.chat_type is None and seed.chat_type is not None:
                        existing.chat_type = seed.chat_type
                    if existing.folder_slug is None and seed.folder_slug is not None:
                        existing.folder_slug = seed.folder_slug
                    if existing.telegram_peer_id is None and seed.telegram_peer_id is not None:
                        existing.telegram_peer_id = seed.telegram_peer_id
                session.add(
                    TargetListItem(
                        target_list_id=target_list.id,
                        target_id=by_ref[chat_ref].id,
                    )
                )
            add_audit_event(
                session,
                event_type="target_list.created",
                entity_type="target_list",
                entity_id=target_list.id,
                details={"name": name, "items": len(unique_refs), "new_targets": added},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(target_list)
            return target_list, added, len(unique_refs) - added

    async def create_target(self, *, chat_ref: str, title: str) -> Target:
        async with self._session_factory() as session:
            existing = await session.scalar(select(Target).where(Target.chat_ref == chat_ref))
            if existing is not None:
                if existing.is_standalone:
                    raise ConflictError("target already exists")
                existing.is_standalone = True
                existing.title = title
                add_audit_event(
                    session,
                    event_type="target.promoted_to_standalone",
                    entity_type="target",
                    entity_id=existing.id,
                    details={"chat_ref": chat_ref},
                    actor="control-bot",
                )
                await session.commit()
                await session.refresh(existing)
                return existing
            target = Target(chat_ref=chat_ref, title=title, is_standalone=True)
            session.add(target)
            await self._flush_or_conflict(session, "target already exists")
            add_audit_event(
                session,
                event_type="target.created",
                entity_type="target",
                entity_id=target.id,
                details={"chat_ref": chat_ref},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(target)
            return target

    async def import_targets(self, chat_refs: list[str]) -> tuple[int, int]:
        unique_refs = list(dict.fromkeys(value.strip() for value in chat_refs if value.strip()))
        if not unique_refs:
            return 0, 0
        async with self._session_factory() as session:
            existing = set(
                await session.scalars(
                    select(Target.chat_ref).where(Target.chat_ref.in_(unique_refs))
                )
            )
            new_refs = [value for value in unique_refs if value not in existing]
            session.add_all(Target(chat_ref=value, title=value) for value in new_refs)
            add_audit_event(
                session,
                event_type="target.batch_imported",
                entity_type="target",
                entity_id=None,
                details={"added": len(new_refs), "skipped": len(unique_refs) - len(new_refs)},
                actor="control-bot",
            )
            await session.commit()
            return len(new_refs), len(unique_refs) - len(new_refs)

    async def set_target_approval(self, target_id: int, approved: bool) -> Target:
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("target not found")
            if approved and target.quality_status == TARGET_QUALITY_BAD:
                raise ConflictError(
                    "чат находится в плохих; сначала верните его в работу из этого раздела"
                )
            target.approved_by_operator = approved
            target.approved_at = utc_now() if approved else None
            add_audit_event(
                session,
                event_type="target.approved" if approved else "target.revoked",
                entity_type="target",
                entity_id=target.id,
                details={"chat_ref": target.chat_ref},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(target)
            return target

    async def set_standalone_targets_approval(self, approved: bool) -> tuple[int, int]:
        """Enable or disable every standalone chat without releasing quarantine."""

        async with self._session_factory() as session:
            targets = list(
                await session.scalars(
                    select(Target).where(Target.is_standalone.is_(True)).order_by(Target.id)
                )
            )
            changed = 0
            skipped_bad = 0
            now = utc_now()
            for target in targets:
                desired = approved
                if approved and target.quality_status == TARGET_QUALITY_BAD:
                    desired = False
                    skipped_bad += 1
                if target.approved_by_operator == desired:
                    continue
                target.approved_by_operator = desired
                target.approved_at = now if desired else None
                if target.quality_status != TARGET_QUALITY_BAD or not approved:
                    changed += 1
            add_audit_event(
                session,
                event_type=(
                    "target.standalone_bulk_approved"
                    if approved
                    else "target.standalone_bulk_revoked"
                ),
                entity_type="target",
                entity_id=None,
                details={
                    "matched": len(targets),
                    "changed": changed,
                    "skipped_bad": skipped_bad,
                },
                actor="control-bot",
            )
            await session.commit()
            return changed, skipped_bad

    async def restore_target_quality(self, target_id: int) -> Target:
        """Start a fresh target-quality window and clear remembered pair bans."""
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("чат не найден")
            now = utc_now()
            previous = {
                "status": target.quality_status,
                "score": target.quality_score,
                "reason": target.quality_reason,
                "attempts": target.quality_attempts,
                "successes": target.quality_successes,
                "failures": target.quality_failures,
                "banned_accounts": target.quality_banned_accounts,
            }
            target.quality_status = TARGET_QUALITY_LEARNING
            target.quality_score = 100
            target.quality_reason = None
            target.quality_attempts = 0
            target.quality_successes = 0
            target.quality_failures = 0
            target.quality_banned_accounts = 0
            target.quality_updated_at = now
            target.quality_bad_at = None
            target.quality_reset_at = now
            target.last_error_code = None
            target.last_error = None
            target.last_error_account_id = None
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(TargetAccountState.target_id == target_id)
                )
            )
            for state in states:
                state.send_blocked = False
                state.blocked_reason = None
                state.blocked_at = None
                state.last_error = None
                state.membership_error_code = None
            add_audit_event(
                session,
                event_type="target.quality_restored",
                entity_type="target",
                entity_id=target_id,
                details={"previous": previous, "cleared_pair_bans": len(states)},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(target)
            return target

    async def set_target_manual_cooldown(
        self,
        target_id: int,
        seconds: int,
    ) -> Target:
        if not 0 <= seconds <= 7 * 24 * 3600:
            raise ConflictError("интервал чата должен быть от 0 до 7 суток")
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("чат не найден")
            target.manual_cooldown_seconds = seconds
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(TargetAccountState.target_id == target_id)
                )
            )
            for state in states:
                last_success = ensure_utc(state.last_success_at)
                if last_success is None:
                    continue
                effective = max(seconds, int(state.slowmode_seconds or 0))
                state.next_send_at = (
                    last_success + timedelta(seconds=effective) if effective else None
                )
            add_audit_event(
                session,
                event_type="target.cooldown_changed",
                entity_type="target",
                entity_id=target.id,
                details={"seconds": seconds},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(target)
            return target

    async def get_target(self, target_id: int) -> Target:
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("target not found")
            return target

    async def target_timings(self, target_id: int) -> list[TargetTimingView]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(TargetAccountState, Account.label)
                    .join(Account, Account.id == TargetAccountState.account_id)
                    .where(TargetAccountState.target_id == target_id)
                    .order_by(Account.label, Account.id)
                )
            ).all()
            return [
                TargetTimingView(
                    target_id=state.target_id,
                    account_id=state.account_id,
                    account_label=str(label),
                    slowmode_seconds=state.slowmode_seconds,
                    next_send_at=state.next_send_at,
                    last_checked_at=state.last_checked_at,
                    last_success_at=state.last_success_at,
                    last_message_id=state.last_message_id,
                    adaptive_interval_seconds=state.adaptive_interval_seconds,
                    activity_messages_per_minute=state.activity_messages_per_minute,
                    adaptive_tier=state.adaptive_tier,
                    activity_sampled_at=state.activity_sampled_at,
                    send_blocked=state.send_blocked,
                    blocked_reason=state.blocked_reason,
                    blocked_at=state.blocked_at,
                    last_error=state.last_error,
                )
                for state, label in rows
            ]

    async def unblock_target_accounts(
        self,
        target_id: int,
        *,
        account_id: int | None = None,
    ) -> int:
        """Explicitly clear remembered send bans for one chat."""
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("чат не найден")
            conditions = [
                TargetAccountState.target_id == target_id,
                TargetAccountState.send_blocked.is_(True),
            ]
            if account_id is not None:
                conditions.append(TargetAccountState.account_id == account_id)
            states = list(await session.scalars(select(TargetAccountState).where(*conditions)))
            for state in states:
                state.send_blocked = False
                state.blocked_reason = None
                state.blocked_at = None
                state.last_error = None
                cooldown_seconds = max(
                    target.manual_cooldown_seconds,
                    state.slowmode_seconds or 0,
                )
                last_success = ensure_utc(state.last_success_at)
                state.next_send_at = (
                    last_success + timedelta(seconds=cooldown_seconds)
                    if last_success is not None and cooldown_seconds > 0
                    else None
                )
            if states:
                add_audit_event(
                    session,
                    event_type="target_account.blocks_cleared",
                    entity_type="target",
                    entity_id=target_id,
                    details={
                        "account_id": account_id,
                        "count": len(states),
                    },
                    actor="control-bot",
                )
                await session.commit()
            return len(states)

    async def delete_target(self, target_id: int) -> None:
        async with self._session_factory() as session:
            target = await session.get(Target, target_id)
            if target is None:
                raise NotFoundError("target not found")
            membership_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(TargetListItem)
                    .where(TargetListItem.target_id == target_id)
                )
                or 0
            )
            campaign_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(Campaign)
                    .where(Campaign.target_id == target_id)
                )
                or 0
            )
            history_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(CampaignRun)
                    .where(CampaignRun.target_id == target_id)
                )
                or 0
            )
            if membership_count or campaign_count or history_count:
                raise ConflictError(
                    "чат входит в список, используется рассылкой или нужен истории; "
                    "его можно выключить, но нельзя удалить"
                )
            add_audit_event(
                session,
                event_type="target.deleted",
                entity_type="target",
                entity_id=target.id,
                details={"chat_ref": target.chat_ref, "title": target.title},
                actor="control-bot",
            )
            await session.delete(target)
            await session.commit()

    async def list_campaigns(self, page: int, page_size: int) -> Page[Campaign]:
        return await self._page(
            Campaign,
            page,
            page_size,
            Campaign.id,
            descending=True,
        )

    async def search_campaigns(self, query: str, limit: int = 20) -> list[Campaign]:
        normalized = query.strip()
        if not normalized:
            return []
        conditions = [Campaign.name.ilike(f"%{normalized}%")]
        if normalized.isdecimal():
            conditions.append(Campaign.id == int(normalized))
        async with self._session_factory() as session:
            return list(
                await session.scalars(
                    select(Campaign)
                    .where(or_(*conditions))
                    .order_by(Campaign.id.desc())
                    .limit(max(1, min(limit, 50)))
                )
            )

    async def list_templates(self, page: int, page_size: int) -> Page[MessageTemplate]:
        return await self._page(
            MessageTemplate,
            page,
            page_size,
            MessageTemplate.id,
            descending=True,
        )

    async def enabled_templates(self, *, require_reply: bool = False) -> list[MessageTemplate]:
        async with self._session_factory() as session:
            conditions = [MessageTemplate.enabled.is_(True)]
            if require_reply:
                conditions.append(func.length(func.trim(MessageTemplate.reply_text)) > 0)
            return list(
                await session.scalars(
                    select(MessageTemplate).where(*conditions).order_by(MessageTemplate.id)
                )
            )

    async def get_template(self, template_id: int) -> MessageTemplate:
        async with self._session_factory() as session:
            template = await session.get(MessageTemplate, template_id)
            if template is None:
                raise NotFoundError("template not found")
            return template

    async def create_template(
        self,
        *,
        name: str,
        primary_text: str,
        reply_text: str,
    ) -> MessageTemplate:
        try:
            validate_spintax(primary_text)
            validate_spintax(reply_text)
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        async with self._session_factory() as session:
            template = MessageTemplate(
                name=name,
                primary_text=primary_text,
                reply_text=reply_text,
            )
            session.add(template)
            await self._flush_or_conflict(session, "template name already exists")
            add_audit_event(
                session,
                event_type="template.created",
                entity_type="message_template",
                entity_id=template.id,
                details={"name": name},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(template)
            return template

    async def toggle_template(self, template_id: int) -> MessageTemplate:
        async with self._session_factory() as session:
            template = await session.get(MessageTemplate, template_id)
            if template is None:
                raise NotFoundError("template not found")
            template.enabled = not template.enabled
            add_audit_event(
                session,
                event_type="template.status_changed",
                entity_type="message_template",
                entity_id=template.id,
                details={"enabled": template.enabled},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(template)
            return template

    async def update_template_texts(
        self,
        template_id: int,
        *,
        primary_text: str,
        reply_text: str,
    ) -> MessageTemplate:
        try:
            validate_spintax(primary_text)
            validate_spintax(reply_text)
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        async with self._session_factory() as session:
            template = await session.get(MessageTemplate, template_id)
            if template is None:
                raise NotFoundError("template not found")
            template.primary_text = primary_text
            template.reply_text = reply_text
            add_audit_event(
                session,
                event_type="template.texts_updated",
                entity_type="message_template",
                entity_id=template.id,
                details={"name": template.name},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(template)
            return template

    async def delete_template(self, template_id: int) -> None:
        async with self._session_factory() as session:
            template = await session.get(MessageTemplate, template_id)
            if template is None:
                raise NotFoundError("template not found")
            add_audit_event(
                session,
                event_type="template.deleted",
                entity_type="message_template",
                entity_id=template.id,
                details={"name": template.name},
                actor="control-bot",
            )
            await session.delete(template)
            await session.commit()

    async def create_campaign(
        self,
        *,
        name: str,
        mode: CampaignMode = CampaignMode.DIALOGUE,
        primary_account_id: int,
        reply_account_id: int | None = None,
        target_id: int | None,
        target_list_id: int | None,
        primary_text: str,
        primary_html: str | None = None,
        primary_media_path: str | None = None,
        primary_media_kind: str | None = None,
        primary_media_name: str | None = None,
        primary_media_size: int | None = None,
        reply_text: str = "",
        reply_delay_seconds: int = 0,
        spread_seconds: int = 0,
        repeat_count: int = 1,
        repeat_interval_seconds: int = 0,
        smart_timer_mode: bool = False,
        smart_duration_seconds: int = 0,
        smart_no_timer_interval_seconds: int = 900,
        auto_sender_failover: bool = False,
        sender_mode: BroadcastSenderMode = BroadcastSenderMode.DISTRIBUTED,
        activity_aware: bool = False,
        min_messages_between_posts: int = 0,
        activity_check_interval_seconds: int = 300,
        visibility_max_wait_seconds: int = 3600,
        sender_account_ids: list[int] | None = None,
        dialogue_steps: list[dict[str, object]] | None = None,
    ) -> Campaign:
        normalized_dialogue_steps = None
        if mode == CampaignMode.DIALOGUE and dialogue_steps:
            try:
                normalized_dialogue_steps = validate_dialogue_steps(dialogue_steps)
            except ValueError as exc:
                raise ConflictError(str(exc)) from exc
            primary_account_id = normalized_dialogue_steps[0].account_id
            reply_account_id = normalized_dialogue_steps[1].account_id
            primary_text = normalized_dialogue_steps[0].text
            reply_text = normalized_dialogue_steps[1].text
            reply_delay_seconds = normalized_dialogue_steps[1].delay_seconds
        else:
            try:
                validate_telegram_text(
                    primary_text,
                    primary_html,
                    has_media=primary_media_path is not None,
                    allow_empty=primary_media_path is not None,
                )
                validate_spintax(reply_text)
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
        sender_ids = list(dict.fromkeys([primary_account_id, *(sender_account_ids or [])]))
        if mode == CampaignMode.DIALOGUE:
            sender_mode = BroadcastSenderMode.DISTRIBUTED
            if primary_html is not None or primary_media_path is not None:
                raise ConflictError("форматирование и медиа доступны только для рассылок")
            if sender_account_ids:
                raise ConflictError("sender pool is only supported for broadcast campaigns")
            if reply_account_id is None:
                raise ConflictError("для диалога нужен аккаунт ответа")
            if primary_account_id == reply_account_id:
                raise ConflictError("primary and reply accounts must be different")
            reply_is_captionless_media = bool(
                normalized_dialogue_steps
                and normalized_dialogue_steps[1].media_path is not None
            )
            if not reply_text.strip() and not reply_is_captionless_media:
                raise ConflictError("для диалога нужен текст ответа")
            if spread_seconds != 0:
                raise ConflictError("spread window is only supported for broadcast campaigns")
            if repeat_count != 1 or repeat_interval_seconds != 0 or auto_sender_failover:
                raise ConflictError("диалог использует один проход или умный режим")
            if smart_timer_mode:
                self._validate_broadcast_schedule(
                    1,
                    0,
                    smart_timer_mode=True,
                    spread_seconds=0,
                    smart_duration_seconds=smart_duration_seconds,
                    smart_no_timer_interval_seconds=smart_no_timer_interval_seconds,
                )
                if activity_aware:
                    if not 1 <= min_messages_between_posts <= 100_000:
                        raise ConflictError("глубина диалога должна быть от 1 до 100000 сообщений")
                    if not 30 <= activity_check_interval_seconds <= 3600:
                        raise ConflictError("проверка активности должна быть от 30 до 3600 секунд")
                    if not 60 <= visibility_max_wait_seconds <= 7 * 24 * 3600:
                        raise ConflictError(
                            "максимальное ожидание должно быть от 1 минуты до 7 суток"
                        )
            elif smart_duration_seconds != 0 or activity_aware:
                raise ConflictError("адаптивная частота требует умный режим диалога")
        elif mode == CampaignMode.BROADCAST:
            if sender_mode == BroadcastSenderMode.INDEPENDENT:
                # A failed lane must not jump into another account's lane and
                # accidentally duplicate its posts.
                auto_sender_failover = False
            if dialogue_steps:
                raise ConflictError("цепочку сообщений можно использовать только в диалоге")
            if reply_account_id is not None:
                raise ConflictError("broadcast campaign must not have a reply account")
            if reply_text.strip():
                raise ConflictError("broadcast campaign must not contain reply text")
            if reply_delay_seconds != 0:
                raise ConflictError("задержку ответа можно использовать только в диалоге")
            if primary_media_path is not None:
                if primary_media_kind not in {"photo", "video", "document", "voice", "video_note"}:
                    raise ConflictError("неизвестный тип медиафайла")
                if len(primary_text) > 1024:
                    raise ConflictError("подпись к медиа должна быть не длиннее 1024 символов")
            elif primary_media_kind is not None:
                raise ConflictError("тип медиа указан без файла")
            self._validate_broadcast_schedule(
                repeat_count,
                repeat_interval_seconds,
                smart_timer_mode=smart_timer_mode,
                spread_seconds=spread_seconds,
                smart_duration_seconds=smart_duration_seconds,
                smart_no_timer_interval_seconds=smart_no_timer_interval_seconds,
            )
            if activity_aware:
                if not smart_timer_mode:
                    raise ConflictError("режим по активности требует умную рассылку")
                if not 1 <= min_messages_between_posts <= 100_000:
                    raise ConflictError("глубина поста должна быть от 1 до 100000 сообщений")
                if not 30 <= activity_check_interval_seconds <= 3600:
                    raise ConflictError("проверка активности должна быть от 30 до 3600 секунд")
                if not 60 <= visibility_max_wait_seconds <= 7 * 24 * 3600:
                    raise ConflictError("максимальное ожидание должно быть от 1 минуты до 7 суток")
        primary_is_captionless_dialogue_media = bool(
            normalized_dialogue_steps
            and normalized_dialogue_steps[0].media_path is not None
        )
        if not primary_text.strip() and not (
            primary_media_path is not None
        ) and not primary_is_captionless_dialogue_media:
            raise ConflictError("primary message must not be empty")
        if (target_id is None) == (target_list_id is None):
            raise ConflictError("choose exactly one target or target list")
        async with self._session_factory() as session:
            campaign = Campaign(
                name=name,
                mode=mode,
                primary_account_id=primary_account_id,
                reply_account_id=reply_account_id,
                target_id=target_id,
                target_list_id=target_list_id,
                primary_text=primary_text,
                primary_html=primary_html,
                primary_media_path=primary_media_path,
                primary_media_kind=primary_media_kind,
                primary_media_name=primary_media_name,
                primary_media_size=primary_media_size,
                reply_text=reply_text,
                reply_delay_seconds=reply_delay_seconds,
                spread_seconds=spread_seconds,
                repeat_count=repeat_count,
                repeat_interval_seconds=repeat_interval_seconds,
                smart_timer_mode=smart_timer_mode,
                smart_duration_seconds=smart_duration_seconds,
                smart_no_timer_interval_seconds=smart_no_timer_interval_seconds,
                auto_sender_failover=auto_sender_failover,
                sender_mode=sender_mode,
                activity_aware=activity_aware,
                min_messages_between_posts=min_messages_between_posts,
                activity_check_interval_seconds=activity_check_interval_seconds,
                visibility_max_wait_seconds=visibility_max_wait_seconds,
                dialogue_steps_json=(
                    serialize_dialogue_steps(normalized_dialogue_steps)
                    if normalized_dialogue_steps is not None
                    else None
                ),
            )
            session.add(campaign)
            await self._flush_or_conflict(session, "campaign name or relations are invalid")
            if mode == CampaignMode.BROADCAST:
                session.add_all(
                    CampaignAccount(campaign_id=campaign.id, account_id=account_id)
                    for account_id in sender_ids
                )
                await self._flush_or_conflict(session, "не удалось сохранить отправителей")
            add_audit_event(
                session,
                event_type="campaign.created",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "name": name,
                    "target_id": target_id,
                    "target_list_id": target_list_id,
                    "sender_account_ids": sender_ids if mode == CampaignMode.BROADCAST else [],
                    "repeat_count": repeat_count,
                    "repeat_interval_seconds": repeat_interval_seconds,
                    "smart_timer_mode": smart_timer_mode,
                    "smart_duration_seconds": smart_duration_seconds,
                    "smart_no_timer_interval_seconds": smart_no_timer_interval_seconds,
                    "auto_sender_failover": auto_sender_failover,
                    "sender_mode": sender_mode.value,
                    "activity_aware": activity_aware,
                    "min_messages_between_posts": min_messages_between_posts,
                    "formatted_text": primary_html is not None,
                    "media_kind": primary_media_kind,
                    "media_size": primary_media_size,
                    "dialogue_step_count": (
                        len(normalized_dialogue_steps)
                        if normalized_dialogue_steps is not None
                        else (2 if mode == CampaignMode.DIALOGUE else 0)
                    ),
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_schedule(
        self,
        campaign_id: int,
        *,
        repeat_count: int,
        repeat_interval_seconds: int,
    ) -> Campaign:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode not in {CampaignMode.BROADCAST, CampaignMode.DIALOGUE}:
                raise ConflictError("расписание недоступно для этого сценария")
            if campaign.mode == CampaignMode.DIALOGUE and (
                repeat_count != 1 or repeat_interval_seconds != 0
            ):
                raise ConflictError("для диалога доступны один проход или умный режим")
            self._validate_broadcast_schedule(repeat_count, repeat_interval_seconds)
            previous = {
                "repeat_count": campaign.repeat_count,
                "repeat_interval_seconds": campaign.repeat_interval_seconds,
                "smart_timer_mode": campaign.smart_timer_mode,
                "smart_duration_seconds": campaign.smart_duration_seconds,
            }
            campaign.repeat_count = repeat_count
            campaign.repeat_interval_seconds = repeat_interval_seconds
            campaign.smart_timer_mode = False
            campaign.smart_duration_seconds = 0
            campaign.auto_sender_failover = False
            campaign.activity_aware = False
            campaign.min_messages_between_posts = 0
            add_audit_event(
                session,
                event_type="campaign.schedule_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "previous": previous,
                    "repeat_count": repeat_count,
                    "repeat_interval_seconds": repeat_interval_seconds,
                    "smart_timer_mode": False,
                    "auto_sender_failover": False,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_smart_schedule(
        self,
        campaign_id: int,
        *,
        smart_duration_seconds: int,
        smart_no_timer_interval_seconds: int,
        auto_sender_failover: bool | None = None,
        activity_aware: bool | None = None,
        min_messages_between_posts: int | None = None,
        activity_check_interval_seconds: int | None = None,
        visibility_max_wait_seconds: int | None = None,
    ) -> Campaign:
        self._validate_broadcast_schedule(
            1,
            0,
            smart_timer_mode=True,
            spread_seconds=0,
            smart_duration_seconds=smart_duration_seconds,
            smart_no_timer_interval_seconds=smart_no_timer_interval_seconds,
        )
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode not in {CampaignMode.BROADCAST, CampaignMode.DIALOGUE}:
                raise ConflictError("умное расписание недоступно для этого сценария")
            effective_failover = (
                campaign.auto_sender_failover
                if auto_sender_failover is None
                else auto_sender_failover
            )
            if (
                campaign.mode == CampaignMode.DIALOGUE
                or campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
            ):
                effective_failover = False
            effective_activity = (
                campaign.activity_aware if activity_aware is None else activity_aware
            )
            effective_depth = (
                campaign.min_messages_between_posts
                if min_messages_between_posts is None
                else min_messages_between_posts
            )
            effective_check_interval = (
                campaign.activity_check_interval_seconds
                if activity_check_interval_seconds is None
                else activity_check_interval_seconds
            )
            effective_max_wait = (
                campaign.visibility_max_wait_seconds
                if visibility_max_wait_seconds is None
                else visibility_max_wait_seconds
            )
            if effective_activity and effective_depth < 1:
                raise ConflictError("для адаптивной видимости укажите глубину поста")
            if not 30 <= effective_check_interval <= 3600:
                raise ConflictError("проверка активности должна быть от 30 до 3600 секунд")
            if not 60 <= effective_max_wait <= 7 * 24 * 3600:
                raise ConflictError("максимальное ожидание должно быть от 1 минуты до 7 суток")
            previous = {
                "smart_timer_mode": campaign.smart_timer_mode,
                "smart_duration_seconds": campaign.smart_duration_seconds,
                "smart_no_timer_interval_seconds": (campaign.smart_no_timer_interval_seconds),
                "auto_sender_failover": campaign.auto_sender_failover,
                "activity_aware": campaign.activity_aware,
                "min_messages_between_posts": campaign.min_messages_between_posts,
            }
            campaign.spread_seconds = 0
            campaign.repeat_count = 1
            campaign.repeat_interval_seconds = 0
            campaign.smart_timer_mode = True
            campaign.smart_duration_seconds = smart_duration_seconds
            campaign.smart_no_timer_interval_seconds = smart_no_timer_interval_seconds
            campaign.auto_sender_failover = effective_failover
            campaign.activity_aware = effective_activity
            campaign.min_messages_between_posts = effective_depth
            campaign.activity_check_interval_seconds = effective_check_interval
            campaign.visibility_max_wait_seconds = effective_max_wait
            add_audit_event(
                session,
                event_type="campaign.smart_schedule_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "previous": previous,
                    "smart_duration_seconds": smart_duration_seconds,
                    "smart_no_timer_interval_seconds": smart_no_timer_interval_seconds,
                    "auto_sender_failover": effective_failover,
                    "activity_aware": effective_activity,
                    "min_messages_between_posts": effective_depth,
                    "activity_check_interval_seconds": effective_check_interval,
                    "visibility_max_wait_seconds": effective_max_wait,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_reactions(
        self,
        campaign_id: int,
        *,
        enabled: bool | None = None,
        count: int | None = None,
        emoji: str | None = None,
        dialogue_step: int | None = None,
    ) -> Campaign:
        allowed = {"👍", "🔥", "❤️", "⚡"}
        if count is not None and not 0 <= count <= 10:
            raise ConflictError("количество реакций должно быть от 0 до 10")
        if emoji is not None and emoji not in allowed:
            raise ConflictError("выберите реакцию из списка")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            if dialogue_step is not None:
                steps = parse_dialogue_steps(
                    campaign.dialogue_steps_json,
                    primary_account_id=campaign.primary_account_id,
                    primary_text=campaign.primary_text,
                    reply_account_id=campaign.reply_account_id,
                    reply_text=campaign.reply_text,
                    reply_delay_seconds=campaign.reply_delay_seconds,
                )
                if not 1 <= dialogue_step <= len(steps):
                    raise ConflictError("такого сообщения в цепочке нет")
                campaign.reaction_dialogue_step = dialogue_step
            if enabled is not None:
                campaign.reactions_enabled = enabled
                if enabled and count is None and campaign.reaction_count == 0:
                    campaign.reaction_count = 1
            if count is not None:
                campaign.reaction_count = count
            if emoji is not None:
                campaign.reaction_emoji = emoji
            if not campaign.reactions_enabled or campaign.reaction_count < 1:
                await session.execute(
                    update(ReactionTask)
                    .where(
                        ReactionTask.campaign_id == campaign.id,
                        ReactionTask.status == ReactionTaskStatus.QUEUED,
                    )
                    .values(
                        status=ReactionTaskStatus.CANCELLED,
                        finished_at=utc_now(),
                        error="reactions disabled by operator",
                        error_code="reactions_disabled",
                    )
                )
                await session.execute(
                    update(ReactionTask)
                    .where(
                        ReactionTask.campaign_id == campaign.id,
                        ReactionTask.status == ReactionTaskStatus.RUNNING,
                    )
                    .values(
                        cancel_requested=True,
                        error="reactions disabled by operator",
                        error_code="cancel_requested",
                    )
                )
            add_audit_event(
                session,
                event_type="campaign.reactions_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "enabled": campaign.reactions_enabled,
                    "count": campaign.reaction_count,
                    "emoji": campaign.reaction_emoji,
                    "dialogue_step": campaign.reaction_dialogue_step,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_hidden_mentions(
        self,
        campaign_id: int,
        *,
        enabled: bool,
    ) -> Campaign:
        """Toggle participant mentions for future broadcast runs only."""

        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("отметки доступны только для рассылки")
            campaign.hidden_mentions_enabled = bool(enabled)
            add_audit_event(
                session,
                event_type="campaign.hidden_mentions_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"enabled": campaign.hidden_mentions_enabled},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_mention_cooldown_days(
        self,
        campaign_id: int,
        days: int,
    ) -> Campaign:
        """Set the per-chat participant re-mention window for future runs."""

        normalized_days = int(days)
        if not 1 <= normalized_days <= 365:
            raise ConflictError("срок ротации отметок должен быть от 1 до 365 дней")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("ротация отметок доступна только для рассылки")
            previous_days = int(campaign.mention_cooldown_days)
            campaign.mention_cooldown_days = normalized_days
            add_audit_event(
                session,
                event_type="campaign.mention_cooldown_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "previous_days": previous_days,
                    "days": normalized_days,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_sender_mode(
        self,
        campaign_id: int,
        sender_mode: BroadcastSenderMode,
    ) -> Campaign:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("режим аккаунтов доступен только для рассылки")
            previous = campaign.sender_mode
            campaign.sender_mode = sender_mode
            if sender_mode == BroadcastSenderMode.INDEPENDENT:
                campaign.auto_sender_failover = False
            add_audit_event(
                session,
                event_type="campaign.sender_mode_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"previous": previous.value, "sender_mode": sender_mode.value},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_senders(
        self,
        campaign_id: int,
        sender_ids: list[int],
    ) -> Campaign:
        """Replace the sender pool used by future broadcast launches only."""

        normalized_ids = list(dict.fromkeys(int(account_id) for account_id in sender_ids))
        if not normalized_ids:
            raise ConflictError("выберите хотя бы одного отправителя")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("пул отправителей доступен только для рассылки")
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(normalized_ids)))
            )
            accounts_by_id = {account.id: account for account in accounts}
            unavailable = [
                account_id
                for account_id in normalized_ids
                if (
                    account_id not in accounts_by_id
                    or accounts_by_id[account_id].status != AccountStatus.ACTIVE
                    or not accounts_by_id[account_id].ownership_confirmed
                )
            ]
            if unavailable:
                raise ConflictError("в пул можно добавить только активные подтверждённые аккаунты")
            previous_sender_ids = list(
                await session.scalars(
                    select(CampaignAccount.account_id)
                    .where(CampaignAccount.campaign_id == campaign_id)
                    .order_by(CampaignAccount.id)
                )
            )
            await session.execute(
                delete(CampaignAccount).where(CampaignAccount.campaign_id == campaign_id)
            )
            session.add_all(
                CampaignAccount(campaign_id=campaign_id, account_id=account_id)
                for account_id in normalized_ids
            )
            campaign.primary_account_id = normalized_ids[0]
            add_audit_event(
                session,
                event_type="campaign.sender_pool_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "previous_sender_ids": previous_sender_ids,
                    "sender_ids": normalized_ids,
                    "applies_to": "future_launches_only",
                },
                actor="control-bot",
            )
            await self._flush_or_conflict(session, "не удалось сохранить отправителей")
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_dialogue_steps(
        self,
        campaign_id: int,
        steps: list[dict[str, object]],
    ) -> Campaign:
        try:
            normalized = validate_dialogue_steps(steps)
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("диалог не найден")
            if campaign.mode != CampaignMode.DIALOGUE:
                raise ConflictError("цепочка доступна только для диалога")
            campaign.primary_account_id = normalized[0].account_id
            campaign.reply_account_id = normalized[1].account_id
            campaign.primary_text = normalized[0].text
            campaign.reply_text = normalized[1].text
            campaign.reply_delay_seconds = normalized[1].delay_seconds
            campaign.dialogue_steps_json = serialize_dialogue_steps(normalized)
            add_audit_event(
                session,
                event_type="campaign.dialogue_chain_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"step_count": len(normalized)},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def update_campaign_primary_text(
        self,
        campaign_id: int,
        text: str,
        *,
        html_text: str | None = None,
        media_path: str | None = None,
        media_kind: str | None = None,
        media_name: str | None = None,
        media_size: int | None = None,
    ) -> Campaign:
        text = text.strip()
        allows_empty_text = media_path is not None
        if not 1 <= len(text) <= 4096 and not (allows_empty_text and not text):
            raise ConflictError("текст должен содержать от 1 до 4096 символов")
        if media_path is not None:
            if media_kind not in {"photo", "video", "document", "voice", "video_note"}:
                raise ConflictError("неизвестный тип медиафайла")
            if len(text) > 1024:
                raise ConflictError("подпись к медиа должна быть не длиннее 1024 символов")
        elif media_kind is not None:
            raise ConflictError("тип медиа указан без файла")
        if media_kind == "video_note" and (text or html_text):
            raise ConflictError("к видеокружку нельзя добавить подпись")
        try:
            validate_telegram_text(
                text,
                html_text,
                has_media=media_path is not None,
                allow_empty=allows_empty_text,
            )
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("для диалога измените цепочку сообщений")
            campaign.primary_text = text
            campaign.primary_html = html_text
            campaign.primary_media_path = media_path
            campaign.primary_media_kind = media_kind
            campaign.primary_media_name = media_name
            campaign.primary_media_size = media_size
            if campaign.content_variants_json:
                variants = self._content_variants(campaign)
                first = variants[0]
                variants[0] = BroadcastContentVariant(
                    variant_id=first.variant_id,
                    name=first.name,
                    text=text,
                    html_text=html_text,
                    media=(
                        (
                            StoredMedia(
                                path=media_path,
                                kind=str(media_kind),
                                name=str(media_name or "media"),
                                size=int(media_size or 0),
                            ),
                        )
                        if media_path is not None
                        else ()
                    ),
                    contact=None,
                    enabled=first.enabled,
                )
                try:
                    campaign.content_variants_json = serialize_content_variants(variants)
                except ContentError as exc:
                    raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.primary_text_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "text_length": len(text),
                    "formatted_text": html_text is not None,
                    "media_kind": media_kind,
                    "media_size": media_size,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def campaign_content_variants(
        self,
        campaign_id: int,
    ) -> list[BroadcastContentVariant]:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("варианты доступны только для рассылок")
            return self._content_variants(campaign)

    async def add_campaign_content_variant(
        self,
        campaign_id: int,
        *,
        name: str,
        text: str,
        html_text: str | None = None,
        media: StoredMedia | None = None,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("варианты доступны только для рассылок")
            variants = self._content_variants(campaign)
            if len(variants) >= MAX_CONTENT_VARIANTS:
                raise ConflictError(f"можно создать не больше {MAX_CONTENT_VARIANTS} вариантов")
            try:
                variant = new_content_variant(
                    name=name,
                    text=text,
                    html_text=html_text,
                    media=(media,) if media else (),
                )
                variants.append(variant)
                campaign.content_variants_json = serialize_content_variants(variants)
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.content_variant_added",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"variant_id": variant.variant_id, "name": variant.name},
                actor="control-bot",
            )
            await session.commit()
            return variant

    async def add_campaign_contact_variant(
        self,
        campaign_id: int,
        contact: StoredContact,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            if campaign.mode != CampaignMode.BROADCAST:
                raise ConflictError("контакт доступен только для рассылок")
            variants = self._content_variants(campaign)
            if len(variants) >= MAX_CONTENT_VARIANTS:
                raise ConflictError(f"можно создать не больше {MAX_CONTENT_VARIANTS} вариантов")
            try:
                variant = new_content_variant(
                    name=f"Контакт · {contact.display_name}"[:80],
                    text="",
                    contact=contact,
                )
                variants.append(variant)
                campaign.content_variants_json = serialize_content_variants(variants)
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.contact_variant_added",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "variant_id": variant.variant_id,
                    "contact_name": contact.display_name,
                    "phone_suffix": contact.phone_number[-4:],
                },
                actor="control-bot",
            )
            await session.commit()
            return variant

    async def update_campaign_variant_contact(
        self,
        campaign_id: int,
        variant_id: str,
        contact: StoredContact,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            if current.contact is None:
                raise ConflictError("этот вариант не является контактом")
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=current.name,
                text="",
                contact=contact,
                enabled=current.enabled,
            )
            try:
                variants[index] = updated
                campaign.content_variants_json = serialize_content_variants(variants)
                self._sync_primary_content(campaign, variants[0])
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.contact_variant_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "variant_id": variant_id,
                    "contact_name": contact.display_name,
                    "phone_suffix": contact.phone_number[-4:],
                },
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def add_campaign_variant_media(
        self,
        campaign_id: int,
        variant_id: str,
        media: StoredMedia,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            if current.contact is not None:
                raise ConflictError("к контакту нельзя добавить медиа")
            if len(current.media) >= MAX_ALBUM_ITEMS:
                raise ConflictError(f"в альбоме может быть не больше {MAX_ALBUM_ITEMS} файлов")
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=current.name,
                text=current.text,
                html_text=current.html_text,
                media=(*current.media, media),
                contact=None,
                enabled=current.enabled,
            )
            try:
                variants[index] = updated
                campaign.content_variants_json = serialize_content_variants(variants)
                self._sync_primary_content(campaign, variants[0])
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.variant_media_added",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "variant_id": variant_id,
                    "media_kind": media.kind,
                    "media_count": len(updated.media),
                },
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def rename_campaign_content_variant(
        self,
        campaign_id: int,
        variant_id: str,
        name: str,
    ) -> BroadcastContentVariant:
        normalized_name = name.strip()
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=normalized_name,
                text=current.text,
                html_text=current.html_text,
                media=current.media,
                contact=current.contact,
                enabled=current.enabled,
            )
            try:
                variants[index] = updated
                campaign.content_variants_json = serialize_content_variants(variants)
                self._sync_primary_content(campaign, variants[0])
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.content_variant_renamed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"variant_id": variant_id, "name": normalized_name},
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def update_campaign_variant_text(
        self,
        campaign_id: int,
        variant_id: str,
        *,
        text: str,
        html_text: str | None,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            if current.contact is not None:
                raise ConflictError("у контакта нет отдельного текста")
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=current.name,
                text=text.strip(),
                html_text=html_text,
                media=current.media,
                contact=None,
                enabled=current.enabled,
            )
            try:
                variants[index] = updated
                campaign.content_variants_json = serialize_content_variants(variants)
                self._sync_primary_content(campaign, variants[0])
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.content_variant_text_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "variant_id": variant_id,
                    "text_length": len(updated.text),
                    "formatted_text": html_text is not None,
                },
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def remove_campaign_variant_media(
        self,
        campaign_id: int,
        variant_id: str,
        media_index: int,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            if not 0 <= media_index < len(current.media):
                raise NotFoundError("медиафайл не найден")
            media_items = list(current.media)
            removed = media_items.pop(media_index)
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=current.name,
                text=current.text,
                html_text=current.html_text,
                media=tuple(media_items),
                contact=current.contact,
                enabled=current.enabled,
            )
            variants[index] = updated
            campaign.content_variants_json = serialize_content_variants(variants)
            self._sync_primary_content(campaign, variants[0])
            add_audit_event(
                session,
                event_type="campaign.variant_media_removed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"variant_id": variant_id, "media_name": removed.name},
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def toggle_campaign_content_variant(
        self,
        campaign_id: int,
        variant_id: str,
    ) -> BroadcastContentVariant:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            index = self._variant_index(variants, variant_id)
            current = variants[index]
            updated = BroadcastContentVariant(
                variant_id=current.variant_id,
                name=current.name,
                text=current.text,
                html_text=current.html_text,
                media=current.media,
                contact=current.contact,
                enabled=not current.enabled,
            )
            variants[index] = updated
            try:
                campaign.content_variants_json = serialize_content_variants(variants)
            except ContentError as exc:
                raise ConflictError(str(exc)) from exc
            add_audit_event(
                session,
                event_type="campaign.content_variant_status_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"variant_id": variant_id, "enabled": updated.enabled},
                actor="control-bot",
            )
            await session.commit()
            return updated

    async def delete_campaign_content_variant(
        self,
        campaign_id: int,
        variant_id: str,
    ) -> int:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка не найдена")
            variants = self._content_variants(campaign)
            if len(variants) <= 1:
                raise ConflictError("нельзя удалить единственный вариант")
            index = self._variant_index(variants, variant_id)
            removed = variants.pop(index)
            campaign.content_variants_json = serialize_content_variants(variants)
            self._sync_primary_content(campaign, variants[0])
            add_audit_event(
                session,
                event_type="campaign.content_variant_deleted",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"variant_id": removed.variant_id, "name": removed.name},
                actor="control-bot",
            )
            await session.commit()
            return len(variants)

    @staticmethod
    def _variant_index(
        variants: list[BroadcastContentVariant],
        variant_id: str,
    ) -> int:
        for index, variant in enumerate(variants):
            if variant.variant_id == variant_id:
                return index
        raise NotFoundError("вариант не найден")

    @staticmethod
    def _content_variants(campaign: Campaign) -> list[BroadcastContentVariant]:
        try:
            return parse_content_variants(
                campaign.content_variants_json,
                fallback_text=campaign.primary_text,
                fallback_html=campaign.primary_html,
                fallback_media_path=campaign.primary_media_path,
                fallback_media_kind=campaign.primary_media_kind,
                fallback_media_name=campaign.primary_media_name,
                fallback_media_size=campaign.primary_media_size,
            )
        except ContentError as exc:
            raise ConflictError(str(exc)) from exc

    @staticmethod
    def _sync_primary_content(
        campaign: Campaign,
        variant: BroadcastContentVariant,
    ) -> None:
        media = variant.media[0] if variant.media else None
        campaign.primary_text = variant.text
        campaign.primary_html = variant.html_text
        campaign.primary_media_path = media.path if media else None
        campaign.primary_media_kind = media.kind if media else None
        campaign.primary_media_name = media.name if media else None
        campaign.primary_media_size = media.size if media else None

    @staticmethod
    def _validate_broadcast_schedule(
        repeat_count: int,
        repeat_interval_seconds: int,
        *,
        smart_timer_mode: bool = False,
        spread_seconds: int = 0,
        smart_duration_seconds: int = 0,
        smart_no_timer_interval_seconds: int = 900,
    ) -> None:
        if not 1 <= repeat_count <= 100:
            raise ConflictError("число проходов должно быть от 1 до 100")
        if smart_timer_mode:
            if repeat_count != 1:
                raise ConflictError("для рассылки по таймерам число проходов должно быть равно 1")
            if repeat_interval_seconds != 0 or spread_seconds != 0:
                raise ConflictError("умная рассылка использует отдельные настройки времени")
            if not 60 <= smart_duration_seconds <= 7 * 24 * 3600:
                raise ConflictError("срок активности должен быть от 1 минуты до 7 суток")
            if not 60 <= smart_no_timer_interval_seconds <= 7 * 24 * 3600:
                raise ConflictError("интервал без таймера должен быть от 1 минуты до 7 суток")
            return
        if smart_duration_seconds != 0:
            raise ConflictError("срок умной рассылки требует режим по таймерам")
        if repeat_count == 1:
            if repeat_interval_seconds != 0:
                raise ConflictError("для одного прохода интервал должен быть равен нулю")
            return
        if not 60 <= repeat_interval_seconds <= 7 * 24 * 3600:
            raise ConflictError("интервал повторов должен быть от 1 минуты до 7 суток")

    async def toggle_campaign(self, campaign_id: int) -> Campaign:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            campaign.enabled = not campaign.enabled
            add_audit_event(
                session,
                event_type="campaign.status_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"enabled": campaign.enabled},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def get_campaign(self, campaign_id: int) -> Campaign:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            return campaign

    async def update_campaign_target(
        self,
        campaign_id: int,
        *,
        target_id: int | None,
        target_list_id: int | None,
    ) -> Campaign:
        if (target_id is None) == (target_list_id is None):
            raise ConflictError("выберите один чат или один список чатов")
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            if target_id is not None:
                target = await session.get(Target, target_id)
                if target is None:
                    raise NotFoundError("чат не найден")
                if not target.approved_by_operator:
                    raise ConflictError("сначала включите выбранный чат")
            else:
                target_list = await session.get(TargetList, target_list_id)
                if target_list is None:
                    raise NotFoundError("список чатов не найден")
                approved = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(TargetListItem)
                        .join(Target, Target.id == TargetListItem.target_id)
                        .where(
                            TargetListItem.target_list_id == target_list_id,
                            TargetListItem.enabled.is_(True),
                        )
                    )
                    or 0
                )
                if approved == 0:
                    raise ConflictError("в выбранной базе нет включённых чатов")
            previous = {
                "target_id": campaign.target_id,
                "target_list_id": campaign.target_list_id,
            }
            campaign.target_id = target_id
            campaign.target_list_id = target_list_id
            add_audit_event(
                session,
                event_type="campaign.target_changed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "previous": previous,
                    "target_id": target_id,
                    "target_list_id": target_list_id,
                },
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def campaign_sender_ids(self, campaign_id: int) -> list[int]:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            if campaign.mode != CampaignMode.BROADCAST:
                return [campaign.primary_account_id]
            sender_ids = list(
                await session.scalars(
                    select(CampaignAccount.account_id)
                    .where(CampaignAccount.campaign_id == campaign_id)
                    .order_by(CampaignAccount.id)
                )
            )
            return sender_ids or [campaign.primary_account_id]

    async def delete_campaign(self, campaign_id: int) -> None:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("рассылка/диалог не найдены")
            run_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(CampaignRun)
                    .where(CampaignRun.campaign_id == campaign_id)
                )
                or 0
            )
            if run_count:
                raise ConflictError(
                    "у этой рассылки или диалога уже есть история; "
                    "поставьте её на паузу, чтобы сохранить журнал"
                )
            add_audit_event(
                session,
                event_type="campaign.deleted",
                entity_type="campaign",
                entity_id=campaign.id,
                details={"name": campaign.name},
                actor="control-bot",
            )
            await session.delete(campaign)
            await session.commit()

    async def list_launches(
        self,
        page: int,
        page_size: int,
        *,
        archived: bool = False,
    ) -> Page[LaunchView]:
        normalized_page = max(page, 0)
        launch_key = func.coalesce(CampaignRun.launch_key, CampaignRun.request_key)
        legacy_namespace = case(
            (CampaignRun.launch_key.is_(None), 1),
            else_=0,
        )
        configured_account_count = (
            select(func.count(CampaignAccount.id))
            .where(CampaignAccount.campaign_id == Campaign.id)
            .correlate(Campaign)
            .scalar_subquery()
        )
        failover_counts = (
            select(
                AuditEvent.entity_id.label("run_id"),
                func.count(AuditEvent.id).label("failover_count"),
            )
            .where(
                AuditEvent.entity_type == "campaign_run",
                AuditEvent.event_type == "run.sender_failed_over",
            )
            .group_by(AuditEvent.entity_id)
            .subquery()
        )
        publication_counts = (
            select(
                ActionLog.run_id.label("run_id"),
                func.count(ActionLog.id).label("publication_count"),
            )
            .where(
                ActionLog.status == ActionStatus.SUCCEEDED,
                ActionLog.external_message_id.is_not(None),
            )
            .group_by(ActionLog.run_id)
            .subquery()
        )
        grouped = (
            select(
                func.max(CampaignRun.id).label("anchor_run_id"),
                Campaign.id.label("campaign_id"),
                Campaign.name.label("campaign_name"),
                Campaign.mode.label("mode"),
                Campaign.sender_mode.label("sender_mode"),
                func.max(case((CampaignRun.repeat_until.is_not(None), 1), else_=0)).label(
                    "smart_timer_mode"
                ),
                Campaign.smart_duration_seconds.label("smart_duration_seconds"),
                func.coalesce(
                    func.max(CampaignRun.no_timer_interval_seconds_snapshot),
                    0,
                ).label("no_timer_interval_seconds"),
                func.coalesce(
                    func.max(CampaignRun.auto_sender_failover_snapshot),
                    False,
                ).label("auto_sender_failover"),
                func.count(CampaignRun.id).label("total"),
                func.sum(case((CampaignRun.status == RunStatus.QUEUED, 1), else_=0)).label(
                    "queued"
                ),
                func.sum(case((CampaignRun.status == RunStatus.RUNNING, 1), else_=0)).label(
                    "running"
                ),
                func.sum(case((CampaignRun.status == RunStatus.SUCCEEDED, 1), else_=0)).label(
                    "succeeded"
                ),
                func.sum(case((CampaignRun.status == RunStatus.FAILED, 1), else_=0)).label(
                    "failed"
                ),
                func.sum(case((CampaignRun.status == RunStatus.CANCELLED, 1), else_=0)).label(
                    "cancelled"
                ),
                func.min(CampaignRun.created_at).label("created_at"),
                func.min(CampaignRun.scheduled_for).label("first_scheduled_for"),
                func.max(CampaignRun.scheduled_for).label("last_scheduled_for"),
                func.max(CampaignRun.repeat_until).label("active_until"),
                func.max(CampaignRun.archived_at).label("archived_at"),
                func.coalesce(
                    func.sum(publication_counts.c.publication_count),
                    0,
                ).label("sent_publications"),
                func.min(
                    case(
                        (
                            CampaignRun.status.in_((RunStatus.QUEUED, RunStatus.RUNNING)),
                            CampaignRun.scheduled_for,
                        ),
                        else_=None,
                    )
                ).label("next_scheduled_for"),
                func.count(func.distinct(CampaignRun.target_id)).label("target_count"),
                func.count(func.distinct(CampaignRun.sender_account_id)).label("run_account_count"),
                configured_account_count.label("configured_account_count"),
                func.max(CampaignRun.sender_pool_snapshot_json).label("sender_pool_snapshot_json"),
                func.coalesce(func.sum(failover_counts.c.failover_count), 0).label(
                    "failover_count"
                ),
            )
            .join(Campaign, Campaign.id == CampaignRun.campaign_id)
            .outerjoin(
                failover_counts,
                failover_counts.c.run_id == cast(CampaignRun.id, String),
            )
            .outerjoin(
                publication_counts,
                publication_counts.c.run_id == CampaignRun.id,
            )
            .where(
                CampaignRun.archived_at.is_not(None)
                if archived
                else CampaignRun.archived_at.is_(None)
            )
            .group_by(
                legacy_namespace,
                launch_key,
                Campaign.id,
                Campaign.name,
                Campaign.mode,
                Campaign.sender_mode,
                Campaign.smart_duration_seconds,
                Campaign.smart_no_timer_interval_seconds,
                Campaign.auto_sender_failover,
            )
        )
        async with self._session_factory() as session:
            total = int(
                await session.scalar(select(func.count()).select_from(grouped.subquery())) or 0
            )
            rows = (
                await session.execute(
                    grouped.order_by(
                        func.min(CampaignRun.created_at).desc(),
                        func.min(CampaignRun.id).desc(),
                    )
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            ).all()
            return Page(
                items=[self._launch_view_from_row(row) for row in rows],
                page=normalized_page,
                page_size=page_size,
                total=total,
            )

    async def search_launches(
        self,
        query: str,
        limit: int = 20,
        *,
        archived: bool = False,
    ) -> list[LaunchView]:
        normalized = query.strip()
        if not normalized:
            return []
        launch_key = func.coalesce(CampaignRun.launch_key, CampaignRun.request_key)
        legacy_namespace = case(
            (CampaignRun.launch_key.is_(None), 1),
            else_=0,
        )
        conditions = [Campaign.name.ilike(f"%{normalized}%")]
        if normalized.isdecimal():
            identifier = int(normalized)
            conditions.extend(
                [
                    Campaign.id == identifier,
                    CampaignRun.id == identifier,
                ]
            )
        anchor = func.max(CampaignRun.id)
        async with self._session_factory() as session:
            anchor_ids = list(
                await session.scalars(
                    select(anchor.label("anchor_run_id"))
                    .join(Campaign, Campaign.id == CampaignRun.campaign_id)
                    .where(
                        CampaignRun.archived_at.is_not(None)
                        if archived
                        else CampaignRun.archived_at.is_(None),
                        or_(*conditions),
                    )
                    .group_by(legacy_namespace, launch_key)
                    .order_by(
                        func.min(CampaignRun.created_at).desc(),
                        func.min(CampaignRun.id).desc(),
                    )
                    .limit(max(1, min(limit, 50)))
                )
            )
            if not anchor_ids:
                return []
            anchor_rows = list(
                await session.scalars(select(CampaignRun).where(CampaignRun.id.in_(anchor_ids)))
            )
            anchors_by_id = {int(item.id): item for item in anchor_rows}
            anchors = [
                anchors_by_id[int(item)] for item in anchor_ids if int(item) in anchors_by_id
            ]
            memberships = [_launch_membership(item) for item in anchors]
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(or_(*memberships)).order_by(CampaignRun.id)
                )
            )
            campaign_ids = {int(run.campaign_id) for run in runs}
            campaigns = {
                int(campaign.id): campaign
                for campaign in await session.scalars(
                    select(Campaign).where(Campaign.id.in_(campaign_ids))
                )
            }
            run_ids = [int(run.id) for run in runs]
            reaction_rows = await session.execute(
                select(
                    ReactionTask.run_id,
                    ReactionTask.status,
                    func.count(ReactionTask.id),
                )
                .where(ReactionTask.run_id.in_(run_ids))
                .group_by(ReactionTask.run_id, ReactionTask.status)
            )
            reactions_by_run: dict[int, dict[ReactionTaskStatus, int]] = {}
            for run_id, status_value, count in reaction_rows:
                reactions_by_run.setdefault(int(run_id), {})[status_value] = int(count)
            publication_rows = await session.execute(
                select(ActionLog.run_id, func.count(ActionLog.id))
                .where(
                    ActionLog.run_id.in_(run_ids),
                    ActionLog.status == ActionStatus.SUCCEEDED,
                    ActionLog.external_message_id.is_not(None),
                )
                .group_by(ActionLog.run_id)
            )
            publications_by_run = {int(run_id): int(count) for run_id, count in publication_rows}
            failover_rows = await session.execute(
                select(AuditEvent.entity_id, func.count(AuditEvent.id))
                .where(
                    AuditEvent.entity_type == "campaign_run",
                    AuditEvent.event_type == "run.sender_failed_over",
                    AuditEvent.entity_id.in_([str(run_id) for run_id in run_ids]),
                )
                .group_by(AuditEvent.entity_id)
            )
            failovers_by_run = {
                int(entity_id): int(count)
                for entity_id, count in failover_rows
                if entity_id is not None and str(entity_id).isdigit()
            }
        runs_by_key: dict[tuple[str, str], list[CampaignRun]] = {}
        for run in runs:
            runs_by_key.setdefault(self._launch_group_key(run), []).append(run)
        launches: list[LaunchView] = []
        for anchor_run in anchors:
            grouped_runs = runs_by_key.get(self._launch_group_key(anchor_run), [])
            campaign = campaigns.get(int(anchor_run.campaign_id))
            if campaign is None or not grouped_runs:
                continue
            reaction_counts: dict[ReactionTaskStatus, int] = {}
            for run in grouped_runs:
                for status_value, count in reactions_by_run.get(int(run.id), {}).items():
                    reaction_counts[status_value] = reaction_counts.get(status_value, 0) + count
            view = self._launch_view_from_runs(
                campaign,
                grouped_runs,
                reaction_counts=reaction_counts,
            )
            launches.append(
                replace(
                    view,
                    sent_publications=sum(
                        publications_by_run.get(int(run.id), 0) for run in grouped_runs
                    ),
                    failover_count=sum(
                        failovers_by_run.get(int(run.id), 0) for run in grouped_runs
                    ),
                )
            )
        return launches

    @staticmethod
    def _launch_group_key(run: CampaignRun) -> tuple[str, str]:
        if run.launch_key is not None:
            return "launch", run.launch_key
        return "legacy", run.request_key

    async def get_launch(self, anchor_run_id: int) -> tuple[LaunchView, list[CampaignRun]]:
        async with self._session_factory() as session:
            anchor = await session.get(CampaignRun, anchor_run_id)
            if anchor is None:
                raise NotFoundError("launch not found")
            runs = list(
                await session.scalars(
                    select(CampaignRun).where(_launch_membership(anchor)).order_by(CampaignRun.id)
                )
            )
            campaign = await session.get(Campaign, anchor.campaign_id)
            if campaign is None or not runs:
                raise NotFoundError("launch not found")
            run_ids = [int(run.id) for run in runs]
            reaction_result = await session.execute(
                select(ReactionTask.status, func.count(ReactionTask.id))
                .where(ReactionTask.run_id.in_(run_ids))
                .group_by(ReactionTask.status)
            )
            reaction_counts = {status: int(count) for status, count in reaction_result}
            reaction_last_error = await session.scalar(
                select(ReactionTask.error)
                .where(
                    ReactionTask.run_id.in_(run_ids),
                    ReactionTask.status == ReactionTaskStatus.FAILED,
                    ReactionTask.error.is_not(None),
                )
                .order_by(ReactionTask.finished_at.desc(), ReactionTask.id.desc())
                .limit(1)
            )
            audit_events = list(
                await session.scalars(
                    select(AuditEvent)
                    .where(
                        AuditEvent.entity_type == "campaign_run",
                        AuditEvent.event_type == "run.sender_failed_over",
                        AuditEvent.entity_id.in_([str(run.id) for run in runs]),
                    )
                    .order_by(AuditEvent.created_at, AuditEvent.id)
                )
            )
            failovers = self._sender_failovers_from_audit(audit_events, runs)
            action_logs = tuple(
                await session.scalars(
                    select(ActionLog)
                    .where(ActionLog.run_id.in_([run.id for run in runs]))
                    .order_by(ActionLog.created_at, ActionLog.id)
                )
            )
            publications = tuple(
                action
                for action in action_logs
                if action.status == ActionStatus.SUCCEEDED
                and action.external_message_id is not None
            )
            attempt_failures = tuple(
                action for action in action_logs if action.status == ActionStatus.FAILED
            )
            account_ids = {
                run.sender_account_id for run in runs if run.sender_account_id is not None
            }
            for run in runs:
                if not run.sender_pool_snapshot_json:
                    continue
                try:
                    account_ids.update(
                        int(item) for item in json.loads(run.sender_pool_snapshot_json)
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
            for failover in failovers:
                account_ids.add(failover.failed_account_id)
                account_ids.add(failover.replacement_account_id)
            account_ids.update(action.account_id for action in publications)
            account_ids.update(action.account_id for action in attempt_failures)
            accounts = (
                list(
                    await session.scalars(
                        select(Account)
                        .where(Account.id.in_(sorted(account_ids)))
                        .order_by(Account.id)
                    )
                )
                if account_ids
                else []
            )
            account_labels = tuple((account.id, account.label) for account in accounts)
            labels_by_id = dict(account_labels)
            timing_states = (
                list(
                    await session.scalars(
                        select(TargetAccountState).where(
                            TargetAccountState.target_id.in_(
                                sorted({run.target_id for run in runs})
                            ),
                            TargetAccountState.account_id.in_(sorted(account_ids)),
                        )
                    )
                )
                if account_ids
                else []
            )
            timings = tuple(
                TargetTimingView(
                    target_id=state.target_id,
                    account_id=state.account_id,
                    account_label=labels_by_id.get(
                        state.account_id,
                        f"аккаунт #{state.account_id}",
                    ),
                    slowmode_seconds=state.slowmode_seconds,
                    next_send_at=state.next_send_at,
                    last_checked_at=state.last_checked_at,
                    last_success_at=state.last_success_at,
                    last_message_id=state.last_message_id,
                    adaptive_interval_seconds=state.adaptive_interval_seconds,
                    activity_messages_per_minute=state.activity_messages_per_minute,
                    adaptive_tier=state.adaptive_tier,
                    activity_sampled_at=state.activity_sampled_at,
                    send_blocked=state.send_blocked,
                    blocked_reason=state.blocked_reason,
                    blocked_at=state.blocked_at,
                    last_error=state.last_error,
                )
                for state in timing_states
            )
            return (
                self._launch_view_from_runs(
                    campaign,
                    runs,
                    account_labels=account_labels,
                    failovers=failovers,
                    publications=publications,
                    attempt_failures=attempt_failures,
                    timings=timings,
                    reaction_counts=reaction_counts,
                    reaction_last_error=reaction_last_error,
                ),
                runs,
            )

    async def set_launch_archived(
        self,
        anchor_run_id: int,
        archived: bool,
    ) -> int:
        async with self._session_factory() as session:
            anchor = await session.get(CampaignRun, anchor_run_id)
            if anchor is None:
                raise NotFoundError("запуск не найден")
            launch_key = anchor.launch_key or anchor.request_key
            runs = list(
                await session.scalars(select(CampaignRun).where(_launch_membership(anchor)))
            )
            if archived and any(
                run.status in {RunStatus.QUEUED, RunStatus.RUNNING} for run in runs
            ):
                raise ConflictError("активный запуск сначала нужно остановить")
            if archived:
                pending_reactions = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(ReactionTask)
                        .where(
                            ReactionTask.run_id.in_([int(run.id) for run in runs]),
                            ReactionTask.status.in_(
                                [ReactionTaskStatus.QUEUED, ReactionTaskStatus.RUNNING]
                            ),
                        )
                    )
                    or 0
                )
                if pending_reactions:
                    raise ConflictError("ожидающие реакции запуска сначала нужно остановить")
            archived_at = utc_now() if archived else None
            for run in runs:
                run.archived_at = archived_at
            add_audit_event(
                session,
                event_type=("launch.archived" if archived else "launch.restored"),
                entity_type="campaign_run",
                entity_id=anchor_run_id,
                details={"launch_key": launch_key, "run_count": len(runs)},
                actor="control-bot",
            )
            await session.commit()
            return len(runs)

    async def list_runs(self, page: int, page_size: int) -> Page[CampaignRun]:
        return await self._page(CampaignRun, page, page_size, CampaignRun.id, descending=True)

    async def get_run(self, run_id: int) -> tuple[CampaignRun, list[ActionLog]]:
        async with self._session_factory() as session:
            run = await session.get(CampaignRun, run_id)
            if run is None:
                raise NotFoundError("run not found")
            actions = list(
                await session.scalars(
                    select(ActionLog).where(ActionLog.run_id == run_id).order_by(ActionLog.id)
                )
            )
            return run, actions

    async def targets_by_ids(self, target_ids: set[int]) -> dict[int, Target]:
        if not target_ids:
            return {}
        async with self._session_factory() as session:
            targets = list(
                await session.scalars(select(Target).where(Target.id.in_(sorted(target_ids))))
            )
            return {target.id: target for target in targets}

    async def account_labels_by_ids(self, account_ids: set[int]) -> dict[int, str]:
        if not account_ids:
            return {}
        async with self._session_factory() as session:
            rows = await session.execute(
                select(Account.id, Account.label).where(Account.id.in_(sorted(account_ids)))
            )
            return {int(account_id): str(label) for account_id, label in rows}

    async def list_audit(self, page: int, page_size: int) -> Page[AuditEvent]:
        return await self._page(AuditEvent, page, page_size, AuditEvent.id, descending=True)

    async def _page[T](
        self,
        model: type[T],
        page: int,
        page_size: int,
        order_column: object,
        *,
        descending: bool = False,
    ) -> Page[T]:
        normalized_page = max(page, 0)
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count()).select_from(model)) or 0)
            order = order_column.desc() if descending else order_column.asc()  # type: ignore[attr-defined]
            items = list(
                await session.scalars(
                    select(model)
                    .order_by(order)
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            )
            return Page(items=items, page=normalized_page, page_size=page_size, total=total)

    @staticmethod
    async def _count(
        session: AsyncSession,
        model: type[object],
        condition: object | None = None,
    ) -> int:
        statement = select(func.count()).select_from(model)
        if condition is not None:
            statement = statement.where(condition)
        return int(await session.scalar(statement) or 0)

    @staticmethod
    def _launch_view_from_row(row: object) -> LaunchView:
        active_until = row.active_until  # type: ignore[attr-defined]
        if (
            active_until is None
            and bool(row.smart_timer_mode)  # type: ignore[attr-defined]
            and int(row.smart_duration_seconds or 0) > 0  # type: ignore[attr-defined]
        ):
            active_until = row.created_at + timedelta(  # type: ignore[attr-defined]
                seconds=int(row.smart_duration_seconds)  # type: ignore[attr-defined]
            )
        account_count = int(row.run_account_count or 0)  # type: ignore[attr-defined]
        if account_count == 0:
            account_count = int(row.configured_account_count or 0)  # type: ignore[attr-defined]
        raw_sender_pool = row.sender_pool_snapshot_json  # type: ignore[attr-defined]
        if (
            isinstance(raw_sender_pool, str) and row.sender_mode != BroadcastSenderMode.INDEPENDENT  # type: ignore[attr-defined]
        ):
            try:
                account_count = len({int(item) for item in json.loads(raw_sender_pool)})
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        return LaunchView(
            anchor_run_id=int(row.anchor_run_id),  # type: ignore[attr-defined]
            campaign_id=int(row.campaign_id),  # type: ignore[attr-defined]
            campaign_name=str(row.campaign_name),  # type: ignore[attr-defined]
            mode=row.mode,  # type: ignore[attr-defined]
            total=int(row.total),  # type: ignore[attr-defined]
            queued=int(row.queued or 0),  # type: ignore[attr-defined]
            running=int(row.running or 0),  # type: ignore[attr-defined]
            succeeded=int(row.succeeded or 0),  # type: ignore[attr-defined]
            failed=int(row.failed or 0),  # type: ignore[attr-defined]
            cancelled=int(row.cancelled or 0),  # type: ignore[attr-defined]
            created_at=row.created_at,  # type: ignore[attr-defined]
            first_scheduled_for=row.first_scheduled_for,  # type: ignore[attr-defined]
            last_scheduled_for=row.last_scheduled_for,  # type: ignore[attr-defined]
            smart_timer_mode=bool(row.smart_timer_mode),  # type: ignore[attr-defined]
            active_until=active_until,
            no_timer_interval_seconds=int(  # type: ignore[attr-defined]
                row.no_timer_interval_seconds or 0
            ),
            auto_sender_failover=bool(row.auto_sender_failover),  # type: ignore[attr-defined]
            sender_mode=row.sender_mode,  # type: ignore[attr-defined]
            sent_publications=int(row.sent_publications or 0),  # type: ignore[attr-defined]
            next_scheduled_for=row.next_scheduled_for,  # type: ignore[attr-defined]
            target_count=int(row.target_count or 0),  # type: ignore[attr-defined]
            account_count=account_count,
            failover_count=int(row.failover_count or 0),  # type: ignore[attr-defined]
            archived=row.archived_at is not None,  # type: ignore[attr-defined]
        )

    @staticmethod
    def _launch_view_from_runs(
        campaign: Campaign,
        runs: list[CampaignRun],
        *,
        account_labels: tuple[tuple[int, str], ...] = (),
        failovers: tuple[SenderFailoverView, ...] = (),
        publications: tuple[ActionLog, ...] = (),
        attempt_failures: tuple[ActionLog, ...] = (),
        timings: tuple[TargetTimingView, ...] = (),
        reaction_counts: dict[ReactionTaskStatus, int] | None = None,
        reaction_last_error: str | None = None,
    ) -> LaunchView:
        reaction_counts = reaction_counts or {}
        counts = {status: 0 for status in RunStatus}
        for run in runs:
            counts[run.status] += 1
        active_runs = [run for run in runs if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}]
        launch_settings_runs = active_runs or runs
        active_until_values = [
            value
            for run in launch_settings_runs
            if (value := ensure_utc(run.repeat_until)) is not None
        ]
        active_until = max(active_until_values, default=None)
        account_ids = {run.sender_account_id for run in runs if run.sender_account_id is not None}
        for run in runs:
            if not run.sender_pool_snapshot_json:
                continue
            try:
                account_ids.update(int(item) for item in json.loads(run.sender_pool_snapshot_json))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        account_ids.update(item.failed_account_id for item in failovers)
        account_ids.update(item.replacement_account_id for item in failovers)
        account_ids.update(account_id for account_id, _ in account_labels)
        smart_timer_mode = any(run.repeat_until is not None for run in runs)
        no_timer_interval_seconds = next(
            (
                run.no_timer_interval_seconds_snapshot
                for run in reversed(launch_settings_runs)
                if run.no_timer_interval_seconds_snapshot is not None
            ),
            0,
        )
        auto_sender_failover = next(
            (
                run.auto_sender_failover_snapshot
                for run in reversed(launch_settings_runs)
                if run.auto_sender_failover_snapshot is not None
            ),
            False,
        )
        return LaunchView(
            anchor_run_id=max(run.id for run in runs),
            campaign_id=campaign.id,
            campaign_name=campaign.name,
            mode=campaign.mode,
            total=len(runs),
            queued=counts[RunStatus.QUEUED],
            running=counts[RunStatus.RUNNING],
            succeeded=counts[RunStatus.SUCCEEDED],
            failed=counts[RunStatus.FAILED],
            cancelled=counts[RunStatus.CANCELLED],
            created_at=min(run.created_at for run in runs),
            first_scheduled_for=min(run.scheduled_for for run in runs),
            last_scheduled_for=max(run.scheduled_for for run in runs),
            smart_timer_mode=smart_timer_mode,
            active_until=active_until,
            no_timer_interval_seconds=no_timer_interval_seconds,
            auto_sender_failover=auto_sender_failover,
            sender_mode=campaign.sender_mode,
            sent_publications=len(publications),
            next_scheduled_for=(
                min(run.scheduled_for for run in active_runs) if active_runs else None
            ),
            target_count=len({run.target_id for run in runs}),
            account_count=len(account_ids),
            failover_count=len(failovers),
            account_labels=account_labels,
            failovers=failovers,
            publications=publications,
            attempt_failures=attempt_failures,
            timings=timings,
            archived=all(run.archived_at is not None for run in runs),
            reaction_queued=reaction_counts.get(ReactionTaskStatus.QUEUED, 0),
            reaction_running=reaction_counts.get(ReactionTaskStatus.RUNNING, 0),
            reaction_succeeded=reaction_counts.get(ReactionTaskStatus.SUCCEEDED, 0),
            reaction_failed=reaction_counts.get(ReactionTaskStatus.FAILED, 0),
            reaction_skipped=reaction_counts.get(ReactionTaskStatus.SKIPPED, 0),
            reaction_cancelled=reaction_counts.get(ReactionTaskStatus.CANCELLED, 0),
            reaction_last_error=reaction_last_error,
        )

    @staticmethod
    def _sender_failovers_from_audit(
        events: list[AuditEvent],
        runs: list[CampaignRun],
    ) -> tuple[SenderFailoverView, ...]:
        target_by_run = {run.id: run.target_id for run in runs}
        failovers: list[SenderFailoverView] = []
        for event in events:
            try:
                run_id = int(event.entity_id or "")
                details = json.loads(event.details_json)
                failed_account_id = int(details["failed_account_id"])
                replacement_account_id = int(details["replacement_account_id"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            target_id = target_by_run.get(run_id)
            if target_id is None:
                continue
            scheduled_for: datetime | None = None
            raw_scheduled_for = details.get("scheduled_for")
            if isinstance(raw_scheduled_for, str):
                try:
                    scheduled_for = ensure_utc(
                        datetime.fromisoformat(raw_scheduled_for.replace("Z", "+00:00"))
                    )
                except ValueError:
                    scheduled_for = None
            raw_code = details.get("error_code")
            failovers.append(
                SenderFailoverView(
                    run_id=run_id,
                    target_id=target_id,
                    failed_account_id=failed_account_id,
                    replacement_account_id=replacement_account_id,
                    occurred_at=event.created_at,
                    scheduled_for=scheduled_for,
                    error_code=str(raw_code) if raw_code else None,
                )
            )
        return tuple(failovers)

    @staticmethod
    async def _flush_or_conflict(session: AsyncSession, message: str) -> None:
        try:
            await session.flush()
        except IntegrityError as exc:
            await session.rollback()
            raise ConflictError(message) from exc
