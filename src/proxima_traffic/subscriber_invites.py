"""Durable, consent-based invitations from subscriber lists to owned chats.

Telegram invitation privacy is authoritative.  This module deliberately never
adds people to contacts, retries an uncertain invitation, or attempts to work
around recipient restrictions.  It records the exact outcome and lets another
explicitly selected admin account be tried only for a route-specific refusal.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import case, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.direct_messages import (
    DIRECT_MESSAGE_MAX_RECIPIENTS,
    IMPORT_BATCH_SIZE,
)
from proxima_traffic.failure_policy import ACCOUNT_WIDE_CODES, failure_code, failure_label
from proxima_traffic.gateways import (
    DirectRecipient,
    GatewayError,
    MessageGateway,
    PermanentGatewayError,
    SubscriberInviteDelivery,
)
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    MentionBlacklistEntry,
    Subscriber,
    SubscriberInviteAccountRoute,
    SubscriberInviteCampaign,
    SubscriberInviteCampaignAccount,
    SubscriberInviteLaunch,
    SubscriberInviteLaunchAccountState,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberListMember,
    SubscriberListStatus,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import (
    AccountSendDeferred,
    CampaignRunner,
    ConflictError,
    NotFoundError,
    safe_error_message,
)
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)

INVITE_TERMINAL_STATUSES = frozenset(
    {
        DirectMessageTaskStatus.SUCCEEDED,
        DirectMessageTaskStatus.FAILED,
        DirectMessageTaskStatus.SKIPPED,
        DirectMessageTaskStatus.CANCELLED,
    }
)
INVITE_ROUTE_CODES = frozenset({"invite_recipient_privacy", "invite_recipient_unresolvable"})
INVITE_RECIPIENT_SKIP_CODES = frozenset(
    {
        "invite_recipient_limit",
        "invite_recipient_unavailable",
        "recipient_identity_mismatch",
    }
)
INVITE_GLOBAL_DESTINATION_CODES = frozenset(
    {
        "invite_destination_invalid",
        "invite_destination_full",
    }
)
INVITE_LAUNCH_ACCOUNT_DESTINATION_CODES = frozenset(
    {"invite_destination_rights", "invite_destination_unavailable"}
)


def normalize_invite_destination_ref(value: str) -> tuple[str, str]:
    """Normalize a public channel/supergroup reference without accepting junk."""

    normalized = value.strip()
    if not normalized or len(normalized) > 255 or any(ord(item) < 32 for item in normalized):
        raise ValueError("укажите ссылку или @username канала/супергруппы")
    public_match = re.fullmatch(
        r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,})/?",
        normalized,
        flags=re.IGNORECASE,
    )
    if public_match:
        normalized = f"@{public_match.group(1)}"
    if normalized.startswith("@"):
        if re.fullmatch(r"@[A-Za-z0-9_]{5,32}", normalized) is None:
            raise ValueError("@username канала или супергруппы имеет неверный формат")
        return normalized, normalized.casefold()
    # Private targets can be used only when every selected sender is already
    # an administrator. The actual Telegram preflight remains authoritative.
    if re.fullmatch(r"https?://t\.me/(?:joinchat/|\+)[A-Za-z0-9_-]+/?", normalized, re.I):
        return normalized, normalized.casefold().rstrip("/")
    raise ValueError("используйте @username или приватную ссылку t.me/+…")


@dataclass(frozen=True, slots=True)
class SubscriberInviteCampaignView:
    campaign: SubscriberInviteCampaign
    list_name: str
    account_ids: tuple[int, ...]
    account_labels: tuple[str, ...]
    eligible_count: int
    recent_count: int
    reserved_count: int


@dataclass(frozen=True, slots=True)
class SubscriberInviteLaunchView:
    launch: SubscriberInviteLaunch
    campaign_name: str
    list_name: str
    queued: int
    running: int
    invited: int
    already_participant: int
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


class SubscriberInviteService:
    """Own invitation campaigns, reservations, exact outcomes and worker leases."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        runner: CampaignRunner,
        settings: Settings,
        *,
        worker_id: str,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._runner = runner
        self._settings = settings
        self._worker_id = worker_id
        self._stop_event = asyncio.Event()
        self._running = False

    @staticmethod
    def _segment_condition(segment: SubscriberSegment) -> object:
        if segment == SubscriberSegment.PREMIUM:
            return Subscriber.is_premium.is_(True)
        if segment == SubscriberSegment.NON_PREMIUM:
            return Subscriber.is_premium.is_(False)
        return Subscriber.id.is_not(None)

    async def create_campaign(
        self,
        *,
        name: str,
        subscriber_list_id: int,
        destination_ref: str,
        segment: SubscriberSegment,
        recipient_limit: int,
        contact_cooldown_days: int,
        account_ids: list[int],
    ) -> SubscriberInviteCampaign:
        normalized_name = " ".join(name.split()).strip()
        if not 1 <= len(normalized_name) <= 150:
            raise ConflictError("название приглашения должно содержать от 1 до 150 символов")
        try:
            normalized_ref, destination_key = normalize_invite_destination_ref(destination_ref)
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc
        if not 1 <= recipient_limit <= DIRECT_MESSAGE_MAX_RECIPIENTS:
            raise ConflictError("количество людей должно быть от 1 до 1 000 000")
        if not 0 <= contact_cooldown_days <= 3650:
            raise ConflictError("защита от повтора должна быть от 0 до 3650 дней")
        unique_account_ids = tuple(
            dict.fromkeys(int(value) for value in account_ids if int(value) > 0)
        )
        if not unique_account_ids:
            raise ConflictError("выберите хотя бы один аккаунт-администратор")
        async with self._session_factory() as session:
            subscriber_list = await session.get(SubscriberList, subscriber_list_id)
            if subscriber_list is None or subscriber_list.status != SubscriberListStatus.READY:
                raise ConflictError("выбранная база подписчиков пока не готова")
            if await session.scalar(
                select(SubscriberInviteCampaign.id).where(
                    SubscriberInviteCampaign.name == normalized_name
                )
            ):
                raise ConflictError("приглашение с таким названием уже существует")
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(unique_account_ids)))
            )
            if len(accounts) != len(unique_account_ids):
                raise ConflictError("один из выбранных аккаунтов не найден")
            for account in accounts:
                CampaignRunner._validate_sender(
                    account, require_live_session=not self._settings.dry_run
                )
            campaign = SubscriberInviteCampaign(
                name=normalized_name,
                subscriber_list_id=subscriber_list_id,
                destination_ref=normalized_ref,
                destination_key=destination_key,
                segment=segment,
                recipient_limit=recipient_limit,
                contact_cooldown_days=contact_cooldown_days,
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                [
                    SubscriberInviteCampaignAccount(campaign_id=campaign.id, account_id=account_id)
                    for account_id in unique_account_ids
                ]
            )
            await session.commit()
            await session.refresh(campaign)
            return campaign

    async def list_campaigns(
        self, page: int, page_size: int
    ) -> tuple[list[SubscriberInviteCampaign], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(SubscriberInviteCampaign.id))) or 0)
            items = list(
                await session.scalars(
                    select(SubscriberInviteCampaign)
                    .order_by(
                        SubscriberInviteCampaign.created_at.desc(),
                        SubscriberInviteCampaign.id.desc(),
                    )
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return items, total

    async def get_campaign_view(self, campaign_id: int) -> SubscriberInviteCampaignView:
        async with self._session_factory() as session:
            campaign = await session.get(SubscriberInviteCampaign, campaign_id)
            if campaign is None:
                raise NotFoundError("приглашение не найдено")
            subscriber_list = await session.get(SubscriberList, campaign.subscriber_list_id)
            if subscriber_list is None:
                raise ConflictError("база этого приглашения удалена")
            rows = list(
                (
                    await session.execute(
                        select(Account.id, Account.label)
                        .join(
                            SubscriberInviteCampaignAccount,
                            SubscriberInviteCampaignAccount.account_id == Account.id,
                        )
                        .where(SubscriberInviteCampaignAccount.campaign_id == campaign.id)
                        .order_by(SubscriberInviteCampaignAccount.id)
                    )
                ).all()
            )
            eligible, recent, reserved = await self._eligibility_counts(session, campaign)
            return SubscriberInviteCampaignView(
                campaign=campaign,
                list_name=subscriber_list.name,
                account_ids=tuple(int(row.id) for row in rows),
                account_labels=tuple(str(row.label) for row in rows),
                eligible_count=eligible,
                recent_count=recent,
                reserved_count=reserved,
            )

    async def _eligibility_counts(
        self,
        session: AsyncSession,
        campaign: SubscriberInviteCampaign,
    ) -> tuple[int, int, int]:
        cooldown_before = utc_now() - timedelta(days=campaign.contact_cooldown_days)
        base = [
            SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
            Subscriber.do_not_contact.is_(False),
            self._segment_condition(campaign.segment),
            ~Subscriber.telegram_user_id.in_(select(MentionBlacklistEntry.telegram_user_id)),
        ]
        recent = (
            Subscriber.last_invite_attempt_at.is_not(None)
            & (Subscriber.last_invite_attempt_at > cooldown_before)
            if campaign.contact_cooldown_days > 0
            else Subscriber.id.is_(None)
        )
        row = (
            await session.execute(
                select(
                    func.sum(
                        case(
                            (~recent & Subscriber.reserved_invite_launch_id.is_(None), 1),
                            else_=0,
                        )
                    ),
                    func.sum(case((recent, 1), else_=0)),
                    func.sum(case((Subscriber.reserved_invite_launch_id.is_not(None), 1), else_=0)),
                )
                .join(SubscriberListMember, SubscriberListMember.subscriber_id == Subscriber.id)
                .where(*base)
            )
        ).one()
        return tuple(int(value or 0) for value in row)  # type: ignore[return-value]

    async def launch_campaign(
        self, campaign_id: int, *, consent_confirmed: bool
    ) -> SubscriberInviteLaunch:
        if not consent_confirmed:
            raise ConflictError("нужно подтвердить согласие людей и права на выбранный канал")
        async with self._session_factory() as session:
            campaign = await session.get(SubscriberInviteCampaign, campaign_id)
            if campaign is None or not campaign.enabled:
                raise ConflictError("приглашение недоступно")
            subscriber_list = await session.get(SubscriberList, campaign.subscriber_list_id)
            if subscriber_list is None or subscriber_list.status != SubscriberListStatus.READY:
                raise ConflictError("база подписчиков не готова")
            configured_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(SubscriberInviteCampaignAccount.account_id)
                    .where(SubscriberInviteCampaignAccount.campaign_id == campaign.id)
                    .order_by(SubscriberInviteCampaignAccount.id)
                )
            )
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(configured_ids)))
            )
            snapshot = {
                "campaign": campaign,
                "configured_ids": configured_ids,
                "accounts": accounts,
            }

        campaign = snapshot["campaign"]
        assert isinstance(campaign, SubscriberInviteCampaign)
        accounts = snapshot["accounts"]
        assert isinstance(accounts, list)
        ready_accounts: list[Account] = []
        preflight_failures: list[str] = []
        destination_title: str | None = None
        for account in accounts:
            try:
                CampaignRunner._validate_sender(
                    account, require_live_session=not self._settings.dry_run
                )
                preflight = await self._gateway.preflight_subscriber_invite(
                    account=account,
                    destination_ref=campaign.destination_ref,
                )
            except GatewayError as exc:
                code = failure_code(exc, "gateway_error")
                if code in ACCOUNT_WIDE_CODES:
                    await self._runner.handle_external_account_failure(int(account.id), exc)
                preflight_failures.append(f"{account.label}: {failure_label(code)}")
                continue
            except ConflictError as exc:
                preflight_failures.append(f"{account.label}: {safe_error_message(exc)}")
                continue
            ready_accounts.append(account)
            destination_title = destination_title or preflight.title
        if not ready_accounts:
            diagnostics = "\n".join(f"• {item}" for item in preflight_failures[:8])
            suffix = f":\n{diagnostics}" if diagnostics else ""
            raise ConflictError(
                "ни один выбранный аккаунт не является готовым администратором этой цели"
                + suffix
            )

        now = utc_now()
        ready_ids = [int(account.id) for account in ready_accounts]
        async with self._session_factory() as session:
            campaign = await session.get(SubscriberInviteCampaign, campaign_id)
            if campaign is None or not campaign.enabled:
                raise ConflictError("приглашение недоступно")
            cooldown_before = now - timedelta(days=campaign.contact_cooldown_days)
            conditions = [
                SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
                Subscriber.do_not_contact.is_(False),
                self._segment_condition(campaign.segment),
                ~Subscriber.telegram_user_id.in_(select(MentionBlacklistEntry.telegram_user_id)),
                Subscriber.reserved_invite_launch_id.is_(None),
            ]
            if campaign.contact_cooldown_days > 0:
                conditions.append(
                    or_(
                        Subscriber.last_invite_attempt_at.is_(None),
                        Subscriber.last_invite_attempt_at <= cooldown_before,
                    )
                )
            _, recent, reserved = await self._eligibility_counts(session, campaign)
            if not await session.scalar(
                select(Subscriber.id)
                .join(SubscriberListMember, SubscriberListMember.subscriber_id == Subscriber.id)
                .where(*conditions)
                .limit(1)
            ):
                raise ConflictError("в выбранном сегменте нет доступных людей для приглашения")
            launch = SubscriberInviteLaunch(
                campaign_id=campaign.id,
                subscriber_list_id=campaign.subscriber_list_id,
                status=DirectLaunchStatus.QUEUED,
                destination_ref_snapshot=campaign.destination_ref,
                destination_key_snapshot=campaign.destination_key,
                destination_title_snapshot=destination_title or campaign.destination_title,
                segment_snapshot=campaign.segment,
                recipient_limit_snapshot=campaign.recipient_limit,
                contact_cooldown_days_snapshot=campaign.contact_cooldown_days,
                account_ids_snapshot_json=json.dumps(ready_ids, separators=(",", ":")),
                excluded_recent=recent,
                excluded_reserved=reserved,
                consent_confirmed_at=now,
            )
            session.add(launch)
            await session.flush()
            claimed_ids: list[int] = []
            misses = 0
            while len(claimed_ids) < campaign.recipient_limit:
                candidates = list(
                    await session.scalars(
                        select(Subscriber.id)
                        .join(
                            SubscriberListMember,
                            SubscriberListMember.subscriber_id == Subscriber.id,
                        )
                        .where(*conditions)
                        .order_by(
                            case((Subscriber.last_invite_attempt_at.is_(None), 0), else_=1),
                            Subscriber.last_invite_attempt_at,
                            Subscriber.id,
                        )
                        .limit(min(IMPORT_BATCH_SIZE, campaign.recipient_limit - len(claimed_ids)))
                    )
                )
                if not candidates:
                    break
                claimed = list(
                    await session.scalars(
                        update(Subscriber)
                        .where(
                            Subscriber.id.in_(candidates),
                            Subscriber.reserved_invite_launch_id.is_(None),
                        )
                        .values(reserved_invite_launch_id=launch.id, updated_at=now)
                        .returning(Subscriber.id)
                    )
                )
                if claimed:
                    claimed_ids.extend(int(item) for item in claimed)
                    misses = 0
                else:
                    misses += 1
                    if misses >= 3:
                        break
            if not claimed_ids:
                raise ConflictError("эти люди уже заняты другим запуском приглашений")
            launch.total_tasks = len(claimed_ids)
            account_count = max(1, len(ready_ids))
            pace = max(0.0, float(self._settings.account_pace_min_seconds))
            max_attempts = min(
                100,
                max(int(self._settings.worker_max_attempts), len(ready_ids) + 2),
            )
            for offset in range(0, len(claimed_ids), IMPORT_BATCH_SIZE):
                rows = []
                for index, subscriber_id in enumerate(
                    claimed_ids[offset : offset + IMPORT_BATCH_SIZE], start=offset
                ):
                    rows.append(
                        {
                            "launch_id": launch.id,
                            "subscriber_id": subscriber_id,
                            "status": DirectMessageTaskStatus.QUEUED,
                            "attempt_count": 0,
                            "max_attempts": max_attempts,
                            "scheduled_for": now
                            + timedelta(seconds=(index // account_count) * pace),
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
                await session.execute(sqlite_insert(SubscriberInviteTask).values(rows))
            if destination_title and not campaign.destination_title:
                campaign.destination_title = destination_title[:255]
            await session.commit()
            await session.refresh(launch)
            return launch

    async def list_launches(
        self, page: int, page_size: int
    ) -> tuple[list[SubscriberInviteLaunchView], int]:
        page = max(0, page)
        page_size = max(1, min(50, page_size))
        async with self._session_factory() as session:
            total = int(await session.scalar(select(func.count(SubscriberInviteLaunch.id))) or 0)
            launches = list(
                await session.scalars(
                    select(SubscriberInviteLaunch)
                    .order_by(
                        SubscriberInviteLaunch.created_at.desc(), SubscriberInviteLaunch.id.desc()
                    )
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            return [await self._launch_view(session, item) for item in launches], total

    async def get_launch_view(self, launch_id: int) -> SubscriberInviteLaunchView:
        async with self._session_factory() as session:
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            return await self._launch_view(session, launch)

    @staticmethod
    async def _launch_view(
        session: AsyncSession, launch: SubscriberInviteLaunch
    ) -> SubscriberInviteLaunchView:
        campaign_name = await session.scalar(
            select(SubscriberInviteCampaign.name).where(
                SubscriberInviteCampaign.id == launch.campaign_id
            )
        )
        list_name = await session.scalar(
            select(SubscriberList.name).where(SubscriberList.id == launch.subscriber_list_id)
        )
        counts = dict(
            (
                await session.execute(
                    select(SubscriberInviteTask.status, func.count(SubscriberInviteTask.id))
                    .where(SubscriberInviteTask.launch_id == launch.id)
                    .group_by(SubscriberInviteTask.status)
                )
            ).all()
        )
        already_participant = int(
            await session.scalar(
                select(func.count(SubscriberInviteTask.id)).where(
                    SubscriberInviteTask.launch_id == launch.id,
                    SubscriberInviteTask.status == DirectMessageTaskStatus.SUCCEEDED,
                    SubscriberInviteTask.already_participant.is_(True),
                )
            )
            or 0
        )
        error = (
            await session.execute(
                select(
                    SubscriberInviteTask.error_code,
                    SubscriberInviteTask.error,
                    SubscriberInviteTask.status,
                    Account.label,
                    Subscriber.username,
                    Subscriber.telegram_user_id,
                )
                .join(Subscriber, Subscriber.id == SubscriberInviteTask.subscriber_id)
                .outerjoin(Account, Account.id == SubscriberInviteTask.sender_account_id)
                .where(
                    SubscriberInviteTask.launch_id == launch.id,
                    SubscriberInviteTask.error_code.is_not(None),
                )
                .order_by(SubscriberInviteTask.updated_at.desc())
                .limit(1)
            )
        ).first()
        return SubscriberInviteLaunchView(
            launch=launch,
            campaign_name=str(campaign_name or f"Приглашение #{launch.campaign_id}"),
            list_name=str(list_name or f"База #{launch.subscriber_list_id}"),
            queued=int(counts.get(DirectMessageTaskStatus.QUEUED, 0)),
            running=int(counts.get(DirectMessageTaskStatus.RUNNING, 0)),
            invited=max(
                0,
                int(counts.get(DirectMessageTaskStatus.SUCCEEDED, 0)) - already_participant,
            ),
            already_participant=already_participant,
            failed=int(counts.get(DirectMessageTaskStatus.FAILED, 0)),
            skipped=int(counts.get(DirectMessageTaskStatus.SKIPPED, 0)),
            cancelled=int(counts.get(DirectMessageTaskStatus.CANCELLED, 0)),
            last_error_code=(
                launch.failure_code
                or (str(error.error_code) if error and error.error_code else None)
            ),
            last_error=launch.failure or (str(error.error) if error and error.error else None),
            last_error_status=(
                DirectMessageTaskStatus(error.status)
                if not launch.failure and error and error.status
                else None
            ),
            last_error_account_label=(
                str(error.label) if not launch.failure and error and error.label else None
            ),
            last_error_recipient=(
                f"@{error.username}"
                if not launch.failure and error and error.username
                else (
                    f"ID {error.telegram_user_id}"
                    if not launch.failure and error and error.telegram_user_id is not None
                    else None
                )
            ),
        )

    async def cancel_launch(self, launch_id: int) -> int:
        now = utc_now()
        async with self._session_factory() as session:
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            launch.cancel_requested = True
            cancelled = await session.execute(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.launch_id == launch_id,
                    SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                )
                .values(
                    status=DirectMessageTaskStatus.CANCELLED,
                    error_code="invite_launch_cancelled",
                    error="запуск остановлен оператором до приглашения",
                    finished_at=now,
                    updated_at=now,
                )
            )
            await session.execute(
                update(Subscriber)
                .where(Subscriber.reserved_invite_launch_id == launch_id)
                .values(reserved_invite_launch_id=None, updated_at=now)
            )
            running = int(
                await session.scalar(
                    select(func.count(SubscriberInviteTask.id)).where(
                        SubscriberInviteTask.launch_id == launch_id,
                        SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                    )
                )
                or 0
            )
            if not running:
                launch.status = DirectLaunchStatus.CANCELLED
                launch.finished_at = now
            await session.commit()
            return int(cancelled.rowcount or 0)

    async def process_next(self) -> SubscriberInviteTask | None:
        task = await self._claim_next()
        if task is None:
            return None
        if task.lease_token is None:
            return await self._finish_task(
                task.id,
                None,
                status=DirectMessageTaskStatus.FAILED,
                error_code="lease_expired",
                error="задача не получила токен выполнения",
                record_attempt=False,
            )
        token = task.lease_token
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(task.id, token, heartbeat_stop))
        try:
            async with asyncio.timeout(float(self._settings.worker_step_timeout_seconds)):
                return await self._process_claimed(task.id, token)
        except TimeoutError:
            return await self._finish_task(
                task.id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="invite_delivery_unconfirmed",
                error="приглашение не завершилось вовремя; автоматический повтор запрещён",
                conservative=True,
            )
        except Exception as exc:  # noqa: BLE001 - worker must not die on one record
            logger.exception("Subscriber invite task %s failed unexpectedly", task.id)
            return await self._finish_task(
                task.id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="unhandled_execution_error",
                error=safe_error_message(exc),
                conservative=True,
            )
        finally:
            heartbeat_stop.set()
            await heartbeat

    async def _claim_next(self) -> SubscriberInviteTask | None:
        now = utc_now()
        token = uuid4().hex
        lease_until = now + timedelta(seconds=max(30, self._settings.worker_lease_seconds))
        recovered_launch_ids: set[int] = set()
        async with self._session_factory() as session:
            uncertain_tasks = list(
                await session.scalars(
                    select(SubscriberInviteTask).where(
                        SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                        SubscriberInviteTask.lease_until.is_not(None),
                        SubscriberInviteTask.lease_until <= now,
                        SubscriberInviteTask.delivery_attempted_at.is_not(None),
                    )
                )
            )
            for uncertain in uncertain_tasks:
                # The durable marker is written immediately before Telegram's
                # non-idempotent InviteToChannel call.  After a worker crash we
                # cannot know whether Telegram accepted it, therefore never
                # place this recipient back into the queue.
                uncertain.status = DirectMessageTaskStatus.FAILED
                uncertain.error_code = "invite_delivery_unconfirmed"
                uncertain.error = (
                    "worker прервался после начала запроса к Telegram; "
                    "автоматический повтор запрещён"
                )
                uncertain.finished_at = now
                self._release_task_lease(uncertain)
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id == uncertain.subscriber_id,
                        Subscriber.reserved_invite_launch_id == uncertain.launch_id,
                    )
                    .values(
                        reserved_invite_launch_id=None,
                        last_invite_attempt_at=now,
                        last_invited_target_ref="delivery_unconfirmed",
                        last_error_code="invite_delivery_unconfirmed",
                        last_error=uncertain.error,
                        updated_at=now,
                    )
                )
                recovered_launch_ids.add(int(uncertain.launch_id))
            await session.execute(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                    SubscriberInviteTask.lease_until.is_not(None),
                    SubscriberInviteTask.lease_until <= now,
                    SubscriberInviteTask.delivery_attempted_at.is_(None),
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
                select(SubscriberInviteTask.id)
                .join(
                    SubscriberInviteLaunch,
                    SubscriberInviteLaunch.id == SubscriberInviteTask.launch_id,
                )
                .where(
                    SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                    SubscriberInviteTask.scheduled_for <= now,
                    SubscriberInviteLaunch.cancel_requested.is_(False),
                )
                .order_by(SubscriberInviteTask.scheduled_for, SubscriberInviteTask.id)
                .limit(1)
                .scalar_subquery()
            )
            claimed = await session.execute(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.id == candidate,
                    SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                )
                .values(
                    status=DirectMessageTaskStatus.RUNNING,
                    lease_owner=self._worker_id,
                    lease_token=token,
                    lease_until=lease_until,
                    delivery_attempted_at=None,
                    started_at=func.coalesce(SubscriberInviteTask.started_at, now),
                    updated_at=now,
                )
                .returning(SubscriberInviteTask.id)
            )
            task_id = claimed.scalar_one_or_none()
            await session.commit()
        for launch_id in recovered_launch_ids:
            await self._refresh_launch(launch_id)
        if task_id is None:
            return None
        async with self._session_factory() as session:
            return await session.get(SubscriberInviteTask, int(task_id))

    async def _process_claimed(self, task_id: int, token: str) -> SubscriberInviteTask:
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            launch = await session.get(SubscriberInviteLaunch, task.launch_id)
            subscriber = await session.get(Subscriber, task.subscriber_id)
            if launch is None or subscriber is None:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.FAILED,
                    error_code="preflight_failed",
                    error="запуск или получатель больше не существует",
                    record_attempt=False,
                )
            if launch.cancel_requested:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.CANCELLED,
                    error_code="invite_launch_cancelled",
                    error="запуск остановлен оператором",
                    record_attempt=False,
                )
            if subscriber.do_not_contact:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_opted_out",
                    error="получатель находится в списке «не писать»",
                    record_attempt=False,
                )
            if await session.get(MentionBlacklistEntry, subscriber.telegram_user_id) is not None:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="recipient_blacklisted",
                    error="получатель находится в глобальном стоп-листе",
                    record_attempt=False,
                )
            if subscriber.reserved_invite_launch_id != launch.id:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.SKIPPED,
                    error_code="invite_reservation_lost",
                    error="получатель уже занят другим запуском приглашений",
                    record_attempt=False,
                )
            account_ids = self._account_ids(launch.account_ids_snapshot_json)
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(account_ids)))
            )
            routes = {
                int(route.account_id): route
                for route in await session.scalars(
                    select(SubscriberInviteAccountRoute).where(
                        SubscriberInviteAccountRoute.subscriber_id == subscriber.id,
                        SubscriberInviteAccountRoute.account_id.in_(account_ids),
                    )
                )
            }
            launch_account_states = {
                int(state.account_id): state
                for state in await session.scalars(
                    select(SubscriberInviteLaunchAccountState).where(
                        SubscriberInviteLaunchAccountState.launch_id == launch.id,
                        SubscriberInviteLaunchAccountState.account_id.in_(account_ids),
                    )
                )
            }

        candidates: list[tuple[datetime | None, Account, SubscriberInviteAccountRoute | None]] = []
        for account in accounts:
            route = routes.get(int(account.id))
            launch_account_state = launch_account_states.get(int(account.id))
            if (route is not None and route.invite_blocked) or (
                launch_account_state is not None and launch_account_state.send_blocked
            ):
                continue
            try:
                CampaignRunner._validate_sender(
                    account, require_live_session=not self._settings.dry_run
                )
            except ConflictError:
                continue
            candidates.append((self._sender_ready_at(account), account, route))
        if not candidates:
            blocked_states = [
                state
                for state in launch_account_states.values()
                if state.send_blocked and state.blocked_reason
            ]
            if len(blocked_states) == len(account_ids):
                code = str(blocked_states[0].blocked_reason)
                error = str(blocked_states[0].last_error or "все администраторы цели недоступны")
                return await self._fail_launch_for_destination(task_id, token, code, error)
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code="invite_recipient_unresolvable",
                error="ни один выбранный администратор не может пригласить этого получателя",
            )
        now = utc_now()
        candidates.sort(key=lambda item: (item[0] is not None, item[0] or now, int(item[1].id)))
        resume_at, account, route = candidates[0]
        if resume_at is not None and resume_at > now:
            return await self._reschedule_task(
                task_id,
                token,
                scheduled_for=resume_at,
                error_code="account_pacing",
                error="приглашение ждёт свободный лимит аккаунта",
                increment_attempt=False,
            )
        recipient = DirectRecipient(
            telegram_user_id=subscriber.telegram_user_id,
            username=subscriber.username,
            access_hash=route.access_hash if route is not None else None,
        )

        async def invite() -> SubscriberInviteDelivery:
            stop = await self._pre_invite_stop_code(
                task_id,
                token,
                launch_id=launch.id,
                subscriber_id=subscriber.id,
                telegram_user_id=subscriber.telegram_user_id,
            )
            if stop is not None:
                raise PermanentGatewayError("приглашение отменено повторной проверкой", code=stop)
            await self._mark_delivery_attempted(task_id, token)
            delivery = await self._gateway.invite_subscriber(
                account=account,
                destination_ref=launch.destination_ref_snapshot,
                recipient=recipient,
            )
            try:
                await self._runner.record_external_account_send(
                    int(account.id), operation_key=f"subscriber-invite:{task_id}"
                )
            except Exception as exc:  # Telegram may already have added the person.
                raise PermanentGatewayError(
                    "Telegram принял приглашение, но локальная фиксация лимита не подтверждена",
                    code="invite_delivery_unconfirmed",
                ) from exc
            return delivery

        try:
            delivery = await self._runner.run_account_send_operation(int(account.id), invite)
        except AccountSendDeferred as exc:
            return await self._reschedule_task(
                task_id,
                token,
                scheduled_for=exc.resume_at,
                error_code="account_pacing",
                error="аккаунт занят другой операцией",
                increment_attempt=False,
            )
        except GatewayError as exc:
            return await self._handle_gateway_failure(
                task_id,
                token,
                subscriber_id=subscriber.id,
                account_id=int(account.id),
                exc=exc,
            )
        if delivery.telegram_user_id != subscriber.telegram_user_id:
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code="recipient_identity_mismatch",
                error="шлюз вернул другой Telegram ID; повтор запрещён",
                sender_account_id=int(account.id),
                conservative=True,
            )
        return await self._record_success(
            task_id,
            token,
            subscriber_id=subscriber.id,
            account_id=int(account.id),
            target_ref=launch.destination_key_snapshot,
            delivery=delivery,
        )

    async def _handle_gateway_failure(
        self,
        task_id: int,
        token: str,
        *,
        subscriber_id: int,
        account_id: int,
        exc: GatewayError,
    ) -> SubscriberInviteTask:
        code = failure_code(exc, "gateway_error")
        message = safe_error_message(exc)[:500]
        if code in ACCOUNT_WIDE_CODES:
            await self._runner.handle_external_account_failure(account_id, exc)
        if code in INVITE_GLOBAL_DESTINATION_CODES:
            return await self._fail_launch_for_destination(task_id, token, code, message)
        if code in INVITE_LAUNCH_ACCOUNT_DESTINATION_CODES:
            launch_id, all_selected_admins_blocked = await self._block_launch_account(
                task_id,
                token,
                account_id=account_id,
                code=code,
                error=message,
            )
            if all_selected_admins_blocked:
                return await self._fail_launch_for_destination(task_id, token, code, message)
            logger.info(
                "Subscriber invite sender %s was removed from launch %s after %s",
                account_id,
                launch_id,
                code,
            )
            return await self._reschedule_task(
                task_id,
                token,
                scheduled_for=utc_now(),
                error_code=code,
                error=message,
                increment_attempt=False,
                sender_account_id=account_id,
            )
        if code == "invite_launch_cancelled":
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.CANCELLED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                record_attempt=False,
            )
        if code in {"recipient_blacklisted", "recipient_opted_out", "invite_reservation_lost"}:
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                record_attempt=False,
            )
        if code in INVITE_RECIPIENT_SKIP_CODES:
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.SKIPPED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        if code == "invite_delivery_unconfirmed":
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
                conservative=True,
            )
        if code in INVITE_ROUTE_CODES:
            await self._block_route(subscriber_id, account_id, code, message)
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            next_attempt = task.attempt_count + 1
        if code in INVITE_ROUTE_CODES | ACCOUNT_WIDE_CODES | {"flood_wait"}:
            if next_attempt >= task.max_attempts:
                return await self._finish_task(
                    task_id,
                    token,
                    status=DirectMessageTaskStatus.SKIPPED,
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
                token,
                scheduled_for=utc_now() + timedelta(seconds=max(1, delay)),
                error_code=code,
                error=message,
                increment_attempt=True,
                sender_account_id=account_id,
            )
        # No idempotency key exists for InviteToChannel. A network/RPC outcome
        # may have added the person before the reply was lost, so retrying it
        # would be less safe than surfacing an explicit unknown result.
        return await self._finish_task(
            task_id,
            token,
            status=DirectMessageTaskStatus.FAILED,
            error_code="invite_delivery_unconfirmed",
            error=message,
            sender_account_id=account_id,
            conservative=True,
        )

    async def _pre_invite_stop_code(
        self,
        task_id: int,
        token: str,
        *,
        launch_id: int,
        subscriber_id: int,
        telegram_user_id: int,
    ) -> str | None:
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            if task.launch_id != launch_id or task.subscriber_id != subscriber_id:
                return "invite_reservation_lost"
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None or launch.cancel_requested:
                return "invite_launch_cancelled"
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None or subscriber.reserved_invite_launch_id != launch_id:
                return "invite_reservation_lost"
            if subscriber.do_not_contact:
                return "recipient_opted_out"
            if await session.get(MentionBlacklistEntry, telegram_user_id) is not None:
                return "recipient_blacklisted"
        return None

    async def _mark_delivery_attempted(self, task_id: int, token: str) -> None:
        """Persist the non-idempotent RPC boundary before calling Telegram.

        A successful RPC reply is not enough to reconstruct a crash in the
        narrow period after Telegram receives the request.  Keeping this marker
        makes lease recovery fail conservatively instead of sending again.
        """

        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            task.delivery_attempted_at = now
            task.updated_at = now
            await session.commit()

    async def _block_launch_account(
        self,
        task_id: int,
        token: str,
        *,
        account_id: int,
        code: str,
        error: str,
    ) -> tuple[int, bool]:
        """Disable just one selected admin for this immutable launch."""

        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            launch = await session.get(SubscriberInviteLaunch, task.launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            statement = sqlite_insert(SubscriberInviteLaunchAccountState).values(
                launch_id=launch.id,
                account_id=account_id,
                send_blocked=True,
                blocked_reason=code,
                last_error=error[:500],
                blocked_at=now,
                updated_at=now,
            )
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberInviteLaunchAccountState.launch_id,
                        SubscriberInviteLaunchAccountState.account_id,
                    ],
                    set_={
                        "send_blocked": True,
                        "blocked_reason": code,
                        "last_error": error[:500],
                        "blocked_at": now,
                        "updated_at": now,
                    },
                )
            )
            blocked_ids = {
                int(value)
                for value in await session.scalars(
                    select(SubscriberInviteLaunchAccountState.account_id).where(
                        SubscriberInviteLaunchAccountState.launch_id == launch.id,
                        SubscriberInviteLaunchAccountState.send_blocked.is_(True),
                    )
                )
            }
            selected_ids = set(self._account_ids(launch.account_ids_snapshot_json))
            await session.commit()
            return int(launch.id), selected_ids.issubset(blocked_ids)

    async def _block_route(
        self, subscriber_id: int, account_id: int, code: str, error: str
    ) -> None:
        now = utc_now()
        async with self._session_factory() as session:
            statement = sqlite_insert(SubscriberInviteAccountRoute).values(
                subscriber_id=subscriber_id,
                account_id=account_id,
                invite_blocked=True,
                blocked_reason=code,
                last_error=error[:500],
                updated_at=now,
            )
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberInviteAccountRoute.subscriber_id,
                        SubscriberInviteAccountRoute.account_id,
                    ],
                    set_={
                        "invite_blocked": True,
                        "blocked_reason": code,
                        "last_error": error[:500],
                        "access_hash": (
                            None
                            if code == "invite_recipient_unresolvable"
                            else SubscriberInviteAccountRoute.access_hash
                        ),
                        "updated_at": now,
                    },
                )
            )
            await session.commit()

    async def _record_success(
        self,
        task_id: int,
        token: str,
        *,
        subscriber_id: int,
        account_id: int,
        target_ref: str,
        delivery: SubscriberInviteDelivery,
    ) -> SubscriberInviteTask:
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            subscriber = await session.get(Subscriber, subscriber_id)
            if subscriber is None:
                raise NotFoundError("получатель исчез после приглашения")
            task.status = DirectMessageTaskStatus.SUCCEEDED
            task.sender_account_id = account_id
            task.already_participant = delivery.already_participant
            task.error_code = "invite_already_participant" if delivery.already_participant else None
            task.error = "уже состоит в цели" if delivery.already_participant else None
            task.finished_at = now
            self._release_task_lease(task)
            subscriber.last_invite_attempt_at = now
            subscriber.last_invited_at = now
            subscriber.last_invited_target_ref = target_ref
            subscriber.invite_count += 0 if delivery.already_participant else 1
            subscriber.reserved_invite_launch_id = None
            subscriber.last_error_code = None
            subscriber.last_error = None
            if delivery.username:
                subscriber.username = delivery.username
            if delivery.is_premium is not None:
                subscriber.is_premium = delivery.is_premium
                subscriber.premium_checked_at = now
            statement = sqlite_insert(SubscriberInviteAccountRoute).values(
                subscriber_id=subscriber_id,
                account_id=account_id,
                access_hash=delivery.access_hash,
                invite_blocked=False,
                blocked_reason=None,
                last_error=None,
                resolved_at=now,
                updated_at=now,
            )
            await session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        SubscriberInviteAccountRoute.subscriber_id,
                        SubscriberInviteAccountRoute.account_id,
                    ],
                    set_={
                        "access_hash": func.coalesce(
                            statement.excluded.access_hash,
                            SubscriberInviteAccountRoute.access_hash,
                        ),
                        "invite_blocked": False,
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
        token: str,
        *,
        scheduled_for: datetime,
        error_code: str,
        error: str,
        increment_attempt: bool,
        sender_account_id: int | None = None,
    ) -> SubscriberInviteTask:
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
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
        token: str | None,
        *,
        status: DirectMessageTaskStatus,
        error_code: str,
        error: str,
        sender_account_id: int | None = None,
        record_attempt: bool = True,
        conservative: bool = False,
    ) -> SubscriberInviteTask:
        if status not in INVITE_TERMINAL_STATUSES:
            raise ValueError("terminal invitation task status required")
        now = utc_now()
        async with self._session_factory() as session:
            task = await session.get(SubscriberInviteTask, task_id)
            if task is None:
                raise NotFoundError("задача приглашения не найдена")
            if token is not None and task.lease_token != token:
                raise ConflictError("задача приглашения потеряла токен выполнения")
            task.status = status
            task.error_code = error_code
            task.error = error[:500]
            if sender_account_id is not None:
                task.sender_account_id = sender_account_id
            task.finished_at = now
            self._release_task_lease(task)
            values: dict[str, object] = {
                "reserved_invite_launch_id": None,
                "last_error_code": error_code,
                "last_error": error[:500],
                "updated_at": now,
            }
            if record_attempt:
                values["last_invite_attempt_at"] = now
            if conservative:
                values["last_invited_target_ref"] = "delivery_unconfirmed"
            await session.execute(
                update(Subscriber)
                .where(
                    Subscriber.id == task.subscriber_id,
                    Subscriber.reserved_invite_launch_id == task.launch_id,
                )
                .values(**values)
            )
            launch_id = task.launch_id
            await session.commit()
            await session.refresh(task)
        await self._refresh_launch(launch_id)
        return task

    async def _fail_launch_for_destination(
        self, task_id: int, token: str, code: str, error: str
    ) -> SubscriberInviteTask:
        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            launch = await session.get(SubscriberInviteLaunch, task.launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            task.status = DirectMessageTaskStatus.FAILED
            task.error_code = code
            task.error = error[:500]
            task.finished_at = now
            self._release_task_lease(task)
            await session.execute(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.launch_id == launch.id,
                    SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
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
                .where(Subscriber.reserved_invite_launch_id == launch.id)
                .values(reserved_invite_launch_id=None, updated_at=now)
            )
            launch.cancel_requested = True
            launch.status = DirectLaunchStatus.FAILED
            launch.failure_code = code
            launch.failure = error[:500]
            launch.finished_at = now
            await session.commit()
            await session.refresh(task)
            return task

    async def _refresh_launch(self, launch_id: int) -> None:
        async with self._session_factory() as session:
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None:
                return
            counts = dict(
                (
                    await session.execute(
                        select(SubscriberInviteTask.status, func.count(SubscriberInviteTask.id))
                        .where(SubscriberInviteTask.launch_id == launch_id)
                        .group_by(SubscriberInviteTask.status)
                    )
                ).all()
            )
            active = int(counts.get(DirectMessageTaskStatus.QUEUED, 0)) + int(
                counts.get(DirectMessageTaskStatus.RUNNING, 0)
            )
            if launch.status == DirectLaunchStatus.FAILED:
                launch.finished_at = launch.finished_at or utc_now()
            elif active:
                launch.status = DirectLaunchStatus.RUNNING
                launch.started_at = launch.started_at or utc_now()
            elif launch.cancel_requested:
                launch.status = DirectLaunchStatus.CANCELLED
                launch.finished_at = utc_now()
            else:
                succeeded = int(counts.get(DirectMessageTaskStatus.SUCCEEDED, 0))
                incomplete = int(counts.get(DirectMessageTaskStatus.FAILED, 0)) + int(
                    counts.get(DirectMessageTaskStatus.SKIPPED, 0)
                )
                launch.status = (
                    DirectLaunchStatus.PARTIAL
                    if succeeded and incomplete
                    else DirectLaunchStatus.SUCCEEDED
                    if succeeded
                    else DirectLaunchStatus.FAILED
                )
                launch.finished_at = utc_now()
            await session.commit()

    async def _require_task(
        self, session: AsyncSession, task_id: int, token: str
    ) -> SubscriberInviteTask:
        task = await session.get(SubscriberInviteTask, task_id)
        if task is None:
            raise NotFoundError("задача приглашения не найдена")
        if task.status != DirectMessageTaskStatus.RUNNING or task.lease_token != token:
            raise ConflictError("задача приглашения потеряла право выполнения")
        return task

    @staticmethod
    def _release_task_lease(task: SubscriberInviteTask) -> None:
        task.lease_owner = None
        task.lease_token = None
        task.lease_until = None

    @staticmethod
    def _account_ids(raw: str) -> tuple[int, ...]:
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConflictError("список администраторов запуска повреждён") from exc
        if not isinstance(values, list):
            raise ConflictError("список администраторов запуска повреждён")
        result = tuple(dict.fromkeys(int(value) for value in values if int(value) > 0))
        if not result:
            raise ConflictError("в запуске нет аккаунтов-администраторов")
        return result

    def _sender_ready_at(self, account: Account) -> datetime | None:
        now = utc_now()
        candidates = [self._runner.account_resume_at(account)]
        reservation_until = ensure_utc(account.send_reservation_until)
        if reservation_until is not None and reservation_until > now:
            candidates.append(reservation_until)
        future = [value for value in candidates if value is not None and value > now]
        return max(future) if future else None

    async def _heartbeat(self, task_id: int, token: str, stop_event: asyncio.Event) -> None:
        interval = max(1.0, float(self._settings.worker_heartbeat_seconds))
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except TimeoutError:
                pass
            async with self._session_factory() as session:
                renewed = await session.execute(
                    update(SubscriberInviteTask)
                    .where(
                        SubscriberInviteTask.id == task_id,
                        SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                        SubscriberInviteTask.lease_token == token,
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
        concurrency = max(1, min(8, int(self._settings.subscriber_invites_worker_concurrency)))
        workers = [
            asyncio.create_task(self._worker_loop(reporter), name=f"subscriber-invite-{index + 1}")
            for index in range(concurrency)
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            self._running = False

    async def _worker_loop(self, reporter: WorkerIterationReporter | None) -> None:
        while not self._stop_event.is_set():
            try:
                task = await self.process_next()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception("Subscriber-invite worker iteration failed")
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
                    timeout=max(0.2, float(self._settings.subscriber_invites_poll_seconds)),
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()
