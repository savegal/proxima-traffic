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
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.blacklist_import import subscriber_blacklisted_condition, user_is_blacklisted
from proxima_traffic.config import Settings
from proxima_traffic.db import is_transient_sqlite_lock
from proxima_traffic.direct_messages import (
    DIRECT_MESSAGE_MAX_RECIPIENTS,
    IMPORT_BATCH_SIZE,
    DirectMessageService,
)
from proxima_traffic.failure_policy import (
    ACCOUNT_FATAL_CODES,
    ACCOUNT_WIDE_CODES,
    failure_code,
    failure_label,
)
from proxima_traffic.gateways import (
    DirectRecipient,
    GatewayError,
    MessageGateway,
    PermanentGatewayError,
    SubscriberInviteDelivery,
    SubscriberInviteDestination,
)
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
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
INVITE_ROUTE_CODES = frozenset({"invite_recipient_unresolvable"})
INVITE_UNCERTAIN_CODES = ("invite_delivery_unconfirmed", "recipient_identity_mismatch")
INVITE_RECIPIENT_SKIP_CODES = frozenset(
    {
        "invite_recipient_limit",
        "invite_recipient_unavailable",
        "invite_recipient_privacy",
        "invite_recipient_contact_required",
        "invite_recipient_privacy_premium_pm",
        "invite_sender_requires_premium",
        "invite_recipient_kicked",
        "invite_recipient_segment_changed",
    }
)
INVITE_GLOBAL_DESTINATION_CODES = frozenset(
    {
        "invite_destination_invalid",
        "invite_destination_full",
        "invite_destination_changed",
        "invite_destination_unverified",
        "invite_destination_link_invalid",
    }
)
INVITE_LAUNCH_ACCOUNT_DESTINATION_CODES = frozenset(
    {"invite_destination_rights", "invite_destination_unavailable"}
)

# A destination check is read-only, and each account has its own Telegram
# client lock.  A small bound keeps a large selected pool responsive without
# producing a burst of RPCs from every account at once.
INVITE_PREFLIGHT_CONCURRENCY = 3
InvitePreflightProgress = Callable[[int, int, int, int], Awaitable[None]]
InviteAdminSetupProgress = Callable[[int, int, int, int], Awaitable[None]]


async def _report_progress(
    callback: InvitePreflightProgress | None, checked: int, total: int, ready: int, failed: int
) -> None:
    if callback is None:
        return
    try:
        async with asyncio.timeout(5):
            await callback(checked, total, ready, failed)
    except Exception:  # noqa: BLE001 - a panel edit must not interrupt Telegram work
        logger.warning("Could not update invitation progress screen", exc_info=True)


@dataclass(frozen=True, slots=True)
class SubscriberInviteAdminSetupResult:
    manager_label: str
    granted_labels: tuple[str, ...]
    failed: tuple[tuple[str, str], ...]


def normalize_invite_destination_ref(value: str) -> tuple[str, str]:
    """Normalize a public channel/supergroup reference without accepting junk."""

    normalized = value.strip()
    if not normalized or len(normalized) > 255 or any(ord(item) < 32 for item in normalized):
        raise ValueError("укажите ссылку или @username канала/супергруппы")
    public_match = re.fullmatch(
        r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(?:s/)?"
        r"([A-Za-z0-9_]{1,32})(?:/\d+)?/?(?:[?#].*)?",
        normalized,
        flags=re.IGNORECASE,
    )
    if public_match:
        normalized = f"@{public_match.group(1)}"
    if normalized.startswith("@"):
        # This is lookup, not registration: collectible/legacy names may be
        # shorter than the minimum for creating a new basic username.
        if re.fullmatch(r"@[A-Za-z0-9_]{1,32}", normalized) is None:
            raise ValueError("@username канала или супергруппы имеет неверный формат")
        return normalized, normalized.casefold()
    # Private targets can be used only when every selected sender is already
    # an administrator. The actual Telegram preflight remains authoritative.
    private_match = re.fullmatch(
        r"https?://(?:www\.)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)"
        r"([A-Za-z0-9_-]+)/?(?:[?#].*)?",
        normalized,
        re.IGNORECASE,
    )
    if private_match:
        invite_hash = private_match.group(1)
        canonical = f"https://t.me/+{invite_hash}"
        return canonical, canonical
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
    uncertain: int = 0
    simulated: bool | None = None
    selected_admin_count: int = 0
    ready_admin_count: int = 0
    last_error_code: str | None = None
    last_error: str | None = None
    last_error_status: DirectMessageTaskStatus | None = None
    last_error_account_label: str | None = None
    last_error_recipient: str | None = None
    issues: tuple[tuple[DirectMessageTaskStatus, str, int], ...] = ()
    next_scheduled_for: datetime | None = None
    preflight_failures: tuple[tuple[str, str], ...] = ()
    blocked_admin_count: int = 0

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

    async def prepare_invite_admins(
        self,
        *,
        manager_id: int,
        account_ids: list[int],
        destination_ref: str,
        on_progress: InviteAdminSetupProgress | None = None,
    ) -> SubscriberInviteAdminSetupResult:
        """Use one explicitly chosen manager to grant invite-only admin rights."""

        normalized_ref, _ = normalize_invite_destination_ref(destination_ref)
        selected_ids = tuple(dict.fromkeys(int(value) for value in account_ids if int(value) > 0))
        if not selected_ids:
            raise ConflictError("сначала выберите аккаунты для приглашений")
        async with self._session_factory() as session:
            manager = await session.get(Account, manager_id)
            accounts = list(
                await session.scalars(select(Account).where(Account.id.in_(selected_ids)))
            )
        if manager is None:
            raise NotFoundError("главный аккаунт не найден")
        CampaignRunner._validate_sender(manager, require_live_session=not self._settings.dry_run)
        by_id = {int(account.id): account for account in accounts if account.id is not None}
        targets = [by_id[account_id] for account_id in selected_ids if account_id in by_id]
        if len(targets) != len(selected_ids):
            raise ConflictError("один из выбранных аккаунтов больше не найден")
        for account in targets:
            CampaignRunner._validate_sender(
                account, require_live_session=not self._settings.dry_run
            )
        granted: list[str] = []
        failed: list[tuple[str, str]] = []
        total = len(targets)
        await _report_progress(on_progress, 0, total, 0, 0)
        manager_failure: str | None = None
        for checked, account in enumerate(targets, start=1):
            if not self._settings.dry_run:
                try:
                    await self._gateway.preflight_subscriber_invite(
                        account=account, destination_ref=normalized_ref
                    )
                except GatewayError as exc:
                    code = failure_code(exc, "gateway_error")
                    if code not in {"invite_destination_rights", "invite_destination_unavailable"}:
                        if code in ACCOUNT_WIDE_CODES:
                            await self._runner.handle_external_account_failure(int(account.id), exc)
                        failed.append((account.label, failure_label(code)))
                        await _report_progress(
                            on_progress, checked, total, len(granted), len(failed)
                        )
                        continue
                else:
                    # Existing permitted admins need no mutation and must not
                    # consume another manager interval on repeated preparation.
                    granted.append(account.label)
                    await _report_progress(on_progress, checked, total, len(granted), len(failed))
                    continue
            if manager_failure is not None:
                failed.append((account.label, manager_failure))
                await _report_progress(on_progress, checked, total, len(granted), len(failed))
                continue

            async def grant(target: Account = account) -> None:
                try:
                    await self._gateway.grant_subscriber_invite_admin(
                        manager=manager, account=target, destination_ref=normalized_ref
                    )
                except GatewayError as exc:
                    cause = exc.__cause__ if isinstance(exc.__cause__, GatewayError) else exc
                    if failure_code(cause, "") in ACCOUNT_WIDE_CODES:
                        await self._runner.handle_external_account_failure(manager_id, cause)
                    raise

            try:
                await self._runner.run_account_send_operation(manager_id, grant)
            except AccountSendDeferred as exc:
                manager_failure = "главный аккаунт соблюдает паузу; повторите подготовку после " + (
                    ensure_utc(exc.resume_at) or utc_now()
                ).strftime("%d.%m %H:%M UTC")
                failed.append((account.label, manager_failure))
            except GatewayError as exc:
                code = failure_code(exc, "gateway_error")
                cause = exc.__cause__ if isinstance(exc.__cause__, GatewayError) else exc
                if failure_code(cause, "") in ACCOUNT_WIDE_CODES:
                    manager_failure = failure_label(failure_code(cause, "gateway_error"))
                elif code in {
                    "invite_admin_manager_rights",
                    "invite_admin_limit",
                    "invite_admin_fresh_restriction",
                }:
                    manager_failure = failure_label(code)
                failed.append((account.label, failure_label(code)))
            except ConflictError as exc:
                failed.append((account.label, safe_error_message(exc)))
            else:
                granted.append(account.label)
            await _report_progress(on_progress, checked, total, len(granted), len(failed))
        return SubscriberInviteAdminSetupResult(
            manager_label=manager.label,
            granted_labels=tuple(granted),
            failed=tuple(failed),
        )

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
        uncertain_delivery_before = utc_now() - timedelta(
            days=max(1, campaign.contact_cooldown_days)
        )
        base = [
            SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
            Subscriber.do_not_contact.is_(False),
            self._segment_condition(campaign.segment),
            ~subscriber_blacklisted_condition(),
        ]
        uncertain_delivery = and_(
            Subscriber.last_invited_target_ref.is_not(None),
            Subscriber.last_invited_target_ref == "delivery_unconfirmed",
            or_(
                Subscriber.last_invite_attempt_at.is_(None),
                Subscriber.last_invite_attempt_at > uncertain_delivery_before,
            ),
        )
        recent = or_(
            (
                Subscriber.last_invite_attempt_at.is_not(None)
                & (Subscriber.last_invite_attempt_at > cooldown_before)
                if campaign.contact_cooldown_days > 0
                else Subscriber.id.is_(None)
            ),
            uncertain_delivery,
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
        self,
        campaign_id: int,
        *,
        consent_confirmed: bool,
        on_preflight_progress: InvitePreflightProgress | None = None,
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

        async def check_account(
            account: Account,
        ) -> tuple[Account, SubscriberInviteDestination | None, str | None]:
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
                return account, None, failure_label(code)
            except ConflictError as exc:
                return account, None, safe_error_message(exc)
            if not self._settings.dry_run and not preflight.telegram_chat_id:
                return account, None, "Telegram не подтвердил ID выбранной цели"
            return account, preflight, None

        # Do not run preflight for a large account pool all at once.  A few
        # independent checks in parallel make the launch screen responsive
        # while preserving Telegram-safe per-account serialization.
        semaphore = asyncio.Semaphore(min(INVITE_PREFLIGHT_CONCURRENCY, max(1, len(accounts))))

        async def bounded_check(
            account: Account,
        ) -> tuple[Account, SubscriberInviteDestination | None, str | None]:
            async with semaphore:
                try:
                    async with asyncio.timeout(float(self._settings.worker_step_timeout_seconds)):
                        return await check_account(account)
                except TimeoutError:
                    return (
                        account,
                        None,
                        "истекло время проверки Telegram; приглашения не отправлены",
                    )

        ready_accounts: list[Account] = []
        preflight_failures: list[tuple[str, str]] = []
        destination_title: str | None = None
        destination_id: int | None = None
        checked = 0
        total_accounts = len(accounts)
        await _report_progress(on_preflight_progress, 0, total_accounts, 0, 0)
        checks = [asyncio.create_task(bounded_check(account)) for account in accounts]
        try:
            for result in asyncio.as_completed(checks):
                account, destination, failure = await result
                checked += 1
                if failure:
                    preflight_failures.append((account.label, failure))
                else:
                    assert destination is not None
                    if (
                        destination_id is not None
                        and destination.telegram_chat_id != destination_id
                    ):
                        raise ConflictError(
                            "выбранные аккаунты видят разные цели; проверьте адрес канала"
                        )
                    ready_accounts.append(account)
                    destination_title = destination_title or destination.title
                    destination_id = destination_id or destination.telegram_chat_id
                await _report_progress(
                    on_preflight_progress,
                    checked,
                    total_accounts,
                    len(ready_accounts),
                    len(preflight_failures),
                )
        finally:
            for check in checks:
                if not check.done():
                    check.cancel()
            await asyncio.gather(*checks, return_exceptions=True)
        if not ready_accounts:
            diagnostics = "\n".join(
                f"• {label}: {reason}" for label, reason in preflight_failures[:8]
            )
            suffix = f":\n{diagnostics}" if diagnostics else ""
            raise ConflictError(
                "ни один выбранный аккаунт не является готовым администратором этой цели" + suffix
            )

        now = utc_now()
        ready_ids = sorted(int(account.id) for account in ready_accounts)
        verified_ref = campaign.destination_ref
        verified_list_id = campaign.subscriber_list_id
        async with self._session_factory() as session:
            campaign = await session.get(SubscriberInviteCampaign, campaign_id)
            if campaign is None or not campaign.enabled:
                raise ConflictError("приглашение недоступно")
            current_account_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(SubscriberInviteCampaignAccount.account_id)
                    .where(SubscriberInviteCampaignAccount.campaign_id == campaign.id)
                    .order_by(SubscriberInviteCampaignAccount.id)
                )
            )
            if (
                campaign.destination_ref != verified_ref
                or campaign.subscriber_list_id != verified_list_id
                or current_account_ids != configured_ids
            ):
                raise ConflictError("параметры изменились во время проверки; повторите запуск")
            subscriber_list = await session.get(SubscriberList, campaign.subscriber_list_id)
            if subscriber_list is None or subscriber_list.status != SubscriberListStatus.READY:
                raise ConflictError("база подписчиков больше не готова к запуску")
            cooldown_before = now - timedelta(days=campaign.contact_cooldown_days)
            uncertain_delivery_before = now - timedelta(days=max(1, campaign.contact_cooldown_days))
            uncertain_delivery = and_(
                Subscriber.last_invited_target_ref.is_not(None),
                Subscriber.last_invited_target_ref == "delivery_unconfirmed",
                or_(
                    Subscriber.last_invite_attempt_at.is_(None),
                    Subscriber.last_invite_attempt_at > uncertain_delivery_before,
                ),
            )
            conditions = [
                SubscriberListMember.subscriber_list_id == campaign.subscriber_list_id,
                Subscriber.do_not_contact.is_(False),
                self._segment_condition(campaign.segment),
                ~subscriber_blacklisted_condition(),
                Subscriber.reserved_invite_launch_id.is_(None),
                ~uncertain_delivery,
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
                destination_telegram_id_snapshot=destination_id,
                segment_snapshot=campaign.segment,
                recipient_limit_snapshot=campaign.recipient_limit,
                contact_cooldown_days_snapshot=campaign.contact_cooldown_days,
                account_ids_snapshot_json=json.dumps(ready_ids, separators=(",", ":")),
                selected_account_ids_snapshot_json=json.dumps(
                    configured_ids, separators=(",", ":")
                ),
                preflight_failures_snapshot_json=json.dumps(preflight_failures, ensure_ascii=False),
                simulated_snapshot=self._settings.dry_run,
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

    async def list_recipients(
        self,
        *,
        launch_id: int | None = None,
        page: int = 0,
        query: str = "",
        result_filter: RecipientReportFilter = RecipientReportFilter.SUCCESS,
    ) -> RecipientReportPage:
        return await list_recipient_report(
            self._session_factory,
            kind=RecipientReportKind.INVITE,
            launch_id=launch_id,
            page=page,
            query=query,
            result_filter=result_filter,
        )

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
            return await self._launch_views(session, launches), total

    async def get_launch_view(self, launch_id: int) -> SubscriberInviteLaunchView:
        async with self._session_factory() as session:
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            return await self._launch_view(session, launch)

    async def refresh_launches(self, launch_ids: Iterable[int]) -> None:
        """Refresh status after a shared stop-list cancels queued invitations."""

        for launch_id in sorted({int(value) for value in launch_ids if int(value) > 0}):
            await self._refresh_launch(launch_id)

    @staticmethod
    async def _launch_view(
        session: AsyncSession, launch: SubscriberInviteLaunch
    ) -> SubscriberInviteLaunchView:
        return (await SubscriberInviteService._launch_views(session, [launch]))[0]

    @staticmethod
    async def _launch_views(
        session: AsyncSession, launches: list[SubscriberInviteLaunch]
    ) -> list[SubscriberInviteLaunchView]:
        """Load exact counters and diagnostics for a page with four queries."""

        if not launches:
            return []
        ids = [launch.id for launch in launches]
        selected_admins = (
            select(func.count(SubscriberInviteCampaignAccount.id))
            .where(
                SubscriberInviteCampaignAccount.campaign_id == SubscriberInviteLaunch.campaign_id
            )
            .correlate(SubscriberInviteLaunch)
            .scalar_subquery()
        )
        blocked_admins = (
            select(func.count(SubscriberInviteLaunchAccountState.id))
            .where(
                SubscriberInviteLaunchAccountState.launch_id == SubscriberInviteLaunch.id,
                SubscriberInviteLaunchAccountState.send_blocked.is_(True),
            )
            .correlate(SubscriberInviteLaunch)
            .scalar_subquery()
        )
        names = {
            row.id: row
            for row in (
                await session.execute(
                    select(
                        SubscriberInviteLaunch.id,
                        SubscriberInviteCampaign.name.label("campaign_name"),
                        SubscriberList.name.label("list_name"),
                        selected_admins.label("selected_admins"),
                        blocked_admins.label("blocked_admins"),
                    )
                    .outerjoin(
                        SubscriberInviteCampaign,
                        SubscriberInviteCampaign.id == SubscriberInviteLaunch.campaign_id,
                    )
                    .outerjoin(
                        SubscriberList,
                        SubscriberList.id == SubscriberInviteLaunch.subscriber_list_id,
                    )
                    .where(SubscriberInviteLaunch.id.in_(ids))
                )
            ).all()
        }
        counts_by_launch: dict[int, dict[DirectMessageTaskStatus, int]] = {id_: {} for id_ in ids}
        already_by_launch: dict[int, int] = {}
        uncertain_by_launch: dict[int, int] = {}
        next_by_launch: dict[int, datetime | None] = {}
        rows = (
            await session.execute(
                select(
                    SubscriberInviteTask.launch_id,
                    SubscriberInviteTask.status,
                    func.count(SubscriberInviteTask.id),
                    func.sum(
                        case((SubscriberInviteTask.already_participant.is_(True), 1), else_=0)
                    ),
                    func.sum(
                        case(
                            (SubscriberInviteTask.error_code.in_(INVITE_UNCERTAIN_CODES), 1),
                            else_=0,
                        )
                    ),
                    func.min(SubscriberInviteTask.scheduled_for),
                )
                .where(SubscriberInviteTask.launch_id.in_(ids))
                .group_by(SubscriberInviteTask.launch_id, SubscriberInviteTask.status)
            )
        ).all()
        for launch_id, status, count, already, uncertain, scheduled in rows:
            status = DirectMessageTaskStatus(status)
            counts_by_launch[launch_id][status] = int(count)
            if status == DirectMessageTaskStatus.SUCCEEDED:
                already_by_launch[launch_id] = int(already or 0)
            if status == DirectMessageTaskStatus.FAILED:
                uncertain_by_launch[launch_id] = int(uncertain or 0)
            if status == DirectMessageTaskStatus.QUEUED:
                next_by_launch[launch_id] = scheduled
        # Rank in SQL so a page never loads an unbounded task history.
        errors = (
            select(
                SubscriberInviteTask.launch_id,
                SubscriberInviteTask.error_code,
                SubscriberInviteTask.error,
                SubscriberInviteTask.status,
                Account.label,
                Subscriber.username,
                Subscriber.telegram_user_id,
                func.row_number()
                .over(
                    partition_by=SubscriberInviteTask.launch_id,
                    order_by=(
                        SubscriberInviteTask.updated_at.desc(),
                        SubscriberInviteTask.id.desc(),
                    ),
                )
                .label("position"),
            )
            .join(Subscriber, Subscriber.id == SubscriberInviteTask.subscriber_id)
            .outerjoin(Account, Account.id == SubscriberInviteTask.sender_account_id)
            .where(
                SubscriberInviteTask.launch_id.in_(ids),
                SubscriberInviteTask.error_code.is_not(None),
                SubscriberInviteTask.status != DirectMessageTaskStatus.SUCCEEDED,
            )
            .subquery()
        )
        errors_by_launch = {
            row.launch_id: row
            for row in (await session.execute(select(errors).where(errors.c.position == 1))).all()
        }
        issues = (
            select(
                SubscriberInviteTask.launch_id,
                SubscriberInviteTask.status,
                SubscriberInviteTask.error_code,
                func.count(SubscriberInviteTask.id).label("count"),
                func.row_number()
                .over(
                    partition_by=SubscriberInviteTask.launch_id,
                    order_by=(
                        func.count(SubscriberInviteTask.id).desc(),
                        SubscriberInviteTask.status,
                        SubscriberInviteTask.error_code,
                    ),
                )
                .label("position"),
            )
            .where(
                SubscriberInviteTask.launch_id.in_(ids),
                SubscriberInviteTask.status.in_(
                    (
                        DirectMessageTaskStatus.QUEUED,
                        DirectMessageTaskStatus.FAILED,
                        DirectMessageTaskStatus.SKIPPED,
                    )
                ),
                SubscriberInviteTask.error_code.is_not(None),
            )
            .group_by(
                SubscriberInviteTask.launch_id,
                SubscriberInviteTask.status,
                SubscriberInviteTask.error_code,
            )
            .subquery()
        )
        issues_by_launch: dict[int, list] = {id_: [] for id_ in ids}
        for row in (
            await session.execute(
                select(issues)
                .where(issues.c.position <= 3)
                .order_by(issues.c.launch_id, issues.c.position)
            )
        ).all():
            issues_by_launch[row.launch_id].append(row)
        views: list[SubscriberInviteLaunchView] = []
        for launch in launches:
            name = names[launch.id]
            campaign_name, list_name = name.campaign_name, name.list_name
            selected_admin_count = int(name.selected_admins or 0)
            if launch.selected_account_ids_snapshot_json is not None:
                selected_admin_count = len(
                    SubscriberInviteService._account_ids(launch.selected_account_ids_snapshot_json)
                )
            try:
                preflight_failures = tuple(
                    (str(label), str(reason))
                    for label, reason in json.loads(launch.preflight_failures_snapshot_json or "[]")
                )
            except (TypeError, ValueError):
                preflight_failures = ()
            try:
                ready_admin_count = len(json.loads(launch.account_ids_snapshot_json or "[]"))
            except (TypeError, json.JSONDecodeError):
                ready_admin_count = 0
            counts = counts_by_launch[launch.id]
            already_participant = already_by_launch.get(launch.id, 0)
            uncertain = uncertain_by_launch.get(launch.id, 0)
            error = errors_by_launch.get(launch.id)
            issue_rows = issues_by_launch[launch.id]
            next_scheduled_for = next_by_launch.get(launch.id)
            views.append(
                SubscriberInviteLaunchView(
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
                    uncertain=uncertain,
                    simulated=launch.simulated_snapshot,
                    skipped=int(counts.get(DirectMessageTaskStatus.SKIPPED, 0)),
                    cancelled=int(counts.get(DirectMessageTaskStatus.CANCELLED, 0)),
                    selected_admin_count=selected_admin_count,
                    ready_admin_count=ready_admin_count,
                    preflight_failures=preflight_failures,
                    blocked_admin_count=int(name.blocked_admins or 0),
                    last_error_code=(
                        launch.failure_code
                        or (str(error.error_code) if error and error.error_code else None)
                    ),
                    last_error=launch.failure
                    or (str(error.error) if error and error.error else None),
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
                    issues=tuple(
                        (
                            DirectMessageTaskStatus(row.status),
                            str(row.error_code),
                            int(row._mapping["count"]),
                        )
                        for row in issue_rows
                        if row.error_code
                    ),
                    next_scheduled_for=ensure_utc(next_scheduled_for),
                )
            )
        return views

    async def cancel_launch(self, launch_id: int) -> int:
        now = utc_now()
        async with self._session_factory() as session:
            launch = await session.get(SubscriberInviteLaunch, launch_id)
            if launch is None:
                raise NotFoundError("запуск приглашений не найден")
            launch.cancel_requested = True
            queued_subscriber_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(SubscriberInviteTask.subscriber_id).where(
                        SubscriberInviteTask.launch_id == launch_id,
                        SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                )
            )
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
            if queued_subscriber_ids:
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id.in_(queued_subscriber_ids),
                        Subscriber.reserved_invite_launch_id == launch_id,
                    )
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
            return await self._handle_interrupted_task(task.id, token, "step_timeout")
        except Exception:  # noqa: BLE001 - worker must not die on one record
            logger.exception("Subscriber invite task %s failed unexpectedly", task.id)
            return await self._handle_interrupted_task(task.id, token, "unhandled_execution_error")
        finally:
            heartbeat_stop.set()
            await heartbeat

    async def _handle_interrupted_task(
        self, task_id: int, token: str, code: str
    ) -> SubscriberInviteTask:
        async with self._session_factory() as session:
            task = await session.get(SubscriberInviteTask, task_id)
            if task is None:
                raise NotFoundError("задача приглашения не найдена")
            if task.status in INVITE_TERMINAL_STATUSES or task.lease_token != token:
                return task
            attempted = task.delivery_attempted_at is not None
            sender_account_id = task.sender_account_id
            exhausted = task.attempt_count + 1 >= task.max_attempts
        if not attempted and not exhausted:
            return await self._reschedule_task(
                task_id,
                token,
                scheduled_for=utc_now()
                + timedelta(seconds=self._settings.worker_retry_base_seconds),
                error_code=code,
                error="сбой до приглашения; задача будет повторена",
                increment_attempt=True,
            )
        if attempted and sender_account_id is not None:
            try:
                await self._runner.record_external_account_send(
                    int(sender_account_id), operation_key=f"subscriber-invite:{task_id}"
                )
            except Exception:  # noqa: BLE001 - preserve unknown Telegram outcome
                logger.exception("Could not reserve quota for interrupted invite task %s", task_id)
        return await self._finish_task(
            task_id,
            token,
            status=DirectMessageTaskStatus.FAILED,
            error_code="invite_delivery_unconfirmed" if attempted else code,
            error=(
                f"приглашение прервалось ({code}); результат Telegram неизвестен, повтор запрещён"
                if attempted
                else "исчерпаны попытки подготовки приглашения"
            ),
            sender_account_id=sender_account_id,
            record_attempt=attempted,
            conservative=attempted,
        )

    async def _claim_next(self) -> SubscriberInviteTask | None:
        now = utc_now()
        token = uuid4().hex
        lease_until = now + timedelta(seconds=max(30, self._settings.worker_lease_seconds))
        recovered_launch_ids: set[int] = set()
        recovered_send_attempts: list[tuple[int, int]] = []
        async with self._session_factory() as session:
            # Serialize recovery with result writes and lease renewal BEFORE
            # observing expired rows, just as the direct-message queue does.
            await session.execute(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                    SubscriberInviteTask.lease_until <= now,
                )
                .values(updated_at=SubscriberInviteTask.updated_at)
            )
            # These tasks have not reached the non-idempotent Telegram RPC yet,
            # so a cancelled launch can safely terminalise them.  Without this
            # branch the generic lease recovery makes them QUEUED while the
            # claim query excludes cancelled launches forever.
            cancelled_pre_rpc_tasks = list(
                await session.scalars(
                    select(SubscriberInviteTask)
                    .join(
                        SubscriberInviteLaunch,
                        SubscriberInviteLaunch.id == SubscriberInviteTask.launch_id,
                    )
                    .where(
                        SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                        SubscriberInviteTask.lease_until.is_not(None),
                        SubscriberInviteTask.lease_until <= now,
                        SubscriberInviteTask.delivery_attempted_at.is_(None),
                        SubscriberInviteLaunch.cancel_requested.is_(True),
                    )
                )
            )
            for task in cancelled_pre_rpc_tasks:
                task.status = DirectMessageTaskStatus.CANCELLED
                task.error_code = "invite_launch_cancelled"
                task.error = "запуск остановлен оператором до приглашения"
                task.finished_at = now
                self._release_task_lease(task)
                await session.execute(
                    update(Subscriber)
                    .where(
                        Subscriber.id == task.subscriber_id,
                        Subscriber.reserved_invite_launch_id == task.launch_id,
                    )
                    .values(reserved_invite_launch_id=None, updated_at=now)
                )
                recovered_launch_ids.add(int(task.launch_id))
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
                if uncertain.sender_account_id is not None:
                    recovered_send_attempts.append(
                        (int(uncertain.sender_account_id), int(uncertain.id))
                    )
            # Flushed terminal statuses exclude these rows without an
            # unbounded NOT IN list when a large cancelled batch is recovered.
            await session.flush()
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
            if task_id is None:
                # A result may have committed before a later summary update
                # failed. Repair completed batches without replaying their RPCs.
                recovered_launch_ids.update(
                    int(value)
                    for value in await session.scalars(
                        select(SubscriberInviteLaunch.id)
                        .where(
                            SubscriberInviteLaunch.status.in_(
                                (DirectLaunchStatus.QUEUED, DirectLaunchStatus.RUNNING)
                            ),
                            ~select(SubscriberInviteTask.id)
                            .where(
                                SubscriberInviteTask.launch_id == SubscriberInviteLaunch.id,
                                SubscriberInviteTask.status.in_(
                                    (
                                        DirectMessageTaskStatus.QUEUED,
                                        DirectMessageTaskStatus.RUNNING,
                                    )
                                ),
                            )
                            .exists(),
                        )
                        .limit(50)
                    )
                )
            await session.commit()
        for account_id, recovered_task_id in recovered_send_attempts:
            try:
                await self._runner.record_external_account_send(
                    account_id, operation_key=f"subscriber-invite:{recovered_task_id}"
                )
            except Exception:  # noqa: BLE001 - unknown invitation remains terminal
                logger.exception(
                    "Could not reserve quota for recovered invite task %s", recovered_task_id
                )
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
            if (
                launch.simulated_snapshot is not None
                and launch.simulated_snapshot != self._settings.dry_run
            ):
                return await self._fail_launch_for_destination(
                    task_id,
                    token,
                    "launch_mode_changed",
                    "режим симуляции изменился после создания запуска",
                )
            if not self._settings.dry_run and launch.destination_telegram_id_snapshot is None:
                return await self._fail_launch_for_destination(
                    task_id,
                    token,
                    "invite_destination_unverified",
                    "в старом запуске не сохранён Telegram ID цели; создайте новый запуск",
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
            if await user_is_blacklisted(session, subscriber.telegram_user_id, subscriber.username):
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
        launch_created_at = ensure_utc(launch.created_at) or utc_now()
        valid_sender_count = 0
        for account in accounts:
            try:
                CampaignRunner._validate_sender(
                    account, require_live_session=not self._settings.dry_run
                )
            except ConflictError:
                continue
            valid_sender_count += 1
            route = routes.get(int(account.id))
            launch_account_state = launch_account_states.get(int(account.id))
            route_blocked_this_launch = (
                route is not None
                and route.invite_blocked
                and (ensure_utc(route.updated_at) or launch_created_at) >= launch_created_at
            )
            if route_blocked_this_launch or (
                launch_account_state is not None and launch_account_state.send_blocked
            ):
                continue
            candidates.append((self._sender_ready_at(account), account, route))
        if not candidates:
            if not valid_sender_count:
                return await self._fail_launch_for_destination(
                    task_id,
                    token,
                    "sender_unavailable",
                    "все выбранные аккаунты выключены или их сессии больше не действительны",
                )
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

        async def invite() -> SubscriberInviteTask:
            async def before_invite() -> None:
                stop = await self._pre_invite_stop_code(
                    task_id,
                    token,
                    launch_id=launch.id,
                    subscriber_id=subscriber.id,
                    telegram_user_id=subscriber.telegram_user_id,
                    account_id=int(account.id),
                )
                if stop is not None:
                    raise PermanentGatewayError(
                        "приглашение отменено повторной проверкой", code=stop
                    )
                await self._mark_delivery_attempted(task_id, token, account_id=int(account.id))

            stop = await self._pre_invite_stop_code(
                task_id,
                token,
                launch_id=launch.id,
                subscriber_id=subscriber.id,
                telegram_user_id=subscriber.telegram_user_id,
                account_id=int(account.id),
            )
            if stop is not None:
                raise PermanentGatewayError("приглашение отменено повторной проверкой", code=stop)
            try:
                delivery = await self._gateway.invite_subscriber(
                    account=account,
                    destination_ref=launch.destination_ref_snapshot,
                    recipient=recipient,
                    expected_destination_id=launch.destination_telegram_id_snapshot,
                    require_premium=(
                        None
                        if launch.segment_snapshot == SubscriberSegment.ALL
                        else launch.segment_snapshot == SubscriberSegment.PREMIUM
                    ),
                    before_invite=before_invite,
                )
            except GatewayError as exc:
                return await self._handle_gateway_failure(
                    task_id, token, subscriber_id=subscriber.id, account_id=int(account.id), exc=exc
                )
            if not delivery.already_participant:
                try:
                    await self._runner.record_external_account_send(
                        int(account.id), operation_key=f"subscriber-invite:{task_id}"
                    )
                except Exception as exc:  # Telegram may already have added the person.
                    raise PermanentGatewayError(
                        "Telegram принял приглашение, но локальная фиксация лимита не подтверждена",
                        code="invite_delivery_unconfirmed",
                    ) from exc
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

        try:
            return await self._runner.run_account_send_operation(int(account.id), invite)
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
        if (
            isinstance(exc.__cause__, GatewayError)
            and failure_code(exc.__cause__, "") in ACCOUNT_WIDE_CODES
        ):
            await self._runner.handle_external_account_failure(account_id, exc.__cause__)
        if code in ACCOUNT_WIDE_CODES:
            await self._runner.handle_external_account_failure(account_id, exc)
        if code in {"account_restricted", "peer_flood"} and (
            self._settings.account_restriction_cooldown_hours == 0
        ):
            await self._block_launch_account(
                task_id, token, account_id=account_id, code=code, error=message
            )
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        if code in INVITE_GLOBAL_DESTINATION_CODES:
            return await self._fail_launch_for_destination(task_id, token, code, message)
        if code in ACCOUNT_FATAL_CODES or code == "sender_unavailable":
            launch_id, all_selected_admins_blocked = await self._block_launch_account(
                task_id,
                token,
                account_id=account_id,
                code=code,
                error=message,
            )
            if all_selected_admins_blocked:
                return await self._fail_launch_for_destination(task_id, token, code, message)
            logger.warning(
                "Subscriber invite sender %s was removed from launch %s after fatal %s",
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
        if code == "recipient_identity_mismatch":
            async with self._session_factory() as session:
                task = await self._require_task(session, task_id, token)
                attempted = task.delivery_attempted_at is not None
            if not attempted:
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
        if code in INVITE_UNCERTAIN_CODES:
            try:
                await self._runner.record_external_account_send(
                    account_id, operation_key=f"subscriber-invite:{task_id}"
                )
            except Exception:  # noqa: BLE001 - preserve the unknown Telegram outcome
                logger.exception("Could not reserve quota for uncertain invite task %s", task_id)
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
        if code in INVITE_ROUTE_CODES | ACCOUNT_WIDE_CODES | {
            "flood_wait",
            # These originate before InviteToChannelRequest: one process owns
            # the Telegram account, or no usable proxy route was established.
            # They therefore cannot mean the recipient was already invited.
            "account_operation_busy",
            "proxy_unavailable",
            "invite_preflight_unavailable",
        }:
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
        if isinstance(exc, PermanentGatewayError):
            # A definite Telegram/local refusal is a known failure, not an
            # ambiguous invitation. Keep the unrecognized RPC detail for review.
            return await self._finish_task(
                task_id,
                token,
                status=DirectMessageTaskStatus.FAILED,
                error_code=code,
                error=message,
                sender_account_id=account_id,
            )
        # No idempotency key exists for InviteToChannel. A network/RPC outcome
        # may have added the person before the reply was lost, so retrying it
        # would be less safe than surfacing an explicit unknown result.
        try:
            await self._runner.record_external_account_send(
                account_id, operation_key=f"subscriber-invite:{task_id}"
            )
        except Exception:  # noqa: BLE001 - preserve the unknown Telegram outcome
            logger.exception("Could not reserve quota for uncertain invite task %s", task_id)
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
        account_id: int | None = None,
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
            if await user_is_blacklisted(session, telegram_user_id, subscriber.username):
                return "recipient_blacklisted"
            if account_id is not None:
                if account_id not in self._account_ids(launch.account_ids_snapshot_json):
                    return "sender_unavailable"
                account = await session.get(Account, account_id)
                try:
                    CampaignRunner._validate_sender(
                        account, require_live_session=not self._settings.dry_run
                    )
                except ConflictError:
                    return "sender_unavailable"
                state = await session.scalar(
                    select(SubscriberInviteLaunchAccountState).where(
                        SubscriberInviteLaunchAccountState.launch_id == launch_id,
                        SubscriberInviteLaunchAccountState.account_id == account_id,
                    )
                )
                if state is not None and state.send_blocked:
                    return "sender_unavailable"
        return None

    async def _mark_delivery_attempted(self, task_id: int, token: str, *, account_id: int) -> None:
        """Persist the non-idempotent RPC boundary before calling Telegram.

        A successful RPC reply is not enough to reconstruct a crash in the
        narrow period after Telegram receives the request.  Keeping this marker
        makes lease recovery fail conservatively instead of sending again.
        """

        now = utc_now()
        async with self._session_factory() as session:
            task = await self._require_task(session, task_id, token)
            launch_id, subscriber_id = task.launch_id, task.subscriber_id
            marked = await session.scalar(
                update(SubscriberInviteTask)
                .where(
                    SubscriberInviteTask.id == task_id,
                    SubscriberInviteTask.status == DirectMessageTaskStatus.RUNNING,
                    SubscriberInviteTask.lease_token == token,
                    select(SubscriberInviteLaunch.id)
                    .where(
                        SubscriberInviteLaunch.id == launch_id,
                        SubscriberInviteLaunch.cancel_requested.is_(False),
                    )
                    .exists(),
                    select(Subscriber.id)
                    .where(
                        Subscriber.id == subscriber_id,
                        Subscriber.reserved_invite_launch_id == launch_id,
                        Subscriber.do_not_contact.is_(False),
                        ~subscriber_blacklisted_condition(),
                    )
                    .exists(),
                )
                .values(delivery_attempted_at=now, sender_account_id=account_id, updated_at=now)
                .returning(SubscriberInviteTask.id)
            )
            await session.commit()
        if marked is None:
            async with self._session_factory() as session:
                subscriber = await session.get(Subscriber, subscriber_id)
            stop = await self._pre_invite_stop_code(
                task_id,
                token,
                launch_id=launch_id,
                subscriber_id=subscriber_id,
                telegram_user_id=subscriber.telegram_user_id if subscriber else 0,
                account_id=account_id,
            )
            raise PermanentGatewayError(
                "приглашение остановлено на границе запроса",
                code=stop or "invite_reservation_lost",
            )

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
            if not delivery.already_participant:
                subscriber.last_invited_at = now
                subscriber.last_invited_target_ref = target_ref
                subscriber.invite_count += 1
            subscriber.reserved_invite_launch_id = None
            subscriber.last_error_code = None
            subscriber.last_error = None
            if delivery.username:
                profile_changed = subscriber.username != delivery.username
                subscriber.username = delivery.username
            else:
                profile_changed = False
            if delivery.is_premium is not None:
                profile_changed = profile_changed or subscriber.is_premium != delivery.is_premium
                subscriber.is_premium = delivery.is_premium
                subscriber.premium_checked_at = now
            if profile_changed:
                await DirectMessageService._refresh_list_counts_for_subscribers(
                    session, {subscriber_id}
                )
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
            # A retry is scheduled only after a determinate local/Telegram
            # result. The non-idempotent RPC boundary belongs to the previous
            # attempt and must not poison a later crash as an unknown delivery.
            task.delivery_attempted_at = None
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
            queued_subscriber_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(SubscriberInviteTask.subscriber_id).where(
                        SubscriberInviteTask.launch_id == launch.id,
                        SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED,
                    )
                )
            )
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
                .where(
                    Subscriber.id.in_((*queued_subscriber_ids, int(task.subscriber_id))),
                    Subscriber.reserved_invite_launch_id == launch.id,
                )
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
            except OperationalError as exc:
                if is_transient_sqlite_lock(exc):
                    logger.warning("Subscriber-invite worker waits for a busy SQLite writer")
                    await asyncio.sleep(0.5)
                    continue
                logger.exception("Subscriber-invite worker iteration failed")
                if reporter is not None:
                    await reporter(exc)
                task = None
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
