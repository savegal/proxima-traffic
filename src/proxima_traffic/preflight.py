from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.content import ContentError, parse_content_variants
from proxima_traffic.dialogues import parse_dialogue_steps
from proxima_traffic.failure_policy import ACCOUNT_TARGET_BLOCK_CODES, ACCOUNT_WIDE_CODES
from proxima_traffic.gateways import GatewayError, MessageGateway, TargetReadiness
from proxima_traffic.models import (
    Account,
    AccountStatus,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    MembershipStatus,
    Target,
    TargetAccountState,
    TargetListItem,
    utc_now,
)
from proxima_traffic.services import (
    TARGET_QUALITY_BAD,
    CampaignRunner,
    ConflictError,
    NotFoundError,
    add_audit_event,
    safe_error_message,
)
from proxima_traffic.spambot import SpamBotStatus

if TYPE_CHECKING:
    from proxima_traffic.spambot_checks import SpamBotCheckService


@dataclass(frozen=True, slots=True)
class PreflightPair:
    account_id: int
    target_id: int
    readiness: TargetReadiness


@dataclass(frozen=True, slots=True)
class CampaignPreflightSummary:
    campaign_id: int
    account_count: int
    target_count: int
    checked_pairs: int
    ready_pairs: int
    member_targets: int
    joinable_targets: int
    blocked_targets: int
    unavailable_targets: int
    oldest_checked_at: datetime | None
    newest_checked_at: datetime | None
    auto_join_enabled: bool = False

    @property
    def routable_targets(self) -> int:
        return self.member_targets + (self.joinable_targets if self.auto_join_enabled else 0)

    @property
    def uncovered_targets(self) -> int:
        return max(0, self.target_count - self.routable_targets)


class PreflightService:
    """Build and persist the live account × target readiness map."""

    _ACCOUNT_WIDE_ERRORS = ACCOUNT_WIDE_CODES | frozenset({"proxy_unavailable"})

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        spambot_checks: SpamBotCheckService | None = None,
        runner: CampaignRunner | None = None,
        *,
        allow_auto_join: bool = False,
        require_live_session: bool = False,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._spambot_checks = spambot_checks
        self._runner = runner
        self._allow_auto_join = allow_auto_join
        self._require_live_session = require_live_session

    async def inspect_campaign(self, campaign_id: int) -> CampaignPreflightSummary:
        campaign, accounts, targets = await self._load_campaign_scope(campaign_id)
        if not accounts:
            raise ConflictError("у рассылки нет аккаунтов-отправителей")
        if not targets:
            raise ConflictError("campaign has no enabled targets")
        default_media_kinds = self._campaign_media_kinds(campaign)
        media_kinds_by_account = (
            self._dialogue_media_kinds_by_account(campaign)
            if campaign.mode.value == "dialogue"
            else {}
        )

        async def inspect_account(account: Account) -> list[PreflightPair]:
            pairs: list[PreflightPair] = []
            account_media_kinds = media_kinds_by_account.get(
                int(account.id),
                default_media_kinds,
            )
            account_wide: TargetReadiness | None = None
            account_failure_reported = False
            if self._spambot_checks is not None:
                result = await self._spambot_checks.check_account(
                    int(account.id),
                    trigger="preflight",
                )
                if result is not None and result.is_restricted:
                    account_wide = TargetReadiness(
                        membership_status=MembershipStatus.UNKNOWN,
                        can_send=False,
                        checked_at=utc_now(),
                        media_kinds=account_media_kinds,
                        error_code="account_restricted",
                        error=(
                            "@SpamBot reports a temporary restriction"
                            if result.status is SpamBotStatus.TEMPORARY_RESTRICTION
                            else "@SpamBot reports an active restriction without an expiry"
                            if result.status is SpamBotStatus.ACTIVE_RESTRICTION
                            else "@SpamBot reports a permanent restriction"
                            if result.status is SpamBotStatus.PERMANENT_RESTRICTION
                            else "@SpamBot response could not be classified"
                        ),
                    )
            for target in targets:
                if account_wide is not None:
                    readiness = TargetReadiness(
                        membership_status=MembershipStatus.UNKNOWN,
                        can_send=False,
                        checked_at=utc_now(),
                        media_kinds=account_media_kinds,
                        error_code=account_wide.error_code,
                        error=account_wide.error,
                    )
                else:
                    try:
                        readiness = await self._gateway.inspect_target(
                            account=account,
                            target=target,
                            media_kinds=account_media_kinds,
                        )
                    except Exception as exc:  # noqa: BLE001 - persist a safe preflight result
                        readiness = TargetReadiness(
                            membership_status=MembershipStatus.UNKNOWN,
                            can_send=False,
                            checked_at=utc_now(),
                            media_kinds=account_media_kinds,
                            error_code=str(getattr(exc, "code", None) or "preflight_error"),
                            error=safe_error_message(exc),
                        )
                    if readiness.error_code in self._ACCOUNT_WIDE_ERRORS:
                        account_wide = readiness
                        if self._runner is not None and not account_failure_reported:
                            await self._runner.handle_external_account_failure(
                                int(account.id),
                                GatewayError(
                                    readiness.error or readiness.error_code,
                                    code=readiness.error_code,
                                ),
                            )
                            account_failure_reported = True
                pairs.append(
                    PreflightPair(
                        account_id=int(account.id),
                        target_id=int(target.id),
                        readiness=readiness,
                    )
                )
            return pairs

        # Each account owns one Telegram connection and is checked sequentially;
        # different accounts can be inspected concurrently.
        grouped = await asyncio.gather(*(inspect_account(account) for account in accounts))
        pairs = [pair for account_pairs in grouped for pair in account_pairs]
        await self._persist(campaign, pairs)
        return self._summarize(
            campaign_id,
            accounts,
            targets,
            pairs,
            require_all_accounts=(
                campaign.mode.value == "dialogue"
                or campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
            ),
            auto_join_enabled=self._allow_auto_join,
        )

    @staticmethod
    def _campaign_media_kinds(campaign: Campaign) -> frozenset[str]:
        if campaign.mode.value == "dialogue":
            try:
                steps = parse_dialogue_steps(
                    campaign.dialogue_steps_json,
                    primary_account_id=campaign.primary_account_id,
                    primary_text=campaign.primary_text,
                    reply_account_id=campaign.reply_account_id,
                    reply_text=campaign.reply_text,
                    reply_delay_seconds=campaign.reply_delay_seconds,
                )
            except ValueError as exc:
                raise ConflictError(str(exc)) from exc
            return frozenset(step.media_kind for step in steps if step.media_kind is not None)
        try:
            variants = parse_content_variants(
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
        kinds = {media.kind for variant in variants if variant.enabled for media in variant.media}
        if any(variant.enabled and variant.contact is not None for variant in variants):
            kinds.add("contact")
        return frozenset(kinds)

    @staticmethod
    def _dialogue_media_kinds_by_account(campaign: Campaign) -> dict[int, frozenset[str]]:
        """Preflight each fixed dialogue role only for media it will actually send."""

        try:
            steps = parse_dialogue_steps(
                campaign.dialogue_steps_json,
                primary_account_id=campaign.primary_account_id,
                primary_text=campaign.primary_text,
                reply_account_id=campaign.reply_account_id,
                reply_text=campaign.reply_text,
                reply_delay_seconds=campaign.reply_delay_seconds,
            )
        except ValueError as exc:
            raise ConflictError(str(exc)) from exc
        media_by_account: dict[int, set[str]] = {int(step.account_id): set() for step in steps}
        for step in steps:
            if step.media_kind is not None:
                media_by_account[int(step.account_id)].add(step.media_kind)
        return {account_id: frozenset(kinds) for account_id, kinds in media_by_account.items()}

    async def cached_campaign_summary(self, campaign_id: int) -> CampaignPreflightSummary:
        campaign, accounts, targets = await self._load_campaign_scope(campaign_id)
        account_ids = [int(account.id) for account in accounts]
        target_ids = [int(target.id) for target in targets]
        async with self._session_factory() as session:
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.account_id.in_(account_ids),
                        TargetAccountState.target_id.in_(target_ids),
                        TargetAccountState.membership_checked_at.is_not(None),
                    )
                )
            )
        pairs = [
            PreflightPair(
                account_id=state.account_id,
                target_id=state.target_id,
                readiness=TargetReadiness(
                    membership_status=state.membership_status,
                    can_send=state.can_send is True,
                    checked_at=state.membership_checked_at or utc_now(),
                    slowmode_seconds=state.slowmode_seconds,
                    next_send_at=state.next_send_at,
                    media_kinds=frozenset(
                        item for item in (state.preflight_media_kinds or "").split(",") if item
                    ),
                    error_code=state.membership_error_code,
                    error=state.last_error,
                ),
            )
            for state in states
        ]
        return self._summarize(
            campaign_id,
            accounts,
            targets,
            pairs,
            require_all_accounts=(
                campaign.mode.value == "dialogue"
                or campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
            ),
            auto_join_enabled=self._allow_auto_join,
        )

    async def _load_campaign_scope(
        self,
        campaign_id: int,
    ) -> tuple[Campaign, list[Account], list[Target]]:
        async with self._session_factory() as session:
            campaign = await session.get(Campaign, campaign_id)
            if campaign is None:
                raise NotFoundError("campaign not found")
            sender_ids = list(
                await session.scalars(
                    select(CampaignAccount.account_id)
                    .where(CampaignAccount.campaign_id == campaign_id)
                    .order_by(CampaignAccount.id)
                )
            )
            if not sender_ids:
                sender_ids = [campaign.primary_account_id]
            if campaign.mode.value == "dialogue":
                try:
                    steps = parse_dialogue_steps(
                        campaign.dialogue_steps_json,
                        primary_account_id=campaign.primary_account_id,
                        primary_text=campaign.primary_text,
                        reply_account_id=campaign.reply_account_id,
                        reply_text=campaign.reply_text,
                        reply_delay_seconds=campaign.reply_delay_seconds,
                    )
                except ValueError as exc:
                    raise ConflictError(str(exc)) from exc
                sender_ids = list(dict.fromkeys(step.account_id for step in steps))
            accounts_by_id = {
                account.id: account
                for account in await session.scalars(
                    select(Account).where(Account.id.in_(sender_ids))
                )
            }
            accounts = [
                accounts_by_id[account_id]
                for account_id in sender_ids
                if account_id in accounts_by_id
            ]
            if campaign.mode.value == "broadcast":
                # Every broadcast mode launches only usable selected senders.
                # In independent mode this means one task per *available*
                # account; a dead/restricted account does not block the rest.
                accounts = [
                    account
                    for account in accounts
                    if account.status == AccountStatus.ACTIVE
                    and account.ownership_confirmed
                    and (
                        not self._require_live_session
                        or (
                            account.authorized
                            and bool(account.session_encrypted)
                            and account.api_id is not None
                            and bool(account.api_hash_encrypted)
                        )
                    )
                ]
            if campaign.target_id is not None:
                target = await session.get(Target, campaign.target_id)
                targets = (
                    [target]
                    if target is not None and target.quality_status != TARGET_QUALITY_BAD
                    else []
                )
            elif campaign.target_list_id is not None:
                targets = list(
                    await session.scalars(
                        select(Target)
                        .join(TargetListItem, TargetListItem.target_id == Target.id)
                        .where(
                            TargetListItem.target_list_id == campaign.target_list_id,
                            TargetListItem.enabled.is_(True),
                            Target.quality_status != TARGET_QUALITY_BAD,
                        )
                        .order_by(Target.id)
                    )
                )
            else:
                targets = []
            return campaign, accounts, targets

    async def _persist(self, campaign: Campaign, pairs: list[PreflightPair]) -> None:
        async with self._session_factory() as session:
            account_ids = {pair.account_id for pair in pairs}
            target_ids = {pair.target_id for pair in pairs}
            # More than one worker may preflight the same account × target
            # pair.  Materialise missing rows with a native upsert, then read
            # the canonical rows back before applying the newest result.
            for pair in pairs:
                await session.execute(
                    sqlite_insert(TargetAccountState)
                    .values(account_id=pair.account_id, target_id=pair.target_id)
                    .on_conflict_do_nothing()
                )
            existing_states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.account_id.in_(account_ids),
                        TargetAccountState.target_id.in_(target_ids),
                    )
                )
            )
            states_by_pair = {
                (int(state.account_id), int(state.target_id)): state for state in existing_states
            }
            for pair in pairs:
                state = states_by_pair.get((pair.account_id, pair.target_id))
                if state is None:  # pragma: no cover - defensive against a deleted pair race
                    raise RuntimeError("preflight state disappeared during reconciliation")
                readiness = pair.readiness
                state.membership_status = readiness.membership_status
                state.can_send = readiness.can_send
                state.membership_checked_at = readiness.checked_at
                state.preflight_media_kinds = ",".join(sorted(readiness.media_kinds))
                state.membership_error_code = readiness.error_code
                state.last_checked_at = readiness.checked_at
                state.last_error = readiness.error
                if readiness.slowmode_seconds is not None:
                    state.slowmode_seconds = readiness.slowmode_seconds
                if readiness.next_send_at is not None:
                    state.next_send_at = readiness.next_send_at
                joinable_via_auto_join = bool(
                    self._allow_auto_join
                    and readiness.membership_status == MembershipStatus.NOT_MEMBER
                    and readiness.error_code == "target_not_joined"
                )
                if joinable_via_auto_join:
                    # Preflight is intentionally read-only.  A normal LIVE send
                    # performs the configured join, so this state is routable and
                    # must not become a durable account/chat block.
                    state.send_blocked = False
                    state.blocked_reason = None
                    state.blocked_at = None
                elif (
                    readiness.membership_status == MembershipStatus.BANNED
                    or readiness.error_code in ACCOUNT_TARGET_BLOCK_CODES
                ):
                    state.send_blocked = True
                    state.blocked_reason = readiness.error_code or "account_banned_in_chat"
                    state.blocked_at = state.blocked_at or readiness.checked_at
                elif readiness.membership_status == MembershipStatus.MEMBER and readiness.can_send:
                    state.send_blocked = False
                    state.blocked_reason = None
                    state.blocked_at = None
            await CampaignRunner.refresh_target_quality_many(session, target_ids)
            add_audit_event(
                session,
                event_type="campaign.preflight_completed",
                entity_type="campaign",
                entity_id=campaign.id,
                details={
                    "account_count": len({pair.account_id for pair in pairs}),
                    "target_count": len({pair.target_id for pair in pairs}),
                    "checked_pairs": len(pairs),
                    "ready_pairs": sum(pair.readiness.can_send for pair in pairs),
                },
                actor="control-bot",
            )
            await session.commit()

    @staticmethod
    def _summarize(
        campaign_id: int,
        accounts: list[Account],
        targets: list[Target],
        pairs: list[PreflightPair],
        *,
        require_all_accounts: bool = False,
        auto_join_enabled: bool = False,
    ) -> CampaignPreflightSummary:
        by_target: dict[int, list[TargetReadiness]] = {}
        for pair in pairs:
            by_target.setdefault(pair.target_id, []).append(pair.readiness)
        member_targets = 0
        joinable_targets = 0
        blocked_targets = 0
        unavailable_targets = 0
        for target in targets:
            states = by_target.get(int(target.id), [])
            ready_states = [
                state
                for state in states
                if state.membership_status == MembershipStatus.MEMBER and state.can_send
            ]
            target_ready = bool(accounts) and (
                len(ready_states) == len(accounts) if require_all_accounts else bool(ready_states)
            )
            if target_ready:
                member_targets += 1
            elif any(state.membership_status == MembershipStatus.NOT_MEMBER for state in states):
                joinable_targets += 1
            elif states and all(
                state.membership_status == MembershipStatus.UNAVAILABLE for state in states
            ):
                unavailable_targets += 1
            elif states and all(
                state.membership_status in {MembershipStatus.BANNED, MembershipStatus.JOIN_PENDING}
                for state in states
            ):
                blocked_targets += 1
        return CampaignPreflightSummary(
            campaign_id=campaign_id,
            account_count=len(accounts),
            target_count=len(targets),
            checked_pairs=len(pairs),
            ready_pairs=sum(pair.readiness.can_send for pair in pairs),
            member_targets=member_targets,
            joinable_targets=joinable_targets,
            blocked_targets=blocked_targets,
            unavailable_targets=unavailable_targets,
            oldest_checked_at=(min(pair.readiness.checked_at for pair in pairs) if pairs else None),
            newest_checked_at=(max(pair.readiness.checked_at for pair in pairs) if pairs else None),
            auto_join_enabled=auto_join_enabled,
        )
