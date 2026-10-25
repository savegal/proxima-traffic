"""Account-level orchestration for low-frequency @SpamBot status checks."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.account_state import apply_flood_pause, locked_account, session_fingerprint
from proxima_traffic.failure_policy import ACCOUNT_FATAL_CODES, failure_code
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    AuditEvent,
    ensure_utc,
    utc_now,
)
from proxima_traffic.spambot import SpamBotRestrictionScope, SpamBotResult, SpamBotStatus
from proxima_traffic.worker_supervision import WorkerIterationReporter

logger = logging.getLogger(__name__)

SPAMBOT_REASONS = frozenset(
    {
        "spambot_temp_ban",
        "spambot_restriction_no_expiry",
        "spambot_restriction_unclear",
        "spambot_permanent_ban",
        "spambot_parse_unknown",
    }
)

LEGACY_UNKNOWN_REASON = "spambot_parse_unknown"


@dataclass(frozen=True, slots=True)
class SpamBotBulkCheckEntry:
    """One account in an explicitly requested @SpamBot-only check."""

    account_id: int
    label: str
    status: str | None
    parser_signal: str | None = None

    @property
    def completed(self) -> bool:
        return self.status is not None and self.status != "error"


@dataclass(frozen=True, slots=True)
class SpamBotBulkCheckSummary:
    """Outcome of a manual @SpamBot check without a session or chat probe."""

    entries: tuple[SpamBotBulkCheckEntry, ...]
    skipped_without_session: int = 0

    @property
    def checked(self) -> int:
        return len(self.entries)

    def count(self, status: SpamBotStatus | str) -> int:
        expected = status.value if isinstance(status, SpamBotStatus) else status
        return sum(entry.status == expected for entry in self.entries)

    @property
    def transport_errors(self) -> tuple[SpamBotBulkCheckEntry, ...]:
        return tuple(entry for entry in self.entries if entry.status in {None, "error"})


class SpamBotGateway(Protocol):
    async def check_spambot_status(self, *, account: Account) -> SpamBotResult: ...


class SpamBotCheckService:
    """Rate-limited status checks and durable account-state updates.

    The raw reply intentionally stays in memory.  Database and audit records
    contain only the classified status, parser signal and expiry timestamp.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: object,
        settings: object | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._locks: dict[int, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def check_account(
        self,
        account_id: int,
        *,
        trigger: str,
        force: bool = False,
    ) -> SpamBotResult | None:
        """Check one account, returning a cached result when a check is too recent."""
        if not bool(self._cfg("spambot_enabled", True)):
            return None
        checker = getattr(self._gateway, "check_spambot_status", None)
        if checker is None:
            return None
        lock = await self._lock_for(account_id)
        async with lock:
            return await self._check_account_locked(account_id, trigger=trigger, force=force)

    async def check_due_accounts(self, *, limit: int) -> int:
        """Refresh paused/active accounts that have not been checked recently."""
        if not bool(self._cfg("spambot_enabled", True)):
            return 0
        minimum = timedelta(seconds=self._minimum_interval_seconds)
        now = utc_now()
        async with self._session_factory() as session:
            account_ids = list(
                await session.scalars(
                    select(Account.id)
                    .where(
                        Account.status.in_((AccountStatus.ACTIVE, AccountStatus.PAUSED)),
                        Account.session_source != AccountSessionSource.NONE,
                        Account.session_encrypted.is_not(None),
                        or_(
                            Account.spambot_checked_at.is_(None),
                            Account.spambot_parser_signal == "peer_flood_error",
                            Account.spambot_checked_at <= now - minimum,
                            (
                                (
                                    (Account.status == AccountStatus.PAUSED)
                                    | (Account.cooldown_reason == "flood_wait")
                                )
                                & Account.cooldown_until.is_not(None)
                                & (Account.cooldown_until <= now)
                            ),
                        ),
                    )
                    .order_by(
                        Account.spambot_checked_at.is_not(None),
                        Account.spambot_checked_at,
                        Account.id,
                    )
                    .limit(max(1, limit))
                )
            )
        checked = 0
        for account_id in account_ids:
            result = await self.check_account(int(account_id), trigger="maintenance")
            if result is not None:
                checked += 1
        return checked

    async def check_all_accounts(
        self,
        *,
        trigger: str = "manual_bulk_check",
        concurrency: int = 2,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> SpamBotBulkCheckSummary:
        """Force a @SpamBot-only check for every account with a saved session.

        The operation neither checks session validity nor inspects or joins
        chats. A small bounded fan-out prevents a burst of Telegram account
        connections, while the progress callback is called only after a real
        account check has completed.
        """
        async with self._session_factory() as session:
            total_accounts = int(await session.scalar(select(func.count(Account.id))) or 0)
            accounts = list(
                await session.execute(
                    select(Account.id, Account.label)
                    .where(Account.session_source != AccountSessionSource.NONE)
                    .order_by(Account.id)
                )
            )

        total = len(accounts)
        if on_progress is not None:
            await on_progress(0, total)
        semaphore = asyncio.Semaphore(max(1, min(int(concurrency), 2)))
        progress_lock = asyncio.Lock()
        completed = 0

        async def report_progress() -> None:
            nonlocal completed
            if on_progress is None:
                return
            async with progress_lock:
                completed += 1
                await on_progress(completed, total)

        async def check_one(account_id: int, label: str) -> SpamBotBulkCheckEntry:
            async with semaphore:
                try:
                    result = await self.check_account(
                        account_id,
                        trigger=trigger,
                        force=True,
                    )
                    if result is not None:
                        return SpamBotBulkCheckEntry(
                            account_id=account_id,
                            label=label,
                            status=result.status.value,
                            parser_signal=result.parser_signal,
                        )
                    async with self._session_factory() as session:
                        account = await session.get(Account, account_id)
                        return SpamBotBulkCheckEntry(
                            account_id=account_id,
                            label=label,
                            status="error",
                            parser_signal=(
                                account.spambot_parser_signal if account is not None else None
                            ),
                        )
                except Exception as exc:  # noqa: BLE001 - one account must not abort a batch
                    logger.warning(
                        "Bulk SpamBot check failed for account %s: %s",
                        account_id,
                        type(exc).__name__,
                    )
                    return SpamBotBulkCheckEntry(
                        account_id=account_id,
                        label=label,
                        status="error",
                        parser_signal="check_failed",
                    )
                finally:
                    await report_progress()

        entries = await asyncio.gather(
            *(check_one(int(account_id), str(label)) for account_id, label in accounts)
        )
        return SpamBotBulkCheckSummary(
            entries=tuple(entries),
            skipped_without_session=max(0, total_accounts - total),
        )

    async def prepare_legacy_unknown_rechecks(self) -> int:
        """Schedule former UNKNOWN pauses for an immediate authoritative recheck.

        Older versions did not distinguish an unfamiliar reply from an active
        restriction without an expiry. Keep those accounts paused until the
        maintenance worker reads the real reply again; never guess either way.
        The update is deliberately narrow and never touches another pause.
        """
        async with self._session_factory() as session:
            accounts = list(
                await session.scalars(
                    select(Account).where(
                        Account.status == AccountStatus.PAUSED,
                        Account.cooldown_reason == LEGACY_UNKNOWN_REASON,
                    )
                )
            )
            if not accounts:
                return 0
            for account in accounts:
                account.spambot_checked_at = None
                account.cooldown_until = utc_now()
                session.add(
                    AuditEvent(
                        actor="spambot",
                        event_type="account.spambot_unknown_recheck_scheduled",
                        entity_type="account",
                        entity_id=str(account.id),
                        details_json=json.dumps(
                            {"reason": "legacy_unknown_requires_reclassification"},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    )
                )
            await session.commit()
            return len(accounts)

    async def _check_account_locked(
        self,
        account_id: int,
        *,
        trigger: str,
        force: bool,
    ) -> SpamBotResult | None:
        async with self._session_factory() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return None
            expected_fingerprint = session_fingerprint(account)
            now = utc_now()
            cooldown = ensure_utc(account.cooldown_until)
            if account.cooldown_reason == "flood_wait" and cooldown and cooldown > now:
                return None
            checked_at = ensure_utc(account.spambot_checked_at)
            if (
                not force
                and account.spambot_status == "error"
                and checked_at is not None
                and now - checked_at < timedelta(seconds=self._minimum_interval_seconds)
                and not (account.cooldown_reason == "flood_wait" and cooldown and cooldown <= now)
            ):
                return None
            cached = self._recent_result(account, now=now, force=force)
            if cached is not None:
                return cached

        checker = self._gateway.check_spambot_status  # type: ignore[attr-defined]
        try:
            result = await checker(account=account)
        except Exception as exc:  # noqa: BLE001 - a check failure must not stop failover
            logger.warning(
                "@SpamBot check failed for account %s: %s (code=%s)",
                account_id,
                type(exc).__name__,
                failure_code(exc, "spambot_check_failed"),
            )
            await self._record_gateway_error(
                account_id, trigger=trigger, error=exc, expected_fingerprint=expected_fingerprint
            )
            return None
        if not isinstance(result, SpamBotResult):
            logger.warning("@SpamBot gateway returned an invalid result for account %s", account_id)
            await self._record_gateway_error(
                account_id,
                trigger=trigger,
                error=TypeError("invalid @SpamBot gateway result"),
                expected_fingerprint=expected_fingerprint,
            )
            return None
        return await self._persist_result(
            account_id, result, trigger=trigger, expected_fingerprint=expected_fingerprint
        )

    def _recent_result(
        self,
        account: Account,
        *,
        now: datetime,
        force: bool,
    ) -> SpamBotResult | None:
        checked_at = ensure_utc(account.spambot_checked_at)
        if (
            force
            or account.spambot_parser_signal == "peer_flood_error"
            or checked_at is None
            or now - checked_at >= timedelta(seconds=self._minimum_interval_seconds)
        ):
            return None
        try:
            status = SpamBotStatus(account.spambot_status or "")
        except ValueError:
            return None
        restriction_scope = SpamBotRestrictionScope.NONE
        allows_reply_to_inbound: bool | None = None
        try:
            snapshot = json.loads(account.spambot_response_json or "{}")
        except (TypeError, ValueError):
            snapshot = {}
        if isinstance(snapshot, dict):
            try:
                restriction_scope = SpamBotRestrictionScope(
                    str(snapshot.get("restriction_scope") or "none")
                )
            except ValueError:
                restriction_scope = SpamBotRestrictionScope.UNSPECIFIED
            raw_reply_permission = snapshot.get("allows_reply_to_inbound")
            if isinstance(raw_reply_permission, bool):
                allows_reply_to_inbound = raw_reply_permission
        restricted_until = None
        if isinstance(snapshot, dict):
            try:
                raw_until = snapshot.get("restricted_until")
                restricted_until = (
                    ensure_utc(datetime.fromisoformat(str(raw_until))) if raw_until else None
                )
                raw_recheck = snapshot.get("recheck_at")
                recheck_at = (
                    ensure_utc(datetime.fromisoformat(str(raw_recheck))) if raw_recheck else None
                )
            except (TypeError, ValueError):
                recheck_at = None
            if recheck_at is not None and recheck_at <= now:
                return None
        if restricted_until is None and account.cooldown_reason in SPAMBOT_REASONS:
            restricted_until = ensure_utc(account.cooldown_until)
        if (
            status is SpamBotStatus.TEMPORARY_RESTRICTION
            and restricted_until
            and restricted_until <= now
        ):
            return None
        if (
            account.cooldown_reason in SPAMBOT_REASONS
            and account.cooldown_until
            and ensure_utc(account.cooldown_until) <= now
        ):
            return None
        return SpamBotResult(
            status=status,
            raw_text="",
            restricted_until=restricted_until,
            parser_signal="cached",
            restriction_scope=restriction_scope,
            allows_reply_to_inbound=allows_reply_to_inbound,
        )

    async def _persist_result(
        self,
        account_id: int,
        result: SpamBotResult,
        *,
        trigger: str,
        expected_fingerprint: str | None = None,
    ) -> SpamBotResult | None:
        now = utc_now()
        async with self._session_factory() as session:
            account = await locked_account(session, account_id)
            if account is None or (
                expected_fingerprint is not None
                and session_fingerprint(account) != expected_fingerprint
            ):
                return None
            if (
                result.status is SpamBotStatus.TEMPORARY_RESTRICTION
                and result.restricted_until is None
            ):
                # A custom gateway may omit the date. Preserve the confirmed
                # restriction instead of degrading it to an inconclusive state.
                result = SpamBotResult(
                    SpamBotStatus.ACTIVE_RESTRICTION,
                    result.raw_text,
                    parser_signal="temporary_without_expiry",
                    button_rows=result.button_rows,
                    format_entities=result.format_entities,
                    restriction_scope=result.restriction_scope,
                    allows_reply_to_inbound=result.allows_reply_to_inbound,
                )
            prior_cooldown = ensure_utc(account.cooldown_until)
            prior_reason = account.cooldown_reason
            prior_disabled = (
                account.status == AccountStatus.DISABLED and prior_reason not in SPAMBOT_REASONS
            )
            recheck_at = ensure_utc(result.restricted_until)
            if result.status in {
                SpamBotStatus.ACTIVE_RESTRICTION,
                SpamBotStatus.RESTRICTION_UNCLEAR,
            }:
                recheck_at = now + timedelta(
                    seconds=max(60, int(self._cfg("spambot_unknown_cooldown_seconds", 7200)))
                )
            account.spambot_checked_at = now
            account.spambot_status = result.status.value
            account.spambot_parser_signal = result.parser_signal
            account.spambot_response_json = json.dumps(
                {
                    "restricted_until": result.restricted_until.isoformat()
                    if result.restricted_until
                    else None,
                    "recheck_at": recheck_at.isoformat() if recheck_at else None,
                    "text": result.raw_text,
                    "button_rows": [list(row) for row in result.button_rows],
                    "format_entities": [list(entity) for entity in result.format_entities],
                    "restriction_scope": result.restriction_scope.value,
                    "allows_reply_to_inbound": result.allows_reply_to_inbound,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            event_type = "account.spambot_checked"
            if result.status is SpamBotStatus.FREE:
                if not account.operator_paused and (
                    (
                        account.status == AccountStatus.PAUSED
                        and account.cooldown_reason in SPAMBOT_REASONS
                    )
                    or (
                        account.status == AccountStatus.DISABLED
                        and account.cooldown_reason == "spambot_permanent_ban"
                        and account.authorized
                        and account.session_source != AccountSessionSource.NONE
                    )
                ):
                    account.status = AccountStatus.ACTIVE
                if account.cooldown_reason in SPAMBOT_REASONS:
                    account.cooldown_until = None
                    account.cooldown_reason = None
                if (
                    account.status in {AccountStatus.ACTIVE, AccountStatus.PAUSED}
                    and account.cooldown_reason
                    in {"flood_wait", "flood_escalation", "peer_flood", "account_restricted"}
                    and (prior_cooldown is None or prior_cooldown <= now)
                ):
                    account.status = (
                        AccountStatus.PAUSED if account.operator_paused else AccountStatus.ACTIVE
                    )
                    account.cooldown_until = None
                    account.cooldown_reason = None
                if account.last_error and account.last_error.startswith("@SpamBot:"):
                    account.last_error = None
                event_type = "account.spambot_clear"
            elif result.status is SpamBotStatus.TEMPORARY_RESTRICTION:
                account.status = AccountStatus.PAUSED
                account.cooldown_until = ensure_utc(result.restricted_until)
                account.cooldown_reason = "spambot_temp_ban"
                account.last_error = (
                    f"@SpamBot: temporary restriction until {account.cooldown_until.isoformat()}"
                )
                event_type = "account.spambot_temporary_ban"
            elif result.status is SpamBotStatus.ACTIVE_RESTRICTION:
                account.status = AccountStatus.PAUSED
                account.cooldown_until = now + timedelta(
                    seconds=max(60, int(self._cfg("spambot_unknown_cooldown_seconds", 7200)))
                )
                account.cooldown_reason = "spambot_restriction_no_expiry"
                account.last_error = "@SpamBot: active restriction; expiry was not provided"
                event_type = "account.spambot_active_restriction"
            elif result.status is SpamBotStatus.RESTRICTION_UNCLEAR:
                # The bot explicitly said that a restriction exists, but the
                # exact wording/scope was new.  This must not be treated as a
                # clean account merely because the locale is unfamiliar.
                account.status = AccountStatus.PAUSED
                account.cooldown_until = now + timedelta(
                    seconds=max(60, int(self._cfg("spambot_unknown_cooldown_seconds", 7200)))
                )
                account.cooldown_reason = "spambot_restriction_unclear"
                account.last_error = (
                    "@SpamBot: Telegram confirmed a restriction, but its exact scope "
                    "was not recognised"
                )
                event_type = "account.spambot_restriction_unclear"
            elif result.status is SpamBotStatus.PERMANENT_RESTRICTION:
                account.status = AccountStatus.DISABLED
                account.cooldown_until = None
                account.cooldown_reason = "spambot_permanent_ban"
                account.last_error = "@SpamBot: permanent restriction"
                event_type = "account.spambot_permanent_ban"
            elif result.status is SpamBotStatus.UNKNOWN:
                # An unfamiliar/empty response is not evidence of a Telegram
                # restriction.  Keep the diagnostic status, while allowing real
                # target inspection and send errors to decide account health.
                if (
                    account.status == AccountStatus.PAUSED
                    and account.cooldown_reason == LEGACY_UNKNOWN_REASON
                ):
                    account.status = (
                        AccountStatus.PAUSED if account.operator_paused else AccountStatus.ACTIVE
                    )
                    account.cooldown_until = None
                    account.cooldown_reason = None
                    if account.last_error and account.last_error.startswith("@SpamBot:"):
                        account.last_error = None
                event_type = "account.spambot_unknown"
            # A SpamBot observation must not shorten an unrelated FloodWait,
            # revive a revoked session, or erase an operator's pause.
            if prior_disabled:
                account.status = AccountStatus.DISABLED
            if prior_reason not in SPAMBOT_REASONS and prior_cooldown and prior_cooldown > now:
                new_cooldown = ensure_utc(account.cooldown_until)
                if new_cooldown is None or new_cooldown < prior_cooldown:
                    account.cooldown_until = prior_cooldown
                    account.cooldown_reason = prior_reason
            session.add(
                AuditEvent(
                    actor="spambot",
                    event_type=event_type,
                    entity_type="account",
                    entity_id=str(account_id),
                    details_json=json.dumps(
                        {
                            "trigger": trigger,
                            "status": result.status.value,
                            "restricted_until": (
                                result.restricted_until.isoformat()
                                if result.restricted_until is not None
                                else None
                            ),
                            "parser_signal": result.parser_signal,
                            "restriction_scope": result.restriction_scope.value,
                            "allows_reply_to_inbound": result.allows_reply_to_inbound,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
            )
            await session.commit()
        return result

    async def _record_gateway_error(
        self,
        account_id: int,
        *,
        trigger: str,
        error: BaseException,
        expected_fingerprint: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            account = await locked_account(session, account_id)
            if account is None or (
                expected_fingerprint is not None
                and session_fingerprint(account) != expected_fingerprint
            ):
                return
            account.spambot_checked_at = utc_now()
            account.spambot_status = "error"
            code = failure_code(error, "spambot_check_failed")
            account.spambot_parser_signal = f"gateway_error:{code}"[:100]
            # Do not leave a prior successful reply labelled as the latest
            # @SpamBot response after a transport failure.  Preserve it only
            # as a previous observation so the operator can still understand
            # an existing protective pause without confusing it with a fresh
            # Telegram verdict.
            previous_snapshot: dict[str, object] = {}
            try:
                decoded = json.loads(account.spambot_response_json or "{}")
            except (TypeError, ValueError):
                decoded = {}
            if isinstance(decoded, dict):
                nested_previous = decoded.get("previous")
                if isinstance(nested_previous, dict):
                    decoded = nested_previous
                if not decoded.get("check_error_code"):
                    previous_snapshot = decoded
            account.spambot_response_json = json.dumps(
                {
                    "check_error_code": code,
                    "previous": previous_snapshot,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            if code == "flood_wait":
                apply_flood_pause(
                    account,
                    float(getattr(error, "retry_after", 0) or 0),
                    minimum=float(self._cfg("flood_min_pause_seconds", 500)),
                )
            if code in ACCOUNT_FATAL_CODES:
                account.status = AccountStatus.DISABLED
                account.authorized = False
                account.cooldown_until = None
                account.cooldown_reason = code
                safe_message = str(getattr(error, "safe_message", None) or error)
                account.last_error = safe_message[:500]
            session.add(
                AuditEvent(
                    actor="spambot",
                    event_type="account.spambot_check_failed",
                    entity_type="account",
                    entity_id=str(account_id),
                    details_json=json.dumps(
                        {
                            "trigger": trigger,
                            "error_type": type(error).__name__,
                            "error_code": code,
                            "account_disabled": code in ACCOUNT_FATAL_CODES,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
            )
            await session.commit()

    async def _lock_for(self, account_id: int) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._locks.get(account_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[account_id] = lock
            return lock

    @property
    def _minimum_interval_seconds(self) -> int:
        return max(60, int(self._cfg("spambot_min_check_interval_seconds", 21600)))

    def _cfg(self, name: str, default: int | bool) -> int | bool:
        if self._settings is None:
            return default
        return getattr(self._settings, name, default)


class SpamBotMaintenanceWorker:
    """Refreshes expired checks without making the normal send path chatty."""

    def __init__(self, checks: SpamBotCheckService, settings: object | None = None) -> None:
        self._checks = checks
        self._settings = settings
        self._stop = asyncio.Event()

    async def run_forever(self, reporter: WorkerIterationReporter | None = None) -> None:
        while not self._stop.is_set():
            try:
                await self._checks.check_due_accounts(limit=self._batch_size)
            except Exception as exc:  # noqa: BLE001 - maintenance must not take down the app lifespan
                logger.exception("@SpamBot maintenance pass failed")
                if reporter is not None:
                    await reporter(exc)
            else:
                if reporter is not None:
                    await reporter()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                continue

    def stop(self) -> None:
        self._stop.set()

    @property
    def _interval_seconds(self) -> int:
        return max(3600, int(self._cfg("spambot_maintenance_interval_seconds", 86400)))

    @property
    def _batch_size(self) -> int:
        return max(1, int(self._cfg("spambot_maintenance_batch_size", 20)))

    def _cfg(self, name: str, default: int) -> int:
        if self._settings is None:
            return default
        return int(getattr(self._settings, name, default))
