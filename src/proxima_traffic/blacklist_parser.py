"""Read-only chat scans that can be explicitly confirmed into the global stop-list."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.account_state import apply_flood_pause, locked_account, session_fingerprint
from proxima_traffic.config import Settings
from proxima_traffic.failure_policy import (
    ACCOUNT_FATAL_CODES,
    ACCOUNT_WIDE_CODES,
    failure_code,
    failure_label,
)
from proxima_traffic.gateways import GatewayError, MessageGateway
from proxima_traffic.models import Account, AccountStatus, ensure_utc, utc_now
from proxima_traffic.services import ConflictError, add_audit_event, safe_error_message

BLACKLIST_MEMBER_SCAN_LIMIT = 1_000_000
BLACKLIST_MESSAGE_SCAN_LIMIT = 100_000
BLACKLIST_PARSE_TTL = timedelta(minutes=30)


@dataclass(frozen=True, slots=True)
class ParsedChatBlacklist:
    token: str
    chat_ref: str
    account_label: str
    mode: str
    telegram_user_ids: tuple[int, ...]
    fallback_used: bool
    reached_limit: bool
    expires_at: datetime
    joined_chat: bool
    scan_limit: int


class ChatBlacklistParser:
    """Keeps an unconfirmed, short-lived scan in memory until operator approval."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: MessageGateway,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._settings = settings
        self._pending: dict[str, ParsedChatBlacklist] = {}
        self._lock = asyncio.Lock()

    async def parse(self, *, chat_ref: str, mode: str) -> ParsedChatBlacklist:
        normalized_ref = self._normalize_chat_ref(chat_ref)
        if mode not in {"auto", "members", "messages"}:
            raise ConflictError("выберите способ чтения чата")
        async with self._session_factory() as session:
            accounts = list(
                await session.scalars(
                    select(Account)
                    .where(
                        Account.status == AccountStatus.ACTIVE,
                        Account.operator_paused.is_(False),
                        Account.ownership_confirmed.is_(True),
                        or_(Account.cooldown_until.is_(None), Account.cooldown_until <= utc_now()),
                    )
                    .order_by(Account.id)
                )
            )
        if not self._settings.dry_run:
            accounts = [
                account for account in accounts if account.authorized and account.session_encrypted
            ]
        if not accounts:
            raise ConflictError("нет готового подключённого аккаунта для чтения этого чата")

        errors: list[str] = []
        planned_modes = ("members", "messages") if mode == "auto" else (mode,)
        for account in accounts:
            joined_chat = False
            for index, effective_mode in enumerate(planned_modes):
                async with self._session_factory() as session:
                    fresh = await session.get(Account, account.id)
                if fresh is None or not self._ready_to_scan(fresh):
                    break
                account = fresh
                expected_fingerprint = session_fingerprint(account)
                try:
                    scan = await self._gateway.scan_chat_blacklist_candidates(
                        account=account,
                        chat_ref=normalized_ref,
                        mode=effective_mode,
                        limit=(
                            BLACKLIST_MEMBER_SCAN_LIMIT
                            if effective_mode == "members"
                            else BLACKLIST_MESSAGE_SCAN_LIMIT
                        ),
                    )
                except GatewayError as exc:
                    code = failure_code(exc, "gateway_error")
                    errors.append(
                        f"{account.label}: {failure_label(code)} ({safe_error_message(exc)})"
                    )
                    if code in ACCOUNT_WIDE_CODES:
                        await self._record_account_failure(account.id, expected_fingerprint, exc)
                        break
                    if (
                        mode == "auto"
                        and effective_mode == "members"
                        and code == "account_missing_admin_rights"
                    ):
                        continue
                    break
                joined_chat = joined_chat or scan.joined_chat
                if scan.telegram_user_ids or effective_mode == "messages" or mode != "auto":
                    return await self._remember(
                        chat_ref=normalized_ref,
                        account_label=account.label,
                        mode=effective_mode,
                        telegram_user_ids=scan.telegram_user_ids,
                        fallback_used=mode == "auto" and index > 0,
                        reached_limit=scan.reached_limit,
                        joined_chat=joined_chat,
                        scan_limit=scan.scan_limit,
                    )
        detail = "; ".join(errors[:3])
        suffix = f" ({detail})" if detail else ""
        raise ConflictError("ни один подключённый аккаунт не смог прочитать этот чат" + suffix)

    def _ready_to_scan(self, account: Account) -> bool:
        cooldown = ensure_utc(account.cooldown_until)
        return bool(
            account.status == AccountStatus.ACTIVE
            and not account.operator_paused
            and account.ownership_confirmed
            and (cooldown is None or cooldown <= utc_now())
            and (self._settings.dry_run or (account.authorized and account.session_encrypted))
        )

    async def _record_account_failure(
        self,
        account_id: int,
        expected_fingerprint: str,
        error: GatewayError,
    ) -> None:
        code = failure_code(error, "gateway_error")
        async with self._session_factory() as session:
            account = await locked_account(session, account_id)
            if account is None or session_fingerprint(account) != expected_fingerprint:
                return
            if code == "flood_wait":
                apply_flood_pause(
                    account, error.retry_after or 0, minimum=self._settings.flood_min_pause_seconds
                )
            elif code in ACCOUNT_FATAL_CODES:
                account.status = AccountStatus.DISABLED
                account.authorized = False
                account.cooldown_reason = code
            elif code in {"account_restricted", "peer_flood"}:
                if account.status != AccountStatus.DISABLED:
                    account.status = AccountStatus.PAUSED
                account.cooldown_reason = code
            account.last_error = safe_error_message(error)[:500]
            add_audit_event(
                session,
                event_type="account.blacklist_scan_failed",
                entity_type="account",
                entity_id=account_id,
                details={"failure_code": code},
                actor="blacklist-parser",
            )
            await session.commit()

    async def take(self, token: str) -> ParsedChatBlacklist | None:
        async with self._lock:
            self._discard_expired()
            return self._pending.pop(token, None)

    async def _remember(
        self,
        *,
        chat_ref: str,
        account_label: str,
        mode: str,
        telegram_user_ids: tuple[int, ...],
        fallback_used: bool,
        reached_limit: bool,
        joined_chat: bool,
        scan_limit: int,
    ) -> ParsedChatBlacklist:
        now = utc_now()
        result = ParsedChatBlacklist(
            token=uuid4().hex[:16],
            chat_ref=chat_ref,
            account_label=account_label,
            mode=mode,
            telegram_user_ids=tuple(dict.fromkeys(telegram_user_ids)),
            fallback_used=fallback_used,
            reached_limit=reached_limit,
            expires_at=now + BLACKLIST_PARSE_TTL,
            joined_chat=joined_chat,
            scan_limit=scan_limit,
        )
        async with self._lock:
            self._discard_expired(now)
            self._pending[result.token] = result
        return result

    def _discard_expired(self, now: datetime | None = None) -> None:
        current = ensure_utc(now) or utc_now()
        self._pending = {
            token: result
            for token, result in self._pending.items()
            if ensure_utc(result.expires_at) is not None and ensure_utc(result.expires_at) > current
        }

    @staticmethod
    def _normalize_chat_ref(value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 255 or any(ord(char) < 32 for char in normalized):
            raise ConflictError("отправьте @username, публичную ссылку t.me или числовой ID чата")
        lowered = normalized.casefold()
        for prefix in ("https://t.me/", "http://t.me/", "https://telegram.me/"):
            if lowered.startswith(prefix):
                slug = normalized[len(prefix) :].split("/", 1)[0]
                if slug.startswith("+") or slug.casefold() == "joinchat":
                    return normalized
                normalized = "@" + slug.lstrip("@")
                break
        if normalized.startswith("@"):
            if len(normalized) < 6 or not normalized[1:].replace("_", "a").isalnum():
                raise ConflictError("@username чата имеет неверный формат")
            return normalized
        if normalized.lstrip("-").isdigit():
            return normalized
        raise ConflictError("отправьте @username, публичную ссылку t.me или числовой ID чата")
