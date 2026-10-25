"""Small shared guards for account observations and protective pauses."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from proxima_traffic.models import Account, ensure_utc, utc_now

# These are state changes made by Telegram safety checks or the sender, not a
# voluntary operator pause.  A generic "Enable" button must not bypass them.
ACCOUNT_PROTECTED_PAUSE_REASONS = frozenset(
    {
        "account_dead",
        "account_unauthorized",
        "account_identity_mismatch",
        "account_restricted",
        "flood_escalation",
        "flood_wait",
        "peer_flood",
        "spambot_temp_ban",
        "spambot_restriction_no_expiry",
        "spambot_restriction_unclear",
        "spambot_permanent_ban",
    }
)


def session_fingerprint(account: object) -> str:
    values = [
        getattr(account, name, None)
        for name in (
            "session_source",
            "session_encrypted",
            "api_id",
            "api_hash_encrypted",
        )
    ]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


async def locked_account(session: AsyncSession, account_id: int) -> Account | None:
    # Acquire SQLite's writer lock before reading, so an import cannot race
    # the credential comparison and the subsequent metadata update.
    await session.execute(
        update(Account)
        .where(Account.id == account_id)
        .values(
            connection_generation=Account.connection_generation,
        )
    )
    return await session.get(Account, account_id, populate_existing=True)


def apply_flood_pause(account: Account, seconds: float, *, minimum: float = 500) -> None:
    until = utc_now() + timedelta(seconds=max(500, minimum, seconds))
    previous = ensure_utc(account.cooldown_until)
    if previous is None or previous < until:
        account.cooldown_until = until
        account.cooldown_reason = "flood_wait"
    # Workers accept ACTIVE senders and defer them using cooldown_until.
    # Changing that status would turn a temporary wait into a failed launch.
    # Preserve manual pauses and disabled accounts exactly as they are.
    account.last_error = "Telegram FloodWait: действие отложено до окончания защитной паузы"
