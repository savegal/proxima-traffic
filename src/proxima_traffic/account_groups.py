"""System-maintained account group helpers."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from proxima_traffic.models import Account, AccountGroup, AccountGroupMember
from proxima_traffic.services import add_audit_event

PREMIUM_ACCOUNT_GROUP_KEY = "telegram_premium"
PREMIUM_ACCOUNT_GROUP_NAME = "⭐ Premium"
PREMIUM_ACCOUNT_GROUP_DESCRIPTION = (
    "Системный раздел: состав обновляется автоматически по данным Telegram."
)


async def sync_premium_account_group(
    session: AsyncSession,
    *,
    actor: str = "system",
) -> AccountGroup:
    """Make the built-in Premium membership match verified Telegram data.

    The caller owns the transaction. A profile check and its group update are
    therefore visible atomically, without any stale manual membership.
    """

    # Every supported production database is SQLite.  Its native upsert keeps
    # API startup, bot startup, account imports, and profile checks from
    # racing to create this one system row.
    insert_result = await session.execute(
        sqlite_insert(AccountGroup)
        .values(
            name=PREMIUM_ACCOUNT_GROUP_NAME,
            description=PREMIUM_ACCOUNT_GROUP_DESCRIPTION,
            system_key=PREMIUM_ACCOUNT_GROUP_KEY,
        )
        .on_conflict_do_nothing()
    )
    created = bool(insert_result.rowcount)
    group = await session.scalar(
        select(AccountGroup).where(AccountGroup.system_key == PREMIUM_ACCOUNT_GROUP_KEY)
    )
    if group is None:
        # Adopt a same-named legacy group instead of failing startup on a
        # uniqueness conflict after upgrading an existing operator database.
        group = await session.scalar(
            select(AccountGroup).where(AccountGroup.name == PREMIUM_ACCOUNT_GROUP_NAME)
        )
        if group is None:  # pragma: no cover - defensive against a deleted row race
            raise RuntimeError("premium account group was not created")
        group.system_key = PREMIUM_ACCOUNT_GROUP_KEY
    group.name = PREMIUM_ACCOUNT_GROUP_NAME
    group.description = PREMIUM_ACCOUNT_GROUP_DESCRIPTION

    premium_ids = set(
        int(account_id)
        for account_id in await session.scalars(
            select(Account.id).where(Account.is_premium.is_(True))
        )
    )
    member_ids = set(
        int(account_id)
        for account_id in await session.scalars(
            select(AccountGroupMember.account_id).where(AccountGroupMember.group_id == group.id)
        )
    )
    stale_ids = member_ids - premium_ids
    missing_ids = premium_ids - member_ids
    if stale_ids:
        await session.execute(
            delete(AccountGroupMember).where(
                AccountGroupMember.group_id == group.id,
                AccountGroupMember.account_id.in_(sorted(stale_ids)),
            )
        )
    added_count = 0
    for account_id in sorted(missing_ids):
        result = await session.execute(
            sqlite_insert(AccountGroupMember)
            .values(group_id=group.id, account_id=account_id)
            .on_conflict_do_nothing()
        )
        added_count += int(result.rowcount or 0)
    if created or stale_ids or added_count:
        add_audit_event(
            session,
            event_type="account_group.premium_synchronised",
            entity_type="account_group",
            entity_id=group.id,
            details={
                "created": created,
                "account_count": len(premium_ids),
                "added": added_count,
                "removed": len(stale_ids),
            },
            actor=actor,
        )
    return group
