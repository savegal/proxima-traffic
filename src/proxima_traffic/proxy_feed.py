"""Conservative, opt-in MTProto proxy discovery from one public Telegram channel.

Only an already authorised and *proxied* user session may read the feed.
Never open a direct connection as an emergency fallback.  Untrusted links are
validated, deduplicated, rate-limited and probed before entering the pool.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import quote

from sqlalchemy import delete, exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyGroup,
    ProxyProtocol,
    ProxyStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import (
    PUBLIC_MTPROTO_GROUP,
    ParsedProxy,
    ProxyInputError,
    mtproto_secret_kind,
    parse_proxy,
    proxy_import_values,
)
from proxima_traffic.services import ConflictError, add_audit_event

if TYPE_CHECKING:
    from proxima_traffic.accounts import AccountService
    from proxima_traffic.config import Settings
    from proxima_traffic.proxies import ProxyService

logger = logging.getLogger(__name__)
CHANNEL = "ProxyMTProto"
GROUP = PUBLIC_MTPROTO_GROUP


@dataclass(frozen=True, slots=True)
class ProxyFeedResult:
    account_id: int
    messages_read: int
    valid_links: int
    tested: int
    responsive: int
    added: int
    reused: int
    healthy_after_save: int
    unsupported_fake_tls: int
    pruned: int = 0


class MTProtoFeedService:
    """Read source messages with one existing account; do not touch others."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        account_service: AccountService,
        proxy_service: ProxyService,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._accounts = account_service
        self._proxies = proxy_service
        self._settings = settings
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        """Optional periodic refresh: not a loop reacting to every failed task."""
        while not self._stop.is_set():
            try:
                result = await self.sync(actor="mtproto-feed-worker")
                logger.info(
                    "MTProto feed checked %d candidate(s), saved %d new, %d usable",
                    result.tested, result.added, result.healthy_after_save,
                )
            except Exception as exc:  # noqa: BLE001 - never log channel secrets
                logger.warning(
                    "MTProto feed unavailable (%s); keeping existing routes",
                    type(exc).__name__,
                )
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._settings.mtproto_feed_interval_seconds
                )
            except TimeoutError:
                continue

    async def sync(self, *, actor: str = "control-bot") -> ProxyFeedResult:
        if self._settings.dry_run:
            raise ConflictError(
                "Синхронизация канала меняет реальный аккаунт: отключите DRY_RUN "
                "только когда готовы к настоящему вступлению"
            )
        if not self._settings.account_require_proxy:
            raise ConflictError(
                "Импорт из Telegram требует PROXIMA_ACCOUNT_REQUIRE_PROXY=true; "
                "прямое соединение запрещено"
            )
        if self._lock.locked():
            raise ConflictError("проверка MTProto-канала уже выполняется")
        async with self._lock:
            account_id, messages = await self._read_recent_messages()
            links = self._extract_links(messages)
            supported: list[tuple[str, ParsedProxy]] = []
            unsupported = 0
            for url, parsed in links:
                if _unsafe_feed_host(parsed.host):
                    continue
                if mtproto_secret_kind(parsed.password or "") == "ee":
                    unsupported += 1
                    continue
                supported.append((url, parsed))
            semaphore = asyncio.Semaphore(4)
            # Treat the public channel as untrusted input. Resolve hostnames,
            # reject any private/non-global answers and pin a public IP before
            # the transport probe to avoid DNS rebinding between checks.
            async def resolve_candidate(
                item: tuple[str, ParsedProxy],
            ) -> tuple[str, ParsedProxy] | None:
                async with semaphore:
                    return await self._resolve_public_candidate(item)

            resolved = await asyncio.gather(
                *(
                    resolve_candidate(item)
                    for item in supported[: self._settings.mtproto_feed_max_candidates * 3]
                )
            )
            candidates: list[tuple[str, ParsedProxy]] = []
            seen_resolved: set[str] = set()
            for item in resolved:
                if item is None or item[1].fingerprint in seen_resolved:
                    continue
                seen_resolved.add(item[1].fingerprint)
                candidates.append(item)
                if len(candidates) >= self._settings.mtproto_feed_max_candidates:
                    break

            async def check(
                item: tuple[str, ParsedProxy],
            ) -> tuple[str, ParsedProxy] | None:
                url, parsed = item
                async with semaphore:
                    result = await self._proxies._probe_mtproto_proxy(
                        parsed.host, parsed.port, parsed.password
                    )
                    return (url, parsed) if result.healthy else None

            responses = await asyncio.gather(*(check(item) for item in candidates))
            responsive = [item for item in responses if item is not None]
            if not responsive:
                # An all-dead feed still needs its retention policy enforced.
                # Otherwise stale public entries accumulate indefinitely.
                pruned = await self._prune_unassigned_feed_proxies(actor=actor)
                return ProxyFeedResult(
                    account_id, len(messages), len(links), len(candidates),
                    0, 0, 0, 0, unsupported, pruned=pruned,
                )

            imported = await self._proxies.import_proxies(
                [url for url, _ in responsive], group_name=GROUP, actor=actor
            )
            fingerprints = [parsed.fingerprint for _, parsed in responsive]
            async with self._session_factory() as session:
                ids = list(
                    await session.scalars(
                        select(ProxyEndpoint.id).where(
                            ProxyEndpoint.fingerprint.in_(fingerprints),
                            ProxyEndpoint.enabled.is_(True),
                        )
                    )
                )

            async def record(proxy_id: int) -> bool:
                async with semaphore:
                    try:
                        result = await self._proxies.check_proxy(proxy_id, actor=actor)
                        return bool(result.healthy)
                    except (ConflictError, ProxyInputError):
                        return False

            checked = await asyncio.gather(*(record(int(pid)) for pid in ids))
            pruned = await self._prune_unassigned_feed_proxies(actor=actor)
            return ProxyFeedResult(
                account_id=account_id,
                messages_read=len(messages),
                valid_links=len(links),
                tested=len(candidates),
                responsive=len(responsive),
                added=imported.added,
                reused=imported.skipped,
                healthy_after_save=sum(checked),
                unsupported_fake_tls=unsupported,
                pruned=pruned,
            )

    async def _resolve_public_candidate(
        self, item: tuple[str, ParsedProxy],
    ) -> tuple[str, ParsedProxy] | None:
        """Pin a global IP so a hostile DNS name cannot rebind to localhost."""
        _, parsed = item
        if _unsafe_feed_host(parsed.host):
            return None
        try:
            address = ipaddress.ip_address(parsed.host)
            addresses = [address]
        except ValueError:
            try:
                answers = await asyncio.wait_for(
                    asyncio.get_running_loop().getaddrinfo(
                        parsed.host, parsed.port, type=socket.SOCK_STREAM,
                    ),
                    timeout=3,
                )
                addresses = [
                    ipaddress.ip_address(info[4][0]) for info in answers
                ]
            except (OSError, ValueError, TimeoutError):
                return None
        if not addresses or any(not address.is_global for address in addresses):
            return None
        selected = sorted(addresses, key=lambda address: address.version)[0]
        safe_url = (
            f"tg://proxy?server={quote(str(selected), safe='')}"
            f"&port={parsed.port}&secret={parsed.password}"
        )
        return safe_url, parse_proxy(safe_url)

    async def _prune_unassigned_feed_proxies(self, *, actor: str) -> int:
        """Bound the public feed without deleting assigned or disabled endpoints.

        This group is reserved for auto-imported feed entries. Other proxy
        groups, account slots and manually disabled addresses are untouched.
        The final DELETE checks assignments again to avoid a concurrent bind.
        """
        async with self._session_factory() as session:
            group = await session.scalar(
                select(ProxyGroup).where(ProxyGroup.name == GROUP)
            )
            if group is None:
                return 0
            rows = list(
                (
                    await session.execute(
                        select(ProxyEndpoint.id, ProxyEndpoint.enabled)
                        .where(ProxyEndpoint.group_id == group.id)
                        .order_by(ProxyEndpoint.created_at.desc(), ProxyEndpoint.id.desc())
                    )
                ).all()
            )
            assigned_ids = set(
                await session.scalars(
                    select(ProxyAssignment.proxy_id)
                    .join(ProxyEndpoint, ProxyEndpoint.id == ProxyAssignment.proxy_id)
                    .where(ProxyEndpoint.group_id == group.id)
                )
            )
            # Retain the newest N *usable/unassigned* entries. Assigned routes
            # and disabled entries are outside the quota and are never deleted.
            available = [
                int(proxy_id) for proxy_id, enabled in rows
                if enabled and proxy_id not in assigned_ids
            ]
            removable = available[self._settings.mtproto_feed_pool_limit:]
            if not removable:
                return 0
            removed = 0
            # Small batches avoid SQLite's parameter limit on big old pools.
            for start in range(0, len(removable), 400):
                batch = removable[start:start + 400]
                result = await session.execute(
                    delete(ProxyEndpoint)
                    .where(
                        ProxyEndpoint.id.in_(batch),
                        ProxyEndpoint.group_id == group.id,
                        ProxyEndpoint.enabled.is_(True),
                        ~exists().where(
                            ProxyAssignment.proxy_id == ProxyEndpoint.id
                        ),
                    )
                )
                removed += int(result.rowcount or 0)
            if removed:
                add_audit_event(
                    session,
                    event_type="proxy.feed_pruned",
                    entity_type="proxy_group",
                    entity_id=group.id,
                    details={
                        "removed": removed,
                        "retention_limit": self._settings.mtproto_feed_pool_limit,
                    },
                    actor=actor,
                )
                await session.commit()
            return removed

    async def _read_recent_messages(self) -> tuple[int, list[object]]:
        """Only select authorised active accounts with a known healthy proxy."""
        async with self._session_factory() as session:
            accounts = list(
                await session.scalars(
                    select(Account).where(
                        Account.authorized.is_(True),
                        Account.ownership_confirmed.is_(True),
                        Account.status == AccountStatus.ACTIVE,
                        Account.operator_paused.is_(False),
                        Account.session_source != AccountSessionSource.NONE,
                        Account.session_encrypted.is_not(None),
                        Account.api_id.is_not(None),
                        Account.api_hash_encrypted.is_not(None),
                    ).order_by(Account.id).limit(12)
                )
            )
        for account in accounts:
            route = await self._proxies.proxy_for_assignment(f"account:{account.id}")
            if (
                route is None
                or not route.proxy.enabled
                or route.proxy.status != ProxyStatus.HEALTHY
            ):
                continue

            selected_account_id = int(account.id)

            async def read(
                client: object, read_account_id: int = selected_account_id
            ) -> list[object]:
                from telethon.errors import UserNotParticipantError  # noqa: PLC0415
                from telethon.tl.functions.channels import (  # noqa: PLC0415
                    GetParticipantRequest,
                    JoinChannelRequest,
                )

                entity = await client.get_entity(CHANNEL)
                try:
                    await client(GetParticipantRequest(channel=entity, participant="me"))
                except UserNotParticipantError:
                    # Joining a feed is still a Telegram channel join and
                    # must obey the exact account-wide anti-flood pacing rules.
                    delay = await self._accounts.join_wait_seconds(
                        read_account_id,
                        daily_limit=self._settings.account_join_daily_limit,
                        min_interval=self._settings.account_join_min_interval_seconds,
                    )
                    if delay > 0:
                        raise ConflictError(
                            f"Аккаунт #{read_account_id} не может вступить в канал: "
                            f"пауза ещё {int(delay) + 1} сек."
                        ) from None
                    await client(JoinChannelRequest(channel=entity))
                    await self._accounts.record_join(read_account_id)
                return list(
                    await client.get_messages(
                        entity, limit=self._settings.mtproto_feed_max_messages
                    )
                )

            try:
                _, messages = await self._accounts._run_profile_operation(
                    selected_account_id, read
                )
                return selected_account_id, list(messages)
            except Exception as exc:  # noqa: BLE001 - joining may have already succeeded
                logger.warning(
                    "Feed reader account %d failed (%s); refusing another account",
                    account.id, type(exc).__name__,
                )
                raise ConflictError(
                    "Не удалось прочитать @ProxyMTProto через выбранный аккаунт. "
                    "Повторное вступление с другого аккаунта не выполнялось"
                ) from exc
        raise ConflictError(
            "Нет авторизованного аккаунта с работающим назначенным прокси, "
            "который может прочитать @ProxyMTProto. Прямое соединение не используется"
        )

    def _extract_links(self, messages: list[object]) -> list[tuple[str, ParsedProxy]]:
        """Newest first; only actual proxy URLs, never advertised text claims."""
        found: list[tuple[str, ParsedProxy]] = []
        seen: set[str] = set()
        cutoff = utc_now() - timedelta(hours=48)
        for message in messages:
            posted = ensure_utc(getattr(message, "date", None))
            if posted is not None and posted < cutoff:
                continue
            text = str(
                getattr(message, "message", None)
                or getattr(message, "raw_text", None)
                or ""
            )
            entity_links = [
                entity.url
                for entity in (getattr(message, "entities", None) or [])
                if isinstance(getattr(entity, "url", None), str)
            ]
            for url in proxy_import_values(text, linked_urls=entity_links):
                try:
                    parsed = parse_proxy(url)
                except ProxyInputError:
                    continue
                if parsed.protocol != ProxyProtocol.MTPROTO:
                    continue
                if parsed.fingerprint in seen:
                    continue
                seen.add(parsed.fingerprint)
                found.append((url, parsed))
                if len(found) >= self._settings.mtproto_feed_max_candidates * 3:
                    return found
        return found


def _unsafe_feed_host(host: str) -> bool:
    """Never probe obvious loopback, link-local or private addresses from a feed."""
    candidate = host.strip().lower().rstrip(".")
    if not candidate or candidate == "localhost" or candidate.endswith(
        (".localhost", ".local", ".internal", ".test")
    ):
        return True
    try:
        return not ipaddress.ip_address(candidate).is_global
    except ValueError:
        return False
