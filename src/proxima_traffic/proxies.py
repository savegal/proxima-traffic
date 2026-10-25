from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import logging
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import timedelta
from enum import StrEnum
from html import unescape
from urllib.parse import parse_qs, quote, unquote, urlsplit

import httpx
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.config import Settings
from proxima_traffic.models import (
    Account,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyGroup,
    ProxyProtocol,
    ProxyStatus,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import ConflictError, NotFoundError, add_audit_event

SUPPORTED_SCHEMES = {item.value for item in ProxyProtocol}


class ProxyFilter(StrEnum):
    ALL = "all"
    HEALTHY = "healthy"
    PROBLEM = "problem"
    UNKNOWN = "unknown"
    DISABLED = "disabled"
    ASSIGNED = "assigned"
    FREE = "free"


class ProxyInputError(ValueError):
    pass


class ProxyRouteUnavailable(ConflictError):
    """Raised when an account cannot obtain a safe, proxy-only network route."""


def _normalize_tuple(value: tuple[str, ...] | str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    return value


def _split_group_concat(value: object) -> tuple[str, ...]:
    if not isinstance(value, str) or not value:
        return ()
    return tuple(item for item in value.split("\n") if item)


def _split_assignment_purposes(value: object, count: int) -> tuple[str, ...]:
    if count <= 0:
        return ()
    purposes = tuple(value.split("\n")) if isinstance(value, str) else ()
    return (purposes + ("",) * count)[:count]


@dataclass(frozen=True, slots=True)
class ParsedProxy:
    protocol: ProxyProtocol
    host: str
    port: int
    username: str | None
    password: str | None

    @property
    def fingerprint(self) -> str:
        # An MTProto secret is part of the endpoint identity.  It must never be
        # displayed or stored in the fingerprint verbatim, but two secrets on
        # the same host:port are still different routes.
        secret_identity = self.password or "" if self.protocol == ProxyProtocol.MTPROTO else ""
        identity = (
            f"{self.protocol.value}|{self.host.lower()}|{self.port}|"
            f"{self.username or ''}|{secret_identity}"
        )
        return hashlib.sha256(identity.encode()).hexdigest()

    @property
    def masked_endpoint(self) -> str:
        auth = f"{self.username}:***@" if self.username else ""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{self.protocol.value}://{auth}{host}:{self.port}"


@dataclass(frozen=True, slots=True)
class ProxyView:
    proxy: ProxyEndpoint
    group_name: str | None
    assignment_keys: tuple[str, ...] | str | None = ()
    assignment_purposes: tuple[str, ...] | str | None = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "assignment_keys", _normalize_tuple(self.assignment_keys))
        object.__setattr__(
            self,
            "assignment_purposes",
            _normalize_tuple(self.assignment_purposes),
        )

    @property
    def assignment_key(self) -> str | None:
        keys = self.assignment_keys
        return keys[0] if keys else None

    @property
    def assignment_purpose(self) -> str | None:
        purposes = self.assignment_purposes
        return purposes[0] if purposes else None

    @property
    def assignment_count(self) -> int:
        return len(self.assignment_keys)

    @property
    def masked_endpoint(self) -> str:
        auth = f"{self.proxy.username}:***@" if self.proxy.username else ""
        host = f"[{self.proxy.host}]" if ":" in self.proxy.host else self.proxy.host
        return f"{self.proxy.protocol.value}://{auth}{host}:{self.proxy.port}"

    @property
    def has_rotation_url(self) -> bool:
        return self.proxy.rotation_url_encrypted is not None


@dataclass(frozen=True, slots=True)
class ProxyPage:
    items: list[ProxyView]
    page: int
    page_size: int
    total: int
    filter_name: ProxyFilter = ProxyFilter.ALL
    group_id: int | None = None
    pool_total: int = 0
    pool_healthy: int = 0
    pool_problem: int = 0
    pool_unknown: int = 0
    pool_assigned: int = 0

    @property
    def pages(self) -> int:
        return max(1, (self.total + self.page_size - 1) // self.page_size)


@dataclass(frozen=True, slots=True)
class ProxyImportResult:
    added: int
    skipped: int
    invalid: int
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProxyCheckResult:
    proxy_id: int
    healthy: bool
    status: ProxyStatus
    latency_ms: int | None
    observed_ip: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class _ProxyProbeResult:
    healthy: bool
    latency_ms: int | None
    observed_ip: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class ProxyNetworkRoute:
    """Decrypted route intended only for an in-memory Telegram transport."""

    proxy_id: int
    protocol: ProxyProtocol
    host: str
    port: int
    username: str | None
    password: str | None = dataclass_field(default=None, repr=False)

    @property
    def transport_protocol(self) -> str:
        # Remote DNS resolution prevents a local DNS request when SOCKS is used.
        return "socks5h" if self.protocol == ProxyProtocol.SOCKS5 else self.protocol.value

    @property
    def is_mtproto(self) -> bool:
        return self.protocol == ProxyProtocol.MTPROTO

    @property
    def connection_url(self) -> str:
        auth = ""
        if self.username is not None:
            auth = quote(self.username, safe="")
            if self.password is not None:
                auth += f":{quote(self.password, safe='')}"
            auth += "@"
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{self.transport_protocol}://{auth}{host}:{self.port}"

    @property
    def masked_endpoint(self) -> str:
        auth = f"{self.username}:***@" if self.username is not None else ""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{self.transport_protocol}://{auth}{host}:{self.port}"


class ProxySecretCipher:
    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("proxy encryption key cannot be empty")
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
        self._fernet = Fernet(key)

    @classmethod
    def from_settings(cls, settings: Settings) -> ProxySecretCipher:
        configured = (
            settings.proxy_encryption_key.get_secret_value()
            if settings.proxy_encryption_key is not None
            else settings.admin_api_key.get_secret_value()
        )
        return cls(configured)

    def encrypt(self, value: str | None) -> str | None:
        if value is None:
            return None
        return self._fernet.encrypt(value.encode()).decode()

    def decrypt(self, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except InvalidToken as exc:
            raise ProxyInputError(
                "не удалось расшифровать пароль; проверьте PROXIMA_PROXY_ENCRYPTION_KEY"
            ) from exc


def proxy_import_values(content: str, *, linked_urls: Sequence[str] = ()) -> list[str]:
    """Extract pasted MTProxy links while retaining ordinary one-per-line imports."""

    pattern = re.compile(
        r"(?:https?://(?:t\.me|telegram\.me)/proxy\?|tg://proxy\?|mtproto://)[^\s<>\[\]()]+",
        re.IGNORECASE,
    )
    values: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # Markdown's visible label can itself be the same URL as its target.
        # Extract that target once, but retain actual repeated input entries so
        # the service can report its normal duplicate count.
        stripped = re.sub(r"\[[^\]]*\]\(([^)]+)\)", r"\1", stripped)
        links = pattern.findall(stripped)
        if links:
            values.extend(unescape(link).replace("\\&", "&").strip("`\"'") for link in links)
        else:
            values.append(stripped)
    # A Telegram text_link entity can contain the whole proxy URL while the
    # visible text is just "Connect". Do not count that label as a bad proxy.
    if linked_urls:
        # Channel captions often contain "Port:22" and "Secret:...". Those
        # descriptive fields must not become fabricated HTTP proxy entries.
        values = [value for value in values if "://" in value]
        for link in linked_urls:
            normalized = unescape(link).replace("\\&", "&")
            if normalized not in values:
                values.append(normalized)
    return values


def parse_proxy(value: str) -> ParsedProxy:
    raw = value.strip()
    if not raw or raw.startswith("#"):
        raise ProxyInputError("пустая строка")
    mtproto_link = _parse_telegram_mtproto_link(raw)
    if mtproto_link is not None:
        return mtproto_link
    if "://" not in raw:
        parts = raw.split(":")
        if len(parts) == 2:
            raw = f"http://{parts[0]}:{parts[1]}"
        elif len(parts) == 4:
            host, port, username, password = parts
            raw = f"http://{quote(username)}:{quote(password)}@{host}:{port}"
        else:
            raise ProxyInputError(
                "используйте scheme://user:pass@host:port или host:port:user:pass"
            )
    parsed = urlsplit(raw)
    scheme = parsed.scheme.lower()
    if scheme not in SUPPORTED_SCHEMES:
        raise ProxyInputError(f"неподдерживаемый протокол: {scheme or 'не указан'}")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ProxyInputError("некорректный порт") from exc
    if not parsed.hostname or port is None or not 1 <= port <= 65535:
        raise ProxyInputError("нужно указать host и port")
    if parsed.path not in {"", "/"} or parsed.fragment:
        raise ProxyInputError("адрес прокси не должен содержать path, query или fragment")
    if scheme == ProxyProtocol.MTPROTO.value:
        if parsed.username is not None or parsed.password is not None:
            raise ProxyInputError("для MTProto укажите secret параметром ?secret=...")
        values = parse_qs(parsed.query, keep_blank_values=True)
        secret_values = values.pop("secret", [])
        if values or len(secret_values) != 1:
            raise ProxyInputError("для MTProto нужен только параметр secret")
        return ParsedProxy(
            protocol=ProxyProtocol.MTPROTO,
            host=parsed.hostname,
            port=port,
            username=None,
            password=_validate_mtproto_secret(secret_values[0]),
        )
    if parsed.query:
        raise ProxyInputError("адрес прокси не должен содержать path, query или fragment")
    username = unquote(parsed.username) if parsed.username is not None else None
    password = unquote(parsed.password) if parsed.password is not None else None
    if password is not None and username is None:
        raise ProxyInputError("для пароля нужно указать логин")
    if username is not None and not username:
        raise ProxyInputError("логин не может быть пустым")
    return ParsedProxy(
        protocol=ProxyProtocol(scheme),
        host=parsed.hostname,
        port=port,
        username=username,
        password=password,
    )


def _parse_telegram_mtproto_link(raw: str) -> ParsedProxy | None:
    """Accept native Telegram MTProxy links without treating them as web proxies."""

    parsed = urlsplit(raw)
    is_tg_link = parsed.scheme.lower() == "tg" and parsed.netloc.lower() == "proxy"
    is_short_link = (
        parsed.scheme.lower() in {"http", "https"}
        and (parsed.hostname or "").lower() in {"t.me", "telegram.me"}
        and parsed.path.rstrip("/").lower() == "/proxy"
    )
    if not is_tg_link and not is_short_link:
        return None
    if parsed.fragment:
        raise ProxyInputError("ссылка MTProto не должна содержать fragment")
    values = parse_qs(parsed.query, keep_blank_values=True)
    servers = values.pop("server", [])
    ports = values.pop("port", [])
    secrets = values.pop("secret", [])
    if values or len(servers) != 1 or len(ports) != 1 or len(secrets) != 1:
        raise ProxyInputError("ссылка MTProto должна содержать server, port и secret")
    host = servers[0].strip().rstrip(".")
    if not host:
        raise ProxyInputError("в ссылке MTProto не указан server")
    try:
        port = int(ports[0])
    except ValueError as exc:
        raise ProxyInputError("в ссылке MTProto указан некорректный port") from exc
    if not 1 <= port <= 65535:
        raise ProxyInputError("в ссылке MTProto указан некорректный port")
    return ParsedProxy(
        protocol=ProxyProtocol.MTPROTO,
        host=host,
        port=port,
        username=None,
        password=_validate_mtproto_secret(secrets[0]),
    )


def _validate_mtproto_secret(value: str) -> str:
    secret = value.strip()
    if not 16 <= len(secret) <= 512:
        raise ProxyInputError("secret MTProto должен содержать от 16 до 512 символов")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_+/=")
    if any(character not in allowed for character in secret):
        raise ProxyInputError("secret MTProto содержит недопустимые символы")

    # If already a valid hex string (with or without ee/dd prefix):
    prefix = secret.lower()[:2]
    hex_body = secret[2:] if prefix in {"ee", "dd"} else secret
    try:
        bytes.fromhex(hex_body)
        return secret
    except ValueError:
        pass

    # Support base64 and URL-safe base64 secrets (common in Telegram share links)
    import base64
    for candidate_body, keep_prefix in [
        (secret[2:], prefix if prefix in {"ee", "dd"} else ""),
        (secret, ""),
    ]:
        for pad in ["", "=", "==", "==="]:
            try:
                decoded = base64.urlsafe_b64decode(candidate_body + pad)
                if len(decoded) >= 16:
                    return keep_prefix + decoded.hex()
            except Exception:
                pass
    return secret


def mtproto_connection_type(secret: str) -> object:
    """Choose Telethon's MTProxy transport from the secret prefix.

    ``dd`` routes use randomized intermediate, ``ee`` routes use intermediate;
    legacy/base64 secrets use abridged.  The secret remains in memory only and
    is never included in diagnostics.
    """

    from telethon.network.connection.tcpmtproxy import (  # noqa: PLC0415
        ConnectionTcpMTProxyAbridged,
        ConnectionTcpMTProxyIntermediate,
        ConnectionTcpMTProxyRandomizedIntermediate,
    )

    prefix = secret.lower()[:2]
    if prefix == "dd":
        return ConnectionTcpMTProxyRandomizedIntermediate
    if prefix == "ee":
        return ConnectionTcpMTProxyIntermediate
    return ConnectionTcpMTProxyAbridged


def _mtproto_probe_logger() -> logging.Logger:
    """Keep expected probe failures out of the operator log and out of alerts."""

    probe_logger = logging.getLogger("proxima_traffic.mtproto_probe")
    probe_logger.setLevel(logging.CRITICAL)
    probe_logger.propagate = False
    if not probe_logger.handlers:
        probe_logger.addHandler(logging.NullHandler())
    return probe_logger


def validate_rotation_url(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ProxyInputError("ссылка ротации не может быть пустой")
    if len(normalized) > 2000:
        raise ProxyInputError("ссылка ротации не должна быть длиннее 2000 символов")
    parsed = urlsplit(normalized)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ProxyInputError("ссылка ротации должна начинаться с http:// или https://")
    if not parsed.netloc:
        raise ProxyInputError("ссылка ротации должна содержать host")
    return normalized


class ProxyService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._cipher = ProxySecretCipher.from_settings(settings)
        # A failed route can be observed by several workers at once.  One
        # recovery pass is enough and prevents them from stampeding the whole
        # proxy pool with duplicate health checks.
        self._account_route_recovery_lock = asyncio.Lock()
        # Several accounts may deliberately share a proxy.  Keep one fresh
        # probe per endpoint for a short recovery/check wave rather than
        # issuing the same HTTP check once per account.
        self._route_probe_cache: dict[int, tuple[float, ProxyCheckResult]] = {}
        self._route_probe_tasks: dict[int, asyncio.Task[ProxyCheckResult]] = {}
        self._route_probe_lock = asyncio.Lock()
        # A provider rotation is a stateful external operation.  Do not let a
        # double tap rotate the same mobile endpoint twice in one process.
        self._rotation_locks: dict[int, asyncio.Lock] = {}

    async def create_group(self, name: str, description: str = "") -> ProxyGroup:
        normalized_name = name.strip()
        if not normalized_name or len(normalized_name) > 100:
            raise ProxyInputError("имя группы должно содержать от 1 до 100 символов")
        async with self._session_factory() as session:
            group = ProxyGroup(name=normalized_name, description=description.strip()[:500])
            session.add(group)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("группа с таким именем уже существует") from exc
            add_audit_event(
                session,
                event_type="proxy_group.created",
                entity_type="proxy_group",
                entity_id=group.id,
                details={"name": group.name},
                actor="control-bot",
            )
            await session.commit()
            await session.refresh(group)
            return group

    async def list_groups(self) -> list[ProxyGroup]:
        async with self._session_factory() as session:
            return list(await session.scalars(select(ProxyGroup).order_by(ProxyGroup.name)))

    async def delete_group(self, group_id: int, *, actor: str = "control-bot") -> None:
        async with self._session_factory() as session:
            group = await session.get(ProxyGroup, group_id)
            if group is None:
                raise NotFoundError("группа прокси не найдена")
            endpoints = list(
                await session.scalars(
                    select(ProxyEndpoint).where(ProxyEndpoint.group_id == group_id)
                )
            )
            for endpoint in endpoints:
                endpoint.group_id = None
            add_audit_event(
                session,
                event_type="proxy_group.deleted",
                entity_type="proxy_group",
                entity_id=group_id,
                details={"name": group.name, "detached_proxies": len(endpoints)},
                actor=actor,
            )
            await session.delete(group)
            await session.commit()

    async def add_proxy(
        self,
        value: str,
        *,
        label: str | None = None,
        group_name: str | None = None,
        rotation_url: str | None = None,
        actor: str = "control-bot",
    ) -> ProxyView:
        parsed = parse_proxy(value)
        if parsed.protocol == ProxyProtocol.MTPROTO and rotation_url is not None:
            raise ProxyInputError("у MTProto-прокси не бывает ссылки ротации IP")
        normalized_rotation_url = (
            validate_rotation_url(rotation_url) if rotation_url is not None else None
        )
        async with self._session_factory() as session:
            group = await self._find_or_create_group(session, group_name)
            endpoint = ProxyEndpoint(
                label=(label or f"{parsed.host}:{parsed.port}").strip()[:100],
                group_id=group.id if group else None,
                protocol=parsed.protocol,
                host=parsed.host,
                port=parsed.port,
                username=parsed.username,
                password_encrypted=self._cipher.encrypt(parsed.password),
                rotation_url_encrypted=self._cipher.encrypt(normalized_rotation_url),
                fingerprint=parsed.fingerprint,
            )
            session.add(endpoint)
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("такой прокси уже существует") from exc
            add_audit_event(
                session,
                event_type="proxy.created",
                entity_type="proxy",
                entity_id=endpoint.id,
                details={"endpoint": parsed.masked_endpoint, "group": group_name},
                actor=actor,
            )
            await session.commit()
            try:
                await self.check_proxy(endpoint.id, actor=actor)
            except Exception:
                pass
            return await self.get_proxy(endpoint.id)

    async def import_proxies(
        self,
        values: list[str],
        *,
        group_name: str | None,
        actor: str = "control-bot",
    ) -> ProxyImportResult:
        parsed_values: list[tuple[int, ParsedProxy]] = []
        errors: list[str] = []
        seen: set[str] = set()
        skipped = 0
        for line_number, value in enumerate(values, start=1):
            if not value.strip() or value.lstrip().startswith("#"):
                continue
            try:
                parsed = parse_proxy(value)
            except ProxyInputError as exc:
                errors.append(f"строка {line_number}: {exc}")
                continue
            if parsed.fingerprint in seen:
                skipped += 1
                continue
            seen.add(parsed.fingerprint)
            parsed_values.append((line_number, parsed))
        if not parsed_values and not errors:
            raise ProxyInputError("список прокси пуст")

        async with self._session_factory() as session:
            group = await self._find_or_create_group(session, group_name)
            fingerprints = [item.fingerprint for _, item in parsed_values]
            existing = set(
                await session.scalars(
                    select(ProxyEndpoint.fingerprint).where(
                        ProxyEndpoint.fingerprint.in_(fingerprints)
                    )
                )
            )
            added = 0
            for _, parsed in parsed_values:
                if parsed.fingerprint in existing:
                    skipped += 1
                    continue
                session.add(
                    ProxyEndpoint(
                        label=f"{parsed.host}:{parsed.port}"[:100],
                        group_id=group.id if group else None,
                        protocol=parsed.protocol,
                        host=parsed.host,
                        port=parsed.port,
                        username=parsed.username,
                        password_encrypted=self._cipher.encrypt(parsed.password),
                        fingerprint=parsed.fingerprint,
                    )
                )
                added += 1
            add_audit_event(
                session,
                event_type="proxy.batch_imported",
                entity_type="proxy",
                entity_id=None,
                details={
                    "group": group_name,
                    "added": added,
                    "skipped": skipped,
                    "invalid": len(errors),
                },
                actor=actor,
            )
            await session.commit()
        return ProxyImportResult(added, skipped, len(errors), tuple(errors[:10]))

    async def list_proxies(
        self,
        page: int,
        page_size: int,
        *,
        status: ProxyStatus | None = None,
        group_id: int | None = None,
        filter_name: ProxyFilter = ProxyFilter.ALL,
    ) -> ProxyPage:
        normalized_page = max(page, 0)
        conditions = []
        if status is not None:
            conditions.append(ProxyEndpoint.status == status)
        if group_id is not None:
            conditions.append(ProxyEndpoint.group_id == group_id)
        if filter_name == ProxyFilter.HEALTHY:
            conditions.append(ProxyEndpoint.status == ProxyStatus.HEALTHY)
        elif filter_name == ProxyFilter.PROBLEM:
            conditions.append(
                ProxyEndpoint.status.in_([ProxyStatus.UNHEALTHY, ProxyStatus.QUARANTINED])
            )
        elif filter_name == ProxyFilter.UNKNOWN:
            conditions.append(ProxyEndpoint.status == ProxyStatus.UNKNOWN)
        elif filter_name == ProxyFilter.DISABLED:
            conditions.append(ProxyEndpoint.enabled.is_(False))
        elif filter_name == ProxyFilter.ASSIGNED:
            conditions.append(ProxyAssignment.id.is_not(None))
        elif filter_name == ProxyFilter.FREE:
            conditions.append(ProxyAssignment.id.is_(None))
        async with self._session_factory() as session:
            count_statement = (
                select(func.count(func.distinct(ProxyEndpoint.id)))
                .select_from(ProxyEndpoint)
                .outerjoin(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                .where(*conditions)
            )
            total = int(await session.scalar(count_statement) or 0)
            pool_total = await self._count_endpoints(session)
            pool_healthy = await self._count_endpoints(
                session, ProxyEndpoint.status == ProxyStatus.HEALTHY
            )
            pool_problem = await self._count_endpoints(
                session,
                ProxyEndpoint.status.in_([ProxyStatus.UNHEALTHY, ProxyStatus.QUARANTINED]),
            )
            pool_unknown = await self._count_endpoints(
                session, ProxyEndpoint.status == ProxyStatus.UNKNOWN
            )
            pool_assigned = int(
                await session.scalar(select(func.count()).select_from(ProxyAssignment)) or 0
            )
            rows = (
                await session.execute(
                    self._view_statement()
                    .where(*conditions)
                    .order_by(ProxyEndpoint.id.desc())
                    .offset(normalized_page * page_size)
                    .limit(page_size)
                )
            ).all()
            return ProxyPage(
                items=[self._row_to_view(row) for row in rows],
                page=normalized_page,
                page_size=page_size,
                total=total,
                filter_name=filter_name,
                group_id=group_id,
                pool_total=pool_total,
                pool_healthy=pool_healthy,
                pool_problem=pool_problem,
                pool_unknown=pool_unknown,
                pool_assigned=pool_assigned,
            )

    async def allocate_proxy(
        self,
        assignment_key: str,
        purpose: str = "",
        *,
        group_name: str | None = None,
        required_account_id: int | None = None,
        actor: str = "control-bot",
    ) -> ProxyView:
        normalized_key = assignment_key.strip()
        if not normalized_key or len(normalized_key) > 150:
            raise ProxyInputError("ключ слота должен содержать от 1 до 150 символов")
        if required_account_id is not None and normalized_key != f"account:{required_account_id}":
            raise ProxyInputError("ключ слота не соответствует аккаунту")
        async with self._session_factory() as session:
            if required_account_id is not None:
                account = await session.get(Account, required_account_id)
                if account is None:
                    raise NotFoundError("аккаунт не найден")
                if account.session_source.value == "none":
                    raise ConflictError("у аккаунта нет Telegram-сессии — сначала импортируйте её")
            existing = await session.scalar(
                select(ProxyAssignment.id).where(ProxyAssignment.assignment_key == normalized_key)
            )
            if existing is not None:
                raise ConflictError("этот слот уже занят")
            assigned_count = func.count(ProxyAssignment.id)
            statement = (
                select(ProxyEndpoint)
                .outerjoin(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                .where(
                    ProxyEndpoint.enabled.is_(True),
                    ProxyEndpoint.status == ProxyStatus.HEALTHY,
                )
                .group_by(ProxyEndpoint.id)
                .order_by(
                    assigned_count,
                    ProxyEndpoint.latency_ms.is_(None),
                    ProxyEndpoint.latency_ms,
                    ProxyEndpoint.id,
                )
            )
            if group_name is not None and group_name.strip():
                statement = statement.join(
                    ProxyGroup, ProxyGroup.id == ProxyEndpoint.group_id
                ).where(ProxyGroup.name == group_name.strip())
            endpoint = await session.scalar(statement.limit(1))
            if endpoint is None:
                raise ConflictError("в выбранном пуле нет свободных исправных прокси")
            session.add(
                ProxyAssignment(
                    proxy_id=endpoint.id,
                    assignment_key=normalized_key,
                    purpose=purpose.strip()[:250],
                )
            )
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("этот слот уже занят") from exc
            add_audit_event(
                session,
                event_type="proxy.auto_assigned",
                entity_type="proxy",
                entity_id=endpoint.id,
                details={
                    "assignment_key": normalized_key,
                    "purpose": purpose.strip()[:250],
                    "group": group_name,
                },
                actor=actor,
            )
            await self._bump_account_connection_generations(session, [normalized_key])
            proxy_id = endpoint.id
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def get_proxy(self, proxy_id: int) -> ProxyView:
        async with self._session_factory() as session:
            row = (
                await session.execute(self._view_statement().where(ProxyEndpoint.id == proxy_id))
            ).first()
            if row is None:
                raise NotFoundError("прокси не найден")
            return self._row_to_view(row)

    async def proxy_for_assignment(self, assignment_key: str) -> ProxyView | None:
        normalized_key = assignment_key.strip()
        if not normalized_key:
            return None
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    self._view_statement().where(ProxyAssignment.assignment_key == normalized_key)
                )
            ).first()
            return self._row_to_view(row) if row is not None else None

    async def require_route(self, assignment_key: str) -> ProxyNetworkRoute:
        """Resolve a healthy route without ever falling back to a direct connection."""
        normalized_key = assignment_key.strip()
        if not normalized_key:
            raise ProxyRouteUnavailable("не указан обязательный слот прокси")
        async with self._session_factory() as session:
            endpoint = await session.scalar(
                select(ProxyEndpoint)
                .join(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                .where(ProxyAssignment.assignment_key == normalized_key)
            )
            if endpoint is None:
                raise ProxyRouteUnavailable(
                    f"для слота {normalized_key!r} не назначен прокси; прямое соединение запрещено"
                )
            if not endpoint.enabled or endpoint.status != ProxyStatus.HEALTHY:
                raise ProxyRouteUnavailable(
                    f"прокси слота {normalized_key!r} не прошёл проверку; "
                    "прямое соединение запрещено"
                )
            return ProxyNetworkRoute(
                proxy_id=endpoint.id,
                protocol=endpoint.protocol,
                host=endpoint.host,
                port=endpoint.port,
                username=endpoint.username,
                password=self._cipher.decrypt(endpoint.password_encrypted),
            )

    async def require_account_route(self, account_id: int) -> ProxyNetworkRoute:
        if account_id <= 0:
            raise ProxyRouteUnavailable("некорректный идентификатор аккаунта")
        return await self.require_route(f"account:{account_id}")

    async def ensure_account_route(
        self,
        account_id: int,
        *,
        actor: str = "runtime",
    ) -> ProxyNetworkRoute:
        """Return a freshly verified account route, replacing a failed proxy when possible.

        The service is deliberately fail-closed: an account never falls back to a
        direct Telegram connection.  Replacement is limited to *enabled* proxy
        endpoints which have just passed the same health probe shown in the UI.
        The old assignment remains untouched if the pool has no working route,
        so an operator can clearly see which proxy failed and why.
        """

        if account_id <= 0:
            raise ProxyRouteUnavailable("некорректный идентификатор аккаунта")
        assignment_key = f"account:{account_id}"
        assigned = await self.proxy_for_assignment(assignment_key)
        if assigned is None:
            try:
                assigned = await self.allocate_proxy(
                    assignment_key, purpose=f"account:{account_id}", actor=actor
                )
            except Exception:
                assigned = None
        if assigned is None:
            raise ProxyRouteUnavailable(
                f"для аккаунта #{account_id} не назначен прокси и нет свободных исправных "
                "прокси в пуле; добавьте рабочий прокси"
            )
        if await self._assigned_proxy_is_healthy(assigned, actor=actor):
            return await self.require_account_route(account_id)

        # Normal connection starts can check their own currently assigned
        # route concurrently.  Only the expensive pool-wide recovery is
        # serialized, so one outage cannot create duplicate check-all waves.
        async with self._account_route_recovery_lock:
            assigned = await self.proxy_for_assignment(assignment_key)
            if assigned is None:
                try:
                    assigned = await self.allocate_proxy(
                        assignment_key, purpose=f"account:{account_id}", actor=actor
                    )
                except Exception:
                    assigned = None
            if assigned is None:
                raise ProxyRouteUnavailable(
                    f"для аккаунта #{account_id} не назначен прокси и нет свободных исправных "
                    "прокси в пуле; добавьте рабочий прокси"
                )
            current_is_healthy = assigned.proxy.status == ProxyStatus.HEALTHY
            if not current_is_healthy:
                current_is_healthy = await self._assigned_proxy_is_healthy(assigned, actor=actor)
            if current_is_healthy:
                return await self.require_account_route(account_id)

            current_id = int(assigned.proxy.id)
            replacement_ids = await self._verify_replacement_candidates(
                excluded_proxy_id=current_id,
                actor=actor,
            )
            replacement = await self._replace_failed_account_route(
                assignment_key,
                failed_proxy_id=current_id,
                candidate_ids=replacement_ids,
                actor=actor,
            )
            if replacement is not None:
                return replacement

            raise ProxyRouteUnavailable(
                "назначенный прокси не работает; проверены все включённые прокси, "
                "но рабочего не найдено. Добавьте рабочий HTTP(S)/SOCKS5-прокси."
            )

    async def auto_assign_healthy_proxies(self, *, actor: str = "runtime") -> int:
        """Assign available healthy proxies to authorized accounts lacking a working route."""
        async with self._session_factory() as session:
            accounts = list(
                await session.scalars(
                    select(Account).where(Account.authorized == 1)
                )
            )
        assigned_count = 0
        for acc in accounts:
            try:
                assignment = await self.proxy_for_assignment(f"account:{acc.id}")
                if assignment is None or assignment.proxy.status != ProxyStatus.HEALTHY:
                    await self.ensure_account_route(int(acc.id), actor=actor)
                    assigned_count += 1
            except Exception:
                pass
        return assigned_count

    async def account_route_check_due(self, account_id: int, *, max_age_seconds: float) -> bool:
        """Whether a cached Telegram client needs a new route health check."""

        if account_id <= 0:
            return True
        max_age = max(1.0, float(max_age_seconds))
        async with self._session_factory() as session:
            endpoint = await session.scalar(
                select(ProxyEndpoint)
                .join(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                .where(ProxyAssignment.assignment_key == f"account:{account_id}")
            )
        if endpoint is None or not endpoint.enabled:
            return True
        # SQLite returns persisted DATETIME fields without tzinfo.  Connection
        # startup compares this value with ``utc_now()``, so normalize legacy
        # rows before deciding whether a route must be probed again.
        checked_at = ensure_utc(endpoint.last_checked_at)
        return checked_at is None or checked_at < utc_now() - timedelta(seconds=max_age)

    async def _assigned_proxy_is_healthy(self, item: ProxyView, *, actor: str) -> bool:
        if not item.proxy.enabled:
            return False
        try:
            return (await self._check_proxy_for_route(int(item.proxy.id), actor=actor)).healthy
        except (ConflictError, NotFoundError, ProxyInputError):
            # A disabled, deleted, or unreadable endpoint is handled like a
            # failed probe.  Do not expose credential errors as a generic
            # Telegram/network failure.
            return False

    async def _verify_replacement_candidates(
        self,
        *,
        excluded_proxy_id: int,
        actor: str,
    ) -> tuple[int, ...]:
        """Probe every enabled alternative and return only healthy endpoint IDs."""

        async with self._session_factory() as session:
            candidate_ids = tuple(
                int(value)
                for value in await session.scalars(
                    select(ProxyEndpoint.id)
                    .where(
                        ProxyEndpoint.enabled.is_(True),
                        ProxyEndpoint.id != excluded_proxy_id,
                    )
                    .order_by(ProxyEndpoint.id)
                )
            )
        if not candidate_ids:
            return ()

        results = await asyncio.gather(
            *(self._check_proxy_for_route(proxy_id, actor=actor) for proxy_id in candidate_ids),
            return_exceptions=True,
        )
        return tuple(
            proxy_id
            for proxy_id, result in zip(candidate_ids, results, strict=True)
            if isinstance(result, ProxyCheckResult) and result.healthy
        )

    async def _check_proxy_for_route(self, proxy_id: int, *, actor: str) -> ProxyCheckResult:
        """Return one recent health probe for route selection.

        This intentionally wraps only automatic account-route recovery.  A
        manual proxy check remains a fresh operator-requested probe.  Both
        successful and failed results are briefly cached: a failed shared IP
        must not trigger a separate full-pool recovery for every account.
        """
        now = time.monotonic()
        async with self._route_probe_lock:
            cached = self._route_probe_cache.get(proxy_id)
            if cached is not None and now - cached[0] < 8.0:
                return cached[1]
            task = self._route_probe_tasks.get(proxy_id)
            if task is None:
                task = asyncio.create_task(self.check_proxy(proxy_id, actor=actor))
                self._route_probe_tasks[proxy_id] = task
        try:
            result = await asyncio.shield(task)
        finally:
            if task.done():
                async with self._route_probe_lock:
                    if self._route_probe_tasks.get(proxy_id) is task:
                        self._route_probe_tasks.pop(proxy_id, None)
        async with self._route_probe_lock:
            self._route_probe_cache[proxy_id] = (time.monotonic(), result)
        return result

    async def _replace_failed_account_route(
        self,
        assignment_key: str,
        *,
        failed_proxy_id: int,
        candidate_ids: tuple[int, ...],
        actor: str,
    ) -> ProxyNetworkRoute | None:
        """Atomically point one account slot at the best freshly healthy proxy."""

        if not candidate_ids:
            return None
        account_id = int(assignment_key.split(":", 1)[1])
        recovered_by_another_worker = False
        async with self._session_factory() as session:
            assignment = await session.scalar(
                select(ProxyAssignment).where(ProxyAssignment.assignment_key == assignment_key)
            )
            if assignment is None:
                return None

            # Another worker may already have recovered the same account while
            # this worker was probing the pool.  Keep that valid replacement.
            if assignment.proxy_id != failed_proxy_id:
                current = await session.get(ProxyEndpoint, assignment.proxy_id)
                if (
                    current is not None
                    and current.enabled
                    and current.status == ProxyStatus.HEALTHY
                ):
                    recovered_by_another_worker = True
                else:
                    return None

            if not recovered_by_another_worker:
                assigned_count = func.count(ProxyAssignment.id)
                candidate = await session.scalar(
                    select(ProxyEndpoint)
                    .outerjoin(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                    .where(
                        ProxyEndpoint.id.in_(candidate_ids),
                        ProxyEndpoint.enabled.is_(True),
                        ProxyEndpoint.status == ProxyStatus.HEALTHY,
                    )
                    .group_by(ProxyEndpoint.id)
                    .order_by(
                        assigned_count,
                        ProxyEndpoint.latency_ms.is_(None),
                        ProxyEndpoint.latency_ms,
                        ProxyEndpoint.id,
                    )
                    .limit(1)
                )
                if candidate is None:
                    return None
                assignment.proxy_id = candidate.id
                add_audit_event(
                    session,
                    event_type="proxy.account_auto_replaced",
                    entity_type="proxy",
                    entity_id=candidate.id,
                    details={
                        "assignment_key": assignment_key,
                        "failed_proxy_id": failed_proxy_id,
                        "replacement_proxy_id": candidate.id,
                    },
                    actor=actor,
                )
                await self._bump_account_connection_generations(session, [assignment_key])
                await session.commit()
        return await self.require_account_route(account_id)

    async def assignment_endpoint(self, assignment_key: str) -> str | None:
        """Return a credential-free endpoint string for an assigned slot."""
        normalized_key = assignment_key.strip()
        if not normalized_key:
            return None
        async with self._session_factory() as session:
            endpoint = await session.scalar(
                select(ProxyEndpoint)
                .join(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
                .where(ProxyAssignment.assignment_key == normalized_key)
            )
            if endpoint is None:
                return None
            host = f"[{endpoint.host}]" if ":" in endpoint.host else endpoint.host
            return f"{endpoint.protocol.value}://{host}:{endpoint.port}"

    async def account_proxy_endpoint(self, account_id: int) -> str | None:
        return await self.assignment_endpoint(f"account:{account_id}")

    async def toggle_proxy(self, proxy_id: int, *, actor: str = "control-bot") -> ProxyView:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            endpoint.enabled = not endpoint.enabled
            endpoint.status = ProxyStatus.UNKNOWN if endpoint.enabled else ProxyStatus.DISABLED
            endpoint.last_error = None if endpoint.enabled else endpoint.last_error
            add_audit_event(
                session,
                event_type="proxy.enabled" if endpoint.enabled else "proxy.disabled",
                entity_type="proxy",
                entity_id=endpoint.id,
                details={"enabled": endpoint.enabled},
                actor=actor,
            )
            await self._bump_proxy_account_generations(session, proxy_id)
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def replace_proxy_url(
        self, proxy_id: int, value: str, *, actor: str = "control-bot"
    ) -> ProxyView:
        parsed = parse_proxy(value)
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            endpoint.protocol = parsed.protocol
            endpoint.host = parsed.host
            endpoint.port = parsed.port
            endpoint.username = parsed.username
            endpoint.password_encrypted = self._cipher.encrypt(parsed.password)
            if parsed.protocol == ProxyProtocol.MTPROTO:
                # A link left over from an HTTP/mobile endpoint must not make
                # the MTProto card advertise an action it cannot perform.
                endpoint.rotation_url_encrypted = None
            endpoint.fingerprint = parsed.fingerprint
            endpoint.status = ProxyStatus.UNKNOWN
            endpoint.consecutive_failures = 0
            endpoint.latency_ms = None
            endpoint.observed_ip = None
            endpoint.last_error = None
            endpoint.last_checked_at = None
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("такой прокси уже существует") from exc
            add_audit_event(
                session,
                event_type="proxy.endpoint_replaced",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"endpoint": parsed.masked_endpoint},
                actor=actor,
            )
            await self._bump_proxy_account_generations(session, proxy_id)
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def set_proxy_rotation_url(
        self,
        proxy_id: int,
        value: str | None,
        *,
        actor: str = "control-bot",
    ) -> ProxyView:
        normalized = validate_rotation_url(value) if value is not None else None
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            if endpoint.protocol == ProxyProtocol.MTPROTO and normalized is not None:
                raise ConflictError("у MTProto-прокси не бывает ссылки ротации IP")
            endpoint.rotation_url_encrypted = self._cipher.encrypt(normalized)
            add_audit_event(
                session,
                event_type=(
                    "proxy.rotation_url_updated"
                    if normalized is not None
                    else "proxy.rotation_url_cleared"
                ),
                entity_type="proxy",
                entity_id=proxy_id,
                details={"configured": normalized is not None},
                actor=actor,
            )
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def rotate_proxy(self, proxy_id: int, *, actor: str = "control-bot") -> ProxyView:
        lock = self._rotation_locks.setdefault(proxy_id, asyncio.Lock())
        if lock.locked():
            raise ConflictError("ротация этого прокси уже выполняется")
        async with lock:
            return await self._rotate_proxy_locked(proxy_id, actor=actor)

    async def _rotate_proxy_locked(self, proxy_id: int, *, actor: str = "control-bot") -> ProxyView:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            if endpoint.protocol == ProxyProtocol.MTPROTO:
                raise ConflictError("MTProto-прокси не поддерживает смену IP по ссылке")
            if endpoint.rotation_url_encrypted is None:
                raise ConflictError("для прокси не задана ссылка ротации")
            try:
                rotation_url = self._cipher.decrypt(endpoint.rotation_url_encrypted)
                password = self._cipher.decrypt(endpoint.password_encrypted)
            except ProxyInputError as exc:
                raise ConflictError("ссылка ротации недоступна для расшифровки") from exc
            if rotation_url is None:
                raise ConflictError("для прокси не задана ссылка ротации")
            old_status = endpoint.status
            old_ip = endpoint.observed_ip
            old_latency_ms = endpoint.latency_ms
            proxy_url = self._build_url(endpoint, password)
            if old_ip is None:
                # Without a pre-rotation IP a successful HTTP response cannot
                # prove that the provider actually changed the route.
                baseline = await self._probe_proxy_url(proxy_url)
                if not baseline.healthy or baseline.observed_ip is None:
                    raise ConflictError(
                        "не удалось определить IP до ротации; сначала проверьте прокси"
                    )
                old_ip = baseline.observed_ip
                old_latency_ms = baseline.latency_ms
            endpoint.status = ProxyStatus.UNKNOWN
            endpoint.latency_ms = None
            endpoint.last_error = None
            await self._bump_proxy_account_generations(session, proxy_id)
            await session.commit()
        request_ambiguous = False
        try:
            timeout = httpx.Timeout(self._settings.proxy_check_timeout_seconds)
            async with httpx.AsyncClient(
                timeout=timeout,
                trust_env=False,
                follow_redirects=True,
            ) as client:
                response = await client.get(rotation_url)
                response.raise_for_status()
        except (httpx.ReadTimeout, httpx.ReadError, httpx.RemoteProtocolError):
            # Reboot links often cut their own response while the modem is already
            # restarting. The observable proxy IP is the source of truth below.
            request_ambiguous = True
        except httpx.HTTPStatusError as exc:
            await self._record_rotation_failure(
                proxy_id,
                old_status=old_status,
                old_ip=old_ip,
                old_latency_ms=old_latency_ms,
                error=f"ссылка ротации вернула HTTP {exc.response.status_code}",
                actor=actor,
            )
            raise ConflictError(
                f"провайдер отклонил ротацию: HTTP {exc.response.status_code}"
            ) from exc
        except (httpx.HTTPError, OSError, ValueError) as exc:
            await self._record_rotation_failure(
                proxy_id,
                old_status=old_status,
                old_ip=old_ip,
                old_latency_ms=old_latency_ms,
                error=f"{type(exc).__name__}: запрос ротации не доставлен",
                actor=actor,
            )
            raise ConflictError(f"запрос ротации не доставлен ({type(exc).__name__})") from exc

        probe: _ProxyProbeResult | None = None
        ip_changed = False
        for attempt in range(4):
            if attempt:
                await asyncio.sleep(3)
            probe = await self._probe_proxy_url(proxy_url)
            ip_changed = bool(
                probe.healthy and old_ip and probe.observed_ip and probe.observed_ip != old_ip
            )
            if ip_changed:
                break

        assert probe is not None
        if probe.healthy and ip_changed:
            async with self._session_factory() as session:
                endpoint = await session.get(ProxyEndpoint, proxy_id)
                if endpoint is None:
                    raise NotFoundError("прокси не найден")
                endpoint.last_rotated_at = utc_now()
                endpoint.last_checked_at = endpoint.last_rotated_at
                endpoint.status = ProxyStatus.HEALTHY
                endpoint.success_count += 1
                endpoint.consecutive_failures = 0
                endpoint.latency_ms = probe.latency_ms
                endpoint.observed_ip = probe.observed_ip
                endpoint.last_error = None
                add_audit_event(
                    session,
                    event_type="proxy.rotated",
                    entity_type="proxy",
                    entity_id=proxy_id,
                    details={
                        "ip_changed": ip_changed,
                        "response_timed_out": request_ambiguous,
                    },
                    actor=actor,
                )
                await session.commit()
            return await self.get_proxy(proxy_id)

        reason = (
            "ссылка не ответила, а изменение IP не подтвердилось"
            if request_ambiguous and probe.healthy
            else probe.error or "прокси не восстановился после ротации"
        )
        await self._record_rotation_probe_failure(proxy_id, probe, reason, actor=actor)
        raise ConflictError(f"ротация не подтверждена: {reason}")

    async def _record_rotation_failure(
        self,
        proxy_id: int,
        *,
        old_status: ProxyStatus,
        old_ip: str | None,
        old_latency_ms: int | None,
        error: str,
        actor: str,
    ) -> None:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            endpoint.status = old_status
            endpoint.observed_ip = old_ip
            endpoint.latency_ms = old_latency_ms
            endpoint.last_error = error[:500]
            add_audit_event(
                session,
                event_type="proxy.rotation_failed",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"error": error[:200]},
                actor=actor,
            )
            await session.commit()

    async def _record_rotation_probe_failure(
        self,
        proxy_id: int,
        probe: _ProxyProbeResult,
        reason: str,
        *,
        actor: str,
    ) -> None:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            endpoint.last_checked_at = utc_now()
            endpoint.latency_ms = probe.latency_ms if probe.healthy else None
            endpoint.observed_ip = probe.observed_ip
            endpoint.last_error = reason[:500]
            if probe.healthy:
                endpoint.status = ProxyStatus.HEALTHY
                endpoint.consecutive_failures = 0
            else:
                endpoint.failure_count += 1
                endpoint.consecutive_failures += 1
                endpoint.status = (
                    ProxyStatus.QUARANTINED
                    if endpoint.consecutive_failures
                    >= self._settings.proxy_quarantine_after_failures
                    else ProxyStatus.UNHEALTHY
                )
            add_audit_event(
                session,
                event_type="proxy.rotation_unconfirmed",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"error": reason[:200], "proxy_healthy": probe.healthy},
                actor=actor,
            )
            await session.commit()

    async def _probe_proxy_url(self, proxy_url: str) -> _ProxyProbeResult:
        started = time.perf_counter()
        try:
            timeout = httpx.Timeout(self._settings.proxy_check_timeout_seconds)
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=timeout,
                trust_env=False,
            ) as client:
                response = await client.get(self._settings.proxy_check_url)
                response.raise_for_status()
                observed_ip = self._extract_ip(response)
                return _ProxyProbeResult(
                    healthy=True,
                    latency_ms=round((time.perf_counter() - started) * 1000),
                    observed_ip=observed_ip,
                    error=None,
                )
        except (httpx.HTTPError, OSError, ValueError) as exc:
            return _ProxyProbeResult(
                healthy=False,
                latency_ms=None,
                observed_ip=None,
                error=f"{type(exc).__name__}: соединение через прокси не установлено",
            )

    async def _probe_mtproto_proxy(
        self,
        host: str,
        port: int,
        secret: str,
    ) -> _ProxyProbeResult:
        """Prove an MTProto route with Telegram itself, not an unrelated HTTP URL.

        MTProxy does not transport HTTP traffic, so testing it through ipify
        would falsely mark every valid route as broken.  This opens a disposable
        unauthorised Telethon connection, performs no API calls and disconnects
        immediately after Telegram accepts the transport.
        """

        api = self._settings.telegram_api
        if api is None:
            return _ProxyProbeResult(
                healthy=False,
                latency_ms=None,
                observed_ip=None,
                error="не настроены Telegram API-креды для проверки MTProto",
            )

        from telethon import TelegramClient  # noqa: PLC0415 - optional heavy dependency
        from telethon.sessions import StringSession  # noqa: PLC0415

        client = None
        started = time.perf_counter()
        try:
            client = TelegramClient(
                StringSession(),
                api[0],
                api[1],
                connection=mtproto_connection_type(secret),
                proxy=(host, int(port), secret),
                connection_retries=0,
                request_retries=0,
                auto_reconnect=False,
                timeout=max(1, int(self._settings.proxy_check_timeout_seconds)),
                receive_updates=False,
                base_logger=_mtproto_probe_logger(),
            )
            await asyncio.wait_for(
                client.connect(),
                timeout=self._settings.proxy_check_timeout_seconds,
            )
            return _ProxyProbeResult(
                healthy=True,
                latency_ms=round((time.perf_counter() - started) * 1000),
                observed_ip=None,
                error=None,
            )
        except Exception as exc:  # noqa: BLE001 - turn transport failures into a stable UI status
            return _ProxyProbeResult(
                healthy=False,
                latency_ms=None,
                observed_ip=None,
                error=(f"{type(exc).__name__}: MTProto-соединение с Telegram не установлено"),
            )
        finally:
            if client is not None:
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=2)
                except (Exception, asyncio.CancelledError):  # pragma: no cover - cleanup only
                    pass

    async def update_proxy_metadata(
        self,
        proxy_id: int,
        *,
        label: str,
        group_name: str | None,
        actor: str = "control-bot",
    ) -> ProxyView:
        normalized_label = label.strip()
        if not normalized_label or len(normalized_label) > 100:
            raise ProxyInputError("метка должна содержать от 1 до 100 символов")
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            group = await self._find_or_create_group(session, group_name)
            endpoint.label = normalized_label
            endpoint.group_id = group.id if group else None
            add_audit_event(
                session,
                event_type="proxy.metadata_updated",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"label": normalized_label, "group": group_name},
                actor=actor,
            )
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def assign_proxy(
        self,
        proxy_id: int,
        assignment_key: str,
        purpose: str = "",
        *,
        actor: str = "control-bot",
    ) -> ProxyView:
        normalized_key = assignment_key.strip()
        if not normalized_key or len(normalized_key) > 150:
            raise ProxyInputError("ключ слота должен содержать от 1 до 150 символов")
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            if not endpoint.enabled or endpoint.status in {
                ProxyStatus.DISABLED,
                ProxyStatus.UNHEALTHY,
                ProxyStatus.QUARANTINED,
            }:
                raise ConflictError("выключенный или проблемный прокси нельзя назначить")
            session.add(
                ProxyAssignment(
                    proxy_id=proxy_id,
                    assignment_key=normalized_key,
                    purpose=purpose.strip()[:250],
                )
            )
            try:
                await session.flush()
            except IntegrityError as exc:
                await session.rollback()
                raise ConflictError("этот слот уже занят") from exc
            add_audit_event(
                session,
                event_type="proxy.assigned",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"assignment_key": normalized_key, "purpose": purpose.strip()[:250]},
                actor=actor,
            )
            await self._bump_account_connection_generations(session, [normalized_key])
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def release_assignment_key(
        self, assignment_key: str, *, actor: str = "control-bot"
    ) -> bool:
        """Release whatever proxy is bound to ``assignment_key`` (health-agnostic)."""
        normalized_key = assignment_key.strip()
        async with self._session_factory() as session:
            assignment = await session.scalar(
                select(ProxyAssignment).where(ProxyAssignment.assignment_key == normalized_key)
            )
            if assignment is None:
                return False
            proxy_id = assignment.proxy_id
            await session.delete(assignment)
            add_audit_event(
                session,
                event_type="proxy.released",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"assignment_key": normalized_key},
                actor=actor,
            )
            await self._bump_account_connection_generations(session, [normalized_key])
            await session.commit()
            return True

    async def move_assignment(
        self,
        source_key: str,
        target_key: str,
        *,
        purpose: str = "",
        actor: str = "control-bot",
    ) -> ProxyView:
        """Atomically rename a reserved proxy slot without exposing a direct-route gap."""
        normalized_source = source_key.strip()
        normalized_target = target_key.strip()
        if not normalized_source or not normalized_target:
            raise ProxyInputError("ключи слотов прокси не могут быть пустыми")
        if len(normalized_target) > 150:
            raise ProxyInputError("ключ слота должен содержать от 1 до 150 символов")
        async with self._session_factory() as session:
            assignment = await session.scalar(
                select(ProxyAssignment).where(ProxyAssignment.assignment_key == normalized_source)
            )
            if assignment is None:
                raise NotFoundError("временное назначение прокси не найдено")
            occupied = await session.scalar(
                select(ProxyAssignment.id).where(
                    ProxyAssignment.assignment_key == normalized_target,
                    ProxyAssignment.id != assignment.id,
                )
            )
            if occupied is not None:
                raise ConflictError("целевой слот прокси уже занят")
            proxy_id = assignment.proxy_id
            assignment.assignment_key = normalized_target
            assignment.purpose = purpose.strip()[:250]
            add_audit_event(
                session,
                event_type="proxy.assignment_moved",
                entity_type="proxy",
                entity_id=proxy_id,
                details={
                    "source_key": normalized_source,
                    "target_key": normalized_target,
                    "purpose": assignment.purpose,
                },
                actor=actor,
            )
            await self._bump_account_connection_generations(
                session,
                [normalized_source, normalized_target],
            )
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def release_proxy(self, proxy_id: int, *, actor: str = "control-bot") -> ProxyView:
        async with self._session_factory() as session:
            assignments = list(
                await session.scalars(
                    select(ProxyAssignment).where(ProxyAssignment.proxy_id == proxy_id)
                )
            )
            if not assignments:
                raise NotFoundError("назначение прокси не найдено")
            assignment_keys = [assignment.assignment_key for assignment in assignments]
            for assignment in assignments:
                await session.delete(assignment)
            add_audit_event(
                session,
                event_type="proxy.released",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"assignment_keys": assignment_keys},
                actor=actor,
            )
            await self._bump_account_connection_generations(session, assignment_keys)
            await session.commit()
        return await self.get_proxy(proxy_id)

    async def delete_proxy(self, proxy_id: int, *, actor: str = "control-bot") -> None:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            assigned = await session.scalar(
                select(ProxyAssignment.id).where(ProxyAssignment.proxy_id == proxy_id)
            )
            if assigned is not None:
                raise ConflictError("перед удалением освободите назначение прокси")
            add_audit_event(
                session,
                event_type="proxy.deleted",
                entity_type="proxy",
                entity_id=proxy_id,
                details={"endpoint": f"{endpoint.host}:{endpoint.port}"},
                actor=actor,
            )
            await session.delete(endpoint)
            await session.commit()

    async def check_proxy(self, proxy_id: int, *, actor: str = "control-bot") -> ProxyCheckResult:
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            if not endpoint.enabled:
                raise ConflictError("перед проверкой включите прокси")
            try:
                password = self._cipher.decrypt(endpoint.password_encrypted)
                is_mtproto = endpoint.protocol == ProxyProtocol.MTPROTO
                proxy_url = None if is_mtproto else self._build_url(endpoint, password)
            except ProxyInputError:
                is_mtproto = False
                proxy_url = None

        if is_mtproto and password:
            probe = await self._probe_mtproto_proxy(endpoint.host, endpoint.port, password)
        elif proxy_url is None:
            probe = _ProxyProbeResult(
                healthy=False,
                latency_ms=None,
                observed_ip=None,
                error="ProxyInputError: зашифрованные учётные данные недоступны",
            )
        else:
            probe = await self._probe_proxy_url(proxy_url)

        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
            previous_status = endpoint.status
            previous_ip = endpoint.observed_ip
            endpoint.last_checked_at = utc_now()
            if probe.healthy:
                endpoint.status = ProxyStatus.HEALTHY
                endpoint.success_count += 1
                endpoint.consecutive_failures = 0
                endpoint.latency_ms = probe.latency_ms
                endpoint.observed_ip = probe.observed_ip
                endpoint.last_error = None
            else:
                endpoint.failure_count += 1
                endpoint.consecutive_failures += 1
                endpoint.latency_ms = None
                endpoint.last_error = probe.error
                endpoint.status = (
                    ProxyStatus.QUARANTINED
                    if endpoint.consecutive_failures
                    >= self._settings.proxy_quarantine_after_failures
                    else ProxyStatus.UNHEALTHY
                )
            add_audit_event(
                session,
                event_type=("proxy.check_succeeded" if probe.healthy else "proxy.check_failed"),
                entity_type="proxy",
                entity_id=proxy_id,
                details={
                    "latency_ms": probe.latency_ms if probe.healthy else None,
                    "status": endpoint.status.value,
                },
                actor=actor,
            )
            if endpoint.status != previous_status or endpoint.observed_ip != previous_ip:
                await self._bump_proxy_account_generations(session, proxy_id)
            await session.commit()
            status = endpoint.status
        return ProxyCheckResult(
            proxy_id,
            probe.healthy,
            status,
            probe.latency_ms if probe.healthy else None,
            probe.observed_ip,
            probe.error,
        )

    async def check_all(
        self, *, concurrency: int = 10, actor: str = "control-bot"
    ) -> list[ProxyCheckResult]:
        async with self._session_factory() as session:
            ids = list(
                await session.scalars(
                    select(ProxyEndpoint.id)
                    .where(ProxyEndpoint.enabled.is_(True))
                    .order_by(ProxyEndpoint.id)
                )
            )
        semaphore = asyncio.Semaphore(max(1, min(concurrency, 25)))

        async def guarded(proxy_id: int) -> ProxyCheckResult:
            async with semaphore:
                return await self.check_proxy(proxy_id, actor=actor)

        return list(await asyncio.gather(*(guarded(proxy_id) for proxy_id in ids)))

    @staticmethod
    async def _bump_account_connection_generations(
        session: AsyncSession,
        assignment_keys: list[str] | tuple[str, ...],
    ) -> None:
        account_ids: set[int] = set()
        for assignment_key in assignment_keys:
            if not assignment_key.startswith("account:"):
                continue
            try:
                account_id = int(assignment_key.split(":", 1)[1])
            except ValueError:
                continue
            if account_id > 0:
                account_ids.add(account_id)
        if account_ids:
            await session.execute(
                update(Account)
                .where(Account.id.in_(account_ids))
                .values(connection_generation=Account.connection_generation + 1)
            )

    async def _bump_proxy_account_generations(
        self,
        session: AsyncSession,
        proxy_id: int,
    ) -> None:
        assignment_keys = list(
            await session.scalars(
                select(ProxyAssignment.assignment_key).where(ProxyAssignment.proxy_id == proxy_id)
            )
        )
        await self._bump_account_connection_generations(session, assignment_keys)

    async def _find_or_create_group(
        self, session: AsyncSession, group_name: str | None
    ) -> ProxyGroup | None:
        if group_name is None or not group_name.strip():
            return None
        normalized = group_name.strip()
        if len(normalized) > 100:
            raise ProxyInputError("имя группы не должно быть длиннее 100 символов")
        group = await session.scalar(select(ProxyGroup).where(ProxyGroup.name == normalized))
        if group is None:
            group = ProxyGroup(name=normalized)
            session.add(group)
            await session.flush()
        return group

    @staticmethod
    async def _count_endpoints(session: AsyncSession, *conditions: object) -> int:
        return int(
            await session.scalar(select(func.count()).select_from(ProxyEndpoint).where(*conditions))
            or 0
        )

    @staticmethod
    def _view_statement():
        return (
            select(
                ProxyEndpoint,
                ProxyGroup.name.label("group_name"),
                func.group_concat(ProxyAssignment.assignment_key, "\n").label("assignment_keys"),
                func.group_concat(ProxyAssignment.purpose, "\n").label("assignment_purposes"),
            )
            .outerjoin(ProxyGroup, ProxyGroup.id == ProxyEndpoint.group_id)
            .outerjoin(ProxyAssignment, ProxyAssignment.proxy_id == ProxyEndpoint.id)
            .group_by(ProxyEndpoint.id, ProxyGroup.name)
        )

    @staticmethod
    def _row_to_view(row: object) -> ProxyView:
        assignment_keys = _split_group_concat(row[2])  # type: ignore[index]
        assignment_purposes = _split_assignment_purposes(  # type: ignore[index]
            row[3], len(assignment_keys)
        )
        return ProxyView(
            proxy=row[0],  # type: ignore[index]
            group_name=row[1],  # type: ignore[index]
            assignment_keys=assignment_keys,
            assignment_purposes=assignment_purposes,
        )

    @staticmethod
    def _build_url(endpoint: ProxyEndpoint, password: str | None) -> str:
        auth = ""
        if endpoint.username is not None:
            auth = quote(endpoint.username, safe="")
            if password is not None:
                auth += f":{quote(password, safe='')}"
            auth += "@"
        host = f"[{endpoint.host}]" if ":" in endpoint.host else endpoint.host
        return f"{endpoint.protocol.value}://{auth}{host}:{endpoint.port}"

    @staticmethod
    def _extract_ip(response: httpx.Response) -> str | None:
        raw = response.text.strip()
        try:
            payload = response.json()
            if isinstance(payload, dict):
                raw = str(payload.get("ip", raw)).strip()
        except json.JSONDecodeError:
            pass
        try:
            return str(ipaddress.ip_address(raw))
        except ValueError:
            return None
