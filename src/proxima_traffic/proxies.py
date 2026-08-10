from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import time
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import StrEnum
from urllib.parse import quote, unquote, urlsplit

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
        identity = f"{self.protocol.value}|{self.host.lower()}|{self.port}|{self.username or ''}"
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


def parse_proxy(value: str) -> ParsedProxy:
    raw = value.strip()
    if not raw or raw.startswith("#"):
        raise ProxyInputError("пустая строка")
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
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
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
        actor: str = "control-bot",
    ) -> ProxyView:
        normalized_key = assignment_key.strip()
        if not normalized_key or len(normalized_key) > 150:
            raise ProxyInputError("ключ слота должен содержать от 1 до 150 символов")
        async with self._session_factory() as session:
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
        async with self._session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, proxy_id)
            if endpoint is None:
                raise NotFoundError("прокси не найден")
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
            endpoint.status = ProxyStatus.UNKNOWN
            endpoint.latency_ms = None
            endpoint.last_error = None
            await self._bump_proxy_account_generations(session, proxy_id)
            await session.commit()
        request_succeeded = False
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
                request_succeeded = True
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
        if probe.healthy and (ip_changed or request_succeeded):
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
                proxy_url = self._build_url(endpoint, password)
            except ProxyInputError:
                proxy_url = None

        if proxy_url is None:
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
