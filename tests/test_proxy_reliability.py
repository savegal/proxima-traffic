"""Proxy failover/MTProto feed regression tests: all network calls are faked."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from proxima_traffic.accounts import build_telethon_transport
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    ProxyEndpoint,
    ProxyProtocol,
    ProxyStatus,
    utc_now,
)
from proxima_traffic.proxies import (
    ProxyInputError,
    ProxyNetworkRoute,
    _ProxyProbeResult,
    _validate_mtproto_secret,
    mtproto_connection_type,
    parse_proxy,
)
from proxima_traffic.proxy_feed import MTProtoFeedService, _unsafe_feed_host
from proxima_traffic.services import ConflictError

DD = "dd0123456789abcdef0123456789abcdef"
EE = "ee0123456789abcdef0123456789abcdef6578616d706c652e6f7267"


def test_mtproto_secret_validation_is_strict_but_accepts_real_base64() -> None:
    assert _validate_mtproto_secret(DD) == DD
    assert _validate_mtproto_secret(EE) == EE
    assert _validate_mtproto_secret("eeNEgYdJvXrFGRMCIMJdCQ") == (
        "79e344818749bd7ac519130220c25d09"
    )
    with pytest.raises(ProxyInputError):
        _validate_mtproto_secret("dd0123")
    with pytest.raises(ProxyInputError):
        _validate_mtproto_secret("a" * 62)
    with pytest.raises(ProxyInputError):
        _validate_mtproto_secret("definitely invalid secret!")


def test_fake_tls_does_not_pretend_to_be_intermediate() -> None:
    assert parse_proxy(f"tg://proxy?server=example.org&port=443&secret={EE}").password == EE
    with pytest.raises(ProxyInputError, match="Fake-TLS"):
        mtproto_connection_type(EE)


def test_feed_rejects_local_or_private_hostnames() -> None:
    assert _unsafe_feed_host("localhost") is True
    assert _unsafe_feed_host("127.0.0.1") is True
    assert _unsafe_feed_host("10.0.0.2") is True
    assert _unsafe_feed_host("169.254.169.254") is True
    assert _unsafe_feed_host("router.internal") is True
    assert _unsafe_feed_host("proxy.example.org") is False


@pytest.mark.asyncio
async def test_stale_probe_cannot_mark_replaced_proxy_healthy(app, monkeypatch):
    service = app.state.proxy_service

    async def initial_probe(url):
        return _ProxyProbeResult(True, 30, "198.51.100.1", None)

    monkeypatch.setattr(service, "_probe_proxy_url", initial_probe)
    entry = await service.add_proxy("http://old.example.org:8080")
    started, finish = asyncio.Event(), asyncio.Event()

    async def delayed_probe(url):
        started.set()
        await asyncio.wait_for(finish.wait(), 5)
        return _ProxyProbeResult(True, 10, "198.51.100.2", None)

    monkeypatch.setattr(service, "_probe_proxy_url", delayed_probe)
    task = asyncio.create_task(service.check_proxy(entry.proxy.id))
    await asyncio.wait_for(started.wait(), 5)
    await service.replace_proxy_url(entry.proxy.id, "http://new.example.org:8080")
    finish.set()
    checked = await asyncio.wait_for(task, 5)
    saved = await service.get_proxy(entry.proxy.id)
    assert checked.healthy is False
    assert "устарел" in checked.error
    assert saved.proxy.status == ProxyStatus.UNKNOWN
    assert saved.proxy.host == "new.example.org"
    assert saved.proxy.last_checked_at is None


@pytest.mark.asyncio
async def test_stale_probe_cannot_reenable_disabled_proxy(app, monkeypatch):
    service = app.state.proxy_service

    async def initial(url):
        return _ProxyProbeResult(True, 40, "198.51.100.3", None)

    monkeypatch.setattr(service, "_probe_proxy_url", initial)
    entry = await service.add_proxy("http://example.org:8080")
    started, finish = asyncio.Event(), asyncio.Event()

    async def blocked(url):
        started.set()
        await asyncio.wait_for(finish.wait(), 5)
        return _ProxyProbeResult(True, 10, "198.51.100.4", None)

    monkeypatch.setattr(service, "_probe_proxy_url", blocked)
    task = asyncio.create_task(service.check_proxy(entry.proxy.id))
    await asyncio.wait_for(started.wait(), 5)
    await service.toggle_proxy(entry.proxy.id)
    finish.set()
    result = await asyncio.wait_for(task, 5)
    saved = await service.get_proxy(entry.proxy.id)
    assert result.healthy is False
    assert saved.proxy.enabled is False
    assert saved.proxy.status == ProxyStatus.DISABLED


@pytest.mark.asyncio
async def test_confirmed_slow_route_can_switch_only_to_faster_verified_route(app, monkeypatch):
    service = app.state.proxy_service
    monkeypatch.setattr(service._settings, "proxy_slow_failover_enabled", True)

    async def probe(url):
        if "slow.example.org" in url:
            return _ProxyProbeResult(True, 9000, "198.51.100.5", None)
        return _ProxyProbeResult(True, 90, "198.51.100.6", None)

    monkeypatch.setattr(service, "_probe_proxy_url", probe)
    slow = await service.add_proxy("http://slow.example.org:8080")
    fast = await service.add_proxy("http://fast.example.org:8080")
    async with app.state.database.session_factory() as session:
        account = Account(
            label="slow-route-account", status=AccountStatus.ACTIVE,
            session_source=AccountSessionSource.SESSION_STRING,
        )
        session.add(account)
        await session.commit()
        account_id = account.id
    await service.assign_proxy(slow.proxy.id, f"account:{account_id}")
    route = await service.ensure_account_route(account_id)
    assert route.proxy_id == fast.proxy.id
    assert (await service.proxy_for_assignment(f"account:{account_id}")).proxy.id == fast.proxy.id


@pytest.mark.asyncio
async def test_feed_imports_only_verified_recent_public_mtproto(app, monkeypatch):
    settings = app.state.proxy_service._settings
    monkeypatch.setattr(settings, "dry_run", False)
    monkeypatch.setattr(settings, "account_require_proxy", True)
    service = app.state.proxy_service
    source = MTProtoFeedService(
        app.state.database.session_factory, app.state.account_service, service, settings
    )
    working = f"tg://proxy?server=working.example.org&port=443&secret={DD}"
    broken = f"tg://proxy?server=broken.example.org&port=443&secret={DD}"
    fake_tls = f"tg://proxy?server=faketls.example.org&port=443&secret={EE}"
    old = f"tg://proxy?server=stale.example.org&port=443&secret={DD}"
    posts = [
        SimpleNamespace(
            message="Connect", entities=[SimpleNamespace(url=working)], date=utc_now()
        ),
        SimpleNamespace(
            message=broken, entities=[], date=utc_now()
        ),
        SimpleNamespace(
            message=fake_tls, entities=[], date=utc_now()
        ),
        SimpleNamespace(
            message=working, entities=[], date=utc_now()
        ),
        SimpleNamespace(
            message=old, entities=[], date=utc_now() - timedelta(days=10)
        ),
    ]

    async def fake_messages():
        return 18, posts

    async def fake_probe(host, port, secret):
        return _ProxyProbeResult(
            host == "working.example.org", 37 if host == "working.example.org" else None,
            None, None if host == "working.example.org" else "offline"
        )

    monkeypatch.setattr(source, "_read_recent_messages", fake_messages)
    monkeypatch.setattr(service, "_probe_mtproto_proxy", fake_probe)
    first = await source.sync()
    assert first.account_id == 18
    assert first.valid_links == 3
    assert first.tested == 2
    assert first.unsupported_fake_tls == 1
    assert first.added == 1
    assert first.healthy_after_save == 1
    second = await source.sync()
    assert second.added == 0
    assert second.reused == 1
    async with app.state.database.session_factory() as session:
        proxies = list(await session.scalars(select(ProxyEndpoint)))
    assert len(proxies) == 1
    assert proxies[0].protocol == ProxyProtocol.MTPROTO
    assert proxies[0].status == ProxyStatus.HEALTHY
    assert proxies[0].password_encrypted is not None
    assert DD not in proxies[0].password_encrypted


@pytest.mark.asyncio
async def test_feed_refuses_to_open_direct_connection(app, monkeypatch):
    settings = app.state.proxy_service._settings
    monkeypatch.setattr(settings, "dry_run", False)
    monkeypatch.setattr(settings, "account_require_proxy", True)
    feed = MTProtoFeedService(
        app.state.database.session_factory, app.state.account_service,
        app.state.proxy_service, settings,
    )
    with pytest.raises(ConflictError, match="Нет авторизованного аккаунта"):
        await feed.sync()
    monkeypatch.setattr(settings, "account_require_proxy", False)
    with pytest.raises(ConflictError, match="прямое соединение запрещено"):
        await feed.sync()


@pytest.mark.asyncio
async def test_old_check_cannot_validate_new_password_on_same_proxy(app, monkeypatch):
    service = app.state.proxy_service

    async def healthy(url):
        return _ProxyProbeResult(True, 20, "198.51.100.7", None)

    monkeypatch.setattr(service, "_probe_proxy_url", healthy)
    entry = await service.add_proxy("http://alice:old-secret@proxy.example.org:8080")
    started, release = asyncio.Event(), asyncio.Event()

    async def waiting(url):
        started.set()
        await asyncio.wait_for(release.wait(), 5)
        return _ProxyProbeResult(True, 12, "198.51.100.8", None)

    monkeypatch.setattr(service, "_probe_proxy_url", waiting)
    task = asyncio.create_task(service.check_proxy(entry.proxy.id))
    await asyncio.wait_for(started.wait(), 5)
    await service.replace_proxy_url(
        entry.proxy.id, "http://alice:new-secret@proxy.example.org:8080"
    )
    release.set()
    result = await asyncio.wait_for(task, 5)
    saved = await service.get_proxy(entry.proxy.id)
    assert result.healthy is False
    assert saved.proxy.status == ProxyStatus.UNKNOWN
    assert saved.proxy.last_checked_at is None


def test_standard_routes_use_native_telethon_python_socks_mapping() -> None:
    route = ProxyNetworkRoute(
        proxy_id=1,
        protocol=ProxyProtocol.SOCKS5,
        host="proxy.example.org",
        port=1080,
        username="operator",
        password="redacted",
    )
    transport = build_telethon_transport(route)
    assert transport.connection is None
    assert transport.proxy == {
        "proxy_type": "socks5",
        "addr": "proxy.example.org",
        "port": 1080,
        "rdns": True,
        "username": "operator",
        "password": "redacted",
    }
    assert "redacted" not in repr(transport)


@pytest.mark.asyncio
async def test_https_proxy_needs_telegram_app_to_claim_transport_readiness(app):
    proxy_service = app.state.proxy_service
    assert proxy_service._settings.telegram_api is None
    assert await proxy_service._probe_telegram_using_standard_proxy(
        "https://proxy.example.org:443"
    ) is False
    assert await proxy_service._probe_telegram_using_standard_proxy(
        "socks5h://proxy.example.org:1080"
    ) is True
