import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from proxima_traffic.accounts import AccountProfileError
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    AccountStatus,
    AuditEvent,
    ProxyEndpoint,
    ProxyStatus,
)
from proxima_traffic.profile_names import ProfileName
from proxima_traffic.proxies import (
    ProxyFilter,
    ProxyInputError,
    ProxyRouteUnavailable,
    _ProxyProbeResult,
    parse_proxy,
)
from proxima_traffic.services import ConflictError, NotFoundError


def test_proxy_parser_supports_common_formats_and_rejects_unsafe_values() -> None:
    uri = parse_proxy("socks5://alice:s3cret@127.0.0.1:1080")
    assert uri.protocol.value == "socks5"
    assert uri.username == "alice"
    assert uri.password == "s3cret"
    assert uri.masked_endpoint == "socks5://alice:***@127.0.0.1:1080"

    compact = parse_proxy("proxy.example:8080:bob:p%40ss")
    assert compact.protocol.value == "http"
    assert compact.host == "proxy.example"
    assert compact.username == "bob"

    with pytest.raises(ProxyInputError, match="неподдерживаемый"):
        parse_proxy("ftp://proxy.example:21")
    with pytest.raises(ProxyInputError, match="path"):
        parse_proxy("http://proxy.example:8080/path")


async def test_proxy_lifecycle_encrypts_secrets_and_enforces_assignments(app: FastAPI) -> None:
    service = app.state.proxy_service
    created = await service.add_proxy(
        "http://alice:plain-secret@127.0.0.1:1",
        label="Local test",
        group_name="EU pool",
    )
    assert created.masked_endpoint == "http://alice:***@127.0.0.1:1"
    assert created.group_name == "EU pool"

    async with app.state.database.session_factory() as session:
        stored = await session.scalar(
            select(ProxyEndpoint).where(ProxyEndpoint.id == created.proxy.id)
        )
        assert stored is not None
        assert stored.password_encrypted != "plain-secret"
        assert "plain-secret" not in (stored.password_encrypted or "")

    with pytest.raises(ConflictError, match="уже существует"):
        await service.add_proxy("http://alice:changed@127.0.0.1:1")

    result = await service.import_proxies(
        [
            "10.0.0.1:8000:user:password",
            "10.0.0.1:8000:user:password",
            "broken",
            "# ignored",
        ],
        group_name="Batch",
    )
    assert (result.added, result.skipped, result.invalid) == (1, 1, 1)

    assigned = await service.assign_proxy(created.proxy.id, "worker-eu-01", "reserved test")
    assert assigned.assignment_key == "worker-eu-01"
    shared = await service.assign_proxy(created.proxy.id, "worker-eu-02")
    assert shared.assignment_count == 2
    with pytest.raises(ConflictError, match="слот уже занят"):
        await service.assign_proxy(created.proxy.id, "worker-eu-02")
    await service.release_proxy(created.proxy.id)
    await service.delete_proxy(created.proxy.id)
    with pytest.raises(NotFoundError, match="не найден"):
        await service.get_proxy(created.proxy.id)


async def test_proxy_route_changes_bump_the_account_connection_generation(
    app: FastAPI,
) -> None:
    service = app.state.proxy_service
    async with app.state.database.session_factory() as session:
        account = Account(label="generation-account", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    assert account.id is not None
    created = await service.add_proxy("http://10.30.0.1:8080", label="Generation")
    async with app.state.database.session_factory() as session:
        endpoint = await session.get(ProxyEndpoint, created.proxy.id)
        assert endpoint is not None
        endpoint.status = ProxyStatus.HEALTHY
        await session.commit()

    await service.assign_proxy(created.proxy.id, f"account:{account.id}")
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, account.id)
        assert stored is not None and stored.connection_generation == 1

    await service.replace_proxy_url(created.proxy.id, "http://10.30.0.2:8080")
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, account.id)
        assert stored is not None and stored.connection_generation == 2

    await service.release_assignment_key(f"account:{account.id}")
    async with app.state.database.session_factory() as session:
        stored = await session.get(Account, account.id)
        assert stored is not None and stored.connection_generation == 3


async def test_failed_checks_move_proxy_to_quarantine(app: FastAPI) -> None:
    service = app.state.proxy_service
    view = await service.add_proxy("http://127.0.0.1:1", label="Offline")
    for expected_status in (
        ProxyStatus.UNHEALTHY,
        ProxyStatus.UNHEALTHY,
        ProxyStatus.QUARANTINED,
    ):
        result = await service.check_proxy(view.proxy.id)
        assert result.healthy is False
        assert result.status == expected_status
    stored = await service.get_proxy(view.proxy.id)
    assert stored.proxy.consecutive_failures == 3
    with pytest.raises(ConflictError, match="проблемный"):
        await service.assign_proxy(view.proxy.id, "blocked-slot")


async def test_proxy_filters_metadata_rotation_and_auto_allocation(app: FastAPI) -> None:
    service = app.state.proxy_service
    slower = await service.add_proxy("http://10.10.0.1:8000", group_name="Auto pool")
    faster = await service.add_proxy("http://10.10.0.2:8000", group_name="Auto pool")
    async with app.state.database.session_factory() as session:
        first = await session.get(ProxyEndpoint, slower.proxy.id)
        second = await session.get(ProxyEndpoint, faster.proxy.id)
        assert first is not None and second is not None
        first.status = ProxyStatus.HEALTHY
        first.latency_ms = 250
        second.status = ProxyStatus.HEALTHY
        second.latency_ms = 40
        await session.commit()

    allocated = await service.allocate_proxy(
        "auto-slot", "fastest endpoint", group_name="Auto pool"
    )
    assert allocated.proxy.id == faster.proxy.id
    moved = await service.move_assignment(
        "auto-slot",
        "account:55",
        purpose="account #55",
    )
    assert moved.assignment_key == "account:55"
    assert moved.assignment_purposes == ("account #55",)
    assert await service.proxy_for_assignment("auto-slot") is None
    assert await service.proxy_for_assignment("account:55") is not None
    assigned = await service.list_proxies(0, 10, filter_name=ProxyFilter.ASSIGNED)
    free = await service.list_proxies(0, 10, filter_name=ProxyFilter.FREE)
    assert [item.proxy.id for item in assigned.items] == [faster.proxy.id]
    assert [item.proxy.id for item in free.items] == [slower.proxy.id]

    updated = await service.update_proxy_metadata(
        slower.proxy.id, label="Backup EU", group_name="Backup pool"
    )
    assert updated.proxy.label == "Backup EU"
    assert updated.group_name == "Backup pool"
    rotated = await service.replace_proxy_url(
        slower.proxy.id, "socks5://new-user:new-password@10.10.0.3:1080"
    )
    assert rotated.masked_endpoint == "socks5://new-user:***@10.10.0.3:1080"
    assert rotated.proxy.status == ProxyStatus.UNKNOWN


async def test_shared_proxy_keeps_blank_assignment_purposes_aligned(app: FastAPI) -> None:
    service = app.state.proxy_service
    created = await service.add_proxy("http://10.20.0.1:8080")
    await service.assign_proxy(created.proxy.id, "account:1", "")
    await service.assign_proxy(created.proxy.id, "account:2", "second account")

    view = await service.get_proxy(created.proxy.id)

    assert view.assignment_keys == ("account:1", "account:2")
    assert view.assignment_purposes == ("", "second account")


async def test_unhealthy_proxy_cannot_be_assigned(app: FastAPI) -> None:
    service = app.state.proxy_service
    created = await service.add_proxy("http://10.20.0.2:8080")
    async with app.state.database.session_factory() as session:
        endpoint = await session.get(ProxyEndpoint, created.proxy.id)
        assert endpoint is not None
        endpoint.status = ProxyStatus.UNHEALTHY
        await session.commit()

    with pytest.raises(ConflictError, match="проблемный"):
        await service.assign_proxy(created.proxy.id, "account:3")


async def test_proxy_rotation_url_is_encrypted_and_triggers_provider_link(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        async def __aenter__(self) -> "FakeAsyncClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, url: str) -> object:
            calls.append(url)
            raise httpx.ReadTimeout(
                "provider rebooted before responding",
                request=httpx.Request("GET", url),
            )

    async def fake_probe(proxy_url: str) -> _ProxyProbeResult:
        assert "127.0.0.1:8080" in proxy_url
        return _ProxyProbeResult(True, 120, "203.0.113.11", None)

    monkeypatch.setattr("proxima_traffic.proxies.httpx.AsyncClient", FakeAsyncClient)
    service = app.state.proxy_service
    monkeypatch.setattr(service, "_probe_proxy_url", fake_probe)
    rotation_url = "https://mobile.example/rotate?token=secret-token"
    created = await service.add_proxy("http://127.0.0.1:8080", rotation_url=rotation_url)
    assert created.has_rotation_url is True

    async with app.state.database.session_factory() as session:
        endpoint = await session.get(ProxyEndpoint, created.proxy.id)
        assert endpoint is not None
        assert endpoint.rotation_url_encrypted is not None
        assert rotation_url not in endpoint.rotation_url_encrypted
        endpoint.status = ProxyStatus.HEALTHY
        endpoint.observed_ip = "203.0.113.10"
        await session.commit()

    rotated = await service.rotate_proxy(created.proxy.id)
    assert calls == [rotation_url]
    assert rotated.proxy.status == ProxyStatus.HEALTHY
    assert rotated.proxy.observed_ip == "203.0.113.11"
    assert rotated.proxy.last_rotated_at is not None

    cleared = await service.set_proxy_rotation_url(created.proxy.id, None)
    assert cleared.has_rotation_url is False
    with pytest.raises(ConflictError, match="ссылка ротации"):
        await service.rotate_proxy(created.proxy.id)


async def test_account_route_is_fail_closed_and_uses_remote_dns(app: FastAPI) -> None:
    service = app.state.proxy_service

    with pytest.raises(ProxyRouteUnavailable, match="прямое соединение запрещено"):
        await service.require_account_route(42)

    view = await service.add_proxy(
        "socks5://alice:p%40ss@proxy.example:1080",
        label="Account route",
    )
    await service.assign_proxy(view.proxy.id, "account:42", "Telegram account #42")
    second_assignment = await service.assign_proxy(
        view.proxy.id, "account:43", "Telegram account #43"
    )
    assert second_assignment.assignment_count == 2
    listed = await service.list_proxies(0, 10, filter_name=ProxyFilter.ASSIGNED)
    assert [item.proxy.id for item in listed.items] == [view.proxy.id]
    assert listed.items[0].assignment_count == 2
    assigned_view = await service.proxy_for_assignment("account:42")
    assert assigned_view is not None
    assert assigned_view.proxy.id == view.proxy.id
    assert await service.proxy_for_assignment("") is None
    with pytest.raises(ProxyRouteUnavailable, match="не прошёл проверку"):
        await service.require_account_route(42)

    async with app.state.database.session_factory() as session:
        endpoint = await session.get(ProxyEndpoint, view.proxy.id)
        assert endpoint is not None
        endpoint.status = ProxyStatus.HEALTHY
        await session.commit()

    route = await service.require_account_route(42)
    second_route = await service.require_account_route(43)
    assert route.transport_protocol == "socks5h"
    assert route.connection_url == "socks5h://alice:p%40ss@proxy.example:1080"
    assert second_route.connection_url == route.connection_url
    assert "p@ss" not in repr(route)
    assert route.masked_endpoint == "socks5h://alice:***@proxy.example:1080"


async def test_deleting_account_releases_its_proxy_slot(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="delete-with-proxy", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
        account_id = account.id

    proxy_service = app.state.proxy_service
    proxy = await proxy_service.add_proxy("http://127.0.0.1:8088")
    await proxy_service.assign_proxy(proxy.proxy.id, f"account:{account_id}", "temporary")
    assert await proxy_service.account_proxy_endpoint(account_id) == "http://127.0.0.1:8088"

    await app.state.account_service.delete_account(account_id)

    assert await proxy_service.account_proxy_endpoint(account_id) is None
    released = await proxy_service.get_proxy(proxy.proxy.id)
    assert released.assignment_key is None


async def test_clearing_account_session_preserves_history_row_and_revokes_access(
    app: FastAPI,
) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="clear-session",
            ownership_confirmed=True,
            session_source=AccountSessionSource.TDATA,
            session_encrypted="encrypted-session",
            api_id=12345,
            api_hash_encrypted="encrypted-hash",
            username="private_name",
            authorized=True,
        )
        session.add(account)
        await session.commit()
        account_id = account.id

    proxy = await app.state.proxy_service.add_proxy("http://127.0.0.1:8090")
    async with app.state.database.session_factory() as session:
        endpoint = await session.get(ProxyEndpoint, proxy.proxy.id)
        endpoint.status = ProxyStatus.HEALTHY
        await session.commit()
    await app.state.proxy_service.assign_proxy(proxy.proxy.id, f"account:{account_id}")

    cleared = await app.state.account_service.clear_session(account_id)

    assert cleared.status == AccountStatus.DISABLED
    assert cleared.session_source == AccountSessionSource.NONE
    assert cleared.session_encrypted is None
    assert cleared.api_id is None
    assert cleared.api_hash_encrypted is None
    assert cleared.username is None
    assert cleared.authorized is False
    assert await app.state.proxy_service.proxy_for_assignment(f"account:{account_id}") is None


async def test_account_profile_name_and_avatar_operations_are_persisted(
    app: FastAPI,
    monkeypatch,
) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="editable-profile",
            ownership_confirmed=True,
            session_source=AccountSessionSource.TDATA,
            session_encrypted="encrypted-session",
            api_id=12345,
            api_hash_encrypted="encrypted-hash",
            authorized=True,
        )
        session.add(account)
        await session.commit()
        account_id = account.id

    requests: list[object] = []
    uploaded_files: list[tuple[bytes, str]] = []
    me = SimpleNamespace(
        first_name="Alice",
        last_name="Operator",
        username="alice_operator",
        phone="123",
        photo=object(),
    )

    class FakeClient:
        async def __call__(self, request):
            requests.append(request)
            return None

        async def upload_file(self, payload: bytes, *, file_name: str):
            uploaded_files.append((payload, file_name))
            return object()

        async def get_profile_photos(self, entity: str):
            assert entity == "me"
            return []

    async def fake_profile_operation(profile_account_id: int, operation):
        assert profile_account_id == account_id
        result = await operation(FakeClient())
        return me, result

    monkeypatch.setattr(
        app.state.account_service,
        "_run_profile_operation",
        fake_profile_operation,
    )

    renamed = await app.state.account_service.update_profile_name(
        account_id,
        first_name="Alice",
        last_name="Operator",
    )
    assert type(requests[-1]).__name__ == "UpdateProfileRequest"
    assert requests[-1].first_name == "Alice"
    assert requests[-1].last_name == "Operator"
    assert renamed.first_name == "Alice"
    assert renamed.last_name == "Operator"

    with pytest.raises(AccountProfileError, match="JPG или PNG"):
        await app.state.account_service.update_profile_photo(
            account_id,
            photo_bytes=b"image",
            file_name="avatar.gif",
        )

    updated_photo = await app.state.account_service.update_profile_photo(
        account_id,
        photo_bytes=b"jpeg-image",
        file_name="portrait.jpg",
    )
    assert type(requests[-1]).__name__ == "UploadProfilePhotoRequest"
    assert uploaded_files == [(b"jpeg-image", "avatar.jpg")]
    assert updated_photo.has_profile_photo is True

    cleared_photo, deleted = await app.state.account_service.delete_profile_photos(account_id)
    assert deleted == 0
    assert cleared_photo.has_profile_photo is False


async def test_bulk_profile_names_continue_after_one_account_failure(
    app: FastAPI,
    monkeypatch,
) -> None:
    async def fake_update_profile_name(
        account_id: int,
        *,
        first_name: str,
        last_name: str = "",
        actor: str = "control-bot",
    ) -> Account:
        assert actor == "control-bot"
        if account_id == 2:
            raise AccountProfileError("session expired")
        return Account(
            id=account_id,
            label=f"account-{account_id}",
            first_name=first_name,
            last_name=last_name,
        )

    monkeypatch.setattr(
        app.state.account_service,
        "update_profile_name",
        fake_update_profile_name,
    )
    result = await app.state.account_service.bulk_update_profile_names(
        {
            1: ProfileName("Лера", "", "ru", "first"),
            2: ProfileName("Mia", "Stone", "en", "full"),
            3: ProfileName("Макс", "В.", "ru", "initial"),
        }
    )

    assert [account.id for account in result.updated] == [1, 3]
    assert [(failure.account_id, failure.error) for failure in result.failures] == [
        (2, "session expired")
    ]
    assert result.total == 3
    async with app.state.database.session_factory() as session:
        event = await session.scalar(
            select(AuditEvent).where(AuditEvent.event_type == "account.profile_names_bulk_updated")
        )
    assert event is not None
    assert json.loads(event.details_json) == {
        "failed": 1,
        "failed_account_ids": [2],
        "requested": 3,
        "updated": 2,
        "updated_account_ids": [1, 3],
    }


async def test_proxy_api_never_returns_password(
    app: FastAPI,
    client,
    admin_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/proxies",
        headers=admin_headers,
        json={
            "url": "http://api-user:api-password@proxy.example:8080",
            "label": "API proxy",
            "group_name": "API group",
        },
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["endpoint"] == "http://api-user:***@proxy.example:8080"
    assert "api-password" not in response.text

    proxy_id = payload["id"]
    assigned = await client.put(
        f"/api/v1/proxies/{proxy_id}/assignment",
        headers=admin_headers,
        json={"assignment_key": "api-slot", "purpose": "integration test"},
    )
    assert assigned.status_code == 200
    assert assigned.json()["assignment_key"] == "api-slot"

    listed = await client.get("/api/v1/proxies", headers=admin_headers)
    assert listed.status_code == 200
    assert "api-password" not in listed.text
    status = await client.get("/api/v1/status", headers=admin_headers)
    assert status.json()["proxies"] == 1


async def test_proxy_api_route_changes_close_assigned_account_clients(
    app: FastAPI,
    client,
    admin_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        async def __aenter__(self) -> "FakeAsyncClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, url: str) -> FakeResponse:
            calls.append(url)
            return FakeResponse()

    class FakeGateway:
        def __init__(self) -> None:
            self.closed_accounts: list[int] = []

        async def close_account(self, account_id: int) -> None:
            self.closed_accounts.append(account_id)

    monkeypatch.setattr("proxima_traffic.proxies.httpx.AsyncClient", FakeAsyncClient)
    fake_gateway = FakeGateway()
    app.state.gateway = fake_gateway

    async def fake_probe(proxy_url: str) -> _ProxyProbeResult:
        assert proxy_url.startswith("http://")
        return _ProxyProbeResult(True, 80, "203.0.113.20", None)

    monkeypatch.setattr(app.state.proxy_service, "_probe_proxy_url", fake_probe)
    rotation_url = "https://mobile.example/rotate?token=api-secret"
    created = await client.post(
        "/api/v1/proxies",
        headers=admin_headers,
        json={
            "url": "http://api-rotation:8080",
            "rotation_url": rotation_url,
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["has_rotation_url"] is True
    assert "api-secret" not in created.text

    assigned = await client.put(
        f"/api/v1/proxies/{body['id']}/assignment",
        headers=admin_headers,
        json={"assignment_key": "account:42", "purpose": "mobile account"},
    )
    assert assigned.status_code == 200
    assert fake_gateway.closed_accounts == [42]
    assigned_second = await client.put(
        f"/api/v1/proxies/{body['id']}/assignment",
        headers=admin_headers,
        json={"assignment_key": "account:43", "purpose": "mobile account 2"},
    )
    assert assigned_second.status_code == 200
    assert assigned_second.json()["assignment_count"] == 2
    assert fake_gateway.closed_accounts == [42, 43]
    fake_gateway.closed_accounts.clear()
    rotated = await client.post(
        f"/api/v1/proxies/{body['id']}/rotate",
        headers=admin_headers,
    )
    assert rotated.status_code == 200
    assert calls == [rotation_url]
    assert set(fake_gateway.closed_accounts) == {42, 43}
    assert rotated.json()["last_rotated_at"] is not None
    assert "api-secret" not in rotated.text

    fake_gateway.closed_accounts.clear()
    replaced = await client.put(
        f"/api/v1/proxies/{body['id']}/endpoint",
        headers=admin_headers,
        json={"url": "http://api-rotation-new:8081"},
    )
    assert replaced.status_code == 200
    assert set(fake_gateway.closed_accounts) == {42, 43}

    fake_gateway.closed_accounts.clear()
    toggled = await client.post(
        f"/api/v1/proxies/{body['id']}/toggle",
        headers=admin_headers,
    )
    assert toggled.status_code == 200
    assert set(fake_gateway.closed_accounts) == {42, 43}

    fake_gateway.closed_accounts.clear()
    released = await client.delete(
        f"/api/v1/proxies/{body['id']}/assignment",
        headers=admin_headers,
    )
    assert released.status_code == 200
    assert set(fake_gateway.closed_accounts) == {42, 43}
