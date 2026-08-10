from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from telethon import functions, types, utils

from proxima_traffic.accounts import AccountService
from proxima_traffic.config import Settings
from proxima_traffic.gateways import PermanentGatewayError, TemporaryGatewayError
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    Target,
    TargetAccountState,
    ensure_utc,
    utc_now,
)


def _channel(
    channel_id: int,
    title: str,
    *,
    megagroup: bool = False,
    broadcast: bool = False,
    username: str | None = None,
    left: bool = True,
    default_banned_rights: types.ChatBannedRights | None = None,
) -> types.Channel:
    return types.Channel(
        id=channel_id,
        title=title,
        photo=types.ChatPhotoEmpty(),
        date=None,
        megagroup=megagroup,
        broadcast=broadcast,
        username=username,
        left=left,
        access_hash=channel_id * 1000,
        default_banned_rights=default_banned_rights,
    )


async def test_folder_preview_imports_only_accessible_writable_groups() -> None:
    public_group = _channel(101, "Public group", megagroup=True, username="PublicRoom")
    private_group = _channel(102, "Private group", megagroup=True)
    broadcast = _channel(103, "News channel", broadcast=True, username="NewsFeed")
    restricted = _channel(
        104,
        "Read-only group",
        megagroup=True,
        default_banned_rights=types.ChatBannedRights(
            until_date=None,
            send_messages=True,
        ),
    )
    inaccessible = types.ChannelForbidden(
        id=105,
        access_hash=105000,
        title="Unavailable group",
        megagroup=True,
    )
    chats = [public_group, private_group, broadcast, restricted, inaccessible]
    peers = [types.PeerChannel(chat.id) for chat in chats]
    result = types.chatlists.ChatlistInvite(
        title=types.TextWithEntities("Work rooms", []),
        peers=peers,
        chats=chats,
        users=[],
    )
    seen_slugs: list[str] = []

    class PreviewClient:
        async def __call__(self, request: object) -> object:
            assert isinstance(request, functions.chatlists.CheckChatlistInviteRequest)
            seen_slugs.append(request.slug)
            return result

    service = object.__new__(AccountService)

    async def fake_operation(account_id: int, operation: object) -> tuple[object, object]:
        assert account_id == 7
        value = await operation(PreviewClient())  # type: ignore[operator]
        return object(), value

    service._run_account_operation = fake_operation  # type: ignore[method-assign]
    preview = await service.inspect_chat_folder(7, slug="FolderToken")

    assert seen_slugs == ["FolderToken"]
    assert preview.title == "Work rooms"
    assert [entry.title for entry in preview.importable_groups] == [
        "Public group",
        "Private group",
    ]
    assert preview.importable_groups[0].chat_ref == "@publicroom"
    assert preview.importable_groups[1].chat_ref == str(utils.get_peer_id(private_group))
    assert preview.skipped_channels == 1
    assert preview.restricted_groups == 1
    assert preview.inaccessible == 1


class _JoinAccounting:
    def __init__(self, wait_seconds: float) -> None:
        self.wait_seconds = wait_seconds
        self.limit_checks = 0
        self.recorded = 0

    async def join_wait_seconds(self, *args: object, **kwargs: object) -> float:
        del args, kwargs
        self.limit_checks += 1
        return self.wait_seconds

    async def record_join(self, account_id: int) -> None:
        assert account_id == 9
        self.recorded += 1


def _gateway(tmp_path, accounting: _JoinAccounting) -> TelethonGateway:
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'folders.db'}",
        admin_api_key="test-admin-key",
        dry_run=False,
        worker_enabled=False,
        account_join_daily_limit=1,
        account_join_min_interval_seconds=120,
    )
    gateway._account_service = accounting
    gateway._entity_cache = {}
    gateway._joined = set()
    return gateway


async def test_existing_membership_does_not_consume_or_check_join_limit(tmp_path) -> None:
    group = _channel(201, "Existing group", megagroup=True, left=False)

    class ExistingMemberClient:
        async def get_entity(self, reference: object) -> object:
            assert reference == "@existing_group"
            return group

        async def __call__(self, request: object) -> object:
            assert isinstance(request, functions.channels.GetParticipantRequest)
            return object()

    accounting = _JoinAccounting(wait_seconds=3600)
    gateway = _gateway(tmp_path, accounting)
    target = Target(id=1, chat_ref="@existing_group", title="Existing group")

    resolved = await gateway._join_and_resolve(ExistingMemberClient(), 9, target)

    assert resolved is group
    assert accounting.limit_checks == 0
    assert accounting.recorded == 0


async def test_existing_basic_group_does_not_use_channel_membership_rpc(tmp_path) -> None:
    group = types.Chat(
        id=202,
        title="Legacy group",
        photo=types.ChatPhotoEmpty(),
        participants_count=4,
        date=None,
        version=1,
        left=False,
    )

    class BasicGroupClient:
        async def get_entity(self, reference: object) -> object:
            assert reference == -202
            return group

        async def __call__(self, request: object) -> object:
            raise AssertionError(type(request).__name__)

    accounting = _JoinAccounting(wait_seconds=3600)
    gateway = _gateway(tmp_path, accounting)
    target = Target(id=3, chat_ref="-202", title="Legacy group")

    resolved = await gateway._join_and_resolve(BasicGroupClient(), 9, target)

    assert resolved is group
    assert accounting.limit_checks == 0


async def test_folder_target_joins_only_selected_group(tmp_path) -> None:
    group = _channel(301, "Folder group", megagroup=True, left=True)
    peer_id = utils.get_peer_id(group)
    invite = types.chatlists.ChatlistInvite(
        title=types.TextWithEntities("Folder", []),
        peers=[types.PeerChannel(group.id)],
        chats=[group],
        users=[],
    )
    requests: list[object] = []

    class FolderClient:
        async def __call__(self, request: object) -> object:
            requests.append(request)
            if isinstance(request, functions.chatlists.CheckChatlistInviteRequest):
                return invite
            if isinstance(request, functions.chatlists.JoinChatlistInviteRequest):
                return SimpleNamespace(chats=[group])
            raise AssertionError(type(request).__name__)

    accounting = _JoinAccounting(wait_seconds=0)
    gateway = _gateway(tmp_path, accounting)
    target = Target(
        id=2,
        chat_ref=str(peer_id),
        title="Folder group",
        chat_type="supergroup",
        folder_slug="FolderToken",
        telegram_peer_id=peer_id,
    )

    resolved = await gateway._join_and_resolve(FolderClient(), 9, target)

    assert resolved is group
    join_requests = [
        request
        for request in requests
        if isinstance(request, functions.chatlists.JoinChatlistInviteRequest)
    ]
    assert len(join_requests) == 1
    assert len(join_requests[0].peers) == 1
    assert accounting.limit_checks == 1
    assert accounting.recorded == 1


async def test_expired_folder_link_still_uses_an_already_joined_private_group(
    tmp_path,
) -> None:
    group = _channel(302, "Joined private group", megagroup=True, left=False)
    peer_id = utils.get_peer_id(group)

    class ExpiredInviteError(Exception):
        message = "INVITE_SLUG_EXPIRED"

    class ExpiredFolderClient:
        async def __call__(self, request: object) -> object:
            assert isinstance(request, functions.chatlists.CheckChatlistInviteRequest)
            raise ExpiredInviteError

        async def iter_dialogs(self):
            yield SimpleNamespace(entity=group)

    accounting = _JoinAccounting(wait_seconds=3600)
    gateway = _gateway(tmp_path, accounting)
    target = Target(
        id=4,
        chat_ref=str(peer_id),
        title="Joined private group",
        chat_type="supergroup",
        folder_slug="ExpiredToken",
        telegram_peer_id=peer_id,
    )

    resolved = await gateway._join_and_resolve(ExpiredFolderClient(), 9, target)

    assert resolved is group
    assert accounting.limit_checks == 0
    assert accounting.recorded == 0


def test_preflight_rejects_read_only_destinations() -> None:
    broadcast = _channel(401, "Read-only channel", broadcast=True)
    restricted_group = _channel(
        402,
        "Muted group",
        megagroup=True,
        default_banned_rights=types.ChatBannedRights(
            until_date=None,
            send_messages=True,
        ),
    )
    plain_restricted_group = _channel(
        403,
        "No plain messages",
        megagroup=True,
        default_banned_rights=types.ChatBannedRights(
            until_date=None,
            send_plain=True,
        ),
    )
    photo_restricted_group = _channel(
        404,
        "No photos",
        megagroup=True,
        default_banned_rights=types.ChatBannedRights(
            until_date=None,
            send_photos=True,
        ),
    )
    media_restricted_group = _channel(
        405,
        "No media",
        megagroup=True,
        default_banned_rights=types.ChatBannedRights(
            until_date=None,
            send_media=True,
        ),
    )

    with pytest.raises(PermanentGatewayError, match="канале"):
        TelethonGateway._ensure_can_send(broadcast)
    with pytest.raises(PermanentGatewayError, match="запрещена"):
        TelethonGateway._ensure_can_send(restricted_group)
    with pytest.raises(PermanentGatewayError, match="запрещена"):
        TelethonGateway._ensure_can_send(plain_restricted_group)
    TelethonGateway._ensure_can_send(photo_restricted_group)
    with pytest.raises(PermanentGatewayError) as photo_error:
        TelethonGateway._ensure_can_send(
            photo_restricted_group,
            media_kinds=frozenset({"photo"}),
        )
    assert photo_error.value.code == "content_forbidden"
    with pytest.raises(PermanentGatewayError) as contact_error:
        TelethonGateway._ensure_can_send(
            media_restricted_group,
            media_kinds=frozenset({"contact"}),
        )
    assert contact_error.value.code == "content_forbidden"


async def test_slow_mode_is_read_before_send_and_saved_per_account(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'slow-mode-state.db'}",
        admin_api_key="test-admin-key",
        dry_run=False,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    async with app.state.database.session_factory() as session:
        account = Account(
            label="slow-account",
            telegram_user_id=8801,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@slow_room",
            title="Slow room",
            manual_cooldown_seconds=600,
        )
        session.add_all([account, target])
        await session.commit()
        account_id, target_id = account.id, target.id

    next_send = utc_now() + timedelta(seconds=90)

    class SlowModeClient:
        async def __call__(self, request: object) -> object:
            assert isinstance(request, functions.channels.GetFullChannelRequest)
            return SimpleNamespace(
                full_chat=SimpleNamespace(
                    slowmode_seconds=120,
                    slowmode_next_send_date=int(next_send.timestamp()),
                )
            )

    gateway = TelethonGateway(app.state.database.session_factory, settings)
    group = _channel(501, "Slow room", megagroup=True, username="slow_room")

    with pytest.raises(TemporaryGatewayError) as raised:
        await gateway._respect_chat_slow_mode(SlowModeClient(), account_id, target_id, group)

    assert raised.value.code == "slow_mode"
    assert 1 <= int(raised.value.retry_after or 0) <= 90
    async with app.state.database.session_factory() as session:
        state = await session.scalar(select(TargetAccountState))
        assert state is not None
        assert state.account_id == account_id
        assert state.target_id == target_id
        assert state.slowmode_seconds == 120
        assert state.next_send_at is not None

    await gateway._record_chat_delivery(account_id, target_id, group, 991, 120)
    async with app.state.database.session_factory() as session:
        state = await session.scalar(select(TargetAccountState))
        stored_target = await session.get(Target, target_id)
        assert state is not None
        assert state.last_message_id == 991
        assert state.last_success_at is not None
        assert ensure_utc(state.next_send_at) >= ensure_utc(state.last_success_at) + timedelta(
            seconds=600
        )
        assert stored_target is not None
        assert stored_target.telegram_peer_id == utils.get_peer_id(group)

    class UnexpectedTelegramCall:
        async def __call__(self, request: object) -> object:
            del request
            raise AssertionError("saved chat timer must be checked before Telegram RPC")

    with pytest.raises(TemporaryGatewayError) as timer:
        await gateway._respect_chat_slow_mode(
            UnexpectedTelegramCall(), account_id, target_id, group
        )
    assert timer.value.code == "chat_timer"
    assert 590 <= int(timer.value.retry_after or 0) <= 600

    await gateway.close()
    await app.state.database.dispose()
