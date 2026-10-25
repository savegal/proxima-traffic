from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from proxima_traffic.blacklist_import import (
    MAX_BLACKLIST_IMPORT_BYTES,
    decode_blacklist_document,
    parse_blacklist_import,
    subscriber_blacklisted_condition,
    user_is_blacklisted,
)
from proxima_traffic.bot.handlers import add_direct_blacklist_ids
from proxima_traffic.direct_messages import DirectBlacklistAddResult, DirectMessageService
from proxima_traffic.gateways import PermanentGatewayError
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.mentions import load_mention_exclusion_user_ids
from proxima_traffic.models import (
    Account,
    DirectCampaign,
    DirectLaunch,
    DirectMessageTask,
    DirectMessageTaskStatus,
    MentionBlacklistEntry,
    MentionBlacklistUsernameEntry,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.services import ConflictError


def test_export_formats_and_case_insensitive_duplicates() -> None:
    ids = parse_blacklist_import("\ufeff46087621\r\n58606201\r\n46087621\n")
    names = parse_blacklist_import("@andriy_krush\r\n@angelcastleee\r\n@ANDRIY_KRUSH\n")
    assert ids.telegram_user_ids == (46087621, 58606201)
    assert ids.usernames == ()
    assert ids.duplicates == 1
    assert names.telegram_user_ids == ()
    assert names.usernames == ("andriy_krush", "angelcastleee")
    assert names.duplicates == 1
    assert parse_blacklist_import("42; @Alice_User, 43 bob_user").usernames == (
        "alice_user",
        "bob_user",
    )


@pytest.mark.parametrize("value", ["", "0", "-12", str(1 << 63), "@bad-name", "42\n@"])
def test_bad_entries_are_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_blacklist_import(value)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16"])
def test_document_encodings(encoding: str) -> None:
    content = "42\r\n@alice_user\r\n"
    decoded = decode_blacklist_document(content.encode(encoding))
    assert parse_blacklist_import(decoded).usernames == ("alice_user",)


def test_document_limits_and_invalid_encoding() -> None:
    with pytest.raises(ValueError, match="2 МБ"):
        decode_blacklist_document(b"1" * (MAX_BLACKLIST_IMPORT_BYTES + 1))
    with pytest.raises(ValueError, match="UTF-8"):
        decode_blacklist_document(b"\xff42")
    with pytest.raises(ValueError, match="100,000"):
        parse_blacklist_import("42\n" * 100_001)


@pytest.fixture
def direct_service(app) -> DirectMessageService:
    return DirectMessageService(
        app.state.database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="test-blacklist-import",
    )


async def test_bulk_import_and_reimport_exceed_500_entries(app, direct_service) -> None:
    ids = tuple(range(8_000_000_000_000_000, 8_000_000_000_001_352))
    names = tuple(f"blocked_{index}" for index in range(601))
    before = (await direct_service.dashboard()).blacklist_count
    result = await direct_service.add_blacklist_entries_bulk(ids, usernames=names)
    assert result.requested == result.added == 1_953
    assert result.already_present == 0
    repeated = await direct_service.add_blacklist_entries_bulk(ids, usernames=names)
    assert repeated.added == 0
    assert repeated.already_present == 1_953
    assert (await direct_service.dashboard()).blacklist_count == before + 1_953
    async with app.state.database.session_factory() as session:
        assert await session.get(MentionBlacklistEntry, ids[-1]) is not None
        assert await session.get(MentionBlacklistUsernameEntry, names[-1]) is not None


async def test_entire_bulk_input_is_validated_before_writing(app, direct_service) -> None:
    ids = tuple(range(8_000_000_000_000_000, 8_000_000_000_000_501))
    with pytest.raises(ConflictError):
        await direct_service.add_blacklist_entries_bulk((*ids, 0))
    with pytest.raises(ConflictError):
        await direct_service.add_blacklist_entries_bulk(ids, usernames=("@bad-name",))
    async with app.state.database.session_factory() as session:
        assert await session.get(MentionBlacklistEntry, ids[0]) is None


async def test_username_blocks_future_subscribers_and_preserves_id_only_audience(
    app, direct_service
) -> None:
    await direct_service.add_blacklist_entries_bulk((), usernames=("@BLOCKED_MEMBER",))
    async with app.state.database.session_factory() as session:
        session.add_all(
            [
                Subscriber(telegram_user_id=8_100_001, username="blocked_member"),
                Subscriber(telegram_user_id=8_100_002, username=None),
                Subscriber(telegram_user_id=8_100_003, username="allowed_member"),
            ]
        )
        await session.commit()
        allowed = set(
            await session.scalars(
                select(Subscriber.telegram_user_id).where(~subscriber_blacklisted_condition())
            )
        )
        assert allowed == {8_100_002, 8_100_003}
        assert await user_is_blacklisted(session, 8_100_001, "BLOCKED_MEMBER")
        assert not await user_is_blacklisted(session, 8_100_002, None)
        assert 8_100_001 in await load_mention_exclusion_user_ids(session)


async def test_username_import_stops_queued_messages_and_invites(app, direct_service) -> None:
    async with app.state.database.session_factory() as session:
        subscribers = [
            Subscriber(telegram_user_id=8_200_001, username="blocked_member"),
            Subscriber(telegram_user_id=8_200_002, username="allowed_member"),
        ]
        subscriber_list = SubscriberList(name="Blacklist import")
        session.add_all([subscriber_list, *subscribers])
        await session.flush()
        direct_campaign = DirectCampaign(
            name="DM", subscriber_list_id=subscriber_list.id, primary_text="Hello"
        )
        invite_campaign = SubscriberInviteCampaign(
            name="Invite",
            subscriber_list_id=subscriber_list.id,
            destination_ref="@owned_channel",
            destination_key="owned_channel",
        )
        session.add_all([direct_campaign, invite_campaign])
        await session.flush()
        direct_launch = DirectLaunch(
            campaign_id=direct_campaign.id,
            subscriber_list_id=subscriber_list.id,
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=2,
            contact_cooldown_days_snapshot=0,
            account_ids_snapshot_json="[]",
            primary_text_snapshot="Hello",
            consent_confirmed_at=utc_now(),
        )
        invite_launch = SubscriberInviteLaunch(
            campaign_id=invite_campaign.id,
            subscriber_list_id=subscriber_list.id,
            destination_ref_snapshot="@owned_channel",
            destination_key_snapshot="owned_channel",
            segment_snapshot=SubscriberSegment.ALL,
            recipient_limit_snapshot=2,
            contact_cooldown_days_snapshot=0,
            account_ids_snapshot_json="[]",
            consent_confirmed_at=utc_now(),
        )
        session.add_all([direct_launch, invite_launch])
        await session.flush()
        for subscriber in subscribers:
            subscriber.reserved_direct_launch_id = direct_launch.id
            subscriber.reserved_invite_launch_id = invite_launch.id
            session.add(
                DirectMessageTask(
                    launch_id=direct_launch.id,
                    subscriber_id=subscriber.id,
                    status=DirectMessageTaskStatus.QUEUED,
                )
            )
            session.add(
                SubscriberInviteTask(
                    launch_id=invite_launch.id,
                    subscriber_id=subscriber.id,
                    status=DirectMessageTaskStatus.QUEUED,
                )
            )
        await session.commit()
        blocked_id = subscribers[0].id
        allowed_id = subscribers[1].id
        invite_launch_id = invite_launch.id
    result = await direct_service.add_blacklist_entries_bulk((), usernames=("@Blocked_Member",))
    assert result.cancelled_tasks == 2
    assert result.cancelled_invite_tasks == 1
    assert result.affected_invite_launch_ids == (invite_launch_id,)
    async with app.state.database.session_factory() as session:
        blocked = await session.get(Subscriber, blocked_id)
        allowed = await session.get(Subscriber, allowed_id)
        assert blocked.reserved_direct_launch_id is None
        assert blocked.reserved_invite_launch_id is None
        assert allowed.reserved_direct_launch_id is not None
        assert allowed.reserved_invite_launch_id is not None
        direct_task = await session.scalar(
            select(DirectMessageTask).where(DirectMessageTask.subscriber_id == blocked_id)
        )
        invite_task = await session.scalar(
            select(SubscriberInviteTask).where(SubscriberInviteTask.subscriber_id == blocked_id)
        )
        assert direct_task.status == invite_task.status == DirectMessageTaskStatus.SKIPPED
        assert direct_task.error_code == invite_task.error_code == "recipient_blacklisted"


async def test_telegram_profile_matches_alias_and_persists_blocked_user_id(app, direct_service):
    await direct_service.add_blacklist_entries_bulk((), usernames=("@BLOCKED_MEMBER",))
    gateway = TelethonGateway(app.state.database.session_factory, app.state.settings)
    profile = SimpleNamespace(
        id=8_300_001,
        username="other_name",
        usernames=[SimpleNamespace(username="blocked_member", active=True)],
    )
    with pytest.raises(PermanentGatewayError) as caught:
        await gateway._ensure_profile_not_blacklisted(profile)
    assert caught.value.code == "recipient_blacklisted"
    async with app.state.database.session_factory() as session:
        assert await session.get(MentionBlacklistEntry, profile.id) is not None


async def test_participant_collection_and_old_snapshots_exclude_usernames(app, direct_service):
    await direct_service.add_blacklist_entries_bulk((), usernames=("@blocked_member",))
    gateway = TelethonGateway(app.state.database.session_factory, app.state.settings)
    profiles = [
        SimpleNamespace(id=8_400_001, username="blocked_member"),
        SimpleNamespace(id=8_400_002, username="allowed_member"),
    ]

    class Client:
        async def iter_participants(self, entity, limit):
            for profile in profiles:
                yield profile

        async def get_entity(self, user_id):
            return next(profile for profile in profiles if profile.id == user_id)

    client = Client()
    collected = await gateway._collect_mentionable_user_ids_from_entity(
        client, object(), exclude_user_id=None, limit=200
    )
    assert collected == (8_400_002,)

    @asynccontextmanager
    async def account_operation(account_id):
        yield

    gateway._account_operation = account_operation
    gateway._ensure_client = AsyncMock(return_value=client)
    # The snapshot was saved before this username was blacklisted. It also
    # contains one user that the sending account can no longer resolve.
    profiles.append(SimpleNamespace(id=8_400_003, username="later_blocked"))
    await direct_service.add_blacklist_entries_bulk((), usernames=("@later_blocked",))
    assert await gateway.filter_mentionable_user_ids(
        account=Account(id=1, label="sender"), user_ids=(8_400_001, 8_400_002, 8_400_003, 8_400_004)
    ) == (8_400_002,)


@pytest.mark.parametrize("origin", ["dm", "inv"])
async def test_bot_accepts_consecutive_txt_files_and_keeps_import_open(origin):
    state = SimpleNamespace(get_data=AsyncMock(return_value={"direct_blacklist_origin": origin}))
    messages = []

    async def download(document, destination):
        destination.write(document.content)

    bot = SimpleNamespace(download=AsyncMock(side_effect=download))
    service = SimpleNamespace(
        add_blacklist_entries_bulk=AsyncMock(return_value=DirectBlacklistAddResult(2, 2, 0, 0)),
        dashboard=AsyncMock(return_value=SimpleNamespace(blacklist_count=4)),
    )
    invites = SimpleNamespace(refresh_launches=AsyncMock())
    for filename, content in [
        ("blacklist_ids_20261005_084510.txt", b"46087621\r\n58606201\r\n"),
        ("blacklist_usernames_20261005_084510.txt", b"@andriy_krush\r\n@angelcastleee\r\n"),
    ]:
        message = SimpleNamespace(
            text=None,
            document=SimpleNamespace(file_name=filename, file_size=len(content), content=content),
            answer=AsyncMock(),
        )
        await add_direct_blacklist_ids(message, state, bot, service, invites)
        messages.append(message)
    assert service.add_blacklist_entries_bulk.call_args_list[0].args == ((46087621, 58606201),)
    assert service.add_blacklist_entries_bulk.call_args_list[1].kwargs["usernames"] == (
        "andriy_krush",
        "angelcastleee",
    )
    assert bot.download.await_count == invites.refresh_launches.await_count == 2
    menu = messages[-1].answer.call_args.kwargs["reply_markup"]
    assert menu.inline_keyboard[0][0].callback_data == (
        "inv:home" if origin == "inv" else "dm:home"
    )


@pytest.mark.parametrize(
    "filename,content,size",
    [
        ("blacklist.csv", b"42", 2),
        ("blacklist.txt", b"42", MAX_BLACKLIST_IMPORT_BYTES + 1),
        ("blacklist.txt", b"42\n@bad-name", None),
        ("blacklist.txt", b"\xff42", None),
    ],
)
async def test_bot_rejects_bad_documents_before_database_writes(filename, content, size):
    state = SimpleNamespace(get_data=AsyncMock(return_value={}))

    async def download(document, destination):
        destination.write(content)

    bot = SimpleNamespace(download=AsyncMock(side_effect=download))
    service = SimpleNamespace(add_blacklist_entries_bulk=AsyncMock())
    message = SimpleNamespace(
        text=None, document=SimpleNamespace(file_name=filename, file_size=size), answer=AsyncMock()
    )
    await add_direct_blacklist_ids(message, state, bot, service, object())
    service.add_blacklist_entries_bulk.assert_not_awaited()
    message.answer.assert_awaited_once()
