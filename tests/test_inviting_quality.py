"""Invitation regressions exercised without live Telegram requests."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy import delete, select, update
from telethon import errors, functions, types
from telethon.extensions import BinaryReader
from telethon.tl.custom import ParticipantPermissions

from proxima_traffic.bot.subscriber_invite_ui import invite_launch_details_menu, invite_launch_text
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import (
    DirectRecipient,
    DryRunGateway,
    PermanentGatewayError,
    SubscriberInviteDestination,
    TemporaryGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    Account,
    AccountStatus,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteCampaignAccount,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberSegment,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError
from proxima_traffic.subscriber_invites import (
    SubscriberInviteAdminSetupResult,
    SubscriberInviteService,
)


@asynccontextmanager
async def _operation(account_id):
    del account_id
    yield


def _admin_permissions(user_id=10, **rights):
    return ParticipantPermissions(
        types.ChannelParticipantAdmin(
            user_id=user_id,
            promoted_by=1,
            date=utc_now(),
            admin_rights=types.ChatAdminRights(**rights),
            rank="Team",
        ),
        chat=False,
    )


def _live_gateway(tmp_path):
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'unused.db'}",
        admin_api_key="test",
        dry_run=True,
        worker_enabled=False,
    )
    channel = types.Channel(
        id=100,
        access_hash=101,
        title="Owned",
        megagroup=True,
        photo=types.ChatPhotoEmpty(),
        date=utc_now(),
    )
    user = types.User(id=300, access_hash=301, premium=False, username="consenting_person")
    client = AsyncMock()
    client.get_permissions = AsyncMock(
        return_value=_admin_permissions(invite_users=True, add_admins=True)
    )
    client.get_entity = AsyncMock(return_value=user)
    gateway._account_operation = _operation
    gateway._ensure_client = AsyncMock(return_value=client)
    gateway._resolve_subscriber_invite_destination = AsyncMock(return_value=channel)
    gateway._ensure_profile_not_blacklisted = AsyncMock()
    return gateway, client, channel, user


def _participant(user_id=300):
    return SimpleNamespace(participant=types.ChannelParticipant(user_id=user_id, date=utc_now()))


def _invited(missing=()):
    return types.messages.InvitedUsers(
        updates=types.Updates(updates=[], users=[], chats=[], date=utc_now(), seq=0),
        missing_invitees=list(missing),
    )


@pytest.mark.parametrize(
    ("premium_invite", "premium_pm", "code"),
    [
        (False, False, "invite_recipient_privacy"),
        (True, False, "invite_sender_requires_premium"),
        (False, True, "invite_recipient_privacy_premium_pm"),
    ],
)
async def test_missing_invitee_is_a_definite_refusal(tmp_path, premium_invite, premium_pm, code):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.side_effect = [
        errors.UserNotParticipantError(None),
        _invited([types.MissingInvitee(300, premium_invite, premium_pm)]),
    ]
    boundary = AsyncMock()
    with pytest.raises(PermanentGatewayError) as result:
        await gateway.invite_subscriber(
            account=Account(id=1),
            destination_ref="@owned_channel",
            recipient=DirectRecipient(300, "consenting_person"),
            before_invite=boundary,
        )
    assert result.value.code == code
    assert boundary.await_count == 1
    assert client.await_count == 2  # No futile readback or alternative PM.
    assert isinstance(client.await_args.args[0], functions.channels.InviteToChannelRequest)


@pytest.mark.parametrize("response", [_invited([types.MissingInvitee(999)]), None])
async def test_malformed_invite_response_never_counts_as_success(tmp_path, response):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.side_effect = [errors.UserNotParticipantError(None), response]
    with pytest.raises(PermanentGatewayError) as result:
        await gateway.invite_subscriber(
            account=Account(id=1),
            destination_ref="@owned_channel",
            recipient=DirectRecipient(300, "consenting_person"),
        )
    assert result.value.code == "invite_delivery_unconfirmed"
    assert client.await_count == 2


async def test_new_invite_requires_absent_then_confirmed_membership(tmp_path):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.side_effect = [errors.UserNotParticipantError(None), _invited(), _participant()]
    result = await gateway.invite_subscriber(
        account=Account(id=1),
        destination_ref="@owned_channel",
        recipient=DirectRecipient(300, "consenting_person"),
        expected_destination_id=100,
    )
    assert result.telegram_user_id == 300
    assert result.already_participant is False
    assert result.is_premium is False
    assert client.await_count == 3


async def test_existing_participant_has_no_invite_rpc_or_attempt_marker(tmp_path):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.return_value = _participant()
    boundary = AsyncMock()
    result = await gateway.invite_subscriber(
        account=Account(id=1),
        destination_ref="@owned_channel",
        recipient=DirectRecipient(300, "consenting_person"),
        before_invite=boundary,
    )
    assert result.already_participant is True
    boundary.assert_not_awaited()
    assert client.await_count == 1
    assert isinstance(client.await_args.args[0], functions.channels.GetParticipantRequest)


@pytest.mark.parametrize("reason", ["destination", "segment", "cancel"])
async def test_invite_guards_stop_before_mutating_rpc(tmp_path, reason):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.side_effect = errors.UserNotParticipantError(None)
    boundary = AsyncMock(
        side_effect=PermanentGatewayError("cancelled", code="invite_launch_cancelled")
    )
    with pytest.raises(PermanentGatewayError) as result:
        await gateway.invite_subscriber(
            account=Account(id=1),
            destination_ref="@owned_channel",
            recipient=DirectRecipient(300, "consenting_person"),
            expected_destination_id=999 if reason == "destination" else 100,
            require_premium=True if reason == "segment" else None,
            before_invite=boundary,
        )
    assert (
        result.value.code
        == {
            "destination": "invite_destination_changed",
            "segment": "invite_recipient_segment_changed",
            "cancel": "invite_launch_cancelled",
        }[reason]
    )
    assert all(
        not isinstance(call.args[0], functions.channels.InviteToChannelRequest)
        for call in client.await_args_list
    )


@pytest.mark.parametrize("stage", ["rights", "membership"])
async def test_pre_invite_network_failure_is_safe_to_retry(tmp_path, stage):
    gateway, client, _, _ = _live_gateway(tmp_path)
    if stage == "rights":
        client.get_permissions.side_effect = OSError("offline")
    else:
        client.side_effect = OSError("offline")
    boundary = AsyncMock()
    with pytest.raises(TemporaryGatewayError) as result:
        await gateway.invite_subscriber(
            account=Account(id=1),
            destination_ref="@owned_channel",
            recipient=DirectRecipient(300, "consenting_person"),
            before_invite=boundary,
        )
    assert result.value.code == "invite_preflight_unavailable"
    boundary.assert_not_awaited()


async def test_real_telethon_admin_permissions_are_accepted(tmp_path):
    gateway, client, channel, _ = _live_gateway(tmp_path)
    await gateway._ensure_can_invite_subscribers(client, channel)
    await gateway._ensure_can_manage_invite_admins(client, channel)
    result = await gateway.preflight_subscriber_invite(
        account=Account(id=1), destination_ref="@owned_channel"
    )
    assert result.telegram_chat_id == 100


async def test_permission_flood_is_preserved_and_never_fails_open(tmp_path):
    gateway, client, channel, _ = _live_gateway(tmp_path)
    client.get_permissions.side_effect = errors.FloodWaitError(None, capture=720)
    with pytest.raises(TemporaryGatewayError) as result:
        await gateway._ensure_can_invite_subscribers(client, channel)
    assert result.value.code == "flood_wait"
    assert result.value.retry_after >= 720
    client.assert_not_awaited()


async def test_admin_preparation_preserves_existing_rights_and_rank(tmp_path):
    gateway, client, _, user = _live_gateway(tmp_path)
    old = _admin_permissions(300, delete_messages=True, pin_messages=True)
    new = _admin_permissions(300, delete_messages=True, pin_messages=True, invite_users=True)
    client.get_permissions.side_effect = [_admin_permissions(add_admins=True), old, new]
    await gateway.grant_subscriber_invite_admin(
        manager=Account(id=1),
        account=Account(id=2, telegram_user_id=user.id, username=user.username),
        destination_ref="@owned_channel",
    )
    request = client.await_args.args[0]
    assert isinstance(request, functions.channels.EditAdminRequest)
    assert request.admin_rights.delete_messages and request.admin_rights.pin_messages
    assert request.admin_rights.invite_users
    assert request.rank == "Team"
    assert old.participant.admin_rights.invite_users is None


async def test_admin_preparation_does_not_overwrite_unknown_existing_rights(tmp_path):
    gateway, client, _, user = _live_gateway(tmp_path)
    client.get_permissions.side_effect = [
        _admin_permissions(add_admins=True),
        errors.FloodWaitError(None, capture=900),
    ]
    with pytest.raises(TemporaryGatewayError) as result:
        await gateway.grant_subscriber_invite_admin(
            manager=Account(id=1),
            account=Account(id=2, telegram_user_id=user.id, username=user.username),
            destination_ref="@owned_channel",
        )
    assert result.value.code == "flood_wait"
    client.assert_not_awaited()


async def test_admin_username_reassignment_never_promotes_a_different_person(tmp_path):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.get_entity.side_effect = [types.User(id=999, access_hash=1000), ValueError("unknown")]
    with pytest.raises(PermanentGatewayError) as result:
        await gateway.grant_subscriber_invite_admin(
            manager=Account(id=1),
            account=Account(id=2, telegram_user_id=300, username="old_name"),
            destination_ref="@owned_channel",
        )
    assert result.value.code == "invite_admin_target_unresolvable"
    client.assert_not_awaited()


@pytest.mark.parametrize("confirmed", [False, True])
async def test_admin_preparation_does_not_claim_unverified_rights(tmp_path, confirmed):
    gateway, client, _, user = _live_gateway(tmp_path)
    client.get_permissions.side_effect = [
        _admin_permissions(add_admins=True),
        _admin_permissions(300),
        _admin_permissions(300, invite_users=confirmed),
    ]
    if confirmed:
        await gateway.grant_subscriber_invite_admin(
            manager=Account(id=1),
            account=Account(id=2, telegram_user_id=user.id),
            destination_ref="@owned_channel",
        )
    else:
        with pytest.raises(PermanentGatewayError) as result:
            await gateway.grant_subscriber_invite_admin(
                manager=Account(id=1),
                account=Account(id=2, telegram_user_id=user.id),
                destination_ref="@owned_channel",
            )
        assert result.value.code == "invite_admin_update_unconfirmed"


@pytest.mark.parametrize(
    ("rpc", "expected"),
    [
        ("USER_BANNED_IN_CHANNEL", "invite_destination_rights"),
        ("USER_KICKED", "invite_recipient_kicked"),
        ("USER_DEACTIVATED_BAN", "account_dead"),
        ("CHAT_MEMBER_ADD_FAILED", "invite_request_rejected"),
        ("USER_NOT_MUTUAL_CONTACT", "invite_recipient_contact_required"),
        ("USER_PRIVACY_RESTRICTED", "invite_recipient_privacy"),
        ("PREMIUM_ACCOUNT_REQUIRED", "invite_sender_requires_premium"),
        ("CHANNEL_MONOFORUM_UNSUPPORTED", "invite_destination_invalid"),
        ("NEW_UNRECOGNIZED_REFUSAL", "invite_request_rejected"),
    ],
)
def test_invitation_rpc_failure_scope_is_precise(tmp_path, rpc, expected):
    gateway, _, _, _ = _live_gateway(tmp_path)
    result = gateway._map_rpc_error(errors.RPCError(None, rpc, 403), context="subscriber_invite")
    assert isinstance(result, PermanentGatewayError)
    assert result.code == expected
    assert rpc in result.safe_message


async def test_restricted_member_is_already_present_but_a_kicked_user_is_not(tmp_path):
    gateway, client, channel, user = _live_gateway(tmp_path)
    restricted = types.ChannelParticipantBanned(
        peer=types.PeerUser(300),
        kicked_by=1,
        date=utc_now(),
        banned_rights=types.ChatBannedRights(until_date=utc_now(), send_messages=True),
    )
    client.return_value = SimpleNamespace(participant=restricted)
    assert await gateway._subscriber_is_participant(client, channel, user)
    restricted.left = True
    with pytest.raises(PermanentGatewayError) as result:
        await gateway._subscriber_is_participant(client, channel, user)
    assert result.value.code == "invite_recipient_kicked"


@asynccontextmanager
async def _service(tmp_path, gateway, records=1):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'invites.db'}",
        admin_api_key="test",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
    )
    database = Database(settings.database_url)
    await database.create_schema()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = SubscriberInviteService(
        database.session_factory, gateway, runner, settings, worker_id="audit"
    )
    direct = DirectMessageService(
        database.session_factory, gateway, runner, settings, worker_id="import"
    )
    source = tmp_path / "people.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(
                {"telegram_id": str(300 + index), "username": f"person_{index}", "is_premium": True}
            )
            for index in range(records)
        ),
        encoding="utf-8",
    )
    imported = await direct.import_subscribers(
        source, list_name="Consenting people", source_filename=source.name
    )
    async with database.session_factory() as session:
        primary, backup = (
            Account(label="primary", ownership_confirmed=True),
            Account(label="backup", ownership_confirmed=True),
        )
        session.add_all([primary, backup])
        await session.commit()
    campaign = await service.create_campaign(
        name="Owned channel",
        subscriber_list_id=imported.subscriber_list.id,
        destination_ref="@owned_channel",
        segment=SubscriberSegment.ALL,
        recipient_limit=records,
        contact_cooldown_days=0,
        account_ids=[primary.id, backup.id],
    )
    try:
        yield database, service, campaign, primary, backup
    finally:
        await database.dispose()


@pytest.mark.parametrize(
    "code",
    [
        "invite_recipient_privacy",
        "invite_sender_requires_premium",
        "invite_request_rejected",
        "recipient_identity_mismatch",
    ],
)
async def test_definite_refusals_have_no_unknown_count_or_account_switch(tmp_path, code):
    class Refusal(DryRunGateway):
        calls = 0

        async def invite_subscriber(self, **kwargs):
            self.calls += 1
            raise PermanentGatewayError("refused before adding", code=code)

    gateway = Refusal()
    async with _service(tmp_path, gateway) as (database, service, campaign, _, _):
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        task = await service.process_next()
        assert task.status in {DirectMessageTaskStatus.SKIPPED, DirectMessageTaskStatus.FAILED}
        assert task.error_code == code
        assert await service.process_next() is None
        view = await service.get_launch_view(launch.id)
        assert view.invited == view.uncertain == 0
        assert gateway.calls == 1
        async with database.session_factory() as session:
            assert all(
                account.sent_today == 0 for account in await session.scalars(select(Account))
            )
            assert (await session.scalar(select(Subscriber))).reserved_invite_launch_id is None


async def test_cancellation_after_gateway_preparation_stops_at_actual_boundary(tmp_path):
    class DelayedPreparation(DryRunGateway):
        async def invite_subscriber(self, **kwargs):
            async with database.session_factory() as session:
                task = await session.scalar(select(SubscriberInviteTask))
                assert task.delivery_attempted_at is None
                launch_id = task.launch_id
            await service.cancel_launch(launch_id)
            return await super().invite_subscriber(**kwargs)

    async with _service(tmp_path, DelayedPreparation()) as (database, service, campaign, _, _):
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        result = await service.process_next()
        assert result.status == DirectMessageTaskStatus.CANCELLED
        assert result.delivery_attempted_at is None
        view = await service.get_launch_view(launch.id)
        assert view.cancelled == 1 and view.invited == view.uncertain == 0
        assert "Отменено: <b>1</b>" in invite_launch_text(view)


async def test_partial_preflight_remains_visible_and_selection_is_immutable(tmp_path):
    class PartialPreflight(DryRunGateway):
        async def preflight_subscriber_invite(self, **kwargs):
            if kwargs["account"].label == "backup":
                raise PermanentGatewayError("no permission", code="invite_destination_rights")
            return SubscriberInviteDestination(title="Owned", telegram_chat_id=100)

    async with _service(tmp_path, PartialPreflight()) as (
        database,
        service,
        campaign,
        primary,
        backup,
    ):

        async def broken_panel(*args):
            raise OSError("panel unavailable")

        launch = await service.launch_campaign(
            campaign.id, consent_confirmed=True, on_preflight_progress=broken_panel
        )
        assert launch.destination_telegram_id_snapshot == 100
        assert json.loads(launch.account_ids_snapshot_json) == [primary.id]
        async with database.session_factory() as session:
            await session.execute(
                delete(SubscriberInviteCampaignAccount).where(
                    SubscriberInviteCampaignAccount.account_id == backup.id
                )
            )
            await session.commit()
        view = await service.get_launch_view(launch.id)
        assert view.selected_admin_count == 2 and view.ready_admin_count == 1
        assert view.preflight_failures[0][0] == "backup"
        assert "Не прошли проверку" in invite_launch_text(view)
        labels = [
            button.text
            for row in invite_launch_details_menu(view, 0).inline_keyboard
            for button in row
        ]
        assert "Все результаты · причины" in labels


async def test_preflight_cancellation_cleans_up_pending_checks(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()

    class SlowPreflight(DryRunGateway):
        async def preflight_subscriber_invite(self, **kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

    async with _service(tmp_path, SlowPreflight()) as (_, service, campaign, _, _):
        task = asyncio.create_task(service.launch_campaign(campaign.id, consent_confirmed=True))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped.is_set()


async def test_admin_setup_stops_all_other_targets_on_manager_flood(tmp_path):
    class FloodedManager(DryRunGateway):
        calls = 0

        async def grant_subscriber_invite_admin(self, **kwargs):
            self.calls += 1
            raise TemporaryGatewayError("flood wait", retry_after=900, code="flood_wait")

    gateway = FloodedManager()
    async with _service(tmp_path, gateway) as (database, service, _, primary, backup):
        result = await service.prepare_invite_admins(
            manager_id=primary.id,
            account_ids=[primary.id, backup.id],
            destination_ref="@owned_channel",
        )
        assert gateway.calls == 1
        assert len(result.failed) == 2 and not result.granted_labels
        async with database.session_factory() as session:
            stored = await session.get(Account, primary.id)
            assert stored.flood_hits == 1
            assert ensure_utc(stored.cooldown_until) > utc_now()


async def test_different_preflight_destination_ids_reject_launch(tmp_path):
    class DifferentDestinations(DryRunGateway):
        async def preflight_subscriber_invite(self, **kwargs):
            return SubscriberInviteDestination(
                title="Owned", telegram_chat_id=100 + kwargs["account"].id
            )

    async with _service(tmp_path, DifferentDestinations()) as (database, service, campaign, _, _):
        with pytest.raises(ConflictError, match="разные цели"):
            await service.launch_campaign(campaign.id, consent_confirmed=True)
        async with database.session_factory() as session:
            assert await session.scalar(select(SubscriberInviteLaunch)) is None
            assert (await session.scalar(select(Subscriber))).reserved_invite_launch_id is None


async def test_paused_pool_does_not_mark_recipients_as_attempted(tmp_path):
    async with _service(tmp_path, DryRunGateway(), records=2) as (
        database,
        service,
        campaign,
        _,
        _,
    ):
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        async with database.session_factory() as session:
            await session.execute(update(Account).values(status=AccountStatus.PAUSED))
            await session.commit()
        result = await service.process_next()
        assert result.error_code == "sender_unavailable"
        assert await service.process_next() is None
        view = await service.get_launch_view(launch.id)
        assert view.failed == 2 and view.uncertain == view.invited == 0
        async with database.session_factory() as session:
            for subscriber in await session.scalars(select(Subscriber)):
                assert subscriber.last_invite_attempt_at is None
                assert subscriber.reserved_invite_launch_id is None


async def test_old_resolution_failure_can_be_rechecked_in_a_new_manual_launch(tmp_path):
    class PreviouslyUnknown(DryRunGateway):
        calls = 0

        async def invite_subscriber(self, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                raise PermanentGatewayError(
                    "not known by sender", code="invite_recipient_unresolvable"
                )
            return await super().invite_subscriber(**kwargs)

    gateway = PreviouslyUnknown()
    async with _service(tmp_path, gateway) as (database, service, campaign, _, _):
        first = await service.launch_campaign(campaign.id, consent_confirmed=True)
        for _ in range(3):
            await service.process_next()
            async with database.session_factory() as session:
                await session.execute(
                    update(SubscriberInviteTask)
                    .where(SubscriberInviteTask.status == DirectMessageTaskStatus.QUEUED)
                    .values(scheduled_for=utc_now())
                )
                await session.commit()
        assert gateway.calls == 2
        assert (await service.get_launch_view(first.id)).skipped == 1
        second = await service.launch_campaign(campaign.id, consent_confirmed=True)
        assert (await service.process_next()).status == DirectMessageTaskStatus.SUCCEEDED
        assert gateway.calls == 3
        assert (await service.get_launch_view(second.id)).invited == 1


async def test_repeated_admin_setup_skips_ready_accounts_and_can_prepare_the_next(tmp_path):
    class ExistingAdmin(DryRunGateway):
        granted = []

        async def preflight_subscriber_invite(self, **kwargs):
            if kwargs["account"].label == "primary":
                return SubscriberInviteDestination(title="Owned", telegram_chat_id=100)
            raise PermanentGatewayError("needs rights", code="invite_destination_rights")

        async def grant_subscriber_invite_admin(self, **kwargs):
            self.granted.append(kwargs["account"].id)

    gateway = ExistingAdmin()
    async with _service(tmp_path, gateway) as (database, service, _, primary, backup):
        async with database.session_factory() as session:
            await session.execute(
                update(Account).values(
                    authorized=True,
                    session_encrypted="test-session",
                    api_id=1,
                    api_hash_encrypted="test-hash",
                )
            )
            await session.commit()
        service._settings = service._settings.model_copy(update={"dry_run": False})
        result = await service.prepare_invite_admins(
            manager_id=primary.id,
            account_ids=[primary.id, backup.id],
            destination_ref="@owned_channel",
        )
        assert result.granted_labels == ("primary", "backup")
        assert not result.failed
        assert gateway.granted == [backup.id]


async def test_cancellation_between_last_check_and_attempt_marker_is_atomic(tmp_path, monkeypatch):
    async with _service(tmp_path, DryRunGateway()) as (_, service, campaign, _, _):
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        original = service._pre_invite_stop_code
        checks = 0

        async def cancel_between_check_and_marker(*args, **kwargs):
            nonlocal checks
            checks += 1
            result = await original(*args, **kwargs)
            if checks == 2:
                await service.cancel_launch(launch.id)
            return result

        monkeypatch.setattr(service, "_pre_invite_stop_code", cancel_between_check_and_marker)
        result = await service.process_next()
        assert result.status == DirectMessageTaskStatus.CANCELLED
        assert result.delivery_attempted_at is None
        assert (await service.get_launch_view(launch.id)).uncertain == 0


async def test_admin_setup_returns_to_the_visible_account_page_and_prunes_stale_choices(
    tmp_path, monkeypatch
):
    from proxima_traffic.bot import handlers

    state = FSMContext(storage=MemoryStorage(), key=StorageKey(bot_id=1, chat_id=1, user_id=1))
    await state.update_data(
        subscriber_invite_manager_id=1,
        subscriber_invite_account_ids=[1, 2],
        subscriber_invite_destination_ref="@owned_channel",
        subscriber_invite_account_page=3,
    )
    service = SimpleNamespace(
        prepare_invite_admins=AsyncMock(
            return_value=SubscriberInviteAdminSetupResult(
                manager_label="main",
                granted_labels=("primary",),
                failed=(("removed", "disabled"),),
            )
        )
    )
    repository = SimpleNamespace(
        active_owned_accounts=AsyncMock(return_value=[Account(id=1, label="primary")])
    )
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'unused-ui.db'}",
        admin_api_key="test",
        dry_run=False,
        worker_enabled=False,
    )
    await handlers.run_subscriber_invite_admin_setup(
        SimpleNamespace(message=AsyncMock()),
        state,
        service,
        repository,
        settings,
    )
    data = await state.get_data()
    assert data["subscriber_invite_account_page"] == 0
    assert data["subscriber_invite_account_ids"] == [1]
    assert "Права подготовлены частично" in edit.await_args.args[1]


@pytest.mark.parametrize(
    "rpc",
    [
        errors.InviteHashEmptyError(None),
        errors.InviteHashExpiredError(None),
        errors.InviteHashInvalidError(None),
        errors.InviteRevokedMissingError(None),
    ],
)
async def test_invalid_private_link_is_a_destination_error_before_any_invite(tmp_path, rpc):
    gateway, client, _, _ = _live_gateway(tmp_path)
    client.side_effect = rpc
    with pytest.raises(PermanentGatewayError) as result:
        await TelethonGateway._resolve_subscriber_invite_destination(
            gateway,
            client,
            1,
            "https://t.me/+Private_123",
        )
    assert result.value.code == "invite_destination_link_invalid"
    assert isinstance(client.await_args.args[0], functions.messages.CheckChatInviteRequest)


async def test_completed_launch_summary_recovers_after_a_post_commit_failure(tmp_path, monkeypatch):
    class CountingGateway(DryRunGateway):
        calls = 0

        async def invite_subscriber(self, **kwargs):
            self.calls += 1
            return await super().invite_subscriber(**kwargs)

    gateway = CountingGateway()
    async with _service(tmp_path, gateway) as (_, service, campaign, _, _):
        launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
        refresh = service._refresh_launch
        failed_once = False

        async def fail_summary_once(launch_id):
            nonlocal failed_once
            if not failed_once:
                failed_once = True
                raise OSError("summary unavailable after saving the receipt")
            await refresh(launch_id)

        monkeypatch.setattr(service, "_refresh_launch", fail_summary_once)
        assert (await service.process_next()).status == DirectMessageTaskStatus.SUCCEEDED
        assert await service.process_next() is None
        view = await service.get_launch_view(launch.id)
        assert view.launch.status.value == "succeeded"
        assert view.invited == 1 and view.uncertain == 0
        assert gateway.calls == 1


async def test_new_admin_rights_accept_the_actual_decoded_telegram_flags(tmp_path):
    gateway, client, _, user = _live_gateway(tmp_path)
    member = ParticipantPermissions(
        types.ChannelParticipant(user_id=300, date=utc_now()), chat=False
    )
    admin = _admin_permissions(300, invite_users=True)
    with BinaryReader(bytes(admin.participant.admin_rights)) as reader:
        admin.participant.admin_rights = reader.tgread_object()
    assert admin.participant.admin_rights.delete_messages is False
    client.get_permissions.side_effect = [_admin_permissions(add_admins=True), member, admin]
    await gateway.grant_subscriber_invite_admin(
        manager=Account(id=1),
        account=Account(id=2, telegram_user_id=user.id),
        destination_ref="@owned_channel",
    )
    assert isinstance(client.await_args.args[0], functions.channels.EditAdminRequest)
