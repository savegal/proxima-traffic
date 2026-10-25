import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import test_direct_message_reliability as reliability_tests
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text, update
from telethon import functions, types

from proxima_traffic.bot.direct_message_ui import (
    direct_messages_home_menu,
    direct_quarantine_menu,
    direct_quarantine_text,
)
from proxima_traffic.direct_messages import DIRECT_RECIPIENT_QUARANTINE_CODES
from proxima_traffic.gateways import (
    DirectMessageDelivery,
    DirectRecipient,
    DirectRecipientCheck,
    PermanentGatewayError,
)
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.models import (
    DirectLaunch,
    DirectMessageTask,
    Subscriber,
    utc_now,
)
from proxima_traffic.services import ConflictError

dm = reliability_tests.dm


async def quarantine(dm, code="recipient_paid_message_required"):
    dm.gateway.send_direct_content = AsyncMock(
        side_effect=PermanentGatewayError("permanent refusal", code=code)
    )
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service.process_next()
    await dm.service.cancel_launch(launch.id)
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, task.subscriber_id)
    return subscriber, launch


async def enable_check(dm):
    dm.settings.dry_run = False
    async with dm.db.session_factory() as session:
        account = await session.get(type(dm.account), dm.account.id)
        account.authorized = True
        account.session_encrypted = "test-only"
        account.api_id = 123
        account.api_hash_encrypted = "test-only"
        account.send_reservation_token = None
        account.send_reservation_until = None
        account.next_send_at = None
        await session.commit()


@pytest.mark.parametrize("code", sorted(DIRECT_RECIPIENT_QUARANTINE_CODES))
async def test_permanent_refusal_survives_import_and_excludes_future_launches(dm, tmp_path, code):
    subscriber, _ = await quarantine(dm, code)
    assert subscriber.direct_quarantine_code == code
    assert subscriber.direct_quarantine_account_id == dm.account.id
    assert subscriber.do_not_contact is False
    assert (await dm.service.dashboard()).quarantined_recipients == 1

    source = tmp_path / "updated.jsonl"
    source.write_text(json.dumps({
        "telegram_id": subscriber.telegram_user_id,
        "username": "updated_username",
        "is_premium": True,
    }), encoding="utf-8")
    await dm.service.import_subscribers(source, list_name="Updated", source_filename=source.name)
    async with dm.db.session_factory() as session:
        stored = await session.get(Subscriber, subscriber.id)
        assert stored.direct_quarantine_code == code
        assert stored.username == "updated_username"
        stored.last_delivery_uncertain_at = utc_now() - timedelta(days=90)
        await session.commit()

    view = await dm.service.get_campaign_view(dm.campaign.id)
    assert view.eligible_count == 3
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    async with dm.db.session_factory() as session:
        assert await session.scalar(select(DirectMessageTask.subscriber_id).where(
            DirectMessageTask.launch_id == launch.id
        )) != subscriber.id


async def test_queued_recipient_and_dispatch_check_observe_quarantine(dm):
    launch = await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    claimed = await dm.service._claim_next()
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, claimed.subscriber_id)
        subscriber.direct_quarantine_code = "recipient_unavailable"
        subscriber.direct_quarantine_error = "saved refusal"
        subscriber.direct_quarantine_at = utc_now()
        await session.commit()
    assert await dm.service._pre_send_stop_code(
        claimed.id, claimed.lease_token, launch_id=launch.id,
        subscriber_id=subscriber.id, telegram_user_id=subscriber.telegram_user_id,
    ) == "recipient_unavailable"
    dm.gateway.send_direct_content = AsyncMock()
    result = await dm.service._process_claimed(claimed.id, claimed.lease_token)
    assert result.error_code == "recipient_unavailable"
    dm.gateway.send_direct_content.assert_not_awaited()


@pytest.mark.parametrize("outcome", ["paid", "unknown", "unavailable", "wrong_id", "stale", "rpc"])
async def test_unconfirmed_or_negative_check_never_releases_quarantine(dm, outcome):
    subscriber, _ = await quarantine(dm)
    await enable_check(dm)

    async def check(**kwargs):
        assert kwargs["account"].id == dm.account.id
        if outcome == "rpc":
            raise PermanentGatewayError("unresolvable", code="recipient_unresolvable")
        result = DirectRecipientCheck(subscriber.telegram_user_id, utc_now(), True, False)
        return {
            "paid": replace(result, paid_message_required=True),
            "unknown": replace(result, paid_message_required=None),
            "unavailable": replace(result, available=False),
            "wrong_id": replace(result, telegram_user_id=result.telegram_user_id + 1),
            "stale": replace(result, checked_at=utc_now() - timedelta(minutes=5)),
        }[outcome]

    dm.gateway.check_direct_recipient = AsyncMock(side_effect=check)
    note = await dm.service.recheck_quarantined_recipient(subscriber.id)
    assert "сохранён" in note
    async with dm.db.session_factory() as session:
        stored = await session.get(Subscriber, subscriber.id)
        assert stored.direct_quarantine_code == "recipient_paid_message_required"
        assert stored.direct_quarantine_checked_at is not None
        assert stored.direct_quarantine_check_note == note


@pytest.mark.parametrize("code", sorted(DIRECT_RECIPIENT_QUARANTINE_CODES))
async def test_fresh_positive_check_releases_only_quarantine(dm, code):
    subscriber, _ = await quarantine(dm, code)
    await enable_check(dm)
    uncertain_at = utc_now()
    async with dm.db.session_factory() as session:
        stored = await session.get(Subscriber, subscriber.id)
        stored.do_not_contact = True
        stored.do_not_contact_reason = "operator"
        stored.last_delivery_uncertain_at = uncertain_at
        await session.commit()

    async def check(**kwargs):
        return DirectRecipientCheck(subscriber.telegram_user_id, utc_now(), True, False)

    dm.gateway.check_direct_recipient = AsyncMock(side_effect=check)
    dm.gateway.send_direct_content.reset_mock()
    assert "снят" in await dm.service.recheck_quarantined_recipient(subscriber.id)
    dm.gateway.send_direct_content.assert_not_awaited()
    async with dm.db.session_factory() as session:
        stored = await session.get(Subscriber, subscriber.id)
        assert stored.direct_quarantine_code is None
        assert stored.do_not_contact is True
        assert stored.do_not_contact_reason == "operator"
        assert stored.last_delivery_uncertain_at is not None


@pytest.mark.parametrize("change", ["new_failure", "username", "reservation", "newer_check"])
async def test_check_cannot_release_a_changed_record(dm, change):
    subscriber, _ = await quarantine(dm)
    await enable_check(dm)

    async def check(**kwargs):
        async with dm.db.session_factory() as session:
            stored = await session.get(Subscriber, subscriber.id)
            if change == "new_failure":
                stored.direct_quarantine_at = utc_now()
                stored.direct_quarantine_code = "recipient_unavailable"
            elif change == "username":
                stored.username = "changed_during_check"
            elif change == "reservation":
                stored.reserved_direct_launch_id = 1
            else:
                stored.direct_quarantine_checked_at = utc_now()
                stored.direct_quarantine_check_note = "newer negative check"
            await session.commit()
        return DirectRecipientCheck(subscriber.telegram_user_id, utc_now(), True, False)

    dm.gateway.check_direct_recipient = AsyncMock(side_effect=check)
    assert "изменилась" in await dm.service.recheck_quarantined_recipient(subscriber.id)
    async with dm.db.session_factory() as session:
        assert (await session.get(Subscriber, subscriber.id)).direct_quarantine_code is not None


async def test_simulation_and_missing_source_account_cannot_release(dm):
    subscriber, _ = await quarantine(dm)
    with pytest.raises(ConflictError, match="Симуляция"):
        await dm.service.recheck_quarantined_recipient(subscriber.id)
    await enable_check(dm)
    async with dm.db.session_factory() as session:
        await session.execute(update(Subscriber).where(Subscriber.id == subscriber.id).values(
            direct_quarantine_account_id=None
        ))
        await session.commit()
    with pytest.raises(ConflictError, match="Исходный аккаунт"):
        await dm.service.recheck_quarantined_recipient(subscriber.id)


@pytest.mark.parametrize("code", ["recipient_content_forbidden", "recipient_privacy_restricted"])
async def test_other_refusals_do_not_create_global_quarantine(dm, code):
    subscriber, _ = await quarantine(dm, code)
    assert subscriber.direct_quarantine_code is None
    assert (await dm.service.dashboard()).quarantined_recipients == 0


async def test_wrong_delivery_identity_quarantines_and_keeps_uncertainty(dm):
    dm.gateway.send_direct_content = AsyncMock(return_value=DirectMessageDelivery(
        message_id=1, telegram_user_id=12345, username=None, is_premium=None, access_hash=None,
    ))
    await dm.service.launch_campaign(dm.campaign.id, consent_confirmed=True)
    task = await dm.service.process_next()
    assert task.error_code == "recipient_identity_mismatch"
    async with dm.db.session_factory() as session:
        subscriber = await session.get(Subscriber, task.subscriber_id)
        assert subscriber.direct_quarantine_code == "recipient_identity_mismatch"
        assert subscriber.last_delivery_uncertain_at is not None


@pytest.mark.parametrize("price_source", ["none", "full", "user", "peer"])
async def test_live_probe_fetches_fresh_full_profile_without_sending(dm, monkeypatch, price_source):
    gateway = TelethonGateway(dm.db.session_factory, dm.settings)
    profile = types.User(
        id=980001, access_hash=11, username="qa_member_0",
        send_paid_messages_stars=10 if price_source == "user" else None,
    )
    full = types.UserFull(
        id=profile.id, settings=types.PeerSettings(
            charge_paid_message_stars=10 if price_source == "peer" else None
        ),
        notify_settings=types.PeerNotifySettings(), common_chats_count=0,
        send_paid_messages_stars=10 if price_source == "full" else None,
    )
    client = AsyncMock(return_value=types.users.UserFull(full_user=full, chats=[], users=[profile]))
    client.get_entity.return_value = profile

    @asynccontextmanager
    async def operation(account_id):
        yield

    monkeypatch.setattr(gateway, "_account_operation", operation)
    monkeypatch.setattr(gateway, "_ensure_client", AsyncMock(return_value=client))
    result = await gateway.check_direct_recipient(
        account=dm.account, recipient=DirectRecipient(profile.id, profile.username, 22)
    )
    assert result.telegram_user_id == profile.id
    assert result.available is True
    assert result.paid_message_required is (price_source != "none")
    assert isinstance(client.call_args.args[0], functions.users.GetFullUserRequest)
    client.get_entity.assert_awaited_once_with(profile.username)
    client.send_message.assert_not_awaited()
    client.send_file.assert_not_awaited()


@pytest.mark.parametrize("invalid", ["missing", "wrong_id", "min", "deleted", "no_settings"])
async def test_live_probe_does_not_treat_incomplete_or_deleted_profile_as_healthy(
    dm, monkeypatch, invalid
):
    gateway = TelethonGateway(dm.db.session_factory, dm.settings)
    initial = types.User(id=980001, access_hash=11, username="qa_member_0")
    fresh = types.User(id=initial.id, access_hash=11)
    full = types.UserFull(
        id=initial.id, settings=types.PeerSettings(),
        notify_settings=types.PeerNotifySettings(), common_chats_count=0,
    )
    if invalid == "wrong_id":
        full.id += 1
    elif invalid == "min":
        fresh.min = True
    elif invalid == "deleted":
        fresh.deleted = True
    elif invalid == "no_settings":
        full.settings = None
    result = types.users.UserFull(full_user=full, chats=[], users=[fresh])
    if invalid == "missing":
        result.users = []
    client = AsyncMock(return_value=result)
    client.get_entity.return_value = initial

    @asynccontextmanager
    async def operation(account_id):
        yield

    monkeypatch.setattr(gateway, "_account_operation", operation)
    monkeypatch.setattr(gateway, "_ensure_client", AsyncMock(return_value=client))
    if invalid in {"missing", "wrong_id", "min"}:
        with pytest.raises(PermanentGatewayError, match="свежий профиль"):
            await gateway.check_direct_recipient(
                account=dm.account, recipient=DirectRecipient(initial.id, initial.username)
            )
    else:
        checked = await gateway.check_direct_recipient(
            account=dm.account, recipient=DirectRecipient(initial.id, initial.username)
        )
        if invalid == "deleted":
            assert checked.available is False
        else:
            assert checked.paid_message_required is None
    client.send_message.assert_not_awaited()


async def test_migration_backfills_history_without_trusting_overwritten_last_error(dm):
    subscriber, launch = await quarantine(dm, "recipient_identity_mismatch")
    async with dm.db.session_factory() as session:
        await session.execute(update(DirectLaunch).where(DirectLaunch.id == launch.id).values(
            simulated_snapshot=False
        ))
        await session.execute(update(Subscriber).where(Subscriber.id == subscriber.id).values(
            last_error_code="launch_cancelled", last_error="unrelated"
        ))
        await session.commit()

    def migrate(connection):
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        command.downgrade(config, "0070_comment_delivery_monitor")
        command.upgrade(config, "head")

    async with dm.db.engine.begin() as connection:
        await connection.run_sync(migrate)
        row = (await connection.execute(text(
            "SELECT direct_quarantine_code, direct_quarantine_account_id "
            "FROM subscribers WHERE id=:id"
        ), {"id": subscriber.id})).one()
        assert row == ("recipient_identity_mismatch", dm.account.id)


async def test_migration_does_not_quarantine_historical_simulation(dm):
    subscriber, _ = await quarantine(dm)

    def migrate(connection):
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        command.downgrade(config, "0070_comment_delivery_monitor")
        command.upgrade(config, "head")

    async with dm.db.engine.begin() as connection:
        await connection.run_sync(migrate)
        assert await connection.scalar(text(
            "SELECT direct_quarantine_code FROM subscribers WHERE id=:id"
        ), {"id": subscriber.id}) is None


async def test_quarantine_ui_exposes_reason_and_check(dm):
    subscriber, _ = await quarantine(dm, "recipient_identity_mismatch")
    text_ = direct_quarantine_text([subscriber], 1, 0)
    assert "username теперь принадлежит другому пользователю" in text_
    assert "Импорт и ожидание не снимают карантин" in text_
    assert "Stars не списываются" in text_
    buttons = direct_quarantine_menu([subscriber], 1, 0).inline_keyboard
    assert buttons[0][0].callback_data == f"dm:quarantinecheck:{subscriber.id}:0"
    assert any(
        button.callback_data == "dm:quarantine:0"
        for row in direct_messages_home_menu().inline_keyboard for button in row
    )


async def test_quarantine_check_handler_displays_result_and_clamps_empty_page(dm, monkeypatch):
    from proxima_traffic.bot import handlers

    subscriber, _ = await quarantine(dm)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    dm.service.recheck_quarantined_recipient = AsyncMock(
        return_value="Проверка не подтвердилась; карантин сохранён"
    )
    callback = SimpleNamespace(data=f"dm:quarantinecheck:{subscriber.id}:99")
    state = SimpleNamespace(clear=AsyncMock())
    await handlers.recheck_direct_quarantine(callback, state, dm.service)
    dm.service.recheck_quarantined_recipient.assert_awaited_once_with(subscriber.id)
    assert "карантин сохранён" in edit.call_args.args[1]
    assert "Страница 1" in edit.call_args.args[1]
