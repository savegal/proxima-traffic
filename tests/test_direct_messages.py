import asyncio
import gzip
import json
import zipfile
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select

from proxima_traffic.config import Settings
from proxima_traffic.content import render_spintax
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import (
    DirectMessageService,
    parse_direct_blacklist_ids,
)
from proxima_traffic.gateways import DirectMessageDelivery, DryRunGateway
from proxima_traffic.models import (
    Account,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.services import CampaignRunner, ConflictError
from proxima_traffic.subscriber_import import (
    SubscriberParseStats,
    iter_subscriber_records,
    subscriber_list_name,
)


def _settings(tmp_path: Path, name: str) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / name}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_typing_enabled=False,
    )


class TimeoutDirectGateway(DryRunGateway):
    async def send_direct_content(self, **kwargs):
        del kwargs
        await asyncio.sleep(0.2)
        raise AssertionError("outer delivery timeout should cancel this operation")


class WrongRecipientGateway(DryRunGateway):
    async def send_direct_content(self, **kwargs):
        delivery = await super().send_direct_content(**kwargs)
        return DirectMessageDelivery(
            message_id=delivery.message_id,
            telegram_user_id=delivery.telegram_user_id + 1,
            username=delivery.username,
            is_premium=delivery.is_premium,
            access_hash=delivery.access_hash,
        )


class CapturingDirectGateway(DryRunGateway):
    def __init__(self) -> None:
        super().__init__()
        self.direct_sender_ids: list[int] = []
        self.direct_texts: list[str] = []

    async def send_direct_content(self, **kwargs):
        self.direct_sender_ids.append(int(kwargs["account"].id))
        self.direct_texts.append(str(kwargs["text"]))
        return await super().send_direct_content(**kwargs)


def test_jsonl_reader_normalizes_real_export_schema_and_skips_bad_rows(tmp_path: Path) -> None:
    source = tmp_path / "join_accounts_all_jsonl_20260809_105504_token.jsonl"
    rows = [
        {"telegram_id": "1001", "username": "Alice_Name", "is_premium": True},
        {"telegram_id": "1002", "username": None, "is_premium": False},
        {"telegram_id": "1001", "username": "duplicate", "is_premium": True},
        {"telegram_id": "bad", "username": "valid_name", "is_premium": False},
    ]
    source.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
        encoding="utf-8",
    )
    stats = SubscriberParseStats()

    records = list(iter_subscriber_records(source, stats=stats))

    assert [item.telegram_user_id for item in records] == [1001, 1002]
    assert records[0].username == "alice_name"
    assert stats.total_lines == 4
    assert stats.valid_records == 2
    assert stats.duplicates == 1
    assert stats.invalid == 1
    assert subscriber_list_name(source.name) == "join accounts all"


def test_direct_blacklist_id_parser_accepts_pasted_lists_and_rejects_bad_values() -> None:
    assert parse_direct_blacklist_ids("123, 456\n123;789") == (123, 456, 789)
    with pytest.raises(ValueError, match="некорректный"):
        parse_direct_blacklist_ids("123 @username")


def test_jsonl_reader_supports_gzip_and_multipart_zip(tmp_path: Path) -> None:
    gzip_path = tmp_path / "subscribers.jsonl.gz"
    with gzip.open(gzip_path, "wt", encoding="utf-8") as stream:
        stream.write('{"telegram_id":"2001","username":"premium_one","is_premium":true}\n')
    assert [item.telegram_user_id for item in iter_subscriber_records(gzip_path)] == [2001]

    zip_path = tmp_path / "subscribers.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "part-2.jsonl",
            '{"telegram_id":"2003","username":"normal_three","is_premium":false}\n',
        )
        archive.writestr(
            "part-1.jsonl",
            '{"telegram_id":"2002","username":"normal_two","is_premium":false}\n',
        )
        archive.writestr("ignored.txt", "not imported")
    assert [item.telegram_user_id for item in iter_subscriber_records(zip_path)] == [2002, 2003]


@pytest.mark.asyncio
async def test_large_import_is_batched_idempotent_and_segmented(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "large-import.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "large.jsonl"
    with source.open("w", encoding="utf-8") as stream:
        for index in range(1, 2_006):
            stream.write(
                json.dumps(
                    {
                        "telegram_id": str(300_000 + index),
                        "username": f"member_{index:06d}",
                        "is_premium": index % 5 == 0,
                    }
                )
                + "\n"
            )

    first = await service.import_subscribers(
        source,
        list_name="Большая база",
        source_filename=source.name,
    )
    second = await service.import_subscribers(
        source,
        list_name="Большая база",
        source_filename=source.name,
    )

    assert first.valid_records == 2_005
    assert first.added_members == 2_005
    assert first.subscriber_list.premium_count == 401
    assert second.added_members == 0
    assert second.updated_profiles == 2_005
    assert second.subscriber_list.member_count == 2_005
    async with database.session_factory() as session:
        assert int(await session.scalar(select(func.count(Subscriber.id))) or 0) == 2_005
    await database.dispose()


@pytest.mark.asyncio
async def test_pre_send_stop_guard_skips_task_without_calling_gateway(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "pre-send-stop.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "guard.jsonl"
    source.write_text(
        '{"telegram_id":"350001","username":"guard_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Guard",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Guard DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    await service.launch_campaign(campaign.id, consent_confirmed=True)

    async def stop_before_send(*args, **kwargs) -> str:
        del args, kwargs
        return "recipient_blacklisted"

    service._pre_send_stop_code = stop_before_send  # type: ignore[method-assign]
    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.SKIPPED
    assert result.error_code == "recipient_blacklisted"
    assert gateway._next_message_id == 10_000
    await database.dispose()


@pytest.mark.asyncio
async def test_direct_sender_selection_avoids_an_account_reserved_by_another_worker(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "direct-sender-reservation.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = CapturingDirectGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "recipient.jsonl"
    source.write_text(
        '{"telegram_id":"355001","username":"available_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Sender selection",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        reserved = Account(
            label="reserved sender",
            ownership_confirmed=True,
            send_reservation_token="another-worker",
            send_reservation_until=utc_now() + timedelta(minutes=2),
        )
        available = Account(label="available sender", ownership_confirmed=True)
        session.add_all([reserved, available])
        await session.commit()
        await session.refresh(reserved)
        await session.refresh(available)
    campaign = await service.create_campaign(
        name="Reservation-aware DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[reserved.id, available.id],
        text="{Prepared|Ready} post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    await service.launch_campaign(campaign.id, consent_confirmed=True)

    task = await service.process_next()

    assert task is not None
    assert task.status == DirectMessageTaskStatus.SUCCEEDED
    assert task.sender_account_id == available.id
    assert gateway.direct_sender_ids == [available.id]
    assert gateway.direct_texts == [
        render_spintax("{Prepared|Ready} post", seed=f"direct-message:{task.id}")
    ]
    await database.dispose()


@pytest.mark.asyncio
async def test_direct_launch_reserves_unique_recipients_and_delivers(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-delivery.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "premium.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(
                {
                    "telegram_id": str(400_000 + index),
                    "username": f"premium_{index:03d}",
                    "is_premium": True,
                }
            )
            for index in range(20)
        ),
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Premium",
        source_filename=source.name,
    )
    blacklist_result = await service.add_blacklist_entries([400_000])
    assert blacklist_result.added == 1
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)

    campaign = await service.create_campaign(
        name="Premium DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.PREMIUM,
        recipient_limit=5,
        contact_cooldown_days=30,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    first_launch, second_launch = await asyncio.gather(
        service.launch_campaign(campaign.id, consent_confirmed=True),
        service.launch_campaign(campaign.id, consent_confirmed=True),
    )
    assert first_launch.total_tasks == 5
    assert second_launch.total_tasks == 5

    for _ in range(10):
        result = await service.process_next()
        assert result is not None
        assert result.status == DirectMessageTaskStatus.SUCCEEDED

    first_view = await service.get_launch_view(first_launch.id)
    second_view = await service.get_launch_view(second_launch.id)
    assert first_view.launch.status == DirectLaunchStatus.SUCCEEDED
    assert second_view.launch.status == DirectLaunchStatus.SUCCEEDED
    assert first_view.succeeded == 5
    assert second_view.succeeded == 5
    async with database.session_factory() as session:
        recipient_ids = list(
            await session.scalars(
                select(DirectMessageTask.subscriber_id).where(
                    DirectMessageTask.launch_id.in_([first_launch.id, second_launch.id])
                )
            )
        )
        assert len(recipient_ids) == len(set(recipient_ids)) == 10
        recipient_telegram_ids = set(
            await session.scalars(
                select(Subscriber.telegram_user_id).where(Subscriber.id.in_(recipient_ids))
            )
        )
        assert 400_000 not in recipient_telegram_ids
        contacted = int(
            await session.scalar(
                select(func.count(Subscriber.id)).where(Subscriber.message_count == 1)
            )
            or 0
        )
        assert contacted == 10
    await database.dispose()


@pytest.mark.asyncio
async def test_direct_launch_requires_explicit_consent_confirmation(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-consent.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )

    with pytest.raises(ConflictError, match="согласие"):
        await service.launch_campaign(1, consent_confirmed=False)
    await database.dispose()


@pytest.mark.asyncio
async def test_uncertain_delivery_is_not_automatically_sent_twice(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-timeout.db")
    settings.worker_step_timeout_seconds = 0.02
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = TimeoutDirectGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "one.jsonl"
    source.write_text(
        '{"telegram_id":"501001","username":"known_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="One",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Timeout DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    await service.launch_campaign(campaign.id, consent_confirmed=True)

    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "step_timeout"
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
        assert subscriber is not None
        assert subscriber.last_delivery_uncertain_at is not None
    with pytest.raises(ConflictError, match="нет доступных получателей"):
        await service.launch_campaign(campaign.id, consent_confirmed=True)
    await database.dispose()


@pytest.mark.asyncio
async def test_adding_blacklist_id_cancels_queued_direct_task(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-blacklist.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "blocked.jsonl"
    source.write_text(
        '{"telegram_id":"601001","username":"blocked_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Blocked",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Blacklist DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    launch = await service.launch_campaign(campaign.id, consent_confirmed=True)

    result = await service.add_blacklist_entries([601001])

    assert result.cancelled_tasks == 1
    assert await service.process_next() is None
    view = await service.get_launch_view(launch.id)
    assert view.skipped == 1
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
        assert subscriber is not None
        assert subscriber.reserved_direct_launch_id is None
    await database.dispose()


@pytest.mark.asyncio
async def test_gateway_delivery_identity_mismatch_never_marks_subscriber_contacted(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "direct-identity.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = WrongRecipientGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "identity.jsonl"
    source.write_text(
        '{"telegram_id":"701001","username":"identity_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Identity",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Identity DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=30,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    await service.launch_campaign(campaign.id, consent_confirmed=True)

    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "recipient_identity_mismatch"
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
        assert subscriber is not None
        assert subscriber.message_count == 0
        assert subscriber.last_delivery_uncertain_at is not None
    await database.dispose()
