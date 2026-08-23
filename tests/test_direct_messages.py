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
from proxima_traffic.gateways import (
    DirectMessageDelivery,
    DryRunGateway,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyProtocol,
    ProxyStatus,
    Subscriber,
    SubscriberList,
    SubscriberListMember,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.proxies import ProxyService, _ProxyProbeResult
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
        self.direct_html_texts: list[str | None] = []

    async def send_direct_content(self, **kwargs):
        self.direct_sender_ids.append(int(kwargs["account"].id))
        self.direct_texts.append(str(kwargs["text"]))
        self.direct_html_texts.append(kwargs["html_text"])
        return await super().send_direct_content(**kwargs)


class NetworkFailureDirectGateway(DryRunGateway):
    async def send_direct_content(self, **kwargs):
        del kwargs
        raise TemporaryGatewayError("network unavailable", code="network_error", retry_after=0)


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


def test_multipart_zip_strips_a_bom_from_each_jsonl_member(tmp_path: Path) -> None:
    zip_path = tmp_path / "bom-subscribers.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "part-1.jsonl",
            b'\xef\xbb\xbf{"telegram_id":"2101","username":"member_one","is_premium":false}\n',
        )
        archive.writestr(
            "part-2.jsonl",
            b'\xef\xbb\xbf{"telegram_id":"2102","username":"member_two","is_premium":true}\n',
        )
    stats = SubscriberParseStats()

    records = list(iter_subscriber_records(zip_path, stats=stats))

    assert [record.telegram_user_id for record in records] == [2101, 2102]
    assert stats.valid_records == 2
    assert stats.invalid == 0


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
async def test_subscriber_counter_refresh_chunks_ids_above_sqlite_parameter_limit(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "counter-refresh.db")
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
    async with database.session_factory() as session:
        subscriber = Subscriber(
            telegram_user_id=987_654,
            username="premium_member",
            is_premium=True,
        )
        subscriber_list = SubscriberList(name="Counter refresh")
        session.add_all([subscriber, subscriber_list])
        await session.flush()
        session.add(
            SubscriberListMember(
                subscriber_list_id=int(subscriber_list.id),
                subscriber_id=int(subscriber.id),
            )
        )
        await session.flush()

        counts = await service._refresh_list_counts_for_subscribers(
            session,
            set(range(1, 40_001)) | {int(subscriber.id)},
        )

        assert counts[int(subscriber_list.id)] == (1, 1, 0, 1)
        assert subscriber_list.member_count == 1
        assert subscriber_list.premium_count == 1
    await database.dispose()


@pytest.mark.asyncio
async def test_shared_subscriber_import_refreshes_segment_counts_in_every_list(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "shared-list-counts.db")
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
    first_source = tmp_path / "first.jsonl"
    first_source.write_text(
        '{"telegram_id":"330001","username":"shared_member"}\n',
        encoding="utf-8",
    )
    second_source = tmp_path / "second.jsonl"
    second_source.write_text(
        '{"telegram_id":"330001","username":"shared_member","is_premium":true}\n',
        encoding="utf-8",
    )

    first = await service.import_subscribers(
        first_source,
        list_name="Первый список",
        source_filename=first_source.name,
    )
    second = await service.import_subscribers(
        second_source,
        list_name="Второй список",
        source_filename=second_source.name,
    )

    async with database.session_factory() as session:
        stored_lists = {
            item.id: item
            for item in await session.scalars(
                select(SubscriberList).where(
                    SubscriberList.id.in_([first.subscriber_list.id, second.subscriber_list.id])
                )
            )
        }
    assert stored_lists[first.subscriber_list.id].premium_count == 1
    assert stored_lists[first.subscriber_list.id].non_premium_count == 0
    assert stored_lists[second.subscriber_list.id].premium_count == 1
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
async def test_cancelling_direct_launch_keeps_running_recipient_reserved(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-cancel-reservation.db")
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
    source = tmp_path / "cancel-reservation.jsonl"
    source.write_text(
        "\n".join(
            [
                '{"telegram_id":"360001","username":"first_member","is_premium":false}',
                '{"telegram_id":"360002","username":"second_member","is_premium":false}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Cancel reservation",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Cancel reservation DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=2,
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
    running_task = await service._claim_next()

    assert running_task is not None
    assert await service.cancel_launch(launch.id) == 1

    async with database.session_factory() as session:
        tasks = list(
            await session.scalars(
                select(DirectMessageTask)
                .where(DirectMessageTask.launch_id == launch.id)
                .order_by(DirectMessageTask.id)
            )
        )
        active = next(task for task in tasks if task.id == running_task.id)
        cancelled = next(task for task in tasks if task.id != running_task.id)
        active_subscriber = await session.get(Subscriber, active.subscriber_id)
        cancelled_subscriber = await session.get(Subscriber, cancelled.subscriber_id)

    assert active.status == DirectMessageTaskStatus.RUNNING
    assert active_subscriber is not None
    assert active_subscriber.reserved_direct_launch_id == launch.id
    assert cancelled.status == DirectMessageTaskStatus.CANCELLED
    assert cancelled_subscriber is not None
    assert cancelled_subscriber.reserved_direct_launch_id is None

    async with database.session_factory() as session:
        expired = await session.get(DirectMessageTask, running_task.id)
        assert expired is not None
        expired.lease_until = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await service.process_next() is None
    async with database.session_factory() as session:
        recovered = await session.get(DirectMessageTask, running_task.id)
        recovered_subscriber = await session.get(Subscriber, running_task.subscriber_id)
    assert recovered is not None
    assert recovered.status == DirectMessageTaskStatus.FAILED
    assert recovered.error_code == "cancelled_outcome_unknown"
    assert recovered_subscriber is not None
    assert recovered_subscriber.reserved_direct_launch_id is None
    assert (await service.get_launch_view(launch.id)).launch.status == DirectLaunchStatus.CANCELLED
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
        text="{Prepared|Ready} ⭐",
        html_text='{Prepared|Ready} <tg-emoji emoji-id="543210987654321">⭐</tg-emoji>',
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
        render_spintax("{Prepared|Ready} ⭐", seed=f"direct-message:{task.id}")
    ]
    assert gateway.direct_html_texts == [
        render_spintax(
            '{Prepared|Ready} <tg-emoji emoji-id="543210987654321">⭐</tg-emoji>',
            seed=f"direct-message:{task.id}",
        )
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
async def test_live_direct_launch_refuses_unhealthy_required_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path, "direct-required-proxy.db")
    settings.dry_run = False
    settings.account_require_proxy = True
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    proxy_service = ProxyService(database.session_factory, settings)

    async def failed_probe(proxy_url: str) -> _ProxyProbeResult:
        assert "127.0.0.1:1080" in proxy_url
        return _ProxyProbeResult(
            healthy=False,
            latency_ms=None,
            observed_ip=None,
            error="ConnectError: соединение через прокси не установлено",
        )

    monkeypatch.setattr(proxy_service, "_probe_proxy_url", failed_probe)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
        proxy_service=proxy_service,
    )
    source = tmp_path / "required-proxy.jsonl"
    source.write_text(
        '{"telegram_id":"501111","username":"recipient","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Required proxy",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(
            label="live sender",
            ownership_confirmed=True,
            authorized=True,
            session_encrypted="session",
            api_id=1,
            api_hash_encrypted="api-hash",
        )
        proxy = ProxyEndpoint(
            label="broken proxy",
            protocol=ProxyProtocol.SOCKS5,
            host="127.0.0.1",
            port=1080,
            fingerprint="a" * 64,
            status=ProxyStatus.UNHEALTHY,
        )
        session.add_all([account, proxy])
        await session.flush()
        session.add(
            ProxyAssignment(
                proxy_id=proxy.id,
                assignment_key=f"account:{account.id}",
            )
        )
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Required proxy DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )

    with pytest.raises(ConflictError, match="обязательный прокси"):
        await service.launch_campaign(campaign.id, consent_confirmed=True)
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count(DirectLaunch.id))) == 0
    await database.dispose()


@pytest.mark.asyncio
async def test_running_direct_launch_stops_and_releases_recipients_when_route_is_lost(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "direct-route-lost.db")
    settings.dry_run = False
    settings.account_require_proxy = True
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
    source = tmp_path / "route-lost.jsonl"
    source.write_text(
        '{"telegram_id":"501113","username":"recipient","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Route lost",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(
            label="live sender",
            ownership_confirmed=True,
            authorized=True,
            session_encrypted="session",
            api_id=1,
            api_hash_encrypted="api-hash",
        )
        proxy = ProxyEndpoint(
            label="initially healthy proxy",
            protocol=ProxyProtocol.SOCKS5,
            host="127.0.0.1",
            port=1080,
            fingerprint="b" * 64,
            status=ProxyStatus.HEALTHY,
        )
        session.add_all([account, proxy])
        await session.flush()
        session.add(
            ProxyAssignment(
                proxy_id=proxy.id,
                assignment_key=f"account:{account.id}",
            )
        )
        await session.commit()
        await session.refresh(account)
        await session.refresh(proxy)
    campaign = await service.create_campaign(
        name="Route lost DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
    async with database.session_factory() as session:
        stored_proxy = await session.get(ProxyEndpoint, proxy.id)
        assert stored_proxy is not None
        stored_proxy.status = ProxyStatus.UNHEALTHY
        await session.commit()

    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "proxy_unavailable"
    view = await service.get_launch_view(launch.id)
    assert view.launch.status == DirectLaunchStatus.FAILED
    assert view.failed == 1
    async with database.session_factory() as session:
        subscriber = await session.scalar(select(Subscriber))
        assert subscriber is not None
        assert subscriber.reserved_direct_launch_id is None
    await database.dispose()


@pytest.mark.asyncio
async def test_direct_network_errors_stop_at_attempt_limit(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-network-limit.db")
    settings.worker_max_attempts = 2
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = NetworkFailureDirectGateway()
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "network-limit.jsonl"
    source.write_text(
        '{"telegram_id":"501112","username":"recipient","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Network limit",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Network limit DM",
        subscriber_list_id=imported.subscriber_list.id,
        segment=SubscriberSegment.ALL,
        recipient_limit=1,
        contact_cooldown_days=0,
        account_ids=[account.id],
        text="Prepared post",
        html_text=None,
        media_path=None,
        media_kind=None,
        media_name=None,
        media_size=None,
    )
    launch = await service.launch_campaign(campaign.id, consent_confirmed=True)

    first = await service.process_next()
    assert first is not None and first.status == DirectMessageTaskStatus.QUEUED
    async with database.session_factory() as session:
        task = await session.scalar(
            select(DirectMessageTask).where(DirectMessageTask.launch_id == launch.id)
        )
        assert task is not None
        task.attempt_count = task.max_attempts - 1
        task.scheduled_for = utc_now()
        await session.commit()
    second = await service.process_next()

    assert second is not None
    assert second.status == DirectMessageTaskStatus.FAILED
    assert second.error_code == "network_error"
    assert second.attempt_count == second.max_attempts
    view = await service.get_launch_view(launch.id)
    assert view.launch.status == DirectLaunchStatus.FAILED
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
        contact_cooldown_days=0,
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
