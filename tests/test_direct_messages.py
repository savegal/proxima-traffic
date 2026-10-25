import asyncio
import gzip
import json
import zipfile
from datetime import timedelta
from itertools import count
from pathlib import Path

import pytest
from sqlalchemy import event, func, select

from proxima_traffic.config import Settings
from proxima_traffic.content import render_spintax
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import (
    PRIMARY_SUBSCRIBER_LIST_NAME,
    DirectMessageService,
    parse_direct_blacklist_ids,
)
from proxima_traffic.gateways import (
    DirectMessageDelivery,
    DryRunGateway,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    AccountStatus,
    DirectCampaign,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyProtocol,
    ProxyStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberList,
    SubscriberListMember,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.proxies import ProxyService, _ProxyProbeResult
from proxima_traffic.services import CampaignRunner, ConflictError
from proxima_traffic.subscriber_import import (
    SubscriberImportError,
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


@pytest.mark.asyncio
async def test_common_subscriber_base_merges_legacy_lists_and_survives_bad_reimport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path, "one-subscriber-base.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    service = DirectMessageService(
        database.session_factory,
        gateway,
        CampaignRunner(database.session_factory, gateway, settings=settings),
        settings,
        worker_id="test-direct",
    )
    legacy_ids = []
    for index in (1, 2):
        source = tmp_path / f"legacy-{index}.jsonl"
        source.write_text(f'{{"telegram_id": "77000{index}"}}\n', encoding="utf-8")
        imported = await service.import_subscribers(
            source, list_name=f"Old {index}", source_filename=source.name
        )
        legacy_ids.append(imported.subscriber_list.id)

    async with database.session_factory() as session:
        session.add_all(
            [
                DirectCampaign(
                    name="Old message", subscriber_list_id=legacy_ids[0],
                    primary_text="Hello",
                ),
                SubscriberInviteCampaign(
                    name="Old invite", subscriber_list_id=legacy_ids[1],
                    destination_ref="@owned_channel", destination_key="owned_channel",
                ),
            ]
        )
        await session.commit()

    primary = await service.get_primary_subscriber_list()
    assert primary.name == PRIMARY_SUBSCRIBER_LIST_NAME
    assert primary.member_count == 2
    async with database.session_factory() as session:
        assert (await session.scalar(select(DirectCampaign))).subscriber_list_id == primary.id
        assert (
            await session.scalar(select(SubscriberInviteCampaign))
        ).subscriber_list_id == primary.id

    invalid = tmp_path / "invalid.jsonl"
    invalid.write_text("not json\n" * 5_000, encoding="utf-8")
    ticks = count(step=3)
    monkeypatch.setattr("proxima_traffic.direct_messages.monotonic", lambda: next(ticks))
    reported = []

    async def record_progress(snapshot) -> None:
        reported.append(snapshot)

    with pytest.raises(SubscriberImportError, match="нет ни одной корректной записи"):
        await service.import_subscribers(
            invalid,
            list_name=PRIMARY_SUBSCRIBER_LIST_NAME,
            source_filename=invalid.name,
            on_progress=record_progress,
        )
    assert reported and reported[0].total_lines == 5_000
    primary = await service.get_primary_subscriber_list()
    assert primary.status.value == "ready"
    assert primary.member_count == 2
    assert primary.last_error is not None
    with pytest.raises(ConflictError, match="общую базу нельзя удалить"):
        await service.delete_subscriber_list(primary.id)
    await database.dispose()


@pytest.mark.asyncio
async def test_repeated_import_skips_unchanged_profile_writes_and_reports_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path, "subscriber-import-progress.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    service = DirectMessageService(
        database.session_factory,
        gateway,
        CampaignRunner(database.session_factory, gateway, settings=settings),
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "audience.jsonl"
    source.write_text(
        "".join(f'{{"telegram_id": {780000 + index}}}\n' for index in range(501)),
        encoding="utf-8",
    )
    await service.import_subscribers(
        source, list_name=PRIMARY_SUBSCRIBER_LIST_NAME, source_filename=source.name
    )
    async with database.session_factory() as session:
        original_update = await session.scalar(
            select(Subscriber.updated_at).where(Subscriber.telegram_user_id == 780000)
        )

    ticks = count(step=3)
    monkeypatch.setattr("proxima_traffic.direct_messages.monotonic", lambda: next(ticks))
    progress = []

    async def record_progress(snapshot) -> None:
        progress.append(snapshot)

    repeated = await service.import_subscribers(
        source,
        list_name=PRIMARY_SUBSCRIBER_LIST_NAME,
        source_filename=source.name,
        on_progress=record_progress,
    )
    assert repeated.added_members == 0
    assert repeated.updated_profiles == 0
    assert progress and progress[-1].valid_records >= 500
    async with database.session_factory() as session:
        assert await session.scalar(
            select(Subscriber.updated_at).where(Subscriber.telegram_user_id == 780000)
        ) == original_update
    changed_source = tmp_path / "changed.jsonl"
    changed_source.write_text(
        '{"telegram_id": 780000, "username": "updated_name", "is_premium": true}\n',
        encoding="utf-8",
    )
    changed = await service.import_subscribers(
        changed_source,
        list_name=PRIMARY_SUBSCRIBER_LIST_NAME,
        source_filename=changed_source.name,
    )
    assert changed.updated_profiles == 1
    assert changed.subscriber_list.member_count == 501
    assert changed.subscriber_list.premium_count == 1
    await database.dispose()


class TimeoutDirectGateway(DryRunGateway):
    def __init__(self, expire_step):
        super().__init__()
        self._expire_step = expire_step

    async def send_direct_content(self, **kwargs):
        del kwargs
        self._expire_step()
        await asyncio.sleep(1)
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


class FirstRecipientUnavailableGateway(DryRunGateway):
    """Refuse one known person so the launch must backfill from its own list."""

    def __init__(self, unavailable_telegram_id: int) -> None:
        super().__init__()
        self.unavailable_telegram_id = unavailable_telegram_id
        self.sent_to: list[int] = []

    async def send_direct_content(self, **kwargs):
        recipient = kwargs["recipient"]
        if int(recipient.telegram_user_id) == self.unavailable_telegram_id:
            raise PermanentGatewayError(
                "recipient unavailable", code="recipient_unavailable"
            )
        self.sent_to.append(int(recipient.telegram_user_id))
        return await super().send_direct_content(**kwargs)


@pytest.mark.asyncio
async def test_dashboard_uses_exact_global_counters_and_launch_list_has_fixed_queries(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "direct-dashboard-fast.db")
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
        '{"telegram_id":"910001","username":"one","is_premium":true}\n'
        '{"telegram_id":"910002","is_premium":false}\n',
        encoding="utf-8",
    )
    second_source = tmp_path / "second.jsonl"
    second_source.write_text(
        '{"telegram_id":"910001","username":"one_new","is_premium":false}\n'
        '{"telegram_id":"910003","username":"three","is_premium":true}\n',
        encoding="utf-8",
    )
    first = await service.import_subscribers(
        first_source, list_name="First", source_filename=first_source.name
    )
    await service.import_subscribers(
        second_source, list_name="Second", source_filename=second_source.name
    )
    dashboard = await service.dashboard()
    assert dashboard.subscriber_count == 3
    assert dashboard.premium_count == 1
    assert dashboard.username_count == 2

    async with database.session_factory() as session:
        campaign = DirectCampaign(
            name="Query-count campaign",
            subscriber_list_id=first.subscriber_list.id,
            segment=SubscriberSegment.ALL,
            recipient_limit=1,
            contact_cooldown_days=0,
            primary_text="Prepared post",
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            [
                DirectLaunch(
                    campaign_id=campaign.id,
                    subscriber_list_id=first.subscriber_list.id,
                    status=DirectLaunchStatus.SUCCEEDED,
                    segment_snapshot=SubscriberSegment.ALL,
                    recipient_limit_snapshot=1,
                    contact_cooldown_days_snapshot=0,
                    account_ids_snapshot_json="[]",
                    primary_text_snapshot="Prepared post",
                    total_tasks=0,
                    consent_confirmed_at=utc_now(),
                )
                for _ in range(12)
            ]
        )
        await session.commit()

    statements: list[str] = []

    def capture_statement(
        connection, cursor, statement, parameters, context, executemany
    ) -> None:
        del connection, cursor, parameters, context, executemany
        statements.append(str(statement))

    event.listen(database.engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        launches, total = await service.list_launches(0, 6)
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", capture_statement)
        await database.dispose()
    assert total == 12
    assert len(launches) == 6
    assert len(statements) == 3


@pytest.mark.asyncio
async def test_recipient_skip_backfills_next_unique_eligible_subscriber(tmp_path: Path) -> None:
    settings = _settings(tmp_path, "direct-recipient-backfill.db")
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = FirstRecipientUnavailableGateway(unavailable_telegram_id=810001)
    runner = CampaignRunner(database.session_factory, gateway, settings=settings)
    service = DirectMessageService(
        database.session_factory,
        gateway,
        runner,
        settings,
        worker_id="test-direct",
    )
    source = tmp_path / "backfill.jsonl"
    source.write_text(
        "\n".join(
            [
                '{"telegram_id":"810001","username":"unavailable_one","is_premium":false}',
                '{"telegram_id":"810002","username":"replacement_two","is_premium":false}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="Backfill",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="Backfill DM",
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

    first = await service.process_next()
    assert first is not None
    assert first.status == DirectMessageTaskStatus.SKIPPED
    assert first.error_code == "recipient_unavailable"
    view = await service.get_launch_view(launch.id)
    assert view.skipped == 1
    assert view.queued == 1
    assert view.replacement_tasks == 1
    assert view.next_scheduled_for is not None

    second = await service.process_next()
    assert second is not None
    assert second.status == DirectMessageTaskStatus.SUCCEEDED
    assert gateway.sent_to == [810002]
    async with database.session_factory() as session:
        tasks = list(
            await session.scalars(
                select(DirectMessageTask)
                .where(DirectMessageTask.launch_id == launch.id)
                .order_by(DirectMessageTask.id)
            )
        )
        assert len(tasks) == 2
        assert len({int(task.subscriber_id) for task in tasks}) == 2
        replacement_subscriber = await session.get(Subscriber, second.subscriber_id)
        assert replacement_subscriber is not None
        assert replacement_subscriber.reserved_direct_launch_id is None
    final_view = await service.get_launch_view(launch.id)
    assert final_view.launch.total_tasks == 1
    assert final_view.succeeded == 1
    assert final_view.simulated is True
    assert final_view.replacement_tasks == 1
    dashboard = await service.dashboard()
    assert dashboard.simulated_succeeded_tasks == 1
    assert dashboard.unknown_mode_succeeded_tasks == 0
    await database.dispose()


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
    assert second.updated_profiles == 0
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
async def test_direct_task_reports_unavailable_sender_pool_without_blaming_recipient(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, "direct-no-sender.db")
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
    source = tmp_path / "recipient.jsonl"
    source.write_text(
        '{"telegram_id":"355101","username":"available_member","is_premium":false}\n',
        encoding="utf-8",
    )
    imported = await service.import_subscribers(
        source,
        list_name="No sender",
        source_filename=source.name,
    )
    async with database.session_factory() as session:
        account = Account(label="sender", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        await session.refresh(account)
    campaign = await service.create_campaign(
        name="No sender DM",
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
    async with database.session_factory() as session:
        stored = await session.get(Account, account.id)
        assert stored is not None
        stored.status = AccountStatus.DISABLED
        await session.commit()

    result = await service.process_next()

    assert result is not None
    assert result.status == DirectMessageTaskStatus.FAILED
    assert result.error_code == "sender_unavailable"
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
    first_launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
    assert first_launch.total_tasks == 5
    with pytest.raises(ConflictError, match="активный запуск"):
        await service.launch_campaign(campaign.id, consent_confirmed=True)

    for _ in range(5):
        result = await service.process_next()
        assert result is not None
        assert result.status == DirectMessageTaskStatus.SUCCEEDED
    second_launch = await service.launch_campaign(campaign.id, consent_confirmed=True)
    assert second_launch.total_tasks == 5
    for _ in range(5):
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
async def test_uncertain_delivery_is_not_automatically_sent_twice(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path, "direct-timeout.db")
    # Database preparation must be allowed to finish even on a loaded host.
    # Expire the real worker timeout only once the gateway has been called.
    settings.worker_step_timeout_seconds = 30
    original_timeout = asyncio.timeout
    step_timeout = None

    def capture_step_timeout(seconds):
        nonlocal step_timeout
        scope = original_timeout(seconds)
        if seconds == settings.worker_step_timeout_seconds:
            step_timeout = scope
        return scope

    def expire_step():
        assert step_timeout is not None
        step_timeout.reschedule(asyncio.get_running_loop().time())

    database = Database(settings.database_url)
    await database.create_schema()
    gateway = TimeoutDirectGateway(expire_step)
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
    monkeypatch.setattr("proxima_traffic.direct_messages.asyncio.timeout", capture_step_timeout)
    try:
        result = await service.process_next()

        assert result is not None
        assert step_timeout is not None and step_timeout.expired()
        assert result.status == DirectMessageTaskStatus.FAILED
        assert result.error_code == "delivery_outcome_unknown"
        async with database.session_factory() as session:
            subscriber = await session.scalar(select(Subscriber))
            assert subscriber is not None
            assert subscriber.last_delivery_uncertain_at is not None
            sender = await session.get(Account, account.id)
            assert sender.sent_today == 1
        with pytest.raises(ConflictError, match="нет доступных получателей"):
            await service.launch_campaign(campaign.id, consent_confirmed=True)
    finally:
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
