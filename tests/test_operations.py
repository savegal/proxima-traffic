import asyncio
import json
import os
import sqlite3
import time
from datetime import timedelta

from fastapi import FastAPI

from proxima_traffic.maintenance import (
    cleanup_media_orphans,
    create_clean_database,
    create_sqlite_backup,
    expected_migration_revision,
    inspect_sqlite,
    verify_sqlite_backup,
)
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignMode,
    CampaignRun,
    ChannelCommentRule,
    DirectCampaign,
    DirectLaunch,
    ProxyAssignment,
    ProxyEndpoint,
    ProxyGroup,
    ProxyProtocol,
    RunStatus,
    RunStep,
    SubscriberList,
    SubscriberListStatus,
    SubscriberSegment,
    Target,
    TargetList,
    TargetListItem,
    utc_now,
)
from proxima_traffic.notifications import RunNotifier


async def test_terminal_run_notification_is_sent_once(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        account_one = Account(label="notify-one", ownership_confirmed=True)
        account_two = Account(label="notify-two", ownership_confirmed=True)
        target = Target(
            chat_ref="@notify",
            title="Notify target",
            approved_by_operator=True,
        )
        session.add_all([account_one, account_two, target])
        await session.flush()
        campaign = Campaign(
            name="Notify campaign",
            primary_account_id=account_one.id,
            reply_account_id=account_two.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="First",
            reply_text="Reply",
            reply_delay_seconds=0,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="notify-request",
            status=RunStatus.SUCCEEDED,
            next_step=RunStep.REPLY,
            attempt_count=2,
            max_attempts=5,
            primary_message_id=100,
            reply_message_id=101,
        )
        session.add(run)
        await session.commit()
        run_id = run.id

    messages: list[str] = []

    async def sender(text: str) -> None:
        messages.append(text)

    notifier = RunNotifier(app.state.database.session_factory, sender)
    assert await notifier.process_pending() == 1
    assert await notifier.process_pending() == 0
    assert len(messages) == 1
    assert f"Запуск #{run_id}" in messages[0]
    assert "Notify campaign" in messages[0]


async def test_concurrent_notifiers_claim_one_terminal_run_once(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="notify-lease", ownership_confirmed=True)
        target = Target(chat_ref="@notify_lease", title="Notify lease", approved_by_operator=True)
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Notification lease campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="First",
            reply_text="",
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="notification-lease-run",
            status=RunStatus.SUCCEEDED,
            next_step=RunStep.PRIMARY,
        )
        session.add(run)
        await session.commit()

    started = asyncio.Event()
    release = asyncio.Event()
    messages: list[str] = []

    async def sender(text: str) -> None:
        messages.append(text)
        started.set()
        await release.wait()

    first = RunNotifier(app.state.database.session_factory, sender)
    second = RunNotifier(app.state.database.session_factory, sender)
    first_task = asyncio.create_task(first.process_pending())
    await started.wait()
    assert await second.process_pending() == 0
    release.set()
    assert await first_task == 1
    assert len(messages) == 1


async def test_failed_notification_does_not_block_later_runs(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        primary_account = Account(label="notify-failure-primary", ownership_confirmed=True)
        reply_account = Account(label="notify-failure-reply", ownership_confirmed=True)
        target = Target(
            chat_ref="@notify_failure",
            title="Notify failure",
            approved_by_operator=True,
        )
        session.add_all([primary_account, reply_account, target])
        await session.flush()
        campaign = Campaign(
            name="Notification failure campaign",
            primary_account_id=primary_account.id,
            reply_account_id=reply_account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="First",
            reply_text="Reply",
            reply_delay_seconds=0,
        )
        session.add(campaign)
        await session.flush()
        first = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="notification-failure-first",
            status=RunStatus.SUCCEEDED,
            next_step=RunStep.REPLY,
        )
        second = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            request_key="notification-failure-second",
            status=RunStatus.FAILED,
            next_step=RunStep.REPLY,
        )
        session.add_all([first, second])
        await session.commit()
        first_id = first.id
        second_id = second.id

    messages: list[str] = []

    async def sender(text: str) -> None:
        if f"#{first_id}:" in text:
            raise RuntimeError("log chat unavailable")
        messages.append(text)

    notifier = RunNotifier(app.state.database.session_factory, sender)
    assert await notifier.process_pending() == 1
    assert len(messages) == 1
    assert f"#{second_id}:" in messages[0]

    async with app.state.database.session_factory() as session:
        stored_first = await session.get(CampaignRun, first_id)
        stored_second = await session.get(CampaignRun, second_id)
        assert stored_first is not None and stored_first.notified_at is None
        assert stored_second is not None and stored_second.notified_at is not None


async def test_smart_broadcast_success_does_not_spam_notifications(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(label="smart-notify", ownership_confirmed=True)
        target = Target(
            chat_ref="@smart_notify",
            title="Smart notify",
            approved_by_operator=True,
        )
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Smart notification campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="Advertisement",
            reply_text="",
            smart_timer_mode=True,
            smart_duration_seconds=3600,
            smart_no_timer_interval_seconds=900,
        )
        session.add(campaign)
        await session.flush()
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=account.id,
            request_key="smart-notification-request",
            status=RunStatus.SUCCEEDED,
            next_step=RunStep.PRIMARY,
            primary_message_id=200,
            repeat_until=utc_now() + timedelta(hours=1),
        )
        session.add(run)
        campaign.smart_timer_mode = False
        campaign.smart_duration_seconds = 0
        await session.commit()
        run_id = run.id

    messages: list[str] = []

    async def sender(text: str) -> None:
        messages.append(text)

    notifier = RunNotifier(app.state.database.session_factory, sender)
    assert await notifier.process_pending() == 0
    assert messages == []
    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run_id)
        assert stored is not None
        assert stored.notified_at is not None


async def test_cancel_all_marks_every_queued_run(app: FastAPI) -> None:
    async with app.state.database.session_factory() as session:
        first = Account(label="stop-one", ownership_confirmed=True)
        second = Account(label="stop-two", ownership_confirmed=True)
        target = Target(chat_ref="@stop", title="Stop", approved_by_operator=True)
        session.add_all([first, second, target])
        await session.flush()
        campaign = Campaign(
            name="Stop campaign",
            primary_account_id=first.id,
            reply_account_id=second.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="First",
            reply_text="Reply",
            reply_delay_seconds=0,
        )
        session.add(campaign)
        await session.flush()
        campaign_id = campaign.id
        await session.commit()

    first_run = await app.state.run_queue.enqueue(campaign_id, "stop-request-one")
    second_run = await app.state.run_queue.enqueue(campaign_id, "stop-request-two")
    assert await app.state.run_queue.cancel_all() == 2

    async with app.state.database.session_factory() as session:
        stored_first = await session.get(CampaignRun, first_run.id)
        stored_second = await session.get(CampaignRun, second_run.id)
        assert stored_first.status == RunStatus.CANCELLED
        assert stored_second.status == RunStatus.CANCELLED


async def test_sqlite_backup_and_doctor(app: FastAPI, tmp_path) -> None:
    database_url = app.state.settings.database_url
    async with app.state.database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "UPDATE alembic_version SET version_num = '0004_run_notifications'"
        )

    report = inspect_sqlite(database_url)
    assert report.healthy is False
    assert report.migration_revision == "0004_run_notifications"
    assert report.expected_migration_revision == expected_migration_revision()
    assert report.migration_current is False
    assert report.missing_columns == []

    async with app.state.database.engine.begin() as connection:
        await connection.exec_driver_sql("DROP TABLE account_operation_leases")

    incomplete_report = inspect_sqlite(database_url)
    assert incomplete_report.healthy is False
    assert "account_operation_leases" in incomplete_report.missing_tables

    backup = create_sqlite_backup(database_url, tmp_path / "backups")
    assert backup.is_file()
    verification = verify_sqlite_backup(backup)
    assert verification.integrity == "ok"
    assert verification.foreign_key_violations == 0
    assert verification.size_bytes == backup.stat().st_size
    assert len(verification.sha256) == 64
    with sqlite3.connect(backup) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    assert integrity == "ok"


async def test_media_gc_preserves_referenced_files_and_requires_explicit_apply(
    app: FastAPI,
    tmp_path,
) -> None:
    storage = tmp_path / "media"
    storage.mkdir()
    campaign_media = storage / "campaign.bin"
    variant_media = storage / "variant.bin"
    snapshot_media = storage / "snapshot.bin"
    direct_campaign_media = storage / "direct-campaign.bin"
    direct_snapshot_media = storage / "direct-snapshot.bin"
    comment_media = storage / "comment.bin"
    comment_dialogue_media = storage / "comment-dialogue.bin"
    orphan = storage / "orphan.bin"
    recent = storage / "recent.bin"
    for media_path in (
        campaign_media,
        variant_media,
        snapshot_media,
        direct_campaign_media,
        direct_snapshot_media,
        comment_media,
        comment_dialogue_media,
        orphan,
        recent,
    ):
        media_path.write_bytes(b"media")
    old_timestamp = time.time() - 48 * 3600
    os.utime(orphan, (old_timestamp, old_timestamp))

    async with app.state.database.session_factory() as session:
        account = Account(label="media-gc", ownership_confirmed=True)
        target = Target(chat_ref="@media_gc", title="Media GC", approved_by_operator=True)
        session.add_all([account, target])
        await session.flush()
        campaign = Campaign(
            name="Media GC campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            target_list_id=None,
            primary_text="Media",
            reply_text="",
            primary_media_path=str(campaign_media),
            content_variants_json=json.dumps({"media": [{"path": str(variant_media)}]}),
        )
        session.add(campaign)
        await session.flush()
        session.add(
            CampaignRun(
                campaign_id=campaign.id,
                target_id=target.id,
                request_key="media-gc-run",
                status=RunStatus.SUCCEEDED,
                next_step=RunStep.PRIMARY,
                primary_media_path_snapshot=str(snapshot_media),
            )
        )
        subscriber_list = SubscriberList(
            name="Media GC subscribers", status=SubscriberListStatus.READY
        )
        session.add(subscriber_list)
        await session.flush()
        direct_campaign = DirectCampaign(
            name="Media GC direct campaign",
            subscriber_list_id=subscriber_list.id,
            primary_text="Media",
            primary_media_path=str(direct_campaign_media),
        )
        session.add(direct_campaign)
        await session.flush()
        session.add(
            DirectLaunch(
                campaign_id=direct_campaign.id,
                subscriber_list_id=subscriber_list.id,
                segment_snapshot=SubscriberSegment.ALL,
                recipient_limit_snapshot=1,
                contact_cooldown_days_snapshot=30,
                account_ids_snapshot_json="[]",
                primary_text_snapshot="Media",
                primary_media_path_snapshot=str(direct_snapshot_media),
                consent_confirmed_at=utc_now(),
            )
        )
        session.add(
            ChannelCommentRule(
                name="Media GC comments",
                channel_ref="@media_gc_comments",
                primary_account_id=account.id,
                primary_text="Media",
                primary_media_path=str(comment_media),
                dialogue_steps_json=json.dumps(
                    [{"account_id": account.id, "media_path": str(comment_dialogue_media)}]
                ),
            )
        )
        await session.commit()

    database_url = app.state.settings.database_url
    preview = cleanup_media_orphans(database_url, storage, min_age_hours=24)
    assert preview.candidate_files == [str(orphan.resolve())]
    assert preview.removed_files == []
    assert orphan.exists()
    assert recent.exists()

    applied = cleanup_media_orphans(database_url, storage, min_age_hours=24, apply=True)
    assert applied.removed_files == [str(orphan.resolve())]
    assert not orphan.exists()
    assert campaign_media.exists()
    assert variant_media.exists()
    assert snapshot_media.exists()
    assert direct_campaign_media.exists()
    assert direct_snapshot_media.exists()
    assert comment_media.exists()
    assert comment_dialogue_media.exists()
    assert recent.exists()


async def test_clean_database_keeps_accounts_proxies_and_chats_only(
    app: FastAPI,
    tmp_path,
) -> None:
    async with app.state.database.session_factory() as session:
        account = Account(
            label="clean-account",
            ownership_confirmed=True,
            session_encrypted="encrypted-session",
        )
        group = ProxyGroup(name="clean-proxies", description="Retained configuration")
        target = Target(
            chat_ref="@clean_chat",
            title="Clean chat",
            is_standalone=False,
            approved_by_operator=False,
        )
        session.add_all([account, group, target])
        await session.flush()
        proxy = ProxyEndpoint(
            label="clean-proxy",
            group_id=group.id,
            protocol=ProxyProtocol.SOCKS5,
            host="127.0.0.1",
            port=1080,
            fingerprint="a" * 64,
        )
        target_list = TargetList(name="Clean source list")
        session.add_all([proxy, target_list])
        await session.flush()
        session.add_all(
            [
                ProxyAssignment(
                    proxy_id=proxy.id,
                    assignment_key=f"account:{account.id}",
                    purpose="account route",
                ),
                TargetListItem(
                    target_list_id=target_list.id,
                    target_id=target.id,
                    enabled=True,
                ),
            ]
        )
        await session.commit()

    async with app.state.database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "UPDATE alembic_version SET version_num = ?",
            (expected_migration_revision(),),
        )

    destination = tmp_path / "proxima-clean.db"
    report = create_clean_database(app.state.settings.database_url, destination)
    assert report.copied_rows == {
        "accounts": 1,
        "proxy_groups": 1,
        "proxy_endpoints": 1,
        "proxy_assignments": 1,
        "targets": 1,
    }
    assert inspect_sqlite(f"sqlite+aiosqlite:///{destination}").healthy is True

    with sqlite3.connect(destination) as connection:
        assert connection.execute("SELECT COUNT(*) FROM campaigns").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM campaign_runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM target_lists").fetchone()[0] == 0
        blacklist_count = connection.execute(
            "SELECT COUNT(*) FROM mention_blacklist_entries"
        ).fetchone()[0]
        assert blacklist_count == 1057
        target = connection.execute(
            "SELECT is_standalone, approved_by_operator FROM targets WHERE chat_ref = '@clean_chat'"
        ).fetchone()
        assert target == (1, 1)
        assignment = connection.execute("SELECT assignment_key FROM proxy_assignments").fetchone()
        assert assignment == ("account:1",)
