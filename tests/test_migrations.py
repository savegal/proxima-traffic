import asyncio
import logging
import shutil

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from proxima_traffic.config import get_settings
from proxima_traffic.db import Database
from proxima_traffic.models import (
    Account,
    ActionKind,
    ActionLog,
    ActionStatus,
    Base,
    Campaign,
    CampaignMode,
    CampaignRun,
    DirectCampaign,
    DirectLaunch,
    DirectMessageTask,
    DirectMessageTaskStatus,
    RunStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberList,
    SubscriberSegment,
    Target,
    utc_now,
)


def test_account_pause_migration_preserves_manual_and_protective_states(
    tmp_path,
    schema_template,
    monkeypatch,
) -> None:
    path = tmp_path / "account-pause.db"
    shutil.copy2(schema_template, path)
    monkeypatch.setenv("PROXIMA_DATABASE_URL", f"sqlite+aiosqlite:///{path.as_posix()}")
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.downgrade(config, "0077_campaign_execution_safety")
        engine = create_engine(f"sqlite:///{path.as_posix()}")
        try:
            with engine.begin() as connection:
                for aid, reason in [(1, None), (2, "flood_wait"), (3, "spambot_temp_ban")]:
                    connection.execute(
                        text(
                            "INSERT INTO accounts (id, label, status, session_source, "
                            "ownership_confirmed, "
                            "authorized, has_profile_photo, connection_generation, sent_today, "
                            "flood_hits, sent_since_break, joins_today, created_at) "
                            "VALUES (:id, :label, 'PAUSED', 'NONE', 0, 0, 0, 0, 0, 0, 0, 0, "
                            "CURRENT_TIMESTAMP)"
                        ),
                        {"id": aid, "label": f"account-{aid}"},
                    )
                    connection.execute(
                        text("UPDATE accounts SET cooldown_reason=:reason WHERE id=:id"),
                        {"id": aid, "reason": reason},
                    )
            command.upgrade(config, "head")
            with engine.connect() as connection:
                rows = connection.execute(
                    text(
                        "SELECT id, operator_paused, status, cooldown_reason "
                        "FROM accounts ORDER BY id"
                    )
                ).all()
            assert rows == [
                (1, 1, "PAUSED", None),
                (2, 0, "PAUSED", "flood_wait"),
                (3, 0, "PAUSED", "spambot_temp_ban"),
            ]
        finally:
            engine.dispose()
    finally:
        get_settings.cache_clear()


def test_legacy_story_campaigns_are_paused_without_guessing_delivery_mode(tmp_path, monkeypatch):
    path = tmp_path / "legacy-stories.db"
    monkeypatch.setenv("PROXIMA_DATABASE_URL", f"sqlite+aiosqlite:///{path.as_posix()}")
    get_settings.cache_clear()
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    try:
        config = Config("alembic.ini")
        command.upgrade(config, "0080_story_viewer")
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO subscriber_lists (id, name, status, created_at, updated_at) "
                "VALUES (1, 'old', 'READY', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ))
            connection.execute(text(
                "INSERT INTO story_tagging_campaigns (name, subscriber_list_id, media_path, status) "
                "VALUES ('old tags', 1, 'story.jpg', 'ACTIVE')"
            ))
            connection.execute(text(
                "INSERT INTO story_viewer_campaigns (name, subscriber_list_id, status) "
                "VALUES ('old views', 1, 'ACTIVE')"
            ))
        command.upgrade(config, "head")
        with engine.connect() as connection:
            for table in ("story_tagging_campaigns", "story_viewer_campaigns"):
                assert connection.execute(text(f"SELECT status, simulated FROM {table}")).one() == (
                    "PAUSED", None,
                )
            assert connection.scalar(text("PRAGMA integrity_check")) == "ok"
            assert connection.execute(text("PRAGMA foreign_key_check")).all() == []
    finally:
        engine.dispose()
        get_settings.cache_clear()


def test_alembic_head_matches_sqlalchemy_metadata(tmp_path, monkeypatch) -> None:
    """Migration regressions must not hide behind create_all-based unit fixtures."""

    database_path = tmp_path / "migrated.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("PROXIMA_DATABASE_URL", database_url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.upgrade(config, "head")
        command.check(config)

        engine = create_engine(f"sqlite:///{database_path.as_posix()}")
        try:
            database_tables = set(inspect(engine).get_table_names())
        finally:
            engine.dispose()
        assert set(Base.metadata.tables).issubset(database_tables)
    finally:
        get_settings.cache_clear()


def test_campaign_dispatch_migration_preserves_history_and_fences_legacy_running(
    tmp_path,
    schema_template,
    monkeypatch,
) -> None:
    path = tmp_path / "legacy-chat-delivery.db"
    shutil.copy2(schema_template, path)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"

    async def prepare() -> None:
        database = Database(url)
        try:
            async with database.session_factory() as session:
                account = Account(label="Legacy sender")
                target = Target(chat_ref="@legacy_chat", title="Legacy chat")
                session.add_all([account, target])
                await session.flush()
                campaign = Campaign(
                    name="Legacy broadcast",
                    mode=CampaignMode.BROADCAST,
                    primary_account_id=account.id,
                    target_id=target.id,
                    primary_text="Hello",
                )
                session.add(campaign)
                await session.flush()
                for index, status in enumerate(
                    [RunStatus.RUNNING, RunStatus.QUEUED, RunStatus.SUCCEEDED]
                ):
                    run = CampaignRun(
                        campaign_id=campaign.id,
                        target_id=target.id,
                        status=status,
                        request_key=f"legacy-chat-{index}",
                        started_at=utc_now(),
                    )
                    session.add(run)
                    await session.flush()
                    if status == RunStatus.SUCCEEDED:
                        session.add(
                            ActionLog(
                                run_id=run.id,
                                account_id=account.id,
                                target_id=target.id,
                                kind=ActionKind.PRIMARY_MESSAGE,
                                status=ActionStatus.SUCCEEDED,
                                simulated=False,
                                external_message_id=1234,
                                text="History kept",
                            )
                        )
                await session.commit()
        finally:
            await database.dispose()

    asyncio.run(prepare())
    monkeypatch.setenv("PROXIMA_DATABASE_URL", url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.downgrade(config, "0076_comment_dispatch_safety")
        command.upgrade(config, "head")
        engine = create_engine(f"sqlite:///{path.as_posix()}")
        try:
            with engine.connect() as connection:
                rows = connection.execute(
                    text(
                        "SELECT status, dispatch_started_at, simulated_snapshot "
                        "FROM campaign_runs ORDER BY id"
                    )
                ).all()
                assert rows[0][0] == "RUNNING" and rows[0][1] is not None
                assert all(row[1] is None for row in rows[1:])
                assert all(row[2] is None for row in rows)
                action = connection.execute(
                    text(
                        "SELECT text, external_message_id, simulated, delayed_delivery_status "
                        "FROM action_logs"
                    )
                ).one()
                assert tuple(action) == ("History kept", 1234, 0, None)
        finally:
            engine.dispose()
    finally:
        get_settings.cache_clear()


def test_service_action_migration_preserves_ordinary_direct_history(
    tmp_path,
    schema_template,
    monkeypatch,
) -> None:
    path = tmp_path / "legacy-direct.db"
    shutil.copy2(schema_template, path)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"

    async def prepare() -> None:
        database = Database(url)
        try:
            async with database.session_factory() as session:
                account = Account(label="Legacy sender")
                subscriber_list = SubscriberList(name="Legacy people")
                subscriber = Subscriber(telegram_user_id=991234, username="legacy_person")
                session.add_all([account, subscriber_list, subscriber])
                await session.flush()
                campaign = DirectCampaign(
                    name="Legacy LS",
                    subscriber_list_id=subscriber_list.id,
                    primary_text="Legacy message",
                )
                session.add(campaign)
                await session.flush()
                launch = DirectLaunch(
                    campaign_id=campaign.id,
                    subscriber_list_id=subscriber_list.id,
                    segment_snapshot=SubscriberSegment.ALL,
                    recipient_limit_snapshot=1,
                    contact_cooldown_days_snapshot=30,
                    account_ids_snapshot_json=f"[{account.id}]",
                    simulated_snapshot=False,
                    primary_text_snapshot="Legacy message",
                    consent_confirmed_at=utc_now(),
                )
                session.add(launch)
                await session.flush()
                session.add(
                    DirectMessageTask(
                        launch_id=launch.id,
                        subscriber_id=subscriber.id,
                        sender_account_id=account.id,
                        status=DirectMessageTaskStatus.SUCCEEDED,
                        external_message_id=777,
                    )
                )
                await session.commit()
        finally:
            await database.dispose()

    asyncio.run(prepare())
    monkeypatch.setenv("PROXIMA_DATABASE_URL", url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.downgrade(config, "0073_campaign_delivery_mode")
        command.upgrade(config, "head")
        engine = create_engine(f"sqlite:///{path.as_posix()}")
        try:
            with engine.connect() as connection:
                assert tuple(
                    connection.execute(
                        text(
                            "SELECT primary_text, operation_kind, auto_delete_seconds, "
                            "service_account_ids_json FROM direct_campaigns"
                        )
                    ).one()
                ) == ("Legacy message", "message", None, None)
                assert tuple(
                    connection.execute(
                        text(
                            "SELECT primary_text_snapshot, operation_kind_snapshot, "
                            "auto_delete_seconds_snapshot FROM direct_launches"
                        )
                    ).one()
                ) == ("Legacy message", "message", None)
                assert tuple(
                    connection.execute(
                        text("SELECT external_message_id, result_code FROM direct_message_tasks")
                    ).one()
                ) == (777, None)
        finally:
            engine.dispose()
    finally:
        get_settings.cache_clear()


def test_invite_snapshot_migration_preserves_history_without_inventing_evidence(
    tmp_path,
    schema_template,
    monkeypatch,
) -> None:
    path = tmp_path / "legacy-invites.db"
    shutil.copy2(schema_template, path)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"

    async def prepare() -> None:
        database = Database(url)
        try:
            async with database.session_factory() as session:
                people = SubscriberList(name="Legacy inviting")
                session.add(people)
                await session.flush()
                campaign = SubscriberInviteCampaign(
                    name="Legacy invite",
                    subscriber_list_id=people.id,
                    destination_ref="@owned_channel",
                    destination_key="@owned_channel",
                )
                session.add(campaign)
                await session.flush()
                session.add(
                    SubscriberInviteLaunch(
                        campaign_id=campaign.id,
                        subscriber_list_id=people.id,
                        destination_ref_snapshot="@owned_channel",
                        destination_key_snapshot="@owned_channel",
                        segment_snapshot=SubscriberSegment.ALL,
                        recipient_limit_snapshot=1,
                        contact_cooldown_days_snapshot=30,
                        account_ids_snapshot_json="[17]",
                        consent_confirmed_at=utc_now(),
                        simulated_snapshot=False,
                        total_tasks=1,
                    )
                )
                await session.commit()
        finally:
            await database.dispose()

    asyncio.run(prepare())
    monkeypatch.setenv("PROXIMA_DATABASE_URL", url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.downgrade(config, "0074_direct_service_actions")
        command.upgrade(config, "head")
        engine = create_engine(f"sqlite:///{path.as_posix()}")
        try:
            with engine.connect() as connection:
                assert tuple(
                    connection.execute(
                        text(
                            "SELECT destination_ref_snapshot, account_ids_snapshot_json, "
                            "total_tasks, destination_telegram_id_snapshot, "
                            "selected_account_ids_snapshot_json, preflight_failures_snapshot_json "
                            "FROM subscriber_invite_launches"
                        )
                    ).one()
                ) == ("@owned_channel", "[17]", 1, None, None, None)
        finally:
            engine.dispose()
    finally:
        get_settings.cache_clear()


def test_create_schema_runs_data_migrations_and_serializes_first_start(tmp_path) -> None:
    """Fresh app instances must receive seed data without a startup race."""

    database_path = tmp_path / "fresh.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    application_logger = logging.getLogger("proxima_traffic.migration_regression")
    application_logger.disabled = False

    async def initialize() -> list[int]:
        first = Database(database_url)
        second = Database(database_url)
        try:
            await asyncio.gather(first.create_schema(), second.create_schema())
            assert application_logger.disabled is False
            async with first.engine.connect() as connection:
                count = int(
                    await connection.scalar(text("SELECT COUNT(*) FROM mention_blacklist_entries"))
                    or 0
                )
                revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
                return [
                    count,
                    int(revision == "0081_story_execution_reliability"),
                    int(await first.ready()),
                ]
        finally:
            await first.dispose()
            await second.dispose()

    assert asyncio.run(initialize()) == [1057, 1, 1]


def test_migrate_to_head_if_needed_skips_a_database_that_is_already_ready(tmp_path) -> None:
    database_path = tmp_path / "ready.db"
    database = Database(f"sqlite+aiosqlite:///{database_path.as_posix()}")

    async def initialize() -> bool:
        try:
            await database.create_schema()

            async def unexpected_upgrade() -> None:
                raise AssertionError("a ready database must not rerun Alembic")

            database.create_schema = unexpected_upgrade  # type: ignore[method-assign]
            return await database.migrate_to_head_if_needed()
        finally:
            await database.dispose()

    assert asyncio.run(initialize()) is False


def test_delivery_mode_migration_preserves_legacy_receipts_without_guessing_their_mode(
    tmp_path,
    schema_template,
    monkeypatch,
) -> None:
    path = tmp_path / "legacy-receipts.db"
    shutil.copy2(schema_template, path)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"

    async def prepare() -> None:
        database = Database(url)
        try:
            async with database.session_factory() as session:
                account = Account(label="Legacy sender")
                target = Target(chat_ref="@legacy", title="Legacy target")
                session.add_all([account, target])
                await session.flush()
                campaign = Campaign(
                    name="Legacy campaign",
                    mode=CampaignMode.BROADCAST,
                    primary_account_id=account.id,
                    target_id=target.id,
                    primary_text="Legacy text",
                )
                session.add(campaign)
                await session.flush()
                run = CampaignRun(
                    campaign_id=campaign.id,
                    target_id=target.id,
                    request_key="legacy",
                    scheduled_for=utc_now(),
                )
                session.add(run)
                await session.flush()
                session.add(
                    ActionLog(
                        run_id=run.id,
                        account_id=account.id,
                        target_id=target.id,
                        kind=ActionKind.PRIMARY_MESSAGE,
                        status=ActionStatus.SUCCEEDED,
                        external_message_id=123,
                        simulated=False,
                        text="Legacy text",
                    )
                )
                await session.commit()
        finally:
            await database.dispose()

    asyncio.run(prepare())
    monkeypatch.setenv("PROXIMA_DATABASE_URL", url)
    get_settings.cache_clear()
    try:
        config = Config("alembic.ini")
        command.downgrade(config, "0072_recipient_report_indexes")
        command.upgrade(config, "head")
        engine = create_engine(f"sqlite:///{path.as_posix()}")
        try:
            with engine.connect() as connection:
                row = connection.execute(
                    text("SELECT external_message_id, text, simulated FROM action_logs")
                ).one()
                assert tuple(row) == (123, "Legacy text", None)
                assert connection.scalar(text("SELECT COUNT(*) FROM campaign_runs")) == 1
        finally:
            engine.dispose()
    finally:
        get_settings.cache_clear()
