import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select

from proxima_traffic.bot.renderers import launch_details_text
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.config import Settings
from proxima_traffic.gateways import SentMessage, TemporaryGatewayError
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    UTC,
    Account,
    AccountStatus,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    MembershipStatus,
    RunStatus,
    Target,
    TargetAccountState,
    TargetList,
    TargetListItem,
    ensure_utc,
    utc_now,
)
from proxima_traffic.proxies import ProxyRouteUnavailable
from proxima_traffic.scheduling import (
    AccountCapacitySlot,
    broadcast_profile,
    detect_broadcast_profile,
)
from proxima_traffic.services import RunQueue


class SlowModeGateway:
    async def send_message(self, **kwargs: object) -> SentMessage:
        raise TemporaryGatewayError("slow mode 120s", retry_after=120, code="slow_mode")


class FloodWaitGateway:
    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise TemporaryGatewayError("flood wait 120s", retry_after=120, code="flood_wait")


class BlockingCountingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.two_started = asyncio.Event()
        self.calls = 0

    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        self.calls += 1
        self.started.set()
        if self.calls >= 2:
            self.two_started.set()
        await self.release.wait()
        return SentMessage(message_id=9000 + self.calls)


def test_broadcast_profiles_are_complete_and_detectable() -> None:
    active = broadcast_profile("active")
    balanced = broadcast_profile("balanced")
    gentle = broadcast_profile("gentle")

    assert active.interval_minutes < balanced.interval_minutes < gentle.interval_minutes
    assert active.interval_minutes == 5
    assert active.min_messages_between_posts == 10
    assert active.activity_check_interval_seconds == 60
    assert active.visibility_max_wait_seconds == 15 * 60
    assert (
        active.min_messages_between_posts
        < balanced.min_messages_between_posts
        < gentle.min_messages_between_posts
    )
    assert detect_broadcast_profile(
        duration_seconds=active.duration_hours * 3600,
        interval_seconds=active.interval_minutes * 60,
        min_messages_between_posts=active.min_messages_between_posts,
        activity_check_interval_seconds=active.activity_check_interval_seconds,
        visibility_max_wait_seconds=active.visibility_max_wait_seconds,
    ) == active


@pytest.mark.asyncio
async def test_spread_delay_distribution() -> None:
    assert RunQueue._spread_delay_seconds(86400, 0, 3) == 0
    assert RunQueue._spread_delay_seconds(86400, 1, 3) == 43200
    assert RunQueue._spread_delay_seconds(86400, 2, 3) == 86400
    assert RunQueue._spread_delay_seconds(0, 1, 3) == 0


@pytest.mark.asyncio
async def test_partial_preflight_launch_skips_only_unready_chats(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'partial-preflight.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    now = utc_now()
    try:
        async with app.state.database.session_factory() as session:
            first = Account(label="partial-first", ownership_confirmed=True)
            second = Account(label="partial-second", ownership_confirmed=True)
            target_list = TargetList(name="partial-ready-targets")
            ready_target = Target(
                chat_ref="@partial_ready",
                title="Ready chat",
                approved_by_operator=True,
                approved_at=now,
            )
            blocked_target = Target(
                chat_ref="@partial_blocked",
                title="Blocked chat",
                approved_by_operator=True,
                approved_at=now,
            )
            session.add_all([first, second, target_list, ready_target, blocked_target])
            await session.flush()
            session.add_all(
                [
                    TargetListItem(
                        target_list_id=target_list.id,
                        target_id=ready_target.id,
                        enabled=True,
                    ),
                    TargetListItem(
                        target_list_id=target_list.id,
                        target_id=blocked_target.id,
                        enabled=True,
                    ),
                ]
            )
            campaign = Campaign(
                name="partial preflight launch",
                mode=CampaignMode.BROADCAST,
                primary_account_id=first.id,
                target_list_id=target_list.id,
                primary_text="Promotion",
                sender_mode=BroadcastSenderMode.DISTRIBUTED,
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                CampaignAccount(campaign_id=campaign.id, account_id=account.id)
                for account in (first, second)
            )
            session.add_all(
                [
                    TargetAccountState(
                        target_id=ready_target.id,
                        account_id=first.id,
                        membership_status=MembershipStatus.MEMBER,
                        can_send=True,
                        membership_checked_at=now,
                    ),
                    TargetAccountState(
                        target_id=blocked_target.id,
                        account_id=first.id,
                        membership_status=MembershipStatus.BANNED,
                        can_send=False,
                        membership_checked_at=now,
                        send_blocked=True,
                    ),
                    TargetAccountState(
                        target_id=blocked_target.id,
                        account_id=second.id,
                        membership_status=MembershipStatus.UNAVAILABLE,
                        can_send=False,
                        membership_checked_at=now,
                    ),
                ]
            )
            await session.commit()
            campaign_id = int(campaign.id)
            ready_target_id = int(ready_target.id)
            blocked_target_id = int(blocked_target.id)

        queue = RunQueue(
            app.state.database.session_factory,
            lease_seconds=30,
            max_attempts=5,
            retry_base_seconds=2,
            preflight_required=True,
            preflight_max_age_seconds=3600,
        )
        runs = await queue.enqueue_campaign(campaign_id, "partial-preflight-launch")
        by_target = {run.target_id: run for run in runs}

        assert by_target[ready_target_id].status == RunStatus.QUEUED
        assert by_target[blocked_target_id].status == RunStatus.CANCELLED
        assert by_target[blocked_target_id].error_code == "preflight_no_ready_sender"
        assert by_target[blocked_target_id].attempt_count == 0

        repository = AdminRepository(app.state.database.session_factory)
        launch, launch_runs = await repository.get_launch(max(run.id for run in runs))
        targets = await repository.targets_by_ids({run.target_id for run in launch_runs})
        report = launch_details_text(launch, launch_runs, targets)
        assert "Не готовы при проверке: <b>1</b>" in report
        assert "остальные запущены" in report
        assert "Blocked chat" in report
    finally:
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_smart_sender_selection_prefers_account_with_free_daily_limit(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'smart-sender-limit.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    now = utc_now()
    try:
        async with app.state.database.session_factory() as session:
            exhausted = Account(
                label="daily-limit-full",
                ownership_confirmed=True,
                sent_today=5,
                day_window_start=now - timedelta(hours=1),
            )
            stale_but_free = Account(
                label="daily-limit-stale-route",
                ownership_confirmed=True,
            )
            available = Account(label="daily-limit-free", ownership_confirmed=True)
            target = Target(
                chat_ref="@smart_sender_limit",
                title="Smart sender limit",
                approved_by_operator=True,
                approved_at=now,
            )
            session.add_all([exhausted, stale_but_free, available, target])
            await session.flush()
            campaign = Campaign(
                name="smart sender daily capacity",
                mode=CampaignMode.BROADCAST,
                primary_account_id=exhausted.id,
                target_id=target.id,
                primary_text="Promotion",
                sender_mode=BroadcastSenderMode.DISTRIBUTED,
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                CampaignAccount(campaign_id=campaign.id, account_id=account.id)
                for account in (exhausted, stale_but_free, available)
            )
            session.add_all(
                TargetAccountState(
                    target_id=target.id,
                    account_id=account.id,
                    membership_status=MembershipStatus.MEMBER,
                    can_send=True,
                    membership_checked_at=(
                        now - timedelta(hours=2) if account is stale_but_free else now
                    ),
                )
                for account in (exhausted, stale_but_free, available)
            )
            await session.commit()
            campaign_id = int(campaign.id)
            available_id = int(available.id)

        queue = RunQueue(
            app.state.database.session_factory,
            lease_seconds=30,
            max_attempts=5,
            retry_base_seconds=2,
            account_daily_limit=5,
            preflight_required=True,
        )
        runs = await queue.enqueue_campaign(campaign_id, "smart-sender-free-limit")

        assert len(runs) == 1
        assert runs[0].sender_account_id == available_id
        assert ensure_utc(runs[0].scheduled_for) < now + timedelta(minutes=1)
    finally:
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_smart_sender_selection_excludes_account_without_proxy(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'smart-sender-proxy.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    try:
        async with app.state.database.session_factory() as session:
            without_proxy = Account(
                label="sender-without-proxy",
                ownership_confirmed=True,
                authorized=True,
                session_encrypted="encrypted-session-a",
                api_id=1001,
                api_hash_encrypted="encrypted-hash-a",
            )
            with_proxy = Account(
                label="sender-with-proxy",
                ownership_confirmed=True,
                authorized=True,
                session_encrypted="encrypted-session-b",
                api_id=1002,
                api_hash_encrypted="encrypted-hash-b",
            )
            target = Target(
                chat_ref="@smart_sender_proxy",
                title="Smart sender proxy",
                approved_by_operator=True,
                approved_at=utc_now(),
            )
            session.add_all([without_proxy, with_proxy, target])
            await session.flush()
            campaign = Campaign(
                name="smart sender proxy route",
                mode=CampaignMode.BROADCAST,
                primary_account_id=without_proxy.id,
                target_id=target.id,
                primary_text="Promotion",
                sender_mode=BroadcastSenderMode.DISTRIBUTED,
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                CampaignAccount(campaign_id=campaign.id, account_id=account.id)
                for account in (without_proxy, with_proxy)
            )
            await session.commit()
            campaign_id = int(campaign.id)
            without_proxy_id = int(without_proxy.id)
            with_proxy_id = int(with_proxy.id)

        class ProxyRoutes:
            async def require_account_route(self, account_id: int) -> object:
                if account_id == without_proxy_id:
                    raise ProxyRouteUnavailable("proxy unavailable")
                return object()

        queue = RunQueue(
            app.state.database.session_factory,
            lease_seconds=30,
            max_attempts=5,
            retry_base_seconds=2,
            require_live_session=True,
            require_proxy_route=True,
            proxy_service=ProxyRoutes(),  # type: ignore[arg-type]
        )
        runs = await queue.enqueue_campaign(campaign_id, "smart-sender-healthy-proxy")

        assert len(runs) == 1
        assert runs[0].sender_account_id == with_proxy_id
        assert json.loads(runs[0].sender_pool_snapshot_json or "[]") == [with_proxy_id]
    finally:
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_campaign_runner_reads_pacing_from_settings(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'pace.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_pace_min_seconds=30.0,
        account_pace_max_seconds=60.0,
    )
    app = create_app(settings=settings)
    runner = app.state.campaign_runner
    assert runner._pace_min_seconds == 30.0
    assert runner._pace_max_seconds == 60.0
    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_account_daily_limit_pauses_at_200_and_plans_next_window(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'daily-limit.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_daily_limit=200,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    now = utc_now()

    legacy_limit_account = Account(
        label="old-150-limit",
        telegram_user_id=90_150,
        ownership_confirmed=True,
        sent_today=150,
        day_window_start=now,
        cooldown_until=now + timedelta(hours=10),
        cooldown_reason="daily_limit",
    )
    legacy_slot = AccountCapacitySlot.from_account(
        legacy_limit_account,
        now,
        daily_limit=200,
    )
    assert legacy_slot.candidate_at(now, daily_limit=200) == now
    assert app.state.campaign_runner._account_resume_at(legacy_limit_account) is None

    async with app.state.database.session_factory() as session:
        account = Account(
            label="daily-limit",
            telegram_user_id=90_200,
            ownership_confirmed=True,
            sent_today=199,
            day_window_start=now,
        )
        session.add(account)
        await session.commit()
        await session.refresh(account)

        slot = AccountCapacitySlot.from_account(account, now, daily_limit=200)
        assert slot.candidate_at(now, daily_limit=200) == now
        slot.reserve(
            now,
            pace_seconds=0,
            micro_break_every=0,
            micro_break_seconds=0,
        )
        assert slot.candidate_at(now, daily_limit=200) >= now + timedelta(hours=24)

        await app.state.campaign_runner._record_account_send(session, account.id)
        await session.commit()
        await session.refresh(account)
        assert account.sent_today == 200
        assert account.cooldown_reason == "daily_limit"
        assert ensure_utc(account.cooldown_until) >= now + timedelta(hours=23, minutes=59)

    await app.state.database.dispose()


async def test_proxy_outage_is_a_soft_defer(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'proxy-soft-defer.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    error = TemporaryGatewayError(
        "proxy unavailable",
        retry_after=60,
        code="proxy_unavailable",
    )

    assert app.state.campaign_runner._throttle_resume_at(error) is not None
    timer = TemporaryGatewayError(
        "chat timer",
        retry_after=60,
        code="chat_timer",
    )
    assert app.state.campaign_runner._throttle_resume_at(timer) is not None
    await app.state.database.dispose()


async def test_independent_mode_builds_every_account_chat_pair(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'independent-pairs.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    async with app.state.database.session_factory() as session:
        accounts = [
            Account(label=f"lane-{index}", ownership_confirmed=True)
            for index in range(1, 3)
        ]
        target_list = TargetList(name="independent targets")
        targets = [
            Target(
                chat_ref=f"@independent_{index}",
                title=f"Independent {index}",
                approved_by_operator=True,
                approved_at=utc_now(),
            )
            for index in range(1, 3)
        ]
        session.add_all([*accounts, target_list, *targets])
        await session.flush()
        session.add_all(
            TargetListItem(
                target_list_id=target_list.id,
                target_id=target.id,
                enabled=True,
            )
            for target in targets
        )
        campaign = Campaign(
            name="independent pair matrix",
            mode=CampaignMode.BROADCAST,
            primary_account_id=accounts[0].id,
            target_list_id=target_list.id,
            primary_text="Hello",
            sender_mode=BroadcastSenderMode.INDEPENDENT,
            smart_timer_mode=True,
            smart_duration_seconds=3600,
            smart_no_timer_interval_seconds=300,
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            CampaignAccount(campaign_id=campaign.id, account_id=account.id)
            for account in accounts
        )
        expected_pairs = {
            (account.id, target.id) for account in accounts for target in targets
        }
        await session.commit()
        campaign_id = campaign.id

    runs = await app.state.run_queue.enqueue_campaign(campaign_id, "independent-pair-matrix")
    assert len(runs) == 4
    assert {(run.sender_account_id, run.target_id) for run in runs} == expected_pairs
    assert all(run.sender_mode_snapshot == BroadcastSenderMode.INDEPENDENT for run in runs)
    await app.state.database.dispose()


async def test_independent_mode_skips_unavailable_selected_senders(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'independent-available.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    async with app.state.database.session_factory() as session:
        healthy = Account(label="independent-healthy", ownership_confirmed=True)
        paused = Account(
            label="independent-paused",
            ownership_confirmed=True,
            status=AccountStatus.PAUSED,
        )
        target = Target(
            chat_ref="@independent_available",
            title="Independent available",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all((healthy, paused, target))
        await session.flush()
        campaign = Campaign(
            name="independent available senders",
            mode=CampaignMode.BROADCAST,
            primary_account_id=healthy.id,
            target_id=target.id,
            primary_text="Hello",
            sender_mode=BroadcastSenderMode.INDEPENDENT,
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            (
                CampaignAccount(campaign_id=campaign.id, account_id=healthy.id),
                CampaignAccount(campaign_id=campaign.id, account_id=paused.id),
            )
        )
        await session.commit()
        campaign_id = int(campaign.id)
        healthy_id = int(healthy.id)

    runs = await app.state.run_queue.enqueue_campaign(
        campaign_id,
        "independent-available-senders",
    )

    assert len(runs) == 1
    assert runs[0].sender_account_id == healthy_id
    assert runs[0].status is RunStatus.QUEUED
    await app.state.database.dispose()


async def test_smart_broadcast_builds_finite_timer_aware_cycles(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'smart-cycles.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "smart-first",
                    "telegram_user_id": 8401,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        second = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "smart-second",
                    "telegram_user_id": 8402,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target_list = (
            await client.post(
                "/api/v1/target-lists",
                headers=headers,
                json={"name": "smart-rooms", "chat_refs": ["@smart_1", "@smart_2"]},
            )
        ).json()
        await client.post(
            f"/api/v1/target-lists/{target_list['id']}/approve",
            headers=headers,
        )
        campaign_response = await client.post(
            "/api/v1/campaigns",
            headers=headers,
            json={
                "name": "smart-repeat",
                "mode": "broadcast",
                "primary_account_id": first["id"],
                "sender_account_ids": [second["id"]],
                "target_list_id": target_list["id"],
                "primary_text": "Hello",
                "spread_seconds": 600,
                "repeat_count": 3,
                "repeat_interval_seconds": 3600,
            },
        )
        assert campaign_response.status_code == 201
        campaign = campaign_response.json()

        runs = await app.state.run_queue.enqueue_campaign(campaign["id"], "smart-repeat-launch")

        assert len(runs) == 6
        assert [run.cycle_index for run in runs] == [0, 0, 1, 1, 2, 2]
        assert [run.sender_account_id for run in runs] == [
            first["id"],
            second["id"],
            first["id"],
            second["id"],
            first["id"],
            second["id"],
        ]
        for target_id in {run.target_id for run in runs}:
            target_runs = [run for run in runs if run.target_id == target_id]
            assert len({run.sender_account_id for run in target_runs}) == 1
            assert (
                3599
                <= (target_runs[1].scheduled_for - target_runs[0].scheduled_for).total_seconds()
                <= 3601
            )
            assert (
                3599
                <= (target_runs[2].scheduled_for - target_runs[1].scheduled_for).total_seconds()
                <= 3601
            )

        replay = await app.state.run_queue.enqueue_campaign(campaign["id"], "smart-repeat-launch")
        assert [run.id for run in replay] == [run.id for run in runs]

    await app.state.database.dispose()


async def test_timer_driven_broadcast_schedules_one_next_post_per_chat(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'timer-driven.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "timer-first",
                    "telegram_user_id": 8611,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        second = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "timer-second",
                    "telegram_user_id": 8612,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target_list = (
            await client.post(
                "/api/v1/target-lists",
                headers=headers,
                json={"name": "timer-targets", "chat_refs": ["@timer_1", "@timer_2"]},
            )
        ).json()
        await client.post(
            f"/api/v1/target-lists/{target_list['id']}/approve",
            headers=headers,
        )
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "timer-driven",
                    "mode": "broadcast",
                    "primary_account_id": first["id"],
                    "sender_account_ids": [second["id"]],
                    "target_list_id": target_list["id"],
                    "primary_text": "Hello",
                    "spread_seconds": 0,
                    "repeat_count": 1,
                    "repeat_interval_seconds": 0,
                    "smart_timer_mode": True,
                    "smart_duration_seconds": 3600,
                    "smart_no_timer_interval_seconds": 900,
                },
            )
        ).json()

    runs = await app.state.run_queue.enqueue_campaign(campaign["id"], "timer-driven-launch")
    assert len(runs) == 2
    scheduled_span = max(run.scheduled_for for run in runs) - min(run.scheduled_for for run in runs)
    assert scheduled_span < timedelta(seconds=1)

    first_run = runs[0]
    allowed_at = utc_now() + timedelta(minutes=15)
    async with app.state.database.session_factory() as session:
        session.add(
            TargetAccountState(
                target_id=first_run.target_id,
                account_id=first_run.sender_account_id,
                next_send_at=allowed_at,
            )
        )
        await session.commit()

    result = await app.state.queue_worker.process_next()
    assert result is not None
    assert result.status == RunStatus.SUCCEEDED

    async with app.state.database.session_factory() as session:
        stored_runs = list(
            await session.scalars(
                select(CampaignRun)
                .where(CampaignRun.launch_key == "timer-driven-launch")
                .order_by(CampaignRun.id)
            )
        )
    follow_up = next(run for run in stored_runs if run.cycle_index == 1)
    assert follow_up.target_id == first_run.target_id
    assert follow_up.sender_account_id == first_run.sender_account_id
    assert ensure_utc(follow_up.scheduled_for) >= allowed_at
    assert follow_up.repeat_until is not None

    await app.state.database.dispose()


async def test_known_chat_timer_delays_new_broadcast_task(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'known-chat-timer.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        account = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "timer-account",
                    "telegram_user_id": 8501,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@timer_room", "title": "Timer room"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "known-timer",
                    "mode": "broadcast",
                    "primary_account_id": account["id"],
                    "target_id": target["id"],
                    "primary_text": "Hello",
                },
            )
        ).json()
        next_send = utc_now() + timedelta(hours=2)
        async with app.state.database.session_factory() as session:
            session.add(
                TargetAccountState(
                    target_id=target["id"],
                    account_id=account["id"],
                    slowmode_seconds=3600,
                    next_send_at=next_send,
                    last_checked_at=utc_now(),
                )
            )
            await session.commit()

        runs = await app.state.run_queue.enqueue_campaign(campaign["id"], "known-timer-launch")

        assert ensure_utc(runs[0].scheduled_for) >= next_send

    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_slow_mode_defers_without_burning_attempt(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'slow.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings, gateway=SlowModeGateway())
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "slow-primary",
                    "telegram_user_id": 801,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        reply = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={"label": "slow-reply", "telegram_user_id": 802, "ownership_confirmed": True},
            )
        ).json()
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@slow_mode_test", "title": "Slow mode"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "slow-mode-test",
                    "primary_account_id": primary["id"],
                    "reply_account_id": reply["id"],
                    "target_id": target["id"],
                    "primary_text": "Hello",
                    "reply_text": "Reply",
                },
            )
        ).json()
        await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "slow-mode-run-001"},
        )
        before = await app.state.run_queue.get_run(1)
        result = await app.state.queue_worker.process_next()
        assert result.status == RunStatus.QUEUED
        assert result.attempt_count == before.attempt_count
        scheduled = result.scheduled_for
        if scheduled.tzinfo is None:
            scheduled = scheduled.replace(tzinfo=UTC)
        assert scheduled > utc_now()

    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_flood_wait_cools_down_account_not_only_current_run(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'flood-cooldown.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
    )
    app = create_app(settings=settings, gateway=FloodWaitGateway())
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "flood-primary",
                    "telegram_user_id": 821,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@flood_wait_test", "title": "Flood wait"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "flood-wait-test",
                    "mode": "broadcast",
                    "primary_account_id": primary["id"],
                    "target_id": target["id"],
                    "primary_text": "Hello",
                },
            )
        ).json()
        await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "flood-wait-run-001"},
        )
        result = await app.state.queue_worker.process_next()
        assert result.status == RunStatus.QUEUED

        async with app.state.database.session_factory() as session:
            account = await session.get(Account, primary["id"])
            assert account is not None
            assert account.cooldown_reason == "flood_wait"
            assert account.cooldown_until is not None
            cooldown_until = account.cooldown_until
            if cooldown_until.tzinfo is None:
                cooldown_until = cooldown_until.replace(tzinfo=UTC)
            assert cooldown_until > utc_now()

    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_capacity_planner_avoids_parallel_due_runs_for_same_account(tmp_path) -> None:
    gateway = BlockingCountingGateway()
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'parallel-pacing.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_pace_min_seconds=3600,
        account_pace_max_seconds=3600,
        worker_step_timeout_seconds=5,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "parallel-primary",
                    "telegram_user_id": 831,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target_list = (
            await client.post(
                "/api/v1/target-lists",
                headers=headers,
                json={"name": "parallel-list", "chat_refs": ["@parallel_1", "@parallel_2"]},
            )
        ).json()
        await client.post(f"/api/v1/target-lists/{target_list['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "parallel-pacing",
                    "mode": "broadcast",
                    "primary_account_id": primary["id"],
                    "target_list_id": target_list["id"],
                    "primary_text": "Hello",
                    "spread_seconds": 0,
                },
            )
        ).json()
        queued = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "parallel-pacing-run"},
        )
        assert queued.status_code == 202
        assert len(queued.json()) == 2

        first = asyncio.create_task(app.state.queue_worker.process_next())
        await gateway.started.wait()
        second = asyncio.create_task(app.state.queue_worker.process_next())
        await asyncio.sleep(0.05)
        gateway.release.set()
        results = await asyncio.gather(first, second)

        assert gateway.calls == 1
        assert [item.status if item is not None else None for item in results] == [
            RunStatus.SUCCEEDED,
            None,
        ]
        async with app.state.database.session_factory() as session:
            remaining = await session.scalar(
                select(CampaignRun).where(CampaignRun.status == RunStatus.QUEUED)
            )
        assert remaining is not None
        assert ensure_utc(remaining.scheduled_for) >= utc_now() + timedelta(minutes=59)

    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_independent_accounts_are_processed_in_parallel(tmp_path) -> None:
    gateway = BlockingCountingGateway()
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'parallel-accounts.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        worker_concurrency=4,
        worker_step_timeout_seconds=5,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        accounts = []
        for index in range(2):
            response = await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": f"parallel-account-{index}",
                    "telegram_user_id": 85_100 + index,
                    "ownership_confirmed": True,
                },
            )
            accounts.append(response.json())
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@parallel_accounts", "title": "Parallel accounts"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "parallel independent accounts",
                    "mode": "broadcast",
                    "primary_account_id": accounts[0]["id"],
                    "sender_account_ids": [accounts[1]["id"]],
                    "sender_mode": "independent",
                    "target_id": target["id"],
                    "primary_text": "Hello",
                },
            )
        ).json()
        launch = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "parallel-independent-accounts"},
        )
        assert len(launch.json()) == 2

    first = asyncio.create_task(app.state.queue_worker.process_next())
    second = asyncio.create_task(app.state.queue_worker.process_next())
    # SQLite and the Windows event loop can need more than one second to claim
    # two concurrent jobs while the full suite is already under load. The
    # assertion still proves both account lanes start before either is released.
    await asyncio.wait_for(gateway.two_started.wait(), timeout=3)
    gateway.release.set()
    results = await asyncio.gather(first, second)

    assert gateway.calls == 2
    assert all(result is not None and result.status == RunStatus.SUCCEEDED for result in results)
    await app.state.database.dispose()


@pytest.mark.asyncio
async def test_live_enqueue_rejects_unauthorized_account(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'live-guard.db'}",
        admin_api_key="test-admin-key",
        dry_run=False,
        worker_enabled=False,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "no-session",
                    "telegram_user_id": 901,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        reply = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "no-session-2",
                    "telegram_user_id": 902,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@live_guard", "title": "Guard"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "live-guard",
                    "primary_account_id": primary["id"],
                    "reply_account_id": reply["id"],
                    "target_id": target["id"],
                    "primary_text": "Hello",
                    "reply_text": "Reply",
                },
            )
        ).json()
        response = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "live-guard-run-001"},
        )
        assert response.status_code == 409
        assert "session" in response.json()["detail"].lower()

    await app.state.database.dispose()


async def _mark_account_live_ready(app, account_id: int) -> None:
    cipher = app.state.account_service._cipher
    from proxima_traffic.models import Account, AccountSessionSource

    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        account.authorized = True
        account.session_source = AccountSessionSource.SESSION_STRING
        account.session_encrypted = cipher.encrypt("1:live-ready-session")
        account.api_id = 12345
        account.api_hash_encrypted = cipher.encrypt("test-api-hash")
        await session.commit()


@pytest.mark.asyncio
async def test_live_enqueue_rejects_account_without_healthy_proxy(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'proxy-guard.db'}",
        admin_api_key="test-admin-key",
        dry_run=False,
        worker_enabled=False,
        account_require_proxy=True,
    )
    app = create_app(settings=settings)
    await app.state.database.create_schema()
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    headers = {"X-Admin-Key": "test-admin-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        primary = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "needs-proxy",
                    "telegram_user_id": 911,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        reply = (
            await client.post(
                "/api/v1/accounts",
                headers=headers,
                json={
                    "label": "needs-proxy-2",
                    "telegram_user_id": 912,
                    "ownership_confirmed": True,
                },
            )
        ).json()
        await _mark_account_live_ready(app, primary["id"])
        await _mark_account_live_ready(app, reply["id"])
        target = (
            await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@proxy_guard", "title": "Proxy guard"},
            )
        ).json()
        await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
        campaign = (
            await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "proxy-guard",
                    "primary_account_id": primary["id"],
                    "reply_account_id": reply["id"],
                    "target_id": target["id"],
                    "primary_text": "Hello",
                    "reply_text": "Reply",
                },
            )
        ).json()
        response = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "proxy-guard-run-001"},
        )
        assert response.status_code == 409
        assert "прокси" in response.json()["detail"].lower()

        proxy_view = await app.state.proxy_service.add_proxy("http://127.0.0.1:9001")
        async with app.state.database.session_factory() as session:
            from proxima_traffic.models import ProxyEndpoint, ProxyStatus

            endpoint = await session.get(ProxyEndpoint, proxy_view.proxy.id)
            assert endpoint is not None
            endpoint.status = ProxyStatus.HEALTHY
            await session.commit()
        await app.state.proxy_service.assign_proxy(proxy_view.proxy.id, f"account:{primary['id']}")
        reply_proxy = await app.state.proxy_service.add_proxy("http://127.0.0.2:9001")
        async with app.state.database.session_factory() as session:
            endpoint = await session.get(ProxyEndpoint, reply_proxy.proxy.id)
            assert endpoint is not None
            endpoint.status = ProxyStatus.HEALTHY
            await session.commit()
        await app.state.proxy_service.assign_proxy(reply_proxy.proxy.id, f"account:{reply['id']}")
        async with app.state.database.session_factory() as session:
            session.add_all(
                TargetAccountState(
                    target_id=target["id"],
                    account_id=account_id,
                    membership_status=MembershipStatus.MEMBER,
                    can_send=True,
                    membership_checked_at=utc_now(),
                )
                for account_id in (primary["id"], reply["id"])
            )
            await session.commit()
        response = await client.post(
            f"/api/v1/campaigns/{campaign['id']}/runs",
            headers=headers,
            json={"request_key": "proxy-guard-run-002"},
        )
        assert response.status_code == 202

    await app.state.database.dispose()
