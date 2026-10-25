import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import delete, select

from proxima_traffic.bot.renderers import launch_details_text
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.config import Settings
from proxima_traffic.content import (
    StoredMedia,
    new_content_variant,
    new_stored_contact,
    render_spintax,
    serialize_content_variants,
)
from proxima_traffic.gateways import (
    ChatActivity,
    PermanentGatewayError,
    SentMessage,
    TargetReadiness,
    TemporaryGatewayError,
)
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    AccountStatus,
    ActionLog,
    Campaign,
    CampaignAccount,
    CampaignRun,
    MembershipStatus,
    RunStatus,
    Target,
    TargetAccountState,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import AccountSendDeferred, ConflictError, RunQueue


def _sent_by(kwargs: dict[str, object]) -> int:
    account = kwargs["account"]
    assert isinstance(account, Account)
    assert account.id is not None
    return account.id


class RecordingGateway:
    def __init__(self) -> None:
        self.account_ids: list[int] = []

    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        return SentMessage(message_id=20_000 + len(self.account_ids))


class ActivityGateway(RecordingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.latest_message_id = 0

    async def inspect_activity(self, **kwargs: object) -> ChatActivity:
        return ChatActivity(latest_message_id=self.latest_message_id, checked_at=utc_now())


class LatestAwareActivityGateway(ActivityGateway):
    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        self.latest_message_id = max(
            self.latest_message_id + 1,
            20_000 + len(self.account_ids),
        )
        return SentMessage(message_id=self.latest_message_id)


class PerAccountActivityGateway(RecordingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.messages_after_by_account: dict[int, int] = {}

    async def inspect_activity(self, **kwargs: object) -> ChatActivity:
        account_id = _sent_by(kwargs)
        last_message_id = int(kwargs["last_message_id"])
        return ChatActivity(
            latest_message_id=(last_message_id + self.messages_after_by_account.get(account_id, 0)),
            checked_at=utc_now(),
        )


class ContentRecordingGateway(RecordingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.content_calls: list[dict[str, object]] = []

    async def send_content(self, **kwargs: object) -> SentMessage:
        self.content_calls.append(kwargs)
        return await self.send_message(**kwargs)


class FailFirstGateway(RecordingGateway):
    def __init__(self, error_code: str) -> None:
        super().__init__()
        self.error_code = error_code

    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        if len(self.account_ids) == 1:
            raise PermanentGatewayError("sender is unavailable", code=self.error_code)
        return SentMessage(message_id=20_000 + len(self.account_ids))


class AlwaysFailGateway(RecordingGateway):
    def __init__(self, error_code: str) -> None:
        super().__init__()
        self.error_code = error_code

    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        raise PermanentGatewayError("sender is unavailable", code=self.error_code)


class StaleAutoJoinMembershipGateway(RecordingGateway):
    """Simulate Telegram evicting a sender after a cached successful route."""

    def __init__(self) -> None:
        super().__init__()
        self.cached_membership = True
        self.invalidations = 0

    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        if self.cached_membership:
            raise PermanentGatewayError(
                "cached membership is no longer valid",
                code="target_not_joined",
            )
        return SentMessage(message_id=20_000 + len(self.account_ids))

    async def invalidate_target_membership(self, **kwargs: object) -> bool:
        del kwargs
        self.invalidations += 1
        if not self.cached_membership:
            return False
        self.cached_membership = False
        return True


class ActiveWindowThrottleGateway(RecordingGateway):
    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise TemporaryGatewayError("slow mode 120s", retry_after=120, code="slow_mode")


class ModerationDeletedGateway(RecordingGateway):
    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        raise PermanentGatewayError(
            "публикация удалена модерацией сразу после отправки; "
            "ответ чата: чтобы писать, подпишитесь на канал",
            code="post_deleted_by_moderation",
            external_message_id=24_001,
        )


class LaterModerationDeletionGateway(LatestAwareActivityGateway):
    async def inspect_activity(self, **kwargs: object) -> ChatActivity:
        last_message_id = int(kwargs["last_message_id"])
        raise PermanentGatewayError(
            "публикация удалена модерацией; ответ чата: пройдите проверку",
            code="post_deleted_by_moderation",
            external_message_id=last_message_id,
        )


class PreflightBannedGateway(RecordingGateway):
    async def inspect_target(self, **kwargs: object) -> TargetReadiness:
        return TargetReadiness(
            membership_status=MembershipStatus.BANNED,
            can_send=False,
            checked_at=utc_now(),
            error_code="account_banned_in_chat",
            error="account is banned in this chat",
        )


class PreflightDeadAccountGateway(RecordingGateway):
    async def inspect_target(self, **kwargs: object) -> TargetReadiness:
        return TargetReadiness(
            membership_status=MembershipStatus.UNKNOWN,
            can_send=False,
            checked_at=utc_now(),
            error_code="account_dead",
            error="session revoked",
        )


class PreflightJoinLimitGateway(RecordingGateway):
    async def inspect_target(self, **kwargs: object) -> TargetReadiness:
        return TargetReadiness(
            membership_status=MembershipStatus.UNAVAILABLE,
            can_send=False,
            checked_at=utc_now(),
            error_code="account_join_limit",
            error="account cannot join another chat",
        )


class BlockingSuccessGateway(RecordingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_message(self, **kwargs: object) -> SentMessage:
        account_id = _sent_by(kwargs)
        self.account_ids.append(account_id)
        self.started.set()
        await self.release.wait()
        return SentMessage(message_id=20_001)


@asynccontextmanager
async def _smart_campaign(
    tmp_path: Path,
    *,
    gateway: object,
    duration_seconds: int = 3600,
    repeat_forever: bool = False,
    no_timer_interval_seconds: int = 900,
    backup_senders: int = 0,
    unselected_senders: int = 0,
    auto_sender_failover: bool = False,
    activity_aware: bool = False,
    min_messages_between_posts: int = 50,
    activity_check_interval_seconds: int = 300,
    visibility_max_wait_seconds: int = 3600,
    sender_mode: str = "distributed",
    account_auto_join: bool = False,
    account_daily_limit: int = 0,
) -> AsyncIterator[tuple[object, dict[str, object], list[dict[str, object]], dict[str, object]]]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'smart-runtime.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=account_daily_limit,
        account_micro_break_every=0,
        account_auto_join=account_auto_join,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    headers = {"X-Admin-Key": "test-admin-key"}
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            accounts: list[dict[str, object]] = []
            for index in range(1 + backup_senders + unselected_senders):
                response = await client.post(
                    "/api/v1/accounts",
                    headers=headers,
                    json={
                        "label": f"smart-sender-{index + 1}",
                        "telegram_user_id": 91_000 + index,
                        "ownership_confirmed": True,
                    },
                )
                assert response.status_code == 201, response.text
                accounts.append(response.json())

            target_response = await client.post(
                "/api/v1/targets",
                headers=headers,
                json={"chat_ref": "@smart_runtime", "title": "Smart runtime"},
            )
            assert target_response.status_code == 201, target_response.text
            target = target_response.json()
            approval = await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
            assert approval.status_code == 200, approval.text

            selected_backups = accounts[1 : 1 + backup_senders]
            campaign_response = await client.post(
                "/api/v1/campaigns",
                headers=headers,
                json={
                    "name": "smart-runtime",
                    "mode": "broadcast",
                    "primary_account_id": accounts[0]["id"],
                    "sender_account_ids": [item["id"] for item in selected_backups],
                    "target_id": target["id"],
                    "primary_text": "Advertisement",
                    "spread_seconds": 0,
                    "repeat_count": 1,
                    "repeat_interval_seconds": 0,
                    "smart_timer_mode": True,
                    "repeat_forever": repeat_forever,
                    "smart_duration_seconds": 0 if repeat_forever else duration_seconds,
                    "smart_no_timer_interval_seconds": no_timer_interval_seconds,
                    "auto_sender_failover": auto_sender_failover,
                    "sender_mode": sender_mode,
                    "activity_aware": activity_aware,
                    "min_messages_between_posts": (
                        min_messages_between_posts if activity_aware else 0
                    ),
                    "activity_check_interval_seconds": activity_check_interval_seconds,
                    "visibility_max_wait_seconds": visibility_max_wait_seconds,
                },
            )
            assert campaign_response.status_code == 201, campaign_response.text
            yield app, campaign_response.json(), accounts, target
    finally:
        await app.state.database.dispose()


async def _launch_runs(app: object, campaign_id: int, launch_key: str) -> list[CampaignRun]:
    return await app.state.run_queue.enqueue_campaign(campaign_id, launch_key)


async def _stored_launch_runs(app: object, launch_key: str) -> list[CampaignRun]:
    async with app.state.database.session_factory() as session:
        return list(
            await session.scalars(
                select(CampaignRun)
                .where(CampaignRun.launch_key == launch_key)
                .order_by(CampaignRun.cycle_index, CampaignRun.id)
            )
        )


@pytest.mark.parametrize("rich_content", [False, True])
@pytest.mark.parametrize("send_failed", [False, True])
async def test_actual_broadcast_attempt_still_paces_other_flows(
    tmp_path, monkeypatch, rich_content, send_failed
):
    gateway = ContentRecordingGateway() if rich_content else RecordingGateway()
    send = AsyncMock(
        side_effect=TemporaryGatewayError("Telegram temporarily unavailable", code="rpc_error")
        if send_failed
        else None,
        return_value=SentMessage(message_id=20001),
    )
    monkeypatch.setattr(gateway, "send_message", send)
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, accounts, _):
        await _launch_runs(app, int(campaign["id"]), "actual-send-pacing")
        app.state.settings.account_pace_min_seconds = 60
        app.state.settings.account_pace_max_seconds = 60
        result = await app.state.queue_worker.process_next()
        assert result is not None
        assert result.status == (RunStatus.QUEUED if send_failed else RunStatus.SUCCEEDED)
        send.assert_awaited_once()
        sender_id = int(accounts[0]["id"])
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, sender_id)
            assert account.sent_today == (0 if send_failed else 1)
            assert ensure_utc(account.next_send_at) >= utc_now() + timedelta(seconds=59)
            assert account.send_reservation_token is None
        with pytest.raises(AccountSendDeferred):
            await app.state.campaign_runner.run_account_send_operation(
                sender_id, lambda: asyncio.sleep(0)
            )


@pytest.mark.asyncio
async def test_independent_accounts_wait_while_managed_post_is_last_in_chat(
    tmp_path: Path,
) -> None:
    gateway = LatestAwareActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        sender_mode="independent",
        activity_aware=False,
    ) as (app, campaign, _, _):
        initial = await _launch_runs(app, int(campaign["id"]), "idle-chat-guard")
        assert len(initial) == 2

        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED
        assert len(gateway.account_ids) == 1

        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None and deferred.status == RunStatus.QUEUED
        assert len(gateway.account_ids) == 1

        gateway.latest_message_id += 1  # one organic message appeared after our post
        async with app.state.database.session_factory() as session:
            stored = await session.get(CampaignRun, deferred.id)
            assert stored is not None
            stored.scheduled_for = utc_now()
            await session.commit()

        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED
        assert len(gateway.account_ids) == 2
        assert gateway.account_ids[0] != gateway.account_ids[1]


@pytest.mark.asyncio
async def test_auto_join_recovers_one_stale_cached_membership_without_blocking_route(
    tmp_path: Path,
) -> None:
    gateway = StaleAutoJoinMembershipGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        account_auto_join=True,
    ) as (app, campaign, accounts, target):
        runs = await _launch_runs(app, int(campaign["id"]), "stale-auto-join-route")

        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None
        assert deferred.id == runs[0].id
        assert deferred.status is RunStatus.QUEUED
        assert deferred.error_code == "target_not_joined"
        assert deferred.attempt_count == 0
        assert gateway.invalidations == 1

        async with app.state.database.session_factory() as session:
            stored = await session.get(CampaignRun, deferred.id)
            assert stored is not None
            stored.scheduled_for = utc_now()
            await session.commit()

        completed = await app.state.queue_worker.process_next()
        assert completed is not None and completed.status is RunStatus.SUCCEEDED
        async with app.state.database.session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == int(accounts[0]["id"]),
                    TargetAccountState.target_id == int(target["id"]),
                )
            )
            assert state is not None
            assert state.membership_status is MembershipStatus.MEMBER
            assert state.can_send is True
            assert state.membership_error_code is None
            assert state.send_blocked is False


@pytest.mark.asyncio
async def test_active_run_is_cancelled_immediately_when_a_throttle_exceeds_its_window(
    tmp_path: Path,
) -> None:
    gateway = ActiveWindowThrottleGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        duration_seconds=60,
    ) as (app, campaign, _, _):
        runs = await _launch_runs(app, int(campaign["id"]), "active-window-throttle")

        result = await app.state.queue_worker.process_next()

        assert result is not None
        assert result.id == runs[0].id
        assert result.status is RunStatus.CANCELLED
        assert result.error_code == "repeat_window_elapsed"
        stored = await _stored_launch_runs(app, "active-window-throttle")
        assert len(stored) == 1
        assert not any(run.status is RunStatus.QUEUED for run in stored)


@pytest.mark.asyncio
async def test_active_launch_rejects_when_daily_capacity_starts_after_its_window(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        duration_seconds=60,
        account_daily_limit=1,
    ) as (app, campaign, accounts, _):
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, int(accounts[0]["id"]))
            assert account is not None
            account.day_window_start = utc_now()
            account.sent_today = 1
            await session.commit()

        with pytest.raises(ConflictError, match="активная рассылка не может стартовать"):
            await _launch_runs(app, int(campaign["id"]), "active-window-no-capacity")

        async with app.state.database.session_factory() as session:
            stored = list(
                await session.scalars(
                    select(CampaignRun).where(
                        CampaignRun.launch_key == "active-window-no-capacity"
                    )
                )
            )
            assert stored == []


@pytest.mark.asyncio
async def test_repeated_target_failures_auto_pause_and_can_be_restored(tmp_path: Path) -> None:
    gateway = AlwaysFailGateway("target_unavailable")
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, target):
        await _launch_runs(app, int(campaign["id"]), "bad-target-0")
        result = await app.state.queue_worker.process_next()
        assert result is not None and result.status == RunStatus.FAILED

        repository = AdminRepository(app.state.database.session_factory)
        bad = await repository.list_bad_targets(0, 10)
        assert bad.total == 1
        rated = bad.items[0]
        assert rated.id == int(target["id"])
        assert rated.quality_status == "bad"
        assert rated.quality_attempts == 1
        assert rated.quality_successes == 0
        assert rated.quality_failures == 1
        assert "Telegram подтвердил" in str(rated.quality_reason)

        with pytest.raises(ConflictError, match="no approved targets"):
            await _launch_runs(app, int(campaign["id"]), "bad-target-blocked")

        restored = await repository.restore_target_quality(int(target["id"]))
        assert restored.quality_status == "learning"
        assert restored.quality_attempts == 0
        assert restored.quality_reason is None
        relaunched = await _launch_runs(app, int(campaign["id"]), "bad-target-restored")
        assert len(relaunched) == 1


@pytest.mark.asyncio
async def test_moderation_deleted_post_immediately_quarantines_target_and_counts_load(
    tmp_path: Path,
) -> None:
    gateway = ModerationDeletedGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, accounts, target):
        await _launch_runs(app, int(campaign["id"]), "moderation-deleted")
        result = await app.state.queue_worker.process_next()
        assert result is not None and result.status == RunStatus.FAILED
        assert result.error_code == "post_deleted_by_moderation"

        async with app.state.database.session_factory() as session:
            stored_target = await session.get(Target, int(target["id"]))
            account = await session.get(Account, int(accounts[0]["id"]))
            assert stored_target is not None
            assert stored_target.quality_status == "bad"
            assert stored_target.quality_score == 0
            assert "публикации исчезают" in str(stored_target.quality_reason)
            assert "подпишитесь" in str(stored_target.quality_reason)
            assert account is not None and account.sent_today == 1
            action = await session.scalar(select(ActionLog).where(ActionLog.run_id == result.id))
            # A custom gateway without a mode flag cannot prove simulation.
            assert action.simulated is None


@pytest.mark.asyncio
async def test_later_moderation_deletion_rewrites_previous_delivery_and_quarantines_target(
    tmp_path: Path,
) -> None:
    gateway = LaterModerationDeletionGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, target):
        await _launch_runs(app, int(campaign["id"]), "later-moderation-deleted")
        delivered = await app.state.queue_worker.process_next()
        assert delivered is not None and delivered.status == RunStatus.SUCCEEDED

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun)
                .where(CampaignRun.launch_key == "later-moderation-deleted")
                .where(CampaignRun.status == RunStatus.QUEUED)
            )
            assert successor is not None
            successor.scheduled_for = utc_now()
            await session.commit()

        stopped = await app.state.queue_worker.process_next()
        assert stopped is not None and stopped.status == RunStatus.FAILED
        assert stopped.error_code == "post_deleted_by_moderation"

        repository = AdminRepository(app.state.database.session_factory)
        rated = await repository.get_target(int(target["id"]))
        assert rated.quality_status == "bad"
        assert rated.quality_successes == 0
        assert rated.quality_failures == 1
        assert "пройдите проверку" in str(rated.quality_reason)


@pytest.mark.asyncio
async def test_two_account_bans_auto_pause_target(tmp_path: Path) -> None:
    gateway = AlwaysFailGateway("account_banned_in_chat")
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        sender_mode="independent",
    ) as (app, campaign, _, target):
        runs = await _launch_runs(app, int(campaign["id"]), "two-account-bans")
        assert len(runs) == 2
        first = await app.state.queue_worker.process_next()
        second = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.FAILED
        assert second is not None and second.status == RunStatus.FAILED

        repository = AdminRepository(app.state.database.session_factory)
        rated = await repository.get_target(int(target["id"]))
        assert rated.quality_status == "bad"
        assert rated.quality_banned_accounts == 2
        assert "2 аккаунта" in str(rated.quality_reason)


@pytest.mark.asyncio
async def test_preflight_bans_also_auto_pause_target(tmp_path: Path) -> None:
    gateway = PreflightBannedGateway()
    async with _smart_campaign(tmp_path, gateway=gateway, backup_senders=1) as (
        app,
        campaign,
        _,
        target,
    ):
        summary = await app.state.preflight_service.inspect_campaign(int(campaign["id"]))
        assert summary.blocked_targets == 1

        repository = AdminRepository(app.state.database.session_factory)
        rated = await repository.get_target(int(target["id"]))
        assert rated.quality_status == "bad"
        assert rated.quality_banned_accounts == 2
        assert "2 аккаунта" in str(rated.quality_reason)


@pytest.mark.asyncio
async def test_preflight_disables_account_after_definitive_session_failure(tmp_path: Path) -> None:
    gateway = PreflightDeadAccountGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, accounts, _):
        await app.state.preflight_service.inspect_campaign(int(campaign["id"]))

        async with app.state.database.session_factory() as session:
            account = await session.get(Account, int(accounts[0]["id"]))
        assert account is not None and account.status == AccountStatus.DISABLED
        assert account.authorized is False


@pytest.mark.asyncio
async def test_preflight_remembers_join_limit_for_each_account_target_route(tmp_path: Path) -> None:
    gateway = PreflightJoinLimitGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
    ) as (app, campaign, accounts, target):
        await app.state.preflight_service.inspect_campaign(int(campaign["id"]))

        async with app.state.database.session_factory() as session:
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.target_id == int(target["id"])
                    )
                )
            )
        assert {state.account_id for state in states} == {
            int(accounts[0]["id"]),
            int(accounts[1]["id"]),
        }
        assert all(state.send_blocked for state in states)
        assert {state.blocked_reason for state in states} == {"account_join_limit"}


@pytest.mark.asyncio
async def test_network_failure_does_not_damage_target_rating(tmp_path: Path) -> None:
    gateway = AlwaysFailGateway("network_error")
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, target):
        await _launch_runs(app, int(campaign["id"]), "network-is-not-chat-quality")
        result = await app.state.queue_worker.process_next()
        assert result is not None and result.status == RunStatus.FAILED

        repository = AdminRepository(app.state.database.session_factory)
        rated = await repository.get_target(int(target["id"]))
        assert rated.quality_status == "learning"
        assert rated.quality_attempts == 0
        assert rated.quality_failures == 0


@pytest.mark.asyncio
async def test_independent_accounts_keep_separate_adaptive_chat_streams(
    tmp_path: Path,
) -> None:
    gateway = PerAccountActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        sender_mode="independent",
        no_timer_interval_seconds=300,
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=60,
        visibility_max_wait_seconds=900,
    ) as (app, campaign, accounts, target):
        initial = await _launch_runs(app, int(campaign["id"]), "independent-lanes")
        account_ids = {int(account["id"]) for account in accounts}

        assert len(initial) == 2
        assert {run.sender_account_id for run in initial} == account_ids
        assert all(
            json.loads(run.sender_pool_snapshot_json or "[]") == [run.sender_account_id]
            for run in initial
        )

        assert await app.state.queue_worker.process_next() is not None
        gateway.messages_after_by_account = {account_id: 1 for account_id in account_ids}
        assert await app.state.queue_worker.process_next() is not None
        stored = await _stored_launch_runs(app, "independent-lanes")
        successors = [run for run in stored if run.cycle_index == 1]
        assert len(successors) == 2
        assert {run.sender_account_id for run in successors} == account_ids

        first_id, second_id = sorted(account_ids)
        gateway.messages_after_by_account = {first_id: 100, second_id: 5}
        sample_start = utc_now() - timedelta(minutes=5)
        async with app.state.database.session_factory() as session:
            due_runs = list(
                await session.scalars(
                    select(CampaignRun).where(
                        CampaignRun.launch_key == "independent-lanes",
                        CampaignRun.cycle_index == 1,
                    )
                )
            )
            for run in due_runs:
                run.scheduled_for = utc_now()
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.target_id == int(target["id"])
                    )
                )
            )
            for state in states:
                state.last_success_at = sample_start
            await session.commit()

        assert await app.state.queue_worker.process_next() is not None
        assert await app.state.queue_worker.process_next() is not None

        async with app.state.database.session_factory() as session:
            learned = {
                state.account_id: state
                for state in await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.target_id == int(target["id"])
                    )
                )
            }
        assert learned[first_id].adaptive_interval_seconds == 60
        assert learned[first_id].adaptive_tier == "turbo"
        # The calculation uses elapsed wall-clock time. The two independent
        # test lanes finish a second or two apart, so insist on the intended
        # adaptive band instead of an impossible exact timestamp result.
        assert 740 <= int(learned[second_id].adaptive_interval_seconds or 0) <= 760
        assert learned[second_id].adaptive_tier == "steady"

        repository = AdminRepository(app.state.database.session_factory)
        launch, launch_runs = await repository.get_launch(initial[0].id)
        targets = await repository.targets_by_ids({int(target["id"])})
        report = launch_details_text(launch, launch_runs, targets)
        assert "Независимые отправки" in report
        assert "независимых направлений" in report
        assert "Охват направлений" in report
        assert "Авточастота по потокам" in report
        assert "турбо" in report


@pytest.mark.asyncio
async def test_adaptive_activity_does_not_count_other_managed_publications(
    tmp_path: Path,
) -> None:
    gateway = ActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        sender_mode="independent",
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=60,
        visibility_max_wait_seconds=900,
    ) as (app, campaign, _, target):
        await _launch_runs(app, int(campaign["id"]), "organic-activity-only")
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.primary_message_id is not None
        gateway.latest_message_id = int(first.primary_message_id) + 1
        assert await app.state.queue_worker.process_next() is not None

        async with app.state.database.session_factory() as session:
            states = list(
                await session.scalars(
                    select(TargetAccountState).where(
                        TargetAccountState.target_id == int(target["id"])
                    )
                )
            )
            measured = min(states, key=lambda state: int(state.last_message_id or 0))
            assert measured.last_message_id is not None
            gateway.latest_message_id = measured.last_message_id + 10
            measured.last_success_at = utc_now() - timedelta(minutes=5)
            successors = list(
                await session.scalars(
                    select(CampaignRun).where(
                        CampaignRun.launch_key == "organic-activity-only",
                        CampaignRun.cycle_index == 1,
                    )
                )
            )
            for run in successors:
                run.scheduled_for = (
                    utc_now()
                    if run.sender_account_id == measured.account_id
                    else utc_now() + timedelta(hours=1)
                )
            measured_account_id = measured.account_id
            await session.commit()

        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None
        assert deferred.status == RunStatus.QUEUED
        assert len(gateway.account_ids) == 2

        async with app.state.database.session_factory() as session:
            learned = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.target_id == int(target["id"]),
                    TargetAccountState.account_id == measured_account_id,
                )
            )
            assert learned is not None
            # This interval is learned from elapsed wall-clock time; the
            # explicit five-minute setup can cross a one-second boundary while
            # the worker reaches the activity probe.
            assert 330 <= int(learned.adaptive_interval_seconds or 0) <= 340


@pytest.mark.asyncio
async def test_chat_timer_shorter_than_default_interval_wins_exactly(tmp_path: Path) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        runs = await _launch_runs(app, int(campaign["id"]), "timer-wins-launch")
        initial = runs[0]
        timer_at = utc_now() + timedelta(minutes=5)
        async with app.state.database.session_factory() as session:
            session.add(
                TargetAccountState(
                    target_id=initial.target_id,
                    account_id=initial.sender_account_id,
                    slowmode_seconds=300,
                    next_send_at=timer_at,
                    last_checked_at=utc_now(),
                )
            )
            await session.commit()

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED

        stored = await _stored_launch_runs(app, "timer-wins-launch")
        assert [run.cycle_index for run in stored] == [0, 1]
        assert ensure_utc(stored[1].scheduled_for) == ensure_utc(timer_at)


@pytest.mark.asyncio
async def test_chat_without_timer_uses_configured_fifteen_minute_interval(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "no-timer-launch")

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED
        assert completed.finished_at is not None

        stored = await _stored_launch_runs(app, "no-timer-launch")
        assert [run.cycle_index for run in stored] == [0, 1]
        assert ensure_utc(stored[1].scheduled_for) == ensure_utc(completed.finished_at) + timedelta(
            minutes=15
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("activity_check_interval_seconds", "visibility_max_wait_seconds", "expected_seconds"),
    [(60, 900, 60), (300, 60, 60)],
)
async def test_activity_aware_first_probe_is_fast_and_respects_deadline(
    tmp_path: Path,
    activity_check_interval_seconds: int,
    visibility_max_wait_seconds: int,
    expected_seconds: int,
) -> None:
    gateway = ActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        no_timer_interval_seconds=900,
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=activity_check_interval_seconds,
        visibility_max_wait_seconds=visibility_max_wait_seconds,
    ) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "fast-first-probe")

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED
        assert completed.finished_at is not None

        stored = await _stored_launch_runs(app, "fast-first-probe")
        successor = next(run for run in stored if run.cycle_index == 1)
        assert ensure_utc(successor.scheduled_for) == ensure_utc(completed.finished_at) + timedelta(
            seconds=expected_seconds
        )


@pytest.mark.asyncio
async def test_activity_aware_repeat_waits_until_post_is_buried(tmp_path: Path) -> None:
    gateway = ActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        activity_aware=True,
        min_messages_between_posts=50,
    ) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "activity-aware-launch")

        first = await app.state.queue_worker.process_next()
        assert first is not None
        assert first.status == RunStatus.SUCCEEDED
        assert gateway.account_ids
        own_message_id = 20_001

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "activity-aware-launch",
                    CampaignRun.cycle_index == 1,
                )
            )
            assert successor is not None
            successor.scheduled_for = utc_now()
            await session.commit()

        gateway.latest_message_id = own_message_id + 49
        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None
        assert deferred.status == RunStatus.QUEUED
        assert len(gateway.account_ids) == 1

        async with app.state.database.session_factory() as session:
            stored = await session.get(CampaignRun, deferred.id)
            assert stored is not None
            stored.scheduled_for = utc_now()
            await session.commit()

        gateway.latest_message_id = own_message_id + 50
        second = await app.state.queue_worker.process_next()
        assert second is not None
        assert second.status == RunStatus.SUCCEEDED
        assert len(gateway.account_ids) == 2


@pytest.mark.asyncio
async def test_strict_visibility_never_uses_a_timer_before_the_message_threshold(
    tmp_path: Path,
) -> None:
    gateway = ActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=30,
        visibility_max_wait_seconds=0,
    ) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "strict-visibility-launch")
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED
        assert first.primary_message_id is not None

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "strict-visibility-launch",
                    CampaignRun.cycle_index == 1,
                )
            )
            assert successor is not None
            successor.scheduled_for = utc_now()
            await session.commit()

        gateway.latest_message_id = int(first.primary_message_id) + 9
        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None and deferred.status == RunStatus.QUEUED
        assert deferred.error_code == "visibility_wait"
        assert len(gateway.account_ids) == 1

        async with app.state.database.session_factory() as session:
            stored = await session.get(CampaignRun, deferred.id)
            assert stored is not None
            stored.scheduled_for = utc_now()
            await session.commit()

        gateway.latest_message_id = int(first.primary_message_id) + 10
        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED
        assert len(gateway.account_ids) == 2


@pytest.mark.asyncio
async def test_idle_chat_guard_is_not_bypassed_by_visibility_deadline(
    tmp_path: Path,
) -> None:
    gateway = ActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        activity_aware=True,
        min_messages_between_posts=10,
        activity_check_interval_seconds=300,
        visibility_max_wait_seconds=60,
    ) as (app, campaign, accounts, target):
        await _launch_runs(app, int(campaign["id"]), "visibility-deadline-launch")
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "visibility-deadline-launch",
                    CampaignRun.cycle_index == 1,
                )
            )
            timing = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == int(accounts[0]["id"]),
                    TargetAccountState.target_id == int(target["id"]),
                )
            )
            assert successor is not None
            assert timing is not None and timing.last_success_at is not None
            successor.scheduled_for = utc_now()
            await session.commit()

        gateway.latest_message_id = int(first.primary_message_id or 20_001)
        before_check = utc_now()
        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None and deferred.status == RunStatus.QUEUED
        assert ensure_utc(deferred.scheduled_for) >= before_check + timedelta(seconds=59)


@pytest.mark.asyncio
async def test_formatted_media_content_is_snapshotted_for_launch(tmp_path: Path) -> None:
    gateway = ContentRecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        media_path = tmp_path / "advertisement.jpg"
        media_path.write_bytes(b"test-image")
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.primary_html = "<b>Advertisement</b>"
            stored_campaign.primary_media_path = str(media_path)
            stored_campaign.primary_media_kind = "photo"
            stored_campaign.primary_media_name = "advertisement.jpg"
            stored_campaign.primary_media_size = media_path.stat().st_size
            await session.commit()

        launched = await _launch_runs(app, int(campaign["id"]), "media-snapshot-launch")
        with pytest.raises(ConflictError, match="media caption"):
            await app.state.run_queue.update_active_launch(
                launched[0].id,
                primary_text="x" * 1025,
            )
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.primary_html = None
            stored_campaign.primary_media_path = None
            stored_campaign.primary_media_kind = None
            stored_campaign.primary_media_name = None
            stored_campaign.primary_media_size = None
            await session.commit()

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED
        assert len(gateway.content_calls) == 1
        call = gateway.content_calls[0]
        assert call["html_text"] == "<b>Advertisement</b>"
        assert call["media_path"] == str(media_path)
        assert call["media_kind"] == "photo"


@pytest.mark.asyncio
async def test_spintax_is_stable_for_the_actual_broadcast_and_audit_log(tmp_path: Path) -> None:
    gateway = ContentRecordingGateway()
    template = "{Привет|Здравствуйте}, {друг|коллега}!"
    html_template = "<b>{Привет|Здравствуйте}</b>, {друг|коллега}!"
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.primary_text = template
            stored_campaign.primary_html = html_template
            await session.commit()

        launched = await _launch_runs(app, int(campaign["id"]), "spintax-launch")
        assert len(launched) == 1
        completed = await app.state.queue_worker.process_next()
        assert completed is not None and completed.status == RunStatus.SUCCEEDED

        expected = render_spintax(template, seed=f"{launched[0].request_key}:primary")
        expected_html = render_spintax(html_template, seed=f"{launched[0].request_key}:primary")
        assert gateway.content_calls[0]["text"] == expected
        assert gateway.content_calls[0]["html_text"] == expected_html

        async with app.state.database.session_factory() as session:
            log = await session.scalar(select(ActionLog).where(ActionLog.run_id == launched[0].id))
            assert log is not None
            assert log.text == expected


@pytest.mark.asyncio
async def test_live_guard_requires_fresh_preflight_coverage(tmp_path: Path) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, accounts, target):
        queue = RunQueue(
            app.state.database.session_factory,
            lease_seconds=30,
            max_attempts=5,
            retry_base_seconds=2,
            preflight_required=True,
            preflight_max_age_seconds=3600,
        )
        with pytest.raises(ConflictError, match="Проверить чаты и аккаунты"):
            await queue.enqueue_campaign(int(campaign["id"]), "missing-preflight")

        async with app.state.database.session_factory() as session:
            session.add(
                TargetAccountState(
                    target_id=int(target["id"]),
                    account_id=int(accounts[0]["id"]),
                    membership_status=MembershipStatus.MEMBER,
                    can_send=True,
                    membership_checked_at=utc_now(),
                )
            )
            await session.commit()

        runs = await queue.enqueue_campaign(int(campaign["id"]), "fresh-preflight")
        assert len(runs) == 1
        assert runs[0].status == RunStatus.QUEUED


@pytest.mark.asyncio
async def test_smart_broadcast_rotates_variants_and_snapshots_album(tmp_path: Path) -> None:
    gateway = ContentRecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        no_timer_interval_seconds=60,
    ) as (app, campaign, _, _):
        first_media = tmp_path / "variant-b-1.jpg"
        second_media = tmp_path / "variant-b-2.mp4"
        first_media.write_bytes(b"photo")
        second_media.write_bytes(b"video")
        variants = [
            new_content_variant(name="A", text="Advertisement A"),
            new_content_variant(
                name="B",
                text="Advertisement B",
                html_text="<b>Advertisement B</b>",
                media=(
                    StoredMedia(
                        path=str(first_media),
                        kind="photo",
                        name=first_media.name,
                        size=first_media.stat().st_size,
                    ),
                    StoredMedia(
                        path=str(second_media),
                        kind="video",
                        name=second_media.name,
                        size=second_media.stat().st_size,
                    ),
                ),
            ),
        ]
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.content_variants_json = serialize_content_variants(variants)
            await session.commit()

        await _launch_runs(app, int(campaign["id"]), "variant-rotation-launch")
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED
        assert gateway.content_calls[0]["text"] == "Advertisement A"
        assert gateway.content_calls[0]["media_items"] == []

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "variant-rotation-launch",
                    CampaignRun.cycle_index == 1,
                )
            )
            assert successor is not None
            assert successor.content_variant_name_snapshot == "B"
            successor.scheduled_for = utc_now()
            await session.commit()

        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED
        call = gateway.content_calls[1]
        assert call["text"] == "Advertisement B"
        assert call["html_text"] == "<b>Advertisement B</b>"
        assert len(call["media_items"]) == 2


@pytest.mark.asyncio
async def test_smart_broadcast_rotates_native_contact_as_an_independent_post(
    tmp_path: Path,
) -> None:
    gateway = ContentRecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        no_timer_interval_seconds=60,
    ) as (app, campaign, _, _):
        contact = new_stored_contact(
            phone_number="+79991234567",
            first_name="Алиса",
            last_name="Соколова",
        )
        variants = [
            new_content_variant(name="Offer", text="Advertisement"),
            new_content_variant(name="Contact", text="", contact=contact),
        ]
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.content_variants_json = serialize_content_variants(variants)
            await session.commit()

        await _launch_runs(app, int(campaign["id"]), "contact-rotation-launch")
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED
        assert gateway.content_calls[0]["contact"] is None

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "contact-rotation-launch",
                    CampaignRun.cycle_index == 1,
                )
            )
            assert successor is not None
            assert successor.content_variant_name_snapshot == "Contact"
            successor.scheduled_for = utc_now()
            await session.commit()

        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED
        assert gateway.content_calls[1]["text"] == ""
        assert gateway.content_calls[1]["contact"] == contact.as_dict()


@pytest.mark.asyncio
async def test_active_launch_keeps_timing_snapshot_after_campaign_edit(tmp_path: Path) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        auto_sender_failover=True,
    ) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "snapshot-launch")
        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
            assert stored_campaign is not None
            stored_campaign.smart_timer_mode = False
            stored_campaign.smart_duration_seconds = 0
            stored_campaign.smart_no_timer_interval_seconds = 60
            stored_campaign.auto_sender_failover = False
            await session.commit()

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED
        assert completed.finished_at is not None

        stored = await _stored_launch_runs(app, "snapshot-launch")
        assert [run.cycle_index for run in stored] == [0, 1]
        assert ensure_utc(stored[1].scheduled_for) == ensure_utc(completed.finished_at) + timedelta(
            minutes=15
        )
        assert stored[1].auto_sender_failover_snapshot is True


@pytest.mark.asyncio
async def test_active_launch_keeps_selected_failover_pool_snapshot(tmp_path: Path) -> None:
    gateway = FailFirstGateway("account_dead")
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, _):
        await _launch_runs(app, int(campaign["id"]), "pool-snapshot-launch")
        backup_id = int(accounts[1]["id"])
        async with app.state.database.session_factory() as session:
            await session.execute(
                delete(CampaignAccount).where(
                    CampaignAccount.campaign_id == int(campaign["id"]),
                    CampaignAccount.account_id == backup_id,
                )
            )
            await session.commit()

        rerouted = await app.state.queue_worker.process_next()
        assert rerouted is not None
        assert rerouted.status == RunStatus.QUEUED
        assert rerouted.sender_account_id == backup_id

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED
        assert gateway.account_ids == [int(accounts[0]["id"]), backup_id]


@pytest.mark.asyncio
async def test_smart_broadcast_does_not_schedule_past_repeat_until(tmp_path: Path) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        duration_seconds=60,
        no_timer_interval_seconds=900,
    ) as (app, campaign, _, _):
        runs = await _launch_runs(app, int(campaign["id"]), "finite-window-launch")
        repeat_until = ensure_utc(runs[0].repeat_until)
        assert repeat_until is not None

        completed = await app.state.queue_worker.process_next()
        assert completed is not None
        assert completed.status == RunStatus.SUCCEEDED

        stored = await _stored_launch_runs(app, "finite-window-launch")
        assert len(stored) == 1
        assert ensure_utc(completed.finished_at) + timedelta(minutes=15) > repeat_until


@pytest.mark.asyncio
async def test_smart_broadcast_keeps_only_one_queued_successor_per_chat(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "single-successor-launch")

        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.SUCCEEDED
        assert await app.state.queue_worker.process_next() is None
        stored = await _stored_launch_runs(app, "single-successor-launch")
        assert [run.status for run in stored].count(RunStatus.QUEUED) == 1

        successor = next(run for run in stored if run.status == RunStatus.QUEUED)
        async with app.state.database.session_factory() as session:
            due = await session.get(CampaignRun, successor.id)
            assert due is not None
            due.scheduled_for = utc_now() - timedelta(seconds=1)
            await session.commit()

        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED
        assert await app.state.queue_worker.process_next() is None

        stored = await _stored_launch_runs(app, "single-successor-launch")
        assert [run.cycle_index for run in stored] == [0, 1, 2]
        assert [run.status for run in stored].count(RunStatus.QUEUED) == 1
        assert len({run.request_key for run in stored}) == len(stored)


@pytest.mark.asyncio
async def test_indefinite_smart_broadcast_keeps_a_single_cancellable_successor(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        repeat_forever=True,
        no_timer_interval_seconds=60,
    ) as (app, campaign, _, _):
        initial = await _launch_runs(app, int(campaign["id"]), "forever-launch")
        assert initial[0].repeat_forever_snapshot is True
        assert initial[0].repeat_until is None

        completed = await app.state.queue_worker.process_next()
        assert completed is not None and completed.status == RunStatus.SUCCEEDED
        stored = await _stored_launch_runs(app, "forever-launch")
        assert len(stored) == 2
        successor = next(run for run in stored if run.status == RunStatus.QUEUED)
        assert successor.repeat_forever_snapshot is True
        assert successor.repeat_until is None

        cancelled = await app.state.run_queue.cancel_launch(initial[0].id)
        assert cancelled == 1
        assert await app.state.queue_worker.process_next() is None
        stored = await _stored_launch_runs(app, "forever-launch")
        assert [run.status for run in stored].count(RunStatus.QUEUED) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    [
        "account_dead",
        "account_unauthorized",
        "account_banned_in_chat",
        "account_join_limit",
    ],
)
async def test_permanent_sender_loss_fails_over_to_another_selected_sender(
    tmp_path: Path,
    error_code: str,
) -> None:
    gateway = FailFirstGateway(error_code)
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        unselected_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, _):
        runs = await _launch_runs(app, int(campaign["id"]), f"failover-{error_code}")
        initial = runs[0]
        primary_id = int(accounts[0]["id"])
        backup_id = int(accounts[1]["id"])
        unselected_id = int(accounts[2]["id"])
        assert initial.sender_account_id == primary_id

        rerouted = await app.state.queue_worker.process_next()
        assert rerouted is not None
        assert rerouted.id == initial.id
        assert rerouted.status == RunStatus.QUEUED
        assert rerouted.sender_account_id == backup_id
        assert rerouted.attempt_count == 0

        delivered = await app.state.queue_worker.process_next()
        assert delivered is not None
        assert delivered.id == initial.id
        assert delivered.status == RunStatus.SUCCEEDED
        assert gateway.account_ids == [primary_id, backup_id]
        assert unselected_id not in gateway.account_ids

        if error_code in {"account_dead", "account_unauthorized"}:
            async with app.state.database.session_factory() as session:
                primary = await session.get(Account, primary_id)
                assert primary is not None
                assert primary.status == AccountStatus.DISABLED
                assert primary.authorized is False


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["account_banned_in_chat", "account_join_limit"])
async def test_blocked_account_route_is_remembered_and_skipped_by_future_launches(
    tmp_path: Path,
    error_code: str,
) -> None:
    gateway = FailFirstGateway(error_code)
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, target):
        primary_id = int(accounts[0]["id"])
        backup_id = int(accounts[1]["id"])
        await _launch_runs(app, int(campaign["id"]), f"remember-route-first-{error_code}")

        rerouted = await app.state.queue_worker.process_next()
        assert rerouted is not None
        assert rerouted.sender_account_id == backup_id
        async with app.state.database.session_factory() as session:
            blocked = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.target_id == int(target["id"]),
                    TargetAccountState.account_id == primary_id,
                )
            )
            assert blocked is not None
            assert blocked.send_blocked is True
            assert blocked.blocked_reason == error_code
            assert blocked.blocked_at is not None

        second_launch = await _launch_runs(
            app,
            int(campaign["id"]),
            f"remember-route-second-{error_code}",
        )
        assert second_launch[0].status == RunStatus.QUEUED
        assert second_launch[0].sender_account_id == backup_id


@pytest.mark.asyncio
async def test_active_launch_with_all_remembered_chat_bans_is_rejected_before_enqueue(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, target):
        async with app.state.database.session_factory() as session:
            session.add_all(
                TargetAccountState(
                    target_id=int(target["id"]),
                    account_id=int(account["id"]),
                    send_blocked=True,
                    blocked_reason="account_banned_in_chat",
                    blocked_at=utc_now(),
                )
                for account in accounts
            )
            await session.commit()

        with pytest.raises(ConflictError, match="активная рассылка не может стартовать"):
            await _launch_runs(app, int(campaign["id"]), "all-chat-bans")
        assert await app.state.queue_worker.process_next() is None


@pytest.mark.asyncio
async def test_smart_failover_does_not_probe_a_known_unready_backup(tmp_path: Path) -> None:
    gateway = FailFirstGateway("account_dead")
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, target):
        primary_id = int(accounts[0]["id"])
        backup_id = int(accounts[1]["id"])
        async with app.state.database.session_factory() as session:
            state = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.target_id == int(target["id"]),
                    TargetAccountState.account_id == backup_id,
                )
            )
            if state is None:
                state = TargetAccountState(
                    target_id=int(target["id"]),
                    account_id=backup_id,
                )
                session.add(state)
            state.membership_status = MembershipStatus.NOT_MEMBER
            state.can_send = False
            state.membership_checked_at = utc_now()
            await session.commit()

        await _launch_runs(app, int(campaign["id"]), "known-unready-failover")
        result = await app.state.queue_worker.process_next()

        assert result is not None and result.status == RunStatus.FAILED
        assert gateway.account_ids == [primary_id]


@pytest.mark.parametrize("failover", [False, True])
async def test_queued_repeat_checks_route_before_activity_probe(tmp_path, monkeypatch, failover):
    gateway = LatestAwareActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=failover,
    ) as (app, campaign, accounts, target):
        await _launch_runs(app, int(campaign["id"]), "queued-unready-route")
        completed = await app.state.queue_worker.process_next()
        assert completed.status == RunStatus.SUCCEEDED
        primary_id, backup_id = (int(a["id"]) for a in accounts)
        inspector = AsyncMock(
            side_effect=PermanentGatewayError("account is not a member", code="target_not_joined")
        )
        monkeypatch.setattr(gateway, "inspect_activity", inspector)
        async with app.state.database.session_factory() as session:
            pending = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "queued-unready-route", CampaignRun.cycle_index == 1
                )
            )
            pending.sender_account_id = backup_id
            pending.scheduled_for = utc_now()
            session.add(
                TargetAccountState(
                    target_id=int(target["id"]),
                    account_id=backup_id,
                    membership_status=MembershipStatus.NOT_MEMBER,
                    can_send=False,
                    membership_checked_at=utc_now(),
                    membership_error_code="target_not_joined",
                    send_blocked=True,
                    blocked_reason="target_not_joined",
                )
            )
            await session.commit()
        result = await app.state.queue_worker.process_next()
        assert result.status == (RunStatus.QUEUED if failover else RunStatus.FAILED)
        assert result.sender_account_id == (primary_id if failover else backup_id)
        if not failover:
            assert result.error_code == "target_not_joined"
        inspector.assert_not_awaited()
        assert gateway.account_ids == [primary_id]


async def test_auto_join_precedes_activity_check_and_preserves_post_counter(tmp_path, monkeypatch):
    gateway = LatestAwareActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        account_auto_join=True,
    ) as (app, campaign, accounts, target):
        # A prior managed post makes the activity check mandatory.
        await _launch_runs(app, int(campaign["id"]), "join-before-activity")
        await app.state.queue_worker.process_next()
        joined = False

        async def prepare(**_kwargs):
            nonlocal joined
            changed = not joined
            joined = True
            return changed

        async def inspect_activity(**_kwargs):
            assert joined
            return ChatActivity(latest_message_id=gateway.latest_message_id, checked_at=utc_now())

        monkeypatch.setattr(gateway, "prepare_broadcast_target", prepare, raising=False)
        monkeypatch.setattr(gateway, "inspect_activity", inspect_activity)
        app.state.settings.account_pace_min_seconds = 60
        app.state.settings.account_pace_max_seconds = 60
        sender_id = int(accounts[0]["id"])
        async with app.state.database.session_factory() as session:
            pending = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "join-before-activity", CampaignRun.cycle_index == 1
                )
            )
            pending.scheduled_for = utc_now()
            timing = await session.scalar(
                select(TargetAccountState).where(
                    TargetAccountState.account_id == sender_id,
                    TargetAccountState.target_id == int(target["id"]),
                )
            )
            timing.membership_status = MembershipStatus.NOT_MEMBER
            timing.membership_error_code = "target_not_joined"
            timing.can_send = False
            timing.send_blocked = True
            timing.blocked_reason = "target_not_joined"
            await session.commit()
        result = await app.state.queue_worker.process_next()
        assert result.status == RunStatus.QUEUED
        assert result.error_code == "join_completed"
        assert len(gateway.account_ids) == 1
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, sender_id)
            assert account.sent_today == 1
            assert ensure_utc(account.next_send_at) >= utc_now() + timedelta(seconds=59)
            # Time advances only in this isolated test database.
            account.next_send_at = utc_now() - timedelta(seconds=1)
            stored = await session.get(CampaignRun, result.id)
            stored.scheduled_for = utc_now()
            await session.commit()
        idle = await app.state.queue_worker.process_next()
        assert idle.error_code == "chat_idle_last_managed_post"
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, sender_id)
            assert ensure_utc(account.next_send_at) < utc_now()
            assert account.sent_today == 1
        assert len(gateway.account_ids) == 1


@pytest.mark.parametrize(
    "code",
    [
        "target_not_joined",
        "account_chat_access_forbidden",
        "account_unauthorized",
    ],
)
async def test_activity_route_refusal_is_not_reported_as_an_idle_chat(tmp_path, monkeypatch, code):
    gateway = LatestAwareActivityGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, _):
        await _launch_runs(app, int(campaign["id"]), "activity-route-refusal")
        await app.state.queue_worker.process_next()
        monkeypatch.setattr(
            gateway,
            "inspect_activity",
            AsyncMock(side_effect=PermanentGatewayError("route refused", code=code)),
        )
        async with app.state.database.session_factory() as session:
            pending = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "activity-route-refusal", CampaignRun.cycle_index == 1
                )
            )
            pending.scheduled_for = utc_now()
            await session.commit()
        result = await app.state.queue_worker.process_next()
        assert result.status == RunStatus.QUEUED
        assert result.sender_account_id == int(accounts[1]["id"])
        assert result.error_code != "chat_idle_last_managed_post"
        assert gateway.account_ids == [int(accounts[0]["id"])]


async def test_readback_flood_preserves_unknown_result_quota_and_account_pause(
    tmp_path, monkeypatch
):
    gateway = RecordingGateway()
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, _):

        async def accepted_but_unreadable(**kwargs):
            gateway.account_ids.append(kwargs["account"].id)
            cause = TemporaryGatewayError("read-back FloodWait", code="flood_wait", retry_after=720)
            raise PermanentGatewayError(
                "accepted publication cannot be verified",
                code="delivery_unconfirmed",
                external_message_id=24001,
            ) from cause

        monkeypatch.setattr(gateway, "send_message", accepted_but_unreadable)
        await _launch_runs(app, int(campaign["id"]), "readback-flood")
        result = await app.state.queue_worker.process_next()
        assert result.status == RunStatus.FAILED
        assert result.error_code == "delivery_unconfirmed"
        assert gateway.account_ids == [int(accounts[0]["id"])]
        async with app.state.database.session_factory() as session:
            account = await session.get(Account, int(accounts[0]["id"]))
            assert account.sent_today == 1
            assert account.flood_hits == 1
            assert ensure_utc(account.cooldown_until) >= utc_now() + timedelta(seconds=719)
            action = await session.scalar(select(ActionLog).where(ActionLog.run_id == result.id))
            assert action.error_code == "delivery_unconfirmed"
            assert action.external_message_id == 24001


@pytest.mark.asyncio
async def test_active_smart_launch_text_frequency_depth_and_deadline_are_editable(
    tmp_path: Path,
) -> None:
    gateway = RecordingGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        await _launch_runs(app, int(campaign["id"]), "editable-active-launch")
        completed = await app.state.queue_worker.process_next()
        assert completed is not None and completed.finished_at is not None

        before = utc_now()
        with pytest.raises(ConflictError, match="Spintax"):
            await app.state.run_queue.update_active_launch(
                completed.id,
                primary_text="{broken",
            )
        updated = await app.state.run_queue.update_active_launch(
            completed.id,
            primary_text="Updated advertisement",
            no_timer_interval_seconds=60,
            min_messages_between_posts=12,
            visibility_max_wait_seconds=0,
            remaining_duration_seconds=7200,
        )
        assert updated == 1

        stored = await _stored_launch_runs(app, "editable-active-launch")
        successor = next(run for run in stored if run.status == RunStatus.QUEUED)
        assert successor.primary_text_snapshot == "Updated advertisement"
        assert successor.no_timer_interval_seconds_snapshot == 60
        assert successor.min_messages_between_posts_snapshot == 12
        assert successor.visibility_max_wait_seconds_snapshot == 0
        assert ensure_utc(successor.scheduled_for) == ensure_utc(completed.finished_at) + timedelta(
            seconds=60
        )
        repeat_until = ensure_utc(successor.repeat_until)
        assert repeat_until is not None
        assert before + timedelta(hours=2) <= repeat_until <= utc_now() + timedelta(hours=2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["account_dead", "account_unauthorized", "account_banned_in_chat"],
)
async def test_permanent_sender_loss_without_backup_keeps_failure_behavior(
    tmp_path: Path,
    error_code: str,
) -> None:
    gateway = AlwaysFailGateway(error_code)
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        auto_sender_failover=True,
    ) as (app, campaign, accounts, _):
        runs = await _launch_runs(app, int(campaign["id"]), f"no-backup-{error_code}")

        failed = await app.state.queue_worker.process_next()
        assert failed is not None
        assert failed.id == runs[0].id
        assert failed.status == RunStatus.FAILED
        assert failed.error_code == error_code
        assert failed.sender_account_id == int(accounts[0]["id"])
        assert gateway.account_ids == [int(accounts[0]["id"])]
        assert len(await _stored_launch_runs(app, f"no-backup-{error_code}")) == 1


@pytest.mark.asyncio
async def test_disabling_failover_preserves_failure_with_a_selected_backup(
    tmp_path: Path,
) -> None:
    gateway = AlwaysFailGateway("account_dead")
    async with _smart_campaign(
        tmp_path,
        gateway=gateway,
        backup_senders=1,
        auto_sender_failover=False,
    ) as (app, campaign, accounts, _):
        runs = await _launch_runs(app, int(campaign["id"]), "failover-disabled")

        failed = await app.state.queue_worker.process_next()
        assert failed is not None
        assert failed.id == runs[0].id
        assert failed.status == RunStatus.FAILED
        assert failed.error_code == "account_dead"
        assert failed.sender_account_id == int(accounts[0]["id"])
        assert gateway.account_ids == [int(accounts[0]["id"])]


@pytest.mark.asyncio
async def test_cancelling_launch_while_send_is_running_does_not_create_successor(
    tmp_path: Path,
) -> None:
    gateway = BlockingSuccessGateway()
    async with _smart_campaign(tmp_path, gateway=gateway) as (app, campaign, _, _):
        runs = await _launch_runs(app, int(campaign["id"]), "cancel-running-launch")
        worker_task = asyncio.create_task(app.state.queue_worker.process_next())
        await gateway.started.wait()

        assert await app.state.run_queue.cancel_launch(runs[0].id) == 1
        gateway.release.set()
        completed = await worker_task
        assert completed is not None

        stored = await _stored_launch_runs(app, "cancel-running-launch")
        assert len(stored) == 1
        assert stored[0].cancel_requested is True
        assert not any(run.status == RunStatus.QUEUED for run in stored)
