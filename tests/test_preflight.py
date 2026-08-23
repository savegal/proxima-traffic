from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from proxima_traffic.db import Database
from proxima_traffic.gateways import TargetReadiness
from proxima_traffic.models import (
    Account,
    AccountStatus,
    BroadcastSenderMode,
    Campaign,
    CampaignAccount,
    CampaignMode,
    MembershipStatus,
    RunStatus,
    Target,
    TargetAccountState,
    ensure_utc,
    utc_now,
)
from proxima_traffic.preflight import PreflightService
from proxima_traffic.services import ConflictError, RunQueue
from proxima_traffic.spambot import SpamBotResult, SpamBotStatus


class ReadinessGateway:
    def __init__(self, readiness: TargetReadiness) -> None:
        self.readiness = readiness
        self.inspect_calls = 0

    async def inspect_target(self, **kwargs: object) -> TargetReadiness:
        del kwargs
        self.inspect_calls += 1
        return self.readiness


class UnknownSpamBotChecks:
    async def check_account(self, *args: object, **kwargs: object) -> SpamBotResult:
        del args, kwargs
        return SpamBotResult(
            SpamBotStatus.UNKNOWN,
            "unclassified reply",
            parser_signal="unrecognised_response",
        )


async def _campaign(database: Database, *, suffix: str) -> tuple[int, int, int]:
    async with database.session_factory() as session:
        account = Account(
            label=f"preflight-account-{suffix}",
            telegram_user_id=9000 + len(suffix),
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref=f"@preflight_{suffix}",
            title=f"Preflight {suffix}",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all((account, target))
        await session.flush()
        campaign = Campaign(
            name=f"preflight-campaign-{suffix}",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Test message",
        )
        session.add(campaign)
        await session.commit()
        return int(campaign.id), int(account.id), int(target.id)


@pytest.mark.asyncio
async def test_unknown_spambot_reply_does_not_skip_real_target_inspection(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-unknown.db'}")
    await database.create_schema()
    campaign_id, account_id, target_id = await _campaign(database, suffix="unknown")
    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
        )
    )
    service = PreflightService(
        database.session_factory,
        gateway,
        UnknownSpamBotChecks(),  # type: ignore[arg-type]
    )

    summary = await service.inspect_campaign(campaign_id)

    assert gateway.inspect_calls == 1
    assert summary.member_targets == 1
    assert summary.uncovered_targets == 0
    async with database.session_factory() as session:
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        assert state is not None
    await database.dispose()


@pytest.mark.asyncio
async def test_preflight_reports_completed_accounts_and_pairs(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-progress.db'}")
    await database.create_schema()
    campaign_id, _, _ = await _campaign(database, suffix="progress")
    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
        )
    )
    progress: list[tuple[int, int, int, int]] = []

    async def on_progress(
        checked_pairs: int,
        total_pairs: int,
        checked_accounts: int,
        total_accounts: int,
    ) -> None:
        progress.append((checked_pairs, total_pairs, checked_accounts, total_accounts))

    summary = await PreflightService(
        database.session_factory,
        gateway,
    ).inspect_campaign(campaign_id, on_progress=on_progress)

    assert summary.checked_pairs == 1
    assert progress == [(0, 1, 0, 1), (1, 1, 1, 1)]
    await database.dispose()


@pytest.mark.asyncio
async def test_preflight_replaces_legacy_chat_alias_with_telegram_title(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-title.db'}")
    await database.create_schema()
    campaign_id, _, target_id = await _campaign(database, suffix="title")
    async with database.session_factory() as session:
        target = await session.get(Target, target_id)
        assert target is not None
        target.title = "старое ручное имя"
        await session.commit()

    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
            telegram_title="Настоящее название Telegram",
        )
    )
    await PreflightService(database.session_factory, gateway).inspect_campaign(campaign_id)

    async with database.session_factory() as session:
        target = await session.get(Target, target_id)
        assert target is not None
        assert target.title == "Настоящее название Telegram"
    await database.dispose()


@pytest.mark.asyncio
async def test_preflight_rejects_an_oversized_matrix_before_telegram_checks(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-size-guard.db'}")
    await database.create_schema()
    campaign_id, account_id, _ = await _campaign(database, suffix="size-guard")
    async with database.session_factory() as session:
        second_account = Account(
            label="preflight-size-guard-second",
            telegram_user_id=9251,
            ownership_confirmed=True,
        )
        session.add(second_account)
        await session.flush()
        session.add_all(
            (
                CampaignAccount(campaign_id=campaign_id, account_id=account_id),
                CampaignAccount(campaign_id=campaign_id, account_id=second_account.id),
            )
        )
        await session.commit()

    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
        )
    )
    service = PreflightService(database.session_factory, gateway, max_pairs=1)

    with pytest.raises(ConflictError, match="проверка слишком велика"):
        await service.inspect_campaign(campaign_id)

    assert gateway.inspect_calls == 0
    await database.dispose()


@pytest.mark.asyncio
async def test_auto_join_target_is_routable_and_not_persisted_as_block(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-autojoin.db'}")
    await database.create_schema()
    campaign_id, account_id, target_id = await _campaign(database, suffix="autojoin")
    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.NOT_MEMBER,
            can_send=False,
            checked_at=utc_now(),
            error_code="target_not_joined",
            error="account has not joined this chat yet",
        )
    )
    service = PreflightService(
        database.session_factory,
        gateway,
        allow_auto_join=True,
    )

    summary = await service.inspect_campaign(campaign_id)

    assert summary.member_targets == 0
    assert summary.joinable_targets == 1
    assert summary.joinable_pairs == 1
    assert summary.planned_join_attempts == 1
    assert summary.routable_targets == 1
    assert summary.uncovered_targets == 0
    async with database.session_factory() as session:
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        assert state is not None
        assert state.send_blocked is False
        assert state.blocked_reason is None

    queue = RunQueue(
        database.session_factory,
        lease_seconds=30,
        max_attempts=3,
        retry_base_seconds=2,
        account_auto_join=True,
        preflight_required=True,
        preflight_max_age_seconds=3600,
    )
    runs = await queue.enqueue_campaign(campaign_id, "preflight-autojoin-launch")

    assert len(runs) == 1
    assert runs[0].status is RunStatus.QUEUED
    assert runs[0].error_code is None
    await database.dispose()


@pytest.mark.asyncio
async def test_preflight_reports_a_write_denial_as_a_blocked_route_not_a_chat_ban(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-write-denial.db'}")
    await database.create_schema()
    campaign_id, account_id, target_id = await _campaign(database, suffix="write-denial")
    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.UNKNOWN,
            can_send=False,
            checked_at=utc_now(),
            error_code="account_send_forbidden",
            error="Telegram denied write access for this account",
        )
    )

    summary = await PreflightService(database.session_factory, gateway).inspect_campaign(
        campaign_id
    )

    assert summary.launchable_units == 0
    assert summary.blocked_targets == 1
    assert summary.unavailable_targets == 0
    async with database.session_factory() as session:
        state = await session.scalar(
            select(TargetAccountState).where(
                TargetAccountState.account_id == account_id,
                TargetAccountState.target_id == target_id,
            )
        )
        assert state is not None
        assert state.send_blocked is True
        assert state.blocked_reason == "account_send_forbidden"
    await database.dispose()


@pytest.mark.asyncio
async def test_distributed_preflight_ignores_unavailable_senders(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-sender-pool.db'}")
    await database.create_schema()
    async with database.session_factory() as session:
        healthy = Account(
            label="preflight-healthy",
            telegram_user_id=9101,
            ownership_confirmed=True,
        )
        paused = Account(
            label="preflight-paused",
            telegram_user_id=9102,
            ownership_confirmed=True,
            status=AccountStatus.PAUSED,
        )
        target = Target(
            chat_ref="@preflight_sender_pool",
            title="Preflight sender pool",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all((healthy, paused, target))
        await session.flush()
        campaign = Campaign(
            name="preflight sender pool",
            mode=CampaignMode.BROADCAST,
            primary_account_id=healthy.id,
            target_id=target.id,
            primary_text="Test",
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
    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
        )
    )
    service = PreflightService(database.session_factory, gateway)

    summary = await service.inspect_campaign(campaign_id)

    assert summary.account_count == 1
    assert summary.ready_pairs == 1
    assert gateway.inspect_calls == 1
    await database.dispose()


@pytest.mark.asyncio
async def test_independent_preflight_keeps_ready_pairs_when_another_pair_is_blocked(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-independent-pairs.db'}")
    await database.create_schema()
    async with database.session_factory() as session:
        ready = Account(
            label="preflight-independent-ready",
            telegram_user_id=9201,
            ownership_confirmed=True,
        )
        blocked = Account(
            label="preflight-independent-blocked",
            telegram_user_id=9202,
            ownership_confirmed=True,
        )
        target = Target(
            chat_ref="@preflight_independent_pairs",
            title="Preflight independent pairs",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all((ready, blocked, target))
        await session.flush()
        campaign = Campaign(
            name="preflight independent pairs",
            mode=CampaignMode.BROADCAST,
            primary_account_id=ready.id,
            target_id=target.id,
            primary_text="Test",
            sender_mode=BroadcastSenderMode.INDEPENDENT,
        )
        session.add(campaign)
        await session.flush()
        session.add_all(
            (
                CampaignAccount(campaign_id=campaign.id, account_id=ready.id),
                CampaignAccount(campaign_id=campaign.id, account_id=blocked.id),
            )
        )
        await session.commit()
        campaign_id = int(campaign.id)
        ready_account_id = int(ready.id)

    class PairReadinessGateway:
        async def inspect_target(self, *, account: Account, **_: object) -> TargetReadiness:
            if account.id == ready_account_id:
                return TargetReadiness(
                    membership_status=MembershipStatus.MEMBER,
                    can_send=True,
                    checked_at=utc_now(),
                )
            return TargetReadiness(
                membership_status=MembershipStatus.BANNED,
                can_send=False,
                checked_at=utc_now(),
                error_code="account_banned_in_chat",
            )

    summary = await PreflightService(
        database.session_factory,
        PairReadinessGateway(),  # type: ignore[arg-type]
    ).inspect_campaign(campaign_id)

    assert summary.independent_mode is True
    assert summary.requires_all_accounts is False
    assert summary.member_targets == 1
    assert summary.routable_pairs == 1
    assert summary.launchable_units == 1
    await database.dispose()


@pytest.mark.asyncio
async def test_preflight_blocks_a_flood_paused_account_instead_of_queueing_tomorrow(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-flood-pause.db'}")
    await database.create_schema()
    campaign_id, account_id, _ = await _campaign(database, suffix="flood-pause")
    resume_at = utc_now() + timedelta(hours=2)
    async with database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        account.cooldown_until = resume_at
        account.cooldown_reason = "flood_escalation"
        await session.commit()

    gateway = ReadinessGateway(
        TargetReadiness(
            membership_status=MembershipStatus.MEMBER,
            can_send=True,
            checked_at=utc_now(),
        )
    )
    summary = await PreflightService(database.session_factory, gateway).inspect_campaign(
        campaign_id
    )

    assert summary.routable_pairs == 1
    assert summary.ready_now_pairs == 0
    assert summary.ready_now_targets == 0
    assert summary.deferred_pairs == 1
    assert summary.earliest_resume_at is not None
    assert summary.launchable_units == 0

    queue = RunQueue(
        database.session_factory,
        lease_seconds=30,
        max_attempts=3,
        retry_base_seconds=2,
        preflight_required=True,
        preflight_max_age_seconds=3600,
    )
    with pytest.raises(ConflictError, match="защитной паузы Telegram"):
        await queue.enqueue_campaign(campaign_id, "flood-pause-launch")
    scheduled = await queue.enqueue_campaign(
        campaign_id,
        "flood-pause-scheduled-launch",
        allow_deferred_start=True,
    )
    assert len(scheduled) == 1
    assert scheduled[0].status is RunStatus.QUEUED
    assert ensure_utc(scheduled[0].scheduled_for) >= resume_at
    await database.dispose()
