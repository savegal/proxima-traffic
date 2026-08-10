from __future__ import annotations

import pytest
from sqlalchemy import select

from proxima_traffic.db import Database
from proxima_traffic.gateways import TargetReadiness
from proxima_traffic.models import (
    Account,
    AccountStatus,
    Campaign,
    CampaignAccount,
    CampaignMode,
    MembershipStatus,
    RunStatus,
    Target,
    TargetAccountState,
    utc_now,
)
from proxima_traffic.preflight import PreflightService
from proxima_traffic.services import RunQueue
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
