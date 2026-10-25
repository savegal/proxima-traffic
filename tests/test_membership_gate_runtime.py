import httpx
import pytest
from sqlalchemy import select

from proxima_traffic.config import Settings
from proxima_traffic.gateways import MembershipGateRecovery, PermanentGatewayError, SentMessage
from proxima_traffic.main import create_app
from proxima_traffic.models import ActionLog, CampaignRun, Target, ensure_utc, utc_now


class GateThenDeliveryGateway:
    def __init__(self, *, retry_fails: bool = False) -> None:
        self.send_calls = 0
        self.recovery_calls = 0
        self.retry_fails = retry_fails
        self.idempotency_keys: list[str] = []

    async def send_message(self, **kwargs: object) -> SentMessage:
        self.idempotency_keys.append(str(kwargs["idempotency_key"]))
        self.send_calls += 1
        if self.send_calls == 1:
            raise PermanentGatewayError(
                "post was removed; bot asked for subscriptions",
                code="post_deleted_by_moderation",
                external_message_id=71_001,
            )
        if self.retry_fails:
            raise PermanentGatewayError(
                "post was removed again after membership recovery",
                code="post_deleted_by_moderation",
                external_message_id=71_002,
            )
        return SentMessage(message_id=71_002)

    async def recover_membership_gate(self, **kwargs: object) -> MembershipGateRecovery:
        del kwargs
        self.recovery_calls += 1
        return MembershipGateRecovery(
            detected=True,
            recovered=True,
            notice="Подпишитесь на канал",
            joined_channel_refs=("@required_news",),
            verification_callback_count=1,
        )


@pytest.mark.parametrize(
    ("retry_fails", "expected_status", "expected_target_status"),
    [(False, "succeeded", "learning"), (True, "failed", "bad")],
)
async def test_membership_gate_defers_exactly_one_retry_and_quarantines_a_second_failure(
    tmp_path,
    retry_fails: bool,
    expected_status: str,
    expected_target_status: str,
) -> None:
    gateway = GateThenDeliveryGateway(retry_fails=retry_fails)
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'membership-gate.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        membership_gate_retry_delay_seconds=300,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    headers = {"X-Admin-Key": "test-admin-key"}
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        account = await client.post(
            "/api/v1/accounts",
            headers=headers,
            json={"label": "gate-account", "telegram_user_id": 81, "ownership_confirmed": True},
        )
        target = await client.post(
            "/api/v1/targets",
            headers=headers,
            json={"chat_ref": "@gate_target", "title": "Gate target"},
        )
        assert account.status_code == 201
        assert target.status_code == 201
        target_id = int(target.json()["id"])
        approved = await client.post(f"/api/v1/targets/{target_id}/approve", headers=headers)
        assert approved.status_code == 200
        campaign = await client.post(
            "/api/v1/campaigns",
            headers=headers,
            json={
                "name": "gate recovery",
                "mode": "broadcast",
                "primary_account_id": account.json()["id"],
                "target_id": target_id,
                "primary_text": "Allowed update",
            },
        )
        assert campaign.status_code == 201
        queued = await client.post(
            f"/api/v1/campaigns/{campaign.json()['id']}/runs",
            headers=headers,
            json={"request_key": "membership-gate-runtime-001"},
        )
        assert queued.status_code == 202

    deferred = await app.state.queue_worker.process_next()
    assert deferred is not None and deferred.status.value == "queued"
    assert deferred.membership_gate_retry_count == 1
    assert gateway.recovery_calls == 1

    async with app.state.database.session_factory() as session:
        run = await session.get(CampaignRun, deferred.id)
        target = await session.get(Target, target_id)
        action = await session.scalar(select(ActionLog).where(ActionLog.run_id == deferred.id))
        assert run is not None and run.error_code == "membership_gate_retry"
        assert ensure_utc(run.scheduled_for) >= utc_now()
        assert target is not None and target.quality_status == "learning"
        assert action is not None and action.error_code == "membership_gate_recovered"
        run.scheduled_for = utc_now()
        await session.commit()

    completed = await app.state.queue_worker.process_next()
    assert completed is not None and completed.status.value == expected_status
    assert gateway.send_calls == 2
    assert gateway.recovery_calls == 1
    assert gateway.idempotency_keys == [
        "membership-gate-runtime-001:primary",
        "membership-gate-runtime-001:primary:membership-gate-retry:1",
    ]

    async with app.state.database.session_factory() as session:
        target = await session.get(Target, target_id)
        assert target is not None and target.quality_status == expected_target_status

    await app.state.database.dispose()
