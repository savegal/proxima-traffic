from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from proxima_traffic.bot.renderers import launch_details_text
from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.config import Settings
from proxima_traffic.gateways import ChatActivity, PermanentGatewayError, SentMessage
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    Campaign,
    CampaignRun,
    RunStatus,
    Target,
    ensure_utc,
    utc_now,
)
from proxima_traffic.preflight import PreflightService


class DialogueActivityGateway:
    def __init__(self) -> None:
        self.latest_message_id = 30_000
        self.calls: list[dict[str, object]] = []

    async def send_message(self, **kwargs: object) -> SentMessage:
        self.latest_message_id += 1
        self.calls.append(kwargs)
        return SentMessage(message_id=self.latest_message_id)

    async def inspect_activity(self, **kwargs: object) -> ChatActivity:
        return ChatActivity(
            latest_message_id=self.latest_message_id,
            checked_at=utc_now(),
        )


class MediaDialogueGateway(DialogueActivityGateway):
    async def send_content(self, **kwargs: object) -> SentMessage:
        self.latest_message_id += 1
        self.calls.append(kwargs)
        return SentMessage(message_id=self.latest_message_id)


class DeletedDialogueAnchorGateway(DialogueActivityGateway):
    async def inspect_activity(self, **kwargs: object) -> ChatActivity:
        last_message_id = int(kwargs["last_message_id"])
        raise PermanentGatewayError(
            "публикация удалена модерацией; ответ чата: сначала пройдите проверку",
            code="post_deleted_by_moderation",
            external_message_id=last_message_id,
        )


async def _build_smart_dialogue(tmp_path: Path, gateway: DialogueActivityGateway):
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'smart-dialogue.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_pace_min_seconds=0,
        account_pace_max_seconds=0,
        account_daily_limit=0,
        account_micro_break_every=0,
    )
    app = create_app(settings=settings, gateway=gateway)
    await app.state.database.create_schema()
    headers = {"X-Admin-Key": "test-admin-key"}
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")

    accounts: list[dict[str, object]] = []
    for index in range(2):
        response = await client.post(
            "/api/v1/accounts",
            headers=headers,
            json={
                "label": f"dialogue-sender-{index + 1}",
                "telegram_user_id": 95_000 + index,
                "ownership_confirmed": True,
            },
        )
        assert response.status_code == 201, response.text
        accounts.append(response.json())

    target_response = await client.post(
        "/api/v1/targets",
        headers=headers,
        json={"chat_ref": "@smart_dialogue", "title": "Smart dialogue"},
    )
    assert target_response.status_code == 201, target_response.text
    target = target_response.json()
    approval = await client.post(f"/api/v1/targets/{target['id']}/approve", headers=headers)
    assert approval.status_code == 200, approval.text

    campaign_response = await client.post(
        "/api/v1/campaigns",
        headers=headers,
        json={
            "name": "smart-dialogue",
            "mode": "dialogue",
            "primary_account_id": accounts[0]["id"],
            "reply_account_id": accounts[1]["id"],
            "target_id": target["id"],
            "primary_text": "Question",
            "reply_text": "Answer",
            "reply_delay_seconds": 0,
            "dialogue_steps": [
                {
                    "account_id": accounts[0]["id"],
                    "text": "Question",
                    "delay_seconds": 0,
                    "reply_to_previous": False,
                },
                {
                    "account_id": accounts[1]["id"],
                    "text": "Answer",
                    "delay_seconds": 0,
                    "reply_to_previous": True,
                },
            ],
            "smart_timer_mode": True,
            "smart_duration_seconds": 3600,
            "smart_no_timer_interval_seconds": 60,
            "activity_aware": True,
            "min_messages_between_posts": 1,
            "activity_check_interval_seconds": 30,
            "visibility_max_wait_seconds": 300,
        },
    )
    assert campaign_response.status_code == 201, campaign_response.text
    return app, client, campaign_response.json(), accounts


async def _make_due(app: object, run_id: int) -> None:
    async with app.state.database.session_factory() as session:
        run = await session.get(CampaignRun, run_id)
        assert run is not None
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()


@pytest.mark.asyncio
async def test_smart_dialogue_repeats_only_after_complete_chain(tmp_path: Path) -> None:
    gateway = DialogueActivityGateway()
    app, client, campaign, accounts = await _build_smart_dialogue(tmp_path, gateway)
    try:
        runs = await app.state.run_queue.enqueue_campaign(
            int(campaign["id"]),
            "smart-dialogue-complete",
        )
        assert len(runs) == 1

        first_step = await app.state.queue_worker.process_next()
        assert first_step is not None
        assert first_step.status == RunStatus.QUEUED
        assert first_step.dialogue_step_index == 1
        assert len(gateway.calls) == 1

        second_step = await app.state.queue_worker.process_next()
        assert second_step is not None
        assert second_step.status == RunStatus.SUCCEEDED
        assert second_step.dialogue_step_index == 2
        assert len(gateway.calls) == 2
        assert gateway.calls[1]["reply_to_message_id"] == second_step.primary_message_id

        async with app.state.database.session_factory() as session:
            stored = list(
                await session.scalars(
                    select(CampaignRun)
                    .where(CampaignRun.launch_key == "smart-dialogue-complete")
                    .order_by(CampaignRun.cycle_index)
                )
            )
        assert len(stored) == 2
        successor = stored[1]
        assert successor.status == RunStatus.QUEUED
        assert successor.cycle_index == 1
        assert successor.dialogue_step_index == 0
        assert successor.repeat_until == stored[0].repeat_until
        assert successor.sender_account_id == int(accounts[0]["id"])

        repository = AdminRepository(app.state.database.session_factory)
        launch, launch_runs = await repository.get_launch(successor.id)
        report = launch_details_text(launch, launch_runs)
        assert "УМНЫЙ ДИАЛОГ" in report
        assert "Реально отправлено сообщений: <b>2</b>" in report
        assert "Завершено диалогов: <b>1</b>" in report
        assert "Продолжаются или ждут: <b>1</b>" in report
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_dialogue_delivers_captionless_voice_and_circle_steps(tmp_path: Path) -> None:
    gateway = MediaDialogueGateway()
    app, client, campaign, accounts = await _build_smart_dialogue(tmp_path, gateway)
    try:
        update = await client.patch(
            f"/api/v1/campaigns/{campaign['id']}",
            headers={"X-Admin-Key": "test-admin-key"},
            json={
                "dialogue_steps": [
                    {
                        "account_id": accounts[0]["id"],
                        "text": "",
                        "media_path": "voice.ogg",
                        "media_kind": "voice",
                        "media_name": "voice.ogg",
                        "media_size": 512,
                        "reply_to_previous": False,
                    },
                    {
                        "account_id": accounts[1]["id"],
                        "text": "",
                        "media_path": "circle.mp4",
                        "media_kind": "video_note",
                        "media_name": "circle.mp4",
                        "media_size": 512,
                        "reply_to_previous": True,
                    },
                ]
            },
        )
        assert update.status_code == 200, update.text

        runs = await app.state.run_queue.enqueue_campaign(
            int(campaign["id"]),
            "captionless-dialogue-media",
        )
        first = await app.state.queue_worker.process_next()
        assert first is not None and first.status == RunStatus.QUEUED
        second = await app.state.queue_worker.process_next()
        assert second is not None and second.status == RunStatus.SUCCEEDED

        assert [call["media_kind"] for call in gateway.calls] == ["voice", "video_note"]
        assert [call["text"] for call in gateway.calls] == ["", ""]
        assert gateway.calls[1]["reply_to_message_id"] == first.primary_message_id
        assert runs[0].id == first.id

        async with app.state.database.session_factory() as session:
            stored_campaign = await session.get(Campaign, int(campaign["id"]))
        assert stored_campaign is not None
        assert PreflightService._dialogue_media_kinds_by_account(stored_campaign) == {
            int(accounts[0]["id"]): frozenset({"voice"}),
            int(accounts[1]["id"]): frozenset({"video_note"}),
        }
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_dialogue_delivers_premium_emoji_markup_for_every_step(tmp_path: Path) -> None:
    gateway = MediaDialogueGateway()
    app, client, campaign, accounts = await _build_smart_dialogue(tmp_path, gateway)
    first_text = "Question ⭐"
    first_html = 'Question <tg-emoji emoji-id="543210987654321">⭐</tg-emoji>'
    second_text = "Answer 🔥"
    second_html = 'Answer <tg-emoji emoji-id="543210987654322">🔥</tg-emoji>'
    try:
        update = await client.patch(
            f"/api/v1/campaigns/{campaign['id']}",
            headers={"X-Admin-Key": "test-admin-key"},
            json={
                "dialogue_steps": [
                    {
                        "account_id": accounts[0]["id"],
                        "text": first_text,
                        "html_text": first_html,
                        "reply_to_previous": False,
                    },
                    {
                        "account_id": accounts[1]["id"],
                        "text": second_text,
                        "html_text": second_html,
                        "reply_to_previous": True,
                    },
                ]
            },
        )
        assert update.status_code == 200, update.text

        await app.state.run_queue.enqueue_campaign(int(campaign["id"]), "premium-dialogue")
        first = await app.state.queue_worker.process_next()
        second = await app.state.queue_worker.process_next()

        assert first is not None and first.status == RunStatus.QUEUED
        assert second is not None and second.status == RunStatus.SUCCEEDED
        assert [call["html_text"] for call in gateway.calls] == [first_html, second_html]
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_dialogue_stops_before_reply_when_moderation_deleted_primary(tmp_path: Path) -> None:
    gateway = DeletedDialogueAnchorGateway()
    app, client, campaign, _ = await _build_smart_dialogue(tmp_path, gateway)
    try:
        await app.state.run_queue.enqueue_campaign(
            int(campaign["id"]),
            "smart-dialogue-moderation-deleted",
        )
        first_step = await app.state.queue_worker.process_next()
        assert first_step is not None and first_step.status == RunStatus.QUEUED

        stopped = await app.state.queue_worker.process_next()
        assert stopped is not None and stopped.status == RunStatus.FAILED
        assert stopped.error_code == "post_deleted_by_moderation"
        assert len(gateway.calls) == 1

        async with app.state.database.session_factory() as session:
            target = await session.scalar(
                select(Target).where(Target.chat_ref == "@smart_dialogue")
            )
            assert target is not None and target.quality_status == "bad"
            assert target.quality_successes == 0
            assert target.quality_failures == 1
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_smart_dialogue_waits_while_service_message_is_last(tmp_path: Path) -> None:
    gateway = DialogueActivityGateway()
    app, client, campaign, _ = await _build_smart_dialogue(tmp_path, gateway)
    try:
        await app.state.run_queue.enqueue_campaign(
            int(campaign["id"]),
            "smart-dialogue-activity",
        )
        await app.state.queue_worker.process_next()
        await app.state.queue_worker.process_next()

        async with app.state.database.session_factory() as session:
            successor = await session.scalar(
                select(CampaignRun).where(
                    CampaignRun.launch_key == "smart-dialogue-activity",
                    CampaignRun.cycle_index == 1,
                )
            )
            assert successor is not None
            successor_id = successor.id

        await _make_due(app, successor_id)
        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None
        assert deferred.status == RunStatus.QUEUED
        assert deferred.error_code == "chat_idle_last_managed_post"
        assert len(gateway.calls) == 2

        gateway.latest_message_id += 1  # A real chat message appeared after the chain.
        await _make_due(app, successor_id)
        resumed = await app.state.queue_worker.process_next()
        assert resumed is not None
        assert resumed.status == RunStatus.QUEUED
        assert resumed.dialogue_step_index == 1
        assert len(gateway.calls) == 3
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_smart_dialogue_rejects_role_failover(tmp_path: Path) -> None:
    gateway = DialogueActivityGateway()
    app, client, campaign, _ = await _build_smart_dialogue(tmp_path, gateway)
    try:
        response = await client.patch(
            f"/api/v1/campaigns/{campaign['id']}",
            headers={"X-Admin-Key": "test-admin-key"},
            json={"auto_sender_failover": True},
        )
        assert response.status_code == 422
        assert "один проход или умный режим" in response.text
    finally:
        await client.aclose()
        await app.state.database.dispose()


@pytest.mark.asyncio
async def test_started_dialogue_finishes_after_launch_window_if_account_must_wait(
    tmp_path: Path,
) -> None:
    gateway = DialogueActivityGateway()
    app, client, campaign, accounts = await _build_smart_dialogue(tmp_path, gateway)
    try:
        runs = await app.state.run_queue.enqueue_campaign(
            int(campaign["id"]),
            "smart-dialogue-finish-started",
        )
        first_step = await app.state.queue_worker.process_next()
        assert first_step is not None and first_step.dialogue_step_index == 1

        cooldown_until = utc_now() + timedelta(minutes=5)
        async with app.state.database.session_factory() as session:
            run = await session.get(CampaignRun, runs[0].id)
            reply_account = await session.get(Account, int(accounts[1]["id"]))
            assert run is not None and reply_account is not None
            run.repeat_until = utc_now() + timedelta(minutes=1)
            run.scheduled_for = utc_now() - timedelta(seconds=1)
            reply_account.cooldown_until = cooldown_until
            reply_account.cooldown_reason = "flood_wait"
            await session.commit()

        deferred = await app.state.queue_worker.process_next()
        assert deferred is not None
        assert deferred.status == RunStatus.QUEUED
        assert deferred.dialogue_step_index == 1
        assert ensure_utc(deferred.scheduled_for) is not None
        assert ensure_utc(deferred.repeat_until) is not None
        assert ensure_utc(deferred.scheduled_for) > ensure_utc(deferred.repeat_until)
        assert ensure_utc(deferred.scheduled_for) >= cooldown_until - timedelta(seconds=1)
        assert len(gateway.calls) == 1
    finally:
        await client.aclose()
        await app.state.database.dispose()
