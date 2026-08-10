from types import SimpleNamespace

import pytest

from proxima_traffic.bot.handlers import (
    _create_campaign_from_order_draft,
    _preflight_and_enqueue_order,
)
from proxima_traffic.models import BroadcastSenderMode
from proxima_traffic.services import ConflictError


class _CampaignRepository:
    def __init__(self) -> None:
        self.values: dict[str, object] | None = None

    async def create_campaign(self, **values: object):
        self.values = values
        return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_new_broadcast_defaults_to_one_distributed_pass_with_failover() -> None:
    repository = _CampaignRepository()

    await _create_campaign_from_order_draft(
        {
            "order_name": "Safe broadcast",
            "order_mode": "broadcast",
            "target_id": 7,
            "sender_account_ids": [11, 12],
            "primary_text": "Promotion",
        },
        repository,  # type: ignore[arg-type]
    )

    assert repository.values is not None
    assert repository.values["smart_timer_mode"] is False
    assert repository.values["activity_aware"] is False
    assert repository.values["sender_mode"] == BroadcastSenderMode.DISTRIBUTED
    assert repository.values["auto_sender_failover"] is True
    assert repository.values["repeat_count"] == 1


@pytest.mark.asyncio
async def test_media_only_broadcast_is_a_valid_post() -> None:
    repository = _CampaignRepository()

    await _create_campaign_from_order_draft(
        {
            "order_name": "Voice update",
            "order_mode": "broadcast",
            "target_id": 7,
            "sender_account_ids": [11],
            "primary_text": "",
            "primary_media_path": "uploads/voice.ogg",
            "primary_media_kind": "voice",
            "primary_media_name": "voice.ogg",
            "primary_media_size": 1024,
        },
        repository,  # type: ignore[arg-type]
    )

    assert repository.values is not None
    assert repository.values["primary_text"] == ""
    assert repository.values["primary_media_path"] == "uploads/voice.ogg"
    assert repository.values["primary_media_kind"] == "voice"


@pytest.mark.asyncio
async def test_order_rejects_a_post_without_text_or_media() -> None:
    with pytest.raises(ConflictError, match="текст или вложение"):
        await _create_campaign_from_order_draft(
            {
                "order_name": "Empty",
                "order_mode": "broadcast",
                "target_id": 7,
                "sender_account_ids": [11],
                "primary_text": "",
            },
            _CampaignRepository(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_order_preflight_always_completes_before_enqueue() -> None:
    events: list[str] = []
    summary = object()
    runs = [object()]

    class Preflight:
        async def inspect_campaign(self, campaign_id: int):
            events.append(f"preflight:{campaign_id}")
            return summary

    class Queue:
        async def enqueue_campaign(self, campaign_id: int, request_key: str):
            events.append(f"enqueue:{campaign_id}:{request_key}")
            return runs

    result = await _preflight_and_enqueue_order(
        17,
        Preflight(),  # type: ignore[arg-type]
        Queue(),  # type: ignore[arg-type]
        request_key="launch-key",
    )

    assert result == (summary, runs)
    assert events == ["preflight:17", "enqueue:17:launch-key"]


@pytest.mark.asyncio
async def test_order_is_not_enqueued_when_preflight_fails() -> None:
    events: list[str] = []

    class Preflight:
        async def inspect_campaign(self, campaign_id: int):
            events.append(f"preflight:{campaign_id}")
            raise ConflictError("not ready")

    class Queue:
        async def enqueue_campaign(self, campaign_id: int, request_key: str):
            events.append("enqueue")
            return []

    with pytest.raises(ConflictError, match="not ready"):
        await _preflight_and_enqueue_order(
            19,
            Preflight(),  # type: ignore[arg-type]
            Queue(),  # type: ignore[arg-type]
            request_key="launch-key",
        )

    assert events == ["preflight:19"]
