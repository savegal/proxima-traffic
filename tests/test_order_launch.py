import asyncio
from types import SimpleNamespace

import pytest

import proxima_traffic.bot.handlers as handlers
from proxima_traffic.bot.handlers import (
    _active_capacity_warning,
    _create_campaign_from_order_draft,
    _order_draft_preview_variant,
)
from proxima_traffic.models import BroadcastSenderMode, Campaign, CampaignMode
from proxima_traffic.scheduling import CapacityEstimate
from proxima_traffic.services import ConflictError


class _CampaignRepository:
    def __init__(self) -> None:
        self.values: dict[str, object] | None = None

    async def create_campaign(self, **values: object):
        self.values = values
        return SimpleNamespace(**values)


class _ProgressCallback:
    message = None

    def __init__(self) -> None:
        self.answers: list[dict[str, object]] = []

    async def answer(self, **kwargs: object) -> None:
        self.answers.append(kwargs)


class _ProgressPreflight:
    def __init__(self, result: object) -> None:
        self.result = result
        self.campaign_id: int | None = None

    async def inspect_campaign(self, campaign_id: int, *, on_progress):  # type: ignore[no-untyped-def]
        self.campaign_id = campaign_id
        for checked_pairs in range(102):
            await on_progress(
                checked_pairs,
                101,
                min(3, (checked_pairs + 33) // 34),
                3,
            )
        return self.result


class _DraftState:
    def __init__(self) -> None:
        self.data: dict[str, object] = {"order_draft_active": True}
        self.clear_count = 0

    async def get_data(self) -> dict[str, object]:
        return dict(self.data)

    async def update_data(self, **values: object) -> dict[str, object]:
        self.data.update(values)
        return dict(self.data)

    async def clear(self) -> None:
        self.clear_count += 1
        self.data.clear()


class _LaunchCallback:
    message = None

    def __init__(self, operator_id: int) -> None:
        self.from_user = SimpleNamespace(id=operator_id)
        self.answers: list[dict[str, object]] = []

    async def answer(self, *args: object, **kwargs: object) -> None:
        if args:
            kwargs["text"] = args[0]
        self.answers.append(kwargs)


@pytest.mark.asyncio
async def test_new_broadcast_defaults_to_active_distributed_delivery_with_failover() -> None:
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
    assert repository.values["smart_timer_mode"] is True
    assert repository.values["activity_aware"] is True
    assert repository.values["sender_mode"] == BroadcastSenderMode.DISTRIBUTED
    assert repository.values["auto_sender_failover"] is True
    assert repository.values["repeat_count"] == 1


@pytest.mark.asyncio
async def test_active_draft_can_run_until_manually_stopped() -> None:
    repository = _CampaignRepository()

    await _create_campaign_from_order_draft(
        {
            "order_name": "Indefinite active broadcast",
            "order_mode": "broadcast",
            "target_id": 7,
            "sender_account_ids": [11, 12],
            "primary_text": "Promotion",
            "smart_timer_mode": True,
            "repeat_forever": True,
            "min_messages_between_posts": 12,
            "visibility_max_wait_seconds": 0,
        },
        repository,  # type: ignore[arg-type]
    )

    assert repository.values is not None
    assert repository.values["repeat_forever"] is True
    assert repository.values["smart_duration_seconds"] == 0
    assert repository.values["min_messages_between_posts"] == 12
    assert repository.values["visibility_max_wait_seconds"] == 0


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
async def test_checked_chats_and_publication_actions_are_saved_with_new_broadcast() -> None:
    repository = _CampaignRepository()

    await _create_campaign_from_order_draft(
        {
            "order_name": "Checked audience",
            "order_selection_key": "7fa1e2",
            "order_mode": "broadcast",
            "order_selected_target_ids": [17, 23, 17],
            "sender_account_ids": [11, 12],
            "primary_text": "Promotion",
            "reactions_enabled": True,
            "reaction_count": 3,
            "reaction_emoji": "🔥",
            "hidden_mentions_enabled": True,
            "mention_cooldown_days": 14,
        },
        repository,  # type: ignore[arg-type]
    )

    assert repository.values is not None
    assert repository.values["target_id"] is None
    assert repository.values["target_list_id"] is None
    assert repository.values["selected_target_ids"] == [17, 23]
    assert str(repository.values["selected_target_list_name"]).startswith("Подборка")
    assert repository.values["reactions_enabled"] is True
    assert repository.values["reaction_count"] == 3
    assert repository.values["reaction_emoji"] == "🔥"
    assert repository.values["hidden_mentions_enabled"] is True
    assert repository.values["mention_cooldown_days"] == 14


def test_order_preview_variant_preserves_formatted_media_draft() -> None:
    preview = _order_draft_preview_variant(
        {
            "order_mode": "broadcast",
            "primary_text": "Hello {world|team}",
            "primary_html": "<b>Hello {world|team}</b>",
            "primary_media_path": "uploads/post.jpg",
            "primary_media_kind": "photo",
            "primary_media_name": "post.jpg",
            "primary_media_size": 1024,
        }
    )

    assert preview.text == "Hello {world|team}"
    assert preview.html_text == "<b>Hello {world|team}</b>"
    assert preview.media[0].path == "uploads/post.jpg"
    assert preview.media[0].kind == "photo"


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
async def test_all_preflight_entry_points_use_visible_throttled_progress(monkeypatch) -> None:
    callback = _ProgressCallback()
    result = object()
    preflight = _ProgressPreflight(result)
    cards: list[str] = []

    async def capture_card(  # type: ignore[no-untyped-def]
        _callback,
        text: str,
        _reply_markup,
        *,
        answer: bool = True,
    ) -> None:
        assert answer is False
        cards.append(text)

    monkeypatch.setattr(handlers, "_edit_callback", capture_card)

    actual = await handlers._inspect_campaign_with_visible_progress(
        callback,  # type: ignore[arg-type]
        preflight,  # type: ignore[arg-type]
        41,
    )

    assert actual is result
    assert preflight.campaign_id == 41
    assert callback.answers == [{"text": "Проверяю готовность чатов и аккаунтов…"}]
    assert len(cards) <= 12
    assert "0/101" in cards[0]
    assert "101/101" in cards[-1]


@pytest.mark.asyncio
async def test_order_launch_ignores_a_second_tap_while_preflight_is_running(monkeypatch) -> None:
    operator_id = 91_001
    handlers._order_launch_locks.pop(operator_id, None)
    state = _DraftState()
    first = _LaunchCallback(operator_id)
    second = _LaunchCallback(operator_id)
    campaign = SimpleNamespace(id=17, smart_timer_mode=True, repeat_count=1)
    started = asyncio.Event()
    proceed = asyncio.Event()
    create_calls = 0
    cards: list[str] = []

    async def create_draft(_data, _repository):  # type: ignore[no-untyped-def]
        nonlocal create_calls
        create_calls += 1
        return campaign

    async def inspect(_callback, _service, _campaign_id, **_kwargs):  # type: ignore[no-untyped-def]
        started.set()
        await proceed.wait()
        return SimpleNamespace(launchable_units=1, target_count=1, deferred_pairs=0)

    async def sender_ids(_campaign_id: int) -> list[int]:
        return [11]

    async def capture_card(  # type: ignore[no-untyped-def]
        _callback,
        text: str,
        _reply_markup,
        *,
        answer: bool = True,
    ) -> None:
        assert answer is False
        cards.append(text)

    monkeypatch.setattr(handlers, "_create_campaign_from_order_draft", create_draft)
    monkeypatch.setattr(handlers, "_inspect_campaign_with_visible_progress", inspect)
    monkeypatch.setattr(handlers, "_edit_callback", capture_card)
    monkeypatch.setattr(handlers, "campaign_launch_review_text", lambda *_args, **_kwargs: "review")
    monkeypatch.setattr(handlers, "campaign_run_confirmation", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        handlers,
        "_campaign_active_capacity_warning",
        lambda *_args, **_kwargs: None,
    )
    repository = SimpleNamespace(campaign_sender_ids=sender_ids)

    first_task = asyncio.create_task(
        handlers.launch_order_draft(
            first,  # type: ignore[arg-type]
            state,  # type: ignore[arg-type]
            repository,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=10)
        await handlers.launch_order_draft(
            second,  # type: ignore[arg-type]
            state,  # type: ignore[arg-type]
            repository,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
        proceed.set()
        await first_task
    finally:
        proceed.set()
        if not first_task.done():
            await first_task
        handlers._order_launch_locks.pop(operator_id, None)

    assert create_calls == 1
    assert state.clear_count == 1
    assert cards == ["review"]
    assert second.answers == [
        {"text": "Проверка этой рассылки уже идёт. Экран обновится по её завершении."}
    ]


@pytest.mark.asyncio
async def test_order_launch_preserves_saved_campaign_when_preflight_crashes(monkeypatch) -> None:
    operator_id = 91_002
    handlers._order_launch_locks.pop(operator_id, None)
    callback = _LaunchCallback(operator_id)
    state = _DraftState()
    campaign = SimpleNamespace(id=18, name="Saved", smart_timer_mode=True, repeat_count=1)
    cards: list[str] = []

    async def create_draft(_data, _repository):  # type: ignore[no-untyped-def]
        return campaign

    async def crash(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("temporary preflight failure")

    async def sender_ids(_campaign_id: int) -> list[int]:
        return [11]

    async def capture_card(  # type: ignore[no-untyped-def]
        _callback,
        text: str,
        _reply_markup,
        *,
        answer: bool = True,
    ) -> None:
        assert answer is False
        cards.append(text)

    monkeypatch.setattr(handlers, "_create_campaign_from_order_draft", create_draft)
    monkeypatch.setattr(handlers, "_inspect_campaign_with_visible_progress", crash)
    monkeypatch.setattr(handlers, "_edit_callback", capture_card)
    monkeypatch.setattr(handlers, "campaign_details_text", lambda *_args, **_kwargs: "details")
    monkeypatch.setattr(handlers, "campaign_details_menu", lambda *_args, **_kwargs: object())
    repository = SimpleNamespace(campaign_sender_ids=sender_ids)

    try:
        await handlers.launch_order_draft(
            callback,  # type: ignore[arg-type]
            state,  # type: ignore[arg-type]
            repository,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
    finally:
        handlers._order_launch_locks.pop(operator_id, None)

    assert state.clear_count == 1
    assert len(cards) == 1
    assert "Рассылка сохранена" in cards[0]
    assert "Ничего не отправлено" in cards[0]


def test_active_capacity_warning_discloses_finite_window_shortfall() -> None:
    campaign = Campaign(
        id=44,
        name="Capacity",
        mode=CampaignMode.BROADCAST,
        primary_account_id=1,
        target_id=3,
        primary_text="Post",
        smart_timer_mode=True,
        smart_duration_seconds=12 * 3600,
        smart_no_timer_interval_seconds=5 * 60,
    )

    warning = _active_capacity_warning(
        campaign,
        CapacityEstimate(
            messages_per_day=120,
            requested_per_day=360,
            full_round_seconds=30 * 60,
        ),
        target_count=10,
        independent_streams=2,
    )

    assert warning is not None
    assert "240 публикаций не успеют" in warning
    assert "не будут отправлены после окончания срока" in warning
