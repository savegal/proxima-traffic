from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from proxima_traffic.channel_comments import ChannelCommentService
from proxima_traffic.dialogues import DialogueStep
from proxima_traffic.gateways import (
    ChannelCommentDelivery,
    ChannelCommentPreflight,
    ChannelPost,
    ChannelPostScan,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.models import (
    Account,
    ChannelCommentMentionState,
    ChannelCommentMode,
    ChannelCommentReactionAccountState,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
    MentionBlacklistEntry,
    ReactionTask,
    ReactionTaskStatus,
    utc_now,
)
from proxima_traffic.reactions import ReactionWorker
from proxima_traffic.services import ConflictError, NotFoundError


class RecordingChannelGateway:
    def __init__(self) -> None:
        self.posts: tuple[ChannelPost, ...] = ()
        self.calls: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self.delivery_error: BaseException | None = None
        self.edit_error: BaseException | None = None
        self.preflight_error: BaseException | None = None
        self.preflight_calls: list[tuple[int, str]] = []
        self.reaction_calls: list[dict[str, object]] = []
        self.reaction_errors: dict[int, BaseException] = {}
        self._message_id = 40_000
        self.participants = (50_001, 50_002, 50_003, 50_004, 50_005, 50_006, 50_007)

    async def preflight_channel_comments(
        self,
        *,
        account: Account,
        channel_ref: str,
    ) -> ChannelCommentPreflight:
        if self.preflight_error is not None:
            raise self.preflight_error
        self.preflight_calls.append((int(account.id or 0), channel_ref))
        return ChannelCommentPreflight(
            title="Тестовый канал",
            channel_post_id=1,
            discussion_root_message_id=1,
        )

    async def scan_channel_posts(
        self,
        *,
        account: Account,
        channel_ref: str,
        after_message_id: int,
        limit: int,
        bootstrap: bool = False,
        after_date: object | None = None,
    ) -> ChannelPostScan:
        del account, channel_ref, bootstrap, after_date
        return ChannelPostScan(
            title="Тестовый канал",
            posts=tuple(post for post in self.posts if post.message_id > after_message_id)[:limit],
        )

    async def send_channel_comment(
        self,
        **kwargs: object,
    ) -> ChannelCommentDelivery:
        if self.delivery_error is not None:
            raise self.delivery_error
        self.calls.append(kwargs)
        self._message_id += 1
        return ChannelCommentDelivery(
            message_id=self._message_id,
            thread_root_message_id=int(kwargs["channel_post_id"]),
        )

    async def edit_channel_comment(
        self,
        **kwargs: object,
    ) -> ChannelCommentDelivery:
        if self.edit_error is not None:
            raise self.edit_error
        self.edits.append(kwargs)
        return ChannelCommentDelivery(
            message_id=int(kwargs["message_id"]),
            thread_root_message_id=int(kwargs["channel_post_id"]),
        )

    async def collect_channel_comment_mentionable_user_ids(
        self,
        **kwargs: object,
    ) -> tuple[int, ...]:
        del kwargs
        return self.participants

    async def send_channel_comment_reaction(self, **kwargs: object) -> None:
        self.reaction_calls.append(kwargs)
        account = kwargs["account"]
        error = self.reaction_errors.get(int(account.id))
        if error is not None:
            raise error


class BlockingChannelGateway(RecordingChannelGateway):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send_channel_comment(
        self,
        **kwargs: object,
    ) -> ChannelCommentDelivery:
        self.started.set()
        await self.release.wait()
        return await super().send_channel_comment(**kwargs)


class HangingScanChannelGateway(RecordingChannelGateway):
    async def scan_channel_posts(self, **kwargs: object) -> ChannelPostScan:
        del kwargs
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def _account(app: FastAPI, label: str) -> Account:
    async with app.state.database.session_factory() as session:
        last_user_id = await session.scalar(select(func.max(Account.telegram_user_id)))
        account = Account(
            label=label,
            telegram_user_id=max(9_000, last_user_id or 0) + 1,
            ownership_confirmed=True,
        )
        session.add(account)
        await session.commit()
        await session.refresh(account)
        return account


def _service(app: FastAPI, gateway: RecordingChannelGateway) -> ChannelCommentService:
    return ChannelCommentService(
        app.state.database.session_factory,
        gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="test-channel-comments",
    )


@pytest.mark.asyncio
async def test_comment_reactions_accept_the_last_step_of_a_long_dialogue(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    accounts = [
        await _account(app, f"long-comment-reaction-{index}") for index in range(3)
    ]
    steps = [
        DialogueStep(
            account_id=accounts[index % 3].id,
            text=f"Comment {index + 1}",
            delay_seconds=0,
            reply_to_previous=index > 0,
        )
        for index in range(5)
    ]
    rule = await service.create_rule(
        name="Long comment reaction step",
        channel_ref="@long_comment_reaction_step",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=int(accounts[0].id),
        primary_text="Comment 1",
        initial_delay_seconds=0,
        dialogue_steps=steps,
    )

    updated = await service.update_rule_reactions(
        int(rule.id),
        enabled=True,
        count=2,
        emoji="🔥",
        dialogue_step=5,
        account_ids=[int(account.id) for account in accounts],
    )

    assert updated.reaction_dialogue_step == 5
    assert updated.reaction_count == 2
    with pytest.raises(ConflictError, match="такого комментария в цепочке нет"):
        await service.update_rule_reactions(int(rule.id), dialogue_step=6)


@pytest.mark.asyncio
async def test_channel_comment_reactions_exclude_author_and_fail_over_blocked_route(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    async with app.state.database.session_factory() as session:
        source = Account(
            label="comment-reaction-source",
            authorized=True,
            ownership_confirmed=True,
        )
        reactors = [
            Account(
                label=f"comment-reactor-{index}",
                authorized=True,
                ownership_confirmed=True,
            )
            for index in range(2)
        ]
        session.add_all([source, *reactors])
        await session.commit()
        source_id = int(source.id)
        reactor_ids = [int(account.id) for account in reactors]

    rule = await service.create_rule(
        name="Comment reactions",
        channel_ref="@comment_reactions",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=source_id,
        primary_text="Own comment",
        initial_delay_seconds=0,
    )
    await service.update_rule_reactions(
        int(rule.id),
        enabled=True,
        count=1,
        emoji="🔥",
        account_ids=[source_id, *reactor_ids],
    )
    gateway.posts = (ChannelPost(message_id=501, published_at=utc_now()),)
    await service.scan_rule(int(rule.id))
    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        run = await session.scalar(select(ChannelCommentRun))
        assert task is not None and run is not None
        assert task.channel_comment_run_id == run.id
        assert task.campaign_id is None and task.run_id is None and task.target_id is None
        assert task.reactor_account_id in reactor_ids
        assert task.reactor_account_id != source_id
        assert task.emoji == "🔥"
        first_reactor_id = int(task.reactor_account_id)
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    gateway.reaction_errors[first_reactor_id] = PermanentGatewayError(
        "reactor is blocked in discussion",
        code="account_banned_in_chat",
    )
    worker = ReactionWorker(
        app.state.database.session_factory,
        gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="comment-reaction-test",
    )
    assert await worker.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        blocked = await session.scalar(select(ChannelCommentReactionAccountState))
        assert task is not None and task.status == ReactionTaskStatus.QUEUED
        assert int(task.reactor_account_id) != first_reactor_id
        assert blocked is not None and blocked.account_id == first_reactor_id
        assert blocked.send_blocked is True
        task.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await worker.process_next() is True
    async with app.state.database.session_factory() as session:
        task = await session.scalar(select(ReactionTask))
        assert task is not None and task.status == ReactionTaskStatus.SUCCEEDED
    successful = gateway.reaction_calls[-1]
    assert successful["channel_ref"] == "@comment_reactions"
    assert successful["channel_post_id"] == 501
    assert successful["message_id"] == 40_001


@pytest.mark.asyncio
async def test_channel_comments_ignore_old_posts_and_do_not_duplicate(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "comment-sender")
    rule = await service.create_rule(
        name="Новости",
        channel_ref="@test_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Полезный комментарий",
        initial_delay_seconds=0,
    )
    gateway.posts = (
        ChannelPost(
            message_id=10,
            published_at=rule.armed_at - timedelta(seconds=1),
        ),
    )

    await service.scan_rule(rule.id)
    assert await service.recent_runs(rule.id) == []

    gateway.posts = (ChannelPost(message_id=11, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    await service.scan_rule(rule.id)

    runs = await service.recent_runs(rule.id)
    assert len(runs) == 1
    assert runs[0].source_post_id == 11
    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        stored_account = await session.get(Account, account.id)
    assert run is not None and run.status == ChannelCommentRunStatus.SUCCEEDED
    assert stored_account is not None and stored_account.sent_today == 1
    assert gateway.calls[0]["reply_to_message_id"] is None


@pytest.mark.asyncio
async def test_channel_comment_timeout_releases_the_run_for_retry(app: FastAPI) -> None:
    app.state.settings.worker_step_timeout_seconds = 0.05
    gateway = BlockingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "timeout-comment-sender")
    rule = await service.create_rule(
        name="Comment timeout",
        channel_ref="@comment_timeout",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Timeout test comment",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=12, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
    assert run is not None
    assert run.status == ChannelCommentRunStatus.QUEUED
    assert run.error_code == "comment_timeout"
    assert run.lease_token is None


@pytest.mark.asyncio
async def test_channel_scan_timeout_is_visible_and_does_not_hang(app: FastAPI) -> None:
    app.state.settings.channel_comments_scan_timeout_seconds = 0.05
    gateway = HangingScanChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "timeout-scan-sender")
    rule = await service.create_rule(
        name="Scan timeout",
        channel_ref="@scan_timeout",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )

    await asyncio.wait_for(service.scan_rule(rule.id), timeout=1)

    view = await service.get_rule_view(rule.id)
    assert view.rule.last_error_code == "channel_scan_timeout"
    assert view.rule.last_error


@pytest.mark.asyncio
async def test_expired_cancelled_channel_comment_has_unknown_outcome(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "expired-cancelled-comment-sender")
    rule = await service.create_rule(
        name="Expired cancelled comment",
        channel_ref="@expired_comment",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=13, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    async with app.state.database.session_factory() as session:
        run = await session.scalar(
            select(ChannelCommentRun).where(ChannelCommentRun.rule_id == rule.id)
        )
        assert run is not None
        run.status = ChannelCommentRunStatus.RUNNING
        run.attempt_count = 1
        run.lease_owner = "expired-worker"
        run.lease_token = "expired-comment-token"
        run.lease_until = utc_now() - timedelta(seconds=1)
        run.cancel_requested = True
        await session.commit()
        run_id = int(run.id)

    assert await service.process_next() is False
    async with app.state.database.session_factory() as session:
        stored = await session.get(ChannelCommentRun, run_id)
        assert stored is not None
        assert stored.status == ChannelCommentRunStatus.FAILED
        assert stored.error_code == "cancelled_outcome_unknown"
        assert stored.lease_token is None


@pytest.mark.asyncio
async def test_channel_comment_dialogue_replies_to_previous_comment(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "first-commenter")
    second = await _account(app, "second-commenter")
    rule = await service.create_rule(
        name="Диалог под постом",
        channel_ref="@dialogue_channel",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=first.id,
        primary_text="Первое сообщение",
        initial_delay_seconds=0,
        dialogue_steps=[
            DialogueStep(
                account_id=first.id,
                text="Первое сообщение",
                delay_seconds=0,
                reply_to_previous=False,
            ),
            DialogueStep(
                account_id=second.id,
                text="Ответ на первое",
                delay_seconds=30,
                reply_to_previous=True,
            ),
        ],
    )
    gateway.posts = (ChannelPost(message_id=20, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        assert run is not None
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
    assert run is not None and run.status == ChannelCommentRunStatus.SUCCEEDED
    assert [call["account"].id for call in gateway.calls] == [first.id, second.id]
    assert gateway.calls[0]["reply_to_message_id"] is None
    assert gateway.calls[1]["reply_to_message_id"] == 40_001


@pytest.mark.asyncio
async def test_channel_comment_hidden_mentions_are_sent_once_on_the_first_comment(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    blocked_user_id = 7_588_293_197
    gateway.participants = (blocked_user_id, 50_001, 50_002, 50_003, 50_004, 50_005, 50_006)
    service = _service(app, gateway)
    account = await _account(app, "mention-comment-sender")
    async with app.state.database.session_factory() as session:
        session.add(
            MentionBlacklistEntry(
                telegram_user_id=blocked_user_id,
                source="test",
            )
        )
        await session.commit()
    rule = await service.create_rule(
        name="Отметки в комментарии",
        channel_ref="@mention_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Первый комментарий",
        initial_delay_seconds=0,
    )
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    gateway.posts = (ChannelPost(message_id=21, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    assert len(gateway.calls) == 1
    html_text = gateway.calls[0]["html_text"]
    assert isinstance(html_text, str)
    assert html_text.startswith("Первый комментарий")
    assert html_text.count("tg://user?id=") in range(5, 11)
    assert f"tg://user?id={blocked_user_id}" not in html_text

    async with app.state.database.session_factory() as session:
        run = await session.scalar(
            select(ChannelCommentRun).where(ChannelCommentRun.rule_id == rule.id)
        )
    assert run is not None
    assert run.hidden_mention_user_ids_snapshot_json is not None
    assert blocked_user_id not in json.loads(run.hidden_mention_user_ids_snapshot_json)
    assert run.hidden_mention_resolved_count in range(5, 11)
    assert run.hidden_mention_skip_reason is None
    view = await service.get_rule_view(rule.id)
    assert view.mentions_resolved_posts == 1
    assert view.mentions_skipped_posts == 0
    assert view.mention_count == run.hidden_mention_resolved_count


@pytest.mark.asyncio
async def test_saved_comment_mentions_are_reconciled_with_owned_accounts(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "reconciled-comment-mention-sender")
    rule = await service.create_rule(
        name="Reconciled comment mentions",
        channel_ref="@reconciled_comment_mentions",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    blocked_user_id = 5_993_714_739
    async with app.state.database.session_factory() as session:
        session.add(
            MentionBlacklistEntry(
                telegram_user_id=blocked_user_id,
                source="test",
            )
        )
        await session.commit()
    gateway.posts = (ChannelPost(message_id=212, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    async with app.state.database.session_factory() as session:
        run = await session.scalar(
            select(ChannelCommentRun).where(ChannelCommentRun.rule_id == rule.id)
        )
        assert run is not None
        run.hidden_mention_user_ids_snapshot_json = (
            f"[{blocked_user_id}, 50001, 50002, 50003, 50004]"
        )
        run.hidden_mention_resolved_count = 5
        await session.commit()
        run_id = int(run.id)

    assert await service.process_next() is True
    assert gateway.calls[0]["html_text"] is None
    async with app.state.database.session_factory() as session:
        stored = await session.get(ChannelCommentRun, run_id)
        assert stored is not None
        assert stored.hidden_mention_user_ids_snapshot_json == "[]"
        assert stored.hidden_mention_resolved_count == 0
        assert stored.hidden_mention_skip_reason == "excluded_user_after_resolution"
    view = await service.get_rule_view(rule.id)
    assert view.mentions_resolved_posts == 0
    assert view.mentions_skipped_posts == 1
    assert view.mention_count == 0


@pytest.mark.asyncio
async def test_failed_channel_comment_does_not_report_mentions_as_delivered(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    gateway.delivery_error = PermanentGatewayError("send rejected", code="request_invalid")
    service = _service(app, gateway)
    account = await _account(app, "failed-mention-comment-sender")
    rule = await service.create_rule(
        name="Недоставленные отметки",
        channel_ref="@failed_mention_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Первый комментарий",
        initial_delay_seconds=0,
    )
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    gateway.posts = (ChannelPost(message_id=211, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    view = await service.get_rule_view(rule.id)
    run = (await service.recent_runs(rule.id))[0]
    assert run.hidden_mention_resolved_count in range(5, 11)
    assert run.sent_steps == 0
    assert view.mentions_resolved_posts == 0
    assert view.mention_count == 0


@pytest.mark.asyncio
async def test_queued_channel_comment_does_not_inherit_a_later_mentions_toggle(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "legacy-mention-comment-sender")
    rule = await service.create_rule(
        name="Старый комментарий без отметок",
        channel_ref="@legacy_mention_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Комментарий",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=22, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    await service.set_rule_hidden_mentions_enabled(rule.id, True)

    assert await service.process_next() is True
    assert gateway.calls[0]["html_text"] is None


@pytest.mark.asyncio
async def test_message_edit_replaces_the_original_comment_after_its_due_time(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "message-edit-sender")
    rule = await service.create_rule(
        name="Message Edit",
        channel_ref="@message_edit_channel",
        mode=ChannelCommentMode.MESSAGE_EDIT,
        primary_account_id=account.id,
        primary_text="Финальный комментарий",
        initial_delay_seconds=0,
        message_edit_initial_text="Ага",
        message_edit_delay_seconds=15 * 60,
    )
    gateway.posts = (ChannelPost(message_id=25, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        stored_account = await session.get(Account, account.id)
        assert run is not None
        assert run.status == ChannelCommentRunStatus.QUEUED
        assert run.step_index == 1
        assert run.sent_steps == 1
        assert run.last_comment_message_id == 40_001
        assert stored_account is not None and stored_account.sent_today == 1
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert gateway.calls[0]["text"] == "Ага"
    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        stored_account = await session.get(Account, account.id)
    assert run is not None
    assert run.status == ChannelCommentRunStatus.SUCCEEDED
    assert run.step_index == 2
    assert run.sent_steps == 2
    assert run.last_comment_message_id == 40_001
    assert stored_account is not None and stored_account.sent_today == 1
    assert len(gateway.edits) == 1
    edit = gateway.edits[0]
    assert edit["account"].id == account.id
    assert edit["channel_ref"] == "@message_edit_channel"
    assert edit["channel_post_id"] == 25
    assert edit["message_id"] == 40_001
    assert edit["text"] == "Финальный комментарий"
    assert edit["idempotency_key"] == f"channel-comment:{run.id}:step:1"


@pytest.mark.asyncio
async def test_message_edit_rejects_delay_outside_five_to_thirty_minutes(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "message-edit-delay")

    with pytest.raises(ConflictError, match="от 5 до 30 минут"):
        await service.create_rule(
            name="Слишком быстро",
            channel_ref="@message_edit_fast",
            mode=ChannelCommentMode.MESSAGE_EDIT,
            primary_account_id=account.id,
            primary_text="Финальный текст",
            initial_delay_seconds=0,
            message_edit_initial_text="Ага",
            message_edit_delay_seconds=4 * 60,
        )


@pytest.mark.asyncio
async def test_message_edit_does_not_switch_author_while_an_edit_is_pending(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "message-edit-first")
    second = await _account(app, "message-edit-second")
    rule = await service.create_rule(
        name="Смена Message Edit",
        channel_ref="@message_edit_replace",
        mode=ChannelCommentMode.MESSAGE_EDIT,
        primary_account_id=first.id,
        primary_text="Финальный текст",
        initial_delay_seconds=0,
        message_edit_initial_text="Ага",
        message_edit_delay_seconds=5 * 60,
    )
    gateway.posts = (ChannelPost(message_id=26, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True

    with pytest.raises(ConflictError, match="ожидается редактирование"):
        await service.replace_primary_account(rule.id, second.id)


@pytest.mark.asyncio
async def test_dialogue_does_not_switch_author_during_an_interstep_delay(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "dialogue-delay-first")
    reply = await _account(app, "dialogue-delay-reply")
    replacement = await _account(app, "dialogue-delay-replacement")
    rule = await service.create_rule(
        name="Смена во время паузы диалога",
        channel_ref="@dialogue_delay_replace",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=first.id,
        primary_text="Первый",
        initial_delay_seconds=0,
        dialogue_steps=[
            DialogueStep(first.id, "Первый", 0, False),
            DialogueStep(reply.id, "Второй", 300, True),
        ],
    )
    gateway.posts = (ChannelPost(message_id=28, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True

    with pytest.raises(ConflictError, match="выполняется цепочка"):
        await service.replace_primary_account(rule.id, replacement.id)

    run = (await service.recent_runs(rule.id))[0]
    assert run.status == ChannelCommentRunStatus.QUEUED
    assert run.sent_steps == 1


@pytest.mark.asyncio
async def test_message_edit_does_not_switch_author_during_initial_send(app: FastAPI) -> None:
    gateway = BlockingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "message-edit-race-first")
    second = await _account(app, "message-edit-race-second")
    rule = await service.create_rule(
        name="Гонка Message Edit",
        channel_ref="@message_edit_race",
        mode=ChannelCommentMode.MESSAGE_EDIT,
        primary_account_id=first.id,
        primary_text="Финальный текст",
        initial_delay_seconds=0,
        message_edit_initial_text="Ага",
        message_edit_delay_seconds=5 * 60,
    )
    gateway.posts = (ChannelPost(message_id=27, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    processing = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    with pytest.raises(ConflictError, match="ожидается редактирование"):
        await service.replace_primary_account(rule.id, second.id)
    gateway.release.set()
    assert await processing is True

    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        assert run is not None
        assert run.status == ChannelCommentRunStatus.QUEUED
        assert run.last_comment_message_id == 40_001
        run.scheduled_for = utc_now() - timedelta(seconds=1)
        await session.commit()

    assert await service.process_next() is True
    assert gateway.edits[0]["account"].id == first.id


@pytest.mark.asyncio
async def test_comments_unavailable_are_recorded_as_skipped(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    gateway.delivery_error = PermanentGatewayError(
        "comments are disabled",
        code="comments_unavailable",
    )
    service = _service(app, gateway)
    account = await _account(app, "unavailable-sender")
    rule = await service.create_rule(
        name="Без комментариев",
        channel_ref="@no_comments_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Комментарий",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=30, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
    assert run is not None
    assert run.status == ChannelCommentRunStatus.SKIPPED
    assert run.error_code == "comments_unavailable"


@pytest.mark.asyncio
async def test_account_session_failure_disables_comment_sender(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    gateway.delivery_error = PermanentGatewayError(
        "session is no longer authorized",
        code="account_unauthorized",
    )
    service = _service(app, gateway)
    account = await _account(app, "expired-commenter")
    rule = await service.create_rule(
        name="Сессия комментария",
        channel_ref="@expired_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Комментарий",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=31, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        stored_account = await session.get(Account, account.id)
    assert run is not None and run.status == ChannelCommentRunStatus.FAILED
    assert run.attempt_count == 1
    assert stored_account is not None and stored_account.status.value == "disabled"


@pytest.mark.asyncio
async def test_chat_ban_auto_pauses_rule_and_cancels_its_remaining_queue(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "banned-commenter")
    rule = await service.create_rule(
        name="Ban-aware comments",
        channel_ref="@banned_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    gateway.posts = (
        ChannelPost(message_id=33, published_at=utc_now()),
        ChannelPost(message_id=34, published_at=utc_now()),
    )
    await service.scan_rule(rule.id)
    gateway.delivery_error = PermanentGatewayError(
        "account was banned in discussion",
        code="account_banned_in_chat",
    )

    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        stored_rule = await session.get(ChannelCommentRule, rule.id)
        runs = list(
            await session.scalars(
                select(ChannelCommentRun).order_by(ChannelCommentRun.source_post_id)
            )
        )
    assert stored_rule is not None and stored_rule.enabled is False
    assert stored_rule.last_error_code == "account_banned_in_chat"
    assert [run.status for run in runs] == [
        ChannelCommentRunStatus.FAILED,
        ChannelCommentRunStatus.CANCELLED,
    ]
    assert runs[0].error_code == "account_banned_in_chat"
    assert runs[1].error_code == "rule_auto_paused"

    gateway.preflight_error = PermanentGatewayError(
        "account is still banned in discussion",
        code="account_banned_in_chat",
    )
    with pytest.raises(ConflictError):
        await service.set_rule_enabled(rule.id, True)
    async with app.state.database.session_factory() as session:
        stored_rule = await session.get(ChannelCommentRule, rule.id)
    assert stored_rule is not None and stored_rule.enabled is False
    assert stored_rule.last_error_code == "account_banned_in_chat"


@pytest.mark.asyncio
async def test_deleted_comment_is_skipped_and_counts_account_workload(
    app: FastAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("WARNING", logger="proxima_traffic.channel_comments")
    gateway = RecordingChannelGateway()
    gateway.delivery_error = PermanentGatewayError(
        "comment removed by moderation",
        code="post_deleted_by_moderation",
        external_message_id=41_001,
    )
    service = _service(app, gateway)
    account = await _account(app, "moderated-commenter")
    rule = await service.create_rule(
        name="Модерация комментария",
        channel_ref="@moderated_comment_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Комментарий",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=32, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    assert await service.process_next() is True
    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
        stored_account = await session.get(Account, account.id)
    assert run is not None and run.status == ChannelCommentRunStatus.SKIPPED
    assert run.error_code == "post_deleted_by_moderation"
    assert stored_account is not None and stored_account.sent_today == 1
    warning = next(
        record for record in caplog.records if "post_deleted_by_moderation" in record.getMessage()
    )
    assert warning.exc_info is None


@pytest.mark.asyncio
async def test_stopping_rule_during_send_does_not_schedule_next_dialogue_step(
    app: FastAPI,
) -> None:
    gateway = BlockingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "race-first")
    second = await _account(app, "race-second")
    rule = await service.create_rule(
        name="Остановка во время отправки",
        channel_ref="@race_channel",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=first.id,
        primary_text="Первый",
        initial_delay_seconds=0,
        dialogue_steps=[
            DialogueStep(first.id, "Первый", 0, False),
            DialogueStep(second.id, "Второй", 30, True),
        ],
    )
    gateway.posts = (ChannelPost(message_id=40, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    processing = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    await service.set_rule_enabled(rule.id, False)
    gateway.release.set()
    assert await processing is True

    async with app.state.database.session_factory() as session:
        run = await session.scalar(select(ChannelCommentRun))
    assert run is not None
    assert run.status == ChannelCommentRunStatus.CANCELLED
    assert run.sent_steps == 1


@pytest.mark.asyncio
async def test_replacing_primary_account_cancels_old_queue_and_rearms_rule(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "replace-first")
    second = await _account(app, "replace-second")
    rule = await service.create_rule(
        name="Замена аккаунта",
        channel_ref="@replace_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=first.id,
        primary_text="Первый комментарий",
        initial_delay_seconds=60,
    )
    gateway.posts = (ChannelPost(message_id=50, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    replaced = await service.replace_primary_account(rule.id, second.id)
    runs = await service.recent_runs(rule.id)

    assert replaced.primary_account_id == second.id
    assert runs[0].status == ChannelCommentRunStatus.CANCELLED
    assert runs[0].error_code == "primary_account_replaced"
    assert replaced.last_error is None


@pytest.mark.asyncio
async def test_replacing_dialogue_primary_rejects_consecutive_same_account(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "dialogue-replace-first")
    second = await _account(app, "dialogue-replace-second")
    rule = await service.create_rule(
        name="Конфликт цепочки",
        channel_ref="@replace_dialogue_channel",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=first.id,
        primary_text="Первое",
        initial_delay_seconds=0,
        dialogue_steps=[
            DialogueStep(first.id, "Первое", 0, False),
            DialogueStep(second.id, "Второе", 30, True),
        ],
    )

    with pytest.raises(ConflictError, match="конфликтует"):
        await service.replace_primary_account(rule.id, second.id)

    current = await service.get_rule_view(rule.id)
    assert current.rule.primary_account_id == first.id


@pytest.mark.asyncio
async def test_deleting_rule_during_send_archives_history_and_records_delivery(
    app: FastAPI,
) -> None:
    gateway = BlockingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "archive-race-sender")
    rule = await service.create_rule(
        name="Archive race",
        channel_ref="@archive_race",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=61, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    processing = asyncio.create_task(service.process_next())
    await asyncio.wait_for(gateway.started.wait(), timeout=1)
    await service.delete_rule(rule.id)
    gateway.release.set()
    assert await processing is True

    with pytest.raises(NotFoundError):
        await service.get_rule_view(rule.id)
    async with app.state.database.session_factory() as session:
        stored_rule = await session.get(ChannelCommentRule, rule.id)
        run = await session.scalar(
            select(ChannelCommentRun).where(ChannelCommentRun.rule_id == rule.id)
        )
    assert stored_rule is not None and stored_rule.archived_at is not None
    assert run is not None
    assert run.status == ChannelCommentRunStatus.CANCELLED
    assert run.sent_steps == 1
    assert run.error_code == "completed_during_cancel"

    recreated = await service.create_rule(
        name="Archive race",
        channel_ref="@archive_race",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="New comment",
        initial_delay_seconds=0,
    )
    assert recreated.id != rule.id


@pytest.mark.asyncio
async def test_network_errors_stop_at_the_configured_attempt_limit(app: FastAPI) -> None:
    app.state.settings.worker_max_attempts = 3
    gateway = RecordingChannelGateway()
    gateway.delivery_error = TemporaryGatewayError(
        "network down",
        code="network_error",
        retry_after=1,
    )
    service = _service(app, gateway)
    account = await _account(app, "bounded-retry-sender")
    rule = await service.create_rule(
        name="Bounded retry",
        channel_ref="@bounded_retry",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    gateway.posts = (ChannelPost(message_id=62, published_at=utc_now()),)
    await service.scan_rule(rule.id)

    for _ in range(3):
        assert await service.process_next() is True
        async with app.state.database.session_factory() as session:
            run = await session.scalar(
                select(ChannelCommentRun).where(ChannelCommentRun.rule_id == rule.id)
            )
            assert run is not None
            if run.status == ChannelCommentRunStatus.QUEUED:
                run.scheduled_for = utc_now() - timedelta(seconds=1)
                await session.commit()

    run = (await service.recent_runs(rule.id))[0]
    assert run.status == ChannelCommentRunStatus.FAILED
    assert run.attempt_count == 3
    assert run.error_code == "network_error"


@pytest.mark.asyncio
async def test_hidden_mentions_rotate_and_respect_rule_cooldown(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    account = await _account(app, "rotating-mention-sender")
    rule = await service.create_rule(
        name="Mention rotation",
        channel_ref="@mention_rotation",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    await service.set_rule_mention_cooldown_days(rule.id, 30)

    gateway.posts = (ChannelPost(message_id=63, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True
    first_run = (await service.recent_runs(rule.id))[0]
    first_mentions = tuple(json.loads(first_run.hidden_mention_user_ids_snapshot_json or "[]"))
    assert len(first_mentions) in range(5, 11)

    gateway.posts = (ChannelPost(message_id=64, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True
    second_run = (await service.recent_runs(rule.id))[0]
    assert second_run.source_post_id == 64
    assert second_run.hidden_mention_resolved_count == 0
    assert second_run.hidden_mention_skip_reason == "mention_cooldown_active"
    assert gateway.calls[-1]["html_text"] is None

    await service.set_rule_mention_cooldown_days(rule.id, 1)
    async with app.state.database.session_factory() as session:
        states = list(
            await session.scalars(
                select(ChannelCommentMentionState).where(
                    ChannelCommentMentionState.rule_id == rule.id
                )
            )
        )
        for state in states:
            state.last_mentioned_at = utc_now() - timedelta(days=2)
        stored_account = await session.get(Account, account.id)
        assert stored_account is not None
        stored_account.next_send_at = None
        stored_account.cooldown_until = None
        stored_account.sent_since_break = 0
        await session.commit()
    gateway.posts = (ChannelPost(message_id=65, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True
    third_run = (await service.recent_runs(rule.id))[0]
    assert third_run.hidden_mention_resolved_count in range(5, 11), (
        third_run.status,
        third_run.attempt_count,
        third_run.error_code,
        third_run.error,
        third_run.scheduled_for,
        third_run.expires_at,
    )
    assert gateway.calls[-1]["html_text"] is not None


@pytest.mark.asyncio
async def test_failed_comment_releases_mention_rotation_reservations(app: FastAPI) -> None:
    gateway = RecordingChannelGateway()
    gateway.delivery_error = PermanentGatewayError("rejected", code="request_invalid")
    service = _service(app, gateway)
    account = await _account(app, "mention-reservation-release")
    rule = await service.create_rule(
        name="Mention reservation release",
        channel_ref="@mention_reservation_release",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=account.id,
        primary_text="Comment",
        initial_delay_seconds=0,
    )
    await service.set_rule_hidden_mentions_enabled(rule.id, True)
    gateway.posts = (ChannelPost(message_id=66, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True

    async with app.state.database.session_factory() as session:
        states = list(
            await session.scalars(
                select(ChannelCommentMentionState).where(
                    ChannelCommentMentionState.rule_id == rule.id
                )
            )
        )
    assert states
    assert all(state.last_mentioned_at is None for state in states)
    assert all(state.reserved_run_id is None for state in states)

    gateway.delivery_error = None
    gateway.posts = (ChannelPost(message_id=67, published_at=utc_now()),)
    await service.scan_rule(rule.id)
    assert await service.process_next() is True
    assert gateway.calls[-1]["html_text"] is not None


@pytest.mark.asyncio
async def test_rule_creation_runs_comment_preflight_for_every_dialogue_account(
    app: FastAPI,
) -> None:
    gateway = RecordingChannelGateway()
    service = _service(app, gateway)
    first = await _account(app, "preflight-first")
    second = await _account(app, "preflight-second")
    rule = await service.create_rule(
        name="Dialogue preflight",
        channel_ref="@dialogue_preflight",
        mode=ChannelCommentMode.DIALOGUE,
        primary_account_id=first.id,
        primary_text="First",
        initial_delay_seconds=0,
        dialogue_steps=[
            DialogueStep(first.id, "First", 0, False),
            DialogueStep(second.id, "Second", 30, True),
        ],
    )
    assert gateway.preflight_calls == [
        (first.id, "@dialogue_preflight"),
        (second.id, "@dialogue_preflight"),
    ]
    assert rule.channel_title == "Тестовый канал"

    gateway.preflight_error = PermanentGatewayError(
        "comments disabled",
        code="comments_unavailable",
    )
    with pytest.raises(ConflictError, match="не прошёл проверку"):
        await service.create_rule(
            name="Rejected preflight",
            channel_ref="@rejected_preflight",
            mode=ChannelCommentMode.SINGLE,
            primary_account_id=first.id,
            primary_text="Comment",
            initial_delay_seconds=0,
        )
