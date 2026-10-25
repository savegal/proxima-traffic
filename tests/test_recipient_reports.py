import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select, text

from proxima_traffic.bot import handlers
from proxima_traffic.bot.recipient_report_ui import recipient_report_menu, recipient_report_text
from proxima_traffic.bot.subscriber_invite_ui import subscriber_invites_home_menu
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.models import (
    Account,
    DirectCampaign,
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTask,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteCampaign,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
    SubscriberList,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.recipient_reports import (
    RecipientReportFilter,
    RecipientReportKind,
    normalize_recipient_query,
)
from proxima_traffic.services import ConflictError, NotFoundError
from proxima_traffic.subscriber_invites import SubscriberInviteService


@pytest.fixture
async def reports(app):
    database = app.state.database
    direct = DirectMessageService(
        database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="report-tests",
    )
    invites = SubscriberInviteService(
        database.session_factory,
        app.state.gateway,
        app.state.campaign_runner,
        app.state.settings,
        worker_id="report-tests",
    )
    async with database.session_factory() as session:
        source = SubscriberList(name="Report audience")
        sender = Account(label="Historical sender", ownership_confirmed=True)
        people = [
            Subscriber(telegram_user_id=750000 + i, username=f"member_{i}") for i in range(16)
        ]
        people[15].username = None
        people[7].direct_quarantine_code = "recipient_unavailable"
        people[6].do_not_contact = True
        session.add_all([source, sender, *people])
        await session.flush()
        campaign = DirectCampaign(
            name="Messages", subscriber_list_id=source.id, primary_text="Post"
        )
        invite_campaign = SubscriberInviteCampaign(
            name="Invites",
            subscriber_list_id=source.id,
            destination_ref="@owned_group",
            destination_key="owned_group",
        )
        session.add_all([campaign, invite_campaign])
        await session.flush()
        common = {
            "subscriber_list_id": source.id,
            "segment_snapshot": SubscriberSegment.ALL,
            "recipient_limit_snapshot": 20,
            "contact_cooldown_days_snapshot": 0,
            "account_ids_snapshot_json": json.dumps([sender.id]),
            "consent_confirmed_at": utc_now(),
            "status": DirectLaunchStatus.SUCCEEDED,
        }
        launches = [
            DirectLaunch(
                campaign_id=campaign.id,
                primary_text_snapshot="Post",
                simulated_snapshot=mode,
                **common,
            )
            for mode in (False, False, True, None)
        ]
        invite_launches = [
            SubscriberInviteLaunch(
                campaign_id=invite_campaign.id,
                destination_ref_snapshot="@owned_group",
                destination_key_snapshot="owned_group",
                simulated_snapshot=mode,
                **common,
            )
            for mode in (False, True, None)
        ]
        session.add_all(launches + invite_launches)
        await session.flush()
        statuses = [DirectMessageTaskStatus.SUCCEEDED] * 12 + [
            DirectMessageTaskStatus.FAILED,
            DirectMessageTaskStatus.SKIPPED,
            DirectMessageTaskStatus.CANCELLED,
            DirectMessageTaskStatus.QUEUED,
        ]
        for person, status in zip(people, statuses, strict=True):
            session.add(
                DirectMessageTask(
                    launch_id=launches[0].id,
                    subscriber_id=person.id,
                    sender_account_id=sender.id,
                    status=status,
                    external_message_id=person.id
                    if status == DirectMessageTaskStatus.SUCCEEDED
                    else None,
                    finished_at=utc_now(),
                    error_code="delivery_unconfirmed"
                    if status == DirectMessageTaskStatus.FAILED
                    else None,
                )
            )
        for launch, indices in ((launches[1], [0]), (launches[2], [2, 3]), (launches[3], [1])):
            for index in indices:
                session.add(
                    DirectMessageTask(
                        launch_id=launch.id,
                        subscriber_id=people[index].id,
                        sender_account_id=sender.id,
                        status=DirectMessageTaskStatus.SUCCEEDED,
                        external_message_id=100 + index,
                        finished_at=utc_now(),
                    )
                )
        for index, (status, already) in enumerate(
            (
                (DirectMessageTaskStatus.SUCCEEDED, False),
                (DirectMessageTaskStatus.SUCCEEDED, True),
                (DirectMessageTaskStatus.FAILED, False),
                (DirectMessageTaskStatus.SKIPPED, False),
            )
        ):
            session.add(
                SubscriberInviteTask(
                    launch_id=invite_launches[0].id,
                    subscriber_id=people[index].id,
                    sender_account_id=sender.id,
                    status=status,
                    already_participant=already,
                    finished_at=utc_now(),
                    error_code="invite_already_participant" if already else None,
                )
            )
        for launch, index in ((invite_launches[1], 4), (invite_launches[2], 5)):
            session.add(
                SubscriberInviteTask(
                    launch_id=launch.id,
                    subscriber_id=people[index].id,
                    sender_account_id=sender.id,
                    status=DirectMessageTaskStatus.SUCCEEDED,
                    finished_at=utc_now(),
                )
            )
        await session.commit()
    return SimpleNamespace(
        database=database,
        direct=direct,
        invites=invites,
        launches=launches,
        invite_launches=invite_launches,
        people=people,
        sender=sender,
        gateway=app.state.gateway,
    )


async def test_success_list_pages_only_confirmed_live_deliveries_without_repeats(reports):
    first = await reports.direct.list_recipients(launch_id=reports.launches[0].id)
    second = await reports.direct.list_recipients(launch_id=reports.launches[0].id, page=1)
    assert first.total == second.total == 12
    assert (len(first.items), len(second.items)) == (10, 2)
    assert len({item.telegram_user_id for item in first.items + second.items}) == 12
    assert all(
        item.simulated is False and item.status == DirectMessageTaskStatus.SUCCEEDED
        for item in first.items + second.items
    )
    past_end = await reports.direct.list_recipients(launch_id=reports.launches[0].id, page=500)
    assert past_end.page == 1
    assert past_end.items == second.items


async def test_global_results_and_id_search_retain_each_launch_result(reports):
    overall = await reports.direct.list_recipients()
    assert overall.total == 13
    user_id = str(reports.people[0].telegram_user_id)
    found = await reports.direct.list_recipients(query=user_id)
    assert found.total == 2
    assert {item.launch_id for item in found.items} == {
        launch.id for launch in reports.launches[:2]
    }
    scoped = await reports.direct.list_recipients(launch_id=reports.launches[0].id, query=user_id)
    assert scoped.total == 1


@pytest.mark.parametrize(
    "query", ["@MEMBER_0", "member_0", "https://t.me/member_0", "t.me/member_0"]
)
async def test_username_search_is_exact_case_insensitive_and_scoped(reports, query):
    direct = await reports.direct.list_recipients(query=query)
    invites = await reports.invites.list_recipients(query=query)
    assert direct.total == 2
    assert invites.total == 1
    assert direct.query == invites.query == "@member_0"
    assert all(item.telegram_user_id == reports.people[0].telegram_user_id for item in direct.items)
    assert (await reports.direct.list_recipients(query="@member")).total == 0


async def test_invitation_results_separate_new_invites_existing_members_and_simulation(reports):
    invited = await reports.invites.list_recipients(launch_id=reports.invite_launches[0].id)
    already = await reports.invites.list_recipients(
        launch_id=reports.invite_launches[0].id,
        result_filter=RecipientReportFilter.ALREADY_PARTICIPANT,
    )
    simulated = await reports.invites.list_recipients(result_filter=RecipientReportFilter.SIMULATED)
    assert invited.total == already.total == simulated.total == 1
    assert invited.items[0].already_participant is False
    assert already.items[0].already_participant is True
    assert simulated.items[0].simulated is True
    assert "уже состоял" in recipient_report_text(already)
    assert "симуляция" in recipient_report_text(simulated)


async def test_legacy_and_uncertain_outcomes_are_available_without_false_success_claims(reports):
    unknown = await reports.direct.list_recipients(
        launch_id=reports.launches[3].id,
        result_filter=RecipientReportFilter.ALL,
    )
    assert unknown.total == 1
    assert "режим запуска не сохранён" in recipient_report_text(unknown)
    assert "ЛС отправлено и подтверждено" not in recipient_report_text(unknown)
    assert (await reports.direct.list_recipients(launch_id=reports.launches[3].id)).total == 0
    uncertain = await reports.direct.list_recipients(
        launch_id=reports.launches[0].id,
        query=str(reports.people[12].telegram_user_id),
        result_filter=RecipientReportFilter.ALL,
    )
    assert uncertain.total == 1
    assert uncertain.items[0].error_code == "delivery_unconfirmed"
    assert (
        await reports.direct.list_recipients(query=str(reports.people[12].telegram_user_id))
    ).total == 0


async def test_directory_search_includes_people_without_launches_and_preserves_contact_controls(
    reports,
):
    directory = await reports.direct.list_subscriber_directory()
    assert directory.total == 16
    by_id = await reports.direct.list_subscriber_directory(
        query=str(reports.people[15].telegram_user_id)
    )
    assert by_id.total == 1 and by_id.items[0].username is None
    assert (
        (await reports.direct.list_subscriber_directory(query="@member_6")).items[0].do_not_contact
    )
    quarantine = await reports.direct.list_subscriber_directory(query="@member_7")
    assert quarantine.items[0].quarantine_code == "recipient_unavailable"
    assert "Карантин ЛС" in recipient_report_text(quarantine)
    menu = recipient_report_menu(quarantine, back_page=0)
    assert any(
        button.callback_data.startswith("rr:h:") for row in menu.inline_keyboard for button in row
    )
    async with reports.database.session_factory() as session:
        session.add(Subscriber(telegram_user_id=990001, username="not_launched_yet"))
        await session.commit()
    assert (await reports.direct.list_subscriber_directory(query="@not_launched_yet")).total == 1
    assert (
        await reports.direct.list_recipients(
            query="@not_launched_yet", result_filter=RecipientReportFilter.ALL
        )
    ).total == 0


async def test_reports_keep_results_if_the_historical_sender_was_deleted(reports):
    async with reports.database.session_factory() as session:
        await session.delete(await session.get(Account, reports.sender.id))
        await session.commit()
    report = await reports.direct.list_recipients(launch_id=reports.launches[0].id)
    assert report.total == 12
    assert all(item.sender_account_id is None for item in report.items)
    assert "отправитель не сохранён" in recipient_report_text(report)


async def test_missing_launch_and_empty_search_do_not_leak_other_launches(reports):
    with pytest.raises(NotFoundError):
        await reports.direct.list_recipients(launch_id=999999)
    empty = await reports.invites.list_recipients(query="@missing_person", page=999)
    assert empty.total == 0 and empty.page == 0 and empty.pages == 1
    assert "Совпадений нет" in recipient_report_text(empty)
    with pytest.raises(ConflictError):
        await reports.direct.list_recipients(
            result_filter=RecipientReportFilter.ALREADY_PARTICIPANT
        )


@pytest.mark.parametrize(
    "value", ["", "0", "-1", "9" * 100, "@bad.name", "%_", "https://bad.org/person"]
)
def test_search_rejects_invalid_identity_and_wildcard_inputs(value):
    with pytest.raises(ConflictError):
        normalize_recipient_query(value)


def test_numeric_username_can_be_searched_explicitly():
    assert normalize_recipient_query("@1234") == "@1234"
    assert normalize_recipient_query("0001234") == "1234"
    assert normalize_recipient_query("HTTPS://T.ME/MEMBER_0") == "@member_0"


async def test_reports_only_read_saved_results_without_resending_or_changing_tasks(reports):
    reports.gateway.send_direct_content = AsyncMock()
    reports.gateway.invite_subscriber = AsyncMock()
    async with reports.database.session_factory() as session:
        before = (
            await session.execute(
                select(
                    DirectMessageTask.id, DirectMessageTask.status, DirectMessageTask.attempt_count
                ).order_by(DirectMessageTask.id)
            )
        ).all()
    await reports.direct.list_recipients()
    await reports.invites.list_recipients()
    await reports.direct.list_subscriber_directory()
    reports.gateway.send_direct_content.assert_not_awaited()
    reports.gateway.invite_subscriber.assert_not_awaited()
    async with reports.database.session_factory() as session:
        after = (
            await session.execute(
                select(
                    DirectMessageTask.id, DirectMessageTask.status, DirectMessageTask.attempt_count
                ).order_by(DirectMessageTask.id)
            )
        ).all()
    assert before == after


async def test_large_page_controls_fit_telegram_and_keep_search_token(reports):
    report = await reports.direct.list_recipients()
    large = replace(report, launch_id=(1 << 63) - 1, total=1_000_000, page=10)
    menu = recipient_report_menu(large, back_page=1234, token="abcd1234")
    callbacks = [button.callback_data for row in menu.inline_keyboard for button in row]
    assert all(len(callback.encode()) <= 64 for callback in callbacks)
    assert f"rr:v:d:{large.launch_id}:s:20:1234:abcd1234" in callbacks
    assert f"rr:v:d:{large.launch_id}:s:99999:1234:abcd1234" in callbacks
    directory = replace(report, kind=RecipientReportKind.DIRECTORY, launch_id=None)
    assert "Людей" in recipient_report_text(directory)


async def test_recipient_queries_are_bounded_and_use_history_indexes(reports):
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(reports.database.engine.sync_engine, "before_cursor_execute", capture)
    try:
        await reports.direct.list_recipients(launch_id=reports.launches[0].id)
    finally:
        event.remove(reports.database.engine.sync_engine, "before_cursor_execute", capture)
    assert len(statements) == 3
    async with reports.database.session_factory() as session:
        for table in ("direct_message_tasks", "subscriber_invite_tasks"):
            plan = await session.execute(
                text(
                    f"EXPLAIN QUERY PLAN SELECT id FROM {table} "
                    "WHERE subscriber_id=1 AND status='SUCCEEDED' ORDER BY id DESC LIMIT 10"
                )
            )
            assert f"ix_{table}_recipient" in " ".join(str(row) for row in plan)


class State:
    def __init__(self):
        self.data = {}
        self.state = None

    async def get_data(self):
        return json.loads(json.dumps(self.data))

    async def update_data(self, **data):
        self.data.update(data)

    async def set_state(self, value):
        self.state = value


async def test_directory_pagination_has_direct_jump_and_controls_at_both_ends(reports):
    report = await reports.direct.list_subscriber_directory()
    menu = recipient_report_menu(report, back_page=1, token="abcd1234")
    first = menu.inline_keyboard[0]
    assert [button.text for button in first] == ["1 / 2 ▾", "Следующая ›"]
    assert first[0].callback_data == "rr:g:b:0:a:0:1:abcd1234"
    assert (
        sum(
            button.callback_data == "rr:g:b:0:a:0:1:abcd1234"
            for row in menu.inline_keyboard
            for button in row
        )
        == 2
    )
    second = await reports.direct.list_subscriber_directory(page=1)
    assert "Показаны: <b>11–16</b> из <b>16</b>" in recipient_report_text(second)
    nav = recipient_report_menu(second, back_page=1).inline_keyboard[0]
    assert [button.text for button in nav] == ["‹ Предыдущая", "2 / 2 ▾"]


async def test_directory_single_page_has_no_useless_page_prompt(reports):
    report = await reports.direct.list_subscriber_directory(query="@member_0")
    menu = recipient_report_menu(report, back_page=0)
    assert menu.inline_keyboard[0][0].text == "1 / 1"
    assert menu.inline_keyboard[0][0].callback_data == "noop"
    assert all(
        not button.callback_data.startswith("rr:g:")
        for row in menu.inline_keyboard
        for button in row
    )


async def test_page_jump_opens_requested_page_and_preserves_directory_origin(reports, monkeypatch):
    state = State()
    callback = SimpleNamespace(data="rr:g:b:0:a:0:1:0", message=None)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit, origin = AsyncMock(), AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    await handlers.begin_recipient_report_page_jump(
        callback, state, reports.direct, reports.invites
    )
    assert state.state == handlers.RecipientReportPageState.page
    assert "до <b>2</b>" in edit.call_args.args[1]
    assert edit.call_args.args[2].inline_keyboard[0][0].callback_data == "rr:v:b:0:a:0:1:0"
    message = SimpleNamespace(text="2", answer=AsyncMock())
    await handlers.jump_to_recipient_report_page(
        message, state, object(), reports.direct, reports.invites
    )
    message.answer.assert_not_awaited()
    assert state.state is None
    assert "11–16" in origin.call_args.args[2]
    callbacks = {
        button.callback_data for row in origin.call_args.args[3].inline_keyboard for button in row
    }
    assert "inv:home" in callbacks
    assert "rr:v:b:0:a:0:1:0" in callbacks


@pytest.mark.parametrize("number", ["0", "-1", "abc", "999999999999999999999", "3", "1,5", "1.5"])
async def test_invalid_page_jump_keeps_prompt_and_does_not_overwrite_list(
    reports, monkeypatch, number
):
    state = State()
    state.data["recipient_report_page_jump"] = {
        "kind": "b",
        "launch_id": None,
        "filter": "a",
        "page": 0,
        "back_page": 0,
        "token": "0",
    }
    state.state = handlers.RecipientReportPageState.page
    origin = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    message = SimpleNamespace(text=number, answer=AsyncMock())
    await handlers.jump_to_recipient_report_page(
        message, state, object(), reports.direct, reports.invites
    )
    message.answer.assert_awaited_once()
    origin.assert_not_awaited()
    assert state.state == handlers.RecipientReportPageState.page


async def test_page_jump_preserves_search_filter_and_history_return(reports, monkeypatch):
    state = State()
    source = {"kind": "b", "launch_id": None}
    token = await handlers._save_recipient_report_query(state, source, "@member_0")
    context = {"kind": "d", "launch_id": None, "filter": "a", "page": 0, "back_page": -1}
    history_token = await handlers._save_recipient_report_query(
        state,
        context,
        str(reports.people[0].telegram_user_id),
        directory_return={"page": 0, "back_page": 1, "token": token},
    )
    callback = SimpleNamespace(data=f"rr:g:d:0:a:0:-1:{history_token}", message=None)
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_callback", AsyncMock())
    origin = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    await handlers.begin_recipient_report_page_jump(
        callback, state, reports.direct, reports.invites
    )
    message = SimpleNamespace(text="1", answer=AsyncMock())
    await handlers.jump_to_recipient_report_page(
        message, state, object(), reports.direct, reports.invites
    )
    callbacks = {
        button.callback_data for row in origin.call_args.args[3].inline_keyboard for button in row
    }
    assert f"rr:v:b:0:a:0:1:{token}" in callbacks
    assert f"rr:s:d:0:a:0:-1:{history_token}" in callbacks
    assert str(reports.people[0].telegram_user_id) in origin.call_args.args[2]


async def test_page_jump_rejects_expired_search_instead_of_opening_another_list(
    reports, monkeypatch
):
    state = State()
    state.data["recipient_report_page_jump"] = {
        "kind": "b",
        "launch_id": None,
        "filter": "a",
        "page": 0,
        "back_page": 0,
        "token": "missing",
    }
    message = SimpleNamespace(text="1", answer=AsyncMock())
    origin = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    await handlers.jump_to_recipient_report_page(
        message, state, object(), reports.direct, reports.invites
    )
    assert "Поиск устарел" in message.answer.call_args.args[0]
    assert state.state is None
    origin.assert_not_awaited()


async def test_search_pagination_retains_scope_and_reset_discards_only_the_filter(
    reports, monkeypatch
):
    state = State()
    callback = SimpleNamespace(
        data=f"rr:s:d:{reports.launches[0].id}:s:0:3:0",
        message=None,
    )
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    origin = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    await handlers.begin_recipient_report_search(callback, state)
    message = SimpleNamespace(text="@MEMBER_0", answer=AsyncMock())
    await handlers.search_recipient_report(
        message, state, object(), reports.direct, reports.invites
    )
    message.answer.assert_not_awaited()
    assert "@member_0" in origin.call_args.args[2]
    token = next(iter(state.data["recipient_report_queries"]))
    callback.data = f"rr:v:d:{reports.launches[0].id}:a:99:3:{token}"
    await handlers.open_recipient_report(callback, state, reports.direct, reports.invites)
    assert "Записей: <b>1</b>" in edit.call_args.args[1]
    assert "@member_0" in edit.call_args.args[1]
    callbacks = [
        button.callback_data for row in edit.call_args.args[2].inline_keyboard for button in row
    ]
    assert f"dm:launchview:{reports.launches[0].id}:3" in callbacks
    callback.data = f"rr:v:d:{reports.launches[0].id}:s:0:3:0"
    await handlers.open_recipient_report(callback, state, reports.direct, reports.invites)
    assert "Записей: <b>12</b>" in edit.call_args.args[1]


async def test_old_search_token_never_uses_another_launch_query(reports, monkeypatch):
    state = State()
    await state.set_state("pending-report-search")
    context = {"kind": "d", "launch_id": reports.launches[0].id}
    token = await handlers._save_recipient_report_query(state, context, "@member_0")
    callback = SimpleNamespace(data=f"rr:v:d:{reports.launches[1].id}:s:0:0:{token}")
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.open_recipient_report(callback, state, reports.direct, reports.invites)
    assert "Поиск устарел" in edit.call_args.args[1]
    assert state.state is None


async def test_directory_links_open_identity_history_and_preserve_return_to_search(
    reports, monkeypatch
):
    state = State()
    token = await handlers._save_recipient_report_query(
        state, {"kind": "b", "launch_id": None}, "@member_0"
    )
    callback = SimpleNamespace(data=f"rr:h:d:{reports.people[0].telegram_user_id}:0:{token}")
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.open_recipient_launch_history(callback, state, reports.direct, reports.invites)
    assert "Записей: <b>2</b>" in edit.call_args.args[1]
    callbacks = [
        button.callback_data for row in edit.call_args.args[2].inline_keyboard for button in row
    ]
    assert f"rr:v:b:0:a:0:0:{token}" in callbacks


@pytest.mark.parametrize("back_page,home", [(0, "dm:home"), (1, "inv:home")])
async def test_directory_returns_to_the_section_that_opened_it(reports, back_page, home):
    report = await reports.direct.list_subscriber_directory()
    menu = recipient_report_menu(report, back_page=back_page)
    callbacks = [button.callback_data for row in menu.inline_keyboard for button in row]
    assert callbacks[-1] == home
    assert f"rr:h:i:{report.items[0].telegram_user_id}:0:{back_page}:0" in callbacks
    invite_callbacks = [
        button.callback_data
        for row in subscriber_invites_home_menu().inline_keyboard
        for button in row
    ]
    assert "rr:v:b:0:a:0:1:0" in invite_callbacks


async def test_two_recipient_histories_keep_their_own_return_search(reports, monkeypatch):
    state = State()
    source = {"kind": "b", "launch_id": None}
    first = await handlers._save_recipient_report_query(state, source, "@member_0")
    second = await handlers._save_recipient_report_query(state, source, "@member_1")
    edit, origin = AsyncMock(), AsyncMock()
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    monkeypatch.setattr(handlers, "_edit_origin", origin)
    callback = SimpleNamespace(
        data=f"rr:h:d:{reports.people[0].telegram_user_id}:0:0:{first}",
        message=None,
    )
    await handlers.open_recipient_launch_history(callback, state, reports.direct, reports.invites)
    history_token = next(reversed(state.data["recipient_report_queries"]))
    callback.data = f"rr:h:i:{reports.people[1].telegram_user_id}:0:1:{second}"
    await handlers.open_recipient_launch_history(callback, state, reports.direct, reports.invites)
    callback.data = f"rr:v:d:0:a:0:-1:{history_token}"
    await handlers.open_recipient_report(callback, state, reports.direct, reports.invites)
    callbacks = [
        button.callback_data for row in edit.call_args.args[2].inline_keyboard for button in row
    ]
    assert callbacks[-1] == f"rr:v:b:0:a:0:0:{first}"
    callback.data = f"rr:s:d:0:a:0:-1:{history_token}"
    await handlers.begin_recipient_report_search(callback, state)
    await handlers.search_recipient_report(
        SimpleNamespace(text="@member_0", answer=AsyncMock()),
        state,
        object(),
        reports.direct,
        reports.invites,
    )
    callbacks = [
        button.callback_data for row in origin.call_args.args[3].inline_keyboard for button in row
    ]
    assert callbacks[-1] == f"rr:v:b:0:a:0:0:{first}"


async def test_opening_history_does_not_expire_its_oldest_directory_search(reports, monkeypatch):
    state = State()
    source = {"kind": "b", "launch_id": None}
    first = await handlers._save_recipient_report_query(state, source, "@member_0")
    for _ in range(9):
        await handlers._save_recipient_report_query(state, source, "@member_1")
    assert len(state.data["recipient_report_queries"]) == 10
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    callback = SimpleNamespace(data=f"rr:h:i:{reports.people[0].telegram_user_id}:0:1:{first}")
    await handlers.open_recipient_launch_history(callback, state, reports.direct, reports.invites)
    assert first in state.data["recipient_report_queries"]
    assert len(state.data["recipient_report_queries"]) == 10
    callbacks = [
        button.callback_data for row in edit.call_args.args[2].inline_keyboard for button in row
    ]
    assert callbacks[-1] == f"rr:v:b:0:a:0:1:{first}"


@pytest.mark.parametrize(
    "kind,back_page,expected",
    [
        ("d", 0, "dm:new"),
        ("i", 0, "inv:new"),
        ("b", 1, "inv:new"),
    ],
)
async def test_expired_search_error_preserves_its_original_section(
    reports, monkeypatch, kind, back_page, expected
):
    state = State()
    callback = SimpleNamespace(data=f"rr:v:{kind}:0:a:0:{back_page}:missing")
    monkeypatch.setattr(handlers, "_safe_callback_answer", AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(handlers, "_edit_callback", edit)
    await handlers.open_recipient_report(callback, state, reports.direct, reports.invites)
    assert "Поиск устарел" in edit.call_args.args[1]
    callbacks = [
        button.callback_data for row in edit.call_args.args[2].inline_keyboard for button in row
    ]
    assert expected in callbacks
