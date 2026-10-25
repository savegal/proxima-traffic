"""Presentation changes must preserve routes, selections and user content."""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from proxima_traffic.bot.channel_comment_ui import channel_comment_details_text
from proxima_traffic.bot.keyboards import main_menu, order_engagement_menu
from proxima_traffic.bot.ui import polish_markup
from proxima_traffic.channel_comments import ChannelCommentRuleView
from proxima_traffic.models import ChannelCommentMode, ChannelCommentRule


def test_main_menu_exposes_every_workflow_without_decorative_buttons() -> None:
    menu = main_menu()
    callbacks = {button.callback_data for row in menu.inline_keyboard for button in row}
    assert {
        "order:new",
        "menu:runs:0",
        "menu:campaigns:0",
        "comments:home:0",
        "pr:home:0",
        "dm:home",
        "inv:home",
        "story:home",
        "story_view:home",
        "dm:lists:0",
        "audiences:home",
        "menu:accounts:0",
        "menu:proxies:0",
        "menu:templates:0",
        "menu:settings",
        "menu:guide",
        "menu:home",
    } == callbacks
    assert len(menu.inline_keyboard) == 9
    assert all(len(row) <= 2 for row in menu.inline_keyboard)
    assert menu.inline_keyboard[0][0].text == "＋ Новая рассылка в чаты"
    story_labels = {
        "story:home": "📖 Теггинг в историях",
        "story_view:home": "👀 Просмотр и реакции",
    }
    assert {
        button.callback_data: button.text
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data in story_labels
    } == story_labels
    assert not any(
        ord(character) >= 0x1F300
        for row in menu.inline_keyboard
        for button in row
        if button.callback_data not in story_labels
        for character in button.text
    )


def test_long_actions_get_space_and_keep_their_original_payloads() -> None:
    source = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✎ Изменить параметры выбранного аккаунта", callback_data="edit:7",
        ),
        InlineKeyboardButton(text="× Удалить аккаунт", callback_data="account:delete:ask:7"),
        InlineKeyboardButton(text="Документация по настройке доступа", url="https://example.com/help"),
    ]])
    before = source.model_dump()
    rendered = polish_markup(source)
    assert source.model_dump() == before
    buttons = [button for row in rendered.inline_keyboard for button in row]
    assert [button.model_dump(exclude={"style"}) for button in buttons] == [
        button.model_dump(exclude={"style"}) for button in source.inline_keyboard[0]
    ]
    assert all(len(row) == 1 for row in rendered.inline_keyboard)
    assert polish_markup(rendered) == rendered


def test_selection_symbols_do_not_replace_actual_telegram_reactions() -> None:
    menu = order_engagement_menu({
        "reactions_enabled": True,
        "reaction_count": 2,
        "reaction_emoji": "🔥",
    })
    labels = {
        button.callback_data: button.text for row in menu.inline_keyboard for button in row
    }
    assert labels["order:engagement:emoji:fire"] == "✓ 🔥"
    assert labels["order:engagement:emoji:like"] == "👍"
    assert labels["order:engagement:count:2"] == "✓ 2"
    assert labels["order:engagement:count:1"] == "1"


def test_pagination_and_operator_names_survive_keyboard_polishing() -> None:
    source = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="‹", callback_data="list:page:0"),
            InlineKeyboardButton(text="2/10", callback_data="noop"),
            InlineKeyboardButton(text="›", callback_data="list:page:2"),
        ],
        [InlineKeyboardButton(text="🔥 Alice & team", callback_data="account:7")],
    ])
    rendered = polish_markup(source)
    assert len(rendered.inline_keyboard[0]) == 3
    assert rendered.inline_keyboard[1][0].text == "🔥 Alice & team"


def test_uneven_actions_do_not_share_a_narrow_half_width_button() -> None:
    source = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Проверить выбранные аккаунты", callback_data="check"),
        InlineKeyboardButton(text="Назад", callback_data="back"),
    ]])
    rendered = polish_markup(source)
    assert [len(row) for row in rendered.inline_keyboard] == [1, 1]
    assert [row[0].callback_data for row in rendered.inline_keyboard] == ["check", "back"]
    assert polish_markup(rendered) == rendered


def test_named_actions_get_two_columns_while_numeric_choices_stay_compact() -> None:
    source = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=name)
         for name in ("Аккаунты", "Прокси", "Шаблоны")],
        [InlineKeyboardButton(text=str(count), callback_data=f"count:{count}")
         for count in range(1, 6)],
    ])
    rendered = polish_markup(source)
    assert [len(row) for row in rendered.inline_keyboard] == [2, 1, 5]
    assert polish_markup(rendered) == rendered


def test_user_message_and_title_are_preserved_and_html_escaped() -> None:
    rule = ChannelCommentRule(
        id=1,
        name="🔥 Team <one>",
        channel_ref="@team_channel",
        mode=ChannelCommentMode.SINGLE,
        primary_account_id=7,
        primary_text="🔥 Предложение <важное> & детали",
        enabled=True,
    )
    view = ChannelCommentRuleView(
        rule=rule, queued=0, running=0, succeeded=0, failed=0, skipped=0,
    )
    text = channel_comment_details_text(view, accounts={7: "🔥 Alice & team"})
    assert "🔥 Team &lt;one&gt;" in text
    assert "🔥 Предложение &lt;важное&gt; &amp; детали" in text
    assert "🔥 Alice &amp; team" in text


def test_readable_pagination_keeps_all_three_controls_on_one_row() -> None:
    source = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="‹ Предыдущая", callback_data="rr:v:b:0:a:10:0:0"),
                InlineKeyboardButton(text="12 / 100 ▾", callback_data="rr:g:b:0:a:11:0:0"),
                InlineKeyboardButton(text="Следующая ›", callback_data="rr:v:b:0:a:12:0:0"),
            ]
        ]
    )
    rendered = polish_markup(source)
    assert len(rendered.inline_keyboard) == 1
    assert [button.callback_data for button in rendered.inline_keyboard[0]] == [
        button.callback_data for button in source.inline_keyboard[0]
    ]
    assert polish_markup(rendered) == rendered


def test_navigation_detection_does_not_merge_named_actions() -> None:
    source = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="‹ Предыдущая", callback_data="view:first"),
                InlineKeyboardButton(text="Другие параметры", callback_data="edit:settings"),
                InlineKeyboardButton(text="Следующая ›", callback_data="view:last"),
            ]
        ]
    )
    rendered = polish_markup(source)
    assert [len(row) for row in rendered.inline_keyboard] == [2, 1]
    assert [button.callback_data for row in rendered.inline_keyboard for button in row] == [
        button.callback_data for button in source.inline_keyboard[0]
    ]
