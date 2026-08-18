from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from html import escape
from math import ceil

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from proxima_traffic.bot.ui import screen_title, section, themed_keyboard
from proxima_traffic.channel_comments import (
    ChannelCommentReactionSummary,
    ChannelCommentRuleView,
)
from proxima_traffic.dialogues import DialogueStep, parse_dialogue_steps
from proxima_traffic.failure_policy import (
    CHANNEL_COMMENT_AUTO_PAUSE_CODES,
    failure_hint,
    failure_label,
)
from proxima_traffic.models import (
    Account,
    ChannelCommentMode,
    ChannelCommentRule,
    ChannelCommentRun,
    ChannelCommentRunStatus,
)

# These are normal, expected skip outcomes.  Their stable error code already
# gives the operator the complete explanation, while the old raw diagnostic may
# be stored from a prior version (and can contain unrecoverable replacement
# characters).  Never expose that diagnostic in the report.
_RUN_CODES_WITHOUT_RAW_DETAILS = frozenset({"album_duplicate"})


def channel_comments_menu(
    views: list[ChannelCommentRuleView],
    *,
    page: int,
    total: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Настроить канал", callback_data="comments:add"))
    for view in views:
        rule = view.rule
        state = "🟢" if rule.enabled else "⏸"
        mode = {
            ChannelCommentMode.SINGLE: "комментарий",
            ChannelCommentMode.DIALOGUE: "цепочка",
            ChannelCommentMode.MESSAGE_EDIT: "с редактированием",
        }[rule.mode]
        builder.row(
            InlineKeyboardButton(
                text=f"{state} {rule.name} · {mode}",
                callback_data=f"comments:details:{rule.id}:{page}",
            )
        )
    pages = max(1, (total + page_size - 1) // page_size)
    if pages > 1:
        builder.row(
            InlineKeyboardButton(text="‹", callback_data=f"comments:home:{max(0, page - 1)}"),
            InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="noop"),
            InlineKeyboardButton(
                text="›", callback_data=f"comments:home:{min(pages - 1, page + 1)}"
            ),
        )
    builder.row(InlineKeyboardButton(text="🔄 Обновить", callback_data=f"comments:home:{page}"))
    builder.row(InlineKeyboardButton(text="🏠 Главная", callback_data="menu:home"))
    return builder.as_markup()


def channel_comment_details_menu(
    view: ChannelCommentRuleView,
    *,
    page: int,
) -> InlineKeyboardMarkup:
    rule = view.rule
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="⏸ Остановить" if rule.enabled else "▶️ Включить",
            callback_data=f"comments:toggle:{rule.id}:{page}",
        ),
        InlineKeyboardButton(text="🔍 Проверить", callback_data=f"comments:check:{rule.id}:{page}"),
    )
    builder.row(
        InlineKeyboardButton(
            text="👤 Аккаунт для комментария",
            callback_data=f"comments:account:replace:{rule.id}:{page}",
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text=(
                "🔁 Автозамена аккаунта · включена"
                if rule.auto_replace_primary_account
                else "🔁 Автозамена аккаунта · выключена"
            ),
            callback_data=f"comments:autoreplace:{rule.id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=(
                "🔔 Отметки участников · включены"
                if rule.hidden_mentions_enabled
                else "🔔 Отметки участников · выключены"
            ),
            callback_data=f"comments:mentions:toggle:{rule.id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=f"🔄 Не повторять отметку · {rule.mention_cooldown_days} дн.",
            callback_data=f"comments:mentions:cooldown:{rule.id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=(
                f"👍 Реакции на комментарий · {rule.reaction_count}"
                if rule.reactions_enabled and rule.reaction_count
                else "👍 Реакции на комментарий · ВЫКЛ"
            ),
            callback_data=f"comments:reactions:open:{rule.id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="🧾 Последние обработки", callback_data=f"comments:runs:{rule.id}:{page}"
        )
    )
    builder.row(
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"comments:delete:{rule.id}:{page}")
    )
    builder.row(
        InlineKeyboardButton(text="⬅️ К автокомментариям", callback_data=f"comments:home:{page}")
    )
    return builder.as_markup()


def channel_comment_reactions_menu(
    view: ChannelCommentRuleView,
    *,
    page: int,
) -> InlineKeyboardMarkup:
    rule = view.rule
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="⏸ Выключить" if rule.reactions_enabled else "▶️ Включить",
            callback_data=f"comments:reactions:toggle:{rule.id}:{page}",
        ),
        InlineKeyboardButton(
            text="👥 Аккаунты",
            callback_data=f"comments:reactions:accounts:{rule.id}:{page}:0",
        ),
    )
    builder.row(
        *(
            InlineKeyboardButton(
                text=f"{'✓ ' if emoji == rule.reaction_emoji else ''}{emoji}",
                callback_data=f"comments:reactions:emoji:{rule.id}:{page}:{key}",
            )
            for key, emoji in (("like", "👍"), ("fire", "🔥"), ("heart", "❤️"), ("bolt", "⚡"))
        )
    )
    for counts in ((1, 2, 3, 4, 5), (6, 7, 8, 9, 10)):
        builder.row(
            *(
                InlineKeyboardButton(
                    text=f"{'✓ ' if count == rule.reaction_count else ''}{count}",
                    callback_data=f"comments:reactions:count:{rule.id}:{page}:{count}",
                )
                for count in counts
            )
        )
    if rule.mode == ChannelCommentMode.DIALOGUE:
        step_count = len(_channel_comment_steps(rule))
        for first_step in range(1, step_count + 1, 4):
            builder.row(
                *(
                    InlineKeyboardButton(
                        text=f"{'✓ ' if step == rule.reaction_dialogue_step else ''}Шаг {step}",
                        callback_data=f"comments:reactions:step:{rule.id}:{page}:{step}",
                    )
                    for step in range(first_step, min(first_step + 4, step_count + 1))
                )
            )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К правилу",
            callback_data=f"comments:details:{rule.id}:{page}",
        )
    )
    return builder.as_markup()


def channel_comment_reactions_text(
    view: ChannelCommentRuleView,
    labels: dict[int, str],
) -> str:
    rule = view.rule
    selected = channel_comment_reaction_account_ids(view)
    account_labels = (
        ", ".join(
            escape(labels.get(account_id, f"#{account_id}")) for account_id in sorted(selected)
        )
        or "не выбраны"
    )
    state = "🟢 включены" if rule.reactions_enabled and rule.reaction_count else "⏸ выключены"
    steps = _channel_comment_steps(rule)
    selected_step = min(max(1, int(rule.reaction_dialogue_step)), len(steps))
    source_account_id = int(steps[selected_step - 1].account_id)
    maximum_reactors = len(selected - {source_account_id})
    lines = [
        screen_title(f"Реакции на комментарий · {escape(rule.name)}", state),
        "",
        section("Настройки"),
        f"Реакция: <b>{rule.reaction_emoji}</b> · количество: <b>{rule.reaction_count}</b>",
    ]
    if rule.mode == ChannelCommentMode.DIALOGUE:
        lines.append(f"Комментарий цепочки: <b>№ {rule.reaction_dialogue_step}</b>")
    lines.extend(
        [
            f"Аккаунты: {account_labels}\n",
            f"Максимум по выбранному набору: <b>{maximum_reactors}</b> реакций\n",
            section("Результаты"),
            f"🕒 В очереди: <b>{view.reaction_queued}</b> · "
            f"выполняется: <b>{view.reaction_running}</b>",
            f"✅ Готово: <b>{view.reaction_succeeded}</b> · "
            f"⏭ Пропущено: <b>{view.reaction_skipped}</b> · "
            f"❌ Ошибки: <b>{view.reaction_failed}</b> · "
            f"⏹ Отменено: <b>{view.reaction_cancelled}</b>\n",
            "<i>Автор комментария всегда исключается. Если выбранный аккаунт "
            "заблокирован в обсуждении или временно недоступен, сервис выберет другой "
            "свободный аккаунт из списка. Настройка действует только на новые "
            "комментарии.</i>",
        ]
    )
    if rule.reactions_enabled and rule.reaction_count > maximum_reactors:
        lines.insert(
            -1,
            "⚠️ <b>Реакций будет меньше настройки:</b> автор исключается, а один "
            "аккаунт ставит не больше одной реакции.",
        )
    return "\n".join(lines)


def channel_comment_reaction_account_picker(
    accounts: list[Account],
    selected: set[int],
    *,
    rule_id: int,
    page: int,
    account_page: int,
) -> InlineKeyboardMarkup:
    size = 8
    pages = max(1, (len(accounts) + size - 1) // size)
    active_page = max(0, min(account_page, pages - 1))
    builder = InlineKeyboardBuilder()
    for account in accounts[active_page * size : (active_page + 1) * size]:
        builder.row(
            InlineKeyboardButton(
                text=f"{'✅' if int(account.id) in selected else '▫️'} {account.label}",
                callback_data=(
                    f"comments:reactions:account:{rule_id}:{page}:{active_page}:{account.id}"
                ),
            )
        )
    if pages > 1:
        row: list[InlineKeyboardButton] = []
        if active_page:
            row.append(
                InlineKeyboardButton(
                    text="‹",
                    callback_data=(
                        f"comments:reactions:accounts:{rule_id}:{page}:{active_page - 1}"
                    ),
                )
            )
        row.append(InlineKeyboardButton(text=f"{active_page + 1}/{pages}", callback_data="noop"))
        if active_page + 1 < pages:
            row.append(
                InlineKeyboardButton(
                    text="›",
                    callback_data=(
                        f"comments:reactions:accounts:{rule_id}:{page}:{active_page + 1}"
                    ),
                )
            )
        builder.row(*row)
    builder.row(
        InlineKeyboardButton(
            text=f"✅ Готово · выбрано {len(selected)}",
            callback_data=f"comments:reactions:open:{rule_id}:{page}",
        )
    )
    return builder.as_markup()


def channel_comment_reaction_account_ids(view: ChannelCommentRuleView) -> set[int]:
    try:
        raw = json.loads(view.rule.reaction_account_ids_json or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    return {
        int(value)
        for value in raw
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }


def _channel_comment_steps(rule: ChannelCommentRule) -> list[DialogueStep]:
    if rule.mode != ChannelCommentMode.DIALOGUE:
        return [
            DialogueStep(
                account_id=int(rule.primary_account_id),
                text=str(rule.primary_text),
                delay_seconds=0,
                reply_to_previous=False,
                media_path=rule.primary_media_path,
                media_kind=rule.primary_media_kind,
                media_name=rule.primary_media_name,
                media_size=rule.primary_media_size,
            )
        ]
    return parse_dialogue_steps(
        rule.dialogue_steps_json,
        primary_account_id=int(rule.primary_account_id),
        primary_text=str(rule.primary_text),
        reply_account_id=None,
        reply_text="",
        reply_delay_seconds=0,
    )


def channel_comment_delete_menu(rule_id: int, *, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить", callback_data=f"comments:delete:confirm:{rule_id}:{page}"
                ),
                InlineKeyboardButton(
                    text="✖️ Отмена", callback_data=f"comments:details:{rule_id}:{page}"
                ),
            ]
        ]
    )


def channel_comment_mention_cooldown_menu(
    rule_id: int,
    *,
    page: int,
    current_days: int,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for values in ((1, 3, 7), (14, 30, 60), (90, 180, 365)):
        builder.row(
            *(
                InlineKeyboardButton(
                    text=f"{'✓ ' if days == current_days else ''}{days} дн.",
                    callback_data=f"comments:mentions:days:{rule_id}:{page}:{days}",
                )
                for days in values
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="✍️ Другое значение",
            callback_data=f"comments:mentions:custom:{rule_id}:{page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⬅️ К правилу",
            callback_data=f"comments:details:{rule_id}:{page}",
        )
    )
    return builder.as_markup()


def channel_comment_mode_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Один комментарий", callback_data="comments:draft:single"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🧩 Цепочка аккаунтов", callback_data="comments:draft:dialogue"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Комментарий с редактированием",
                    callback_data="comments:draft:message_edit",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К автокомментариям", callback_data="comments:home:0")],
        ]
    )


def channel_comment_account_picker(
    accounts: list[Account],
    *,
    callback_prefix: str,
    back_callback: str,
    page: int = 0,
    page_size: int = 8,
    pager_callback_prefix: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    # Account pools can be large.  Showing every account in one message makes the
    # first-comment wizard hard to use and can exceed Telegram's keyboard limit.
    # Callers that opt into a pager get a bounded, stable page; callers without a
    # pager retain the old behaviour for compatibility.
    if pager_callback_prefix:
        page_size = max(1, page_size)
        total_pages = max(1, ceil(len(accounts) / page_size))
        page = min(max(0, page), total_pages - 1)
        visible_accounts = accounts[page * page_size : (page + 1) * page_size]
    else:
        total_pages = 1
        page = 0
        visible_accounts = accounts

    for account in visible_accounts:
        builder.row(
            InlineKeyboardButton(
                text=f"👤 {account.label}", callback_data=f"{callback_prefix}:{account.id}"
            )
        )

    if pager_callback_prefix and total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(text="◀️", callback_data=f"{pager_callback_prefix}:{page - 1}")
            )
        navigation.append(
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop")
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(text="▶️", callback_data=f"{pager_callback_prefix}:{page + 1}")
            )
        builder.row(*navigation)
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback))
    return builder.as_markup()


def channel_comment_chain_menu(step_count: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ Добавить ответ", callback_data="comments:chain:add")]]
    if step_count >= 2:
        rows.append(
            [InlineKeyboardButton(text="✅ Цепочка готова", callback_data="comments:chain:finish")]
        )
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_comment_confirmation_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Включить отслеживание", callback_data="comments:create:confirm"
                )
            ],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="fsm:cancel")],
        ]
    )


def channel_comments_text(views: list[ChannelCommentRuleView], total: int) -> str:
    active = sum(1 for view in views if view.rule.enabled)
    intro = (
        screen_title(
            "Автокомментарии",
            "Комментарии только под новыми постами выбранных каналов.",
        )
        + "\n\n"
        f"{section('Состояние')}\n"
        f"Правил: <b>{total}</b> · включено на этой странице: <b>{active}</b>\n\n"
        f"{section('Как это работает')}\n"
        "01 — Настройте канал, аккаунт и текст.\n"
        "02 — Сервис замечает новые публикации и создаёт отдельную задачу.\n"
        "03 — Доступ, ограничения и результат видны в «Последних обработках».\n\n"
        "Можно выбрать один комментарий, цепочку ответов или комментарий с редактированием. "
        "Старые посты не затрагиваются."
    )
    return (
        intro
        if views
        else intro
        + "\n\n<b>С чего начать</b>\nНажмите «Настроить канал» и пройдите короткую настройку."
    )


def channel_comment_details_text(view: ChannelCommentRuleView, accounts: dict[int, str]) -> str:
    rule = view.rule
    mode = {
        ChannelCommentMode.SINGLE: "💬 Один комментарий",
        ChannelCommentMode.DIALOGUE: "🧩 Цепочка",
        ChannelCommentMode.MESSAGE_EDIT: "✏️ С редактированием",
    }[rule.mode]
    if rule.enabled:
        state = "🟢 Отслеживание включено"
    elif rule.last_error_code in CHANNEL_COMMENT_AUTO_PAUSE_CODES:
        state = f"⛔ Автоматически остановлено: {failure_label(rule.last_error_code)}"
    else:
        state = "⏸ Отслеживание остановлено"
    title = escape(rule.channel_title or rule.channel_ref)
    text = escape(rule.primary_text)
    if len(text) > 500:
        text = f"{text[:497]}…"
    delay = _delay(int(rule.initial_delay_seconds or 0))
    account_label = escape(accounts.get(rule.primary_account_id, f"#{rule.primary_account_id}"))
    last_scanned = _moment(rule.last_scanned_at)
    last_success = _moment(rule.last_success_at)
    lines = [
        screen_title(escape(rule.name), state),
        "",
        section("Канал и режим"),
        f"📢 {title} · <code>{escape(rule.channel_ref)}</code>",
        f"Режим: <b>{mode}</b>\n",
        section("Настройки"),
        f"👤 Первый аккаунт: <b>{account_label}</b>",
        (
            "🔁 Автозамена первого аккаунта: <b>включена</b> · при блокировке "
            "подбирается другой готовый аккаунт после проверки канала"
            "\n<i>Уже начатые цепочки и редактирования не меняются.</i>"
            if rule.auto_replace_primary_account
            else "🔁 Автозамена первого аккаунта: <b>выключена</b>"
        ),
        f"⏱ Первый комментарий: <b>{delay}</b> после поста",
        (
            "🔔 Отметки участников: <b>включены</b> · 5–10 в первом комментарии"
            if rule.hidden_mentions_enabled
            else "🔔 Отметки участников: <b>выключены</b>"
        ),
        f"🔄 Повторная отметка: не раньше чем через <b>{rule.mention_cooldown_days} дн.</b>",
        (
            f"👍 Реакции на свой комментарий: <b>{rule.reaction_emoji} × "
            f"{rule.reaction_count}</b> · комментарий № {rule.reaction_dialogue_step}"
            if rule.reactions_enabled and rule.reaction_count
            else "👍 Реакции на свой комментарий: <b>выключены</b>"
        ),
    ]
    if rule.mode == ChannelCommentMode.MESSAGE_EDIT:
        initial_text = escape(rule.message_edit_initial_text or "")
        if len(initial_text) > 500:
            initial_text = f"{initial_text[:497]}…"
        lines.append(
            f"✏️ Редактирование через: <b>{_delay(int(rule.message_edit_delay_seconds or 0))}</b>"
        )
        lines.extend(
            [
                "\n" + section("Содержание"),
                f"🫧 Исходный текст:\n<blockquote>{initial_text}</blockquote>",
                f"📝 Текст после редактирования:\n<blockquote>{text}</blockquote>",
            ]
        )
    else:
        lines.extend(["\n" + section("Содержание")])
        if rule.primary_media_kind:
            media_label = {
                "photo": "фото",
                "video": "видео",
                "document": "документ",
                "voice": "голосовое",
                "video_note": "видеокружок",
            }.get(rule.primary_media_kind, rule.primary_media_kind)
            lines.append(f"📎 Медиа: <b>{escape(media_label)}</b>")
        if text:
            lines.append(f"📝 Текст:\n<blockquote>{text}</blockquote>")
    lines.extend(
        [
            "\n" + section("Работа правила"),
            f"🔍 Проверка Telegram: <b>{last_scanned}</b>",
            f"✅ Последний обработанный пост: <b>{last_success}</b>",
            f"🕒 В очереди: <b>{view.queued}</b> · выполняется: <b>{view.running}</b>",
            (
                f"✅ Готово: <b>{view.succeeded}</b> · ⏭ Пропущено: <b>{view.skipped}</b> "
                f"· ❌ Ошибки: <b>{view.failed}</b>"
            ),
        ]
    )
    if rule.hidden_mentions_enabled:
        lines.extend(
            [
                "\n<b>Отметки участников</b>",
                f"✅ С отметками: <b>{view.mentions_resolved_posts}</b> постов · "
                f"участников отмечено: <b>{view.mention_count}</b>",
                f"⏭ Без отметок: <b>{view.mentions_skipped_posts}</b> постов",
            ]
        )
        for reason, count in view.mention_skip_reasons[:3]:
            lines.append(f"• {_mention_skip_label(reason)}: <b>{count}</b>")
    if rule.reactions_enabled:
        lines.extend(
            [
                "\n<b>Реакции на комментарии</b>",
                f"🕒 В очереди: <b>{view.reaction_queued}</b> · "
                f"идёт: <b>{view.reaction_running}</b>",
                f"✅ Готово: <b>{view.reaction_succeeded}</b> · "
                f"⏭ Пропущено: <b>{view.reaction_skipped}</b> · "
                f"❌ Ошибки: <b>{view.reaction_failed}</b> · "
                f"⏹ Отменено: <b>{view.reaction_cancelled}</b>",
            ]
        )
    if rule.last_error:
        label = failure_label(rule.last_error_code) if rule.last_error_code else "ошибка правила"
        lines.append(f"\n⚠️ <b>Последняя проблема: {escape(label)}</b>")
        if rule.last_error_account_id is not None:
            failed_account = escape(
                accounts.get(rule.last_error_account_id, f"#{rule.last_error_account_id}")
            )
            lines.append(
                f"Аккаунт: <b>{failed_account}</b> · канал: <code>{escape(rule.channel_ref)}</code>"
            )
        hint = failure_hint(rule.last_error_code)
        if hint:
            lines.append(f"💡 {escape(hint)}")
        if rule.enabled:
            lines.append(
                "Система: эта попытка перенесена или пропущена; наблюдение канала продолжается."
            )
        else:
            lines.append("Система: правило остановлено автоматически, чтобы не повторять ошибку.")
        lines.append(f"<code>{escape(rule.last_error[:300])}</code>")
    return "\n".join(lines)


def channel_comment_runs_text(
    runs: list[ChannelCommentRun],
    *,
    channel_ref: str,
    reaction_summaries: Mapping[int, ChannelCommentReactionSummary] | None = None,
) -> str:
    lines = [
        screen_title(
            "Последние обработки",
            "Новые посты выбранного канала и результаты автокомментинга",
        )
    ]
    if not runs:
        lines.append("\nПока нет новых постов после включения отслеживания.")
        return "\n".join(lines)
    labels = {
        ChannelCommentRunStatus.QUEUED: "🕒 ждёт",
        ChannelCommentRunStatus.RUNNING: "🔵 отправляется",
        ChannelCommentRunStatus.SUCCEEDED: "✅ готово",
        ChannelCommentRunStatus.SKIPPED: "⏭ пропущен",
        ChannelCommentRunStatus.FAILED: "❌ ошибка",
        ChannelCommentRunStatus.CANCELLED: "⏸ отменён",
    }
    for run in runs:
        published = (
            run.source_post_at.astimezone(UTC).strftime("%d.%m %H:%M UTC")
            if run.source_post_at is not None
            else "время неизвестно"
        )
        retry_details = ""
        if run.status == ChannelCommentRunStatus.QUEUED and int(run.attempt_count or 0) > 0:
            retry_details = (
                f"\n🔁 Попытка: <b>{run.attempt_count}/{run.max_attempts}</b> · "
                f"следующая: <b>{_moment(run.scheduled_for)}</b>"
            )
        error_details = ""
        if run.error_code:
            error_details = f"\nПричина: <b>{escape(failure_label(run.error_code))}</b>"
            if run.error and run.error_code not in _RUN_CODES_WITHOUT_RAW_DETAILS:
                error_details += f"\n<code>{escape(run.error[:250])}</code>"
        elif run.error:
            error_details = f"\n<code>{escape(run.error[:250])}</code>"
        post_link = _channel_comment_post_link(channel_ref, run.source_post_id)
        comment_link = _channel_comment_link(
            channel_ref,
            run.source_post_id,
            run.last_comment_message_id,
        )
        opened_post = f' · <a href="{post_link}">Открыть пост ↗</a>' if post_link else ""
        opened_comment = (
            f'\n💬 <a href="{comment_link}">Открыть последний комментарий ↗</a>'
            if comment_link
            else ""
        )
        mentions = _run_mention_status(run)
        reactions = _run_reaction_status((reaction_summaries or {}).get(int(run.id or 0)))
        lines.append(
            f"\n<b>Пост #{run.source_post_id}</b> · {published}{opened_post}\n"
            f"{labels[run.status]} · шагов: <b>{int(run.sent_steps or 0)}</b>"
            + opened_comment
            + mentions
            + reactions
            + retry_details
            + error_details
        )
    return "\n".join(lines)


def _run_mention_status(run: ChannelCommentRun) -> str:
    """Show the result of hidden mentions beside the exact post that used them."""

    if not run.hidden_mentions_enabled_snapshot:
        return ""
    count = int(run.hidden_mention_resolved_count or 0)
    if count:
        return f"\n🔔 Отметки: добавлено <b>{count}</b>"
    if run.hidden_mention_skip_reason:
        return f"\n🔔 Отметки: не добавлены — {_mention_skip_label(run.hidden_mention_skip_reason)}"
    if int(run.sent_steps or 0) > 0:
        return "\n🔔 Отметки: не добавлены"
    return "\n🔔 Отметки: будут подобраны перед первой отправкой"


def _run_reaction_status(summary: ChannelCommentReactionSummary | None) -> str:
    """Render all reaction states so a queued or cancelled reaction is never invisible."""

    if summary is None:
        return ""
    items = (
        ("ожидают", summary.queued),
        ("отправляются", summary.running),
        ("готово", summary.succeeded),
        ("пропущено", summary.skipped),
        ("ошибки", summary.failed),
        ("отменено", summary.cancelled),
    )
    shown = [f"{label}: <b>{count}</b>" for label, count in items if count]
    return f"\n👍 Реакции: {' · '.join(shown)}" if shown else ""


def _channel_comment_post_link(channel_ref: str, post_id: int) -> str | None:
    username = channel_ref.strip().removeprefix("@")
    if post_id <= 0 or not username.isascii() or not username.replace("_", "").isalnum():
        return None
    return f"https://t.me/{username}/{post_id}"


def _channel_comment_link(
    channel_ref: str,
    post_id: int,
    comment_message_id: int | None,
) -> str | None:
    if comment_message_id is None or comment_message_id <= 0:
        return None
    post_link = _channel_comment_post_link(channel_ref, post_id)
    return f"{post_link}?comment={comment_message_id}" if post_link else None


def channel_comment_draft_text(data: dict[str, object], accounts: dict[int, str]) -> str:
    mode = ChannelCommentMode(str(data.get("comment_mode", "single")))
    steps = [
        DialogueStep(**item) for item in data.get("comment_steps", []) if isinstance(item, dict)
    ]
    heading = {
        ChannelCommentMode.SINGLE: "💬 Комментарий",
        ChannelCommentMode.DIALOGUE: "🧩 Цепочка",
        ChannelCommentMode.MESSAGE_EDIT: "✏️ С редактированием",
    }[mode]
    raw_refs = data.get("channel_refs")
    channel_refs = (
        [str(value) for value in raw_refs if str(value).strip()]
        if isinstance(raw_refs, (list, tuple))
        else []
    )
    if not channel_refs:
        channel_refs = [str(data.get("channel_ref") or "не задан")]
    shown_refs = ", ".join(f"<code>{escape(value)}</code>" for value in channel_refs[:5])
    if len(channel_refs) > 5:
        shown_refs += f" и ещё {len(channel_refs) - 5}"
    lines = [
        screen_title(
            f"{heading} к новому посту",
            "Черновик: ничего не отслеживается и не отправляется.",
        ),
        "",
        f"Название: <b>{escape(str(data.get('comment_name') or 'не задано'))}</b>",
        (
            f"Канал: {shown_refs}"
            if len(channel_refs) == 1
            else f"Каналы: <b>{len(channel_refs)}</b> · {shown_refs}"
        ),
    ]
    if mode != ChannelCommentMode.MESSAGE_EDIT:
        lines.append(
            "Задержка первого комментария: "
            f"<b>{_delay(int(data.get('initial_delay_seconds') or 0))}</b>"
        )
    if mode == ChannelCommentMode.SINGLE:
        account_id = int(data.get("primary_account_id") or 0)
        text = escape(str(data.get("primary_text") or ""))
        lines.append(f"\n👤 {escape(accounts.get(account_id, 'аккаунт не выбран'))}")
        media_kind = str(data.get("primary_media_kind") or "")
        if media_kind:
            media_label = {
                "photo": "фото",
                "video": "видео",
                "document": "документ",
                "voice": "голосовое",
                "video_note": "видеокружок",
            }.get(media_kind, media_kind)
            lines.append(f"📎 {escape(media_label)}")
        if text:
            lines.append(f"<blockquote>{text}</blockquote>")
    elif mode == ChannelCommentMode.DIALOGUE:
        lines.append(f"\n<b>Шаги цепочки · {len(steps)} / 20</b>")
        for index, step in enumerate(steps, start=1):
            label = escape(accounts.get(step.account_id, f"#{step.account_id}"))
            delay = "сразу" if index == 1 else f"через {_delay(step.delay_seconds)}"
            preview = escape(step.text[:160] + ("…" if len(step.text) > 160 else ""))
            media = ""
            if step.media_kind:
                media_kind = {
                    "photo": "фото",
                    "video": "видео",
                    "document": "документ",
                    "voice": "голосовое",
                    "video_note": "видеокружок",
                }.get(step.media_kind, step.media_kind)
                media = f" · {escape(media_kind)}"
            body = f"\n<blockquote>{preview}</blockquote>" if preview else ""
            lines.append(f"{index}. <b>{label}</b> · {delay}{media}{body}")
    else:
        account_id = int(data.get("primary_account_id") or 0)
        initial_text = escape(str(data.get("message_edit_initial_text") or ""))
        final_text = escape(str(data.get("primary_text") or ""))
        delay = _delay(int(data.get("message_edit_delay_seconds") or 0))
        lines.extend(
            [
                f"\n👤 {escape(accounts.get(account_id, 'аккаунт не выбран'))}",
                f"🫧 Сначала: <blockquote>{initial_text or 'текст не задан'}</blockquote>",
                f"📝 Затем: <blockquote>{final_text or 'текст не задан'}</blockquote>",
                f"✏️ Редактировать через: <b>{delay}</b>",
            ]
        )
    return "\n".join(lines)


def _delay(seconds: int) -> str:
    if seconds <= 0:
        return "сразу"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ч"
    if seconds % 60 == 0:
        return f"{seconds // 60} мин"
    return f"{seconds} сек"


def _moment(value: datetime | None) -> str:
    if value is None:
        return "ещё не было"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%d.%m %H:%M UTC")


def _mention_skip_label(reason: str) -> str:
    return {
        "album_duplicate": "общая ветка альбома уже обработана",
        "content_does_not_support_mentions": "формат сообщения не поддерживает отметки",
        "message_has_no_room_for_mentions": "в сообщении не хватило места",
        "gateway_does_not_support_participant_collection": "сервис не получил список участников",
        "fewer_than_five_eligible_members": "меньше пяти подходящих участников",
        "participant_collection_unavailable": "список участников временно недоступен",
        "mention_cooldown_active": "подходящие участники ещё на паузе ротации",
        "mention_reservation_conflict": "участников зарезервировала параллельная задача",
        "owned_account_excluded_after_resolution": "собственные аккаунты исключены",
        "excluded_user_after_resolution": "запрещённые участники исключены перед отправкой",
    }.get(reason, escape(reason))


for _factory_name, _factory in tuple(globals().items()):
    if (
        not _factory_name.startswith("_")
        and callable(_factory)
        and _factory_name.endswith(("menu", "picker", "confirmation", "cancel"))
    ):
        globals()[_factory_name] = themed_keyboard(_factory)
