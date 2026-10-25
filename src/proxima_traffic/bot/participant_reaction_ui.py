from __future__ import annotations

import json
from html import escape

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from proxima_traffic.bot.repository import Page, TargetListView
from proxima_traffic.bot.ui import screen_title, section, themed_keyboard
from proxima_traffic.failure_policy import failure_hint, failure_label
from proxima_traffic.models import Account, ParticipantReactionTask, ParticipantReactionTaskStatus
from proxima_traffic.participant_reactions import ParticipantReactionRuleView


def participant_reactions_menu(
    views: list[ParticipantReactionRuleView], *, page: int, total: int, page_size: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="＋ Настроить реакции", callback_data="pr:add"))
    for view in views:
        builder.button(
            text=f"{'●' if view.rule.enabled else 'Ⅱ'} {view.rule.name}",
            callback_data=f"pr:details:{view.rule.id}:{page}",
        )
    builder.adjust(1)
    pages = max(1, (total + page_size - 1) // page_size)
    if pages > 1:
        row = []
        if page:
            row.append(InlineKeyboardButton(text="‹", callback_data=f"pr:home:{page - 1}"))
        row.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="noop"))
        if page + 1 < pages:
            row.append(InlineKeyboardButton(text="›", callback_data=f"pr:home:{page + 1}"))
        builder.row(*row)
    builder.row(
        InlineKeyboardButton(text="↻ Обновить", callback_data=f"pr:home:{page}"),
        InlineKeyboardButton(text="⌂ Главная", callback_data="menu:home"),
    )
    return builder.as_markup()


def participant_reaction_details_menu(
    view: ParticipantReactionRuleView, *, page: int
) -> InlineKeyboardMarkup:
    action = "Ⅱ Остановить" if view.rule.enabled else "› Включить"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=action, callback_data=f"pr:toggle:{view.rule.id}:{page}")],
            [
                InlineKeyboardButton(
                    text="Настроить", callback_data=f"pr:edit:{view.rule.id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="↻ Проверить сейчас", callback_data=f"pr:scan:{view.rule.id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Последние реакции", callback_data=f"pr:runs:{view.rule.id}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="× Удалить", callback_data=f"pr:delete:ask:{view.rule.id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="‹ К реакциям участникам", callback_data=f"pr:home:{page}")],
        ]
    )


def participant_reaction_delete_menu(rule_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="× Да, удалить", callback_data=f"pr:delete:confirm:{rule_id}:{page}"
                )
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data=f"pr:details:{rule_id}:{page}")],
        ]
    )


def participant_reaction_list_picker(page: Page[TargetListView]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for item in page.items:
        builder.button(
            text=f"{item.name} · {item.approved_count}", callback_data=f"pr:list:pick:{item.id}"
        )
    builder.adjust(1)
    if page.pages > 1:
        row = []
        if page.page:
            row.append(
                InlineKeyboardButton(text="‹", callback_data=f"pr:list:page:{page.page - 1}")
            )
        row.append(InlineKeyboardButton(text=f"{page.page + 1}/{page.pages}", callback_data="noop"))
        if page.page + 1 < page.pages:
            row.append(
                InlineKeyboardButton(text="›", callback_data=f"pr:list:page:{page.page + 1}")
            )
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="× Отмена", callback_data="pr:home:0"))
    return builder.as_markup()


def participant_reaction_account_picker(
    accounts: list[Account], selected: set[int], *, page: int
) -> InlineKeyboardMarkup:
    size = 8
    pages = max(1, (len(accounts) + size - 1) // size)
    builder = InlineKeyboardBuilder()
    for account in accounts[page * size : (page + 1) * size]:
        mark = "✓" if account.id in selected else "○"
        builder.button(
            text=f"{mark} {account.label}", callback_data=f"pr:account:{account.id}:{page}"
        )
    builder.adjust(1)
    if pages > 1:
        row = []
        if page:
            row.append(InlineKeyboardButton(text="‹", callback_data=f"pr:account:page:{page - 1}"))
        row.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="noop"))
        if page + 1 < pages:
            row.append(InlineKeyboardButton(text="›", callback_data=f"pr:account:page:{page + 1}"))
        builder.row(*row)
    builder.row(
        InlineKeyboardButton(
            text=f"✓ Готово · выбрано {len(selected)}", callback_data="pr:account:done"
        )
    )
    builder.row(InlineKeyboardButton(text="× Отмена", callback_data="pr:home:0"))
    return builder.as_markup()


def participant_reaction_sample_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 из 20 · редко", callback_data="pr:sample:1:20"),
                InlineKeyboardButton(text="1 из 10 · обычно", callback_data="pr:sample:1:10"),
            ],
            [
                InlineKeyboardButton(text="3 из 10 · заметно", callback_data="pr:sample:3:10"),
                InlineKeyboardButton(text="5 из 10 · часто", callback_data="pr:sample:5:10"),
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="pr:home:0")],
        ]
    )


def participant_reaction_emoji_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👍", callback_data="pr:emoji:thumb"),
                InlineKeyboardButton(text="🔥", callback_data="pr:emoji:fire"),
                InlineKeyboardButton(text="❤️", callback_data="pr:emoji:heart"),
                InlineKeyboardButton(text="⚡", callback_data="pr:emoji:bolt"),
            ],
            [InlineKeyboardButton(text="× Отмена", callback_data="pr:home:0")],
        ]
    )


def participant_reaction_confirm_menu(
    callback_data: str = "pr:create", *, label: str = "✓ Сохранить и включить"
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=callback_data)],
            [InlineKeyboardButton(text="× Отмена", callback_data="pr:home:0")],
        ]
    )


def participant_reactions_text(views: list[ParticipantReactionRuleView], total: int) -> str:
    active = sum(view.rule.enabled for view in views)
    queued = sum(view.queued for view in views)
    running = sum(view.running for view in views)
    text = (
        screen_title(
            "Реакции участникам",
            "На новые сообщения в выбранных чатах",
        )
        + "\n\n"
        f"{section('Состояние')}\n"
        f"Правил: <b>{total}</b> · включено на этой странице: <b>{active}</b>\n"
        f"Здесь в очереди: <b>{queued}</b> · выполняется: <b>{running}</b>\n\n"
        f"{section('Как это работает')}\n"
        "Выберите список чатов, аккаунты, реакцию и частоту. "
        "Используются только выбранные аккаунты.\n\n"
        "Старые сообщения, боты, каналы и собственные аккаунты исключаются. "
        "Пропуски и ошибки доступны в истории правила."
    )
    return (
        text
        if views
        else text + "\n\n<b>С чего начать</b>\nНастройте правило и выберите готовый список чатов."
    )


def participant_reaction_details_text(
    view: ParticipantReactionRuleView, labels: dict[int, str]
) -> str:
    try:
        ids = [int(value) for value in json.loads(view.rule.account_ids_json)]
    except (TypeError, ValueError):
        ids = []
    accounts = ", ".join(escape(labels.get(item, f"#{item}")) for item in ids) or "не выбраны"
    rule = view.rule
    frequency = _sample_frequency(rule.sample_numerator, rule.sample_denominator)
    lines = [
        screen_title(
            escape(rule.name),
            "Правило включено" if rule.enabled else "Ⅱ Правило остановлено",
        ),
        "",
        section("Где и кем"),
        f"Список чатов: <b>{escape(view.target_list_name)}</b> · {view.target_count} чатов",
        f"Только эти аккаунты: {accounts}\n",
        section("Как реагировать"),
        f"Реакция: <b>{rule.emoji}</b>",
        f"Частота: <b>{frequency}</b>\n",
        section("Результаты"),
        f"В очереди: <b>{view.queued}</b> · выполняется: <b>{view.running}</b>",
        f"✓ Готово: <b>{view.succeeded}</b> · ›› Пропущено: <b>{view.skipped}</b> "
        f"· × Ошибки: <b>{view.failed}</b>",
    ]
    if rule.last_error:
        label = failure_label(rule.last_error_code) if rule.last_error_code else "ошибка правила"
        lines.extend(["", f"! <b>Последняя проблема: {escape(label)}</b>"])
        hint = failure_hint(rule.last_error_code)
        if hint:
            lines.append(f"· {escape(hint)}")
        lines.append(f"<code>{escape(rule.last_error[:300])}</code>")
    return "\n".join(lines)


def participant_reaction_draft_text(
    data: dict[str, object], list_name: str, labels: dict[int, str]
) -> str:
    ids = [int(value) for value in data.get("pr_account_ids", [])]
    accounts = ", ".join(escape(labels.get(item, f"#{item}")) for item in ids) or "не выбраны"
    numerator = int(data.get("pr_numerator", 1))
    denominator = int(data.get("pr_denominator", 10))
    return (
        screen_title("Новое правило реакций", "Проверьте настройки перед включением.") + "\n\n"
        f"Название: <b>{escape(str(data.get('pr_name', '')))}</b>\n"
        f"Список чатов: <b>{escape(list_name)}</b>\n"
        f"Только эти аккаунты: {accounts}\n"
        f"Реакция: <b>{data.get('pr_emoji', '👍')}</b>\n"
        f"Частота: <b>{_sample_frequency(numerator, denominator)}</b>\n\n"
        "<i>Первая проверка только запомнит текущую границу каждого чата. Старые "
        "сообщения, боты, каналы и собственные аккаунты не получат реакций. Реакцию "
        "сможет поставить только аккаунт из указанного выше списка.</i>"
    )


def participant_reaction_runs_text(
    tasks: list[ParticipantReactionTask],
    *,
    account_labels: dict[int, str] | None = None,
    target_labels: dict[int, str] | None = None,
) -> str:
    labels = {
        ParticipantReactionTaskStatus.QUEUED: "ждёт",
        ParticipantReactionTaskStatus.RUNNING: "выполняется",
        ParticipantReactionTaskStatus.SUCCEEDED: "✓ готово",
        ParticipantReactionTaskStatus.SKIPPED: "›› пропущено",
        ParticipantReactionTaskStatus.FAILED: "× ошибка",
        ParticipantReactionTaskStatus.CANCELLED: "Ⅱ отменено",
    }
    lines = [screen_title("История реакций"), ""]
    account_labels = account_labels or {}
    target_labels = target_labels or {}
    for task in tasks:
        account_label = escape(
            account_labels.get(task.reactor_account_id, f"#{task.reactor_account_id}")
        )
        target_label = escape(target_labels.get(task.target_id, f"чат #{task.target_id}"))
        task_label = (
            f"{labels[task.status]} · {target_label}\n"
            f"Аккаунт: <b>{account_label}</b> · сообщение #{task.message_id} · {task.emoji}"
        )
        details = ""
        if task.error_code:
            details = f"\nПричина: <b>{escape(failure_label(task.error_code))}</b>"
            if task.status == ParticipantReactionTaskStatus.QUEUED:
                details += "\nСистема: реакция отложена и будет проверена повторно."
            elif task.status == ParticipantReactionTaskStatus.SKIPPED:
                details += "\nСистема: эту реакцию безопасно пропустили."
            elif task.status == ParticipantReactionTaskStatus.FAILED:
                details += "\nСистема: задача завершена без небезопасного повтора."
        if task.error:
            details += f"\n<code>{escape(task.error)}</code>"
        lines.append(task_label + details)
    return "\n".join(lines if tasks else [*lines, "Пока нет задач."])


def _sample_frequency(numerator: int, denominator: int) -> str:
    if numerator == 1:
        return f"примерно 1 из {denominator} новых сообщений"
    return f"примерно {numerator} из {denominator} новых сообщений"


for _factory_name, _factory in tuple(globals().items()):
    if (
        not _factory_name.startswith("_")
        and callable(_factory)
        and _factory_name.endswith(("menu", "picker", "confirmation", "cancel"))
    ):
        globals()[_factory_name] = themed_keyboard(_factory)
