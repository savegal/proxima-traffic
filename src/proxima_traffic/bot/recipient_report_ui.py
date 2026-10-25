"""Paginated recipient results shared by messages, invitations and the audience directory."""

from html import escape

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from proxima_traffic.bot.ui import screen_title, themed_keyboard
from proxima_traffic.direct_actions import DIRECT_SERVICE_ACTIONS
from proxima_traffic.failure_policy import failure_label
from proxima_traffic.models import DirectMessageTaskStatus, ensure_utc
from proxima_traffic.recipient_reports import (
    RecipientReportFilter,
    RecipientReportKind,
    RecipientReportPage,
    RecipientResult,
)


def report_callback(
    action: str,
    kind: RecipientReportKind,
    launch_id: int | None,
    result_filter: RecipientReportFilter,
    page: int,
    back_page: int,
    token: str = "0",
) -> str:
    return f"rr:{action}:{kind}:{launch_id or 0}:{result_filter}:{page}:{back_page}:{token}"


def _result_label(item: RecipientResult, kind: RecipientReportKind) -> str:
    if item.status == DirectMessageTaskStatus.SUCCEEDED:
        if item.simulated is True:
            return "симуляция · успешно"
        if item.simulated is None:
            return "успешная задача · режим запуска не сохранён"
        if item.already_participant:
            return "уже состоял в группе / канале"
        if item.operation_kind in DIRECT_SERVICE_ACTIONS:
            return (
                "настройка и сервисное сообщение подтверждены"
                if item.result_code == "service_notification_confirmed"
                else "настройка подтверждена · сервисное сообщение не подтверждено"
            )
        return "ЛС отправлено и подтверждено" if kind == RecipientReportKind.DIRECT else "приглашён"
    label = {
        DirectMessageTaskStatus.QUEUED: "в очереди",
        DirectMessageTaskStatus.RUNNING: "в работе",
        DirectMessageTaskStatus.FAILED: "ошибка",
        DirectMessageTaskStatus.SKIPPED: "пропущен",
        DirectMessageTaskStatus.CANCELLED: "отменён",
    }.get(item.status, "результат неизвестен")
    if item.error_code:
        label += " · " + failure_label(item.error_code)
    if item.simulated is True:
        label = "симуляция · " + label
    return label


def recipient_report_text(report: RecipientReportPage) -> str:
    directory = report.kind == RecipientReportKind.DIRECTORY
    title = (
        "Общая база · люди"
        if directory
        else (
            "Получатели ЛС"
            if report.kind == RecipientReportKind.DIRECT
            else "Получатели приглашений"
        )
    )
    scope = (
        f"Запуск #{report.launch_id}"
        if report.launch_id is not None
        else ("По всем сохранённым запускам" if not directory else "Поиск и история человека")
    )
    filters = {
        RecipientReportFilter.SUCCESS: "Подтверждённые результаты ЛС"
        if report.kind == RecipientReportKind.DIRECT
        else "Новые приглашённые",
        RecipientReportFilter.ALL: "Все результаты",
        RecipientReportFilter.ALREADY_PARTICIPANT: "Уже были участниками",
        RecipientReportFilter.SIMULATED: "Успешные симуляции",
    }
    lines = [screen_title(title, scope)]
    if not directory:
        lines.append(f"Показано: <b>{filters[report.result_filter]}</b>")
    if report.query:
        lines.append(f"Поиск: <code>{escape(report.query)}</code>")
    lines.append(
        f"{'Людей' if directory else 'Записей'}: <b>{report.total:,}</b> · "
        f"страница <b>{report.page + 1}/{report.pages}</b>"
    )
    if report.items:
        first = report.page * report.page_size + 1
        last = first + len(report.items) - 1
        lines.append(f"Показаны: <b>{first:,}–{last:,}</b> из <b>{report.total:,}</b>")
    if directory and report.pages > 1:
        lines.append("<i>Нажмите номер страницы для быстрого перехода.</i>")
    if report.launch_id is not None and report.launch_simulated is True:
        lines.append("Этот запуск — симуляция. Реальных отправок или приглашений в нём нет.")
    elif report.launch_id is not None and report.launch_simulated is None:
        lines.append("У старого запуска не сохранён режим; реальное выполнение не подтверждено.")
    for offset, item in enumerate(report.items, start=report.page * report.page_size + 1):
        username = f"@{escape(item.username)}" if item.username else "без username"
        text = f"<b>{offset}. {username}</b> · ID <code>{item.telegram_user_id}</code>"
        if directory:
            if item.do_not_contact:
                text += "\nВ списке «не писать»"
            if item.quarantine_code:
                text += "\nКарантин ЛС: " + escape(failure_label(item.quarantine_code))
        else:
            text += "\n" + escape(_result_label(item, report.kind))
            sender = (
                escape((item.sender_label or f"аккаунт #{item.sender_account_id}")[:40])
                if item.sender_account_id is not None
                else "отправитель не сохранён"
            )
            text += f"\nЗапуск #{item.launch_id} · {sender}"
            finished = ensure_utc(item.finished_at)
            if finished is not None:
                text += f" · {finished:%d.%m %H:%M} UTC"
        lines.append(text)
    if not report.items:
        lines.append("Совпадений нет." if report.query else "В этом списке пока нет получателей.")
    if not directory and report.launch_id is None:
        lines.append(
            "Одна запись — один результат запуска; человек может встречаться несколько раз."
        )
    lines.append("Username может меняться. Telegram ID — основной идентификатор человека.")
    return "\n\n".join(lines)


@themed_keyboard
def recipient_report_menu(
    report: RecipientReportPage,
    *,
    back_page: int,
    token: str = "0",
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    def callback(
        action: str,
        page: int,
        filter_: RecipientReportFilter | None = None,
        query_token: str = token,
    ) -> str:
        return report_callback(
            action,
            report.kind,
            report.launch_id,
            filter_ or report.result_filter,
            page,
            back_page,
            query_token,
        )

    rows = []
    if report.kind == RecipientReportKind.DIRECTORY:
        for item in report.items:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"ЛС · {item.telegram_user_id}",
                        callback_data=(
                            f"rr:h:d:{item.telegram_user_id}:{report.page}:{back_page}:{token}"
                        ),
                    ),
                    InlineKeyboardButton(
                        text="Приглашения",
                        callback_data=(
                            f"rr:h:i:{item.telegram_user_id}:{report.page}:{back_page}:{token}"
                        ),
                    ),
                ]
            )
    elif report.launch_id is None:
        for launch_id in dict.fromkeys(item.launch_id for item in report.items):
            prefix = "dm" if report.kind == RecipientReportKind.DIRECT else "inv"
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"Открыть запуск #{launch_id}",
                        callback_data=f"{prefix}:launchview:{launch_id}:0",
                    )
                ]
            )
    # Keep the navigation immediately below the list text, before histories.
    detail_rows = rows
    rows = []
    nav = []
    if report.page > 0:
        nav.append(
            InlineKeyboardButton(text="‹ Предыдущая", callback_data=callback("v", report.page - 1))
        )
    nav.append(
        InlineKeyboardButton(
            text=(f"{report.page + 1:,} / {report.pages:,} ▾" if report.pages > 1 else "1 / 1"),
            callback_data=callback("g", report.page) if report.pages > 1 else "noop",
        )
    )
    if report.page + 1 < report.pages:
        nav.append(
            InlineKeyboardButton(text="Следующая ›", callback_data=callback("v", report.page + 1))
        )
    rows.append(nav)
    if report.pages > 2:
        rows.append(
            [
                InlineKeyboardButton(text="« В начало", callback_data=callback("v", 0)),
                InlineKeyboardButton(
                    text="В конец »", callback_data=callback("v", report.pages - 1)
                ),
            ]
        )
    if report.pages > 12:
        jumps = []
        if report.page >= 10:
            jumps.append(
                InlineKeyboardButton(
                    text="−10 страниц", callback_data=callback("v", report.page - 10)
                )
            )
        if report.page + 10 < report.pages:
            jumps.append(
                InlineKeyboardButton(
                    text="+10 страниц", callback_data=callback("v", report.page + 10)
                )
            )
        if jumps:
            rows.append(jumps)
    rows.append(
        [
            InlineKeyboardButton(
                text="⌕ Поиск по ID / username", callback_data=callback("s", report.page)
            )
        ]
    )
    if report.query:
        rows.append(
            [
                InlineKeyboardButton(
                    text="× Сбросить поиск", callback_data=callback("v", 0, query_token="0")
                )
            ]
        )
    if report.kind != RecipientReportKind.DIRECTORY:
        filters = [
            (RecipientReportFilter.SUCCESS, "✓ Успешные"),
            (RecipientReportFilter.ALL, "≡ Все результаты"),
            (RecipientReportFilter.SIMULATED, "◇ Симуляции"),
        ]
        if report.kind == RecipientReportKind.INVITE:
            filters.append((RecipientReportFilter.ALREADY_PARTICIPANT, "Уже участники"))
        rows.append(
            [
                InlineKeyboardButton(text=label, callback_data=callback("v", 0, filter_))
                for filter_, label in filters
                if filter_ != report.result_filter
            ]
        )
    rows.extend(detail_rows)
    if report.kind == RecipientReportKind.DIRECTORY and len(report.items) > 5 and report.pages > 1:
        rows.append([button.model_copy() for button in nav])
    rows.append([InlineKeyboardButton(text="↻ Обновить", callback_data=callback("v", report.page))])
    if back_callback is None:
        prefix = "dm" if report.kind != RecipientReportKind.INVITE else "inv"
        if report.kind == RecipientReportKind.DIRECTORY and back_page == 1:
            prefix = "inv"
        back_callback = (
            f"{prefix}:launchview:{report.launch_id}:{back_page}"
            if report.launch_id is not None
            else f"{prefix}:home"
        )
    rows.append([InlineKeyboardButton(text="‹ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)
