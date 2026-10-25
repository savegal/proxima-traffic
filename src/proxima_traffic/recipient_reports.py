"""Read-only recipient lists and exact identity search over durable launch results."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import ceil
from urllib.parse import urlsplit

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.models import (
    Account,
    DirectLaunch,
    DirectMessageTask,
    DirectMessageTaskStatus,
    Subscriber,
    SubscriberInviteLaunch,
    SubscriberInviteTask,
)
from proxima_traffic.services import ConflictError, NotFoundError

RECIPIENT_REPORT_PAGE_SIZE = 10


class RecipientReportKind(StrEnum):
    DIRECT = "d"
    INVITE = "i"
    DIRECTORY = "b"


class RecipientReportFilter(StrEnum):
    SUCCESS = "s"
    ALL = "a"
    ALREADY_PARTICIPANT = "p"
    SIMULATED = "t"


def normalize_recipient_query(value: str) -> str:
    query = value.strip()
    if query.isascii() and query.isdecimal():
        user_id = int(query) if len(query) <= 19 else 0
        if not 1 <= user_id <= (1 << 63) - 1:
            raise ConflictError(
                "Telegram ID должен быть положительным числом в допустимом диапазоне"
            )
        return str(user_id)
    if query.casefold().startswith(("https://", "http://", "t.me/")):
        try:
            url = urlsplit(query if "://" in query else "https://" + query)
        except ValueError as exc:
            raise ConflictError(
                "Отправьте корректную ссылку t.me/username или Telegram ID"
            ) from exc
        if url.hostname not in {"t.me", "telegram.me"} or url.query or url.fragment:
            raise ConflictError("Отправьте Telegram ID, @username или ссылку t.me/username")
        query = url.path.strip("/")
    query = query.removeprefix("@").casefold()
    if not re.fullmatch(r"[a-z0-9_]{1,32}", query):
        raise ConflictError("Отправьте точный Telegram ID или username, например @username")
    return "@" + query


@dataclass(frozen=True, slots=True)
class RecipientResult:
    telegram_user_id: int
    username: str | None
    launch_id: int | None = None
    task_id: int | None = None
    status: DirectMessageTaskStatus | None = None
    finished_at: datetime | None = None
    sender_account_id: int | None = None
    sender_label: str | None = None
    simulated: bool | None = None
    already_participant: bool = False
    error_code: str | None = None
    do_not_contact: bool = False
    quarantine_code: str | None = None
    operation_kind: str = "message"
    result_code: str | None = None


@dataclass(frozen=True, slots=True)
class RecipientReportPage:
    kind: RecipientReportKind
    launch_id: int | None
    result_filter: RecipientReportFilter
    query: str
    items: tuple[RecipientResult, ...]
    total: int
    page: int
    page_size: int
    launch_simulated: bool | None = None

    @property
    def pages(self) -> int:
        return max(1, ceil(self.total / self.page_size))


async def list_recipient_report(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    kind: RecipientReportKind,
    launch_id: int | None = None,
    result_filter: RecipientReportFilter = RecipientReportFilter.SUCCESS,
    query: str = "",
    page: int = 0,
    page_size: int = RECIPIENT_REPORT_PAGE_SIZE,
) -> RecipientReportPage:
    kind = RecipientReportKind(kind)
    result_filter = RecipientReportFilter(result_filter)
    if launch_id is not None and launch_id <= 0:
        raise NotFoundError("запуск не найден")
    if kind == RecipientReportKind.DIRECTORY and launch_id is not None:
        raise ConflictError("общая база не ограничивается одним запуском")
    if (
        result_filter == RecipientReportFilter.ALREADY_PARTICIPANT
        and kind != RecipientReportKind.INVITE
    ):
        raise ConflictError("фильтр «Уже участники» доступен только для приглашений")
    normalized = normalize_recipient_query(query) if query.strip() else ""
    page_size = max(1, min(20, page_size))
    conditions = []
    if normalized:
        conditions.append(
            Subscriber.username == normalized[1:]
            if normalized.startswith("@")
            else Subscriber.telegram_user_id == int(normalized)
        )
    async with session_factory() as session:
        simulated = None
        if kind == RecipientReportKind.DIRECTORY:
            result_filter = RecipientReportFilter.ALL
            count = select(func.count(Subscriber.id)).where(*conditions)
            rows = (
                select(
                    Subscriber.telegram_user_id,
                    Subscriber.username,
                    Subscriber.do_not_contact,
                    Subscriber.direct_quarantine_code,
                )
                .where(*conditions)
                .order_by(Subscriber.id.desc())
            )
        else:
            task, launch = (
                (DirectMessageTask, DirectLaunch)
                if kind == RecipientReportKind.DIRECT
                else (SubscriberInviteTask, SubscriberInviteLaunch)
            )
            if launch_id is not None:
                launch_row = (
                    await session.execute(
                        select(launch.simulated_snapshot).where(launch.id == launch_id)
                    )
                ).one_or_none()
                if launch_row is None:
                    raise NotFoundError("запуск не найден")
                simulated = launch_row.simulated_snapshot
                conditions.append(task.launch_id == launch_id)
            if result_filter == RecipientReportFilter.SUCCESS:
                conditions.extend(
                    (
                        task.status == DirectMessageTaskStatus.SUCCEEDED,
                        launch.simulated_snapshot.is_(False),
                    )
                )
                if kind == RecipientReportKind.INVITE:
                    conditions.append(task.already_participant.is_(False))
            elif result_filter == RecipientReportFilter.ALREADY_PARTICIPANT:
                conditions.extend(
                    (
                        task.status == DirectMessageTaskStatus.SUCCEEDED,
                        launch.simulated_snapshot.is_(False),
                        task.already_participant.is_(True),
                    )
                )
            elif result_filter == RecipientReportFilter.SIMULATED:
                conditions.extend(
                    (
                        task.status == DirectMessageTaskStatus.SUCCEEDED,
                        launch.simulated_snapshot.is_(True),
                    )
                )
            count = (
                select(func.count(task.id))
                .select_from(task)
                .join(Subscriber, Subscriber.id == task.subscriber_id)
                .join(launch, launch.id == task.launch_id)
                .where(*conditions)
            )
            columns = [
                Subscriber.telegram_user_id,
                Subscriber.username,
                task.id.label("task_id"),
                task.launch_id,
                task.status,
                task.finished_at,
                task.sender_account_id,
                task.error_code,
                launch.simulated_snapshot,
                Account.label.label("sender_label"),
            ]
            if kind == RecipientReportKind.INVITE:
                columns.append(task.already_participant)
            elif kind == RecipientReportKind.DIRECT:
                columns.extend((launch.operation_kind_snapshot, task.result_code))
            rows = (
                select(*columns)
                .select_from(task)
                .join(Subscriber, Subscriber.id == task.subscriber_id)
                .join(launch, launch.id == task.launch_id)
                .outerjoin(Account, Account.id == task.sender_account_id)
                .where(*conditions)
                .order_by(task.id.desc())
            )
        total = int(await session.scalar(count) or 0)
        page = min(max(0, page), max(0, ceil(total / page_size) - 1))
        records = (await session.execute(rows.offset(page * page_size).limit(page_size))).all()
        if kind == RecipientReportKind.DIRECTORY:
            items = tuple(
                RecipientResult(
                    telegram_user_id=row.telegram_user_id,
                    username=row.username,
                    do_not_contact=row.do_not_contact,
                    quarantine_code=row.direct_quarantine_code,
                )
                for row in records
            )
        else:
            items = tuple(
                RecipientResult(
                    telegram_user_id=row.telegram_user_id,
                    username=row.username,
                    launch_id=row.launch_id,
                    task_id=row.task_id,
                    status=row.status,
                    finished_at=row.finished_at,
                    sender_account_id=row.sender_account_id,
                    sender_label=row.sender_label,
                    simulated=row.simulated_snapshot,
                    already_participant=row.already_participant
                    if kind == RecipientReportKind.INVITE
                    else False,
                    error_code=row.error_code,
                    operation_kind=row.operation_kind_snapshot
                    if kind == RecipientReportKind.DIRECT
                    else "message",
                    result_code=row.result_code if kind == RecipientReportKind.DIRECT else None,
                )
                for row in records
            )
        return RecipientReportPage(
            kind=kind,
            launch_id=launch_id,
            result_filter=result_filter,
            query=normalized,
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            launch_simulated=simulated,
        )
