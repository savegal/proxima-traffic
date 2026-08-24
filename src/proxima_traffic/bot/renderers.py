from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from html import escape
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from proxima_traffic.accounts import (
    AccountBulkCheckSummary,
    AccountInvalidSessionCleanupSummary,
)
from proxima_traffic.bot.repository import (
    AccountGroupView,
    DashboardStats,
    LaunchView,
    Page,
    TargetListView,
    TargetTimingView,
)
from proxima_traffic.bot.ui import screen_title, section
from proxima_traffic.config import Settings, get_settings
from proxima_traffic.content import (
    BroadcastContentVariant,
    ContentError,
    parse_content_variants,
    spintax_group_count,
)
from proxima_traffic.dialogues import parse_dialogue_steps
from proxima_traffic.failure_policy import failure_hint, failure_label
from proxima_traffic.models import (
    Account,
    ActionLog,
    AuditEvent,
    BroadcastSenderMode,
    Campaign,
    CampaignMode,
    CampaignRun,
    MessageTemplate,
    ProxyGroup,
    RunStatus,
    Target,
)
from proxima_traffic.preflight import CampaignPreflightSummary
from proxima_traffic.profile_names import NameLocale, profile_name_capacity
from proxima_traffic.proxies import ProxyPage, ProxyView
from proxima_traffic.scheduling import BROADCAST_PROFILES, detect_broadcast_profile
from proxima_traffic.spambot import SpamBotStatus
from proxima_traffic.spambot_checks import SpamBotBulkCheckSummary
from proxima_traffic.worker_supervision import WorkerHealthSnapshot


def _campaign_preflight_state(summary: CampaignPreflightSummary) -> str:
    if summary.target_count == 0:
        return "🔴 Нет включённых чатов"
    if summary.independent_mode:
        expected_pairs = summary.target_count * summary.account_count
        if summary.launchable_units == expected_pairs:
            return "🟢 Все направления аккаунт–чат готовы"
        if summary.launchable_units:
            return "🟡 Готова только часть направлений аккаунт–чат"
        if summary.deferred_pairs:
            return "🟠 Доступ подтверждён, но аккаунты на безопасной паузе"
        return "🔴 Нет готовых направлений аккаунт–чат"
    if summary.launchable_units == summary.target_count:
        return "🟢 Всё готово к отправке"
    if summary.launchable_units:
        return "🟡 Готова только часть аудитории"
    if summary.deferred_pairs:
        return "🟠 Доступ подтверждён, но аккаунты на безопасной паузе"
    return "🔴 Нет готовых маршрутов"


def _campaign_preflight_details(summary: CampaignPreflightSummary) -> str:
    checked_label = (
        summary.newest_checked_at.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
        if summary.newest_checked_at is not None
        else "ещё не выполнялась"
    )
    route_details = ""
    if summary.independent_mode:
        pair_total = summary.target_count * summary.account_count
        skipped_pairs = max(0, pair_total - summary.routable_pairs)
        route_details = (
            f"🔐 Маршрутов аккаунт→чат с доступом: "
            f"<b>{summary.routable_pairs}/{pair_total}</b>\n"
            f"🚀 Можно начать сейчас: "
            f"<b>{summary.launchable_units}/{pair_total}</b>\n"
        )
        if skipped_pairs:
            route_details += (
                f"⏭ Не готовых пар: <b>{skipped_pairs}</b> — они будут пропущены, "
                "а готовые пары продолжат работу.\n"
            )
    else:
        route_details = (
            f"🚀 Чатов можно начать сейчас: "
            f"<b>{summary.launchable_units}/{summary.target_count}</b>\n"
        )
    if summary.deferred_pairs:
        resume_label = (
            summary.earliest_resume_at.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
            if summary.earliest_resume_at is not None
            else "после завершения защитной паузы"
        )
        route_details += (
            f"⏸ На безопасной паузе Telegram: <b>{summary.deferred_pairs}</b> пар; "
            f"ближайшая готовность — <b>{resume_label}</b>. "
            "Обычный запуск их пропустит; отдельной кнопкой ниже их можно "
            "запланировать на безопасное время.\n"
        )
    if summary.auto_join_enabled and summary.planned_join_attempts:
        route_details += (
            f"⚠️ Перед отправкой потребуется до <b>{summary.planned_join_attempts}</b> "
            f"вступлений в <b>{summary.joinable_targets}</b> чатов. "
            "К каждому аккаунту применяются дневной лимит и минимальный интервал вступлений.\n"
        )
    if summary.requires_all_accounts:
        route_details += "👥 Для диалога должны быть готовы все аккаунты цепочки в каждом чате.\n"
    return (
        f"👥 Аккаунты: <b>{summary.account_count}</b>\n"
        f"🎯 Чаты: <b>{summary.target_count}</b>\n"
        f"🔎 Проверено пар: <b>{summary.checked_pairs}</b>\n"
        f"✅ Уже в чатах и могут писать: <b>{summary.ready_pairs}</b> пар\n"
        f"{route_details}\n"
        f"🕓 Последняя проверка: <b>{checked_label}</b>\n\n"
        "<b>Покрытие чатов</b>\n"
        f"✅ Можно отправлять: <b>{summary.member_targets}</b>\n"
        f"➕ {'Вступят автоматически' if summary.auto_join_enabled else 'Требуют вступления'}: "
        f"<b>{summary.joinable_targets}</b>\n"
        f"⏳ Заблокированы/ожидают: <b>{summary.blocked_targets}</b>\n"
        f"❌ Недоступны: <b>{summary.unavailable_targets}</b>\n"
        f"❔ Без готового маршрута: <b>{summary.uncovered_targets}</b>\n\n"
        "<i>Проверка ничего не отправляет и не вступает в группы. Если включено "
        "автовступление, оно выполняется только после подтверждения запуска.</i>"
    )


def campaign_preflight_progress_text(
    *,
    checked_pairs: int,
    total_pairs: int,
    checked_accounts: int,
    total_accounts: int,
) -> str:
    """Live, factual progress while preflight reads Telegram state."""
    return (
        screen_title("Проверка перед запуском", "⏳ идёт")
        + "\n\n"
        + "Проверено аккаунтов: "
        + f"<b>{checked_accounts}/{total_accounts}</b>\n"
        + "Проверено направлений аккаунт→чат: "
        + f"<b>{checked_pairs}/{total_pairs}</b>\n\n"
        + "Сейчас сервис только читает состояние Telegram: доступ к чату, право писать и "
        + "необходимость вступления. Сообщения не отправляются, а вступления не выполняются "
        + "до отдельного подтверждения запуска."
    )


def campaign_preflight_text(summary: CampaignPreflightSummary) -> str:
    if summary.launchable_units:
        next_step = (
            "Свежий результат можно использовать сразу: подтвердите запуск готовых "
            "направлений кнопкой ниже. Повторной Telegram-проверки не будет — перед "
            "очередью сервис сверит только срок актуальности результата."
        )
    elif summary.deferred_pairs:
        next_step = (
            "Сейчас отправлять небезопасно. Можно вернуться к настройкам или поставить "
            "рассылку на ближайшее безопасное время кнопкой ниже."
        )
    else:
        next_step = (
            "Сейчас запускать нечего: исправьте доступ к чатам или аккаунтам и выполните "
            "проверку заново."
        )
    return (
        screen_title("Готовность рассылки", _campaign_preflight_state(summary))
        + "\n\n"
        + _campaign_preflight_details(summary)
        + "\n\n"
        + section("Зачем нужна эта проверка")
        + "\n"
        + "Telegram может изменить доступ, право писать или защитную паузу в любой "
        "момент. Поэтому перед живым запуском выполняется одна проверка без публикаций "
        "и вступлений. После подтверждения повторного Telegram-прогона нет: сервис "
        "использует этот свежий результат и сверяет только его актуальность."
        + "\n\n"
        + section("Что дальше")
        + "\n"
        + next_step
    )


def campaign_launch_not_ready_text(
    summary: CampaignPreflightSummary,
    *,
    planning_reason: str | None = None,
) -> str:
    """Explain an automatic LIVE preflight without exposing a technical alert."""

    if planning_reason is not None:
        reason = planning_reason
        next_step = (
            "Измените срок active-рассылки, её частоту, аудиторию или пул отправителей, "
            "затем выполните проверку и подтвердите запуск снова."
        )
    elif summary.deferred_pairs:
        reason = (
            "Доступ к чатам подтверждён, но Telegram временно ограничил отправку "
            "выбранных аккаунтов."
        )
        next_step = (
            "Дождитесь указанного времени, не сбрасывая FloodWait, либо выберите аккаунты, "
            "которые уже готовы к отправке. Затем просто нажмите «Запустить» снова."
        )
    else:
        reason = "Ни один готовый маршрут до выбранных чатов сейчас не найден."
        next_step = (
            "Откройте «Проверить доступ без запуска», посмотрите состояние чатов и аккаунтов, "
            "исправьте доступ или выберите другой чат, затем выполните проверку и подтвердите "
            "запуск снова."
        )
    return (
        screen_title(
            "Отправка пока не началась",
            "Проверка перед запуском выполнена автоматически; ничего не отправлено.",
        )
        + "\n\n"
        + section("Что произошло")
        + f"\n{reason} "
        "Очередь не создана, чтобы не тратить аккаунты на заведомо недоступную отправку."
        + "\n\n"
        + section("Результат проверки")
        + "\n"
        + _campaign_preflight_details(summary)
        + "\n\n"
        + section("Что делать")
        + f"\n{next_step}"
    )


def dashboard_text(
    stats: DashboardStats,
    *,
    dry_run: bool,
    require_proxy: bool = True,
    worker_health: tuple[WorkerHealthSnapshot, ...] = (),
    worker_campaign_scope: frozenset[int] | None = None,
) -> str:
    mode = (
        "Тестовый режим · сообщения не отправляются"
        if dry_run
        else "Рабочий режим · реальные отправки"
    )
    next_steps: list[str] = []
    usable_accounts = stats.accounts_active if dry_run else stats.accounts_ready
    if usable_accounts == 0:
        next_steps.append(
            "👥 Добавьте активный аккаунт"
            if dry_run
            else "👥 Импортируйте аккаунт и проверьте сессию"
        )
    if not dry_run and require_proxy and stats.proxies_healthy == 0:
        next_steps.append("🌐 Добавьте и проверьте прокси")
    elif not dry_run and require_proxy and stats.accounts_ready > stats.accounts_with_healthy_proxy:
        missing = stats.accounts_ready - stats.accounts_with_healthy_proxy
        next_steps.append(f"🌐 Назначьте рабочий прокси ещё {missing} аккаунт(ам)")
    if stats.standalone_targets_approved + stats.audience_enabled_count == 0:
        next_steps.append("🎯 Добавьте и включите хотя бы один чат")
    campaign_active_runs = stats.queued_runs + stats.running_runs
    queued_work = (
        stats.queued_runs
        + stats.direct_queued_tasks
        + stats.subscriber_invite_queued_tasks
        + stats.campaign_reaction_queued_tasks
        + stats.channel_comment_queued_runs
        + stats.participant_reaction_queued_tasks
    )
    running_work = (
        stats.running_runs
        + stats.direct_running_tasks
        + stats.subscriber_invite_running_tasks
        + stats.campaign_reaction_running_tasks
        + stats.channel_comment_running_runs
        + stats.participant_reaction_running_tasks
    )
    active_launches = (
        stats.active_launches
        + stats.direct_active_launches
        + stats.subscriber_invite_active_launches
    )
    automated_comments = stats.channel_comment_queued_runs + stats.channel_comment_running_runs
    automated_participant_reactions = (
        stats.participant_reaction_queued_tasks + stats.participant_reaction_running_tasks
    )
    next_send = (
        f"<code>{_date_time(stats.next_scheduled_at)}</code>"
        if stats.next_scheduled_at is not None
        else "<i>не запланирована</i>"
    )
    if next_steps:
        operation_state = "🟡 Нужно закончить настройку"
    elif running_work:
        operation_state = "🔵 Идёт отправка"
    elif queued_work:
        operation_state = "🕒 Отправки стоят в очереди"
    else:
        operation_state = "🟢 Всё готово к отправке"
    manual_total = stats.standalone_targets_total
    manual_active = stats.standalone_targets_approved
    audience_active = manual_active + stats.audience_enabled_count
    audience_total = manual_total + stats.audience_member_count
    account_ready = stats.accounts_active if dry_run else stats.accounts_ready
    proxy_line = ""
    if require_proxy and not dry_run:
        proxy_line = (
            f"\n🌐 Маршруты: <b>{stats.accounts_with_healthy_proxy}/{stats.accounts_ready}</b>"
            f" · прокси <b>{stats.proxies_healthy}/{stats.proxies_total}</b>"
        )
    next_action_block = (
        "\n\n<b>Что сделать сейчас</b>\n" + "\n".join(f"• {item}" for item in next_steps)
        if next_steps
        else "\n\n<b>Следующий шаг</b>\n• 🚀 Создайте рассылку или откройте текущий запуск"
    )
    worker_block = _worker_health_block(worker_health)
    worker_scope_block = _worker_campaign_scope_block(worker_campaign_scope)
    return (
        screen_title("PROXIMA", "Операционный центр") + "\n"
        f"<code>{mode}</code>\n\n"
        f"<b>{operation_state}</b>\n\n"
        f"{section('Сейчас')}\n"
        f"🚀 Активных запусков: <b>{active_launches}</b>\n"
        f"📨 Всего в очереди: <b>{queued_work}</b> · выполняется: <b>{running_work}</b>\n"
        f"📣 Чаты: <b>{campaign_active_runs}</b> · реакции: "
        f"<b>{stats.campaign_reaction_queued_tasks + stats.campaign_reaction_running_tasks}</b>\n"
        f"💬 ЛС: <b>{stats.direct_queued_tasks}</b> в очереди · "
        f"<b>{stats.direct_running_tasks}</b> выполняется\n"
        f"✉️ Приглашения: <b>{stats.subscriber_invite_queued_tasks}</b> в очереди · "
        f"<b>{stats.subscriber_invite_running_tasks}</b> выполняется\n"
        f"⚙️ Автоматизация: комментарии <b>{automated_comments}</b> "
        f"· реакции участников <b>{automated_participant_reactions}</b>\n"
        f"⏭ Ближайший пост в чате: {next_send}\n\n"
        f"{section('Сегодня')}\n"
        f"✅ Публикаций в чатах: <b>{stats.sent_messages_24h}</b>"
        f" · ❌ не отправилось: <b>{stats.failed_messages_24h}</b>\n\n"
        f"{section('Готовность к работе')}\n"
        f"👥 Аккаунты: <b>{account_ready}/{stats.accounts_total}</b> готовы\n"
        f"🎯 Чаты: <b>{audience_active}/{audience_total}</b> включены"
        f" · списков: <b>{stats.target_lists_total}</b>"
        f"{proxy_line}"
        f"{next_action_block}"
        f"{worker_block}\n\n"
        f"{worker_scope_block}"
        f"{section('За всё время')}\n"
        f"✅ Завершено: <b>{stats.succeeded_runs}</b>"
        f" · ❌ проблем: <b>{stats.failed_runs}</b>"
    )


def _worker_health_block(worker_health: tuple[WorkerHealthSnapshot, ...]) -> str:
    if not worker_health:
        return "\n\n⚪ Фоновые сервисы: <i>выключены</i>"
    unhealthy = [item for item in worker_health if item.state != "running"]
    if not unhealthy:
        return f"\n\n🟢 Фоновые сервисы: <b>{len(worker_health)}/{len(worker_health)}</b> работают"
    lines = [
        "\n\n<b>⚠️ Фоновые сервисы деградировали</b>",
        f"Работают: <b>{len(worker_health) - len(unhealthy)}/{len(worker_health)}</b>",
    ]
    for item in unhealthy[:3]:
        error = escape(item.last_error or "нет ответа")
        state = {
            "degraded": "есть повторяющиеся сбои",
            "failed": "остановлен",
            "starting": "запускается",
        }.get(item.state, item.state)
        lines.append(f"• <code>{escape(item.name)}</code>: <b>{escape(state)}</b> · {error}")
    if len(unhealthy) > 3:
        lines.append(f"• ещё проблемных сервисов: <b>{len(unhealthy) - 3}</b>")
    return "\n".join(lines)


def _worker_campaign_scope_block(scope: frozenset[int] | None) -> str:
    if scope is None:
        return ""
    campaign_ids = ", ".join(f"#{campaign_id}" for campaign_id in sorted(scope))
    return (
        "<b>⚠️ Ограничение фонового worker</b>\n"
        f"Сейчас он обслуживает только: <code>{campaign_ids}</code>. "
        "Задания остальных рассылок останутся в очереди, пока ограничение не снимут "
        "и worker не перезапустят.\n\n"
    )


def guide_text() -> str:
    return (
        screen_title("Как начать", "Четыре понятных шага до первой рассылки и её контроля.")
        + "\n\n"
        "<b>01 — Подготовьте аккаунты</b>\n"
        "Импортируйте сессии, при необходимости назначьте прокси "
        "и проверьте, что аккаунты готовы.\n\n"
        "<b>02 — Соберите аудиторию</b>\n"
        "Создайте список и добавьте в него чаты. Сервис сам ведёт "
        "список неподходящих чатов отдельно.\n\n"
        "<b>03 — Настройте и проверьте рассылку</b>\n"
        "Выберите пост, чаты и частоту. Перед отправкой можно открыть "
        "предпросмотр и выполнить проверку.\n\n"
        "<b>04 — Контролируйте результат</b>\n"
        "В «Запусках и результатах» видны прогресс, причины пропусков и результат по "
        "каждому чату.\n\n"
        "<i>Каждый запуск можно безопасно остановить; сохранённые рассылки остаются "
        "доступны для повторной проверки и нового старта.</i>"
    )


def order_type_text() -> str:
    return (
        screen_title("Создание рассылки", "Выберите формат — дальше сервис проведёт по шагам.")
        + "\n\n"
        "<b>📢 Рассылка</b>\n"
        "Активная рассылка выбрана по умолчанию: сервис поддерживает позицию поста в чате. "
        "Один проход можно выбрать в черновике.\n\n"
        "<b>💬 Диалог</b>\n"
        "Несколько сообщений от разных аккаунтов с паузами и ответами друг на друга.\n\n"
        "<i>Сначала вы всё увидите в черновике. Отправка начнётся только после отдельного "
        "подтверждения.</i>"
    )


def order_draft_text(
    data: dict[str, object],
    *,
    launch_confirmation: bool = False,
    dry_run: bool = False,
) -> str:
    mode = str(data.get("order_mode", "broadcast"))
    name = escape(str(data.get("order_name") or "Без названия"))
    audience = escape(str(data.get("audience_label") or "не выбрана"))
    if launch_confirmation:
        launch_mode = "Тестовый режим · сообщения не отправятся" if dry_run else "Рабочая отправка"
        lines = [
            "<b>🔎 Проверить готовность?</b>",
            f"<code>{launch_mode}</code>\n",
            f"<b>{'📢' if mode == 'broadcast' else '💬'} {name}</b>",
        ]
    else:
        lines = [
            f"<b>{'📢' if mode == 'broadcast' else '💬'} {name}</b>",
            f"<code>{'РАССЫЛКА' if mode == 'broadcast' else 'ДИАЛОГ'} · НАСТРОЙКА</code>\n",
        ]
    if launch_confirmation:
        lines.append(
            "<i>Сначала бот покажет, какая часть аудитории доступна. "
            "Постановка в очередь потребует отдельного подтверждения.</i>"
        )
    has_audience = data.get("target_id") is not None or data.get("target_list_id") is not None
    if not has_audience:
        # Old saved drafts may only have a label.  Do not make their interface look
        # broken while the launch validation still performs the authoritative check.
        has_audience = bool(data.get("audience_label"))
    lines.append(f"{'✅' if has_audience else '⚠️'} <b>Чаты:</b> {audience}")
    if mode == "broadcast":
        sender_labels = [str(item) for item in data.get("sender_labels", [])]
        sender_text = ", ".join(escape(item) for item in sender_labels[:5])
        if len(sender_labels) > 5:
            sender_text += f" и ещё {len(sender_labels) - 5}"
        primary_text = str(data.get("primary_text") or "").strip()
        media_kind = str(data.get("primary_media_kind") or "")
        media_label = {
            "photo": "фото",
            "video": "видео",
            "document": "документ",
            "voice": "голосовое",
            "video_note": "видеокружок",
        }.get(media_kind)
        has_post_content = bool(primary_text or str(data.get("primary_media_path") or "").strip())
        missing: list[str] = []
        if not has_audience:
            missing.append("чаты")
        if not sender_labels:
            missing.append("отправители")
        if not has_post_content:
            missing.append("пост")
        if not launch_confirmation:
            if missing:
                lines.extend(
                    [
                        f"<b>🟡 До запуска осталось: {escape(', '.join(missing))}</b>",
                        "<i>Нажмите на пункт с ⚠️ в клавиатуре ниже — черновик сохранится.</i>\n",
                    ]
                )
            else:
                lines.extend(
                    [
                        "<b>🟢 Черновик готов к проверке</b>",
                        "<i>Проверьте параметры и подтвердите запуск отдельной кнопкой.</i>\n",
                    ]
                )
        lines.extend(
            [
                f"{'✅' if sender_labels else '⚠️'} <b>Отправители:</b> "
                f"<b>{sender_text or 'не выбраны'}</b>",
                f"{'✅' if has_post_content else '⚠️'} <b>Пост</b>"
                f"{f' · {media_label}' if media_label else ''}: "
                + (
                    f"<blockquote>{_preview(primary_text, 240)}</blockquote>"
                    if primary_text
                    else "<i>без подписи — будет отправлено только вложение</i>"
                    if has_post_content
                    else "<b>не задан</b>"
                ),
            ]
        )
        if bool(data.get("smart_timer_mode", mode == "broadcast")):
            default_profile = BROADCAST_PROFILES["active"]
            profile = BROADCAST_PROFILES.get(str(data.get("broadcast_profile", "active")))
            activity_aware = bool(data.get("activity_aware", True))
            visibility_depth = int(
                data.get(
                    "min_messages_between_posts",
                    default_profile.min_messages_between_posts,
                )
            )
            visibility_wait = int(
                data.get(
                    "visibility_max_wait_seconds",
                    default_profile.visibility_max_wait_seconds,
                )
            )
            check_interval = int(
                data.get(
                    "activity_check_interval_seconds",
                    default_profile.activity_check_interval_seconds,
                )
            )
            no_timer_interval_minutes = int(
                data.get(
                    "smart_no_timer_interval_minutes",
                    default_profile.interval_minutes,
                )
            )
            lines.extend(
                [
                    "",
                    "<b>Режим отправки</b>",
                    (
                        f"{profile.icon} Профиль: <b>{profile.title}</b> · "
                        f"{escape(profile.description)}"
                        if profile is not None
                        else "🛠 Профиль: <b>ручная настройка</b>"
                    ),
                    (
                        "♾ Повторять автоматически до ручной остановки"
                        if bool(data.get("repeat_forever", False))
                        else "♻️ Активная рассылка · повторять до конца срока"
                    ),
                    "• с таймером — сразу после разрешённого времени",
                    "• без таймера — стартовый ритм <b>"
                    f"{no_timer_interval_minutes} мин.</b>, затем свой интервал для каждого чата",
                    (
                        "• срок работы — <b>до ручной остановки</b>"
                        if bool(data.get("repeat_forever", False))
                        else (
                            f"• срок работы — <b>{int(data.get('smart_duration_hours', 24))} ч.</b>"
                        )
                    ),
                    (
                        "• пост вернётся после <b>"
                        f"{visibility_depth} новых сообщений</b>; "
                        f"{_visibility_wait_text(visibility_wait)}"
                        if activity_aware
                        else "• адаптивная видимость — <b>выключена</b>"
                    ),
                    (
                        f"• проверка позиции поста — каждые <b>{_duration_text(check_interval)}</b>"
                        if activity_aware
                        else "• проверка позиции поста не выполняется"
                    ),
                    (
                        f"• аккаунты — <b>{len(sender_labels)} независимых потоков "
                        "на каждый чат</b>; "
                        "отправка одного не останавливает остальные"
                        if str(data.get("sender_mode") or "distributed") == "independent"
                        else f"• умный выбор — <b>{len(sender_labels)} аккаунтов</b>; "
                        "для каждого чата учитываются доступ, лимит и ближайшее разрешённое "
                        "время; автозамена "
                        "<b>"
                        f"{'включена' if data.get('auto_sender_failover', True) else 'выключена'}"
                        "</b>"
                    ),
                ]
            )
        else:
            spread = int(data.get("spread_hours", 0))
            lines.extend(
                [
                    "",
                    "<b>Как будет работать</b>",
                    (
                        f"1️⃣ По сообщению от каждого из {len(sender_labels)} аккаунтов в каждый чат"
                        if str(data.get("sender_mode") or "distributed") == "independent"
                        else "1️⃣ По одному сообщению в каждый чат"
                    ),
                    f"• отправки: <b>{'сразу' if spread == 0 else f'за {spread} ч.'}</b>",
                ]
            )
    else:
        steps = [dict(item) for item in data.get("dialogue_steps", []) if isinstance(item, dict)]
        account_labels = {
            int(account_id): str(label)
            for account_id, label in dict(data.get("dialogue_account_labels", {})).items()
        }
        lines.extend(
            [
                (
                    "<b>🟢 Черновик готов к проверке</b>\n"
                    if not launch_confirmation and len(steps) >= 2 and has_audience
                    else "<b>🟡 До запуска нужны чаты и цепочка минимум из 2 сообщений</b>\n"
                    if not launch_confirmation
                    else ""
                ),
                f"{'✅' if len(steps) >= 2 else '⚠️'} <b>Диалог:</b> {len(steps)} сообщений",
                "",
                "<b>Сообщения по порядку</b>",
            ]
        )
        for index, step in enumerate(steps[:8], start=1):
            delay = int(step.get("delay_seconds") or 0)
            timing = "старт" if index == 1 else f"+{delay} сек."
            account_id = int(step.get("account_id") or 0)
            account_label = escape(account_labels.get(account_id, f"аккаунт #{account_id}"))
            lines.append(
                f"• <b>{index} · {account_label}</b> · "
                f"{timing} · {_preview(str(step.get('text') or ''), 70)}"
            )
        if len(steps) > 8:
            lines.append(f"• ещё сообщений: {len(steps) - 8}")
        if bool(data.get("smart_timer_mode", False)):
            default_profile = BROADCAST_PROFILES["active"]
            profile = BROADCAST_PROFILES.get(str(data.get("broadcast_profile", "active")))
            duration_hours = int(data.get("smart_duration_hours", default_profile.duration_hours))
            interval_minutes = int(
                data.get(
                    "smart_no_timer_interval_minutes",
                    default_profile.interval_minutes,
                )
            )
            activity_depth = int(
                data.get(
                    "min_messages_between_posts",
                    default_profile.min_messages_between_posts,
                )
            )
            lines.extend(
                [
                    "",
                    "<b>Режим отправки</b>",
                    (
                        f"{profile.icon} Профиль: <b>{profile.title}</b> · "
                        f"{escape(profile.description)}"
                        if profile is not None
                        else "🛠 Профиль: <b>ручная настройка</b>"
                    ),
                    "♻️ Повторяется вся цепочка, а не отдельные реплики",
                    "• новая цепочка начинается только после завершения предыдущей",
                    "• если сообщение сервиса осталось последним — повтор ждёт",
                    "• каждый аккаунт и чат проверяются перед своей репликой",
                    f"• срок работы — <b>{duration_hours} ч.</b>",
                    f"• без таймера — базовый интервал <b>{interval_minutes} мин.</b>",
                    f"• ориентир активности — <b>{activity_depth} новых сообщений</b>",
                ]
            )
        else:
            lines.extend(["", "1️⃣ <b>Один проход цепочки по каждому выбранному чату</b>"])
        lines.append(
            "<i>Перед каждой репликой проверяются таймер, лимит и доступность её аккаунта.</i>"
        )
    if launch_confirmation:
        lines.extend(
            [
                "",
                *_order_launch_volume_lines(data),
                "",
                (
                    "<b>После подтверждения сервис сначала проверит аккаунты и чаты.</b>"
                    if not dry_run
                    else "<b>После подтверждения сервис сначала выполнит проверку.</b>"
                ),
                "• проверяются сессии, прокси, членство и права отправки",
                "• сообщения попадут в очередь только после успешной проверки",
                "<i>Вернитесь к параметрам, если объём или настройки выглядят неверно.</i>",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "<i>Нажмите на нужный пункт ниже, чтобы изменить его. "
                "Всё сохраняется в черновике.</i>",
            ]
        )
    return "\n".join(lines)


def _order_launch_volume_lines(data: dict[str, object]) -> list[str]:
    """Describe the exact first pass before a potentially unbounded smart schedule."""

    mode = str(data.get("order_mode", "broadcast"))
    target_count = max(0, int(data.get("audience_target_count") or 0))
    smart = bool(data.get("smart_timer_mode", False))
    lines = ["<b>Объём отправки</b>", f"• чатов — <b>{target_count}</b>"]
    if mode == "broadcast":
        sender_count = len(data.get("sender_account_ids", []))
        independent = str(data.get("sender_mode") or "distributed") == "independent"
        initial_publications = target_count * sender_count if independent else target_count
        lines.append(f"• публикаций в первом проходе — <b>{initial_publications}</b>")
        if independent:
            lines.append(f"• расчёт — {target_count} чатов × {sender_count} аккаунтов")
        else:
            lines.append(f"• {sender_count} аккаунтов разделят чаты между собой")
    else:
        step_count = len(data.get("dialogue_steps", []))
        lines.extend(
            [
                f"• диалогов в первом проходе — <b>{target_count}</b>",
                f"• сообщений в первом проходе — <b>{target_count * step_count}</b>",
            ]
        )
    if smart:
        lines.append(
            "• затем возможны автоматические повторы; итог зависит от таймеров и активности чатов"
        )
    else:
        lines.append("• автоматических повторов не будет")
    return lines


def order_advanced_text(data: dict[str, object]) -> str:
    default_profile = BROADCAST_PROFILES["active"]
    profile = BROADCAST_PROFILES.get(str(data.get("broadcast_profile", "active")))
    activity_aware = bool(data.get("activity_aware", True))
    depth = int(data.get("min_messages_between_posts", default_profile.min_messages_between_posts))
    check_seconds = int(
        data.get("activity_check_interval_seconds", default_profile.activity_check_interval_seconds)
    )
    maximum_wait_seconds = int(
        data.get("visibility_max_wait_seconds", default_profile.visibility_max_wait_seconds)
    )
    no_timer_minutes = int(
        data.get("smart_no_timer_interval_minutes", default_profile.interval_minutes)
    )
    duration_hours = int(data.get("smart_duration_hours", default_profile.duration_hours))
    profile_label = (
        f"{profile.icon} {profile.title}" if profile is not None else "🛠 Ручная настройка"
    )
    activity_label = "включено" if activity_aware else "выключено"
    return (
        "<b>⚙️ Частота и условия</b>\n"
        "<i>Это тонкая настройка. Для обычной работы достаточно выбрать профиль выше.</i>\n\n"
        f"Профиль: <b>{profile_label}</b>\n"
        f"Работать: <b>{duration_hours} ч.</b>\n"
        f"Без таймера чата: <b>раз в {no_timer_minutes} мин.</b>\n\n"
        "<b>Когда повторять пост</b>\n"
        f"Подстраиваться к активности: <b>{activity_label}</b>\n"
        f"Ориентир: <b>{depth} новых сообщений после нашего поста</b>\n"
        f"Проверять чат: <b>раз в {_duration_text(check_seconds)}</b>\n"
        f"Ожидание при низкой активности: <b>{_visibility_wait_text(maximum_wait_seconds)}</b>\n\n"
        "<i>Сервис учитывает все новые сообщения, кроме публикаций, отправленных им самим; "
        "он не пытается угадывать, является ли чужое сообщение рекламой. Если публикация "
        "сервиса остаётся последней в чате, повтор не отправится. Время ожидания не отменяет "
        "это правило.</i>"
    )


def order_schedule_mode_text(data: dict[str, object]) -> str:
    """Explain the two delivery modes before the operator makes a choice."""

    mode = str(data.get("order_mode", "broadcast"))
    duration_hours = max(1, int(data.get("smart_duration_hours", 24)))
    if mode == "dialogue":
        active = (
            "Активный режим повторяет всю цепочку сообщений, только после завершения "
            "предыдущей цепочки в конкретном чате."
        )
    else:
        active = (
            "Активный режим поддерживает позицию поста: после публикации он ждёт новые "
            "сообщения в конкретном чате и затем планирует следующий пост."
        )
    return (
        "<b>♻️ Режим запуска</b>\n\n"
        "<b>1️⃣ Один проход</b> — один раз отправить по аудитории и завершить запуск.\n\n"
        f"<b>♻️ Активная рассылка</b> — {active} По умолчанию она работает "
        f"<b>{duration_hours} ч.</b>; срок и условие повтора можно изменить на следующем "
        "экране.\n\n"
        "<i>Следующий пост никогда не будет добавлен, пока предыдущая публикация сервиса "
        "остаётся последней. Остановить работающий запуск можно из его карточки.</i>"
    )


def audiences_text(stats: DashboardStats) -> str:
    if stats.target_lists_total + stats.standalone_targets_total == 0:
        next_step = "📥 Импортируйте TXT-список или добавьте один чат вручную."
    elif stats.audience_enabled_count + stats.standalone_targets_approved == 0:
        next_step = "▶️ Откройте список или чат и включите подходящую аудиторию."
    else:
        next_step = "🚀 Аудитория готова: её можно выбрать при создании рассылки."
    return (
        screen_title(
            "Чаты и списки",
            "Собирайте чаты в списки, проверяйте их состояние и используйте в рассылках.",
        )
        + "\n\n"
        f"{section('Следующий шаг')}\n"
        f"{next_step}\n\n"
        f"{section('Аудитория')}\n"
        f"📚 Списков чатов: <b>{stats.target_lists_total}</b>"
        f" · включено/всего: <b>{stats.audience_enabled_count}/"
        f"{stats.audience_member_count}</b>\n"
        f"📌 Отдельных чатов: <b>{stats.standalone_targets_total}</b>"
        f" · включено: <b>{stats.standalone_targets_approved}</b>\n"
        f"🚫 Исключено сервисом: <b>{stats.bad_targets}</b>\n\n"
        "Чаты, загруженные из TXT или Telegram-папки, находятся только внутри своего "
        "списка и не засоряют раздел отдельных чатов и выбор аудитории.\n\n"
        "<i>Для рассылки можно выбрать готовый список целиком или отметить несколько "
        "отдельных чатов галочками.</i>"
    )


_ACCOUNT_SOURCE_LABELS = {
    "none": "метаданные",
    "tdata": "tdata",
    "session_file": ".session Telethon/Pyrogram",
    "session_string": "строка сессии / JSON",
}


def _account_source(account: Account) -> str:
    source = getattr(account, "session_source", None)
    return source.value if source is not None else "none"


def _account_daily_load(
    account: Account,
    daily_limit: int,
) -> tuple[int, int | None, datetime | None]:
    limit = max(0, int(daily_limit))
    window_start = getattr(account, "day_window_start", None)
    if window_start is None:
        used = 0
        reset_at = None
    else:
        normalized_start = _as_utc(window_start)
        reset_at = normalized_start + timedelta(hours=24)
        if reset_at <= datetime.now(UTC):
            used = 0
            reset_at = None
        else:
            used = max(0, int(getattr(account, "sent_today", 0) or 0))
    remaining = max(0, limit - used) if limit > 0 else None
    return used, remaining, reset_at


def _profile_missing_parts(account: Account) -> tuple[str, ...]:
    """Return the profile fields that are still meaningful to prepare."""

    missing: list[str] = []
    if not (getattr(account, "first_name", None) or "").strip():
        missing.append("имя")
    if not getattr(account, "has_profile_photo", False):
        missing.append("аватар")
    if not (getattr(account, "about", None) or "").strip():
        missing.append("описание")
    return tuple(missing)


def accounts_text(
    page: Page[Account],
    *,
    filter_label: str | None = None,
    group_id: int | None = None,
) -> str:
    live = sum(_account_source(account) != "none" for account in page.items)
    daily_limit = get_settings().account_daily_limit
    scope = filter_label or "Все аккаунты"
    if group_id == 0:
        scope += " · без группы"
    elif group_id is not None:
        scope += " · выбранная группа"
    lines = [
        screen_title(
            "Аккаунты",
            f"{scope} · найдено {page.total} · сессий на странице {live}",
        ),
        "",
    ]
    for account in page.items:
        status = {"active": "🟢", "paused": "⏸", "disabled": "🔴"}[account.status.value]
        source_value = _account_source(account)
        source_label = _ACCOUNT_SOURCE_LABELS[source_value]
        if source_value == "none":
            session_note = "📄 без сессии, для рабочей отправки не используется"
        elif getattr(account, "authorized", False):
            session_note = f"🔓 сессия подтверждена · {source_label}"
        else:
            session_note = f"🟡 сессия загружена, нужна проверка · {source_label}"
        if getattr(account, "last_error", None):
            session_note += " · ⚠️ есть диагностика"
        used, remaining, _ = _account_daily_load(account, daily_limit)
        load_note = (
            f" · 📤 {used}/{daily_limit}, осталось {remaining}"
            if daily_limit > 0 and remaining is not None
            else " · 📤 без суточного лимита"
        )
        handle = f" · @{escape(account.username)}" if account.username else ""
        user_id = f" · <code>{account.telegram_user_id}</code>" if account.telegram_user_id else ""
        premium_value = getattr(account, "is_premium", None)
        premium_note = (
            " · ⭐ Premium"
            if premium_value is True
            else " · обычный"
            if premium_value is False
            else " · Premium не проверен"
        )
        lines.append(
            f"{status} <b>{escape(account.label)}</b>{handle}{user_id}{premium_note}\n"
            f"└ {session_note}{load_note}"
        )
        if getattr(account, "authorized", False):
            missing = _profile_missing_parts(account)
            profile_note = (
                f"🧩 профиль: нужно {', '.join(missing)}" if missing else "👤 профиль: оформлен"
            )
            lines.append(f"  {profile_note}")
    if not page.items:
        lines.append(
            "<b>С чего начать</b>\nИмпортируйте один аккаунт как <b>TData</b>, "
            "<b>.session Telethon/Pyrogram</b>, <b>.json</b> или строку сессии — либо "
            "загрузите <b>ZIP с несколькими .session</b>."
        )
    else:
        lines.append(
            "\n<b>Что можно сделать</b>\n"
            "Откройте аккаунт, чтобы проверить сессию, назначить прокси, сменить IP, "
            "отредактировать профиль Telegram или распределить его по своим группам."
        )
    return "\n".join(lines)


def account_bulk_check_text(summary: AccountBulkCheckSummary) -> str:
    lines = [
        screen_title(
            "Быстрая проверка сессий",
            "Проверяет подключение через маршрут аккаунта; @SpamBot не запрашивается.",
        ),
        "",
        section("Итог"),
        f"Проверено: <b>{len(summary.entries)}</b>",
        f"✅ Готовы к работе: <b>{len(summary.ready)}</b>",
        f"🔴 Сессия недействительна: <b>{len(summary.session_revoked)}</b>",
        f"⚠️ Нужна повторная проверка: <b>{len(summary.issues)}</b>",
    ]
    if not summary.entries:
        lines.append("\nНет аккаунтов с загруженной Telegram-сессией.")
        return "\n".join(lines)
    if summary.session_revoked:
        lines.extend(["\n" + section("Недействительные сессии")])
        for entry in summary.session_revoked[:12]:
            lines.append(f"• 🔴 <b>{escape(entry.label)}</b>")
        if len(summary.session_revoked) > 12:
            lines.append(f"• …и ещё <b>{len(summary.session_revoked) - 12}</b> в списке аккаунтов")
        lines.append(
            "\nМожно удалить ненужную запись кнопкой ниже. Для аккаунтов с историей "
            "бот предложит безопасно стереть только доступ."
        )
    if summary.issues:
        lines.extend(["\n" + section("Не удалять автоматически")])
        for entry in summary.issues[:6]:
            reason = escape(entry.error or "состояние сессии требует повторной проверки")
            lines.append(f"• ⚠️ <b>{escape(entry.label)}</b> · {reason}")
        if len(summary.issues) > 6:
            lines.append(f"• …и ещё <b>{len(summary.issues) - 6}</b>")
        lines.append("Эти случаи обычно связаны с сетью, прокси или данными импорта.")
    return "\n".join(lines)


def account_bulk_check_progress_text(*, completed: int, total: int) -> str:
    return (
        screen_title("Быстрая проверка сессий", "⏳ идёт")
        + "\n\n"
        + f"Проверено аккаунтов: <b>{completed}/{total}</b>\n\n"
        + "Каждая сессия проверяется через её назначенный маршрут. Сообщения не отправляются, "
        + "@SpamBot не запрашивается, а ошибки связи и прокси не приводят к удалению аккаунта."
    )


def account_bulk_spambot_progress_text(*, completed: int, total: int) -> str:
    return (
        screen_title("Проверка @SpamBot", "⏳ идёт")
        + "\n\n"
        + f"Проверено аккаунтов: <b>{completed}/{total}</b>\n\n"
        + "Проверяется только ответ @SpamBot через маршрут каждого аккаунта. "
        + "Сессии отдельно не тестируются, сообщения не отправляются и вступления в чаты "
        + "не выполняются."
    )


def account_bulk_spambot_text(summary: SpamBotBulkCheckSummary) -> str:
    lines = [
        screen_title("Проверка @SpamBot", "завершена"),
        "",
        section("Итог"),
        f"Проверено через @SpamBot: <b>{summary.checked}</b>",
        f"✅ Без ограничений: <b>{summary.count(SpamBotStatus.FREE)}</b>",
        f"⏳ Временное ограничение: <b>{summary.count(SpamBotStatus.TEMPORARY_RESTRICTION)}</b>",
        f"⛔ Активное без даты: <b>{summary.count(SpamBotStatus.ACTIVE_RESTRICTION)}</b>",
        f"⛔ Постоянное: <b>{summary.count(SpamBotStatus.PERMANENT_RESTRICTION)}</b>",
        f"⚠️ Ограничение с неясной формулировкой: "
        f"<b>{summary.count(SpamBotStatus.RESTRICTION_UNCLEAR)}</b>",
        f"❔ Неоднозначный ответ: <b>{summary.count(SpamBotStatus.UNKNOWN)}</b>",
        f"🌐 Нет ответа из-за связи/прокси: <b>{len(summary.transport_errors)}</b>",
    ]
    if summary.skipped_without_session:
        lines.append(
            f"⏭ Без сохранённой сессии, не запрашивались: <b>{summary.skipped_without_session}</b>"
        )
    if not summary.entries:
        lines.append("\nНет аккаунтов с загруженной Telegram-сессией.")
    if summary.transport_errors:
        lines.extend(["\n" + section("Связь не установлена")])
        for entry in summary.transport_errors[:8]:
            signal = escape(entry.parser_signal or "проверка не завершена")
            lines.append(f"• ⚠️ <b>{escape(entry.label)}</b> · <code>{signal}</code>")
        if len(summary.transport_errors) > 8:
            lines.append(f"• …и ещё <b>{len(summary.transport_errors) - 8}</b>")
        lines.append("Это не бан: проверьте маршрут/прокси и повторите только эту проверку позже.")
    lines.append(
        "\n<i>Проверка не отправляет сообщений и не вступает в чаты. Ограниченный аккаунт "
        "остается на защитной паузе до свободного ответа @SpamBot.</i>"
    )
    return "\n".join(lines)


def account_bulk_cleanup_text(summary: AccountInvalidSessionCleanupSummary) -> str:
    """Render a precise outcome for the destructive bulk-session action."""
    checks = summary.check_summary
    lines = [
        screen_title(
            "Очистка невалидных сессий",
            "Перед удалением Telegram-сессии были проверены заново; сообщения не отправлялись.",
        ),
        "",
        section("Итог повторной проверки"),
        f"Проверено: <b>{len(checks.entries)}</b>",
        f"Telegram подтвердил недействительный доступ: <b>{len(checks.session_revoked)}</b>",
        f"Полностью удалено: <b>{len(summary.deleted)}</b>",
        f"Доступ стёрт, запись оставлена для истории: <b>{len(summary.preserved_for_history)}</b>",
        f"Не тронуто из-за активной работы или гонки данных: <b>{len(summary.skipped)}</b>",
        f"Неопределённых ошибок (не удалялись): <b>{len(checks.issues)}</b>",
    ]
    if not checks.session_revoked:
        lines.append("\n✅ Сейчас нет сессий, для которых Telegram подтвердил потерю доступа.")
    if summary.skipped:
        lines.extend(["\n" + section("Оставлены без изменений")])
        for item in summary.skipped[:8]:
            lines.append(f"• ⚠️ <b>{escape(item.label)}</b> · {escape(item.reason)}")
        if len(summary.skipped) > 8:
            lines.append(f"• …и ещё <b>{len(summary.skipped) - 8}</b>")
    if checks.issues:
        lines.append(
            "\nОшибки сети, прокси, flood и неопределённые статусы не являются причиной удаления."
        )
    return "\n".join(lines)


def account_details_text(
    account: Account,
    *,
    proxy_slot: str | None,
    proxy_view: ProxyView | None = None,
    shared_proxy_accounts: int = 0,
    daily_limit: int | None = None,
) -> str:
    status_names = {"active": "🟢 Активен", "paused": "⏸ На паузе", "disabled": "🔴 Отключён"}
    source = _ACCOUNT_SOURCE_LABELS[_account_source(account)]
    lines = [
        screen_title(escape(account.label), status_names[account.status.value]),
        "",
        section("Подключение"),
        f"Источник сессии: <b>{source}</b>",
    ]
    if _account_source(account) != "none":
        auth = "🔓 авторизован" if getattr(account, "authorized", False) else "🟡 не подтверждён"
        lines.append(f"Сессия: <b>{auth}</b>")
        full_name = " ".join(
            value
            for value in [
                getattr(account, "first_name", None),
                getattr(account, "last_name", None),
            ]
            if value
        )
        lines.append(
            f"Имя Telegram: <b>{escape(full_name)}</b>"
            if full_name
            else "Имя Telegram: <i>не загружено</i>"
        )
        lines.append(
            "Аватар Telegram: <b>установлен</b>"
            if getattr(account, "has_profile_photo", False)
            else "Аватар Telegram: <i>не установлен</i>"
        )
        about = getattr(account, "about", None)
        lines.append(
            f"Описание: <code>{escape(about)}</code>" if about else "Описание: <i>не задано</i>"
        )
    if account.telegram_user_id:
        lines.append(f"Telegram ID: <code>{account.telegram_user_id}</code>")
    if account.username:
        lines.append(f"Username: <b>@{escape(account.username)}</b>")
    if account.phone:
        lines.append(f"Телефон: <code>{escape(account.phone)}</code>")
    premium_value = getattr(account, "is_premium", None)
    if premium_value is True:
        lines.append("Telegram Premium: <b>⭐ есть</b>")
    elif premium_value is False:
        lines.append("Telegram Premium: <b>обычный аккаунт</b>")
    else:
        lines.append("Telegram Premium: <i>не определён — проверьте сессию</i>")
    if proxy_slot:
        proxy_status_names = {
            "unknown": "⚪️ нужна проверка",
            "healthy": "🟢 рабочий",
            "unhealthy": "🔴 не работает",
            "quarantined": "🟠 не работает · карантин",
            "disabled": "⏸ выключен",
        }
        proxy_status = (
            proxy_status_names.get(proxy_view.proxy.status.value, "⚪️ нужна проверка")
            if proxy_view is not None
            else "⚪️ статус неизвестен"
        )
        lines.append(f"Прокси: <code>{escape(proxy_slot)}</code> · <b>{proxy_status}</b>")
        if proxy_view is not None and proxy_view.proxy.last_checked_at is not None:
            lines.append(
                f"Прокси проверен: <code>{_date_time(proxy_view.proxy.last_checked_at)}</code>"
            )
        if proxy_view is not None and proxy_view.proxy.status.value in {
            "unhealthy",
            "quarantined",
            "disabled",
        }:
            lines.append(
                "🔁 При следующем подключении автозамена проверит все включённые прокси "
                "и назначит рабочий."
            )
    else:
        lines.append("Прокси: <i>не назначен</i>")
    if shared_proxy_accounts > 1:
        lines.append(
            f"🔁 Общий IP: <b>{shared_proxy_accounts}</b> аккаунта(ов). "
            "Ротация переподключит их все."
        )
    if _account_source(account) == "none":
        next_step = (
            "📥 Следующий шаг: импортируйте сессию этого Telegram-аккаунта ещё раз. "
            "Карточка, группы и история запусков сохранятся."
        )
    elif not getattr(account, "authorized", False):
        next_step = "🔍 Следующий шаг: проверьте сессию. При ошибке соединения назначьте прокси."
    elif account.status.value == "paused" and getattr(account, "cooldown_reason", None) in {
        "spambot_temp_ban",
        "spambot_restriction_no_expiry",
        "spambot_restriction_unclear",
        "spambot_parse_unknown",
    }:
        next_step = (
            "🩺 Следующий шаг: это защитная пауза @SpamBot. Нажмите «Проверить сессию + "
            "@SpamBot»; после подтверждения свободного статуса аккаунт включится сам."
        )
    elif account.status.value == "paused" and getattr(account, "spambot_status", None) == "error":
        next_step = (
            "⚠️ Следующий шаг: @SpamBot сейчас недоступен, поэтому защитная пауза сохранена. "
            "Повторите проверку позже; вручную включать аккаунт не нужно."
        )
    elif get_settings().account_require_proxy and not proxy_slot:
        next_step = "🌐 Следующий шаг: назначьте рабочий прокси для этого аккаунта."
    elif proxy_view is not None and proxy_view.proxy.status.value in {
        "unhealthy",
        "quarantined",
        "disabled",
    }:
        next_step = (
            "🌐 Следующий шаг: добавьте хотя бы один рабочий прокси. При запуске "
            "автозамена проверит пул и переключит аккаунт сама."
        )
    elif account.status.value != "active":
        next_step = "▶️ Следующий шаг: включите аккаунт, чтобы использовать его в рассылках."
    elif missing_profile := _profile_missing_parts(account):
        next_step = (
            "🧩 Сессия подтверждена. Перед первой рассылкой оформите профиль: "
            + ", ".join(missing_profile)
            + "."
        )
    else:
        next_step = "✅ Аккаунт готов: его можно выбрать при создании рассылки."
    lines.append(f"\n{section('Следующий шаг')}\n{next_step}")
    resolved_daily_limit = (
        get_settings().account_daily_limit if daily_limit is None else max(0, daily_limit)
    )
    used, remaining, reset_at = _account_daily_load(account, resolved_daily_limit)
    lines.append("\n" + section("Нагрузка аккаунта"))
    if resolved_daily_limit > 0 and remaining is not None:
        lines.append(
            f"За 24 часа: <b>{used}/{resolved_daily_limit}</b> отправок · "
            f"осталось <b>{remaining}</b>"
        )
        if reset_at is not None:
            lines.append(f"Новое окно: <code>{_date_time(reset_at)}</code>")
        if remaining == 0:
            lines.append("⏸ <b>Лимит исчерпан:</b> новые задания автоматически отложены.")
    else:
        lines.append("За 24 часа: <b>лимит выключен</b>")
    if account.last_checked_at is not None:
        checked = _date_time(account.last_checked_at)
        lines.append(f"Последняя проверка: <code>{checked}</code>")
    if account.spambot_checked_at is not None:
        checked = _date_time(account.spambot_checked_at)
        status = {
            "free": "✅ ограничений нет",
            "temporary_restriction": "⏳ временное ограничение",
            "active_restriction": "⛔ активное ограничение без указанной даты",
            "permanent_restriction": "⛔ постоянное ограничение",
            "restriction_unclear": "⚠️ ограничение подтверждено, точный вид не распознан",
            "unknown": "❔ ответ пока не распознан",
            "error": "⚠️ проверка не завершена",
        }.get(account.spambot_status or "unknown", account.spambot_status or "unknown")
        lines.append(f"@SpamBot: <b>{escape(status)}</b> · <code>{checked}</code>")
        if account.spambot_parser_signal:
            lines.append(f"Распознавание: <code>{escape(account.spambot_parser_signal)}</code>")
        try:
            spambot_snapshot = json.loads(account.spambot_response_json or "{}")
        except (TypeError, ValueError):
            spambot_snapshot = {}
        if isinstance(spambot_snapshot, dict):
            check_error_code = str(spambot_snapshot.get("check_error_code") or "")
            if check_error_code:
                check_error_text = {
                    "proxy_unavailable": ("нет доступного подключения через прокси аккаунта"),
                    "account_operation_busy": "аккаунт занят другой операцией",
                }.get(check_error_code, "ответ Telegram не был получен")
                lines.append(
                    "⚠️ <b>Проверка @SpamBot не состоялась:</b> "
                    f"{check_error_text}. Статус ограничения не менялся."
                )
            previous_snapshot = spambot_snapshot.get("previous")
            observation = (
                previous_snapshot
                if check_error_code and isinstance(previous_snapshot, dict)
                else spambot_snapshot
            )
            restriction_scope = str(observation.get("restriction_scope") or "none")
            scope_text = {
                "direct_outreach": (
                    "Telegram запретил первые личные сообщения людям, у которых нет номера "
                    "аккаунта в контактах."
                ),
                "direct_outreach_and_invites": (
                    "Telegram запретил первые личные сообщения незнакомым и добавление людей "
                    "в группы/каналы."
                ),
                "unspecified": (
                    "Telegram подтвердил ограничение, но в ответе не указан его точный объём."
                ),
            }.get(restriction_scope)
            if scope_text:
                lines.append(f"<b>Что именно ограничено:</b> {scope_text}")
                if observation.get("allows_reply_to_inbound") is True:
                    lines.append(
                        "↩️ Telegram разрешает отвечать только после входящего сообщения. "
                        "Это не разрешение на автоматическую рассылку."
                    )
                lines.append(
                    "💡 Доступ к отдельному старому чату не считается разрешением на публикацию: "
                    "рассылки этим аккаунтом остаются на паузе до свободного статуса @SpamBot."
                )
            response_text = str(observation.get("text") or "")
            if response_text:
                visible_response = response_text[:1200]
                suffix = "\n…" if len(response_text) > len(visible_response) else ""
                response_label = "Последний успешный" if check_error_code else "Последний"
                lines.append(
                    f"<b>{response_label} ответ @SpamBot:</b>\n"
                    f"<pre>{escape(visible_response + suffix)}</pre>"
                )
            button_rows = observation.get("button_rows")
            if isinstance(button_rows, list) and button_rows:
                visible_buttons = "\n".join(
                    " | ".join(str(button) for button in row)
                    for row in button_rows
                    if isinstance(row, list)
                )[:500]
                if visible_buttons:
                    lines.append(f"Кнопки ответа:\n<pre>{escape(visible_buttons)}</pre>")
    if account.last_error:
        lines.append(
            f"\n❌ <b>Последняя диагностика</b>\n<code>{escape(account.last_error)}</code>"
        )
    return "\n".join(lines)


def account_name_confirmation_text(data: dict[str, object]) -> str:
    first_name = escape(str(data["profile_first_name"]))
    last_name = escape(str(data.get("profile_last_name") or ""))
    full_name = f"{first_name} {last_name}".strip()
    return (
        "<b>✏️ Изменить имя Telegram?</b>\n\n"
        f"Новое имя: <b>{full_name}</b>\n\n"
        "<i>Изменение будет видно всем пользователям Telegram.</i>"
    )


def account_name_catalog_text(
    accounts: list[Account],
    *,
    single_account: Account | None = None,
    scope_label: str | None = None,
) -> str:
    scope = (
        f"АККАУНТ #{single_account.id} · {escape(single_account.label)}"
        if single_account is not None
        else (
            f"ОБЛАСТЬ · {escape(scope_label)} · ДОСТУПНО {len(accounts)} АККАУНТОВ"
            if scope_label
            else f"МАССОВО · ДОСТУПНО {len(accounts)} АККАУНТОВ"
        )
    )
    return (
        "<b>🎲 Молодые имена Telegram</b>\n"
        f"<code>{scope}</code>\n\n"
        "Каталог собирает естественные разные профили: часть только с именем, часть с "
        "фамилией или инициалом. Русские формы согласованы по полу, одинаковые имена в одной "
        "партии не повторяются.\n\n"
        f"🇷🇺 Русский запас: <b>{profile_name_capacity('ru')}</b> вариантов\n"
        f"🌍 Латиницей: <b>{profile_name_capacity('en')}</b> вариантов\n"
        f"🎲 Смешанный: <b>{profile_name_capacity('mixed')}</b> вариантов\n\n"
        "<b>Выберите стиль списка.</b> Сначала откроется точное превью; без отдельного "
        "подтверждения ничего не изменится.\n\n"
        "<i>Важно: имя меняется в настоящем профиле Telegram даже при включённом тестовом режиме "
        "рассылок.</i>"
    )


def account_generated_names_preview_text(
    assignments: list[dict[str, object]],
    *,
    locale: NameLocale,
    scope_label: str | None = None,
) -> str:
    locale_label = {
        "mixed": "🎲 смешанные",
        "ru": "🇷🇺 русские",
        "en": "🌍 латиницей",
    }[locale]
    scope = f" · {escape(scope_label)}" if scope_label else ""
    lines = [
        "<b>👀 Проверка имён перед применением</b>",
        f"<code>{locale_label.upper()}{scope} · АККАУНТОВ {len(assignments)}</code>\n",
    ]
    for assignment in assignments[:12]:
        label = escape(str(assignment["label"]))
        name = escape(f"{assignment['first_name']} {assignment.get('last_name') or ''}".strip())
        lines.append(f"• <b>{label}</b> → {name}")
    if len(assignments) > 12:
        lines.append(f"• ещё аккаунтов: {len(assignments) - 12}")
    lines.extend(
        [
            "",
            "<b>После подтверждения:</b>",
            "• каждый аккаунт подключится через свой настроенный маршрут;",
            "• успешные изменения и ошибки будут показаны отдельно;",
            "• сбой одного аккаунта не остановит остальные.",
            "",
            "<i>Это реальное изменение публичных профилей Telegram.</i>",
        ]
    )
    return "\n".join(lines)


def account_profile_scope_text(
    groups: list[AccountGroupView],
    *,
    operation_label: str,
    ready_count: int,
) -> str:
    """Explain the exact account set a future bulk profile edit may use."""

    available_groups = [group for group in groups if group.ready_count > 0]
    group_hint = (
        "\n".join(
            f"• {('⭐' if group.is_system else '🗂')} <b>{escape(group.name)}</b> "
            f"· готово {group.ready_count} из {group.account_count}"
            for group in available_groups[:8]
        )
        if available_groups
        else "Подходящих групп пока нет."
    )
    more_groups = (
        f"\n• …ещё групп: {len(available_groups) - 8}" if len(available_groups) > 8 else ""
    )
    return (
        f"<b>🧭 Выберите область: {escape(operation_label)}</b>\n\n"
        "Изменения затронут только аккаунты с готовой Telegram-сессией на момент "
        "создания превью. Сначала будет показан точный список; без подтверждения "
        "профили Telegram не меняются.\n\n"
        f"<b>Все готовые:</b> {max(0, int(ready_count))}\n\n"
        "<b>Группы с готовыми аккаунтами</b>\n"
        f"{group_hint}{more_groups}"
    )


def account_names_result_text(
    *,
    updated_count: int,
    failures: list[tuple[str, str]],
) -> str:
    state = "✅ Готово" if not failures else "🟡 Завершено с ошибками"
    lines = [
        f"<b>{state}</b>",
        f"Имена обновлены: <b>{updated_count}</b> · ошибок: <b>{len(failures)}</b>",
    ]
    if failures:
        lines.append("\n<b>Не удалось изменить</b>")
        for label, error in failures[:5]:
            lines.append(f"• <b>{escape(label)}</b>: {_preview(error, 100)}")
        if len(failures) > 5:
            lines.append(f"• ещё ошибок: {len(failures) - 5}")
    return "\n".join(lines)


def account_avatar_text(account: Account) -> str:
    status = "🟢 установлен" if getattr(account, "has_profile_photo", False) else "⚪️ отсутствует"
    return (
        f"<b>🖼 Аватар Telegram · {escape(account.label)}</b>\n"
        f"<code>АККАУНТ #{account.id}</code>\n\n"
        f"Текущий аватар: <b>{status}</b>\n\n"
        "Новый аватар станет текущим в профиле Telegram. "
        "Удаление очистит всю историю профильных фотографий."
    )


def targets_text(page: Page[Target]) -> str:
    approved = sum(target.approved_by_operator for target in page.items)
    lines = [
        screen_title("Отдельные чаты", f"Добавлено вручную: {page.total} · включено: {approved}"),
        "",
    ]
    for target in page.items:
        status = (
            "🚫 исключён сервисом"
            if target.quality_status == "bad"
            else "🔴 ошибка"
            if target.last_error_code
            else "🟢 используется"
            if target.approved_by_operator
            else "⚪️ выключен"
        )
        lines.append(
            f"<b>{escape(target.title)}</b> · {status}\n└ <code>{escape(target.chat_ref)}</code>"
        )
    if not page.items:
        lines.append("<b>С чего начать</b>\nДобавьте нужный чат вручную или импортируйте список.")
    else:
        lines.append(
            "\n<b>Быстрые действия</b>\n"
            "Выключенные чаты не попадут в новые рассылки. Можно включить все доступные "
            "чаты одной кнопкой или открыть отдельный чат для точной настройки."
        )
    return "\n".join(lines)


def bad_targets_text(page: Page[Target]) -> str:
    lines = [
        screen_title("Исключённые чаты", f"Автоматически исключено: {page.total}"),
        "",
        "Сюда попадают чаты с повторными подтверждёнными проблемами. "
        "Сетевые ошибки, прокси и общий FLOOD_WAIT рейтинг чата не портят.\n",
    ]
    for target in page.items:
        lines.append(
            f"🚫 <b>{escape(target.title)}</b> · рейтинг <b>{target.quality_score}/100</b>\n"
            f"├ <code>{escape(target.chat_ref)}</code>\n"
            f"├ попыток {target.quality_attempts} · успешно {target.quality_successes} · "
            f"ошибок {target.quality_failures} · банов {target.quality_banned_accounts}\n"
            f"└ {escape(target.quality_reason or 'причина не сохранена')}"
        )
    if not page.items:
        lines.append("✅ Исключённых чатов пока нет.")
    lines.append(
        "\n<i>Новые задания в эти чаты не создаются. Откройте чат, чтобы посмотреть "
        "причину или вручную вернуть его в работу с чистой статистикой.</i>"
    )
    return "\n".join(lines)


def target_details_text(
    target: Target,
    timings: list[TargetTimingView] | None = None,
) -> str:
    status = (
        "🚫 Автоматически исключён"
        if target.quality_status == "bad"
        else "🟢 Используется"
        if target.approved_by_operator
        else "⚪️ Не используется"
    )
    lines = [
        screen_title(escape(target.title), status),
        "",
        f"Адрес: <code>{escape(target.chat_ref)}</code>",
    ]
    if target.chat_type:
        type_label = {
            "group": "группа",
            "supergroup": "супергруппа",
            "channel": "канал",
        }.get(target.chat_type, target.chat_type)
        lines.append(f"Тип: <b>{escape(type_label)}</b>")
    if target.manual_cooldown_seconds:
        minutes = target.manual_cooldown_seconds // 60
        value = (
            f"{minutes // 1440} д."
            if minutes >= 1440 and minutes % 1440 == 0
            else f"{minutes // 60} ч."
            if minutes >= 60 and minutes % 60 == 0
            else f"{minutes} мин."
        )
        lines.append(f"Интервал одного аккаунта: <b>{value}</b>")
    else:
        lines.append("Интервал одного аккаунта: <i>не задан</i>")
    if target.folder_slug:
        slug_preview = f"{target.folder_slug[:8]}…"
        lines.append(f"Источник: <b>папка Telegram</b> · <code>{escape(slug_preview)}</code>")
    if target.last_success_at is not None:
        lines.append(f"Последняя отправка: <code>{_date_time(target.last_success_at)}</code>")
    quality_label = {
        "bad": "🚫 плохой",
        "working": "🟢 рабочий",
        "learning": "🟡 изучается",
    }.get(target.quality_status, target.quality_status)
    lines.extend(
        [
            "\n" + section("Рейтинг чата"),
            f"Состояние: <b>{quality_label}</b> · балл <b>{target.quality_score}/100</b>",
            f"Попыток: <b>{target.quality_attempts}</b> · успешно: "
            f"<b>{target.quality_successes}</b> · целевых ошибок: "
            f"<b>{target.quality_failures}</b>",
            f"Забанено аккаунтов в чате: <b>{target.quality_banned_accounts}</b>",
        ]
    )
    if target.quality_reason:
        lines.append(f"Причина исключения: <b>{escape(target.quality_reason)}</b>")
    if target.quality_bad_at is not None:
        lines.append(f"Исключён: <code>{_date_time(target.quality_bad_at)}</code>")
    active_timings = [
        item
        for item in (timings or [])
        if (
            item.send_blocked
            or item.slowmode_seconds
            or item.next_send_at
            or item.last_success_at
            or item.adaptive_interval_seconds
        )
    ]
    if active_timings:
        lines.append("\n<b>Таймеры и доступ аккаунтов</b>")
        for item in active_timings[:6]:
            if item.send_blocked:
                blocked_at = (
                    f" · с <code>{_date_time(item.blocked_at)}</code>"
                    if item.blocked_at is not None
                    else ""
                )
                lines.append(
                    f"• ⛔ <b>{escape(item.account_label)}</b> "
                    f"<code>#{item.account_id}</code>: навсегда исключён для этого чата"
                    f"{blocked_at}"
                )
                continue
            interval = (
                f"slow mode {item.slowmode_seconds} сек."
                if item.slowmode_seconds
                else "без slow mode"
            )
            available = (
                f" · можно с <code>{_date_time(item.next_send_at)}</code>"
                if item.next_send_at is not None
                else ""
            )
            lines.append(
                f"• <b>{escape(item.account_label)}</b> <code>#{item.account_id}</code>: "
                f"{interval}{available}"
            )
            if item.adaptive_interval_seconds is not None:
                tier = {
                    "turbo": "турбо",
                    "fast": "быстро",
                    "steady": "средне",
                    "quiet": "тихо",
                }.get(item.adaptive_tier or "", "обучается")
                activity = (
                    f" · {item.activity_messages_per_minute:g} сообщ./мин"
                    if item.activity_messages_per_minute is not None
                    else ""
                )
                lines.append(
                    f"  └ авточастота: <b>{tier}</b> · "
                    f"{_duration_text(item.adaptive_interval_seconds)}{activity}"
                )
        if len(active_timings) > 6:
            lines.append(f"• ещё аккаунтов: {len(active_timings) - 6}")
        blocked_count = sum(item.send_blocked for item in active_timings)
        if blocked_count:
            lines.append(
                f"\n<i>Заблокированных пар: {blocked_count}. Софт не выберет их снова, "
                "пока вы вручную не сбросите память.</i>"
            )
    if target.last_error_code:
        account = (
            f" · аккаунт #{target.last_error_account_id}"
            if target.last_error_account_id is not None
            else ""
        )
        lines.append(
            f"\n❌ <b>{_target_error_label(target.last_error_code)}</b>{account}\n"
            f"<code>{escape(target.last_error or target.last_error_code)}</code>"
        )
        hint = failure_hint(target.last_error_code)
        if hint:
            lines.append(f"💡 {escape(hint)}")
    lines.append(
        "\n<i>Таймер, авточастота и память блокировок хранятся отдельно для каждого "
        "аккаунта в этом чате.</i>"
    )
    return "\n".join(lines)


def campaigns_text(
    page: Page[Campaign],
    *,
    worker_campaign_scope: frozenset[int] | None = None,
) -> str:
    lines = [
        screen_title(
            "Рассылки",
            f"Сохраните настройки один раз и запускайте, когда нужно · всего: {page.total}",
        ),
        "",
    ]
    if worker_campaign_scope is not None:
        campaign_ids = ", ".join(f"#{campaign_id}" for campaign_id in sorted(worker_campaign_scope))
        lines.extend(
            [
                "<b>⚠️ Важно: очередь сейчас ограничена</b>",
                f"Фоновый worker берёт только рассылки: <code>{campaign_ids}</code>.",
                "Другие запуски не начнутся, пока это ограничение не снимут и worker не "
                "перезапустят.",
                "",
            ]
        )
    for campaign in page.items:
        status = "🟢 включена" if campaign.enabled else "⏸ выключена"
        audience_name = escape(
            str(
                campaign.audience_label
                or (
                    f"чат #{campaign.target_id}"
                    if campaign.target_id is not None
                    else f"список #{campaign.target_list_id}"
                )
            )
        )
        target_source = (
            f"чат · {audience_name}"
            if campaign.target_id is not None
            else f"список · {audience_name}"
        )
        audience_count = getattr(campaign, "audience_target_count", None)
        if audience_count is not None:
            target_source += f" · {int(audience_count)} чатов"
        if campaign.mode == CampaignMode.BROADCAST:
            mode_label = "📢 Посты"
            if campaign.smart_timer_mode:
                duration = (
                    "до остановки"
                    if campaign.repeat_forever
                    else _duration_text(campaign.smart_duration_seconds)
                )
                timing = (
                    f"умная · {duration} · без таймера / "
                    f"{_duration_text(campaign.smart_no_timer_interval_seconds)}"
                )
            else:
                timing = (
                    f"на {campaign.spread_seconds // 3600} ч."
                    if campaign.spread_seconds >= 3600
                    else f"на {campaign.spread_seconds // 60} мин."
                    if campaign.spread_seconds >= 60
                    else "сразу"
                )
            if not campaign.smart_timer_mode and campaign.repeat_count > 1:
                timing += (
                    f" · {campaign.repeat_count} проходов / "
                    f"{_duration_text(campaign.repeat_interval_seconds)}"
                )
        else:
            mode_label = "💬 Диалог"
            try:
                dialogue_steps = parse_dialogue_steps(
                    campaign.dialogue_steps_json,
                    primary_account_id=campaign.primary_account_id,
                    primary_text=campaign.primary_text,
                    reply_account_id=campaign.reply_account_id,
                    reply_text=campaign.reply_text,
                    reply_delay_seconds=campaign.reply_delay_seconds,
                )
                timing = f"цепочка {len(dialogue_steps)} сообщ."
                if campaign.smart_timer_mode:
                    timing += (
                        f" · умная {_duration_text(campaign.smart_duration_seconds)}"
                        " · без таймера / "
                        f"{_duration_text(campaign.smart_no_timer_interval_seconds)}"
                    )
                else:
                    timing += " · один проход"
            except ValueError:
                timing = "ошибка цепочки"
        lines.append(
            f"{mode_label} · <b>{escape(campaign.name)}</b> · {status}\n"
            f"└ {target_source} · {timing}"
        )
    if not page.items:
        lines.append("<b>С чего начать</b>\nСоздайте первую рассылку или диалог кнопкой ниже.")
    else:
        lines.append(
            "\n<b>Что дальше</b>\nОткройте рассылку: там можно посмотреть пост, чаты, "
            "частоту, выполнить проверку и начать отправку."
        )
    return "\n".join(lines)


def campaign_details_text(campaign: Campaign, sender_ids: list[int]) -> str:
    audience_name = escape(
        str(
            campaign.audience_label
            or (
                f"чат #{campaign.target_id}"
                if campaign.target_id is not None
                else f"список #{campaign.target_list_id}"
            )
        )
    )
    target = (
        f"Чат: <b>{audience_name}</b>"
        if campaign.target_id is not None
        else f"Список чатов: <b>{audience_name}</b>"
    )
    audience_count = getattr(campaign, "audience_target_count", None)
    if audience_count is not None:
        target += f" · <b>{int(audience_count)}</b> чатов в подборке"
    status = "🟢 Готова" if campaign.enabled else "⏸ Остановлена"
    lines = [
        screen_title(escape(campaign.name), status),
        "",
        section("Настройки сценария"),
        f"🎯 {target}",
    ]
    lines.append(
        "<i>Изменения сценария применятся к будущим запускам. Уже поставленные в очередь "
        "посты сохраняют свой текст, аудиторию и пул отправителей.</i>"
    )
    if campaign.mode == CampaignMode.BROADCAST:
        variants = _campaign_content_variants(campaign)
        enabled_variants = sum(variant.enabled for variant in variants)
        album_items = sum(len(variant.media) for variant in variants)
        contact_items = sum(variant.contact is not None for variant in variants)
        accounts = ", ".join(f"#{account_id}" for account_id in sender_ids)
        lines.append(f"👥 Отправители: <code>{accounts}</code>")
        lines.append(
            f"📝 Посты: <b>{enabled_variants} активн. из {len(variants)} вариантов</b>"
            f" · медиа {album_items} · контактов {contact_items}"
        )
        lines.append(
            "🔔 Отметки участников: <b>включены</b> · 5–10 скрытых упоминаний · "
            f"без повтора {campaign.mention_cooldown_days} дн. в каждом чате"
            if campaign.hidden_mentions_enabled
            else (
                "🔔 Отметки участников: <b>выключены</b> · ротация "
                f"{campaign.mention_cooldown_days} дн."
            )
        )
        if len(variants) > 1:
            lines.append("Ротация: <b>по кругу отдельно для каждого чата</b>")
        if campaign.primary_media_kind:
            media_label = {
                "photo": "фото",
                "video": "видео",
                "document": "документ",
                "voice": "голосовое",
                "video_note": "видеокружок",
            }.get(campaign.primary_media_kind, campaign.primary_media_kind)
            size_mb = (campaign.primary_media_size or 0) / (1024 * 1024)
            lines.append(
                f"Медиа: <b>{escape(media_label)}</b> · "
                f"{escape(campaign.primary_media_name or 'файл')} · {size_mb:.1f} МБ"
            )
        if campaign.primary_html:
            lines.append("Форматирование: <b>сохранено</b>")
        if campaign.smart_timer_mode:
            profile = (
                detect_broadcast_profile(
                    duration_seconds=campaign.smart_duration_seconds,
                    interval_seconds=campaign.smart_no_timer_interval_seconds,
                    min_messages_between_posts=campaign.min_messages_between_posts,
                    activity_check_interval_seconds=(campaign.activity_check_interval_seconds),
                    visibility_max_wait_seconds=campaign.visibility_max_wait_seconds,
                )
                if campaign.activity_aware
                else None
            )
            lines.extend(
                [
                    "\n<b>Автоматическая отправка</b>",
                    (
                        f"Профиль: <b>{profile.icon} {profile.title}</b> · "
                        f"{escape(profile.description)}"
                        if profile is not None
                        else "Профиль: <b>🛠 ручная настройка</b>"
                    ),
                    (
                        f"Аккаунты: <b>{len(sender_ids)} работают независимо в каждом чате</b>"
                        if campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
                        else f"Пул: <b>{len(sender_ids)} аккаунтов</b> · "
                        "для каждого чата выбирается доступный аккаунт со свободным лимитом"
                    ),
                    (
                        "Срок активности: <b>до ручной остановки</b>"
                        if campaign.repeat_forever
                        else (
                            "Срок активности: <b>"
                            f"{_duration_text(campaign.smart_duration_seconds)}</b>"
                        )
                    ),
                    "Чаты с таймером: <b>сразу после окончания таймера</b>",
                    "Чаты без таймера: стартовый интервал "
                    f"<b>{_duration_text(campaign.smart_no_timer_interval_seconds)}</b>, "
                    "затем авточастота",
                    (
                        "Умная частота: отдельно для каждого аккаунта и чата · после "
                        f"<b>{campaign.min_messages_between_posts} новых сообщений</b> · "
                        "проверка раз в "
                        f"<b>{_duration_text(campaign.activity_check_interval_seconds)}</b> · "
                        f"{_visibility_wait_text(campaign.visibility_max_wait_seconds)}"
                        if campaign.activity_aware
                        else "Умная видимость: <b>выключена</b>"
                    ),
                    (
                        "Независимость: <b>один аккаунт не останавливает остальные</b>"
                        if campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
                        else (
                            "Автозамена аккаунта: <b>включена</b> · только выбранный пул"
                            if campaign.auto_sender_failover
                            else "Автозамена аккаунта: <b>выключена</b>"
                        )
                    ),
                ]
            )
        else:
            timing = (
                f"{campaign.spread_seconds // 3600} ч."
                if campaign.spread_seconds >= 3600
                else f"{campaign.spread_seconds // 60} мин."
                if campaign.spread_seconds >= 60
                else "сразу"
            )
            lines.extend(
                [
                    "\n<b>Разовая отправка</b>",
                    f"Окно отправки: <b>{timing}</b>",
                    (
                        "Умные повторы: <b>выключены · один проход</b>"
                        if campaign.repeat_count == 1
                        else (
                            f"Умные повторы: <b>{campaign.repeat_count} проходов</b> · "
                            f"не раньше чем через "
                            f"<b>{_duration_text(campaign.repeat_interval_seconds)}</b>"
                        )
                    ),
                ]
            )
        lines.append(
            f"\n<b>Ваш пост</b>\n"
            f"<blockquote>{_text_preview(campaign.primary_text, 1600)}</blockquote>"
        )
    else:
        try:
            steps = parse_dialogue_steps(
                campaign.dialogue_steps_json,
                primary_account_id=campaign.primary_account_id,
                primary_text=campaign.primary_text,
                reply_account_id=campaign.reply_account_id,
                reply_text=campaign.reply_text,
                reply_delay_seconds=campaign.reply_delay_seconds,
            )
        except ValueError:
            steps = []
        roles = " → ".join(f"#{step.account_id}" for step in steps)
        lines.extend(
            [
                f"💬 Диалог: <b>{len(steps)} сообщений</b>",
                f"👥 Аккаунты по порядку: <code>{roles or 'ошибка настройки'}</code>",
                "Таймеры: <b>проверяются отдельно перед каждым сообщением</b>",
            ]
        )
        if campaign.smart_timer_mode:
            profile = (
                detect_broadcast_profile(
                    duration_seconds=campaign.smart_duration_seconds,
                    interval_seconds=campaign.smart_no_timer_interval_seconds,
                    min_messages_between_posts=campaign.min_messages_between_posts,
                    activity_check_interval_seconds=campaign.activity_check_interval_seconds,
                    visibility_max_wait_seconds=campaign.visibility_max_wait_seconds,
                )
                if campaign.activity_aware
                else None
            )
            lines.extend(
                [
                    "\n<b>Автоматическое повторение диалогов</b>",
                    (
                        f"Профиль: <b>{profile.icon} {profile.title}</b> · "
                        f"{escape(profile.description)}"
                        if profile is not None
                        else "Профиль: <b>🛠 ручная настройка</b>"
                    ),
                    (
                        "Срок активности: <b>до ручной остановки</b>"
                        if campaign.repeat_forever
                        else (
                            "Срок активности: <b>"
                            f"{_duration_text(campaign.smart_duration_seconds)}</b>"
                        )
                    ),
                    "Следующий диалог: <b>только после завершения всей цепочки</b>",
                    "Чаты без таймера: базовый интервал "
                    f"<b>{_duration_text(campaign.smart_no_timer_interval_seconds)}</b>",
                    (
                        "Активность: ориентир "
                        f"<b>{campaign.min_messages_between_posts} новых сообщений</b> · "
                        f"проверка {_duration_text(campaign.activity_check_interval_seconds)} · "
                        f"максимум {_duration_text(campaign.visibility_max_wait_seconds)}"
                        if campaign.activity_aware
                        else "Активность чата: <b>выключена</b>"
                    ),
                    "Защита от дубля: <b>не повторять, пока ваш пост последний</b>",
                ]
            )
        else:
            lines.append("\n<b>Разовая отправка:</b> один проход цепочки")
        for index, step in enumerate(steps, start=1):
            delay = "старт" if index == 1 else f"пауза {step.delay_seconds} сек."
            media_label = {
                "photo": "фото",
                "video": "видео",
                "document": "документ",
                "voice": "голосовое",
                "video_note": "видеокружок",
            }.get(step.media_kind or "")
            preview = _text_preview(step.text, 180) if step.text else "без подписи"
            lines.append(
                f"\n<b>{index} · аккаунт #{step.account_id}</b> · {delay}"
                f"{' · ' + media_label if media_label else ''}\n"
                f"<blockquote>{preview}</blockquote>"
            )
    return "\n".join(lines)


def campaign_variants_text(
    campaign: Campaign,
    variants: list[BroadcastContentVariant],
) -> str:
    enabled = sum(variant.enabled for variant in variants)
    media_count = sum(len(variant.media) for variant in variants)
    contact_count = sum(variant.contact is not None for variant in variants)
    spintax_variants = sum(
        variant.contact is None and spintax_group_count(variant.text) > 0 for variant in variants
    )
    lines = [
        f"<b>🎨 Контент рассылки · {escape(campaign.name)}</b>",
        f"<code>СЦЕНАРИЙ #{campaign.id}</code>\n",
        f"Активно вариантов: <b>{enabled} из {len(variants)}</b>",
        f"Медиа в альбомах: <b>{media_count}</b>\n",
        f"Контактных публикаций: <b>{contact_count}</b>\n",
        "Spintax: "
        f"<b>{spintax_variants} из {len(variants)} вариантов</b> · "
        "<code>{Привет|Здравствуйте}</code>\n",
        "<b>Как это работает</b>",
        "Варианты идут по кругу для каждого чата: A → B → C → A. "
        "Выключенные варианты пропускаются. Текст, форматирование и альбом "
        "сохраняются в рассылке при её создании. Spintax выбирает одну фразу "
        "для каждой отправки; повтор задачи сохранит тот же результат.\n",
    ]
    for index, variant in enumerate(variants, start=1):
        state = "🟢" if variant.enabled else "⚪️"
        if variant.contact is not None:
            lines.append(
                f"{state} <b>{index}. {escape(variant.name)}</b> · 📇 контакт\n"
                f"└ {escape(variant.contact.display_name)} · "
                f"<code>{escape(variant.contact.phone_number)}</code>"
            )
        else:
            media = f" · {len(variant.media)} медиа" if variant.media else ""
            lines.append(
                f"{state} <b>{index}. {escape(variant.name)}</b>{media}\n"
                f"└ {_preview(variant.text, 110)}"
            )
    lines.append("\n<i>Нажмите вариант, чтобы открыть альбом или отправить себе предпросмотр.</i>")
    return "\n".join(lines)


def campaign_advanced_text(campaign: Campaign) -> str:
    profile = (
        detect_broadcast_profile(
            duration_seconds=campaign.smart_duration_seconds,
            interval_seconds=campaign.smart_no_timer_interval_seconds,
            min_messages_between_posts=campaign.min_messages_between_posts,
            activity_check_interval_seconds=campaign.activity_check_interval_seconds,
            visibility_max_wait_seconds=campaign.visibility_max_wait_seconds,
        )
        if campaign.activity_aware
        else None
    )

    profile_label = (
        f"{profile.icon} {profile.title}" if profile is not None else "🛠 Ручная настройка"
    )
    mode_label = (
        "умная диалоговая рассылка" if campaign.mode == CampaignMode.DIALOGUE else "умная рассылка"
    )
    account_line = (
        "Цепочка: <b>фиксированные аккаунты, без подмены ролей</b>"
        if campaign.mode == CampaignMode.DIALOGUE
        else "Автозамена аккаунта: "
        f"<b>{'включена' if campaign.auto_sender_failover else 'выключена'}</b>"
    )
    return (
        f"<b>⚙️ Точная настройка · {escape(campaign.name)}</b>\n"
        f"<code>СЦЕНАРИЙ #{campaign.id}</code>\n\n"
        f"Режим: <b>{mode_label}</b>\n"
        f"Профиль: <b>{profile_label}</b>\n"
        f"Срок работы: <b>{_duration_text(campaign.smart_duration_seconds)}</b>\n"
        "Базовый интервал без Telegram-таймера: "
        f"<b>{_duration_text(campaign.smart_no_timer_interval_seconds)}</b>\n\n"
        f"Адаптивная видимость: <b>{'включена' if campaign.activity_aware else 'выключена'}</b>\n"
        f"Глубина: <b>{campaign.min_messages_between_posts} новых сообщений</b>\n"
        "Проверка позиции: каждые "
        f"<b>{_duration_text(campaign.activity_check_interval_seconds)}</b>\n"
        "Условие без тайм-аута: "
        f"<b>{_visibility_wait_text(campaign.visibility_max_wait_seconds)}</b>\n"
        f"{account_line}\n\n"
        "<i>Повтор разрешается при достижении глубины или по выбранному тайм-ауту, "
        "но не выполняется, пока сообщение сервиса остаётся последним. Telegram slow mode, "
        "ручной интервал чата, FloodWait и лимиты аккаунта всегда имеют приоритет.</i>"
    )


def campaign_reactions_text(
    campaign: Campaign,
    step_count: int,
    *,
    sender_count: int | None = None,
) -> str:
    place = (
        "к каждому посту рассылки"
        if campaign.mode == CampaignMode.BROADCAST
        else f"к сообщению № {campaign.reaction_dialogue_step} из {step_count}"
    )
    state = "включены" if campaign.reactions_enabled and campaign.reaction_count else "выключены"
    lines = [
        f"<b>👍 Реакции на публикации · {escape(campaign.name)}</b>\n"
        f"<code>СЦЕНАРИЙ #{campaign.id}</code>",
        "",
        f"Статус: <b>{state}</b>",
        f"Реакция: <b>{campaign.reaction_emoji}</b>",
        f"Сколько: <b>{campaign.reaction_count}</b>",
        f"Куда: <b>{place}</b>",
    ]
    if campaign.mode == CampaignMode.BROADCAST:
        if sender_count is not None:
            maximum_reactors = max(0, sender_count - 1)
            lines.append(f"Максимум из выбранного пула: <b>{maximum_reactors}</b> реакций")
            if campaign.reactions_enabled and campaign.reaction_count > maximum_reactors:
                reason = (
                    "нужен второй готовый аккаунт"
                    if maximum_reactors == 0
                    else "автор поста не ставит реакцию, а каждый другой аккаунт — только одну"
                )
                lines.extend(
                    [
                        "",
                        "⚠️ <b>Реакций в очереди будет меньше настройки:</b> " + reason + ".",
                    ]
                )
    else:
        try:
            steps = parse_dialogue_steps(
                campaign.dialogue_steps_json,
                primary_account_id=campaign.primary_account_id,
                primary_text=campaign.primary_text,
                reply_account_id=campaign.reply_account_id,
                reply_text=campaign.reply_text,
                reply_delay_seconds=campaign.reply_delay_seconds,
            )
        except ValueError:
            steps = []
        selected_index = int(campaign.reaction_dialogue_step) - 1
        maximum_reactors = 0
        if 0 <= selected_index < len(steps):
            source_account_id = int(steps[selected_index].account_id)
            maximum_reactors = len(
                {
                    int(step.account_id)
                    for step in steps
                    if int(step.account_id) != source_account_id
                }
            )
        lines.append(f"Максимум по составу цепочки: <b>{maximum_reactors}</b> реакций")
        if campaign.reactions_enabled and campaign.reaction_count > maximum_reactors:
            lines.extend(
                [
                    "",
                    "⚠️ <b>Реакций будет меньше настройки:</b> автор исключается, а один "
                    "аккаунт ставит не больше одной реакции.",
                ]
            )
    lines.extend(
        [
            "",
            "<i>После успешной отправки ставятся обычные реакции выбранными аккаунтами "
            "сценария, кроме автора самого сообщения. Они идут отдельной очередью с "
            "короткой разной паузой; публикация и её отчёт от этого не ждут. "
            "Если Telegram не принимает выбранный emoji, сервис безопасно попробует другой "
            "из списка 👍, 🔥, ❤️, ⚡ и покажет фактический результат в отчёте. "
            "Платные реакции не используются.</i>",
        ]
    )
    return "\n".join(lines)


def campaign_variant_text(
    campaign: Campaign,
    variant: BroadcastContentVariant,
) -> str:
    state = "🟢 участвует в ротации" if variant.enabled else "⚪️ выключен"
    if variant.contact is not None:
        return "\n".join(
            [
                f"<b>📇 {escape(variant.name)}</b>",
                f"<code>СЦЕНАРИЙ #{campaign.id} · ВАРИАНТ {escape(variant.variant_id)}</code>",
                f"Состояние: <b>{state}</b>",
                "Тип публикации: <b>контакт Telegram</b>\n",
                "<b>Карточка контакта</b>",
                f"Имя: <b>{escape(variant.contact.display_name)}</b>",
                f"Телефон: <code>{escape(variant.contact.phone_number)}</code>\n",
                "<i>Контакт отправляется как нативная карточка Telegram. Предпросмотр "
                "приходит только вам и ничего не публикует в чатах.</i>",
            ]
        )
    spintax_groups = spintax_group_count(variant.text)
    lines = [
        f"<b>🎨 {escape(variant.name)}</b>",
        f"<code>СЦЕНАРИЙ #{campaign.id} · ВАРИАНТ {escape(variant.variant_id)}</code>",
        f"Состояние: <b>{state}</b>",
        f"Текст: <b>{len(variant.text)} символов</b>",
        "Spintax: "
        f"<b>{spintax_groups} блоков</b>"
        + (" · при отправке выбирается один вариант" if spintax_groups else ""),
        f"Форматирование: <b>{'сохранено' if variant.html_text else 'обычный текст'}</b>",
        f"Альбом: <b>{len(variant.media)} из 10 файлов</b>\n",
        "<b>Текст публикации</b>",
        _preview(variant.text, 900),
    ]
    if variant.media:
        labels = {
            "photo": "фото",
            "video": "видео",
            "document": "документ",
            "voice": "голосовое",
            "video_note": "видеокружок",
        }
        lines.append("\n<b>Медиа</b>")
        for index, item in enumerate(variant.media, start=1):
            size_mb = item.size / (1024 * 1024)
            lines.append(
                f"{index}. {labels.get(item.kind, item.kind)} · "
                f"{escape(item.name)} · {size_mb:.1f} МБ"
            )
    lines.append(
        "\n<i>Spintax: <code>{Привет|Здравствуйте}</code>. Вложенные блоки поддерживаются; "
        "буквенные скобки экранируйте как <code>\\{</code> и <code>\\}</code>. "
        "Предпросмотр приходит только вам и ничего не публикует в чатах.</i>"
    )
    return "\n".join(lines)


def target_lists_text(page: Page[TargetListView]) -> str:
    first_item = page.page * page.page_size + 1 if page.total else 0
    last_item = min(page.total, (page.page + 1) * page.page_size)
    lines = [
        screen_title("Списки чатов"),
        f"<i>Всего: {page.total} · страница {page.page + 1}/{page.pages} · "
        f"показано: {first_item}–{last_item}</i>\n",
    ]
    for item in page.items:
        lines.append(
            f"<b>#{item.id} {escape(item.name)}</b>\n"
            f"├ всего {item.item_count} · 🟢 {item.approved_count} · "
            f"⚪️ {item.disabled_count} · 🚫 {item.bad_count}\n"
            f"└ проблем сейчас: {item.problem_count}"
        )
    if not page.items:
        lines.append("<b>С чего начать</b>\nЗагрузите UTF-8 TXT — один чат на строку.")
    else:
        lines.append(
            "\n<i>Откройте список, чтобы отфильтровать проблемные чаты, найти нужный "
            "и включить доступные одной кнопкой.</i>"
        )
    return "\n".join(lines)


def target_list_details_text(item: TargetListView) -> str:
    return (
        screen_title(escape(item.name), f"Список #{item.id}") + "\n\n"
        f"Всего       <b>{item.item_count}</b>\n"
        f"🟢 Открыто   <b>{item.approved_count}</b>\n"
        f"⚪️ Закрыто   <b>{item.disabled_count}</b>\n"
        f"🚫 Плохих    <b>{item.bad_count}</b>\n"
        f"🔴 С ошибкой <b>{item.problem_count}</b>\n\n"
        "<i>В рассылку попадут только включённые чаты. Эта настройка действует только "
        "внутри выбранного списка и не затрагивает другие списки.</i>"
    )


def target_list_targets_text(
    item: TargetListView,
    page: Page[Target],
    *,
    filter_name: str = "all",
) -> str:
    filter_label = {
        "all": "все чаты",
        "enabled": "используются",
        "disabled": "выключены",
        "bad": "исключённые",
        "problem": "с ошибками",
    }.get(filter_name, "все чаты")
    first_item = page.page * page.page_size + 1 if page.total else 0
    last_item = min(page.total, (page.page + 1) * page.page_size)
    lines = [
        screen_title(escape(item.name), "Чаты списка"),
        f"<code>ФИЛЬТР: {filter_label.upper()}</code>\n"
        f"Всего: <b>{page.total}</b> · Страница <b>{page.page + 1}/{page.pages}</b> · "
        f"показано <b>{first_item}–{last_item}</b>\n",
    ]
    for target in page.items:
        status = (
            "🚫 плохой · исключён"
            if target.quality_status == "bad"
            else "🔴 ошибка"
            if target.last_error_code
            else "🟢 используется"
            if target.approved_by_operator
            else "⚪️ выключен"
        )
        lines.append(
            f"<b>{escape(target.title)}</b> · {status}\n└ <code>{escape(target.chat_ref)}</code>"
        )
    if not page.items:
        lines.append("В этой базе больше нет чатов.")
    lines.append(
        "\n<i>Нажмите чат для подробностей. Ниже можно найти его по названию, "
        "username или ID и массово изменить видимую страницу.</i>"
    )
    return "\n".join(lines)


def templates_text(page: Page[MessageTemplate]) -> str:
    lines = [screen_title("Шаблоны сообщений", f"Всего: {page.total}"), ""]
    for template in page.items:
        status = "🟢" if template.enabled else "⏸"
        reply_preview = _preview(template.reply_text) if template.reply_text else "<i>не задан</i>"
        lines.append(
            f"{status} <b>#{template.id} {escape(template.name)}</b>\n"
            f"├ Первое: {_preview(template.primary_text)}\n"
            f"└ Ответ: {reply_preview}"
        )
    if not page.items:
        lines.append(
            "<b>С чего начать</b>\n"
            "Шаблонов нет. Можно создать готовую пару сообщений или ввести текст "
            "прямо при создании рассылки."
        )
    else:
        lines.append(
            "\n<i>Для рассылки используется первое сообщение, для диалога — первое и ответ. "
            "Изменение шаблона не меняет уже созданные рассылки.</i>"
        )
    return "\n".join(lines)


def template_details_text(template: MessageTemplate) -> str:
    status = "🟢 Активен" if template.enabled else "⏸ На паузе"
    reply_text = _text_preview(template.reply_text, 1400) if template.reply_text else "не задан"
    return (
        screen_title(escape(template.name), f"Шаблон #{template.id} · {status}") + "\n\n"
        f"<b>1 · Первое сообщение</b>\n"
        f"<blockquote>{_text_preview(template.primary_text, 1400)}</blockquote>\n"
        f"<b>2 · Адресный ответ</b>\n"
        f"<blockquote>{reply_text}</blockquote>\n"
        "<i>Созданные рассылки и диалоги хранят собственную копию этих текстов.</i>"
    )


def template_confirmation_text(data: dict[str, object]) -> str:
    reply_text = (
        _text_preview(str(data["reply_text"]), 1400) if data.get("reply_text") else "не задан"
    )
    return (
        "<b>✅ Проверка перед сохранением</b>\n"
        "<code>НОВЫЙ ШАБЛОН</code>\n\n"
        f"Название: <b>{escape(str(data['name']))}</b>\n\n"
        f"<b>1 · Первое сообщение</b>\n"
        f"<blockquote>{_text_preview(str(data['primary_text']), 1400)}</blockquote>\n"
        f"<b>2 · Адресный ответ</b>\n"
        f"<blockquote>{reply_text}</blockquote>"
    )


def launches_text(
    page: Page[LaunchView],
    *,
    archived: bool = False,
) -> str:
    title = "Архив результатов" if archived else "Работа и результаты"
    subtitle = (
        "Здесь лежат завершённые рассылки. Их можно вернуть в основной список."
        if archived
        else "Откройте рассылку, чтобы увидеть, что отправлено, что ждёт и где есть проблемы."
    )
    lines = [screen_title(title, subtitle), f"<i>Всего: {page.total}</i>\n"]
    for launch in page.items:
        icon = _launch_icon(launch)
        name = _preview(launch.campaign_name, 64)
        if launch.smart_timer_mode:
            next_send = _date_time(launch.next_scheduled_for)
            active_until = (
                "до ручной остановки" if launch.repeat_forever else _date_time(launch.active_until)
            )
            problem = f" · ❌ {launch.failed}" if launch.failed else ""
            lines.append(
                f"{icon} <b>{name}</b>\n"
                f"├ опубликовано <b>{launch.sent_publications}</b> · "
                f"в работе <b>{launch.active}</b>"
                f"{problem}\n"
                f"└ следующий пост: <code>{next_send}</code> · {active_until}"
            )
            continue
        mode = "рассылка" if launch.mode == CampaignMode.BROADCAST else "диалог"
        percent = _percent(launch.finished, launch.total)
        lines.append(
            f"{icon} <b>{name}</b> · {mode}\n"
            f"├ готово <b>{launch.finished}/{launch.total}</b> · {percent}% · "
            f"✅ {launch.succeeded} · ❌ {launch.failed}\n"
            f"└ сейчас {launch.active} · пропущено/остановлено {launch.cancelled}"
        )
    if not page.items:
        lines.append(
            "Архив пуст. Завершённую рассылку можно убрать сюда из её отчёта."
            if archived
            else "Пока ничего не отправляется. Создайте рассылку или откройте сохранённую."
        )
    lines.append("\n<i>Одна карточка — одна начатая отправка по выбранным чатам.</i>")
    return "\n".join(lines)


def launch_details_text(
    launch: LaunchView,
    runs: list[CampaignRun],
    targets: dict[int, Target] | None = None,
) -> str:
    targets = targets or {}
    if launch.smart_timer_mode:
        return _smart_launch_details_text(launch, runs, targets)
    account_labels = dict(launch.account_labels)
    cycle_total = max((run.cycle_index for run in runs), default=0) + 1
    mode = "📢 Рассылка" if launch.mode == CampaignMode.BROADCAST else "💬 Диалог"
    state = _launch_state_label(launch)
    target_total = launch.target_count or len({run.target_id for run in runs})
    covered_targets = len(
        {
            run.target_id
            for run in runs
            if run.status == RunStatus.SUCCEEDED
            or run.primary_message_id is not None
            or run.reply_message_id is not None
        }
    )
    preflight_skipped = _preflight_skipped_by_target(runs)
    lines = [
        f"<b>📊 {_preview(launch.campaign_name, 80)}</b>",
        f"<code>{mode.upper()} · РЕЗУЛЬТАТЫ</code>{' · 🗄 АРХИВ' if launch.archived else ''}\n",
        f"Статус: <b>{state}</b>\n",
        "<b>Итог</b>",
        f"<code>{_progress_bar(launch.finished, launch.total)}</code> "
        f"<b>{launch.finished}/{launch.total}</b> отправок обработано",
        f"✅ Опубликовано <b>{launch.succeeded}</b> · ❌ ошибок <b>{launch.failed}</b> · "
        f"🚫 пропущено/остановлено <b>{launch.cancelled}</b>",
        f"🔵 Сейчас <b>{launch.running}</b> · 🕒 ждёт <b>{launch.queued}</b>",
        f"🎯 Чатов с публикацией: <b>{covered_targets}/{target_total}</b>",
        "",
        "<b>Время работы</b>",
        f"🚀 Начато: <code>{_date_time(launch.created_at)}</code>",
        f"🗓 Окно: <code>{_date_time(launch.first_scheduled_for)}</code> → "
        f"<code>{_date_time(launch.last_scheduled_for)}</code>",
    ]
    if cycle_total > 1:
        lines.append(f"🔁 Проходов: <b>{cycle_total}</b>")
    if (
        launch.mode == CampaignMode.BROADCAST
        and launch.sender_mode == BroadcastSenderMode.DISTRIBUTED
    ):
        lines.append(
            "🤖 Отправители назначены по доступу к чату, свободному лимиту "
            "и ближайшему разрешённому времени"
        )
    if preflight_skipped:
        lines.append(
            f"⏭ Не готовы при проверке: <b>{len(preflight_skipped)}</b> чатов; остальные запущены"
        )
    lines.extend(_variant_delivery_lines(runs))
    lines.extend(_reaction_task_lines(launch))

    failed = [run for run in runs if run.status == RunStatus.FAILED]
    if failed or launch.attempt_failures or preflight_skipped:
        lines.append("\n<b>⚠️ Требует внимания</b>")
        if preflight_skipped:
            lines.append(f"• Пропущено без попытки отправки: <b>{len(preflight_skipped)}</b> чатов")
            for target_id, skipped_run in list(preflight_skipped.items())[:3]:
                target = targets.get(target_id)
                title = _target_title(target, target_id, 42)
                lines.append(
                    f"⏭ <b>{title}</b>\n└ {_preflight_skip_reason(skipped_run.error_code)}"
                )
            if len(preflight_skipped) > 3:
                lines.append(f"• ещё пропущено: {len(preflight_skipped) - 3}")
        if failed:
            lines.append(f"• Не отправились после повторов: <b>{len(failed)}</b>")
        if launch.attempt_failures:
            lines.append(f"• Зафиксировано сбоев попыток: <b>{len(launch.attempt_failures)}</b>")
        for run in failed[-3:]:
            error = _failure_preview(run.error_code, run.error, 120)
            target = targets.get(run.target_id)
            title = _target_title(target, run.target_id, 42)
            cycle = f" · проход {run.cycle_index + 1}/{cycle_total}" if cycle_total > 1 else ""
            lines.append(f"❌ <b>{title}</b>{cycle}\n└ {error}")
        if len(failed) > 3:
            lines.append(f"• ещё ошибок: {len(failed) - 3}")
        for action in reversed(launch.attempt_failures[-2:]):
            target = targets.get(action.target_id)
            title = _target_title(target, action.target_id, 38)
            account = _compact_account_label(action.account_id, account_labels, 24)
            lines.append(
                f"🔄 <b>{title}</b> · {account}\n"
                f"└ {_preview(action.error or 'неизвестная ошибка', 100)}"
            )

    lines.append("\n<b>Последние публикации</b>")
    if launch.publications:
        for action in reversed(launch.publications[-6:]):
            target = targets.get(action.target_id)
            title = _target_title(target, action.target_id, 42)
            sender = _compact_account_label(action.account_id, account_labels)
            link = _telegram_message_link(target, action.external_message_id)
            opened = f' · <a href="{link}">открыть ↗</a>' if link else ""
            relation = ""
            if launch.mode == CampaignMode.DIALOGUE:
                relation = (
                    f" · ответ на #{action.reply_to_message_id}"
                    if action.reply_to_message_id is not None
                    else " · начало цепочки"
                )
            lines.append(
                f"• <code>{_date_time(action.created_at)}</code> · <b>{title}</b> · "
                f"{sender}{relation}{opened}"
            )
        if len(launch.publications) > 6:
            lines.append(f"• более ранних сообщений: {len(launch.publications) - 6}")
    else:
        succeeded = sorted(
            (run for run in runs if run.status == RunStatus.SUCCEEDED),
            key=lambda item: (item.finished_at or item.updated_at, item.id),
            reverse=True,
        )
        for run in succeeded[:5]:
            target = targets.get(run.target_id)
            title = _target_title(target, run.target_id, 42)
            link = _telegram_message_link(target, run.primary_message_id)
            opened = f' · <a href="{link}">сообщение</a>' if link else ""
            cycle = f" · проход {run.cycle_index + 1}/{cycle_total}" if cycle_total > 1 else ""
            lines.append(
                f"✅ {title} · <code>{_date_time(run.finished_at or run.updated_at)}</code>"
                f"{cycle}{opened}"
            )
        if not succeeded:
            lines.append("<i>Успешных отправок пока нет.</i>")

    pending = sorted(
        (run for run in runs if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}),
        key=lambda item: (item.scheduled_for, item.id),
    )
    if pending:
        lines.append("\n<b>Что будет дальше</b>")
        for run in pending[:5]:
            target = targets.get(run.target_id)
            title = _target_title(target, run.target_id, 42)
            marker = "🔵" if run.status == RunStatus.RUNNING else "🕒"
            cycle = f" · проход {run.cycle_index + 1}/{cycle_total}" if cycle_total > 1 else ""
            lines.append(
                f"{marker} <b>{title}</b> · <code>{_date_time(run.scheduled_for)}</code> "
                f"· {_schedule_distance(run.scheduled_for)}{cycle}"
            )
        if len(pending) > 5:
            lines.append(f"• ещё в очереди: {len(pending) - 5}")
    lines.extend(_hidden_mention_delivery_lines(runs))
    return _report_text(lines)


def _smart_launch_details_text(
    launch: LaunchView,
    runs: list[CampaignRun],
    targets: dict[int, Target],
) -> str:
    account_labels = dict(launch.account_labels)
    is_dialogue = launch.mode == CampaignMode.DIALOGUE
    independent = not is_dialogue and launch.sender_mode == BroadcastSenderMode.INDEPENDENT
    stream_key = lambda run: (  # noqa: E731
        (run.target_id, run.sender_account_id) if independent else (run.target_id, None)
    )
    all_streams = {stream_key(run) for run in runs}
    stream_count = len(all_streams) or _expected_stream_count(launch)
    covered_streams = {stream_key(run) for run in runs if run.primary_message_id is not None}
    active_runs = [run for run in runs if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}]
    active_streams = {stream_key(run) for run in active_runs}
    due_now = sum(
        run.status == RunStatus.QUEUED and _is_due(run.scheduled_for) for run in active_runs
    )
    blocked_timings = [timing for timing in launch.timings if timing.send_blocked]
    preflight_skipped = _preflight_skipped_by_target(runs)
    learned_timings = [
        timing
        for timing in launch.timings
        if not timing.send_blocked and timing.adaptive_interval_seconds is not None
    ]
    state = _launch_state_label(launch, blocked_count=len(blocked_timings))
    next_send = (
        f"<code>{_date_time(launch.next_scheduled_for)}</code> · "
        f"{_schedule_distance(launch.next_scheduled_for)}"
        if launch.next_scheduled_for is not None
        else "<b>нет активных отправок</b>"
    )
    settings_runs = active_runs or runs
    latest_settings = settings_runs[-1] if settings_runs else None
    activity_aware = bool(latest_settings is not None and latest_settings.activity_aware_snapshot)
    execution_label = (
        "УМНЫЙ ДИАЛОГ"
        if is_dialogue
        else "УМНАЯ · НЕЗАВИСИМО"
        if independent
        else "УМНАЯ · РАСПРЕДЕЛЕНИЕ"
    )
    lines = [
        f"<b>📊 {_preview(launch.campaign_name, 80)}</b>",
        f"<code>{execution_label}{' · АРХИВ' if launch.archived else ''}</code>",
        f"Статус: <b>{state}</b>\n",
        "<b>Сейчас</b>",
        f"⏭ Ближайшая отправка: {next_send}",
        f"📡 Активных направлений: <b>{len(active_streams)}/{stream_count}</b> · "
        f"отправляется {launch.running} · готово сейчас {due_now}",
        (
            "♾ Работает: <b>до ручной остановки</b>"
            if launch.repeat_forever
            else f"🗓 Работает до: <code>{_date_time(launch.active_until)}</code>"
        ),
        "",
        "<b>Итог</b>",
        (
            f"📨 Реально отправлено сообщений: <b>{launch.sent_publications}</b>"
            if is_dialogue
            else f"📨 Реально отправлено: <b>{launch.sent_publications}</b>"
        ),
        *(
            [
                f"💬 Завершено диалогов: <b>{launch.succeeded}</b>",
                f"🧩 Продолжаются или ждут: <b>{launch.active}</b>",
            ]
            if is_dialogue
            else []
        ),
        f"🎯 Охват направлений: <b>{len(covered_streams)}/{stream_count}</b> "
        f"<code>{_progress_bar(len(covered_streams), stream_count)}</code>",
        f"✅ Успешно <b>{launch.succeeded}</b> · ❌ с ошибкой <b>{launch.failed}</b> · "
        f"🚫 пропущено/остановлено <b>{launch.cancelled}</b>",
    ]
    if launch.attempt_failures:
        lines.append(f"🔄 Сбоев отдельных попыток: <b>{len(launch.attempt_failures)}</b>")
    lines.extend(_variant_delivery_lines(runs))
    lines.extend(_reaction_task_lines(launch))

    lines.extend(
        [
            "\n<b>Как работает рассылка</b>",
            (
                f"💬 Чаты: <b>{launch.target_count}</b> · аккаунты цепочки: "
                f"<b>{launch.account_count}</b> · потоков: <b>{stream_count}</b>"
                if is_dialogue
                else f"🚀 Аккаунты: <b>{launch.account_count}</b> · чаты: "
                f"<b>{launch.target_count}</b> · независимых направлений: <b>{stream_count}</b>"
                if independent
                else f"🔀 Чаты: <b>{launch.target_count}</b> · аккаунты: "
                f"<b>{launch.account_count}</b> · режим: <b>распределение</b>"
            ),
            *(
                [
                    "🤖 Отправитель для каждого чата выбирается по доступу, "
                    "свободному лимиту и ближайшему разрешённому времени"
                ]
                if not is_dialogue and not independent
                else []
            ),
            "⏱ Если в чате нет таймера: "
            f"каждые <b>{_duration_text(launch.no_timer_interval_seconds)}</b>",
        ]
    )
    if activity_aware and latest_settings is not None:
        check_interval = _duration_text(
            latest_settings.activity_check_interval_seconds_snapshot or 0
        )
        maximum_wait = _visibility_wait_text(
            latest_settings.visibility_max_wait_seconds_snapshot or 0
        )
        lines.append(
            f"🧠 Умная частота: после <b>"
            f"{latest_settings.min_messages_between_posts_snapshot or '—'} новых сообщений</b> · "
            f"проверка {check_interval} · {maximum_wait}"
        )
        lines.append(f"📈 Настроено по активности: <b>{len(learned_timings)}/{stream_count}</b>")
    elif latest_settings is not None:
        lines.append("🧠 Авточастота: <b>выключена</b>")

    waiting_runs = [run for run in active_runs if run.status == RunStatus.QUEUED]
    if waiting_runs and not launch.running:
        first_waiting = min(waiting_runs, key=lambda run: (_as_utc(run.scheduled_for), run.id))
        if first_waiting.error_code:
            lines.append(
                "💡 <b>Почему сейчас ждёт:</b> "
                + _smart_waiting_state(first_waiting.error_code, first_waiting.scheduled_for)
            )

    if launch.failed or blocked_timings or preflight_skipped:
        lines.append("\n<b>⚠️ Требует внимания</b>")
        if preflight_skipped:
            lines.append(
                f"⏭ Не готовы при проверке: <b>{len(preflight_skipped)}</b> чатов; "
                "остальные продолжают работу"
            )
        if blocked_timings:
            lines.append(f"⛔ Исключено сочетаний аккаунт + чат: <b>{len(blocked_timings)}</b>")
        if launch.failed:
            lines.append(f"❌ Не отправились после повторов: <b>{launch.failed}</b>")
    else:
        lines.append("\n✅ <b>Критических проблем не обнаружено.</b>")

    lines.append("\n<b>Последние реплики</b>" if is_dialogue else "\n<b>Последние публикации</b>")
    recent_actions = sorted(
        launch.publications,
        key=lambda action: (action.created_at, action.id),
        reverse=True,
    )
    if recent_actions:
        for action in recent_actions[:5]:
            target = targets.get(action.target_id)
            title = _target_title(target, action.target_id, 40)
            sender = _compact_account_label(action.account_id, account_labels)
            link = _telegram_message_link(target, action.external_message_id)
            opened = (
                f' · <a href="{link}">открыть реплику ↗</a>'
                if link and is_dialogue
                else f' · <a href="{link}">открыть пост ↗</a>'
                if link
                else ""
            )
            relation = (
                f" · ответ на #{action.reply_to_message_id}"
                if is_dialogue and action.reply_to_message_id is not None
                else " · начало цепочки"
                if is_dialogue
                else ""
            )
            lines.append(
                f"• <code>{_date_time(action.created_at)}</code> · <b>{title}</b> · "
                f"{sender}{relation}{opened}"
            )
        if len(recent_actions) > 5:
            lines.append(f"• более ранних публикаций: {len(recent_actions) - 5}")
    else:
        publications = sorted(
            (run for run in runs if run.primary_message_id is not None),
            key=lambda item: (item.finished_at or item.updated_at or item.created_at, item.id),
            reverse=True,
        )
        for run in publications[:5]:
            target = targets.get(run.target_id)
            title = _target_title(target, run.target_id, 40)
            sender = _compact_account_label(run.sender_account_id, account_labels)
            link = _telegram_message_link(target, run.primary_message_id)
            opened = f' · <a href="{link}">открыть пост ↗</a>' if link else ""
            published_at = run.finished_at or run.updated_at or run.created_at
            lines.append(
                f"• <code>{_date_time(published_at)}</code> · <b>{title}</b> · {sender}{opened}"
            )
        if not publications:
            lines.append("<i>Успешных публикаций пока нет.</i>")

    runs_by_target: dict[int, list[CampaignRun]] = {}
    for run in runs:
        runs_by_target.setdefault(run.target_id, []).append(run)
    if runs_by_target:
        lines.append("\n<b>Чаты · сначала требующие внимания</b>")
        target_rows: list[tuple[tuple[int, datetime, str], str]] = []
        for target_id, target_runs in runs_by_target.items():
            target = targets.get(target_id)
            title = _target_title(target, target_id, 40)
            sent = [run for run in target_runs if run.primary_message_id is not None]
            active = [
                run for run in target_runs if run.status in {RunStatus.QUEUED, RunStatus.RUNNING}
            ]
            running = any(run.status == RunStatus.RUNNING for run in active)
            failed = sum(run.status == RunStatus.FAILED for run in target_runs)
            skipped_before_launch = next(
                (run for run in target_runs if run.error_code in _PREFLIGHT_SKIP_CODES),
                None,
            )
            blocked = sum(
                timing.send_blocked for timing in launch.timings if timing.target_id == target_id
            )
            target_streams = {stream_key(run) for run in target_runs}
            covered = {stream_key(run) for run in sent}
            state_text = (
                _preflight_skip_reason(skipped_before_launch.error_code)
                if skipped_before_launch is not None
                else "отправляется сейчас"
                if running
                else "цикл завершён"
            )
            next_target_send = datetime.max.replace(tzinfo=UTC)
            if active and not running:
                # SQLite returns legacy DATETIME values without tzinfo, while newly
                # created runs are UTC-aware.  Always normalize before comparing so
                # opening a live launch report cannot fail on a mixed dataset.
                next_target_send = min(_as_utc(run.scheduled_for) for run in active)
                wait_reason = next(
                    (run.error_code for run in active if run.error_code),
                    None,
                )
                state_text = _smart_waiting_state(wait_reason, next_target_send)
            elif running:
                next_target_send = min(_as_utc(run.scheduled_for) for run in active)
            issue_count = failed + blocked + (1 if skipped_before_launch is not None else 0)
            if skipped_before_launch is not None:
                marker = "⏭"
            elif blocked:
                marker = "⛔"
            elif failed and not active:
                marker = "❌"
            elif running:
                marker = "🔵"
            elif active:
                marker = "🕒"
            else:
                marker = "✅"
            opened = ""
            if sent:
                latest_sent = max(sent, key=lambda item: item.id)
                link = _telegram_message_link(target, latest_sent.primary_message_id)
                opened = f' · <a href="{link}">последний пост</a>' if link else ""
            issues = []
            if skipped_before_launch is not None:
                issues.append("пропущен до запуска")
            if failed:
                issues.append(f"ошибок {failed}")
            if blocked:
                issues.append(f"исключено {blocked}")
            issue_note = f" · {', '.join(issues)}" if issues else ""
            completed_chains = sum(run.status == RunStatus.SUCCEEDED for run in target_runs)
            delivery_count = f"цепочек {completed_chains}" if is_dialogue else f"постов {len(sent)}"
            target_rows.append(
                (
                    (-issue_count, _as_utc(next_target_send), title.casefold()),
                    f"{marker} <b>{title}</b> · {delivery_count} · "
                    f"потоков {len(covered)}/{len(target_streams)}\n"
                    f"└ {state_text}{issue_note}{opened}",
                )
            )
        target_rows.sort(key=lambda item: item[0])
        lines.extend(row for _, row in target_rows[:6])
        if len(target_rows) > 6:
            lines.append(f"• ещё чатов: {len(target_rows) - 6}")

    run_accounts = {run.sender_account_id for run in runs if run.sender_account_id is not None}
    failover_accounts = {
        account_id
        for failover in launch.failovers
        for account_id in (
            failover.failed_account_id,
            failover.replacement_account_id,
        )
    }
    account_ids = run_accounts | failover_accounts | set(account_labels)
    if account_ids:
        lines.append("\n<b>По аккаунтам</b>")
        account_rows: list[tuple[tuple[int, str], str]] = []
        for account_id in account_ids:
            account_runs = [run for run in runs if run.sender_account_id == account_id]
            action_sent = sum(action.account_id == account_id for action in launch.publications)
            sent = action_sent or sum(run.primary_message_id is not None for run in account_runs)
            active = sum(
                run.status in {RunStatus.QUEUED, RunStatus.RUNNING} for run in account_runs
            )
            chat_count = len(
                {
                    action.target_id
                    for action in launch.publications
                    if action.account_id == account_id
                }
                or {run.target_id for run in account_runs}
            )
            failed = sum(run.status == RunStatus.FAILED for run in account_runs)
            attempt_failures = sum(
                action.account_id == account_id for action in launch.attempt_failures
            )
            replaced = sum(item.failed_account_id == account_id for item in launch.failovers)
            accepted = sum(item.replacement_account_id == account_id for item in launch.failovers)
            label = _compact_account_label(account_id, account_labels)
            changes: list[str] = []
            if failed:
                changes.append(f"ошибок {failed}")
            if attempt_failures:
                changes.append(f"сбоев {attempt_failures}")
            if replaced:
                changes.append(f"заменён {replaced}×")
            if accepted:
                changes.append(f"принял {accepted}×")
            change_note = f" · {', '.join(changes)}" if changes else ""
            account_rows.append(
                (
                    (-(failed + attempt_failures), label.casefold()),
                    f"• {label}: отправлено {sent} · активных {active} · "
                    f"чатов {chat_count}{change_note}",
                )
            )
        account_rows.sort(key=lambda item: item[0])
        lines.extend(row for _, row in account_rows[:5])
        if len(account_rows) > 5:
            lines.append(f"• ещё аккаунтов: {len(account_rows) - 5}")

    if launch.timings:
        lines.append(
            "\n<b>Авточастота по потокам</b>"
            if activity_aware
            else "\n<b>Таймеры по парам аккаунт × чат</b>"
        )
        timing_rows: list[tuple[tuple[int, datetime], str]] = []
        for timing in launch.timings:
            target = targets.get(timing.target_id)
            title = _target_title(target, timing.target_id, 32)
            account = _compact_account_label(timing.account_id, account_labels, 24)
            if timing.send_blocked:
                detail = "⛔ исключён из этого чата"
            elif not activity_aware:
                detail = "умная частота выключена в этой рассылке"
                if timing.slowmode_seconds:
                    detail += f" · slow mode {_duration_text(timing.slowmode_seconds)}"
            elif timing.adaptive_interval_seconds is not None:
                tier = {
                    "turbo": "турбо",
                    "fast": "быстро",
                    "steady": "средне",
                    "quiet": "тихо",
                }.get(timing.adaptive_tier or "", timing.adaptive_tier or "обучается")
                detail = (
                    f"{tier} · интервал <b>{_duration_text(timing.adaptive_interval_seconds)}</b>"
                )
                if timing.activity_messages_per_minute is not None:
                    detail += f" · активность {timing.activity_messages_per_minute:g} сообщ./мин"
            else:
                detail = "обучается на первых отправках"
            if timing.next_send_at is not None:
                detail += f" · разрешено с <code>{_date_time(timing.next_send_at)}</code>"
            sort_time = timing.activity_sampled_at or datetime.min.replace(tzinfo=UTC)
            timing_rows.append(
                (
                    (0 if timing.send_blocked else 1, -_datetime_rank(sort_time)),
                    f"• <b>{title}</b> · {account}\n└ {detail}",
                )
            )
        timing_rows.sort(key=lambda item: item[0])
        lines.extend(row for _, row in timing_rows[:6])
        if len(timing_rows) > 6:
            lines.append(f"• ещё потоков: {len(timing_rows) - 6}")

    if launch.failovers:
        lines.append(f"\n<b>Автозамены · {launch.failover_count}</b>")
        for failover in launch.failovers[-3:]:
            target = targets.get(failover.target_id)
            title = _target_title(target, failover.target_id, 38)
            failed_account = _compact_account_label(failover.failed_account_id, account_labels)
            replacement = _compact_account_label(failover.replacement_account_id, account_labels)
            reason = _failover_reason(failover.error_code)
            resume = (
                f" · продолжение <code>{_date_time(failover.scheduled_for)}</code>"
                if failover.scheduled_for is not None
                else ""
            )
            lines.append(
                f"🔁 <b>{title}</b>: {failed_account} → {replacement}\n"
                f"└ {reason} · <code>{_date_time(failover.occurred_at)}</code>{resume}"
            )
        if launch.failover_count > 3:
            lines.append(f"• ещё замен: {launch.failover_count - 3}")

    if launch.attempt_failures:
        lines.append("\n<b>Последние сбои отправки</b>")
        for action in reversed(launch.attempt_failures[-3:]):
            target = targets.get(action.target_id)
            title = _target_title(target, action.target_id, 36)
            account = _compact_account_label(action.account_id, account_labels, 24)
            lines.append(
                f"❌ <b>{title}</b> · {account} · "
                f"<code>{_date_time(action.created_at)}</code>\n"
                f"└ {_preview(action.error or 'неизвестная ошибка', 100)}"
            )
    else:
        failed_runs = [run for run in runs if run.status == RunStatus.FAILED]
        if failed_runs:
            lines.append("\n<b>Последние ошибки</b>")
            for run in failed_runs[-3:]:
                target = targets.get(run.target_id)
                title = _target_title(target, run.target_id, 36)
                lines.append(f"• {title}: {_failure_preview(run.error_code, run.error, 100)}")

    lines.append(
        f"\n<i>Технических отправок создано: {launch.total}. Данные обновляются кнопкой ниже.</i>"
    )
    lines.extend(_hidden_mention_delivery_lines(runs))
    return _report_text(lines)


_PREFLIGHT_SKIP_CODES = frozenset(
    {
        "preflight_no_ready_sender",
        "preflight_independent_not_ready",
        "preflight_dialogue_not_ready",
        "preflight_account_cooldown",
    }
)


def _preflight_skipped_by_target(runs: list[CampaignRun]) -> dict[int, CampaignRun]:
    skipped: dict[int, CampaignRun] = {}
    for run in runs:
        if run.error_code in _PREFLIGHT_SKIP_CODES:
            skipped.setdefault(run.target_id, run)
    return skipped


def _preflight_skip_reason(error_code: str | None) -> str:
    return {
        "preflight_no_ready_sender": "нет аккаунта с подтверждённым доступом к чату",
        "preflight_independent_not_ready": (
            "этот аккаунт не имеет подтверждённого доступа; другие пары продолжают работу"
        ),
        "preflight_dialogue_not_ready": "не все аккаунты диалога готовы работать в чате",
        "preflight_account_cooldown": (
            "у аккаунта есть доступ к чату, но Telegram временно поставил его на паузу"
        ),
    }.get(error_code or "", "чат не прошёл проверку готовности")


def _hidden_mention_delivery_lines(runs: list[CampaignRun]) -> list[str]:
    requested = [run for run in runs if run.hidden_mentions_enabled_snapshot is True]
    if not requested:
        return []
    resolved = [run for run in requested if run.hidden_mention_user_ids_snapshot_json is not None]
    applied = [
        run
        for run in resolved
        if _hidden_mention_count(run) > 0 and run.primary_message_id is not None
    ]
    total_mentions = sum(_hidden_mention_count(run) for run in applied)
    skipped = [run for run in resolved if _hidden_mention_count(run) == 0]
    not_delivered = [
        run for run in resolved if _hidden_mention_count(run) > 0 and run.primary_message_id is None
    ]
    reasons: dict[str, int] = {}
    for run in skipped:
        reason = run.hidden_mention_skip_reason or "unknown"
        reasons[reason] = reasons.get(reason, 0) + 1
    lines = [
        "\n<b>Скрытые отметки участников</b>",
        f"✅ Добавлены: <b>{len(applied)}</b> публикаций · всего отметок: <b>{total_mentions}</b>",
        f"⏭ Без отметок: <b>{len(skipped)}</b> · ещё не обработано: "
        f"<b>{len(requested) - len(resolved)}</b>",
    ]
    if not_delivered:
        lines.append(f"⚠️ Подготовлены, но доставка не подтверждена: <b>{len(not_delivered)}</b>")
    for reason, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:3]:
        lines.append(f"• {_hidden_mention_skip_label(reason)}: <b>{count}</b>")
    return lines


def _hidden_mention_count(run: CampaignRun) -> int:
    if run.hidden_mention_resolved_count is not None:
        return max(0, int(run.hidden_mention_resolved_count))
    try:
        raw = json.loads(run.hidden_mention_user_ids_snapshot_json or "[]")
    except (TypeError, json.JSONDecodeError):
        return 0
    return len(raw) if isinstance(raw, list) else 0


def _hidden_mention_skip_label(reason: str) -> str:
    return {
        "content_does_not_support_mentions": "формат сообщения не поддерживает отметки",
        "message_has_no_room_for_mentions": "в сообщении не хватило места",
        "gateway_does_not_support_participant_collection": "сервис не получил список участников",
        "fewer_than_five_eligible_members": "меньше пяти подходящих участников",
        "participant_collection_unavailable": "список участников временно недоступен",
        "mention_cooldown_active": "участники ещё находятся на паузе ротации",
        "mention_reservation_conflict": "участников одновременно заняла другая публикация",
        "owned_account_excluded_after_resolution": (
            "после подготовки исключены собственные аккаунты"
        ),
        "excluded_user_after_resolution": ("после подготовки исключены запрещённые участники"),
        "unknown": "причина не записана",
    }.get(reason, escape(reason))


def _reaction_task_lines(launch: LaunchView) -> list[str]:
    total = (
        launch.reaction_queued
        + launch.reaction_running
        + launch.reaction_succeeded
        + launch.reaction_failed
        + launch.reaction_skipped
        + launch.reaction_cancelled
    )
    if not launch.reactions_enabled:
        return []
    configured = (
        f"Настроено: <b>{launch.reaction_requested_count}</b> на публикацию · "
        f"реакция <b>{escape(launch.reaction_emoji)}</b>"
    )
    if total == 0:
        if launch.sent_publications:
            status = "Задач пока нет: для реакции нужен другой готовый аккаунт с доступом к чату."
        else:
            status = "Задачи появятся после первой подтверждённой публикации."
        return ["\n<b>Реакции к публикациям</b>", configured, status]
    lines = [
        "\n<b>Реакции к публикациям</b>",
        configured,
        f"Всего: <b>{total}</b> · ✅ подтверждено Telegram: {launch.reaction_succeeded} · "
        f"❌ {launch.reaction_failed} · 🚫 {launch.reaction_cancelled}",
        f"🕒 ждёт <b>{launch.reaction_queued}</b> · "
        f"🔵 выполняется <b>{launch.reaction_running}</b> · "
        f"⏭ пропущено <b>{launch.reaction_skipped}</b>",
    ]
    if launch.reaction_delivered_emojis:
        actual = " · ".join(
            f"{escape(emoji)} × <b>{count}</b>" for emoji, count in launch.reaction_delivered_emojis
        )
        lines.append(f"✅ Фактически поставлено: {actual}")
        if any(emoji != launch.reaction_emoji for emoji, _ in launch.reaction_delivered_emojis):
            lines.append(
                "🔁 Telegram не принял часть выбранных реакций: использован другой emoji "
                "из списка панели."
            )
    for code, count in launch.reaction_skipped_reasons:
        lines.append(f"• Пропущено · {escape(failure_label(code))}: <b>{count}</b>")
    if launch.reaction_last_error:
        reason = (
            failure_label(launch.reaction_last_error_code)
            if launch.reaction_last_error_code
            else _preview(launch.reaction_last_error, 140)
        )
        lines.append(f"Последняя причина реакции: {escape(reason)}")
        hint = failure_hint(launch.reaction_last_error_code or "")
        if hint:
            lines.append(f"<i>{escape(hint)}</i>")
    return lines


def launch_cancel_confirmation_text(launch: LaunchView) -> str:
    return (
        f"<b>🛑 Остановить «{escape(launch.campaign_name)}»?</b>\n\n"
        f"Будут остановлены все ещё не отправленные сообщения: "
        f"<b>{launch.active}</b>, ожидающие реакции: <b>{launch.reaction_active}</b>. "
        "Выполняющееся действие завершит фиксацию результата после ответа Telegram. "
        "Уже отправленные сообщения не удаляются."
    )


def runs_text(page: Page[CampaignRun]) -> str:
    lines = [
        "<b>🧾 Технические операции</b>",
        "<i>Редко нужен: здесь отдельные действия внутри рассылок.</i>\n",
        f"<code>ВСЕГО · {page.total}</code>\n",
    ]
    for run in page.items:
        icon = {
            "queued": "🕒",
            "running": "🔵",
            "succeeded": "✅",
            "failed": "❌",
            "cancelled": "🚫",
        }[run.status.value]
        status = _run_status(run.status.value)
        step = _run_step(run.next_step.value)
        lines.append(
            f"{icon} <b>Отправка #{run.id}</b> · {status}\n"
            f"└ рассылка #{run.campaign_id} · проход {run.cycle_index + 1} · "
            f"{step} · попытка "
            f"{run.attempt_count}/{run.max_attempts}"
        )
    if not page.items:
        lines.append("Операций пока нет.")
    return "\n".join(lines)


def run_details_text(
    run: CampaignRun,
    actions: list[ActionLog],
    target: Target | None = None,
) -> str:
    lines = [
        f"<b>🧾 Отправка #{run.id}</b>",
        f"<code>{_run_status(run.status.value).upper()}</code>\n",
        f"Рассылка: <b>#{run.campaign_id}</b>",
        f"Проход: <b>{run.cycle_index + 1}</b>",
        f"Что делает сейчас: <b>{_run_step(run.next_step.value)}</b>",
        f"Попытки: <b>{run.attempt_count}/{run.max_attempts}</b>",
        f"Первое сообщение: <code>{run.primary_message_id or '—'}</code>",
        f"Ответ: <code>{run.reply_message_id or '—'}</code>",
    ]
    if run.error_code == "membership_gate_retry":
        lines.append(
            "\n🔄 <b>Требование подписки выполнено.</b> "
            f"Повтор этого сообщения: <code>{_date_time(run.scheduled_for)}</code>"
        )
    if run.dialogue_steps_snapshot_json:
        try:
            step_count = len(json.loads(run.dialogue_steps_snapshot_json))
        except (TypeError, json.JSONDecodeError):
            step_count = 0
        if step_count:
            lines.extend(
                [
                    f"Цепочка: <b>{min(run.dialogue_step_index, step_count)}/{step_count}</b> "
                    "сообщений отправлено",
                    f"Последнее сообщение: <code>{run.dialogue_last_message_id or '—'}</code>",
                ]
            )
    if run.error:
        code = f" · <code>{escape(run.error_code)}</code>" if run.error_code else ""
        if run.error_code in _PREFLIGHT_SKIP_CODES:
            lines.append(
                f"\n⏭ <b>Причина пропуска</b>{code}\n"
                f"{escape(_preflight_skip_reason(run.error_code))}"
            )
        else:
            label = _failure_title(run.error_code) if run.error_code else "Ошибка выполнения"
            lines.append(f"\n❌ <b>{escape(label)}</b>{code}")
            hint = failure_hint(run.error_code)
            if hint:
                lines.append(f"💡 {escape(hint)}")
            lines.append(f"<code>{escape(run.error[:500])}</code>")
    lines.append("\n<b>Журнал этапов</b>")
    for index, action in enumerate(actions, start=1):
        icon = "✅" if action.status.value == "succeeded" else "❌"
        kind = (
            "первое сообщение"
            if action.kind.value == "primary_message"
            else f"сообщение цепочки #{index}"
        )
        link = _telegram_message_link(target, action.external_message_id)
        opened = f' · <a href="{link}">открыть</a>' if link else ""
        lines.append(
            f"{icon} <b>{kind}</b> · аккаунт <code>#{action.account_id}</code> · "
            f"<code>{_date_time(action.created_at)}</code>\n"
            f"└ ID сообщения <code>{action.external_message_id or '—'}</code> · "
            f"ответ на <code>{action.reply_to_message_id or '—'}</code>{opened}\n"
            f"  <i>{_preview(action.text, 120)}</i>"
        )
    if not actions:
        lines.append("Этапы ещё не выполнялись.")
    return "\n".join(lines)


def audit_text(page: Page[AuditEvent]) -> str:
    lines = [screen_title("Системный аудит", f"Событий: {page.total}"), ""]
    for event in page.items:
        entity = f" #{event.entity_id}" if event.entity_id else ""
        lines.append(
            f"• <b>{escape(event.event_type)}</b>\n"
            f"  └ {escape(event.entity_type)}{escape(entity)} · <code>{escape(event.actor)}</code>"
        )
    if not page.items:
        lines.append("Событий нет. Здесь появятся изменения настроек и состояния системы.")
    return "\n".join(lines)


def settings_text(settings: Settings, stats: DashboardStats) -> str:
    log_chat = (
        f"<code>{settings.bot_log_chat_id}</code>"
        if settings.bot_log_chat_id is not None
        else "не настроен"
    )
    typing_status = "выключена"
    if settings.account_typing_enabled and settings.account_typing_max_seconds > 0:
        typing_status = (
            "«печатает…» "
            f"{settings.account_typing_min_seconds:g}–{settings.account_typing_max_seconds:g} сек."
        )
    return (
        screen_title(
            "Настройки и сервис",
            "Тестовый режим" if settings.dry_run else "Рабочий режим",
        )
        + "\n\n"
        f"{section('Очередь отправок')}\n"
        f"Состояние: <b>{'🟢 включён' if settings.worker_enabled else '⏸ выключен'}</b>\n"
        f"Одновременных операций: <b>{settings.worker_concurrency}</b>\n"
        f"Повторов при ошибке: <b>{settings.worker_max_attempts}</b>\n"
        f"Активных операций: <b>{stats.queued_runs + stats.running_runs}</b>\n\n"
        f"{section('Нагрузка аккаунтов')}\n"
        f"Суточный предел: <b>{settings.account_daily_limit or 'без лимита'}</b> "
        "успешных отправок на аккаунт\n"
        f"Подача перед сообщением: <b>{typing_status}</b>\n"
        "После достижения предела очередь использует другой свободный аккаунт или "
        "переносит задание на новое 24-часовое окно.\n\n"
        f"{section('Проверка прокси')}\n"
        f"Адрес: <code>{escape(settings.proxy_check_url)}</code>\n"
        f"Ожидание ответа: <b>{settings.proxy_check_timeout_seconds} сек.</b> · карантин после "
        f"<b>{settings.proxy_quarantine_after_failures}</b> ошибок\n\n"
        f"{section('Уведомления')}\n"
        f"Лог-чат: {log_chat}\n"
        f"Время в интерфейсе: <b>{escape(settings.timezone)}</b>\n\n"
        f"{section('Быстрые действия')}\n"
        "• 🩺 «Диагностика» проверяет базу и не меняет данные.\n"
        "• 💾 «Резервная копия» создаёт проверяемую копию базы.\n"
        "• 📜 «Журнал действий» показывает важные изменения.\n"
        "• 🛑 «Остановить все отправки» требует отдельного подтверждения и не удаляет "
        "сохранённые рассылки."
    )


def proxies_text(page: ProxyPage) -> str:
    filter_names = {
        "all": "все",
        "healthy": "рабочие",
        "problem": "проблемные",
        "unknown": "непроверенные",
        "disabled": "выключенные",
        "assigned": "с аккаунтами",
        "free": "без аккаунтов",
    }
    lines = [
        screen_title("Прокси для аккаунтов", f"Фильтр: {filter_names[page.filter_name.value]}"),
        "",
        f"🟢 Рабочие <b>{page.pool_healthy}</b>  ·  🔴 не работают <b>{page.pool_problem}</b>",
        f"⚪️ Не проверены <b>{page.pool_unknown}</b>  ·  "
        f"👥 назначений аккаунтам <b>{page.pool_assigned}</b>",
        f"Всего прокси <b>{page.pool_total}</b> · показано <b>{page.total}</b>\n",
    ]
    icons = {
        "unknown": "⚪️",
        "healthy": "🟢",
        "unhealthy": "🔴",
        "quarantined": "🟠",
        "disabled": "⏸",
    }
    for item in page.items:
        proxy = item.proxy
        latency = f"{proxy.latency_ms} мс" if proxy.latency_ms is not None else "—"
        status_names = {
            "unknown": "не проверен",
            "healthy": "рабочий",
            "unhealthy": "не работает",
            "quarantined": "не работает · карантин",
            "disabled": "выключен",
        }
        entry = (
            f"{icons[proxy.status.value]} <b>#{proxy.id} · {escape(proxy.label)}</b>\n"
            f"├ состояние: <b>{status_names[proxy.status.value]}</b> · задержка: {latency}\n"
            f"└ <code>{escape(item.masked_endpoint)}</code>"
        )
        if item.group_name:
            entry += f"\n   папка: <b>{escape(item.group_name)}</b>"
        if item.assignment_count:
            entry += "\n   " + _assignment_summary(item)
        lines.append(entry)
    if not page.items:
        if page.pool_total:
            lines.append("По этому фильтру ничего нет. Выберите другой фильтр.")
        else:
            lines.append(
                "<b>С чего начать</b>\nДобавьте SOCKS5/HTTP или MTProto-прокси, "
                "либо импортируйте TXT."
            )
    lines.append(
        "\n🔐 <i>«Рабочий» означает: последняя проверка успешно установила соединение "
        "через этот прокси. HTTP(S)/SOCKS5 проверяются через сеть, MTProto — "
        "подключением к Telegram без отправки сообщений. Пароли, secret и ссылки "
        "ротации зашифрованы и не показываются обратно.</i>"
    )
    return "\n".join(lines)


def proxy_details_text(item: ProxyView) -> str:
    proxy = item.proxy
    status_names = {
        "unknown": "⚪️ Не проверен",
        "healthy": "🟢 Рабочий",
        "unhealthy": "🔴 Не работает",
        "quarantined": "🟠 Не работает · карантин",
        "disabled": "⏸ Выключен",
    }
    latency = f"{proxy.latency_ms} мс" if proxy.latency_ms is not None else "—"
    is_mtproto = proxy.protocol.value == "mtproto"
    connection_line = (
        f"Задержка Telegram: <b>{latency}</b> · внешний IP <i>MTProto не раскрывает</i>"
        if is_mtproto
        else f"Задержка: <b>{latency}</b> · IP <code>{escape(proxy.observed_ip or '—')}</code>"
    )
    lines = [
        screen_title(
            escape(proxy.label),
            f"Прокси #{proxy.id} · {status_names[proxy.status.value]}",
        ),
        "",
        f"<code>{escape(item.masked_endpoint)}</code>\n",
        "<b>Подключение</b>",
        "Тип: <b>Telegram MTProto</b>" if is_mtproto else "Тип: <b>HTTP/SOCKS5</b>",
        connection_line,
        f"Проверки: ✅ {proxy.success_count} · ❌ {proxy.failure_count} "
        f"· подряд {proxy.consecutive_failures}",
    ]
    if item.group_name:
        lines.append(f"Папка: <b>{escape(item.group_name)}</b>")
    if proxy.last_checked_at is not None:
        lines.append(f"Последняя проверка: <code>{_date_time(proxy.last_checked_at)}</code>")
    if is_mtproto:
        lines.append("Ротация IP: <i>недоступна для MTProto</i>")
    else:
        rotation = "есть" if item.has_rotation_url else "не задана"
        lines.append(f"Ротация IP: <b>{rotation}</b>")
    if proxy.last_rotated_at is not None:
        lines.append(f"Последняя ротация: <code>{_date_time(proxy.last_rotated_at)}</code>")
    if proxy.last_error:
        lines.append(f"\n❌ <b>Почему не работает</b>\n<code>{escape(proxy.last_error)}</code>")
    if item.assignment_count:
        lines.extend(
            [
                "\n<b>Используют этот прокси</b>",
                _assignment_details(item),
            ]
        )
    else:
        lines.extend(["\n<b>Используют этот прокси</b>", "Пока ни один аккаунт не назначен."])
    return "\n".join(lines)


def _assignment_summary(item: ProxyView) -> str:
    account_ids = _account_ids_from_assignments(item.assignment_keys)
    if account_ids:
        preview = ", ".join(f"#{account_id}" for account_id in account_ids[:3])
        return f"👥 аккаунтов: <b>{len(account_ids)}</b> · {preview}" + (
            "…" if len(account_ids) > 3 else ""
        )
    if item.assignment_count == 1:
        return f"📌 <code>{escape(item.assignment_key or '')}</code>"
    return (
        f"📌 назначений: <b>{item.assignment_count}</b> · "
        + ", ".join(f"<code>{escape(key)}</code>" for key in item.assignment_keys[:3])
        + ("…" if item.assignment_count > 3 else "")
    )


def _assignment_details(item: ProxyView) -> str:
    account_ids = _account_ids_from_assignments(item.assignment_keys)
    if account_ids:
        lines = [f"👥 Аккаунтов: <b>{len(account_ids)}</b>"]
    else:
        lines = [f"📌 Назначений: <b>{item.assignment_count}</b>"]
    for key, purpose in zip(item.assignment_keys[:8], item.assignment_purposes[:8], strict=False):
        account_id = _account_id_from_assignment(key)
        tail = f" · {escape(purpose)}" if purpose else ""
        label = f"Аккаунт #{account_id}" if account_id is not None else key
        lines.append(f"• <code>{escape(label)}</code>{tail}")
    if item.assignment_count > 8:
        lines.append(f"• … ещё {item.assignment_count - 8}")
    return "\n".join(lines)


def _account_ids_from_assignments(keys: tuple[str, ...]) -> list[int]:
    return [
        account_id for key in keys if (account_id := _account_id_from_assignment(key)) is not None
    ]


def _account_id_from_assignment(key: str) -> int | None:
    if not key.startswith("account:"):
        return None
    try:
        return int(key.split(":", 1)[1])
    except ValueError:
        return None


def proxy_groups_text(groups: list[ProxyGroup]) -> str:
    lines = [screen_title("Группы прокси", f"Всего: {len(groups)}"), ""]
    for group in groups:
        lines.append(
            f"<b>#{group.id} · {escape(group.name)}</b>\n"
            f"└ {escape(group.description or 'без описания')}"
        )
    if not groups:
        lines.append("Групп нет. Создайте вручную или укажите новое имя во время импорта.")
    lines.append("\n<i>Удаление группы не удаляет находящиеся в ней прокси.</i>")
    return "\n".join(lines)


def campaign_run_confirmation_text(
    campaign: Campaign,
    *,
    sender_count: int | None = None,
    target_count: int | None = None,
    capacity_warning: str | None = None,
) -> str:
    audience_name = escape(
        str(
            campaign.audience_label
            or (
                f"чат #{campaign.target_id}"
                if campaign.target_id is not None
                else f"список #{campaign.target_list_id}"
            )
        )
    )
    target = (
        f"чат · <b>{audience_name}</b>"
        if campaign.target_id is not None
        else f"список чатов · <b>{audience_name}</b>"
    )
    if target_count is not None:
        target += f" · <b>{target_count}</b> чатов"
    if campaign.mode == CampaignMode.BROADCAST:
        if campaign.smart_timer_mode:
            timing = (
                "срок активности: <b>до ручной остановки</b>"
                if campaign.repeat_forever
                else f"срок активности: <b>{_duration_text(campaign.smart_duration_seconds)}</b>"
            )
        elif campaign.spread_seconds >= 3600:
            timing = f"окно отправки: <b>{campaign.spread_seconds // 3600} ч.</b>"
        elif campaign.spread_seconds >= 60:
            timing = f"окно отправки: <b>{campaign.spread_seconds // 60} мин.</b>"
        elif campaign.spread_seconds > 0:
            timing = f"окно отправки: <b>{campaign.spread_seconds} сек.</b>"
        else:
            timing = "отправить сразу"
        senders = f"\nОтправителей: <b>{sender_count}</b>" if sender_count is not None else ""
        independent = campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
        sender_mode = (
            "\nРабота аккаунтов: <b>независимо · отдельный поток каждого в каждом чате</b>"
            if independent
            else ("\nРабота аккаунтов: <b>умный выбор по доступу, лимиту и времени</b>")
        )
        repeats = (
            "непрерывно · с таймером сразу после него · без таймера каждые "
            f"<b>{_duration_text(campaign.smart_no_timer_interval_seconds)}</b> · "
            + (
                "авточастота отдельно для каждой пары"
                if independent
                else f"автозамена {'включена' if campaign.auto_sender_failover else 'выключена'}"
            )
            if campaign.smart_timer_mode
            else "один проход"
            if campaign.repeat_count == 1
            else (
                f"<b>{campaign.repeat_count}</b> проходов · минимум "
                f"<b>{_duration_text(campaign.repeat_interval_seconds)}</b>"
            )
        )
        if target_count is None:
            task_count = 1 if campaign.smart_timer_mode else campaign.repeat_count
        elif campaign.smart_timer_mode:
            task_count = target_count
        else:
            task_count = target_count * campaign.repeat_count
        if independent:
            task_count *= max(1, sender_count or 1)
        mode_block = (
            f"Формат: <b>📢 посты в чатах</b>{senders}{sender_mode}\n{timing}\n"
            f"Повторы: {repeats}\nОтправок подготовлено: <b>{task_count}</b>"
        )
    else:
        mode_block = (
            "Формат: <b>💬 диалог</b>\n"
            f"Пауза перед ответом: <b>{campaign.reply_delay_seconds} сек.</b>"
        )
    warning = f"\n\n{capacity_warning}" if capacity_warning else ""
    return (
        f"<b>🚀 Начать «{escape(campaign.name)}»?</b>\n\n"
        f"Куда: <b>{target}</b>\n"
        f"{mode_block}\n\n"
        "После подтверждения бот автоматически проверит доступ к чатам и аккаунтам. "
        f"Если всё готово — отправка начнётся без дополнительных действий.{warning}"
    )


def campaign_launch_review_text(
    campaign: Campaign,
    summary: CampaignPreflightSummary,
    *,
    planned_tasks: int,
    capacity_warning: str | None = None,
) -> str:
    """Render the operator's last review before a partial launch can be queued."""

    independent = (
        campaign.mode == CampaignMode.BROADCAST
        and campaign.sender_mode == BroadcastSenderMode.INDEPENDENT
    )
    ready = summary.launchable_units
    total = summary.target_count * summary.account_count if independent else summary.target_count
    unit = "направлений аккаунт–чат" if independent else "чатов"
    if ready == 0:
        decision = (
            "<b>В очередь ничего не будет поставлено.</b> Telegram временно поставил "
            "доступные аккаунты на защитную паузу; дождитесь указанного времени или "
            "выберите готовые аккаунты."
            if summary.deferred_pairs
            else "<b>В очередь ничего не будет поставлено.</b> Исправьте доступ к чатам или "
            "аккаунтам и проверьте рассылку ещё раз."
        )
    elif ready == total:
        decision = (
            f"<b>В очередь войдут все {total} {unit}.</b> Подтвердите запуск ниже: "
            "сервис использует именно этот свежий результат и перед очередью проверит "
            "только его актуальность, без повторного Telegram-прогона."
        )
    else:
        decision = (
            f"<b>В очередь войдут только {ready} из {total} {unit}.</b> Остальные "
            "не будут получать попытки отправки. Подтвердите запуск ниже: сервис "
            "использует именно этот свежий результат и проверит только его актуальность, "
            "без повторного Telegram-прогона."
        )
    task_line = (
        f"Ожидаемых технических отправок: <b>{planned_tasks}</b>"
        if ready
        else "Ожидаемых технических отправок: <b>0</b>"
    )
    warning = f"\n\n{capacity_warning}" if capacity_warning else ""
    return (
        screen_title(
            "Готовность перед запуском",
            f"{escape(campaign.name)} · свежая проверка завершена",
        )
        + "\n\n"
        + _campaign_preflight_details(summary)
        + "\n\n"
        + section("Решение")
        + "\n"
        + decision
        + "\n"
        + task_line
        + warning
    )


def campaign_confirmation_text(data: dict[str, object]) -> str:
    target_source = (
        f"Чат: <code>#{data['target_id']}</code>"
        if data.get("target_id") is not None
        else f"Список чатов: <code>#{data['target_list_id']}</code>"
    )
    mode = str(data.get("mode", "dialogue"))
    spread_hours = int(data.get("spread_hours", 0))
    spread_label = "сразу" if spread_hours == 0 else f"за {spread_hours} ч."
    repeat_count = int(data.get("repeat_count", 1))
    repeat_minutes = int(data.get("repeat_interval_minutes", 0))
    smart_timer_mode = bool(data.get("smart_timer_mode", False))
    repeat_forever = bool(data.get("repeat_forever", False))
    smart_duration_hours = int(data.get("smart_duration_hours", 0))
    default_profile = BROADCAST_PROFILES["active"]
    smart_no_timer_interval_minutes = int(
        data.get("smart_no_timer_interval_minutes", default_profile.interval_minutes)
    )
    min_messages_between_posts = int(
        data.get("min_messages_between_posts", default_profile.min_messages_between_posts)
    )
    visibility_max_wait_seconds = int(
        data.get("visibility_max_wait_seconds", default_profile.visibility_max_wait_seconds)
    )
    activity_check_interval_seconds = int(
        data.get(
            "activity_check_interval_seconds",
            default_profile.activity_check_interval_seconds,
        )
    )
    lines = [
        "<b>✅ Проверьте перед сохранением</b>",
        "<code>НОВЫЙ ЗАПУСК</code>\n",
        f"Название: <b>{escape(str(data['name']))}</b>",
    ]
    if mode == "broadcast":
        sender_ids = [int(item) for item in data.get("sender_account_ids", [])]
        sender_text = ", ".join(f"#{account_id}" for account_id in sender_ids)
        sender_text = sender_text or f"#{data['primary_account_id']}"
        lines.extend(
            [
                "Режим: <b>📢 рассылка</b> (одно сообщение на чат)",
                f"Отправители: <code>{escape(sender_text)}</code>",
                target_source,
                (
                    (
                        "Срок активности: <b>до ручной остановки</b>\n"
                        if repeat_forever
                        else f"Срок активности: <b>{smart_duration_hours} ч.</b>\n"
                    )
                    if smart_timer_mode
                    else f"Окно отправки: <b>{spread_label}</b>\n"
                ),
                (
                    "Рассылка: <b>♻️ умная непрерывная</b>\n"
                    "• с таймером — сразу после его окончания\n"
                    "• без таймера — каждые "
                    f"<b>{_duration_text(smart_no_timer_interval_minutes * 60)}</b>\n"
                    "• видимость — после "
                    f"<b>{min_messages_between_posts} новых сообщений</b>"
                    + (
                        " · только после порога\n"
                        if visibility_max_wait_seconds == 0
                        else (
                            " или максимум через <b>"
                            f"{_duration_text(visibility_max_wait_seconds)}</b>\n"
                        )
                    )
                    + "• проверка позиции — каждые "
                    + f"<b>{_duration_text(activity_check_interval_seconds)}</b>\n"
                    + "• автозамена — <b>включена для выбранных аккаунтов</b>"
                    if smart_timer_mode
                    else "Повторы: <b>один проход</b>"
                    if repeat_count == 1
                    else (
                        f"Повторы: <b>{repeat_count} проходов</b> · минимум "
                        f"<b>{_duration_text(repeat_minutes * 60)}</b>"
                    )
                ),
                f"<b>Сообщение</b>\n"
                f"<blockquote>{_text_preview(str(data['primary_text']), 1600)}</blockquote>",
            ]
        )
    else:
        raw_steps = data.get("dialogue_steps", [])
        steps = [dict(item) for item in raw_steps if isinstance(item, dict)]
        if not steps:
            steps = [
                {
                    "account_id": data["primary_account_id"],
                    "text": data["primary_text"],
                    "delay_seconds": 0,
                },
                {
                    "account_id": data["reply_account_id"],
                    "text": data["reply_text"],
                    "delay_seconds": data["reply_delay_seconds"],
                },
            ]
        roles = " → ".join(f"#{int(step['account_id'])}" for step in steps)
        lines.extend(
            [
                f"Режим: <b>💬 цепочка диалога · {len(steps)} сообщений</b>",
                f"Роли: <code>{roles}</code>",
                target_source,
                "Таймер каждого аккаунта в каждом чате: <b>учитывается автоматически</b>",
            ]
        )
        for index, step in enumerate(steps, start=1):
            delay = int(step.get("delay_seconds") or 0)
            delay_label = "старт" if index == 1 else f"через {delay} сек."
            media_label = {
                "photo": "фото",
                "video": "видео",
                "document": "документ",
                "voice": "голосовое",
                "video_note": "видеокружок",
            }.get(str(step.get("media_kind") or ""))
            preview = _text_preview(str(step.get("text") or ""), 180) or "без подписи"
            lines.append(
                f"\n<b>{index} · аккаунт #{int(step['account_id'])}</b> · {delay_label}"
                f"{' · ' + media_label if media_label else ''}\n"
                f"<blockquote>{preview}</blockquote>"
            )
    return "\n".join(lines)


def _preview(value: str, limit: int = 90) -> str:
    compact = " ".join(value.split())
    return _text_preview(compact, limit)


def _campaign_content_variants(campaign: Campaign) -> list[BroadcastContentVariant]:
    try:
        return parse_content_variants(
            campaign.content_variants_json,
            fallback_text=campaign.primary_text,
            fallback_html=campaign.primary_html,
            fallback_media_path=campaign.primary_media_path,
            fallback_media_kind=campaign.primary_media_kind,
            fallback_media_name=campaign.primary_media_name,
            fallback_media_size=campaign.primary_media_size,
        )
    except ContentError:
        return [
            BroadcastContentVariant(
                variant_id="primary",
                name="Основной",
                text=campaign.primary_text,
            )
        ]


def _variant_delivery_lines(runs: list[CampaignRun]) -> list[str]:
    stats: dict[tuple[str, bool], dict[str, int]] = {}
    for run in runs:
        name = str(getattr(run, "content_variant_name_snapshot", None) or "Основной")
        is_contact = _run_uses_contact(run)
        row = stats.setdefault((name, is_contact), {"sent": 0, "failed": 0, "waiting": 0})
        if run.status == RunStatus.SUCCEEDED:
            row["sent"] += 1
        elif run.status == RunStatus.FAILED:
            row["failed"] += 1
        elif run.status in {RunStatus.QUEUED, RunStatus.RUNNING}:
            row["waiting"] += 1
    if len(stats) <= 1 and not any(is_contact for _, is_contact in stats):
        return []
    lines = ["\n<b>🎨 Результат по контенту</b>"]
    rows = list(stats.items())
    for (name, is_contact), row in rows[:5]:
        label = f"📇 {name}" if is_contact else name
        lines.append(
            f"• <b>{escape(label)}</b>: доставлено {row['sent']} · "
            f"ошибок {row['failed']} · ожидает {row['waiting']}"
        )
    if len(rows) > 5:
        lines.append(f"• ещё вариантов: {len(rows) - 5}")
    return lines


def _run_uses_contact(run: CampaignRun) -> bool:
    if not run.content_variants_snapshot_json:
        return False
    try:
        variants = json.loads(run.content_variants_snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(variants, list):
        return False
    for index, raw in enumerate(variants):
        if not isinstance(raw, dict):
            continue
        same_id = raw.get("id") == run.content_variant_id_snapshot
        same_index = run.content_variant_id_snapshot is None and index == run.content_variant_index
        if same_id or same_index:
            return isinstance(raw.get("contact"), dict)
    return False


def _percent(value: int, total: int) -> int:
    if total <= 0:
        return 0
    return min(100, max(0, round(value * 100 / total)))


def _progress_bar(value: int, total: int, width: int = 8) -> str:
    percent = _percent(value, total)
    filled = min(width, max(0, round(width * percent / 100)))
    return f"{'█' * filled}{'░' * (width - filled)} {percent}%"


def _expected_stream_count(launch: LaunchView) -> int:
    if launch.sender_mode == BroadcastSenderMode.INDEPENDENT:
        return max(0, launch.target_count * launch.account_count)
    return max(0, launch.target_count)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _is_due(value: datetime) -> bool:
    return _as_utc(value) <= datetime.now(UTC)


def _datetime_rank(value: datetime) -> int:
    normalized = _as_utc(value)
    return (
        normalized.toordinal() * 86400
        + normalized.hour * 3600
        + normalized.minute * 60
        + normalized.second
    )


def _schedule_distance(value: datetime) -> str:
    seconds = int((_as_utc(value) - datetime.now(UTC)).total_seconds())
    if seconds < -30:
        return "готово к отправке"
    if seconds <= 30:
        return "сейчас"
    if seconds < 3600:
        return f"через {(seconds + 59) // 60} мин."
    if seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"через {hours} ч. {minutes} мин." if minutes else f"через {hours} ч."
    days = seconds // 86400
    return f"через {days} сут."


def _launch_state_label(launch: LaunchView, *, blocked_count: int = 0) -> str:
    if launch.archived:
        return "🗄 В архиве"
    if launch.running:
        if launch.failed or blocked_count:
            return "🔵 Отправляет сейчас · ⚠️ есть проблемы"
        return "🔵 Отправляет прямо сейчас"
    if launch.active:
        if launch.failed or blocked_count:
            return "🟡 Работает, есть проблемы"
        if launch.sent_publications == 0 and launch.succeeded == 0:
            return "🕒 Запланирован"
        return "🟢 Работает штатно"
    if launch.reaction_active:
        return "🟣 Публикации готовы · реакции продолжаются"
    if launch.failed:
        return "🔴 Завершён с ошибками"
    if launch.cancelled:
        return "🚫 Остановлен"
    return "✅ Завершён успешно"


def _target_title(target: Target | None, target_id: int, limit: int = 40) -> str:
    return _preview(target.title, limit) if target is not None else f"чат #{target_id}"


def _compact_account_label(
    account_id: int | None,
    labels: dict[int, str],
    limit: int = 28,
) -> str:
    if account_id is None:
        return "аккаунт не назначен"
    label = labels.get(account_id)
    return _preview(label, limit) if label else f"аккаунт #{account_id}"


def _report_text(lines: list[str], limit: int = 4096) -> str:
    text = "\n".join(lines)
    if len(text) <= limit:
        return text
    footer = "\n<i>Часть подробностей скрыта из‑за лимита Telegram.</i>"
    compact = list(lines)
    while len(compact) > 5 and len("\n".join(compact) + footer) > limit:
        compact.pop()
    return "\n".join(compact) + footer


def _text_preview(value: str, limit: int) -> str:
    if len(value) > limit:
        value = value[: limit - 1].rstrip() + "…"
    return escape(value)


def _run_status(value: str) -> str:
    return {
        "queued": "ожидает",
        "running": "выполняется",
        "succeeded": "завершён",
        "failed": "ошибка",
        "cancelled": "отменён",
    }[value]


def _run_step(value: str) -> str:
    return {"primary": "первое сообщение", "reply": "адресный ответ"}[value]


def _launch_icon(launch: LaunchView) -> str:
    if launch.running:
        return "🔵"
    if launch.queued:
        return "🕒"
    if launch.failed:
        return "❌"
    if launch.succeeded == launch.total:
        return "✅"
    if launch.cancelled == launch.total:
        return "🚫"
    return "🟠"


def _date_time(value: object) -> str:
    if not isinstance(value, datetime):
        return "—"
    timezone_name = get_settings().timezone
    try:
        display_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        display_timezone = UTC
        timezone_name = "UTC"
    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value
    local_value = normalized.astimezone(display_timezone)
    return f"{local_value:%d.%m · %H:%M} ({timezone_name})"


def _visibility_wait_text(seconds: int) -> str:
    if seconds <= 0:
        return "без тайм-аутной публикации · только после порога"
    return f"максимум через {_duration_text(seconds)}"


def _smart_waiting_state(reason: str | None, scheduled_for: datetime) -> str:
    when = f"<code>{_date_time(scheduled_for)}</code> · {_schedule_distance(scheduled_for)}"
    if reason == "chat_idle_last_managed_post":
        return f"ждём первое новое сообщение: наш пост последний · проверка {when}"
    if reason == "visibility_wait":
        return f"ждём достижения порога новых сообщений · проверка {when}"
    if reason == "activity_probe_unavailable":
        return f"не удалось проверить активность; безопасно ждём повторную проверку {when}"
    if reason == "chat_timer":
        return f"ждём таймер чата · следующая {when}"
    if reason == "join_request_pending":
        return (
            "заявка на вступление отправлена; ждём одобрения администратора "
            f"· повторная проверка {when}"
        )
    return f"следующая {when}"


def _duration_text(seconds: int) -> str:
    if seconds % 86400 == 0 and seconds >= 86400:
        return f"{seconds // 86400} сут."
    if seconds % 3600 == 0 and seconds >= 3600:
        return f"{seconds // 3600} ч."
    if seconds % 60 == 0 and seconds >= 60:
        return f"{seconds // 60} мин."
    return f"{seconds} сек."


def _account_label(account_id: int | None, labels: dict[int, str]) -> str:
    if account_id is None:
        return "аккаунт не назначен"
    label = labels.get(account_id)
    return escape(label) if label else f"аккаунт #{account_id}"


def _failover_reason(error_code: str | None) -> str:
    return escape(failure_label(error_code)) if error_code else "аккаунт недоступен"


def _telegram_message_link(target: Target | None, message_id: int | None) -> str | None:
    if target is None or message_id is None:
        return None
    chat_ref = target.chat_ref.strip()
    if chat_ref.startswith("@"):
        return f"https://t.me/{chat_ref[1:]}/{message_id}"
    peer_id = target.telegram_peer_id
    if peer_id is None and chat_ref.startswith("-100") and chat_ref[4:].isdigit():
        peer_id = int(chat_ref)
    if peer_id is None:
        return None
    value = str(abs(int(peer_id)))
    if not value.startswith("100") or len(value) <= 3:
        return None
    return f"https://t.me/c/{value[3:]}/{message_id}"


def _target_error_label(code: str) -> str:
    return _failure_title(code)


def _failure_title(code: str | None) -> str:
    label = failure_label(code)
    return label[:1].upper() + label[1:] if label else "Ошибка выполнения"


def _failure_preview(code: str | None, raw: str | None, limit: int) -> str:
    if code:
        return _preview(failure_label(code), limit)
    return _preview(raw or "неизвестная ошибка", limit)


def wizard_text(
    title: str,
    step: int,
    total: int,
    body: str,
    *,
    hint: str = "Команда /cancel отменяет ввод без сохранения.",
) -> str:
    return (
        screen_title(escape(title), f"Шаг {step} из {total}") + "\n\n"
        f"{body}\n\n"
        f"<i>Подсказка: {escape(hint)}</i>"
    )
