from proxima_traffic.bot.direct_message_ui import (
    direct_launch_details_text,
    direct_messages_home_text,
)
from proxima_traffic.direct_messages import DirectLaunchView, DirectMessageDashboard
from proxima_traffic.models import (
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    SubscriberSegment,
    utc_now,
)


def _launch(*, total_tasks: int) -> DirectLaunch:
    return DirectLaunch(
        id=41,
        campaign_id=4,
        subscriber_list_id=9,
        status=DirectLaunchStatus.PARTIAL,
        segment_snapshot=SubscriberSegment.ALL,
        recipient_limit_snapshot=total_tasks,
        contact_cooldown_days_snapshot=30,
        account_ids_snapshot_json="[1]",
        primary_text_snapshot="hello",
        total_tasks=total_tasks,
        consent_confirmed_at=utc_now(),
    )


def test_direct_home_does_not_claim_delivery_or_read() -> None:
    text = direct_messages_home_text(
        DirectMessageDashboard(
            list_count=1,
            subscriber_count=10,
            premium_count=2,
            username_count=8,
            blacklist_count=1,
            campaign_count=1,
            active_launches=1,
            queued_tasks=2,
            running_tasks=1,
            succeeded_tasks=3,
            failed_tasks=1,
            skipped_tasks=1,
        )
    )

    assert "Telegram подтвердил отправку: <b>3</b>" in text
    assert "Доставлено" not in text
    assert "Пропущено без отправки: <b>1</b>" in text
    assert "Ошибка или неясный результат: <b>1</b>" in text
    assert "отменённые задачи видны внутри конкретного запуска" in text
    assert "не означает, что человек прочитал" in text


def test_unavailable_recipient_does_not_promise_future_quarantine() -> None:
    view = DirectLaunchView(
        launch=_launch(total_tasks=1),
        campaign_name="Direct test",
        list_name="Subscribers",
        queued=0,
        running=0,
        succeeded=0,
        failed=0,
        skipped=1,
        cancelled=0,
        last_error_code="recipient_unavailable",
        last_error="recipient unavailable",
        last_error_status=DirectMessageTaskStatus.SKIPPED,
        last_error_recipient="ID 123",
    )

    text = direct_launch_details_text(view)

    assert "может снова получить тот же отказ Telegram" in text
    assert "останется недоступной для новых запусков" not in text
    assert "подтверждённой отправки ему нет" in text


def test_direct_launch_surfaces_inconsistent_task_totals() -> None:
    view = DirectLaunchView(
        launch=_launch(total_tasks=10),
        campaign_name="Direct test",
        list_name="Subscribers",
        queued=1,
        running=1,
        succeeded=1,
        failed=1,
        skipped=1,
        cancelled=1,
    )

    assert "Сводка задач расходится с запуском" in direct_launch_details_text(view)
