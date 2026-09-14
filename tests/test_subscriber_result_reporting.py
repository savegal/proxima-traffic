from proxima_traffic.bot.direct_message_ui import (
    direct_launch_details_text,
    subscriber_import_prompt_text,
    subscriber_import_result_text,
)
from proxima_traffic.bot.subscriber_invite_ui import invite_launch_text, invite_launches_text
from proxima_traffic.direct_messages import DirectLaunchView, SubscriberImportResult
from proxima_traffic.models import (
    DirectLaunch,
    DirectLaunchStatus,
    DirectMessageTaskStatus,
    SubscriberInviteLaunch,
    SubscriberList,
    SubscriberSegment,
    utc_now,
)
from proxima_traffic.subscriber_invites import SubscriberInviteLaunchView


def test_direct_launch_reports_confirmed_send_without_claiming_read_or_delivery() -> None:
    launch = DirectLaunch(
        id=41,
        campaign_id=4,
        subscriber_list_id=9,
        status=DirectLaunchStatus.PARTIAL,
        segment_snapshot=SubscriberSegment.ALL,
        recipient_limit_snapshot=6,
        contact_cooldown_days_snapshot=30,
        account_ids_snapshot_json="[1]",
        primary_text_snapshot="hello",
        total_tasks=6,
        consent_confirmed_at=utc_now(),
    )
    view = DirectLaunchView(
        launch=launch,
        campaign_name="Direct test",
        list_name="Subscribers",
        queued=1,
        running=1,
        succeeded=1,
        failed=1,
        skipped=1,
        cancelled=1,
        last_error_code="delivery_unconfirmed",
        last_error="network reply was lost",
        last_error_status=DirectMessageTaskStatus.FAILED,
    )

    text = direct_launch_details_text(view)

    assert "Telegram подтвердил отправку: <b>1</b>" in text
    assert "Доставлено" not in text
    assert "не означает, что человек прочитал" in text
    assert "В очереди: <b>1</b>" in text
    assert "Выполняется сейчас: <b>1</b>" in text
    assert "Пропущено без отправки: <b>1</b>" in text
    assert "Ошибка или неясный результат: <b>1</b>" in text
    assert "Отменено до отправки: <b>1</b>" in text
    assert "Итог отправки не подтверждён" in text
    assert "Сводка задач расходится" not in text


def test_invite_launch_reports_every_task_bucket_and_uncertain_result() -> None:
    launch = SubscriberInviteLaunch(
        id=52,
        campaign_id=7,
        subscriber_list_id=9,
        status=DirectLaunchStatus.PARTIAL,
        destination_ref_snapshot="@owned_chat",
        destination_key_snapshot="@owned_chat",
        segment_snapshot=SubscriberSegment.ALL,
        recipient_limit_snapshot=7,
        contact_cooldown_days_snapshot=90,
        account_ids_snapshot_json="[1]",
        total_tasks=7,
        consent_confirmed_at=utc_now(),
    )
    view = SubscriberInviteLaunchView(
        launch=launch,
        campaign_name="Invite test",
        list_name="Subscribers",
        queued=1,
        running=1,
        invited=1,
        already_participant=1,
        failed=1,
        skipped=1,
        cancelled=1,
        last_error_code="invite_delivery_unconfirmed",
        last_error="RPC outcome unknown",
        last_error_status=DirectMessageTaskStatus.FAILED,
        issues=((DirectMessageTaskStatus.FAILED, "invite_delivery_unconfirmed", 1),),
    )

    text = invite_launch_text(view)
    list_text = invite_launches_text([view], total=1, page=0, page_size=10)

    assert "Telegram подтвердил приглашение: <b>1</b>" in text
    assert "Уже были участниками: <b>1</b>" in text
    assert "В очереди: <b>1</b>" in text
    assert "Выполняется сейчас: <b>1</b>" in text
    assert "Пропущено без приглашения: <b>1</b>" in text
    assert "Ошибка или неясный результат: <b>1</b>" in text
    assert "Отменено до выполнения: <b>1</b>" in text
    assert "Telegram не подтвердил итог" in text
    assert "Сводка задач расходится" not in text
    assert "уже были 1" in list_text
    assert "пропущено 1" in list_text
    assert "отменено 1" in list_text


def test_subscriber_import_explains_merge_semantics_and_existing_profile_counter() -> None:
    prompt = subscriber_import_prompt_text(20 * 1024 * 1024)
    assert "автоматически из существующей базы не удаляются" in prompt

    subscriber_list = SubscriberList(
        id=3,
        name="Import test",
        member_count=8,
        premium_count=2,
        non_premium_count=5,
        username_count=6,
    )
    result = SubscriberImportResult(
        subscriber_list=subscriber_list,
        total_lines=10,
        valid_records=8,
        added_members=2,
        inserted_profiles=1,
        updated_profiles=7,
        duplicates=1,
        invalid=1,
        issues=(),
    )

    text = subscriber_import_result_text(result)

    assert "Существующих профилей обработано: <b>7</b>" in text
    assert "Обновлено профилей" not in text
    assert "добавление/обновление, а не как полная замена базы" in text
