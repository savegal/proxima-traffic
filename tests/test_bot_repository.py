import pytest
from fastapi import FastAPI
from sqlalchemy import delete

from proxima_traffic.bot.repository import AdminRepository
from proxima_traffic.content import StoredMedia, new_stored_contact
from proxima_traffic.models import (
    Account,
    AccountSessionSource,
    ActionKind,
    ActionLog,
    ActionStatus,
    CampaignAccount,
    CampaignMode,
    CampaignRun,
    ReactionTask,
    ReactionTaskStatus,
    RunStatus,
    Target,
    TargetList,
    ensure_utc,
    utc_now,
)
from proxima_traffic.services import ConflictError, NotFoundError, add_audit_event


async def test_broadcast_content_variants_are_manageable_as_a_rotation(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="variant-sender", telegram_user_id=4901, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@variant_room", title="Variant room")
    campaign = await repository.create_campaign(
        name="Variant rotation",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Offer A",
    )

    implicit = await repository.campaign_content_variants(campaign.id)
    assert [(item.variant_id, item.name, item.text) for item in implicit] == [
        ("primary", "Основной", "Offer A")
    ]

    variant = await repository.add_campaign_content_variant(
        campaign.id,
        name="Offer B",
        text="Second offer",
    )
    variant = await repository.add_campaign_variant_media(
        campaign.id,
        variant.variant_id,
        StoredMedia(path="stored/a.jpg", kind="photo", name="a.jpg", size=100),
    )
    variant = await repository.add_campaign_variant_media(
        campaign.id,
        variant.variant_id,
        StoredMedia(path="stored/b.mp4", kind="video", name="b.mp4", size=200),
    )
    assert [item.kind for item in variant.media] == ["photo", "video"]

    variant = await repository.rename_campaign_content_variant(
        campaign.id, variant.variant_id, "Evening offer"
    )
    variant = await repository.update_campaign_variant_text(
        campaign.id,
        variant.variant_id,
        text="Updated second offer",
        html_text="<b>Updated second offer</b>",
    )
    assert variant.name == "Evening offer"
    assert variant.text == "Updated second offer"
    assert len(variant.media) == 2

    disabled = await repository.toggle_campaign_content_variant(campaign.id, variant.variant_id)
    assert disabled.enabled is False
    with pytest.raises(ConflictError, match="хотя бы один"):
        await repository.toggle_campaign_content_variant(campaign.id, "primary")
    await repository.toggle_campaign_content_variant(campaign.id, variant.variant_id)

    assert await repository.delete_campaign_content_variant(campaign.id, variant.variant_id) == 1
    with pytest.raises(ConflictError, match="единственный"):
        await repository.delete_campaign_content_variant(campaign.id, "primary")


async def test_checked_chat_selection_is_atomic_and_replaces_only_its_campaign_audience(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="checked-selection-sender",
        telegram_user_id=4811,
        ownership_confirmed=True,
    )
    first = await repository.create_target(chat_ref="@checked_first", title="Checked first")
    second = await repository.create_target(chat_ref="@checked_second", title="Checked second")
    await repository.set_target_approval(first.id, True)
    await repository.set_target_approval(second.id, True)

    campaign = await repository.create_campaign(
        name="Checked selection",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=None,
        target_list_id=None,
        selected_target_ids=[first.id, second.id, first.id],
        selected_target_list_name="Checked selection audience",
        primary_text="Offer",
        reactions_enabled=True,
        reaction_count=2,
        reaction_emoji="🔥",
        hidden_mentions_enabled=True,
        mention_cooldown_days=14,
    )

    assert campaign.target_id is None
    assert campaign.target_list_id is not None
    assert campaign.audience_label == "Checked selection audience"
    assert campaign.audience_target_count == 2
    assert campaign.reactions_enabled is True
    assert campaign.hidden_mentions_enabled is True
    old_list_id = campaign.target_list_id
    assert (await repository.get_target_list_view(old_list_id)).approved_count == 2

    updated = await repository.update_campaign_selected_targets(
        campaign.id,
        target_ids=[second.id],
        target_list_name="Checked selection replacement",
    )

    assert updated.target_id is None
    assert updated.target_list_id is not None and updated.target_list_id != old_list_id
    assert updated.audience_label == "Checked selection replacement"
    assert updated.audience_target_count == 1
    assert (await repository.get_target_list_view(old_list_id)).approved_count == 2
    assert (await repository.get_target_list_view(updated.target_list_id)).approved_count == 1


async def test_contact_variant_can_be_managed_and_sent_as_the_only_post(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="contact-sender", telegram_user_id=4911, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@contact_room", title="Contact room")
    await repository.set_target_approval(target.id, True)
    campaign = await repository.create_campaign(
        name="Contact rotation",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Temporary text",
    )
    contact = new_stored_contact(
        phone_number="+7 999 123-45-67",
        first_name="Алиса",
        last_name="Соколова",
    )

    variant = await repository.add_campaign_contact_variant(campaign.id, contact)
    renamed = await repository.rename_campaign_content_variant(
        campaign.id, variant.variant_id, "Запись к Алисе"
    )
    assert renamed.contact == contact
    with pytest.raises(ConflictError, match="нет отдельного текста"):
        await repository.update_campaign_variant_text(
            campaign.id,
            variant.variant_id,
            text="Wrong",
            html_text=None,
        )
    with pytest.raises(ConflictError, match="нельзя добавить медиа"):
        await repository.add_campaign_variant_media(
            campaign.id,
            variant.variant_id,
            StoredMedia(path="stored/wrong.jpg", kind="photo", name="wrong.jpg", size=1),
        )

    updated_contact = new_stored_contact(
        phone_number="+7 999 765-43-21",
        first_name="Маша",
    )
    updated = await repository.update_campaign_variant_contact(
        campaign.id, variant.variant_id, updated_contact
    )
    assert updated.contact == updated_contact
    await repository.delete_campaign_content_variant(campaign.id, "primary")

    only_variant = (await repository.campaign_content_variants(campaign.id))[0]
    assert only_variant.contact == updated_contact
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "contact-only-launch")
    completed = await app.state.queue_worker.process_next()

    assert len(runs) == 1
    assert completed is not None and completed.status == RunStatus.SUCCEEDED


async def test_editing_smart_timing_preserves_manual_adaptive_settings(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="adaptive-sender", telegram_user_id=4902, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@adaptive_room", title="Adaptive room")
    campaign = await repository.create_campaign(
        name="Manual adaptive",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Offer",
        smart_timer_mode=True,
        smart_duration_seconds=24 * 3600,
        smart_no_timer_interval_seconds=5 * 60,
        activity_aware=True,
        min_messages_between_posts=7,
        activity_check_interval_seconds=45,
        visibility_max_wait_seconds=12 * 60,
        auto_sender_failover=False,
    )

    updated = await repository.update_campaign_smart_schedule(
        campaign.id,
        smart_duration_seconds=48 * 3600,
        smart_no_timer_interval_seconds=8 * 60,
    )

    assert updated.min_messages_between_posts == 7
    assert updated.activity_check_interval_seconds == 45
    assert updated.visibility_max_wait_seconds == 12 * 60
    assert updated.activity_aware is True
    assert updated.auto_sender_failover is False


async def test_sender_pool_can_be_replaced_for_future_broadcasts(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    first = await repository.create_account(
        label="first sender", telegram_user_id=4971, ownership_confirmed=True
    )
    second = await repository.create_account(
        label="second sender", telegram_user_id=4972, ownership_confirmed=True
    )
    replacement = await repository.create_account(
        label="replacement sender", telegram_user_id=4973, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@sender_pool_room", title="Sender pool room")
    campaign = await repository.create_campaign(
        name="Editable sender pool",
        mode=CampaignMode.BROADCAST,
        primary_account_id=first.id,
        sender_account_ids=[second.id],
        target_id=target.id,
        target_list_id=None,
        primary_text="Offer",
    )

    updated = await repository.update_campaign_senders(
        campaign.id,
        [replacement.id, second.id, replacement.id],
    )

    assert updated.primary_account_id == replacement.id
    assert await repository.campaign_sender_ids(campaign.id) == [replacement.id, second.id]

    await repository.toggle_account(second.id)
    with pytest.raises(ConflictError, match="активные подтверждённые"):
        await repository.update_campaign_senders(campaign.id, [second.id])


async def test_template_library_and_target_batch_import(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)

    template = await repository.create_template(
        name="Reusable pair",
        primary_text="First message",
        reply_text="Direct reply",
    )
    assert template.enabled is True

    page = await repository.list_templates(0, 7)
    assert page.total == 1
    assert page.items[0].name == "Reusable pair"
    updated = await repository.update_template_texts(
        template.id,
        primary_text="Updated first",
        reply_text="Updated reply",
    )
    assert updated.primary_text == "Updated first"

    added, skipped = await repository.import_targets(["@owned_one", "@owned_two", "@owned_one"])
    assert (added, skipped) == (2, 0)

    added_again, skipped_again = await repository.import_targets(["@owned_one", "@owned_two"])
    assert (added_again, skipped_again) == (0, 2)

    targets = await repository.list_targets(0, 7)
    assert targets.total == 2
    assert all(target.approved_by_operator is False for target in targets.items)

    stats = await repository.dashboard()
    assert stats.templates_total == 1
    assert stats.targets_total == 2
    assert stats.targets_approved == 0


async def test_existing_standalone_target_is_found_before_duplicate_creation(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    target = await repository.create_target(chat_ref="@already_added", title="Already added")

    existing = await repository.find_target_by_chat_ref("@already_added")

    assert existing is not None
    assert existing.id == target.id
    with pytest.raises(ConflictError, match="этот чат уже добавлен"):
        await repository.create_target(chat_ref="@already_added", title="Duplicate")


async def test_manual_target_creation_does_not_require_an_operator_alias(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)

    target = await repository.create_target(chat_ref="@telegram_named_chat")

    assert target.title == "@telegram_named_chat"


async def test_named_target_list_fans_out_to_one_run_per_approved_target(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="fanout-primary", telegram_user_id=501, ownership_confirmed=True
    )
    reply = await repository.create_account(
        label="fanout-reply", telegram_user_id=502, ownership_confirmed=True
    )
    target_list, added, reused = await repository.create_target_list(
        name="Owned rooms",
        chat_refs=["@owned_a", "@owned_b"],
    )
    assert (added, reused) == (2, 0)
    assert await repository.set_target_list_approval(target_list.id, True) == 2

    campaign = await repository.create_campaign(
        name="Fanout campaign",
        primary_account_id=primary.id,
        reply_account_id=reply.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="First",
        reply_text="Reply",
        reply_delay_seconds=0,
    )
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "fanout-request")
    assert len(runs) == 2
    assert len({run.target_id for run in runs}) == 2

    launches = await repository.list_launches(0, 7)
    assert launches.total == 1
    assert launches.items[0].total == 2
    assert launches.items[0].queued == 2
    launch, launch_runs = await repository.get_launch(runs[-1].id)
    assert launch.campaign_name == "Fanout campaign"
    assert [run.id for run in launch_runs] == [run.id for run in runs]

    processed = []
    for _ in range(4):
        processed.append(await app.state.queue_worker.process_next())
    assert all(item is not None for item in processed)
    stored_runs = await repository.list_runs(0, 10)
    assert {run.status.value for run in stored_runs.items} == {"succeeded"}


async def test_one_sender_queues_a_separate_broadcast_for_every_selected_chat(
    app: FastAPI,
) -> None:
    """One account is a valid sender pool; pacing serializes its deliveries."""

    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="one-sender", telegram_user_id=503, ownership_confirmed=True
    )
    target_list, added, _ = await repository.create_target_list(
        name="One sender rooms",
        chat_refs=["@one_sender_a", "@one_sender_b", "@one_sender_c"],
    )
    assert added == 3
    assert await repository.set_target_list_approval(target_list.id, True) == 3
    campaign = await repository.create_campaign(
        name="One sender fanout",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        sender_account_ids=[account.id],
        target_id=None,
        target_list_id=target_list.id,
        primary_text="One account, many chats",
        spread_seconds=0,
    )

    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "one-sender-fanout")

    assert len(runs) == 3
    assert len({run.target_id for run in runs}) == 3
    assert {run.sender_account_id for run in runs} == {account.id}
    assert all(run.status == RunStatus.QUEUED for run in runs)


async def test_bulk_standalone_chat_activation_never_releases_bad_targets(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    await repository.import_targets(["@bulk_good_a", "@bulk_bad", "@bulk_good_b"])
    targets = await repository.list_targets(0, 10)
    bad_target = next(target for target in targets.items if target.chat_ref == "@bulk_bad")
    async with app.state.database.session_factory() as session:
        stored_bad = await session.get(Target, bad_target.id)
        assert stored_bad is not None
        stored_bad.quality_status = "bad"
        stored_bad.quality_reason = "test quarantine"
        stored_bad.approved_by_operator = True
        await session.commit()

    changed, skipped_bad = await repository.set_standalone_targets_approval(True)

    assert changed == 2
    assert skipped_bad == 1
    targets = await repository.list_targets(0, 10)
    by_ref = {target.chat_ref: target for target in targets.items}
    assert by_ref["@bulk_good_a"].approved_by_operator is True
    assert by_ref["@bulk_good_b"].approved_by_operator is True
    assert by_ref["@bulk_bad"].approved_by_operator is False

    changed, skipped_bad = await repository.set_standalone_targets_approval(False)
    assert changed == 2
    assert skipped_bad == 0
    assert not any(
        target.approved_by_operator for target in (await repository.list_targets(0, 10)).items
    )


async def test_target_list_contents_can_be_browsed_and_one_chat_removed(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    target_list, _, _ = await repository.create_target_list(
        name="Manage rooms",
        chat_refs=["@manage_a", "@manage_b"],
    )
    await repository.set_target_list_approval(target_list.id, True)

    page = await repository.list_target_list_targets(target_list.id, 0, 7)
    assert page.total == 2
    assert (await repository.list_targets(0, 7)).total == 0
    removed = page.items[0]
    kept = page.items[1]

    await repository.remove_target_from_list(target_list.id, removed.id)

    updated = await repository.list_target_list_targets(target_list.id, 0, 7)
    view = await repository.get_target_list_view(target_list.id)
    assert updated.total == 1
    assert updated.items[0].id == kept.id
    assert view.item_count == 1
    assert view.approved_count == 1
    with pytest.raises(NotFoundError):
        await repository.get_target(removed.id)
    with pytest.raises(NotFoundError):
        await repository.remove_target_from_list(target_list.id, removed.id)


async def test_target_list_catalog_filters_searches_and_bulk_updates_page(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    target_list, _, _ = await repository.create_target_list(
        name="Working catalog",
        chat_refs=["@zeta_room", "@alpha_room", "@beta_room", "@gamma_room"],
    )
    await repository.set_target_list_approval(target_list.id, True)
    all_members = await repository.list_target_list_targets(target_list.id, 0, 10)
    by_ref = {target.chat_ref: target for target in all_members.items}
    await repository.set_target_list_target_approval(
        target_list.id,
        by_ref["@alpha_room"].id,
        False,
    )
    async with app.state.database.session_factory() as session:
        bad = await session.get(Target, by_ref["@beta_room"].id)
        problem = await session.get(Target, by_ref["@gamma_room"].id)
        assert bad is not None and problem is not None
        bad.quality_status = "bad"
        bad.quality_reason = "test quarantine"
        problem.last_error_code = "target_unavailable"
        await session.commit()

    view = await repository.get_target_list_view(target_list.id)
    assert view.item_count == 4
    assert view.approved_count == 2
    assert view.disabled_count == 1
    assert view.bad_count == 1
    assert view.problem_count == 1

    all_page = await repository.list_target_list_targets(target_list.id, 0, 10)
    enabled = await repository.list_target_list_targets(
        target_list.id, 0, 10, filter_name="enabled"
    )
    disabled = await repository.list_target_list_targets(
        target_list.id, 0, 10, filter_name="disabled"
    )
    bad = await repository.list_target_list_targets(target_list.id, 0, 10, filter_name="bad")
    problem = await repository.list_target_list_targets(
        target_list.id, 0, 10, filter_name="problem"
    )
    assert [target.title for target in all_page.items] == [
        "@alpha_room",
        "@beta_room",
        "@gamma_room",
        "@zeta_room",
    ]
    assert {target.chat_ref for target in enabled.items} == {"@gamma_room", "@zeta_room"}
    assert [target.chat_ref for target in disabled.items] == ["@alpha_room"]
    assert [target.chat_ref for target in bad.items] == ["@beta_room"]
    assert [target.chat_ref for target in problem.items] == ["@gamma_room"]
    assert [
        target.chat_ref
        for target in await repository.search_target_list_targets(target_list.id, "amm")
    ] == ["@gamma_room"]

    changed, skipped = await repository.set_target_list_targets_approval(
        target_list.id,
        [target.id for target in all_page.items],
        True,
    )
    assert changed == 1
    assert skipped == 1
    assert (await repository.get_target_list_view(target_list.id)).approved_count == 3
    quarantined = await repository.list_target_list_targets(
        target_list.id,
        0,
        10,
        filter_name="bad",
    )
    assert quarantined.items[0].approved_by_operator is False

    assert await repository.set_target_list_approval(target_list.id, False) == 3
    disabled_view = await repository.get_target_list_view(target_list.id)
    assert disabled_view.approved_count == 0
    assert disabled_view.disabled_count == 3
    assert await repository.set_target_list_approval(target_list.id, True) == 3
    enabled_view = await repository.get_target_list_view(target_list.id)
    assert enabled_view.approved_count == 3
    assert enabled_view.bad_count == 1
    quarantined = await repository.list_target_list_targets(
        target_list.id,
        0,
        10,
        filter_name="bad",
    )
    assert quarantined.items[0].approved_by_operator is False
    renamed = await repository.rename_target_list(target_list.id, "  Main   active chats  ")
    assert renamed.name == "Main active chats"


async def test_deleting_target_list_cleans_hidden_orphans_but_preserves_manual_chat(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    manual = await repository.create_target(chat_ref="@shared_room", title="Shared")
    target_list, _, _ = await repository.create_target_list(
        name="Disposable audience",
        chat_refs=["@shared_room", "@list_only_room"],
    )
    members = await repository.list_target_list_targets(target_list.id, 0, 7)
    list_only = next(item for item in members.items if item.chat_ref == "@list_only_room")

    standalone = await repository.list_targets(0, 7)
    assert [item.id for item in standalone.items] == [manual.id]

    await repository.delete_target_list(target_list.id)

    assert (await repository.get_target(manual.id)).is_standalone is True
    with pytest.raises(NotFoundError):
        await repository.get_target(list_only.id)
    with pytest.raises(NotFoundError):
        await repository.get_target_list_view(target_list.id)


async def test_campaign_target_can_change_without_rewriting_previous_launch(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="editable-primary", telegram_user_id=551, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Original rooms", chat_refs=["@original_a", "@original_b"]
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Editable broadcast",
        mode=CampaignMode.BROADCAST,
        primary_account_id=primary.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Hello",
    )
    original_runs = await app.state.run_queue.enqueue_campaign(campaign.id, "editable-original")
    replacement = await repository.create_target(
        chat_ref="@replacement_room", title="Replacement room"
    )
    await repository.set_target_approval(replacement.id, True)

    updated = await repository.update_campaign_target(
        campaign.id,
        target_id=replacement.id,
        target_list_id=None,
    )
    new_runs = await app.state.run_queue.enqueue_campaign(campaign.id, "editable-new")

    assert updated.target_id == replacement.id
    assert updated.target_list_id is None
    assert {run.target_id for run in original_runs} != {replacement.id}
    assert [run.target_id for run in new_runs] == [replacement.id]


async def test_queued_launch_keeps_its_target_snapshot_after_list_item_is_disabled(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="snapshot-primary", telegram_user_id=552, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Snapshot rooms", chat_refs=["@snapshot_room"]
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Snapshot broadcast",
        mode=CampaignMode.BROADCAST,
        primary_account_id=primary.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Hello",
    )
    queued = await app.state.run_queue.enqueue_campaign(campaign.id, "snapshot-launch")
    assert len(queued) == 1

    await repository.set_target_list_target_approval(
        target_list.id,
        queued[0].target_id,
        False,
    )

    completed = await app.state.queue_worker.process_next()
    assert completed is not None
    assert completed.id == queued[0].id
    assert completed.status.value == "succeeded"


async def test_live_campaign_account_picker_only_returns_checked_sessions(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    await repository.create_account(
        label="metadata-only", telegram_user_id=601, ownership_confirmed=True
    )
    async with app.state.database.session_factory() as session:
        pending = Account(
            label="pending-session",
            telegram_user_id=602,
            ownership_confirmed=True,
            session_source=AccountSessionSource.TDATA,
            session_encrypted="encrypted-session",
            api_id=12345,
            api_hash_encrypted="encrypted-api-hash",
            authorized=False,
        )
        ready = Account(
            label="ready-session",
            telegram_user_id=603,
            ownership_confirmed=True,
            session_source=AccountSessionSource.TDATA,
            session_encrypted="encrypted-session",
            api_id=12345,
            api_hash_encrypted="encrypted-api-hash",
            authorized=True,
        )
        session.add_all([pending, ready])
        await session.commit()

    dry_run_accounts = await repository.active_owned_accounts()
    live_accounts = await repository.active_owned_accounts(require_authorized_session=True)

    assert {account.label for account in dry_run_accounts} == {
        "metadata-only",
        "pending-session",
        "ready-session",
    }
    assert [account.label for account in live_accounts] == ["ready-session"]


async def test_bot_repository_broadcast_uses_multiple_senders_over_spread_window(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="broadcast-primary", telegram_user_id=701, ownership_confirmed=True
    )
    secondary = await repository.create_account(
        label="broadcast-secondary", telegram_user_id=702, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Broadcast rooms",
        chat_refs=["@room_1", "@room_2", "@room_3"],
    )
    await repository.set_target_list_approval(target_list.id, True)

    campaign = await repository.create_campaign(
        name="Broadcast from bot",
        mode=CampaignMode.BROADCAST,
        primary_account_id=primary.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Hello",
        spread_seconds=24 * 3600,
        sender_account_ids=[secondary.id],
    )
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "bot-broadcast-pool")

    assert [run.sender_account_id for run in runs] == [
        primary.id,
        secondary.id,
        primary.id,
    ]
    scheduled = [run.scheduled_for for run in runs]
    assert scheduled[0] is not None and scheduled[1] is not None and scheduled[2] is not None
    assert 43199 <= (scheduled[1] - scheduled[0]).total_seconds() <= 43201
    assert 86399 <= (scheduled[2] - scheduled[0]).total_seconds() <= 86401


async def test_bot_repository_deletes_drafts_but_preserves_launch_history(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="delete-primary", telegram_user_id=801, ownership_confirmed=True
    )
    reply = await repository.create_account(
        label="delete-reply", telegram_user_id=802, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@delete_room", title="Delete room")
    await repository.set_target_approval(target.id, True)
    template = await repository.create_template(
        name="Temporary text", primary_text="First", reply_text="Reply"
    )

    await repository.delete_template(template.id)
    assert (await repository.list_templates(0, 7)).total == 0

    draft = await repository.create_campaign(
        name="Temporary campaign",
        primary_account_id=primary.id,
        reply_account_id=reply.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="First",
        reply_text="Reply",
    )
    await repository.delete_campaign(draft.id)
    assert (await repository.list_campaigns(0, 7)).total == 0

    launched = await repository.create_campaign(
        name="Campaign with history",
        primary_account_id=primary.id,
        reply_account_id=reply.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="First",
        reply_text="Reply",
    )
    await app.state.run_queue.enqueue_campaign(launched.id, "history-launch")
    with pytest.raises(ConflictError, match="история"):
        await repository.delete_campaign(launched.id)


async def test_cancel_launch_stops_the_whole_target_list_batch(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="batch-primary", telegram_user_id=901, ownership_confirmed=True
    )
    reply = await repository.create_account(
        label="batch-reply", telegram_user_id=902, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Batch rooms", chat_refs=["@batch_one", "@batch_two", "@batch_three"]
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Batch cancel",
        primary_account_id=primary.id,
        reply_account_id=reply.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="First",
        reply_text="Reply",
    )
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "cancel-batch")

    assert await app.state.run_queue.cancel_launch(runs[0].id) == 3
    launch, stored_runs = await repository.get_launch(runs[0].id)
    assert launch.cancelled == 3
    assert launch.active == 0
    assert {run.status.value for run in stored_runs} == {"cancelled"}


async def test_saved_broadcast_schedule_can_change_and_retry_only_failures(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="smart-repository", telegram_user_id=951, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Retry rooms", chat_refs=["@retry_one", "@retry_two"]
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Editable smart schedule",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Hello",
    )

    campaign = await repository.update_campaign_schedule(
        campaign.id,
        repeat_count=2,
        repeat_interval_seconds=3600,
    )
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "retry-smart-launch")
    assert len(runs) == 4

    async with app.state.database.session_factory() as session:
        failed = await session.get(type(runs[0]), runs[0].id)
        succeeded = await session.get(type(runs[1]), runs[1].id)
        assert failed is not None and succeeded is not None
        failed.status = RunStatus.FAILED
        failed.error = "temporary test failure"
        failed.finished_at = utc_now()
        succeeded.status = RunStatus.SUCCEEDED
        succeeded.finished_at = utc_now()
        await session.commit()

    assert await app.state.run_queue.retry_failed_launch(runs[0].id) == 1
    _, stored = await repository.get_launch(runs[0].id)
    by_id = {run.id: run for run in stored}
    assert by_id[runs[0].id].status == RunStatus.QUEUED
    assert by_id[runs[0].id].attempt_count == 0
    assert by_id[runs[1].id].status == RunStatus.SUCCEEDED


async def test_timer_driven_launch_report_tracks_dynamic_posts_and_failover(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    primary = await repository.create_account(
        label="report-primary", telegram_user_id=961, ownership_confirmed=True
    )
    replacement = await repository.create_account(
        label="report-replacement", telegram_user_id=962, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Report rooms", chat_refs=["@report_one", "@report_two"]
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Live smart report",
        mode=CampaignMode.BROADCAST,
        primary_account_id=primary.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Fresh promotion",
        smart_timer_mode=True,
        smart_duration_seconds=24 * 3600,
        smart_no_timer_interval_seconds=15 * 60,
        auto_sender_failover=True,
        sender_account_ids=[replacement.id],
    )
    initial_runs = await app.state.run_queue.enqueue_campaign(campaign.id, "smart-report-launch")
    assert len(initial_runs) == 2

    sent = await app.state.queue_worker.process_next()
    assert sent is not None
    assert sent.status == RunStatus.SUCCEEDED

    _, dynamic_runs = await repository.get_launch(initial_runs[0].id)
    assert len(dynamic_runs) == 3
    failover_run = next(run for run in dynamic_runs if run.id != sent.id)
    failed_account_id = failover_run.sender_account_id
    assert failed_account_id is not None
    replacement_account_id = primary.id if failed_account_id != primary.id else replacement.id
    async with app.state.database.session_factory() as session:
        stored = await session.get(type(failover_run), failover_run.id)
        assert stored is not None
        stored.sender_account_id = replacement_account_id
        add_audit_event(
            session,
            event_type="run.sender_failed_over",
            entity_type="campaign_run",
            entity_id=stored.id,
            details={
                "error_code": "account_unauthorized",
                "failed_account_id": failed_account_id,
                "replacement_account_id": replacement_account_id,
                "scheduled_for": ensure_utc(stored.scheduled_for).isoformat(),
            },
            actor="worker",
        )
        session.add(
            ActionLog(
                run_id=stored.id,
                account_id=failed_account_id,
                target_id=stored.target_id,
                kind=ActionKind.PRIMARY_MESSAGE,
                status=ActionStatus.FAILED,
                text="Fresh promotion",
                error="session expired during first attempt",
                attempt_number=1,
            )
        )
        await session.commit()

    page = await repository.list_launches(0, 7)
    assert page.total == 1
    summary = page.items[0]
    assert summary.smart_timer_mode is True
    assert summary.total == 3
    assert summary.sent_publications == 1
    assert summary.active == 2
    assert summary.target_count == 2
    assert summary.account_count == 2
    assert summary.failover_count == 1
    assert summary.next_scheduled_for is not None
    assert summary.active_until is not None

    details, stored_runs = await repository.get_launch(initial_runs[0].id)
    assert len(stored_runs) == 3
    assert details.anchor_run_id == max(run.id for run in stored_runs)
    assert details.no_timer_interval_seconds == 15 * 60
    assert details.auto_sender_failover is True
    assert details.sent_publications == 1
    assert details.failover_count == 1
    assert details.failovers[0].failed_account_id == failed_account_id
    assert details.failovers[0].replacement_account_id == replacement_account_id
    assert len(details.attempt_failures) == 1
    assert details.attempt_failures[0].account_id == failed_account_id
    assert details.attempt_failures[0].error == "session expired during first attempt"
    assert dict(details.account_labels) == {
        primary.id: "report-primary",
        replacement.id: "report-replacement",
    }


async def test_same_chat_can_be_enabled_independently_in_two_lists(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    first, _, _ = await repository.create_target_list(
        name="Audience A",
        chat_refs=["@shared_independent"],
    )
    second, _, _ = await repository.create_target_list(
        name="Audience B",
        chat_refs=["@shared_independent"],
    )
    first_member = (await repository.list_target_list_targets(first.id, 0, 7)).items[0]

    await repository.set_target_list_target_approval(first.id, first_member.id, True)

    first_view = await repository.get_target_list_view(first.id)
    second_view = await repository.get_target_list_view(second.id)
    assert first_view.approved_count == 1
    assert second_view.approved_count == 0
    assert (await repository.get_target_list_target(first.id, first_member.id)).approved_by_operator
    assert not (
        await repository.get_target_list_target(second.id, first_member.id)
    ).approved_by_operator

    await repository.set_target_list_approval(second.id, True)
    await repository.set_target_list_approval(first.id, False)
    assert (await repository.get_target_list_view(first.id)).approved_count == 0
    assert (await repository.get_target_list_view(second.id)).approved_count == 1


async def test_campaigns_and_launches_stay_sorted_by_real_creation_time(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="sort-account", telegram_user_id=9901, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@sort_room", title="Sort room")
    await repository.set_target_approval(target.id, True)
    older = await repository.create_campaign(
        name="Older smart launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Older",
        smart_timer_mode=True,
        smart_duration_seconds=3600,
        smart_no_timer_interval_seconds=900,
    )
    older_runs = await app.state.run_queue.enqueue_campaign(older.id, "older-smart-key")
    newer = await repository.create_campaign(
        name="Newer launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Newer",
    )
    await app.state.run_queue.enqueue_campaign(newer.id, "newer-launch-key")

    processed = await app.state.queue_worker.process_next()
    assert processed is not None and processed.campaign_id == older.id
    older_details, older_dynamic_runs = await repository.get_launch(older_runs[0].id)
    assert len(older_dynamic_runs) == 2
    assert older_details.anchor_run_id > older_runs[0].id

    campaigns = await repository.list_campaigns(0, 7)
    launches = await repository.list_launches(0, 7)
    assert [item.id for item in campaigns.items[:2]] == [newer.id, older.id]
    assert [item.campaign_id for item in launches.items[:2]] == [newer.id, older.id]


async def test_completed_launch_can_be_archived_and_restored_as_a_batch(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="archive-account", telegram_user_id=9902, ownership_confirmed=True
    )
    target_list, _, _ = await repository.create_target_list(
        name="Archive audience",
        chat_refs=["@archive_a", "@archive_b"],
    )
    await repository.set_target_list_approval(target_list.id, True)
    campaign = await repository.create_campaign(
        name="Archive launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Archive me",
    )
    runs = await app.state.run_queue.enqueue_campaign(campaign.id, "archive-launch-key")

    with pytest.raises(ConflictError, match="активный"):
        await repository.set_launch_archived(runs[0].id, True)
    assert await app.state.run_queue.cancel_launch(runs[0].id) == 2
    assert await repository.set_launch_archived(runs[0].id, True) == 2
    assert (await repository.list_launches(0, 7)).total == 0
    archived = await repository.list_launches(0, 7, archived=True)
    assert archived.total == 1 and archived.items[0].archived is True

    assert await repository.set_launch_archived(runs[-1].id, False) == 2
    restored = await repository.list_launches(0, 7)
    assert restored.total == 1 and restored.items[0].archived is False
    assert (await repository.list_launches(0, 7, archived=True)).total == 0


async def test_launch_cannot_be_archived_while_its_reactions_are_pending(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    source = await repository.create_account(
        label="reaction-archive-source", telegram_user_id=9910, ownership_confirmed=True
    )
    reactor = await repository.create_account(
        label="reaction-archive-reactor", telegram_user_id=9911, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@reaction_archive", title="Reaction archive")
    campaign = await repository.create_campaign(
        name="Reaction archive launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=source.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Published",
    )
    async with app.state.database.session_factory() as session:
        run = CampaignRun(
            campaign_id=campaign.id,
            target_id=target.id,
            sender_account_id=source.id,
            request_key="reaction-archive-launch",
            launch_key="reaction-archive-launch",
            status=RunStatus.SUCCEEDED,
        )
        session.add(run)
        await session.flush()
        session.add(
            ReactionTask(
                campaign_id=campaign.id,
                run_id=run.id,
                target_id=target.id,
                source_account_id=source.id,
                reactor_account_id=reactor.id,
                message_id=123,
                emoji="👍",
                status=ReactionTaskStatus.QUEUED,
            )
        )
        await session.commit()
        run_id = int(run.id)

    launch, _ = await repository.get_launch(run_id)
    assert launch.active == 0 and launch.reaction_active == 1
    with pytest.raises(ConflictError, match="реакции"):
        await repository.set_launch_archived(run_id, True)
    assert await app.state.run_queue.cancel_launch(run_id) == 1
    assert await repository.set_launch_archived(run_id, True) == 1


async def test_launch_search_builds_summaries_in_bulk_without_get_launch(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="bulk-search-account", telegram_user_id=9920, ownership_confirmed=True
    )
    second = await repository.create_account(
        label="bulk-search-second", telegram_user_id=9921, ownership_confirmed=True
    )
    replacement = await repository.create_account(
        label="bulk-search-replacement", telegram_user_id=9922, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@bulk_search", title="Bulk search")
    await repository.set_target_approval(target.id, True)
    campaign = await repository.create_campaign(
        name="Bulk searchable launch",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        sender_account_ids=[second.id],
        target_id=target.id,
        target_list_id=None,
        primary_text="Search me",
    )
    await app.state.run_queue.enqueue_campaign(campaign.id, "bulk-search-launch")
    async with app.state.database.session_factory() as session:
        await session.execute(
            delete(CampaignAccount).where(CampaignAccount.campaign_id == campaign.id)
        )
        session.add(CampaignAccount(campaign_id=campaign.id, account_id=replacement.id))
        await session.commit()

    launches = await repository.list_launches(0, 7)
    assert launches.items[0].account_count == 2

    async def forbidden_get_launch(*args: object, **kwargs: object):
        del args, kwargs
        raise AssertionError("search_launches must not perform one get_launch per result")

    monkeypatch.setattr(repository, "get_launch", forbidden_get_launch)
    results = await repository.search_launches("Bulk searchable")
    assert len(results) == 1
    assert results[0].campaign_id == campaign.id
    assert results[0].total == 1
    assert results[0].account_count == 2


async def test_target_list_details_are_not_limited_to_first_500_rows(app: FastAPI) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    async with app.state.database.session_factory() as session:
        rows = [TargetList(name=f"Large list {index:03d}") for index in range(501)]
        session.add_all(rows)
        await session.commit()
        oldest_id = rows[0].id

    view = await repository.get_target_list_view(oldest_id)
    assert view.name == "Large list 000"
    assert view.item_count == 0


async def test_legacy_request_key_cannot_merge_with_a_modern_launch_key(
    app: FastAPI,
) -> None:
    repository = AdminRepository(app.state.database.session_factory)
    account = await repository.create_account(
        label="collision-account", telegram_user_id=9903, ownership_confirmed=True
    )
    target = await repository.create_target(chat_ref="@collision_direct", title="Collision direct")
    await repository.set_target_approval(target.id, True)
    legacy_campaign = await repository.create_campaign(
        name="Legacy collision",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=target.id,
        target_list_id=None,
        primary_text="Legacy",
    )
    legacy_run = (
        await app.state.run_queue.enqueue_campaign(
            legacy_campaign.id,
            "shared-collision-key",
        )
    )[0]
    async with app.state.database.session_factory() as session:
        stored = await session.get(type(legacy_run), legacy_run.id)
        assert stored is not None
        stored.launch_key = None
        await session.commit()

    target_list, _, _ = await repository.create_target_list(
        name="Modern collision audience",
        chat_refs=["@collision_a", "@collision_b"],
    )
    await repository.set_target_list_approval(target_list.id, True)
    modern_campaign = await repository.create_campaign(
        name="Modern collision",
        mode=CampaignMode.BROADCAST,
        primary_account_id=account.id,
        target_id=None,
        target_list_id=target_list.id,
        primary_text="Modern",
    )
    modern_runs = await app.state.run_queue.enqueue_campaign(
        modern_campaign.id,
        "shared-collision-key",
    )

    legacy_view, legacy_members = await repository.get_launch(legacy_run.id)
    modern_view, modern_members = await repository.get_launch(modern_runs[0].id)
    assert legacy_view.campaign_id == legacy_campaign.id
    assert [item.id for item in legacy_members] == [legacy_run.id]
    assert modern_view.campaign_id == modern_campaign.id
    assert [item.id for item in modern_members] == [item.id for item in modern_runs]
    assert (await repository.dashboard()).active_launches == 2

    await app.state.run_queue.cancel(legacy_run.id)
    await repository.set_launch_archived(legacy_run.id, True)
    modern_view, _ = await repository.get_launch(modern_runs[0].id)
    assert modern_view.archived is False
