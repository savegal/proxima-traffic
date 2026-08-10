import asyncio
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI
from sqlalchemy import text

from proxima_traffic.content import (
    new_content_variant,
    parse_content_variants,
    serialize_content_variants,
)
from proxima_traffic.models import (
    Campaign,
    CampaignRun,
    Target,
    TargetList,
    TargetListItem,
    utc_now,
)


async def test_health_is_public_and_api_is_protected(client: httpx.AsyncClient) -> None:
    health = await client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"status": "ok", "dry_run": True}

    ready = await client.get("/ready")
    assert ready.status_code == 200
    assert ready.json() == {
        "status": "ready",
        "database": True,
        "worker": True,
        "dry_run": True,
    }

    protected = await client.get("/api/v1/status")
    assert protected.status_code == 401


async def test_readiness_rejects_an_outdated_database_revision(
    app: FastAPI,
    client: httpx.AsyncClient,
) -> None:
    async with app.state.database.engine.begin() as connection:
        await connection.execute(
            text("UPDATE alembic_version SET version_num = '0046_delayed_delivery_leases'")
        )

    ready = await client.get("/ready")

    assert ready.status_code == 503
    assert ready.json()["status"] == "not_ready"
    assert ready.json()["database"] is False


async def test_worker_diagnostics_are_protected(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    denied = await client.get("/api/v1/worker")
    assert denied.status_code == 401
    response = await client.get("/api/v1/worker", headers=admin_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["worker_id"].startswith("local-")
    assert body["running"] is False
    assert body["healthy"] is True
    assert body["background_workers"] == {}
    assert body["active_steps"] == 0
    assert body["expired_leases"] == 0


async def test_worker_diagnostics_detect_a_stopped_background_worker(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    app.state.settings.api_worker_enabled = True
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    app.state.background_tasks["reactions"] = task

    ready = await client.get("/ready")
    assert ready.status_code == 503
    assert ready.json()["status"] == "not_ready"
    assert ready.json()["worker"] is False

    response = await client.get("/api/v1/worker", headers=admin_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["healthy"] is False
    assert body["background_workers"] == {"reactions": False}


async def test_two_account_reply_workflow_is_approved_logged_and_idempotent(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "primary", 101)
    reply = await _create_account(client, admin_headers, "reply", 202)

    target_response = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@owned_test_chat", "title": "Owned test chat"},
    )
    assert target_response.status_code == 201
    target = target_response.json()
    assert target["manual_cooldown_seconds"] == 0

    cooldown = await client.patch(
        f"/api/v1/targets/{target['id']}/cooldown",
        headers=admin_headers,
        json={"manual_cooldown_seconds": 3600},
    )
    assert cooldown.status_code == 200
    assert cooldown.json()["manual_cooldown_seconds"] == 3600

    campaign_response = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "first-reply-test",
            "primary_account_id": primary["id"],
            "reply_account_id": reply["id"],
            "target_id": target["id"],
            "primary_text": "First message",
            "reply_text": "Direct reply",
            "reply_delay_seconds": 0,
        },
    )
    assert campaign_response.status_code == 201
    campaign = campaign_response.json()

    missing_request_key = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={},
    )
    assert missing_request_key.status_code == 422

    blocked = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={"request_key": "same-request-001"},
    )
    assert blocked.status_code == 409
    assert blocked.json()["detail"] == "target must be explicitly approved"

    approved = await client.post(
        f"/api/v1/targets/{target['id']}/approve",
        headers=admin_headers,
    )
    assert approved.status_code == 200
    assert approved.json()["approved_by_operator"] is True

    first_run = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={"request_key": "same-request-001"},
    )
    assert first_run.status_code == 202
    run = first_run.json()[0]
    assert run["status"] == "queued"

    first_step = await app.state.queue_worker.process_next()
    assert first_step.status.value == "queued"
    assert first_step.next_step.value == "reply"
    assert first_step.primary_message_id is not None

    second_step = await app.state.queue_worker.process_next()
    assert second_step.status.value == "succeeded"
    assert second_step.reply_message_id is not None

    replay = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={"request_key": "same-request-001"},
    )
    assert replay.status_code == 202
    assert replay.json()[0]["id"] == run["id"]

    details_response = await client.get(
        f"/api/v1/runs/{run['id']}",
        headers=admin_headers,
    )
    assert details_response.status_code == 200
    actions = details_response.json()["actions"]
    assert len(actions) == 2
    assert actions[0]["kind"] == "primary_message"
    assert actions[0]["reply_to_message_id"] is None
    assert actions[1]["kind"] == "reply_message"
    assert actions[1]["reply_to_message_id"] == actions[0]["external_message_id"]


async def test_campaign_rejects_missing_database_relations(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "invalid-relations",
            "primary_account_id": 404,
            "reply_account_id": 405,
            "target_id": 406,
            "primary_text": "First message",
            "reply_text": "Direct reply",
        },
    )
    assert response.status_code == 409


async def test_patch_updates_reject_explicit_nulls(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    account = await _create_account(client, admin_headers, "null-account", 611)
    account_update = await client.patch(
        f"/api/v1/accounts/{account['id']}",
        headers=admin_headers,
        json={"status": None},
    )
    assert account_update.status_code == 422

    template = await client.post(
        "/api/v1/templates",
        headers=admin_headers,
        json={"name": "Null guard", "primary_text": "First", "reply_text": "Reply"},
    )
    assert template.status_code == 201
    template_update = await client.patch(
        f"/api/v1/templates/{template.json()['id']}",
        headers=admin_headers,
        json={"primary_text": None},
    )
    assert template_update.status_code == 422


async def test_api_rejects_broken_spintax_in_templates_and_legacy_dialogues(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    broken_template = await client.post(
        "/api/v1/templates",
        headers=admin_headers,
        json={"name": "Broken", "primary_text": "{broken", "reply_text": "Reply"},
    )
    assert broken_template.status_code == 422

    template = await client.post(
        "/api/v1/templates",
        headers=admin_headers,
        json={"name": "Valid", "primary_text": "{A|B}", "reply_text": "Reply"},
    )
    assert template.status_code == 201
    broken_update = await client.patch(
        f"/api/v1/templates/{template.json()['id']}",
        headers=admin_headers,
        json={"reply_text": "{empty"},
    )
    assert broken_update.status_code == 422

    primary = await _create_account(client, admin_headers, "spintax-primary", 8611)
    reply = await _create_account(client, admin_headers, "spintax-reply", 8612)
    target = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@legacy_spintax_guard", "title": "Legacy Spintax"},
    )
    assert target.status_code == 201
    broken_dialogue = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Broken legacy dialogue",
            "mode": "dialogue",
            "primary_account_id": primary["id"],
            "reply_account_id": reply["id"],
            "target_id": target.json()["id"],
            "primary_text": "{broken",
            "reply_text": "Reply",
        },
    )
    assert broken_dialogue.status_code == 422


async def test_dialogue_rejects_broadcast_only_timing(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "dialogue-primary", 731)
    reply = await _create_account(client, admin_headers, "dialogue-reply", 732)
    target = (
        await client.post(
            "/api/v1/targets",
            headers=admin_headers,
            json={"chat_ref": "@dialogue_mode_guard", "title": "Dialogue guard"},
        )
    ).json()

    create = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Bad dialogue spread",
            "mode": "dialogue",
            "primary_account_id": primary["id"],
            "reply_account_id": reply["id"],
            "target_id": target["id"],
            "primary_text": "Primary",
            "reply_text": "Reply",
            "spread_seconds": 3600,
        },
    )
    assert create.status_code == 422

    good = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Good dialogue",
            "mode": "dialogue",
            "primary_account_id": primary["id"],
            "reply_account_id": reply["id"],
            "target_id": target["id"],
            "primary_text": "Primary",
            "reply_text": "Reply",
        },
    )
    assert good.status_code == 201
    update = await client.patch(
        f"/api/v1/campaigns/{good.json()['id']}",
        headers=admin_headers,
        json={"spread_seconds": 3600},
    )
    assert update.status_code == 422
    sender_pool_update = await client.patch(
        f"/api/v1/campaigns/{good.json()['id']}",
        headers=admin_headers,
        json={"sender_account_ids": [primary["id"]]},
    )
    assert sender_pool_update.status_code == 422
    null_update = await client.patch(
        f"/api/v1/campaigns/{good.json()['id']}",
        headers=admin_headers,
        json={"reply_text": None},
    )
    assert null_update.status_code == 422


async def test_template_and_target_list_backend_fanout(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "list-primary", 701)
    reply = await _create_account(client, admin_headers, "list-reply", 702)
    template = await client.post(
        "/api/v1/templates",
        headers=admin_headers,
        json={"name": "Pair", "primary_text": "First", "reply_text": "Reply"},
    )
    assert template.status_code == 201

    target_list = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={"name": "Owned list", "chat_refs": ["@owned_1", "@owned_2"]},
    )
    assert target_list.status_code == 201
    list_body = target_list.json()
    assert list_body["item_count"] == 2
    approved = await client.post(
        f"/api/v1/target-lists/{list_body['id']}/approve",
        headers=admin_headers,
    )
    assert approved.json()["approved_count"] == 2

    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "List campaign",
            "primary_account_id": primary["id"],
            "reply_account_id": reply["id"],
            "target_list_id": list_body["id"],
            "primary_text": template.json()["primary_text"],
            "reply_text": template.json()["reply_text"],
            "reply_delay_seconds": 0,
        },
    )
    assert campaign.status_code == 201
    launch = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "target-list-api-launch"},
    )
    assert launch.status_code == 202
    assert len(launch.json()) == 2
    assert len({item["target_id"] for item in launch.json()}) == 2


async def test_target_list_launch_replay_is_stable_after_list_changes(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "stable-launch-primary", 711)
    target_list = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={"name": "Stable launch list", "chat_refs": ["@stable_1", "@stable_2"]},
    )
    assert target_list.status_code == 201
    list_body = target_list.json()
    await client.post(f"/api/v1/target-lists/{list_body['id']}/approve", headers=admin_headers)
    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Stable launch campaign",
            "mode": "broadcast",
            "primary_account_id": primary["id"],
            "target_list_id": list_body["id"],
            "primary_text": "Stable message",
        },
    )
    assert campaign.status_code == 201

    launch = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "stable-launch-key"},
    )
    assert launch.status_code == 202
    first_runs = launch.json()
    assert len(first_runs) == 2

    async with app.state.database.session_factory() as session:
        extra = Target(
            chat_ref="@stable_3",
            title="Stable 3",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add(extra)
        await session.flush()
        session.add(
            TargetListItem(
                target_list_id=list_body["id"],
                target_id=extra.id,
                enabled=True,
            )
        )
        await session.commit()

    replay = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "stable-launch-key"},
    )
    assert replay.status_code == 202
    assert [run["id"] for run in replay.json()] == [run["id"] for run in first_runs]

    fresh = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "stable-launch-key-2"},
    )
    assert fresh.status_code == 202
    assert len(fresh.json()) == 3


async def test_dialogue_launch_snapshots_texts_and_reply_delay(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "snapshot-primary", 751)
    reply = await _create_account(client, admin_headers, "snapshot-reply", 752)
    target = (
        await client.post(
            "/api/v1/targets",
            headers=admin_headers,
            json={"chat_ref": "@snapshot_dialogue", "title": "Snapshot dialogue"},
        )
    ).json()
    await client.post(f"/api/v1/targets/{target['id']}/approve", headers=admin_headers)
    campaign = (
        await client.post(
            "/api/v1/campaigns",
            headers=admin_headers,
            json={
                "name": "Snapshot dialogue",
                "primary_account_id": primary["id"],
                "reply_account_id": reply["id"],
                "target_id": target["id"],
                "primary_text": "Original primary",
                "reply_text": "Original reply",
                "reply_delay_seconds": 30,
            },
        )
    ).json()
    launch = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={"request_key": "dialogue-snapshot-001"},
    )
    assert launch.status_code == 202
    run_id = launch.json()[0]["id"]

    update = await client.patch(
        f"/api/v1/campaigns/{campaign['id']}",
        headers=admin_headers,
        json={
            "primary_text": "Changed primary",
            "reply_text": "Changed reply",
            "reply_delay_seconds": 3600,
        },
    )
    assert update.status_code == 200

    first_step = await app.state.queue_worker.process_next()
    assert first_step.status.value == "queued"
    scheduled = first_step.scheduled_for
    if scheduled.tzinfo is None:
        scheduled = scheduled.replace(tzinfo=UTC)
    assert 20 <= (scheduled - utc_now()).total_seconds() <= 31

    async with app.state.database.session_factory() as session:
        stored = await session.get(CampaignRun, run_id)
        assert stored is not None
        stored.scheduled_for = utc_now()
        await session.commit()

    second_step = await app.state.queue_worker.process_next()
    assert second_step.status.value == "succeeded"

    details = await client.get(f"/api/v1/runs/{run_id}", headers=admin_headers)
    assert details.status_code == 200
    actions = details.json()["actions"]
    assert [action["text"] for action in actions] == ["Original primary", "Original reply"]


async def test_long_dialogue_chain_runs_in_every_target_with_durable_snapshot(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    first = await _create_account(client, admin_headers, "chain-first", 761)
    second = await _create_account(client, admin_headers, "chain-second", 762)
    third = await _create_account(client, admin_headers, "chain-third", 763)
    target_list_response = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={"name": "Chain chats", "chat_refs": ["@chain_one", "@chain_two"]},
    )
    assert target_list_response.status_code == 201
    target_list = target_list_response.json()
    approval = await client.post(
        f"/api/v1/target-lists/{target_list['id']}/approve",
        headers=admin_headers,
    )
    assert approval.json()["approved_count"] == 2
    steps = [
        {
            "account_id": first["id"],
            "text": "Question",
            "delay_seconds": 0,
            "reply_to_previous": False,
        },
        {
            "account_id": second["id"],
            "text": "First answer",
            "delay_seconds": 0,
            "reply_to_previous": True,
        },
        {
            "account_id": third["id"],
            "text": "Follow-up",
            "delay_seconds": 0,
            "reply_to_previous": True,
        },
    ]
    campaign_response = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Three message chain",
            "mode": "dialogue",
            "primary_account_id": first["id"],
            "target_list_id": target_list["id"],
            "dialogue_steps": steps,
        },
    )
    assert campaign_response.status_code == 201, campaign_response.text
    campaign = campaign_response.json()
    assert [step["text"] for step in campaign["dialogue_steps"]] == [
        "Question",
        "First answer",
        "Follow-up",
    ]

    launch_response = await client.post(
        f"/api/v1/campaigns/{campaign['id']}/runs",
        headers=admin_headers,
        json={"request_key": "long-dialogue-chain"},
    )
    assert launch_response.status_code == 202, launch_response.text
    runs = launch_response.json()
    assert len(runs) == 2

    changed_steps = [dict(step) for step in steps]
    changed_steps[2]["text"] = "Changed after launch"
    update = await client.patch(
        f"/api/v1/campaigns/{campaign['id']}",
        headers=admin_headers,
        json={"dialogue_steps": changed_steps},
    )
    assert update.status_code == 200, update.text

    processed = []
    for _ in range(6):
        result = await app.state.queue_worker.process_next()
        assert result is not None
        processed.append(result)
    assert all(run.status.value == "succeeded" for run in processed[-2:])

    for run in runs:
        details = await client.get(
            f"/api/v1/runs/{run['id']}",
            headers=admin_headers,
        )
        actions = details.json()["actions"]
        assert [action["text"] for action in actions] == [
            "Question",
            "First answer",
            "Follow-up",
        ]
        assert [action["account_id"] for action in actions] == [
            first["id"],
            second["id"],
            third["id"],
        ]
        assert actions[0]["reply_to_message_id"] is None
        assert actions[1]["reply_to_message_id"] == actions[0]["external_message_id"]
        assert actions[2]["reply_to_message_id"] == actions[1]["external_message_id"]


async def test_broadcast_api_launches_target_list_across_24h_sender_pool(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "broadcast-primary", 801)
    secondary = await _create_account(client, admin_headers, "broadcast-secondary", 802)
    target_list = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={"name": "24h owned list", "chat_refs": ["@day_1", "@day_2", "@day_3"]},
    )
    assert target_list.status_code == 201
    list_body = target_list.json()
    approved = await client.post(
        f"/api/v1/target-lists/{list_body['id']}/approve",
        headers=admin_headers,
    )
    assert approved.json()["approved_count"] == 3

    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "24h broadcast",
            "mode": "broadcast",
            "primary_account_id": primary["id"],
            "sender_account_ids": [secondary["id"]],
            "target_list_id": list_body["id"],
            "primary_text": "One message per chat",
            "spread_seconds": 24 * 3600,
        },
    )
    assert campaign.status_code == 201
    campaign_body = campaign.json()
    assert campaign_body["mode"] == "broadcast"
    assert campaign_body["reply_account_id"] is None
    assert campaign_body["sender_account_ids"] == [primary["id"], secondary["id"]]
    assert campaign_body["spread_seconds"] == 24 * 3600

    launch = await client.post(
        f"/api/v1/campaigns/{campaign_body['id']}/runs",
        headers=admin_headers,
        json={"request_key": "broadcast-24h-api"},
    )
    assert launch.status_code == 202
    runs = launch.json()
    assert len(runs) == 3
    assert [run["sender_account_id"] for run in runs] == [
        primary["id"],
        secondary["id"],
        primary["id"],
    ]
    scheduled = [_parse_dt(run["scheduled_for"]) for run in runs]
    deltas = [(item - scheduled[0]).total_seconds() for item in scheduled]
    assert deltas[0] == 0
    assert 43199 <= deltas[1] <= 43201
    assert 86399 <= deltas[2] <= 86401


async def test_broadcast_campaign_patch_replaces_sender_pool(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "patch-broadcast-primary", 821)
    secondary = await _create_account(client, admin_headers, "patch-broadcast-secondary", 822)
    tertiary = await _create_account(client, admin_headers, "patch-broadcast-tertiary", 823)
    target_list = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={
            "name": "Patch sender list",
            "chat_refs": ["@patch_sender_1", "@patch_sender_2", "@patch_sender_3"],
        },
    )
    assert target_list.status_code == 201
    list_body = target_list.json()
    await client.post(f"/api/v1/target-lists/{list_body['id']}/approve", headers=admin_headers)

    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Patch sender broadcast",
            "mode": "broadcast",
            "primary_account_id": primary["id"],
            "sender_account_ids": [secondary["id"]],
            "target_list_id": list_body["id"],
            "primary_text": "Updated sender pool should be used",
        },
    )
    assert campaign.status_code == 201
    assert campaign.json()["sender_account_ids"] == [primary["id"], secondary["id"]]

    patched = await client.patch(
        f"/api/v1/campaigns/{campaign.json()['id']}",
        headers=admin_headers,
        json={"sender_account_ids": [tertiary["id"], tertiary["id"], primary["id"]]},
    )
    assert patched.status_code == 200
    assert patched.json()["sender_account_ids"] == [primary["id"], tertiary["id"]]

    launch = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "patched-sender-pool"},
    )
    assert launch.status_code == 202
    assert [run["sender_account_id"] for run in launch.json()] == [
        primary["id"],
        tertiary["id"],
        primary["id"],
    ]


async def test_broadcast_patch_synchronizes_the_primary_content_variant(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    account = await _create_account(client, admin_headers, "variant-patch-account", 824)
    target = (
        await client.post(
            "/api/v1/targets",
            headers=admin_headers,
            json={"chat_ref": "@variant_patch_target", "title": "Variant patch target"},
        )
    ).json()
    campaign = (
        await client.post(
            "/api/v1/campaigns",
            headers=admin_headers,
            json={
                "name": "Variant patch campaign",
                "mode": "broadcast",
                "primary_account_id": account["id"],
                "target_id": target["id"],
                "primary_text": "Original primary",
            },
        )
    ).json()
    async with app.state.database.session_factory() as session:
        stored = await session.get(Campaign, campaign["id"])
        assert stored is not None
        stored.content_variants_json = serialize_content_variants(
            [
                new_content_variant(name="Primary", text="Original primary"),
                new_content_variant(name="Alternative", text="Alternative text"),
            ]
        )
        await session.commit()

    patched = await client.patch(
        f"/api/v1/campaigns/{campaign['id']}",
        headers=admin_headers,
        json={"primary_text": "Updated primary"},
    )
    assert patched.status_code == 200, patched.text
    async with app.state.database.session_factory() as session:
        stored = await session.get(Campaign, campaign["id"])
        assert stored is not None
        variants = parse_content_variants(
            stored.content_variants_json,
            fallback_text=stored.primary_text,
        )
    assert variants[0].text == "Updated primary"
    assert variants[1].text == "Alternative text"


async def test_multistep_dialogue_rejects_partial_chain_updates(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    first = await _create_account(client, admin_headers, "chain-first", 825)
    second = await _create_account(client, admin_headers, "chain-second", 826)
    target = (
        await client.post(
            "/api/v1/targets",
            headers=admin_headers,
            json={"chat_ref": "@chain_patch_target", "title": "Chain patch target"},
        )
    ).json()
    campaign = (
        await client.post(
            "/api/v1/campaigns",
            headers=admin_headers,
            json={
                "name": "Multi-step update guard",
                "mode": "dialogue",
                "primary_account_id": first["id"],
                "target_id": target["id"],
                "dialogue_steps": [
                    {"account_id": first["id"], "text": "First"},
                    {"account_id": second["id"], "text": "Second", "delay_seconds": 30},
                    {"account_id": first["id"], "text": "Third", "delay_seconds": 30},
                ],
            },
        )
    ).json()
    response = await client.patch(
        f"/api/v1/campaigns/{campaign['id']}",
        headers=admin_headers,
        json={"primary_text": "Would otherwise be ignored"},
    )
    assert response.status_code == 422
    assert "dialogue_steps" in response.json()["detail"]


async def test_campaign_rejects_html_that_renders_to_a_different_text(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    account = await _create_account(client, admin_headers, "html-pair-account", 827)
    target = (
        await client.post(
            "/api/v1/targets",
            headers=admin_headers,
            json={"chat_ref": "@html_pair_target", "title": "HTML pair target"},
        )
    ).json()
    response = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Mismatched formatted content",
            "mode": "broadcast",
            "primary_account_id": account["id"],
            "target_id": target["id"],
            "primary_text": "Text shown in preview",
            "primary_html": "<b>Text actually sent</b>",
        },
    )
    assert response.status_code == 422
    assert "форматированный" in str(response.json()["detail"])


async def test_broadcast_sender_pool_ignores_unavailable_account_before_enqueue(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    primary = await _create_account(client, admin_headers, "pool-primary", 811)
    disabled = await _create_account(client, admin_headers, "pool-disabled", 812)
    patched = await client.patch(
        f"/api/v1/accounts/{disabled['id']}",
        headers=admin_headers,
        json={"status": "disabled"},
    )
    assert patched.status_code == 200
    target = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@pool_guard", "title": "Pool guard"},
    )
    assert target.status_code == 201
    await client.post(f"/api/v1/targets/{target.json()['id']}/approve", headers=admin_headers)

    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "pool guard broadcast",
            "mode": "broadcast",
            "primary_account_id": primary["id"],
            "sender_account_ids": [disabled["id"]],
            "target_id": target.json()["id"],
            "primary_text": "Hello",
            "spread_seconds": 24 * 3600,
        },
    )
    assert campaign.status_code == 201
    launch = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "pool-guard-broadcast"},
    )
    assert launch.status_code == 202
    assert len(launch.json()) == 1
    assert launch.json()[0]["sender_account_id"] == primary["id"]


async def test_target_list_members_do_not_pollute_default_target_list(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    target_list = await client.post(
        "/api/v1/target-lists",
        headers=admin_headers,
        json={"name": "Hidden API audience", "chat_refs": ["@hidden_api_member"]},
    )
    assert target_list.status_code == 201

    standalone = await client.get("/api/v1/targets", headers=admin_headers)
    all_targets = await client.get(
        "/api/v1/targets?include_list_members=true",
        headers=admin_headers,
    )
    assert standalone.status_code == 200
    assert standalone.json()["items"] == []
    assert standalone.json()["total"] == 0
    assert [item["chat_ref"] for item in all_targets.json()["items"]] == [
        "@hidden_api_member"
    ]

    promoted = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@hidden_api_member", "title": "Promoted manually"},
    )
    assert promoted.status_code == 201
    assert promoted.json()["is_standalone"] is True
    standalone = await client.get("/api/v1/targets", headers=admin_headers)
    assert [item["chat_ref"] for item in standalone.json()["items"]] == [
        "@hidden_api_member"
    ]


async def test_launch_archive_api_is_safe_and_reversible(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    account = await _create_account(client, admin_headers, "archive-api", 8991)
    target = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@archive_api", "title": "Archive API"},
    )
    await client.post(
        f"/api/v1/targets/{target.json()['id']}/approve",
        headers=admin_headers,
    )
    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Archive API campaign",
            "mode": "broadcast",
            "primary_account_id": account["id"],
            "target_id": target.json()["id"],
            "primary_text": "Archive",
        },
    )
    launch = await client.post(
        f"/api/v1/campaigns/{campaign.json()['id']}/runs",
        headers=admin_headers,
        json={"request_key": "archive-api-launch"},
    )
    run_id = launch.json()[0]["id"]

    active_archive = await client.post(
        f"/api/v1/launches/{run_id}/archive",
        headers=admin_headers,
    )
    assert active_archive.status_code == 409
    await client.post(f"/api/v1/runs/{run_id}/cancel", headers=admin_headers)
    archived = await client.post(
        f"/api/v1/launches/{run_id}/archive",
        headers=admin_headers,
    )
    assert archived.status_code == 200
    assert archived.json() == {
        "anchor_run_id": run_id,
        "archived": True,
        "updated_tasks": 1,
    }
    restored = await client.post(
        f"/api/v1/launches/{run_id}/restore",
        headers=admin_headers,
    )
    assert restored.status_code == 200
    assert restored.json()["archived"] is False


async def test_target_list_pagination_exposes_remaining_pages_and_total(
    app: FastAPI,
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    async with app.state.database.session_factory() as session:
        session.add_all(TargetList(name=f"page-{index:03d}") for index in range(501))
        await session.commit()

    response = await client.get(
        "/api/v1/target-lists?page=1&page_size=500",
        headers=admin_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 501
    assert body["page"] == 1
    assert body["page_size"] == 500
    assert [item["name"] for item in body["items"]] == ["page-000"]


async def test_collection_endpoints_use_the_same_pagination_envelope(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    account = await _create_account(client, admin_headers, "collection-page", 9001)
    target_response = await client.post(
        "/api/v1/targets",
        headers=admin_headers,
        json={"chat_ref": "@collection_page", "title": "Collection page"},
    )
    assert target_response.status_code == 201
    target = target_response.json()
    template = await client.post(
        "/api/v1/templates",
        headers=admin_headers,
        json={"name": "Collection page", "primary_text": "Hello", "reply_text": "Reply"},
    )
    assert template.status_code == 201
    campaign = await client.post(
        "/api/v1/campaigns",
        headers=admin_headers,
        json={
            "name": "Collection page",
            "mode": "broadcast",
            "primary_account_id": account["id"],
            "target_id": target["id"],
            "primary_text": "Hello",
        },
    )
    assert campaign.status_code == 201

    for path in ("accounts", "targets", "templates", "campaigns", "target-lists"):
        response = await client.get(f"/api/v1/{path}?page=0&page_size=1", headers=admin_headers)

        assert response.status_code == 200
        body = response.json()
        assert set(body) == {"items", "total", "page", "page_size"}
        assert body["total"] >= (1 if path != "target-lists" else 0)
        assert body["page"] == 0
        assert body["page_size"] == 1
        assert len(body["items"]) <= 1


async def _create_account(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    label: str,
    telegram_user_id: int,
) -> dict[str, object]:
    response = await client.post(
        "/api/v1/accounts",
        headers=headers,
        json={
            "label": label,
            "telegram_user_id": telegram_user_id,
            "ownership_confirmed": True,
        },
    )
    assert response.status_code == 201
    return response.json()


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed
