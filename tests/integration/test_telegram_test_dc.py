"""Opt-in Telegram Test DC contract tests.

These tests are skipped during the normal suite. They require a StringSession
that was authorized on Telegram's Test DC, never a production account session.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession

from proxima_traffic.config import Settings
from proxima_traffic.content import StoredContact
from proxima_traffic.live_gateway import TelethonGateway
from proxima_traffic.mentions import append_hidden_mentions

TEST_API_ID = os.getenv("PROXIMA_TEST_DC_API_ID", "").strip()
TEST_API_HASH = os.getenv("PROXIMA_TEST_DC_API_HASH", "").strip()
TEST_SESSION = os.getenv("PROXIMA_TEST_DC_SESSION", "").strip()
TEST_TARGET = os.getenv("PROXIMA_TEST_DC_TARGET", "").strip()
TEST_DC_READY = all((TEST_API_ID, TEST_API_HASH, TEST_SESSION, TEST_TARGET))

pytestmark = [
    pytest.mark.telegram_test_dc,
    pytest.mark.skipif(
        not TEST_DC_READY,
        reason="Telegram Test DC credentials are not configured",
    ),
]


@pytest_asyncio.fixture
async def test_dc_client():
    client = TelegramClient(
        StringSession(TEST_SESSION),
        int(TEST_API_ID),
        TEST_API_HASH,
        flood_sleep_threshold=0,
    )
    await client.connect()
    if not await client.is_user_authorized():
        pytest.fail("PROXIMA_TEST_DC_SESSION is not authorized on Telegram Test DC")
    try:
        yield client
    finally:
        await client.disconnect()


@pytest_asyncio.fixture
async def test_dc_entity(test_dc_client):
    target: object = TEST_TARGET
    if TEST_TARGET.lstrip("-").isdigit():
        target = int(TEST_TARGET)
    return await test_dc_client.get_entity(target)


def _gateway() -> TelethonGateway:
    gateway = object.__new__(TelethonGateway)
    gateway._settings = Settings(
        admin_api_key="telegram-test-dc-only",
        dry_run=True,
        worker_enabled=False,
        account_delivery_verify_delay_seconds=0,
    )
    return gateway


async def test_test_dc_native_contact(test_dc_client, test_dc_entity) -> None:
    phone = os.getenv("PROXIMA_TEST_DC_CONTACT_PHONE", "+9996610000")
    sent = await _gateway()._send_contact(
        test_dc_client,
        test_dc_entity,
        StoredContact(phone_number=phone, first_name="Proxima", last_name="Test DC"),
        None,
        f"test-dc-contact:{uuid4().hex}",
    )
    assert sent.message_id > 0


async def test_test_dc_album_uses_real_upload_media(
    test_dc_client,
    test_dc_entity,
    tmp_path: Path,
) -> None:
    # Valid 1x1 PNG; enough for an MTProto upload/send contract check.
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Z4ZkAAAAASUVORK5CYII="
    )
    first = tmp_path / "first.png"
    second = tmp_path / "second.png"
    first.write_bytes(png)
    second.write_bytes(png)
    sent = await _gateway()._send_album(
        test_dc_client,
        test_dc_entity,
        "Proxima Test DC album",
        None,
        f"test-dc-album:{uuid4().hex}",
        html_text=None,
        media_items=[(str(first), "photo"), (str(second), "photo")],
    )
    assert sent.message_id > 0


async def test_test_dc_reaction_and_participant_message_scan(
    test_dc_client,
    test_dc_entity,
) -> None:
    sent = await test_dc_client.send_message(
        test_dc_entity,
        f"Proxima Test DC reaction {uuid4().hex}",
    )
    await test_dc_client(
        functions.messages.SendReactionRequest(
            peer=test_dc_entity,
            msg_id=sent.id,
            reaction=[types.ReactionEmoji(emoticon="👍")],
        )
    )
    observed = [
        message
        async for message in test_dc_client.iter_messages(
            test_dc_entity,
            min_id=max(0, sent.id - 1),
            limit=10,
        )
    ]
    assert any(message.id == sent.id for message in observed)


@pytest.mark.skipif(
    len(os.getenv("PROXIMA_TEST_DC_MENTION_USER_IDS", "").split(",")) < 5,
    reason="hidden mentions require five Test DC user IDs",
)
async def test_test_dc_hidden_tg_user_mentions(test_dc_client, test_dc_entity) -> None:
    user_ids = tuple(
        int(value.strip())
        for value in os.environ["PROXIMA_TEST_DC_MENTION_USER_IDS"].split(",")
        if value.strip().isdigit()
    )
    html = append_hidden_mentions(
        text="Proxima Test DC mentions",
        html_text=None,
        user_ids=user_ids[:10],
    )
    assert html is not None
    sent = await test_dc_client.send_message(test_dc_entity, html, parse_mode="html")
    assert sent.id > 0
    mention_entities = sum(
        isinstance(item, types.MessageEntityMentionName) for item in sent.entities or []
    )
    assert mention_entities >= 5


@pytest.mark.skipif(
    not os.getenv("PROXIMA_TEST_DC_SECOND_SESSION", "").strip(),
    reason="multi-account dialogue requires a second Test DC session",
)
async def test_test_dc_two_account_dialogue_reply(test_dc_client, test_dc_entity) -> None:
    second = TelegramClient(
        StringSession(os.environ["PROXIMA_TEST_DC_SECOND_SESSION"].strip()),
        int(TEST_API_ID),
        TEST_API_HASH,
        flood_sleep_threshold=0,
    )
    await second.connect()
    try:
        if not await second.is_user_authorized():
            pytest.fail("PROXIMA_TEST_DC_SECOND_SESSION is not authorized on Test DC")
        first = await test_dc_client.send_message(
            test_dc_entity,
            f"Proxima Test DC dialogue {uuid4().hex}",
        )
        second_entity = await second.get_entity(TEST_TARGET)
        reply = await second.send_message(second_entity, "Test DC reply", reply_to=first.id)
        assert reply.reply_to is not None
        assert reply.reply_to.reply_to_msg_id == first.id
    finally:
        await second.disconnect()


@pytest.mark.skipif(
    not os.getenv("PROXIMA_TEST_DC_CHANNEL_POST_ID", "").strip().isdigit(),
    reason="discussion comment requires a Test DC channel post ID",
)
async def test_test_dc_channel_discussion_comment(test_dc_client, test_dc_entity) -> None:
    comment = await test_dc_client.send_message(
        test_dc_entity,
        f"Proxima Test DC comment {uuid4().hex}",
        comment_to=int(os.environ["PROXIMA_TEST_DC_CHANNEL_POST_ID"]),
    )
    assert comment.id > 0
    assert comment.reply_to is not None


@pytest.mark.skipif(
    os.getenv("PROXIMA_TEST_DC_ALLOW_PROFILE", "").strip() != "1",
    reason="profile mutation requires PROXIMA_TEST_DC_ALLOW_PROFILE=1",
)
async def test_test_dc_profile_update_and_restore(test_dc_client) -> None:
    me = await test_dc_client.get_me()
    assert me is not None
    original_first = str(getattr(me, "first_name", None) or "Proxima")
    original_last = str(getattr(me, "last_name", None) or "")
    try:
        await test_dc_client(
            functions.account.UpdateProfileRequest(
                first_name="Proxima Test",
                last_name="DC",
            )
        )
        updated = await test_dc_client.get_me()
        assert updated is not None and updated.first_name == "Proxima Test"
    finally:
        await test_dc_client(
            functions.account.UpdateProfileRequest(
                first_name=original_first,
                last_name=original_last,
            )
        )
