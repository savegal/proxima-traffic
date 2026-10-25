from aiogram.fsm.storage.base import StorageKey
from sqlalchemy import event, select

from proxima_traffic.bot.handlers import _proxy_draft_url
from proxima_traffic.bot.states import CampaignCreateState
from proxima_traffic.bot.storage import DatabaseFsmStorage
from proxima_traffic.config import Settings
from proxima_traffic.models import BotFsmState
from proxima_traffic.proxies import ProxySecretCipher


async def test_database_fsm_storage_survives_a_storage_instance_restart(app) -> None:
    key = StorageKey(
        bot_id=100,
        chat_id=200,
        user_id=300,
        thread_id=None,
        business_connection_id=None,
        destiny="default",
    )
    first = DatabaseFsmStorage(app.state.database.session_factory)
    await first.set_state(key, CampaignCreateState.primary_text)
    await first.set_data(key, {"campaign_name": "Durable draft", "account_ids": [1, 2]})

    second = DatabaseFsmStorage(app.state.database.session_factory)
    assert await second.get_state(key) == CampaignCreateState.primary_text.state
    assert await second.get_data(key) == {
        "campaign_name": "Durable draft",
        "account_ids": [1, 2],
    }

    updated = await second.update_data(key, {"target_id": 77})
    assert updated["campaign_name"] == "Durable draft"
    assert updated["target_id"] == 77

    statements: list[str] = []

    def capture_statement(
        connection, cursor, statement, parameters, context, executemany
    ) -> None:
        del connection, cursor, parameters, context, executemany
        statements.append(str(statement))

    event.listen(app.state.database.engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        await second.clear(key)
    finally:
        event.remove(
            app.state.database.engine.sync_engine, "before_cursor_execute", capture_statement
        )
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith("DELETE FROM BOT_FSM_STATES")
    assert await second.get_state(key) is None
    assert await second.get_data(key) == {}


async def test_proxy_wizard_draft_does_not_persist_credentials_in_plaintext(app) -> None:
    key = StorageKey(
        bot_id=100,
        chat_id=200,
        user_id=300,
        thread_id=None,
        business_connection_id=None,
        destiny="proxy-draft",
    )
    settings = Settings(proxy_encryption_key="proxy-draft-encryption-key")
    raw_proxy_url = "socks5://operator:secret-password@proxy.example:1080"
    encrypted_url = ProxySecretCipher.from_settings(settings).encrypt(raw_proxy_url)
    assert encrypted_url is not None
    storage = DatabaseFsmStorage(app.state.database.session_factory)

    await storage.set_data(
        key,
        {"proxy_url_encrypted": encrypted_url, "proxy_protocol": "socks5"},
    )

    async with app.state.database.session_factory() as session:
        row = await session.scalar(
            select(BotFsmState).where(BotFsmState.storage_key == storage._key(key))
        )
    assert row is not None
    assert raw_proxy_url not in row.data_json
    assert "secret-password" not in row.data_json
    assert _proxy_draft_url(await storage.get_data(key), settings) == raw_proxy_url
