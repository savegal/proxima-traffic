from aiogram.fsm.storage.base import StorageKey

from proxima_traffic.bot.states import CampaignCreateState
from proxima_traffic.bot.storage import DatabaseFsmStorage


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
