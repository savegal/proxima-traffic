from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from proxima_traffic.models import BotFsmState


class DatabaseFsmStorage(BaseStorage):
    """Small durable aiogram FSM storage backed by the service database."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def set_state(self, key: StorageKey, state: str | State | None = None) -> None:
        normalized = state.state if isinstance(state, State) else state
        async with self._session_factory() as session:
            statement = insert(BotFsmState).values(
                storage_key=self._key(key),
                state=normalized,
                data_json="{}",
            )
            statement = statement.on_conflict_do_update(
                index_elements=[BotFsmState.storage_key],
                set_={"state": normalized},
            )
            await session.execute(statement)
            await session.commit()

    async def get_state(self, key: StorageKey) -> str | None:
        async with self._session_factory() as session:
            row = await session.get(BotFsmState, self._key(key))
            return row.state if row is not None else None

    async def set_data(self, key: StorageKey, data: Mapping[str, Any]) -> None:
        payload = json.dumps(dict(data), ensure_ascii=False, separators=(",", ":"))
        async with self._session_factory() as session:
            statement = insert(BotFsmState).values(
                storage_key=self._key(key),
                state=None,
                data_json=payload,
            )
            statement = statement.on_conflict_do_update(
                index_elements=[BotFsmState.storage_key],
                set_={"data_json": payload},
            )
            await session.execute(statement)
            await session.commit()

    async def get_data(self, key: StorageKey) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = await session.get(BotFsmState, self._key(key))
            if row is None:
                return {}
            try:
                data = json.loads(row.data_json)
            except (TypeError, json.JSONDecodeError):
                return {}
            return data if isinstance(data, dict) else {}

    async def close(self) -> None:
        return None

    @staticmethod
    def _key(key: StorageKey) -> str:
        raw = json.dumps(
            [
                key.bot_id,
                key.chat_id,
                key.user_id,
                key.thread_id,
                key.business_connection_id,
                key.destiny,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
