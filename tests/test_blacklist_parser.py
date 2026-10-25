from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from proxima_traffic.blacklist_parser import ChatBlacklistParser
from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.direct_messages import DirectMessageService
from proxima_traffic.gateways import (
    ChatBlacklistScan,
    DryRunGateway,
    PermanentGatewayError,
    TemporaryGatewayError,
)
from proxima_traffic.models import Account, AccountStatus, ensure_utc, utc_now
from proxima_traffic.services import CampaignRunner, ConflictError


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'blacklist-parser.db'}",
        admin_api_key="test-admin-key",
        dry_run=True,
        worker_enabled=False,
        account_require_proxy=False,
    )


class HiddenMembersGateway(DryRunGateway):
    async def scan_chat_blacklist_candidates(self, **kwargs):
        if kwargs["mode"] == "members":
            return ChatBlacklistScan(telegram_user_ids=(), mode="members")
        return ChatBlacklistScan(
            telegram_user_ids=(101, 102, 101), mode="messages", joined_chat=True
        )


class UnavailableGateway(DryRunGateway):
    async def scan_chat_blacklist_candidates(self, **kwargs):
        del kwargs
        raise PermanentGatewayError("no access", code="target_unavailable")


class RestrictedThenReadyGateway(DryRunGateway):
    async def scan_chat_blacklist_candidates(self, **kwargs):
        if kwargs["account"].label == "restricted":
            raise PermanentGatewayError(
                "resolve: FrozenMethodInvalidError", code="account_restricted"
            )
        return ChatBlacklistScan(telegram_user_ids=(777,), mode=str(kwargs["mode"]))


@pytest.mark.asyncio
async def test_chat_blacklist_parser_falls_back_to_message_authors(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    database = Database(settings.database_url)
    await database.create_schema()
    async with database.session_factory() as session:
        session.add(Account(label="reader", ownership_confirmed=True))
        await session.commit()
    parser = ChatBlacklistParser(database.session_factory, HiddenMembersGateway(), settings)

    result = await parser.parse(chat_ref="https://t.me/example_chat", mode="auto")

    assert result.chat_ref == "@example_chat"
    assert result.mode == "messages"
    assert result.fallback_used is True
    assert result.joined_chat is True
    assert result.telegram_user_ids == (101, 102)
    assert await parser.take(result.token) == result
    assert await parser.take(result.token) is None


@pytest.mark.asyncio
async def test_chat_blacklist_parser_explains_when_no_account_can_read(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    database = Database(settings.database_url)
    await database.create_schema()
    async with database.session_factory() as session:
        session.add(Account(label="reader", ownership_confirmed=True))
        await session.commit()
    parser = ChatBlacklistParser(database.session_factory, UnavailableGateway(), settings)

    with pytest.raises(Exception, match="ни один подключённый аккаунт"):
        await parser.parse(chat_ref="@example_chat", mode="members")


@pytest.mark.asyncio
async def test_chat_blacklist_parser_tries_next_account_after_restriction(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    database = Database(settings.database_url)
    await database.create_schema()
    async with database.session_factory() as session:
        session.add_all(
            [
                Account(label="restricted", ownership_confirmed=True),
                Account(label="ready", ownership_confirmed=True),
            ]
        )
        await session.commit()
    parser = ChatBlacklistParser(database.session_factory, RestrictedThenReadyGateway(), settings)

    result = await parser.parse(chat_ref="@example_chat", mode="members")

    assert result.account_label == "ready"
    assert result.telegram_user_ids == (777,)


@pytest.mark.asyncio
async def test_bulk_blacklist_addition_chunks_large_parser_result(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    database = Database(settings.database_url)
    await database.create_schema()
    gateway = DryRunGateway()
    service = DirectMessageService(
        database.session_factory,
        gateway,
        CampaignRunner(database.session_factory, gateway, settings=settings),
        settings,
        worker_id="test-direct",
    )

    result = await service.add_blacklist_entries_bulk(range(1_000_000, 1_001_201))

    assert result.requested == 1_201
    assert result.added == 1_201


@pytest.mark.parametrize(
    "code",
    ["flood_wait", "account_dead", "account_unauthorized", "account_restricted", "peer_flood"],
)
async def test_account_failure_stops_fallback_and_is_saved_before_using_backup(app, code):
    factory = app.state.database.session_factory
    async with factory() as session:
        limited = Account(label="limited", ownership_confirmed=True, authorized=True)
        ready = Account(label="ready", ownership_confirmed=True)
        session.add_all([limited, ready])
        await session.commit()
        limited_id = limited.id

    class Gateway(DryRunGateway):
        calls = []

        async def scan_chat_blacklist_candidates(self, **kwargs):
            account = kwargs["account"]
            self.calls.append((account.label, kwargs["mode"]))
            if account.id == limited_id:
                raise TemporaryGatewayError("account failure", code=code, retry_after=900)
            async with factory() as session:
                saved = await session.get(Account, limited_id)
                if code == "flood_wait":
                    assert ensure_utc(saved.cooldown_until) > utc_now()
                else:
                    assert saved.status != AccountStatus.ACTIVE
            return ChatBlacklistScan(telegram_user_ids=(777,), mode=kwargs["mode"])

    gateway = Gateway()
    parser = ChatBlacklistParser(factory, gateway, app.state.settings)
    before = utc_now()
    result = await parser.parse(chat_ref="@example_chat", mode="auto")
    assert result.account_label == "ready"
    assert gateway.calls == [("limited", "members"), ("ready", "members")]
    async with factory() as session:
        saved = await session.get(Account, limited_id)
        if code == "flood_wait":
            assert saved.status == AccountStatus.ACTIVE
            assert ensure_utc(saved.cooldown_until) >= before + timedelta(seconds=900)
        elif code in {"account_dead", "account_unauthorized"}:
            assert saved.status == AccountStatus.DISABLED
            assert saved.authorized is False
        else:
            assert saved.status == AccountStatus.PAUSED
        assert saved.cooldown_reason == code
    await parser.parse(chat_ref="@example_chat", mode="auto")
    assert gateway.calls[-1] == ("ready", "members")
    assert len(gateway.calls) == 3


@pytest.mark.parametrize(
    "code,expected_modes",
    [
        ("account_missing_admin_rights", ["members", "messages"]),
        ("network_error", ["members"]),
        ("target_unavailable", ["members"]),
    ],
)
async def test_only_participant_permissions_allow_same_account_fallback(app, code, expected_modes):
    async with app.state.database.session_factory() as session:
        session.add(Account(label="reader", ownership_confirmed=True))
        await session.commit()

    class Gateway(DryRunGateway):
        modes = []

        async def scan_chat_blacklist_candidates(self, **kwargs):
            self.modes.append(kwargs["mode"])
            if kwargs["mode"] == "members":
                raise PermanentGatewayError("scan failure", code=code)
            return ChatBlacklistScan(telegram_user_ids=(101,), mode="messages")

    gateway = Gateway()
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    if code == "account_missing_admin_rights":
        result = await parser.parse(chat_ref="@example_chat", mode="auto")
        assert result.fallback_used is True
    else:
        with pytest.raises(ConflictError):
            await parser.parse(chat_ref="@example_chat", mode="auto")
    assert gateway.modes == expected_modes


async def test_parser_skips_manual_pause_and_future_cooldown(app):
    async with app.state.database.session_factory() as session:
        session.add_all(
            [
                Account(
                    label="manual",
                    ownership_confirmed=True,
                    status=AccountStatus.PAUSED,
                    operator_paused=True,
                ),
                Account(
                    label="waiting",
                    ownership_confirmed=True,
                    cooldown_until=utc_now() + timedelta(hours=1),
                ),
                Account(label="operator-hold", ownership_confirmed=True, operator_paused=True),
                Account(label="ready", ownership_confirmed=True),
            ]
        )
        await session.commit()

    class Gateway(DryRunGateway):
        readers = []

        async def scan_chat_blacklist_candidates(self, **kwargs):
            self.readers.append(kwargs["account"].label)
            return ChatBlacklistScan(telegram_user_ids=(101,), mode=kwargs["mode"])

    gateway = Gateway()
    parser = ChatBlacklistParser(app.state.database.session_factory, gateway, app.state.settings)
    result = await parser.parse(chat_ref="@example_chat", mode="auto")
    assert result.account_label == "ready"
    assert gateway.readers == ["ready"]


async def test_parser_rechecks_state_before_permission_fallback(app):
    factory = app.state.database.session_factory
    async with factory() as session:
        account = Account(label="reader", ownership_confirmed=True)
        session.add(account)
        await session.commit()
        aid = account.id

    class Gateway(DryRunGateway):
        modes = []

        async def scan_chat_blacklist_candidates(self, **kwargs):
            self.modes.append(kwargs["mode"])
            async with factory() as session:
                saved = await session.get(Account, aid)
                saved.cooldown_until = utc_now() + timedelta(minutes=20)
                await session.commit()
            raise PermanentGatewayError("members hidden", code="account_missing_admin_rights")

    gateway = Gateway()
    parser = ChatBlacklistParser(factory, gateway, app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref="@example_chat", mode="auto")
    assert gateway.modes == ["members"]


async def test_old_scan_failure_does_not_disable_replaced_session(app):
    factory = app.state.database.session_factory
    async with factory() as session:
        account = Account(
            label="reader",
            ownership_confirmed=True,
            authorized=True,
            session_encrypted="old-test-session",
        )
        session.add(account)
        await session.commit()
        aid = account.id

    class Gateway(DryRunGateway):
        async def scan_chat_blacklist_candidates(self, **kwargs):
            async with factory() as session:
                saved = await session.get(Account, aid)
                saved.session_encrypted = "new-test-session"
                await session.commit()
            raise PermanentGatewayError("old session revoked", code="account_dead")

    parser = ChatBlacklistParser(factory, Gateway(), app.state.settings)
    with pytest.raises(ConflictError):
        await parser.parse(chat_ref="@example_chat", mode="auto")
    async with factory() as session:
        saved = await session.get(Account, aid)
        assert saved.authorized is True
        assert saved.status == AccountStatus.ACTIVE
