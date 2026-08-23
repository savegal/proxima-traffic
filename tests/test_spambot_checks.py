import json
from datetime import UTC, datetime, timedelta

import pytest

from proxima_traffic.gateways import TemporaryGatewayError
from proxima_traffic.models import Account, AccountSessionSource, AccountStatus, ensure_utc
from proxima_traffic.spambot import SpamBotResult, SpamBotStatus
from proxima_traffic.spambot_checks import SpamBotCheckService


class MockGateway:
    def __init__(self):
        self.responses = []
        self.calls = []

    async def check_spambot_status(self, *, account: Account) -> SpamBotResult:
        self.calls.append(account.id)
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response
        return SpamBotResult(SpamBotStatus.UNKNOWN, "error", parser_signal="mock")


class MockSettings:
    spambot_enabled = True
    spambot_unknown_cooldown_seconds = 60
    spambot_min_check_interval_seconds = 21600


@pytest.fixture
def mock_gateway():
    return MockGateway()


@pytest.fixture
def checks_service(mock_gateway, app):
    # use app database session_factory
    return SpamBotCheckService(
        session_factory=app.state.database.session_factory,
        gateway=mock_gateway,
        settings=MockSettings(),
    )


@pytest.mark.asyncio
async def test_check_account_free(checks_service, mock_gateway, app):
    mock_gateway.responses.append(SpamBotResult(SpamBotStatus.FREE, "free", parser_signal="mock"))
    async with app.state.database.session_factory() as session:
        account = Account(
            label="test-free",
            telegram_user_id=1,
            status=AccountStatus.PAUSED,
            cooldown_reason="spambot_temp_ban",
        )
        session.add(account)
        await session.commit()
        acc_id = account.id

    result = await checks_service.check_account(acc_id, trigger="test")
    assert result.status is SpamBotStatus.FREE

    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, acc_id)
        assert updated.status is AccountStatus.ACTIVE
        assert updated.cooldown_reason is None


@pytest.mark.asyncio
async def test_check_account_permanent_ban(checks_service, mock_gateway, app):
    mock_gateway.responses.append(
        SpamBotResult(
            SpamBotStatus.PERMANENT_RESTRICTION,
            "banned forever",
            parser_signal="mock",
        )
    )
    async with app.state.database.session_factory() as session:
        account = Account(label="test-perm", telegram_user_id=2, status=AccountStatus.ACTIVE)
        session.add(account)
        await session.commit()
        acc_id = account.id

    result = await checks_service.check_account(acc_id, trigger="test")
    assert result.status is SpamBotStatus.PERMANENT_RESTRICTION

    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, acc_id)
        assert updated.status is AccountStatus.DISABLED
        assert updated.cooldown_reason == "spambot_permanent_ban"
        assert updated.cooldown_until is None


@pytest.mark.asyncio
async def test_check_account_temporary_ban(checks_service, mock_gateway, app):
    expiry = datetime.now(UTC) + timedelta(days=2)
    mock_gateway.responses.append(
        SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION,
            "temp ban",
            restricted_until=expiry,
            parser_signal="mock",
        )
    )
    async with app.state.database.session_factory() as session:
        account = Account(label="test-temp", telegram_user_id=3, status=AccountStatus.ACTIVE)
        session.add(account)
        await session.commit()
        acc_id = account.id

    result = await checks_service.check_account(acc_id, trigger="test")
    assert result.status is SpamBotStatus.TEMPORARY_RESTRICTION

    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, acc_id)
        assert updated.status is AccountStatus.PAUSED
        assert updated.cooldown_reason == "spambot_temp_ban"
        assert ensure_utc(updated.cooldown_until) == expiry


@pytest.mark.asyncio
async def test_check_account_active_restriction_without_expiry(
    checks_service,
    mock_gateway,
    app,
):
    mock_gateway.responses.append(
        SpamBotResult(
            SpamBotStatus.ACTIVE_RESTRICTION,
            "While the account is limited...",
            parser_signal="active_restriction_without_expiry",
            button_rows=(("This is a mistake",),),
        )
    )
    async with app.state.database.session_factory() as session:
        account = Account(label="test-active-restriction", telegram_user_id=4)
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    result = await checks_service.check_account(account_id, trigger="test")

    assert result is not None
    assert result.status is SpamBotStatus.ACTIVE_RESTRICTION
    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, account_id)
        assert updated is not None
        assert updated.status is AccountStatus.PAUSED
        assert updated.cooldown_reason == "spambot_restriction_no_expiry"
        assert ensure_utc(updated.cooldown_until) > datetime.now(UTC)
        assert updated.spambot_parser_signal == "active_restriction_without_expiry"
        assert '"button_rows":[["This is a mistake"]]' in updated.spambot_response_json


@pytest.mark.asyncio
async def test_explicit_but_unrecognised_restriction_remains_protectively_paused(
    checks_service,
    mock_gateway,
    app,
):
    mock_gateway.responses.append(
        SpamBotResult(
            SpamBotStatus.RESTRICTION_UNCLEAR,
            "Your account is restricted in a newly-worded response.",
            parser_signal="restriction_confirmed_scope_unclear",
        )
    )
    async with app.state.database.session_factory() as session:
        account = Account(label="unclear-restriction", telegram_user_id=5)
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    result = await checks_service.check_account(account_id, trigger="test")

    assert result is not None
    assert result.status is SpamBotStatus.RESTRICTION_UNCLEAR
    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, account_id)
        assert updated is not None
        assert updated.status is AccountStatus.PAUSED
        assert updated.cooldown_reason == "spambot_restriction_unclear"
        assert ensure_utc(updated.cooldown_until) > datetime.now(UTC)
        assert updated.spambot_status == SpamBotStatus.RESTRICTION_UNCLEAR.value


@pytest.mark.asyncio
async def test_transport_error_keeps_previous_observation_separate_from_latest_verdict(
    checks_service,
    mock_gateway,
    app,
):
    mock_gateway.responses.append(
        TemporaryGatewayError("proxy unavailable", code="proxy_unavailable")
    )
    async with app.state.database.session_factory() as session:
        account = Account(
            label="transport-error",
            telegram_user_id=6,
            status=AccountStatus.PAUSED,
            cooldown_reason="spambot_temp_ban",
            spambot_response_json=(
                '{"text":"old restriction","restriction_scope":"direct_outreach"}'
            ),
        )
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    assert await checks_service.check_account(account_id, trigger="test") is None

    async with app.state.database.session_factory() as session:
        updated = await session.get(Account, account_id)
        assert updated is not None
        snapshot = json.loads(updated.spambot_response_json)
        assert updated.status is AccountStatus.PAUSED
        assert updated.cooldown_reason == "spambot_temp_ban"
        assert updated.spambot_status == "error"
        assert snapshot["check_error_code"] == "proxy_unavailable"
        assert snapshot["previous"]["text"] == "old restriction"


@pytest.mark.asyncio
async def test_bulk_spambot_check_reports_real_progress_and_skips_no_session_accounts(
    checks_service,
    mock_gateway,
    app,
):
    mock_gateway.responses.extend(
        (
            SpamBotResult(SpamBotStatus.FREE, "free", parser_signal="mock"),
            SpamBotResult(SpamBotStatus.ACTIVE_RESTRICTION, "limited", parser_signal="mock"),
        )
    )
    async with app.state.database.session_factory() as session:
        session.add_all(
            (
                Account(
                    label="spambot-first",
                    telegram_user_id=7,
                    session_source=AccountSessionSource.SESSION_STRING,
                ),
                Account(
                    label="spambot-second",
                    telegram_user_id=8,
                    session_source=AccountSessionSource.SESSION_STRING,
                ),
                Account(label="no-session", telegram_user_id=9),
            )
        )
        await session.commit()

    progress: list[tuple[int, int]] = []

    async def on_progress(completed: int, total: int) -> None:
        progress.append((completed, total))

    summary = await checks_service.check_all_accounts(concurrency=1, on_progress=on_progress)

    assert [entry.label for entry in summary.entries] == ["spambot-first", "spambot-second"]
    assert summary.count(SpamBotStatus.FREE) == 1
    assert summary.count(SpamBotStatus.ACTIVE_RESTRICTION) == 1
    assert summary.skipped_without_session == 1
    assert progress == [(0, 2), (1, 2), (2, 2)]
