from datetime import UTC, datetime, timedelta

import pytest

from proxima_traffic.models import Account, AccountStatus, ensure_utc
from proxima_traffic.spambot import SpamBotResult, SpamBotStatus
from proxima_traffic.spambot_checks import SpamBotCheckService


class MockGateway:
    def __init__(self):
        self.responses = []
        self.calls = []

    async def check_spambot_status(self, *, account: Account) -> SpamBotResult:
        self.calls.append(account.id)
        if self.responses:
            return self.responses.pop(0)
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
        settings=MockSettings()
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
