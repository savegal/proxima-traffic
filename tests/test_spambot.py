from datetime import UTC, datetime, timedelta

import pytest

from proxima_traffic.config import Settings
from proxima_traffic.db import Database
from proxima_traffic.gateways import PermanentGatewayError, SentMessage, TemporaryGatewayError
from proxima_traffic.main import create_app
from proxima_traffic.models import (
    Account,
    AccountStatus,
    Campaign,
    CampaignMode,
    Target,
    ensure_utc,
    utc_now,
)
from proxima_traffic.spambot import (
    SpamBotRestrictionScope,
    SpamBotResult,
    SpamBotStatus,
    parse_spambot_response,
)
from proxima_traffic.spambot_checks import SpamBotCheckService

REFERENCE_TIME = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "message",
    [
        "Good news! No limits are currently applied to your account.",
        "Ваш аккаунт свободен от каких-либо ограничений.",
        "Ваш обліковий запис не має жодних обмежень.",
        "Tu cuenta no tiene restricciones.",
        "A sua conta está sem restrições.",
        "Dein Account hat keine Einschränkungen.",
        "Votre compte ne présente aucune restriction.",
        "Il tuo account non ha nessuna limitazione.",
        "Twoje konto ma brak ograniczeń.",
        "Hesabınızda herhangi bir kısıtlama yok.",
        "Akun Anda tidak ada batasan.",
        "لا توجد قيود على حسابك.",
        "你的帐号没有任何限制。",
        "アカウントに制限はありません。",
        "계정에 제한이 없습니다.",
    ],
)
def test_parser_recognizes_free_responses_in_common_locales(message: str) -> None:
    result = parse_spambot_response(message, now=REFERENCE_TIME)

    assert result.status is SpamBotStatus.FREE
    assert result.restricted_until is None


@pytest.mark.parametrize(
    "message",
    [
        "Good news, your account has no restrictions. You're free as a bird!",
        "Good news, all restrictions have been removed from your account!",
        "Your account is not limited. No limitations are currently applied.",
    ],
)
def test_parser_recognizes_current_english_free_variants(message: str) -> None:
    result = parse_spambot_response(message, now=REFERENCE_TIME)

    assert result.status is SpamBotStatus.FREE
    assert result.is_restricted is False


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "Your account is limited until 15 August 2026 14:30 UTC.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
        (
            "Ваш аккаунт ограничен до 15 августа 2026 14:30 UTC.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
        (
            "Tu cuenta está limitada hasta el 15 de agosto de 2026 a las 14:30 UTC.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
        (
            "Dein Account ist eingeschränkt bis 2026-08-15 16:30 UTC+02:00.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
        (
            "你的帐号限制至 2026年8月15日 14:30 UTC。",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
    ],
)
def test_parser_extracts_temporary_restriction_expiry(
    message: str,
    expected: datetime,
) -> None:
    result = parse_spambot_response(message, now=REFERENCE_TIME)

    assert result.status is SpamBotStatus.TEMPORARY_RESTRICTION
    assert result.restricted_until == expected


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "Your account will remain limited until 2026-08-15 14:30 UTC.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
        (
            "All restrictions will be lifted on August 15, 2026 at 14:30 UTC.",
            datetime(2026, 8, 15, 14, 30, tzinfo=UTC),
        ),
    ],
)
def test_parser_extracts_current_english_temporary_variants(
    message: str,
    expected: datetime,
) -> None:
    result = parse_spambot_response(message, now=REFERENCE_TIME)

    assert result.status is SpamBotStatus.TEMPORARY_RESTRICTION
    assert result.restricted_until == expected


@pytest.mark.parametrize(
    "message",
    [
        "К сожалению, ваш аккаунт ограничен навсегда за серьезные нарушения.",
        "Your account was permanently restricted and this cannot be undone.",
        "Tu cuenta está restringida permanentemente.",
        "你的帐号已被永久限制。",
        "アカウントは永久に制限されています。",
    ],
)
def test_parser_recognizes_permanent_restrictions_without_dates(message: str) -> None:
    result = parse_spambot_response(message, now=REFERENCE_TIME)

    assert result.status is SpamBotStatus.PERMANENT_RESTRICTION
    assert result.restricted_until is None


def test_parser_uses_safe_unknown_for_changed_bot_copy() -> None:
    result = parse_spambot_response(
        "The account report could not be generated.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.UNKNOWN
    assert result.parser_signal == "unrecognised_response"
    assert result.is_restricted is False


def test_explicit_restriction_wins_over_positive_surrounding_copy() -> None:
    result = parse_spambot_response(
        "Good news: your account is restricted until 15 August 2026 14:30 UTC.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.TEMPORARY_RESTRICTION
    assert result.restricted_until == datetime(2026, 8, 15, 14, 30, tzinfo=UTC)
    assert result.parser_signal == "restriction_marker_with_date"


def test_parser_recognizes_captured_active_restriction_without_expiry() -> None:
    result = parse_spambot_response(
        "Hello Карина!\n\n"
        "I’m very sorry that you had to contact me. Unfortunately, some actions can "
        "trigger a harsh response from our anti-spam systems. If you think your account "
        "was limited by mistake, you can submit a complaint to our moderators.\n\n"
        "While the account is limited, you will not be able to send messages to people "
        "who do not have your number in their phone contacts or add them to groups and "
        "channels. Of course, when people contact you first, you can always reply to them.",
        now=REFERENCE_TIME,
        button_rows=(
            ("OK",),
            ("What is spam?",),
            ("I was wrong, please release me",),
            ("This is a mistake",),
        ),
    )

    assert result.status is SpamBotStatus.ACTIVE_RESTRICTION
    assert result.is_restricted is True
    assert result.restricted_until is None
    assert result.parser_signal == "active_restriction_without_expiry"
    assert result.button_rows[-1] == ("This is a mistake",)
    assert result.restriction_scope is SpamBotRestrictionScope.DIRECT_OUTREACH_AND_INVITES
    assert result.allows_reply_to_inbound is True


def test_parser_recognizes_captured_dutch_active_restriction() -> None:
    result = parse_spambot_response(
        "Hoi Chris!\n\n"
        "Terwijl je account beperkt is, kun je geen berichten sturen naar gebruikers die "
        "jou niet als contact hebben opgeslagen. Je kunt hen ook niet aan groepen en "
        "kanalen toevoegen.",
        now=REFERENCE_TIME,
        button_rows=(
            ("OK",),
            ("Wat is spam?",),
            ("Mijn fout, geef me weer vrij",),
            ("Dit is een fout.",),
        ),
    )

    assert result.status is SpamBotStatus.ACTIVE_RESTRICTION
    assert result.parser_signal == "active_restriction_without_expiry"


def test_parser_keeps_explicit_but_new_restriction_copy_in_protective_state() -> None:
    result = parse_spambot_response(
        "Your account is restricted. We cannot provide an exact date at this time.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.RESTRICTION_UNCLEAR
    assert result.is_restricted is True
    assert result.restriction_scope is SpamBotRestrictionScope.UNSPECIFIED


def test_parser_does_not_treat_future_lifting_copy_as_a_free_account() -> None:
    result = parse_spambot_response(
        "Your account is currently limited. If this was a mistake, all limitations will "
        "be lifted from your account soon. While the account is limited, you cannot send "
        "messages to people who do not have your number in their contacts.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.ACTIVE_RESTRICTION
    assert result.is_restricted is True


def test_parser_keeps_onboarding_question_unknown_not_restricted() -> None:
    result = parse_spambot_response(
        "Hello! I can help you find out if your account was limited.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.UNKNOWN
    assert result.is_restricted is False
    assert result.parser_signal == "status_inquiry_without_verdict"


def test_parser_does_not_misread_a_reply_capability_as_chat_delivery_permission() -> None:
    result = parse_spambot_response(
        "While your account is limited, you cannot send messages to people who don't have "
        "your number in their contacts. Remember that you can still reply to anyone who "
        "messages you first.",
        now=REFERENCE_TIME,
    )

    assert result.status is SpamBotStatus.ACTIVE_RESTRICTION
    assert result.restriction_scope is SpamBotRestrictionScope.DIRECT_OUTREACH
    assert result.allows_reply_to_inbound is True


class SpamBotGateway:
    def __init__(self, result: SpamBotResult) -> None:
        self.result = result
        self.check_calls = 0

    async def check_spambot_status(self, **kwargs: object) -> SpamBotResult:
        del kwargs
        self.check_calls += 1
        return self.result


@pytest.mark.asyncio
async def test_check_service_persists_temporary_ban_and_uses_cached_result(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'spambot.db'}")
    await database.create_schema()
    expiry = utc_now() + timedelta(days=5)
    gateway = SpamBotGateway(
        SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION,
            f"limited until {expiry.isoformat()}",
            restricted_until=expiry,
        )
    )
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'spambot.db'}",
        worker_enabled=False,
        spambot_min_check_interval_seconds=3600,
    )
    checks = SpamBotCheckService(database.session_factory, gateway, settings)
    async with database.session_factory() as session:
        account = Account(label="temporary-spambot", telegram_user_id=8001, authorized=True)
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    first = await checks.check_account(account_id, trigger="test")
    second = await checks.check_account(account_id, trigger="preflight")

    assert first is not None and first.status is SpamBotStatus.TEMPORARY_RESTRICTION
    assert second is not None and second.parser_signal == "cached"
    assert gateway.check_calls == 1
    async with database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        assert account.status is AccountStatus.PAUSED
        assert ensure_utc(account.cooldown_until) == expiry
        assert account.cooldown_reason == "spambot_temp_ban"
        assert account.spambot_status == SpamBotStatus.TEMPORARY_RESTRICTION.value
        assert account.spambot_checked_at is not None
    await database.dispose()


@pytest.mark.asyncio
async def test_check_service_records_unknown_without_pausing_account(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'spambot-unknown.db'}")
    await database.create_schema()
    gateway = SpamBotGateway(SpamBotResult(SpamBotStatus.UNKNOWN, "unexpected reply"))
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'spambot-unknown.db'}",
        worker_enabled=False,
        spambot_unknown_cooldown_seconds=120,
    )
    checks = SpamBotCheckService(database.session_factory, gateway, settings)
    async with database.session_factory() as session:
        account = Account(label="unknown-spambot", telegram_user_id=8002, authorized=True)
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    await checks.check_account(account_id, trigger="test")

    async with database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        assert account.status is AccountStatus.ACTIVE
        assert account.cooldown_reason is None
        assert account.cooldown_until is None
        assert account.spambot_status == SpamBotStatus.UNKNOWN.value
    await database.dispose()


@pytest.mark.asyncio
async def test_check_service_schedules_only_legacy_unknown_pauses_for_recheck(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'spambot-repair.db'}")
    await database.create_schema()
    checks = SpamBotCheckService(
        database.session_factory,
        SpamBotGateway(SpamBotResult(SpamBotStatus.UNKNOWN, "unexpected reply")),
    )
    async with database.session_factory() as session:
        legacy = Account(
            label="legacy-unknown",
            telegram_user_id=8010,
            authorized=True,
            status=AccountStatus.PAUSED,
            cooldown_until=utc_now() + timedelta(hours=1),
            cooldown_reason="spambot_parse_unknown",
            last_error="@SpamBot: status could not be classified",
        )
        real_pause = Account(
            label="real-temporary-ban",
            telegram_user_id=8011,
            authorized=True,
            status=AccountStatus.PAUSED,
            cooldown_until=utc_now() + timedelta(days=1),
            cooldown_reason="spambot_temp_ban",
        )
        session.add_all((legacy, real_pause))
        await session.commit()
        legacy_id = int(legacy.id)
        real_pause_id = int(real_pause.id)

    assert await checks.prepare_legacy_unknown_rechecks() == 1

    async with database.session_factory() as session:
        legacy = await session.get(Account, legacy_id)
        real_pause = await session.get(Account, real_pause_id)
        assert legacy is not None
        assert legacy.status is AccountStatus.PAUSED
        assert ensure_utc(legacy.cooldown_until) <= utc_now()
        assert legacy.cooldown_reason == "spambot_parse_unknown"
        assert legacy.spambot_checked_at is None
        assert legacy.last_error == "@SpamBot: status could not be classified"
        assert real_pause is not None
        assert real_pause.status is AccountStatus.PAUSED
        assert real_pause.cooldown_reason == "spambot_temp_ban"
    await database.dispose()


class RestrictedSendGateway(SpamBotGateway):
    async def send_message(self, **kwargs: object) -> SentMessage:
        del kwargs
        raise TemporaryGatewayError("peer flood", code="account_restricted")


class UnauthorizedSpamBotGateway:
    async def check_spambot_status(self, **kwargs: object) -> SpamBotResult:
        del kwargs
        raise PermanentGatewayError(
            "account session is no longer authorized",
            code="account_unauthorized",
        )


@pytest.mark.asyncio
async def test_spambot_check_disables_an_unauthorized_session(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'spambot-unauthorized.db'}")
    await database.create_schema()
    checks = SpamBotCheckService(database.session_factory, UnauthorizedSpamBotGateway())
    async with database.session_factory() as session:
        account = Account(
            label="spambot-unauthorized",
            telegram_user_id=8012,
            authorized=True,
        )
        session.add(account)
        await session.commit()
        account_id = int(account.id)

    assert await checks.check_account(account_id, trigger="test", force=True) is None

    async with database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        assert account.status is AccountStatus.DISABLED
        assert account.authorized is False
        assert account.spambot_status == "error"
        assert account.spambot_parser_signal == "gateway_error:account_unauthorized"
        assert "no longer authorized" in account.last_error
    await database.dispose()


@pytest.mark.asyncio
async def test_send_restriction_uses_spambot_as_the_source_of_truth(tmp_path) -> None:
    expiry = utc_now() + timedelta(days=2)
    gateway = RestrictedSendGateway(
        SpamBotResult(
            SpamBotStatus.TEMPORARY_RESTRICTION,
            "limited until later",
            restricted_until=expiry,
        )
    )
    app = create_app(
        settings=Settings(
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'spambot-reactive.db'}",
            admin_api_key="test-admin-key",
            dry_run=True,
            worker_enabled=False,
        ),
        gateway=gateway,
    )
    await app.state.database.create_schema()
    async with app.state.database.session_factory() as session:
        account = Account(
            label="reactive-spambot",
            telegram_user_id=8003,
            ownership_confirmed=True,
            authorized=True,
        )
        target = Target(
            chat_ref="@spambot_reactive_target",
            title="SpamBot reactive target",
            approved_by_operator=True,
            approved_at=utc_now(),
        )
        session.add_all((account, target))
        await session.flush()
        campaign = Campaign(
            name="spambot-reactive-campaign",
            mode=CampaignMode.BROADCAST,
            primary_account_id=account.id,
            target_id=target.id,
            primary_text="Test",
        )
        session.add(campaign)
        await session.commit()
        campaign_id = int(campaign.id)
        account_id = int(account.id)

    await app.state.run_queue.enqueue(campaign_id, "spambot-reactive-run")
    run = await app.state.queue_worker.process_next()

    assert run is not None
    assert run.error_code == "account_restricted"
    assert gateway.check_calls == 1
    async with app.state.database.session_factory() as session:
        account = await session.get(Account, account_id)
        assert account is not None
        assert account.status is AccountStatus.PAUSED
        assert ensure_utc(account.cooldown_until) == expiry
        assert account.cooldown_reason == "spambot_temp_ban"
    await app.state.database.dispose()
